"""Build a static DAG and concrete task instances from convergence rules."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Mapping, Optional, Protocol, Tuple

from .models import DAG, TaskDefinition, TaskInstance, make_instance_id

class ConfigLoaderProtocol(Protocol):
    """The subset of the existing ConfigLoader used by DAGBuilder."""

    def validate(self) -> List[str]: ...

    def get_all_rules(self) -> List[Dict[str, Any]]: ...


class DAGBuilder:
    def __init__(self, config_loader: ConfigLoaderProtocol):
        self.config_loader = config_loader

    def build(self) -> DAG:
        errors = self.config_loader.validate()
        if errors:
            details = "\n".join(f"- {error}" for error in errors)
            raise ValueError(f"Invalid convergence configuration:\n{details}")

        definitions = {
            definition.taskname: definition
            for definition in (
                TaskDefinition.from_rule(rule)
                for rule in self.config_loader.get_all_rules()
            )
        }
        dependencies = {
            name: definition.depends_on
            for name, definition in definitions.items()
        }
        order = _topological_sort(dependencies)
        return DAG(definitions, dependencies, tuple(order))

    def create_instance(
        self,
        dag: DAG,
        taskname: str,
        scheduled_time: datetime,
    ) -> TaskInstance:
        task = dag.get_task(taskname)
        data_start, data_end = calculate_data_window(
            scheduled_time,
            task.aggregation_interval,
        )
        batch_id = make_batch_id(data_end)
        upstream_ids = required_upstream_ids(dag, task, data_start, data_end)
        return TaskInstance(
            task=task,
            batch_id=batch_id,
            scheduled_time=scheduled_time,
            data_start=data_start,
            data_end=data_end,
            upstream_instance_ids=upstream_ids,
        )

    def create_batch_instances(
        self,
        dag: DAG,
        data_end: datetime,
        tasknames: Optional[Iterable[str]] = None,
    ) -> List[TaskInstance]:
        """Create instances for a manual/backfill batch.

        All instances share a batch boundary. Their data_start still follows
        each task's own aggregation_interval.
        """
        selected = set(tasknames) if tasknames is not None else set(dag.tasks)
        batch_id = make_batch_id(data_end)
        instances: List[TaskInstance] = []

        for taskname in dag.topological_order:
            if taskname not in selected:
                continue
            task = dag.get_task(taskname)
            data_start = subtract_interval(data_end, task.aggregation_interval)
            instances.append(
                TaskInstance(
                    task=task,
                    batch_id=batch_id,
                    scheduled_time=data_end,
                    data_start=data_start,
                    data_end=data_end,
                    upstream_instance_ids=required_upstream_ids(dag, task, data_start, data_end),
                )
            )
        return instances


def make_batch_id(data_end: datetime) -> str:
    return data_end.isoformat(timespec="seconds")


def required_upstream_ids(dag: DAG, task: TaskDefinition,
                          data_start: datetime, data_end: datetime) -> Tuple[str, ...]:
    """Require every complete parent window covering the child's [start, end)."""
    result = []
    for parent_name in task.depends_on:
        parent = dag.get_task(parent_name)
        interval = parent.aggregation_interval
        if align_data_end(data_start, interval) != data_start or align_data_end(data_end, interval) != data_end:
            raise ValueError(f"Incompatible aggregation windows: {parent_name} -> {task.taskname}")
        cursor = data_end
        ends = []
        while cursor > data_start:
            ends.append(cursor)
            cursor = subtract_interval(cursor, interval)
        if cursor != data_start:
            raise ValueError(f"Parent windows do not cover {task.taskname}: {parent_name}")
        result.extend(make_instance_id(parent_name, make_batch_id(end)) for end in reversed(ends))
    return tuple(result)


def calculate_data_window(
    scheduled_time: datetime,
    aggregation_interval: str,
) -> Tuple[datetime, datetime]:
    data_end = align_data_end(scheduled_time, aggregation_interval)
    return subtract_interval(data_end, aggregation_interval), data_end


def align_data_end(scheduled_time: datetime, interval: str) -> datetime:
    if interval == "PT5M":
        floored_minute = (scheduled_time.minute // 5) * 5
        return scheduled_time.replace(
            minute=floored_minute, second=0, microsecond=0
        )
    if interval == "PT1H":
        return scheduled_time.replace(minute=0, second=0, microsecond=0)
    if interval == "P1D":
        return scheduled_time.replace(hour=0, minute=0, second=0, microsecond=0)
    if interval == "P1M":
        return scheduled_time.replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        )
    raise ValueError(f"Unsupported aggregation_interval: {interval}")


def subtract_interval(data_end: datetime, interval: str) -> datetime:
    if interval == "PT5M":
        return data_end - timedelta(minutes=5)
    if interval == "PT1H":
        return data_end - timedelta(hours=1)
    if interval == "P1D":
        return data_end - timedelta(days=1)
    if interval == "P1M":
        year, month = data_end.year, data_end.month - 1
        if month == 0:
            year, month = year - 1, 12
        return data_end.replace(year=year, month=month, day=1)
    raise ValueError(f"Unsupported aggregation_interval: {interval}")


def _topological_sort(
    dependencies: Mapping[str, Tuple[str, ...]],
) -> List[str]:
    state: Dict[str, int] = {}
    result: List[str] = []
    path: List[str] = []

    def visit(taskname: str) -> None:
        current_state = state.get(taskname, 0)
        if current_state == 2:
            return
        if current_state == 1:
            cycle_start = path.index(taskname)
            cycle = path[cycle_start:] + [taskname]
            raise ValueError(f"DAG contains a cycle: {' -> '.join(cycle)}")

        state[taskname] = 1
        path.append(taskname)
        for parent in dependencies.get(taskname, ()):
            if parent not in dependencies:
                raise ValueError(
                    f"Task '{taskname}' depends on unknown task '{parent}'"
                )
            visit(parent)
        path.pop()
        state[taskname] = 2
        result.append(taskname)

    for name in dependencies:
        visit(name)
    return result
