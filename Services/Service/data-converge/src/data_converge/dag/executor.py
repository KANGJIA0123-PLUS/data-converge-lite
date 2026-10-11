"""Execute complete dependency windows with durable duplicate suppression."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Callable, Dict, Iterable, List, Mapping, Optional

from .models import DAG, TaskInstance, TaskStatus

if TYPE_CHECKING:
    from data_converge.state import StateStore

TaskRunner = Callable[[TaskInstance], None]


@dataclass(frozen=True)
class ExecutionReport:
    instances: Mapping[str, TaskInstance]

    @property
    def succeeded(self) -> List[TaskInstance]:
        return [i for i in self.instances.values() if i.status == TaskStatus.SUCCESS]

    @property
    def failed(self) -> List[TaskInstance]:
        return [i for i in self.instances.values() if i.status == TaskStatus.FAILED]

    @property
    def blocked(self) -> List[TaskInstance]:
        return [i for i in self.instances.values() if i.status == TaskStatus.BLOCKED]

    @property
    def unknown(self) -> List[TaskInstance]:
        return [i for i in self.instances.values() if i.status == TaskStatus.UNKNOWN]


class DAGExecutor:
    def __init__(self, dag: DAG, runner: TaskRunner,
                 known_statuses: Optional[Dict[str, TaskStatus]] = None,
                 max_concurrent_tasks: int = 1, *, state_store: StateStore | None = None,
                 max_attempts: int = 3, retry_delay_seconds: float = 30,
                 readiness_check: Callable[[TaskInstance, datetime], str | None] | None = None):
        if max_concurrent_tasks <= 0 or max_attempts <= 0 or retry_delay_seconds < 0:
            raise ValueError("Concurrency and attempts must be positive; retry delay cannot be negative")
        self.dag = dag
        self.runner = runner
        self.known_statuses = known_statuses if known_statuses is not None else {}
        self.max_concurrent_tasks = max_concurrent_tasks
        self.state_store = state_store
        self.max_attempts = max_attempts
        self.retry_delay_seconds = retry_delay_seconds
        self.readiness_check = readiness_check
        self._memory: dict[str, dict] = {}

    def execute(self, instances: Iterable[TaskInstance], *, now: datetime | None = None,
                max_executions: int | None = None,
                should_stop: Callable[[], bool] | None = None) -> ExecutionReport:
        instance_list = list(instances)
        by_id = {instance.instance_id: instance for instance in instance_list}
        if len(by_id) != len(instance_list):
            raise ValueError("Duplicate instance_id in execute()")
        if not instance_list:
            return ExecutionReport({})
        now = now or self._now(instance_list[0])
        if self.state_store:
            self.state_store.enqueue_and_advance(instance_list, {})
        for instance in instance_list:
            self._restore(instance)

        order = {name: index for index, name in enumerate(self.dag.topological_order)}
        instance_list.sort(key=lambda i: (i.data_end, order[i.taskname]))
        attempted: set[str] = set()
        budget = max_executions if max_executions is not None else len(instance_list)
        while len(attempted) < budget:
            if should_stop and should_stop():
                break
            ready = []
            for instance in instance_list:
                if instance.instance_id in attempted or not self._eligible(instance, now):
                    continue
                reason = self.readiness_check(instance, now) if self.readiness_check else None
                if reason is not None:
                    self._mark_blocked(instance, reason)
                    continue
                if self._upstreams_succeeded(instance, by_id):
                    ready.append(instance)
            ready = ready[:min(self.max_concurrent_tasks, budget - len(attempted))]
            if not ready:
                break
            claimed = [i for i in ready if self._claim(i, now)]
            attempted.update(i.instance_id for i in ready)
            if claimed:
                self._execute_ready_instances(claimed, now)

        for instance in instance_list:
            if instance.status not in (TaskStatus.PENDING, TaskStatus.BLOCKED):
                continue
            if not self._upstreams_succeeded(instance, by_id):
                self._mark_blocked(instance, self._build_blocked_reason(instance, by_id))
        return ExecutionReport(by_id)

    def _mark_blocked(self, instance: TaskInstance, reason: str) -> None:
        instance.status = TaskStatus.BLOCKED
        instance.error = reason
        if self.state_store:
            self.state_store.mark_blocked(instance.instance_id, reason)
        else:
            self.known_statuses[instance.instance_id] = instance.status
            if instance.instance_id in self._memory:
                self._memory[instance.instance_id].update(status=instance.status, error=reason)

    def _restore(self, instance: TaskInstance) -> None:
        record = self.state_store.get_instance(instance.instance_id) if self.state_store else self._memory.get(instance.instance_id)
        if record:
            instance.status = TaskStatus(record["status"])
            instance.error = record.get("error")
            instance.query_id = record.get("query_id")
            for attr in ("started_at", "finished_at"):
                value = record.get(attr)
                setattr(instance, attr, datetime.fromisoformat(value) if isinstance(value, str) else value)
        elif instance.instance_id in self.known_statuses:
            instance.status = self.known_statuses[instance.instance_id]

    def _eligible(self, instance: TaskInstance, now: datetime) -> bool:
        if instance.status == TaskStatus.PENDING:
            return True
        if instance.status not in (TaskStatus.BLOCKED, TaskStatus.FAILED):
            return False
        record = self.state_store.get_instance(instance.instance_id) if self.state_store else self._memory.get(instance.instance_id)
        if instance.status == TaskStatus.BLOCKED and (not record or record.get("attempts", 0) == 0):
            return True
        if not record or not record.get("retryable") or record["attempts"] >= self.max_attempts:
            return False
        deadline = record.get("next_retry_at")
        if isinstance(deadline, str):
            deadline = datetime.fromisoformat(deadline)
        return deadline is None or deadline <= now

    def _claim(self, instance: TaskInstance, now: datetime) -> bool:
        if self.state_store:
            if not self.state_store.claim(instance.instance_id, now=now, max_attempts=self.max_attempts):
                self._restore(instance)
                return False
            self._restore(instance)
        else:
            record = self._memory.setdefault(instance.instance_id, {"attempts": 0})
            record.update(status=TaskStatus.RUNNING, attempts=record["attempts"] + 1)
            instance.status = TaskStatus.RUNNING
            instance.started_at = now
        return True

    def _execute_ready_instances(self, instances: List[TaskInstance], now: datetime) -> None:
        with ThreadPoolExecutor(max_workers=len(instances), thread_name_prefix="dag-task") as pool:
            futures = {pool.submit(self.runner, instance): instance for instance in instances}
            for future in as_completed(futures):
                instance = futures[future]
                retryable = False
                try:
                    future.result()
                except Exception as exc:
                    instance.status, retryable, instance.error = self._classify_error(exc)
                else:
                    instance.status = TaskStatus.SUCCESS
                    instance.error = None
                instance.finished_at = self._now(instance)
                next_retry_at = instance.finished_at + timedelta(seconds=self.retry_delay_seconds) if retryable else None
                # A failed success commit must stop dispatch, leaving RUNNING for recovery.
                # Keep persistence outside the runner exception handler.
                if self.state_store:
                    self.state_store.finish(instance.instance_id, instance.status, error=instance.error,
                                            retryable=retryable, next_retry_at=next_retry_at,
                                            now=instance.finished_at)
                else:
                    self._memory[instance.instance_id].update(status=instance.status, error=instance.error,
                                                              retryable=retryable, next_retry_at=next_retry_at)
                    self.known_statuses[instance.instance_id] = instance.status

    @staticmethod
    def _classify_error(exc: Exception) -> tuple[TaskStatus, bool, str]:
        from data_converge.sql_execution.client import SQLExecutionError
        if isinstance(exc, SQLExecutionError):
            if exc.outcome_unknown:
                return TaskStatus.UNKNOWN, False, str(exc)
            return TaskStatus.FAILED, exc.retryable, str(exc)
        return TaskStatus.UNKNOWN, False, f"UNEXPECTED_RUNNER_ERROR:{type(exc).__name__}"

    def _status(self, instance_id: str, current: Mapping[str, TaskInstance]) -> TaskStatus | None:
        if instance_id in current:
            return current[instance_id].status
        if self.state_store:
            return self.state_store.get_status(instance_id)
        return self.known_statuses.get(instance_id)

    def _upstreams_succeeded(self, instance: TaskInstance,
                            current_instances: Mapping[str, TaskInstance]) -> bool:
        return all(self._status(parent, current_instances) == TaskStatus.SUCCESS
                   for parent in instance.upstream_instance_ids)

    def _build_blocked_reason(self, instance: TaskInstance,
                              current_instances: Mapping[str, TaskInstance]) -> str:
        details = []
        for parent in instance.upstream_instance_ids:
            status = self._status(parent, current_instances)
            if status != TaskStatus.SUCCESS:
                details.append(f"{parent}={status.value if status else 'NOT_FOUND'}")
        return "Upstream task instances are not successful: " + ", ".join(details)

    @staticmethod
    def _now(instance: TaskInstance) -> datetime:
        return datetime.now(tz=instance.data_end.tzinfo)
