"""Compile cron groups without constructing a runtime scheduler."""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, Mapping
from zoneinfo import ZoneInfo

from apscheduler.triggers.cron import CronTrigger

if TYPE_CHECKING:
    from data_converge.dag import DAG


@dataclass(frozen=True)
class CronGroup:
    id: str
    name: str
    trigger: CronTrigger
    tasknames: tuple[str, ...]

    @property
    def kwargs(self) -> Mapping[str, tuple[str, ...]]:
        """Keep the existing job inspection contract without mutable jobs."""
        return MappingProxyType({"tasknames": self.tasknames})


def compile_cron_groups(dag: DAG, timezone: str | ZoneInfo) -> tuple[CronGroup, ...]:
    """Validate exact cron expressions and keep DAG order within each group."""
    groups: dict[str, list[str]] = {}
    for taskname in dag.topological_order:
        groups.setdefault(dag.get_task(taskname).cron, []).append(taskname)

    compiled = []
    for index, (expression, tasknames) in enumerate(groups.items(), start=1):
        try:
            trigger = CronTrigger.from_crontab(expression, timezone=timezone)
        except ValueError as exc:
            raise ValueError(
                f"任务组的 Cron 表达式无效：cron={expression}, tasks={tasknames}"
            ) from exc
        compiled.append(CronGroup(
            id=f"cron_group_{index:03d}",
            name=f"Cron任务组[{expression}]",
            trigger=trigger,
            tasknames=tuple(tasknames),
        ))
    return tuple(compiled)
