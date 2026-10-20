from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Mapping, Optional, Tuple


def make_instance_id(taskname: str, batch_id: str) -> str:
    return f"{taskname}@{batch_id}"

class TaskStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    BLOCKED = "BLOCKED"
    UNKNOWN = "UNKNOWN"

@dataclass(frozen=True)
class TaskDefinition:
    taskname: str
    description: str
    source_tables: Tuple[str, ...]
    target_table: str
    aggregation_interval: str
    cron: str
    depends_on: Tuple[str, ...]
    sql_id: str
    cleanup_column: str = "window_time"  # D3: 用于幂等清理的时间列

    @classmethod
    def from_rule(cls, rule: Mapping[str, Any]) -> "TaskDefinition":
        return cls(
            taskname=rule["taskname"],
            description=rule.get("description", ""),
            source_tables=tuple(rule["source_tables"]),
            target_table=rule["target_table"],
            aggregation_interval=rule["aggregation_interval"],
            cron=rule["cron"],
            depends_on=tuple(rule.get("depends_on", [])),
            sql_id=rule["sql_id"],
            cleanup_column=rule.get("cleanup_column", "window_time"),
        )


@dataclass
class TaskInstance:
    """One execution of a task for a specific data window.

    batch_id is based on data_end, so different aggregation levels ending at
    the same boundary can refer to each other deterministically.
    """

    task: TaskDefinition
    batch_id: str
    scheduled_time: datetime
    data_start: datetime
    data_end: datetime
    upstream_instance_ids: Tuple[str, ...]
    status: TaskStatus = TaskStatus.PENDING
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    error: Optional[str] = None
    query_id: Optional[str] = None

    @property
    def instance_id(self) -> str:
        return make_instance_id(self.task.taskname, self.batch_id)

    @property
    def taskname(self) -> str:
        return self.task.taskname



@dataclass(frozen=True)
class DAG:
    tasks: Mapping[str, TaskDefinition]
    dependencies: Mapping[str, Tuple[str, ...]]
    topological_order: Tuple[str, ...]

    def get_task(self, taskname: str) -> TaskDefinition:
        try:
            return self.tasks[taskname]
        except KeyError as exc:
            raise KeyError(f"Unknown taskname: {taskname}") from exc
