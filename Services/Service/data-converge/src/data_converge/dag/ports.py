"""Contracts required by orchestration, independent of persistence and SQL."""

from __future__ import annotations

from datetime import datetime
from typing import Iterable, Mapping, Protocol

from .models import TaskInstance, TaskStatus


class TaskRunner(Protocol):
    """Execute one already claimed task; failures follow TaskExecutionError."""

    def __call__(self, instance: TaskInstance) -> None: ...


class ExecutionRepository(Protocol):
    """Own attempt transitions; claim and finish complete before returning.

    The executor registers instances with empty cursors. Durable schedulers may
    use the same registration transaction to advance their schedule cursors.
    The scheduler thread owns this interface; task worker threads only run SQL.
    """

    def enqueue_and_advance(self, instances: Iterable[TaskInstance],
                            next_cursors: Mapping[str, datetime]) -> None: ...

    def get_instance(self, instance_id: str) -> dict | None: ...

    def get_status(self, instance_id: str) -> TaskStatus | None: ...

    def claim(self, instance_id: str, *, now: datetime | None = None,
              max_attempts: int = 3) -> bool: ...

    def finish(self, instance_id: str, status: TaskStatus, *, error: str | None = None,
               retryable: bool = False, next_retry_at: datetime | None = None,
               now: datetime | None = None) -> None: ...

    def mark_blocked(self, instance_id: str, reason: str) -> None: ...
