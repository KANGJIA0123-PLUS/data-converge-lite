"""Process-local execution state for callers without a durable ledger."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable, Mapping

from .models import TaskInstance, TaskStatus
from .retry import can_start_attempt


class InMemoryExecutionRepository:
    """Preserve shared status dictionaries without claiming durable recovery.

    Execution metadata belongs to the current TaskInstance, as in the original
    in-memory executor. Completed records retain only status and retry facts.
    """

    def __init__(self, known_statuses: dict[str, TaskStatus] | None = None):
        self.known_statuses = known_statuses if known_statuses is not None else {}
        self.records: dict[str, dict] = {}
        self._instances: dict[str, TaskInstance] = {}
        self._claims: dict[str, dict] = {}

    def enqueue_and_advance(self, instances: Iterable[TaskInstance],
                            next_cursors: Mapping[str, datetime]) -> None:
        if next_cursors:
            raise ValueError("In-memory execution does not manage schedule cursors")
        self._instances = {instance.instance_id: instance for instance in instances}

    def get_instance(self, instance_id: str) -> dict | None:
        record = self.records.get(instance_id)
        if record is None:
            return None
        snapshot = record.copy()
        snapshot.update(self._claims.get(instance_id, {}))
        return snapshot

    def get_status(self, instance_id: str) -> TaskStatus | None:
        if instance_id in self.records:
            return TaskStatus(self.records[instance_id]["status"])
        return self.known_statuses.get(instance_id)

    def claim(self, instance_id: str, *, now: datetime | None = None,
              max_attempts: int = 3) -> bool:
        instance = self._instances.get(instance_id)
        now = now or datetime.now(timezone.utc)
        record = self.records.get(instance_id, {})
        if instance is None and not record:
            raise KeyError(instance_id)
        initial = instance.status if instance is not None else TaskStatus.PENDING
        status = record.get("status", self.known_statuses.get(instance_id, initial))
        if not can_start_attempt(status, record.get("attempts", 0),
                                 record.get("retryable", False), record.get("next_retry_at"),
                                 now=now, max_attempts=max_attempts):
            return False
        record = self.records.setdefault(instance_id, {"attempts": 0})
        record.update(status=TaskStatus.RUNNING, attempts=record["attempts"] + 1)
        self._claims[instance_id] = dict(
            started_at=now, finished_at=getattr(instance, "finished_at", None),
            error=getattr(instance, "error", record.get("error")),
            query_id=getattr(instance, "query_id", None))
        return True

    def finish(self, instance_id: str, status: TaskStatus, *, error: str | None = None,
               retryable: bool = False, next_retry_at: datetime | None = None,
               now: datetime | None = None) -> None:
        if status not in (TaskStatus.SUCCESS, TaskStatus.FAILED, TaskStatus.UNKNOWN):
            raise ValueError("finish requires SUCCESS, FAILED or UNKNOWN")
        record = self.records.get(instance_id)
        if record is None or record["status"] != TaskStatus.RUNNING:
            raise RuntimeError(f"Cannot finish an unclaimed instance: {instance_id}")
        record.update(status=status, error=error, retryable=retryable and status == TaskStatus.FAILED,
                      next_retry_at=next_retry_at)
        self.known_statuses[instance_id] = status
        self._claims.pop(instance_id, None)

    def mark_blocked(self, instance_id: str, reason: str) -> None:
        self.known_statuses[instance_id] = TaskStatus.BLOCKED
        if instance_id in self.records:
            self.records[instance_id].update(status=TaskStatus.BLOCKED, error=reason)
