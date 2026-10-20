"""Shared attempt eligibility for scheduling and durable claims."""

from __future__ import annotations

from datetime import datetime

from .models import TaskStatus
from .builder import timeline_time


def can_start_attempt(status: TaskStatus, attempts: int, retryable: bool,
                      next_retry_at: datetime | str | None, *, now: datetime,
                      max_attempts: int) -> bool:
    """Decide from supplied facts without reading or changing task state."""
    if status == TaskStatus.PENDING:
        return True
    if status == TaskStatus.BLOCKED and attempts == 0:
        return True
    if status not in (TaskStatus.FAILED, TaskStatus.BLOCKED):
        return False
    if not retryable or attempts >= max_attempts:
        return False
    if isinstance(next_retry_at, str):
        next_retry_at = datetime.fromisoformat(next_retry_at)
    return next_retry_at is None or timeline_time(next_retry_at) <= timeline_time(now)
