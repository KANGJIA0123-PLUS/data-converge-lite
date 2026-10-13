"""Contracts used by readiness coordination, independent of the message broker."""

from datetime import datetime
from typing import Protocol


class SignalSourceError(RuntimeError):
    """A source operation failed; the coordinator never logs its private body."""


class ReadySignalSource(Protocol):
    """Keep messages recoverable until committed, or explicitly rejected, then ack."""

    def receive(self, block: bool = True) -> str | bytes | None:
        ...

    def ack(self, raw: str | bytes) -> int | None:
        ...

    def close(self) -> None:
        ...


class ReadinessState(Protocol):
    """Idempotent readiness facts; record must commit before returning."""

    def record_dwd_signal(self, window_start: datetime, dwd_type: str,
                          received_at: datetime) -> None:
        ...

    def get_dwd_types(self, window_start: datetime) -> set[str]:
        ...
