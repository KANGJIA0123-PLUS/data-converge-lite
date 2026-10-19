"""Ownership contract consumed by application services and execution guards."""

from typing import Protocol


class WriterLease(Protocol):
    """Check local lease validity; never substitute a remote GET before a write."""

    def acquire(self) -> bool: ...

    def assert_owned(self) -> None: ...

    @property
    def lost(self) -> bool: ...
