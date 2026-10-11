"""Small local window ledger; this is not a distributed lock or SQL transaction."""

from .store import StateStore

__all__ = ["StateStore"]
