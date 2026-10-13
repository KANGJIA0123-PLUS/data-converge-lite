"""Small local window ledger; this is not a distributed lock or SQL transaction."""

__all__ = ["StateStore"]

from .store import StateStore
