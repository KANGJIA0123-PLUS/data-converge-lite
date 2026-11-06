"""Decide whether a leadership term ended for a recoverable coordination reason.

A term ends when Redis becomes unreachable, the lease expires, or another writer
fenced this one. These are expected in a primary/standby deployment: the process
must release everything it owns and compete again as a standby, not exit and
depend on an external restart. Configuration, ledger and programming errors are
not coordination failures and still stop the process.
"""

from __future__ import annotations

from data_converge.dwd_ready.ports import SignalSourceError
from data_converge.infrastructure.redis.client import RedisConnectionError
from data_converge.leader_election import LeadershipError
from data_converge.state.redis_mirror import StateSyncError

# FencedOut subclasses StateSyncError; RedisSignalError subclasses SignalSourceError.
RECOVERABLE = (LeadershipError, StateSyncError, SignalSourceError, RedisConnectionError)


def is_term_end(error: BaseException) -> bool:
    return isinstance(error, RECOVERABLE)


def reason(error: BaseException) -> str:
    """Fixed public code for logs; never Redis exception text."""
    text = str(error)
    return text if text.replace('_', '').isalnum() and text.isupper() else type(error).__name__
