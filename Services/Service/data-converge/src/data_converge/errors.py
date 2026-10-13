"""Execution failures understood by runners and task orchestration."""

from __future__ import annotations


class TaskExecutionError(RuntimeError):
    """Safe diagnostic and execution evidence, independent of a transport.

    An unknown outcome always prevents automatic retry, even when retryable
    is also set. Runners must not include credentials or remote SQL in message.
    """

    def __init__(self, message: str, *, retryable: bool = False,
                 outcome_unknown: bool = False, query_id: str | None = None):
        super().__init__(message)
        self.retryable = retryable
        self.outcome_unknown = outcome_unknown
        self.query_id = query_id
