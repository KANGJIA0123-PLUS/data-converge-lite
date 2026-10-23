"""Retry only the read-only startup query, never ambiguous business writes."""

from __future__ import annotations

import errno
import json
import logging
from time import monotonic, sleep

from data_converge.config.startup import StartupSettings
from data_converge.errors import safe_error_details
from data_converge.sql_execution.client import SQLExecutionError

logger = logging.getLogger(__name__)
_TRANSIENT_HTTP = {408, 429, 500, 502, 503, 504}
_TRANSIENT_TRANSPORT = {
    'gaierror', 'ConnectionRefusedError', 'ConnectionResetError', 'ConnectionAbortedError',
    'BrokenPipeError', 'TimeoutError', 'SSLEOFError', 'RemoteDisconnected', 'IncompleteRead',
}
_TRANSIENT_ERRNOS = {
    errno.ECONNREFUSED, errno.ECONNRESET, errno.ECONNABORTED, errno.ETIMEDOUT,
    errno.EHOSTUNREACH, errno.ENETUNREACH, errno.EPIPE,
}


def retryable_startup_error(error: SQLExecutionError) -> bool:
    """Use structured evidence; certificate/configuration/SQL errors fail promptly."""
    details = error.diagnostic
    status, code = details.get('http_status'), details.get('clickhouse_exception_code')
    if (status in (401, 403) or details.get('error_type') == 'SSLCertVerificationError'
            or 'tls_verify_code' in details):
        return False
    if code is not None:
        return code == 81  # UNKNOWN_DATABASE: installation has not created the business DB yet.
    if status is not None:
        return status in _TRANSIENT_HTTP
    if str(error) == 'CLICKHOUSE_CONNECT_FAILED':
        return True
    if str(error) != 'CLICKHOUSE_OUTCOME_UNKNOWN':
        return False
    # QueryDeadline can report expiry without a transport exception. SELECT 1 is
    # safe to repeat even when the server's response was lost; INSERT is not.
    return (not details or details.get('error_type') in _TRANSIENT_TRANSPORT
            or details.get('os_errno') in _TRANSIENT_ERRNOS)


def _continue_waiting(should_stop, assert_owned):
    assert_owned()
    return not should_stop()


def _backoff(delay, should_stop, assert_owned):
    until = monotonic() + delay
    while _continue_waiting(should_stop, assert_owned):
        remaining = until - monotonic()
        if remaining <= 0:
            return True
        sleep(min(0.2, remaining))
    return False


def _timed_out(error, attempt, started):
    details = safe_error_details(error) if error is not None else {}
    logger.error('CLICKHOUSE_STARTUP_TIMEOUT: attempts=%s elapsed_seconds=%.3f diagnostic=%s',
                 attempt, monotonic() - started, json.dumps(details, sort_keys=True))
    raise SQLExecutionError('CLICKHOUSE_STARTUP_TIMEOUT',
                            query_id=getattr(error, 'query_id', None), diagnostic=details) from error


def wait_for_clickhouse(probe, settings: StartupSettings, should_stop, assert_owned) -> bool:
    """Wait for SELECT 1 in the configured database while keeping the runtime alive."""
    started = monotonic()
    deadline = started + settings.timeout_seconds
    delay, attempt, last_error = settings.initial_retry_delay_seconds, 0, None
    while _continue_waiting(should_stop, assert_owned):
        remaining = deadline - monotonic()
        if remaining <= 0:
            _timed_out(last_error, attempt, started)
        attempt += 1
        try:
            probe(min(settings.probe_timeout_seconds, remaining))
        except SQLExecutionError as error:
            if not _continue_waiting(should_stop, assert_owned):
                return False
            if not retryable_startup_error(error):
                raise
            last_error = error
            remaining = deadline - monotonic()
            if remaining <= 0:
                _timed_out(error, attempt, started)
            pause = min(delay, remaining)
            logger.warning('CLICKHOUSE_STARTUP_WAIT: attempt=%s elapsed_seconds=%.3f '
                           'retry_in_seconds=%.3f diagnostic=%s', attempt, monotonic() - started,
                           pause, json.dumps(safe_error_details(error), sort_keys=True))
            if not _backoff(pause, should_stop, assert_owned):
                return False
            delay = min(delay * 2, settings.max_retry_delay_seconds)
        else:
            if not _continue_waiting(should_stop, assert_owned):
                return False
            if monotonic() >= deadline:
                _timed_out(last_error, attempt, started)
            logger.info('CLICKHOUSE_STARTUP_READY: attempts=%s elapsed_seconds=%.3f',
                        attempt, monotonic() - started)
            return True
    return False
