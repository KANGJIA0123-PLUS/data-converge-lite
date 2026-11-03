"""Execution failures understood by runners and task orchestration."""

from __future__ import annotations

import re
import ssl
import urllib.error


_PUBLIC_CODES = frozenset("""
    CONFIG_UNREADABLE CLICKHOUSE_CONFIG_MISSING CLICKHOUSE_CONNECTION_INVALID
    CLICKHOUSE_ENV_MISSING CLICKHOUSE_ENV_INVALID CLICKHOUSE_IDENTITY_INVALID
    CLICKHOUSE_PROVIDER_INVALID CLICKHOUSE_CREDENTIAL_MISSING CLICKHOUSE_CREDENTIAL_UNRESOLVED
    CLICKHOUSE_TIMEOUT_INVALID CLICKHOUSE_QUERY_ID_INVALID CLICKHOUSE_DIAGNOSTIC_RESPONSE_INVALID
    KMS_CONFIGURATION_INVALID KMS_PMS_INVALID KMS_DECRYPT_TIMEOUT KMS_OUTPUT_LIMIT
    KMS_DECRYPT_FAILED KMS_SECRET_INVALID KMS_PLATFORM_UNSUPPORTED KMS_DECRYPT_START_FAILED
    KMS_PROTOCOL_INVALID KMS_SECRET_UNREADABLE DWD_READY_CONFIG_INVALID DWD_READY_ENV_MISSING
    DWD_READY_ENV_INVALID DWD_READY_QUEUE_SLOT_INVALID DWD_READY_CLUSTER_REQUIRES_DB_ZERO
    DWD_READY_SOCKET_TIMEOUT_TOO_SHORT DWD_REDIS_PMS_INVALID DWD_REDIS_CREDENTIAL_FAILED
    DWD_REDIS_BINDING_CHANGED DWD_READY_REDIS_CLOSED DWD_READY_REDIS_RECEIVE_FAILED
    DWD_READY_REDIS_ACK_FAILED DWD_READY_REDIS_CREATE_FAILED DWD_READY_REDIS_CREDENTIAL_FAILED
    DWD_READY_REDIS_PSM_FAILED DWD_READY_REDIS_TLS_CLIENT_UNSUPPORTED DWD_READY_REDIS_INSPECT_FAILED
    DWD_READY_REDIS_AUTH_FAILED DWD_READY_REDIS_CONNECT_FAILED DWD_READY_REDIS_SESSION_FAILED
    LOGGING_SETTING_INVALID TIME_REQUIRES_ISO8601_WITH_TIMEZONE STATE_DEPLOYMENT_BINDING_CHANGED
    STATE_START_CANNOT_BE_CHANGED START_TIME_MUST_BE_A_FIVE_MINUTE_BOUNDARY
    STATE_DEPLOYMENT_BINDING_CHANGED_OR_UNINITIALIZED WINDOW_PRECEDES_COVERAGE_START
    SCHEDULER_SETTING_INVALID STARTUP_SETTING_INVALID TASK_TIMEOUT_OUT_OF_RANGE TASK_PARALLELISM_OUT_OF_RANGE SKIP_MODE_INVALID
    CROSS_PERIOD_WAIT_PERIOD_INVALID CROSS_PERIOD_WAIT_TASK_UNKNOWN REDIS_POOL_CONFIG_INVALID REDIS_STATE_CONFIG_INVALID
    REDIS_STATE_FENCED REDIS_STATE_WRITE_FAILED REDIS_STATE_READ_FAILED
    CLICKHOUSE_POOL_CONFIG_INVALID CLICKHOUSE_POOL_EXHAUSTED
    PREAGGREGATE_FORMAL_WINDOW_NOT_ENDED
    CONFIG_PATH_INVALID CONFIG_PATH_ENV_MISSING NO_TASKS_CONFIGURED DWD_READY_REQUIRES_FIVE_MINUTE_ROOT_TASKS
    STATE_DATABASE_INVALID STATE_DATABASE_MISSING STATE_DIRECTORY_NOT_EMPTY STATE_NOT_INITIALIZED
    LEADERSHIP_BUSY LEADERSHIP_LOST LEADERSHIP_REDIS_FAILED LEADERSHIP_LEDGER_MISMATCH
    LEADERSHIP_REGISTRY_MISSING LEADERSHIP_CLUSTER_CHANGED LEADERSHIP_REQUIRED_FOR_LEDGER
    LEADERSHIP_CONFIG_INVALID LEADERSHIP_REQUIRES_STANDALONE_OR_SENTINEL
    LEADERSHIP_ALREADY_ACQUIRED LEADERSHIP_SETTINGS_INVALID LEADERSHIP_RENEWAL_START_FAILED
    DATASET_ID_INVALID CONFIGURATION_MANIFEST_INVALID CONFIGURATION_SQL_UNREADABLE
    CONFIG_MIGRATION_REQUIRED CONFIG_PREVIOUS_CONFIG_REQUIRED CONFIG_PREVIOUS_BINDING_MISMATCH
    CONFIG_PREVIOUS_RESOURCES_REQUIRED CONFIG_CONFIRMATION_REQUIRED CONFIG_DATASET_CHANGED
    CONFIG_TASKS_UNRESOLVED CONFIG_PLAN_STALE CONFIG_MIGRATION_BLOCKED CONFIG_EFFECTIVE_FROM_REQUIRED
    CONFIG_EFFECTIVE_FROM_INVALID CONFIG_EFFECTIVE_FROM_PAST CONFIG_STATE_INVALID CONFIGURATION_STATE_CHANGED
    TASK_WINDOW_BEFORE_EFFECTIVE_START CONFIG_COORDINATION_CHANGED CONFIG_RUNNING_INSTANCES
    CONFIG_EFFECTIVE_BEFORE_COVERAGE CONFIG_TASK_WINDOW_OVERLAP CONFIGURATION_TRANSACTION_ACTIVE
    CONFIGURATION_BACKUP_INVALID
    HISTORICAL_INSTANCE_RETIRED LEGACY_RETIREMENT_INVALID LEGACY_RECOVERY_INVALID
    LEGACY_RECOVERY_CONFIGURATION_CHANGED
    ARCHIVE_SETTINGS_INVALID ARCHIVE_FILE_INVALID ARCHIVE_CONTENT_INVALID ARCHIVE_FILE_CHANGED
    ARCHIVE_CANCELLED ARCHIVE_TIMEOUT ARCHIVE_DIRECTORY_INVALID ARCHIVE_BUSY ARCHIVE_FAILED
    ARCHIVE_STATUS_INVALID ARCHIVE_STATUS_WRITE_FAILED
    ARCHIVE_TIMESTAMP_INVALID ARCHIVE_LOCK_INVALID ARCHIVE_MAINTENANCE_FAILED
""".split())
_PREAGGREGATE_CODES = frozenset("""
    PREAGGREGATE_COVERAGE_TASK_CHANGED PREAGGREGATE_COVERAGE_DATASET_CHANGED
    PREAGGREGATE_COVERAGE_TIMEZONE_CHANGED PREAGGREGATE_COVERAGE_CLEANUP_CHANGED
    PREAGGREGATE_COVERAGE_START_CHANGED PREAGGREGATE_COVERAGE_EFFECTIVE_CHANGED
    PREAGGREGATE_COVERAGE_RETIRED PREAGGREGATE_COVERAGE_CURSOR_PASSED
    PREAGGREGATE_NOT_UNKNOWN PREAGGREGATE_UNRESOLVED
""".split())
_TEXT_FIELDS = {"error_code": r"[A-Z][A-Z0-9_]{0,127}",
                "error_type": r"[A-Za-z_][A-Za-z0-9_]{0,63}",
                "query_id": r"[A-Za-z0-9_.:-]{1,128}"}
_NUMBER_FIELDS = {"http_status": (100, 599), "clickhouse_exception_code": (0, 2**31 - 1),
                  "tls_verify_code": (0, 2**31 - 1), "os_errno": (-2**31, 2**31 - 1)}


def safe_diagnostic_details(details: dict | None) -> dict:
    """Copy only bounded identifiers and numeric evidence; discard all other data."""
    if not isinstance(details, dict):
        return {}
    safe = {}
    for name, pattern in _TEXT_FIELDS.items():
        value = details.get(name)
        if isinstance(value, str) and re.fullmatch(pattern, value):
            safe[name] = value
    for name, (minimum, maximum) in _NUMBER_FIELDS.items():
        value = details.get(name)
        if type(value) is int and minimum <= value <= maximum:
            safe[name] = value
    return safe


def _http_error_details(error: urllib.error.HTTPError) -> dict:
    details = {"http_status": error.code}
    header = error.headers.get("X-ClickHouse-Exception-Code") if error.headers is not None else None
    if isinstance(header, str) and re.fullmatch(r"[0-9]{1,10}", header):
        details["clickhouse_exception_code"] = int(header)
    return safe_diagnostic_details(details)


def _public_error_code(exc):
    if len(exc.args) != 1 or not isinstance(exc.args[0], str):
        return None
    message = exc.args[0]
    if message in _PUBLIC_CODES:
        return message
    code, _, identifier = message.partition(':')
    if code in _PREAGGREGATE_CODES and re.fullmatch(r'[A-Za-z0-9_.:-]{1,128}', identifier):
        return code
    return None


def safe_error_details(exc: BaseException) -> dict:
    """Describe failures without generic exception messages, SQL or credentials."""
    reason = exc.reason if isinstance(exc, urllib.error.URLError) else exc
    reason = reason if isinstance(reason, BaseException) else exc
    details = {"error_type": type(reason).__name__}
    if isinstance(reason, urllib.error.HTTPError):
        details.update(_http_error_details(reason))
    if isinstance(reason, OSError):
        details["os_errno"] = reason.errno
    if isinstance(reason, ssl.SSLCertVerificationError):
        details["tls_verify_code"] = getattr(reason, "verify_code", None)
    if isinstance(exc, TaskExecutionError):
        details.update(safe_diagnostic_details(exc.diagnostic))
        details.update(error_code=str(exc), query_id=exc.query_id)
    elif code := _public_error_code(exc):
        details["error_code"] = code
    return safe_diagnostic_details(details)


class TaskExecutionError(RuntimeError):
    """Safe diagnostic and execution evidence, independent of a transport.

    An unknown outcome always prevents automatic retry, even when retryable
    is also set. Runners must not include credentials or remote SQL in message.
    """

    def __init__(self, message: str, *, retryable: bool = False,
                 outcome_unknown: bool = False, query_id: str | None = None,
                 diagnostic: dict | None = None):
        super().__init__(message)
        self.retryable = retryable
        self.outcome_unknown = outcome_unknown
        self.query_id = query_id
        self.diagnostic = safe_diagnostic_details(diagnostic)
