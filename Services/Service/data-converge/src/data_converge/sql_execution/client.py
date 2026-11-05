"""Submit ClickHouse queries and classify outcomes without exposing private data."""

from __future__ import annotations

import base64
import errno
import http.client
import json
import math
import re
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from data_converge.errors import TaskExecutionError, safe_error_details
from data_converge.time_policy import system_timezone, system_timezone_name

from . import debug
# Keep the established client configuration API for operational commands.
from .config import (
    connection_origin,
    connection_parameters,
    load_config,
    resolve_credentials,
    validate_config,
)
from .transport import QueryDeadline, build_query_handlers

MAX_RESPONSE_BYTES = 4 * 1024 * 1024


class SQLExecutionError(TaskExecutionError):
    """Safe diagnostic plus the evidence needed for retry/recovery decisions."""


def load_sql(sql_file: Path) -> str:
    """Read a nonempty SQL file without expanding ClickHouse placeholders."""
    if not sql_file.exists():
        raise FileNotFoundError(f"SQL 文件不存在：{sql_file}")
    sql = sql_file.read_text(encoding="utf-8").strip()
    if not sql:
        raise ValueError(f"SQL 文件内容为空：{sql_file}")
    return sql


def format_clickhouse_datetime(value: datetime | str) -> str:
    """Encode an absolute Unix second for the SQL UInt32 window parameters."""
    if isinstance(value, str):
        value = value.strip()
        if not value:
            raise ValueError("时间参数不能为空")
        value = datetime.fromisoformat(value)
    if value.tzinfo is None:
        value = value.replace(tzinfo=system_timezone())
    return str(int(value.timestamp()))


def _build_sql_request(clickhouse, origin, sql, timeout, start_time, end_time, query_id,
                       settings=None):
    """Keep credentials in headers and preserve ClickHouse's SQL placeholders."""
    parameters = dict(settings or {})
    parameters.update(database=clickhouse["database"], default_format="JSONCompact",
                      wait_end_of_query="1", max_execution_time=str(max(1, math.ceil(timeout))),
                      query_id=query_id,
                      param_system_timezone=system_timezone_name())
    if start_time is not None:
        parameters.update(param_start_time=format_clickhouse_datetime(start_time),
                          param_end_time=format_clickhouse_datetime(end_time))
    password = clickhouse.get("password")
    if not isinstance(password, str) or not password:
        raise ValueError("CLICKHOUSE_CREDENTIAL_UNRESOLVED") from None
    token = base64.b64encode(f'{clickhouse["username"]}:{password}'.encode("utf-8")).decode("ascii")
    return urllib.request.Request(
        origin + "?" + urllib.parse.urlencode(parameters), data=sql.encode("utf-8"), method="POST",
        headers={"Authorization": "Basic " + token, "Content-Type": "text/plain; charset=utf-8"})


def execute_sql(config: dict[str, Any], sql: str, timeout: int = 300,
                start_time: datetime | str | None = None,
                end_time: datetime | str | None = None, *, query_id: str | None = None,
                pool=None) -> list:
    """POST one query; ambiguous failures are never eligible for automatic retry.

    With ``pool`` the query waits for an aggregation slot (within its budget),
    uses the pool's connect timeout and sends its ClickHouse settings.
    """
    if pool is None:
        return _execute(config, sql, timeout, start_time, end_time, query_id, None, None)
    started = time.monotonic()
    try:
        with pool.slot(timeout):
            remaining = max(1.0, timeout - (time.monotonic() - started))
            return _execute(config, sql, remaining, start_time, end_time, query_id,
                            pool.query_parameters(), pool.connect_timeout_seconds)
    except TimeoutError:
        # Nothing was sent: safe to retry later.
        raise SQLExecutionError("CLICKHOUSE_POOL_EXHAUSTED", retryable=True,
                                query_id=query_id) from None


def _execute(config, sql, timeout, start_time, end_time, query_id, settings, connect_timeout):
    if (start_time is None) != (end_time is None):
        raise ValueError("start_time 和 end_time 必须同时提供")
    if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("CLICKHOUSE_TIMEOUT_INVALID") from None
    deadline = QueryDeadline(timeout, connect_timeout=connect_timeout)
    clickhouse = config["clickhouse"]
    origin, _, secure, verify = connection_parameters(clickhouse)
    if query_id is None:
        query_id = uuid.uuid4().hex
    if not isinstance(query_id, str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", query_id):
        raise ValueError("CLICKHOUSE_QUERY_ID_INVALID") from None
    request = _build_sql_request(clickhouse, origin, sql, timeout, start_time, end_time, query_id,
                                 settings)
    handlers = build_query_handlers(deadline, secure, verify)
    try:
        body, remaining = _post_sql_request(config, request, handlers, deadline, timeout, query_id)
        if deadline.expired:
            error = SQLExecutionError("CLICKHOUSE_OUTCOME_UNKNOWN", outcome_unknown=True, query_id=query_id)
            debug.log_error(config, query_id, error)
            raise error from None
        try:
            return _decode_query_response(body, remaining, query_id)
        except SQLExecutionError as error:
            debug.log_error(config, query_id, error,
                            body=body if str(error) == "CLICKHOUSE_RESPONSE_INVALID" else None)
            raise
    except SQLExecutionError:
        # Success stays quiet; render only the failed operation's bound SQL.
        debug.log_request(config, request)
        raise


def _post_sql_request(config, request, handlers, deadline, timeout, query_id):
    """Capture optional error evidence while the response deadline is active."""
    response_started = False
    with deadline:
        try:
            with urllib.request.build_opener(*handlers).open(request, timeout=timeout) as response:
                response_started = True
                body = response.read(MAX_RESPONSE_BYTES + 1)
                # HTTPResponse.read(n) can silently return short data on disconnect.
                # Its remaining Content-Length must reach zero before accepting success.
                remaining = getattr(response, "length", None)
        except urllib.error.HTTPError as error:
            raise _http_failure(config, query_id, error, deadline) from None
        except (OSError, http.client.HTTPException, ValueError) as error:
            reason = error.reason if isinstance(error, urllib.error.URLError) else error
            unsent = not deadline.expired and not response_started and (
                isinstance(reason, (ConnectionRefusedError, socket.gaierror)) or (
                    isinstance(reason, OSError) and reason.errno == errno.ECONNREFUSED))
            debug.log_error(config, query_id, error)
            raise SQLExecutionError("CLICKHOUSE_CONNECT_FAILED" if unsent else "CLICKHOUSE_OUTCOME_UNKNOWN",
                                    retryable=unsent, outcome_unknown=not unsent, query_id=query_id,
                                    diagnostic=safe_error_details(error)) from None
    return body, remaining


def _http_failure(config, query_id, error, deadline):
    # Freeze classification before optional diagnostic I/O: a slow error body
    # must not turn a definite 401 rejection into an unknown execution outcome.
    expired, code = deadline.expired, error.code
    diagnostic = safe_error_details(error)
    try:
        debug.log_http_error(config, query_id, error)
    finally:
        error.close()
    message = "CLICKHOUSE_OUTCOME_UNKNOWN" if expired else (
        "CLICKHOUSE_AUTH_FAILED" if code == 401 else "CLICKHOUSE_HTTP_FAILED")
    return SQLExecutionError(message, outcome_unknown=expired or code != 401,
                             query_id=query_id, diagnostic=diagnostic)


def _decode_query_response(body, remaining, query_id):
    """Accept only complete, bounded success responses and redact remote errors."""
    if len(body) > MAX_RESPONSE_BYTES:
        raise SQLExecutionError("CLICKHOUSE_RESPONSE_LIMIT", outcome_unknown=True, query_id=query_id) from None
    if remaining not in (None, 0):
        raise SQLExecutionError("CLICKHOUSE_RESPONSE_INCOMPLETE", outcome_unknown=True, query_id=query_id) from None
    if not body.strip():
        return []
    try:
        document = json.loads(body)
        if not isinstance(document, dict) or "exception" in document or not isinstance(document.get("data"), list):
            raise ValueError
        return document["data"]
    except (ValueError, RecursionError):
        raise SQLExecutionError("CLICKHOUSE_RESPONSE_INVALID", outcome_unknown=True, query_id=query_id) from None


NOTHING_TO_KILL = 785


def _query_id_list(query_ids) -> list[str]:
    return [value for value in query_ids if isinstance(value, str)
            and re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", value)]


def kill_queries(config: dict[str, Any], query_ids, timeout: float = 30) -> None:
    """Ask ClickHouse to cancel queries left by a previous leader (best effort)."""
    ids = _query_id_list(query_ids)
    if not ids:
        return
    quoted = ", ".join("'" + value + "'" for value in ids)
    execute_sql(config, f"KILL QUERY WHERE query_id IN ({quoted}) ASYNC", timeout=timeout)


def stop_queries(config: dict[str, Any], query_ids, timeout: float = 30) -> set[str]:
    """Cancel queries synchronously and return the ids ClickHouse still reports running.

    An empty result is positive evidence that none of the queries can still write.
    Any transport or server failure raises: callers must treat that as unconfirmed.
    """
    ids = _query_id_list(query_ids)
    if not ids:
        return set()
    quoted = ", ".join("'" + value + "'" for value in ids)
    # Filtering system.processes first avoids an expected NOTHING_TO_KILL error
    # (logged as SQL_ERROR) on every confirmation round after the query ended.
    sql = f"SELECT query_id FROM system.processes WHERE query_id IN ({quoted})"
    running = {row[0] for row in execute_sql(config, sql, timeout=timeout) if row}
    if not running:
        return set()
    try:
        execute_sql(config, f"KILL QUERY WHERE query_id IN ({quoted}) SYNC", timeout=timeout)
    except SQLExecutionError as error:
        # 785: the queries ended between the two statements; the check below confirms it.
        if (error.diagnostic or {}).get("clickhouse_exception_code") != NOTHING_TO_KILL:
            raise
    return {row[0] for row in execute_sql(config, sql, timeout=timeout) if row}
