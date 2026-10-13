"""Submit ClickHouse queries and classify outcomes without exposing private data."""

from __future__ import annotations

import base64
import errno
import http.client
import json
import math
import re
import socket
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from data_converge.errors import TaskExecutionError

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
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    value = value.strip()
    if not value:
        raise ValueError("时间参数不能为空")
    return value


def _build_sql_request(clickhouse, origin, sql, timeout, start_time, end_time, query_id):
    """Keep credentials in headers and preserve ClickHouse's SQL placeholders."""
    parameters = dict(database=clickhouse["database"], default_format="JSONCompact",
                      wait_end_of_query="1", max_execution_time=str(timeout), query_id=query_id)
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
                end_time: datetime | str | None = None, *, query_id: str | None = None) -> list:
    """POST one query; ambiguous failures are never eligible for automatic retry."""
    if (start_time is None) != (end_time is None):
        raise ValueError("start_time 和 end_time 必须同时提供")
    if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("CLICKHOUSE_TIMEOUT_INVALID") from None
    deadline = QueryDeadline(timeout)
    clickhouse = config["clickhouse"]
    origin, _, secure, verify = connection_parameters(clickhouse)
    if query_id is None:
        query_id = uuid.uuid4().hex
    if not isinstance(query_id, str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", query_id):
        raise ValueError("CLICKHOUSE_QUERY_ID_INVALID") from None
    request = _build_sql_request(clickhouse, origin, sql, timeout, start_time, end_time, query_id)
    handlers = build_query_handlers(deadline, secure, verify)
    response_started = False
    try:
        with deadline:
            with urllib.request.build_opener(*handlers).open(request, timeout=timeout) as response:
                response_started = True
                body = response.read(MAX_RESPONSE_BYTES + 1)
                # HTTPResponse.read(n) can silently return short data on disconnect.
                # Its remaining Content-Length must reach zero before accepting success.
                remaining = getattr(response, "length", None)
    except urllib.error.HTTPError as error:
        code = error.code
        error.close()
        message = "CLICKHOUSE_OUTCOME_UNKNOWN" if deadline.expired else (
            "CLICKHOUSE_AUTH_FAILED" if code == 401 else "CLICKHOUSE_HTTP_FAILED")
        raise SQLExecutionError(message, outcome_unknown=deadline.expired or code != 401,
                                query_id=query_id) from None
    except (OSError, http.client.HTTPException, ValueError) as error:
        reason = error.reason if isinstance(error, urllib.error.URLError) else error
        unsent = not deadline.expired and not response_started and (
            isinstance(reason, (ConnectionRefusedError, socket.gaierror)) or (
                isinstance(reason, OSError) and reason.errno == errno.ECONNREFUSED))
        raise SQLExecutionError("CLICKHOUSE_CONNECT_FAILED" if unsent else "CLICKHOUSE_OUTCOME_UNKNOWN",
                                retryable=unsent, outcome_unknown=not unsent, query_id=query_id) from None
    if deadline.expired:
        raise SQLExecutionError("CLICKHOUSE_OUTCOME_UNKNOWN", outcome_unknown=True, query_id=query_id) from None
    return _decode_query_response(body, remaining, query_id)


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
