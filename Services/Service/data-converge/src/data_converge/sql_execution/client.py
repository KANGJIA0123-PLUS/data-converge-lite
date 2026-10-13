"""Small, direct ClickHouse HTTP transport and startup configuration loader."""

from __future__ import annotations

import base64
import errno
import http.client
import json
import math
import os
import re
import socket
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from copy import deepcopy
from datetime import datetime
from functools import partial
from pathlib import Path
from typing import Any

import yaml

from data_converge.errors import TaskExecutionError

from .credentials import load_password, validate_provider

MAX_RESPONSE_BYTES = 4 * 1024 * 1024


class SQLExecutionError(TaskExecutionError):
    """Safe diagnostic plus the evidence needed for retry/recovery decisions."""


def _expand(value):
    if not isinstance(value, str):
        return value

    def replace(match):
        result = os.environ.get(match.group(1))
        if not result:
            raise ValueError("CLICKHOUSE_ENV_MISSING") from None
        return result

    value = re.sub(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}", replace, value)
    if "$" in value:
        raise ValueError("CLICKHOUSE_ENV_INVALID") from None
    return value


def _connection(clickhouse):
    """Validate the authority before assembling a URL with private headers."""
    try:
        host = clickhouse["host"]
        port = clickhouse["port"]
        if not isinstance(host, str) or not host or any(c.isspace() or ord(c) < 32 for c in host):
            raise ValueError
        if type(port) is not int:
            if not isinstance(port, str) or not re.fullmatch(r"[0-9]+", port):
                raise ValueError
            port = int(port)
        if not 1 <= port <= 65535 or any(char in host for char in "?#\\%"):
            raise ValueError
        has_scheme = "://" in host
        parsed = urllib.parse.urlsplit(host if has_scheme else "//" + host)
        secure = clickhouse.get("secure", parsed.scheme == "https" if has_scheme else False)
        verify = clickhouse.get("verify", True)
        if type(secure) is not bool or type(verify) is not bool:
            raise ValueError
        scheme = "https" if secure else "http"
        if has_scheme and parsed.scheme != scheme:
            raise ValueError
        if (not parsed.hostname or parsed.username is not None or parsed.password is not None
                or parsed.path not in ("", "/") or parsed.query or parsed.fragment
                or parsed.port not in (None, port)):
            raise ValueError
        hostname = parsed.hostname
        if not re.fullmatch(r"[A-Za-z0-9_.:-]+", hostname):
            raise ValueError
        authority = f"[{hostname}]" if ":" in hostname else hostname
        return f"{scheme}://{authority}:{port}/", port, secure, verify
    except (ValueError, KeyError, TypeError):
        raise ValueError("CLICKHOUSE_CONNECTION_INVALID") from None


def connection_origin(clickhouse: dict[str, Any]) -> str:
    """Return the validated endpoint used for execution and deployment binding."""
    return _connection(clickhouse)[0]


def load_config(config_path: Path, *, resolve_password: bool = True) -> dict[str, Any]:
    """Read and validate a configuration, optionally resolving its credential."""
    config_path = Path(config_path)
    try:
        with config_path.open("r", encoding="utf-8") as stream:
            config = yaml.safe_load(stream)
    except (OSError, UnicodeError, yaml.YAMLError):
        raise ValueError("CONFIG_UNREADABLE") from None
    return validate_config(config, config_path, resolve_password=resolve_password)


def validate_config(config: dict[str, Any], config_path: Path, *,
                    resolve_password: bool = True) -> dict[str, Any]:
    """Validate an independent snapshot without rereading the configuration file."""
    if not isinstance(config, dict) or not isinstance(config.get("clickhouse"), dict):
        raise ValueError("CLICKHOUSE_CONFIG_MISSING") from None
    config = deepcopy(config)
    clickhouse = config["clickhouse"]
    for field in ("host", "port"):
        clickhouse[field] = _expand(clickhouse.get(field))
    _, clickhouse["port"], clickhouse["secure"], clickhouse["verify"] = _connection(clickhouse)
    for field in ("database", "username"):
        value = clickhouse.get(field)
        if not isinstance(value, str) or not value.strip() or any(ord(c) < 32 for c in value):
            raise ValueError("CLICKHOUSE_IDENTITY_INVALID") from None
    if ":" in clickhouse["username"]:
        raise ValueError("CLICKHOUSE_IDENTITY_INVALID") from None
    validate_provider(clickhouse, Path(config_path).parent)
    return resolve_credentials(config) if resolve_password else config


def resolve_credentials(config: dict[str, Any]) -> dict[str, Any]:
    """Resolve a validated snapshot's password without modifying the snapshot."""
    resolved = deepcopy(config)
    clickhouse = resolved["clickhouse"]
    if "password" not in clickhouse:
        clickhouse["password"] = load_password(clickhouse)
    return resolved


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


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        # Never forward private authentication headers to a redirected endpoint.
        return None


class _QueryDeadline:
    """Cancel socket I/O at an absolute deadline, including HTTP/TLS headers.

    A blocking platform getaddrinfo call cannot be forcibly cancelled. Once it
    returns, an expired deadline prevents any new connection or SQL submission.
    """

    def __init__(self, timeout):
        self.expires_at = time.monotonic() + timeout
        self._lock = threading.Lock()
        self._expired = threading.Event()
        self._socket = None
        self._watchdog = None

    @property
    def expired(self):
        return self._expired.is_set() or time.monotonic() >= self.expires_at

    def remaining(self):
        remaining = self.expires_at - time.monotonic()
        if self._expired.is_set() or remaining <= 0:
            raise TimeoutError("query deadline exceeded")
        return remaining

    @staticmethod
    def _close(sock, *, interrupt=False):
        if sock is None:
            return
        if interrupt:
            try:
                # close() alone does not interrupt sockets retained by makefile().
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        sock.close()

    def _expire(self):
        with self._lock:
            self._expired.set()
            sock = self._socket
        self._close(sock, interrupt=True)

    def __enter__(self):
        self._watchdog = threading.Timer(max(0, self.expires_at - time.monotonic()), self._expire)
        self._watchdog.name = "clickhouse-query-deadline"
        self._watchdog.daemon = True
        self._watchdog.start()
        return self

    def __exit__(self, *_):
        self._watchdog.cancel()
        self._watchdog.join()
        self._close(self._socket)
        self._socket = None

    def connect(self, address, timeout=None, source_address=None):
        # Retain the actual socket before connect so the watchdog can interrupt
        # connection attempts as well as reads after urllib relinquishes ownership.
        host, port = address
        addresses = socket.getaddrinfo(host, port, 0, socket.SOCK_STREAM)
        last_error = None
        for family, kind, protocol, _, target in addresses:
            self.remaining()
            sock = socket.socket(family, kind, protocol)
            try:
                with self._lock:
                    self.remaining()
                    self._socket = sock
                sock.settimeout(self.remaining())
                if source_address:
                    sock.bind(source_address)
                sock.connect(target)
                return sock
            except OSError as error:
                sock.close()
                self.remaining()
                last_error = error
        if last_error is not None:
            raise last_error
        raise OSError("no connection address")

    def wrap_tls(self, sock, context, hostname):
        # Avoid a gap where wrap_socket detaches the tracked raw socket while the
        # TLS handshake blocks. Register the TLS socket before starting handshake.
        with self._lock:
            self.remaining()
            wrapped = context.wrap_socket(sock, server_hostname=hostname, do_handshake_on_connect=False)
            self._socket = wrapped
            return wrapped


class _DeadlineConnection:
    def __init__(self, *args, deadline, **kwargs):
        super().__init__(*args, **kwargs)
        self._deadline = deadline
        self._create_connection = deadline.connect


class _DeadlineHTTPConnection(_DeadlineConnection, http.client.HTTPConnection):
    pass


class _DeadlineHTTPSConnection(_DeadlineConnection, http.client.HTTPSConnection):
    def connect(self):
        http.client.HTTPConnection.connect(self)
        self.sock = self._deadline.wrap_tls(self.sock, self._context, self.host)
        self.sock.do_handshake()


class _DeadlineHTTPHandler(urllib.request.HTTPHandler):
    def __init__(self, deadline):
        super().__init__()
        self._deadline = deadline

    def http_open(self, request):
        return self.do_open(partial(_DeadlineHTTPConnection, deadline=self._deadline), request)


class _DeadlineHTTPSHandler(urllib.request.HTTPSHandler):
    def __init__(self, deadline, context):
        super().__init__(context=context)
        self._deadline = deadline

    def https_open(self, request):
        return self.do_open(partial(_DeadlineHTTPSConnection, deadline=self._deadline), request,
                            context=self._context)


def execute_sql(config: dict[str, Any], sql: str, timeout: int = 300,
                start_time: datetime | str | None = None,
                end_time: datetime | str | None = None, *, query_id: str | None = None) -> list:
    """POST one query; ambiguous failures are never eligible for automatic retry."""
    if (start_time is None) != (end_time is None):
        raise ValueError("start_time 和 end_time 必须同时提供")
    if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("CLICKHOUSE_TIMEOUT_INVALID") from None
    deadline = _QueryDeadline(timeout)
    clickhouse = config["clickhouse"]
    origin, _, secure, verify = _connection(clickhouse)
    if query_id is None:
        query_id = uuid.uuid4().hex
    if not isinstance(query_id, str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", query_id):
        raise ValueError("CLICKHOUSE_QUERY_ID_INVALID") from None
    parameters = dict(database=clickhouse["database"], default_format="JSONCompact",
                      wait_end_of_query="1", max_execution_time=str(timeout), query_id=query_id)
    if start_time is not None:
        parameters.update(param_start_time=format_clickhouse_datetime(start_time),
                          param_end_time=format_clickhouse_datetime(end_time))
    password = clickhouse.get("password")
    if not isinstance(password, str) or not password:
        raise ValueError("CLICKHOUSE_CREDENTIAL_UNRESOLVED") from None
    token = base64.b64encode(f'{clickhouse["username"]}:{password}'.encode("utf-8")).decode("ascii")
    request = urllib.request.Request(
        origin + "?" + urllib.parse.urlencode(parameters), data=sql.encode("utf-8"), method="POST",
        headers={"Authorization": "Basic " + token, "Content-Type": "text/plain; charset=utf-8"})
    handlers = [urllib.request.ProxyHandler({}), _NoRedirect(), _DeadlineHTTPHandler(deadline)]
    if secure:
        context = ssl.create_default_context()
        if not verify:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
        handlers.append(_DeadlineHTTPSHandler(deadline, context))
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
    except (urllib.error.URLError, OSError, http.client.HTTPException, ValueError) as error:
        reason = error.reason if isinstance(error, urllib.error.URLError) else error
        unsent = not deadline.expired and not response_started and (
            isinstance(reason, (ConnectionRefusedError, socket.gaierror)) or (
                isinstance(reason, OSError) and reason.errno == errno.ECONNREFUSED))
        raise SQLExecutionError("CLICKHOUSE_CONNECT_FAILED" if unsent else "CLICKHOUSE_OUTCOME_UNKNOWN",
                                retryable=unsent, outcome_unknown=not unsent, query_id=query_id) from None
    if deadline.expired:
        raise SQLExecutionError("CLICKHOUSE_OUTCOME_UNKNOWN", outcome_unknown=True, query_id=query_id) from None
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
    except (ValueError, UnicodeError, RecursionError):
        raise SQLExecutionError("CLICKHOUSE_RESPONSE_INVALID", outcome_unknown=True, query_id=query_id) from None
