"""HTTP transport boundaries; never contact a ClickHouse server."""

import base64
import errno
import http.client
import io
import json
import socket
import socketserver
import ssl
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
import yaml

from data_converge.sql_execution import client as client_module
from data_converge.sql_execution import runner as runner_module
from data_converge.errors import TaskExecutionError

WINDOW_SQL = "SELECT toDateTime({start_time:UInt32}, 'UTC'), toDateTime({end_time:UInt32}, 'UTC')"


def make_clickhouse_config(**values):
    config = dict(host="127.0.0.1", port=8123, database="metrics", username="test_user",
                  password="private test password", secure=False)
    config.update(values)
    return {"clickhouse": config}


def capture_http(monkeypatch, body=b"", error=None):
    calls = []

    class Opener:
        def open(self, request, timeout):
            calls.append((request, timeout))
            if error is not None:
                raise error
            return io.BytesIO(body)

    handlers = []
    def build_opener(*items):
        handlers.extend(items)
        return Opener()

    monkeypatch.setattr(urllib.request, "build_opener", build_opener)
    # Ensure the old kubectl path fails without launching anything during RED.
    if hasattr(client_module, "subprocess"):
        monkeypatch.setattr(client_module.subprocess, "run",
                            lambda *a, **k: pytest.fail("legacy subprocess transport"))
    return calls, handlers


def test_execute_sql_posts_original_sql_and_parameterized_window(monkeypatch):
    calls, handlers = capture_http(monkeypatch, b'{"data":[[1]]}')
    sql = WINDOW_SQL
    rows = client_module.execute_sql(
        make_clickhouse_config(), sql, timeout=123,
        start_time=datetime(2026, 9, 9, 1, tzinfo=timezone.utc),
        end_time=datetime(2026, 9, 9, 2, tzinfo=timezone.utc))
    request, timeout = calls[0]
    params = urllib.parse.parse_qs(urllib.parse.urlsplit(request.full_url).query)
    assert request.get_method() == "POST"
    assert request.data == sql.encode()
    assert params["param_start_time"] == ["1788915600"]
    assert params["param_end_time"] == ["1788919200"]
    assert params["database"] == ["metrics"]
    assert params["default_format"] == ["JSONCompact"]
    assert params["wait_end_of_query"] == ["1"]
    assert params["max_execution_time"] == ["123"]
    assert len(params["query_id"][0]) >= 32
    assert "private" not in request.full_url
    assert base64.b64decode(request.get_header("Authorization").split()[1]) == b"test_user:private test password"
    assert timeout == 123 and rows == [[1]]
    assert any(isinstance(h, urllib.request.ProxyHandler) and not h.proxies for h in handlers)
    redirects = [h for h in handlers if isinstance(h, urllib.request.HTTPRedirectHandler)]
    assert redirects and redirects[0].redirect_request(request, None, 307, "redirect", {}, "https://elsewhere/") is None


@pytest.mark.parametrize("verify, expected", [(True, ssl.CERT_REQUIRED), (False, ssl.CERT_NONE)])
def test_execute_sql_honors_tls_verification(monkeypatch, verify, expected):
    calls, handlers = capture_http(monkeypatch)
    assert client_module.execute_sql(make_clickhouse_config(host="https://db.example", port=8443,
                                                           secure=True, verify=verify), "SELECT 1") == []
    assert calls[0][0].full_url.startswith("https://db.example:8443/?")
    tls = next(h for h in handlers if isinstance(h, urllib.request.HTTPSHandler))
    assert tls._context.verify_mode == expected


@pytest.mark.parametrize("error, retryable, unknown", [
    (urllib.error.URLError(ConnectionRefusedError(errno.ECONNREFUSED, "private credential")), True, False),
    (urllib.error.URLError(socket.gaierror(socket.EAI_NONAME, "private credential")), True, False),
    (urllib.error.URLError(TimeoutError("private credential")), False, True),
    (ConnectionResetError("private credential"), False, True),
    (http.client.RemoteDisconnected("private credential"), False, True),
    (http.client.IncompleteRead(b"private credential"), False, True),
    (urllib.error.HTTPError("secret url", 400, "private credential", {}, io.BytesIO(b"SQL password")), False, True),
    (urllib.error.HTTPError("secret url", 500, "private credential", {}, io.BytesIO(b"SQL password")), False, True),
    (urllib.error.HTTPError("secret url", 401, "private credential", {}, io.BytesIO(b"SQL password")), False, False),
])
def test_failure_classification_prevents_unsafe_retry_and_hides_remote_details(monkeypatch, error, retryable, unknown):
    capture_http(monkeypatch, error=error)
    with pytest.raises(RuntimeError) as caught:
        client_module.execute_sql(make_clickhouse_config(), "INSERT sensitive SQL")
    assert caught.value.retryable is retryable
    assert caught.value.outcome_unknown is unknown
    assert all(secret not in str(caught.value) for secret in ("private", "secret url", "sensitive", "password"))
    assert caught.value.__suppress_context__


@pytest.mark.parametrize("body", [b"Code: SQL private password", b'{"exception":"private password"}', b'{"data":null}'])
def test_invalid_success_response_has_unknown_outcome(monkeypatch, body):
    capture_http(monkeypatch, body=body)
    with pytest.raises(RuntimeError) as caught:
        client_module.execute_sql(make_clickhouse_config(), "INSERT INTO source")
    assert caught.value.outcome_unknown and not caught.value.retryable
    assert "private" not in str(caught.value)


def test_execute_sql_requires_complete_data_window():
    with pytest.raises(ValueError, match="start_time 和 end_time 必须同时提供"):
        client_module.execute_sql(make_clickhouse_config(), "SELECT 1", start_time=datetime(2026, 9, 9))


def test_validate_config_normalizes_an_independent_snapshot(tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_CH_HOST", "127.0.0.1")
    raw = {"clickhouse": dict(host="${TEST_CH_HOST}", port="8123", database="metrics",
                              username="user", password_env="TEST_CH_PASSWORD"),
           "scheduler": {"max_concurrent_tasks": 4}}
    config = client_module.validate_config(raw, tmp_path / "app.yaml", resolve_password=False)
    assert raw["clickhouse"]["host"] == "${TEST_CH_HOST}"
    assert raw["clickhouse"]["port"] == "8123"
    assert "password" not in config["clickhouse"]
    raw["clickhouse"]["host"] = "changed.example"
    raw["scheduler"]["max_concurrent_tasks"] = 99
    assert client_module.connection_origin(config["clickhouse"]) == "http://127.0.0.1:8123/"
    assert config["scheduler"]["max_concurrent_tasks"] == 4


def test_runner_uses_snapshot_without_rereading_or_mutating_it(tmp_path, monkeypatch):
    path = tmp_path / "app.yaml"
    monkeypatch.setenv("TEST_CH_HOST", "127.0.0.1")
    monkeypatch.setenv("TEST_CH_PASSWORD", "first password")
    config = client_module.validate_config({"clickhouse": dict(
        host="${TEST_CH_HOST}", port=8123, database="metrics", username="user",
        password_env="TEST_CH_PASSWORD")}, path, resolve_password=False)
    path.write_text("invalid replacement configuration")
    monkeypatch.setenv("TEST_CH_HOST", "replacement.example")
    (tmp_path / "hour.sql").write_text("INSERT INTO t_test_hour SELECT 1")
    calls, _ = capture_http(monkeypatch)
    runner = runner_module.SQLTaskRunner(path, tmp_path, config=config)
    assert "password" not in config["clickhouse"]
    config["clickhouse"]["host"] = "mutated.example"
    monkeypatch.setenv("TEST_CH_PASSWORD", "changed password")
    instance = SimpleNamespace(
        task=SimpleNamespace(sql_id="hour", target_table="t_test_hour", cleanup_column="window_time"),
        data_start=None,
        data_end=None,
        taskname="test_task"
    )
    runner(instance)
    runner(instance)
    # Should be 4 calls now: 2 cleanup + 2 aggregation
    assert len(calls) == 4
    for request, _ in calls:
        assert request.full_url.startswith("http://127.0.0.1:8123/?")
        assert base64.b64decode(request.get_header("Authorization").split()[1]) == b"user:first password"


def test_runner_resolves_password_once_and_preserves_window(tmp_path, monkeypatch):
    path = tmp_path / "app.yaml"
    path.write_text(yaml.safe_dump({"clickhouse": dict(host="127.0.0.1", port=8123, database="metrics",
                                                     username="user", password_env="TEST_CH_PASSWORD")}))
    monkeypatch.setenv("TEST_CH_PASSWORD", "first password")
    sql_directory = tmp_path / "sql"
    sql_directory.mkdir()
    (sql_directory / "hour.sql").write_text(WINDOW_SQL)
    calls, _ = capture_http(monkeypatch)
    runner = runner_module.SQLTaskRunner(path, sql_directory, timeout=120)
    monkeypatch.setenv("TEST_CH_PASSWORD", "changed password")
    instance = SimpleNamespace(
        task=SimpleNamespace(sql_id="hour", target_table="t_test_hour", cleanup_column="window_time"),
        data_start=datetime(2026, 9, 9, 1),
        data_end=datetime(2026, 9, 9, 2),
    )
    runner(instance)
    runner(instance)
    # Should be 4 calls now: 2 cleanup + 2 aggregation
    assert len(calls) == 4
    for request, timeout in calls:
        assert base64.b64decode(request.get_header("Authorization").split()[1]) == b"user:first password"
        assert timeout == 120
        assert request.data.decode() == WINDOW_SQL
    urls = [urllib.parse.parse_qs(urllib.parse.urlsplit(request.full_url).query) for request, _ in calls]
    assert urls[0]["query_id"] != urls[1]["query_id"]


def test_sql_task_runner_raises_when_sql_file_missing(tmp_path, monkeypatch):
    monkeypatch.setattr(runner_module, "load_config", lambda _: make_clickhouse_config())
    runner = runner_module.SQLTaskRunner(tmp_path / "app.yaml", tmp_path / "sql")
    instance = SimpleNamespace(task=SimpleNamespace(sql_id="missing"), data_start=None, data_end=None)
    with pytest.raises(TaskExecutionError, match="SQL_PREFLIGHT_FAILED"):
        runner(instance)


def test_explicit_query_id_is_used_for_unknown_outcome_evidence(monkeypatch):
    calls, _ = capture_http(monkeypatch, error=TimeoutError("private password"))
    with pytest.raises(RuntimeError) as caught:
        client_module.execute_sql(make_clickhouse_config(), "INSERT INTO source", query_id="durable-attempt-42")
    params = urllib.parse.parse_qs(urllib.parse.urlsplit(calls[0][0].full_url).query)
    assert params["query_id"] == ["durable-attempt-42"]
    assert caught.value.query_id == "durable-attempt-42"
    assert caught.value.outcome_unknown


def test_runner_passes_persisted_query_id_to_clickhouse(tmp_path, monkeypatch):
    monkeypatch.setattr(runner_module, "load_config", lambda _: make_clickhouse_config())
    (tmp_path / "hour.sql").write_text("SELECT 1")
    calls, _ = capture_http(monkeypatch)
    runner = runner_module.SQLTaskRunner(tmp_path / "app.yaml", tmp_path)
    instance = SimpleNamespace(
        task=SimpleNamespace(sql_id="hour", target_table="t_test_hour", cleanup_column="window_time"),
        data_start=None,
        data_end=None,
        query_id="durable-attempt-42"
    )
    runner(instance)
    # Should be 2 calls now: 1 cleanup + 1 aggregation
    assert len(calls) == 2
    params = urllib.parse.parse_qs(urllib.parse.urlsplit(calls[1][0].full_url).query)
    assert params["query_id"] == ["durable-attempt-42"]


@pytest.fixture
def localhost_clickhouse():
    """Exercise urllib over an actual socket without reaching any external host."""
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    observed = SimpleNamespace(requests=[], body=b'{"data":[[7]]}', status=200, disconnect=False, declared_length=None)

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_POST(self):
            observed.requests.append({
                "method": self.command,
                "path": self.path,
                "authorization": self.headers.get("Authorization"),
                "body": self.rfile.read(int(self.headers["Content-Length"])),
            })
            if observed.disconnect:
                self.close_connection = True
                return
            self.send_response(observed.status)
            self.send_header("Content-Type", "application/json")
            length = len(observed.body) if observed.declared_length is None else observed.declared_length
            self.send_header("Content-Length", str(length))
            self.end_headers()
            self.wfile.write(observed.body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    thread.start()
    observed.port = server.server_port
    try:
        yield observed
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_real_http_posts_exact_sql_parameters_and_basic_auth(localhost_clickhouse, monkeypatch):
    observed = localhost_clickhouse
    # Direct transport must not hand credentials to an environment-configured proxy.
    monkeypatch.setenv("http_proxy", "http://127.0.0.1:1")
    monkeypatch.setenv("no_proxy", "")
    monkeypatch.setattr(client_module, "system_timezone_name", lambda: "UTC")
    sql = WINDOW_SQL
    rows = client_module.execute_sql(
        make_clickhouse_config(port=observed.port), sql, timeout=2,
        start_time="2026-09-09T01:00:00Z", end_time="2026-09-09T02:00:00Z",
        query_id="persisted-local-attempt")
    assert rows == [[7]]
    assert len(observed.requests) == 1
    request = observed.requests[0]
    assert request["method"] == "POST" and request["body"] == sql.encode()
    assert base64.b64decode(request["authorization"].split()[1]) == b"test_user:private test password"
    params = urllib.parse.parse_qs(urllib.parse.urlsplit(request["path"]).query)
    assert params == {
        "database": ["metrics"], "default_format": ["JSONCompact"],
        "wait_end_of_query": ["1"], "max_execution_time": ["2"],
        "query_id": ["persisted-local-attempt"],
        "param_start_time": ["1788915600"],
        "param_end_time": ["1788919200"],
        "param_system_timezone": ["UTC"],
    }
    assert "private" not in request["path"]


@pytest.mark.parametrize("disconnect, status, body, unknown", [
    (True, 200, b"", True),
    (False, 200, b"DB exception: private password and SQL", True),
    (False, 400, b"DB exception after private SQL began executing", True),
    (False, 401, b"private authentication details", False),
])
def test_real_http_failure_is_redacted_and_never_retried(localhost_clickhouse, disconnect, status, body, unknown):
    observed = localhost_clickhouse
    observed.disconnect, observed.status, observed.body = disconnect, status, body
    with pytest.raises(client_module.SQLExecutionError) as caught:
        client_module.execute_sql(make_clickhouse_config(port=observed.port), "INSERT private SQL",
                                  timeout=2, query_id="persisted-local-failure")
    assert caught.value.outcome_unknown is unknown
    assert caught.value.retryable is False
    assert caught.value.query_id == "persisted-local-failure"
    assert "private" not in str(caught.value)
    assert caught.value.__suppress_context__
    assert len(observed.requests) == 1


@pytest.mark.parametrize("body", [b"", b'{"data":[[7]]}'])
def test_real_http_truncated_response_is_unknown_even_if_body_looks_successful(localhost_clickhouse, body):
    observed = localhost_clickhouse
    observed.body = body
    observed.declared_length = len(body) + 20
    with pytest.raises(client_module.SQLExecutionError) as caught:
        client_module.execute_sql(make_clickhouse_config(port=observed.port), "INSERT private SQL", timeout=2)
    assert caught.value.outcome_unknown and not caught.value.retryable
    assert len(observed.requests) == 1


def test_connection_error_after_response_headers_is_never_retried(monkeypatch):
    class InterruptedResponse(io.BytesIO):
        def read(self, *_):
            raise ConnectionRefusedError(errno.ECONNREFUSED, "private response detail")

    opener = SimpleNamespace(open=lambda *args, **kwargs: InterruptedResponse())
    monkeypatch.setattr(urllib.request, "build_opener", lambda *args: opener)
    with pytest.raises(client_module.SQLExecutionError) as caught:
        client_module.execute_sql(make_clickhouse_config(), "INSERT private SQL")
    assert caught.value.outcome_unknown and not caught.value.retryable


def make_slow_response_handler(phase, stopped):
    """Drip bytes during one HTTP phase until the test requests cleanup."""
    class SlowResponse(socketserver.StreamRequestHandler):
        def handle(self):
            while self.rfile.readline().strip():
                pass
            if phase == "headers":
                prefix, drip, suffix = b"HTTP/1.1 200 OK\r\nX-Drip: ", b"x" * 40, b"\r\nContent-Length: 0\r\n\r\n"
            elif phase == "chunk_size":
                prefix, drip, suffix = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n", b"0" * 40, b"\r\n\r\n"
            else:
                prefix, drip, suffix = b"HTTP/1.1 200 OK\r\nContent-Length: 40\r\n\r\n", b" " * 40, b""
            try:
                self.wfile.write(prefix)
                for byte in drip:
                    if stopped.wait(0.03):
                        return
                    self.wfile.write(bytes([byte]))
                self.wfile.write(suffix)
            except OSError:
                pass

    return SlowResponse


@pytest.mark.parametrize("phase", ["headers", "chunk_size", "body"])
@pytest.mark.parametrize("secure", [False, True])
def test_absolute_deadline_interrupts_continuous_slow_http_bytes(phase, secure, request, monkeypatch):
    """Every byte arrives before the idle timeout, but total time must stay bounded."""
    import threading
    import time

    stopped = threading.Event()
    timers, real_timer = [], threading.Timer

    def make_timer(*args, **kwargs):
        timer = real_timer(*args, **kwargs)
        timers.append(timer)
        return timer

    monkeypatch.setattr(threading, "Timer", make_timer)
    handler = make_slow_response_handler(phase, stopped)
    with socketserver.ThreadingTCPServer(("127.0.0.1", 0), handler) as server:
        if secure:
            context = request.getfixturevalue("tls_server_context")
            server.socket = context.wrap_socket(server.socket, server_side=True)
        server.daemon_threads = True
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        thread.start()
        error = None
        started = time.monotonic()
        try:
            try:
                client_module.execute_sql(make_clickhouse_config(port=server.server_address[1], secure=secure,
                                                                 verify=False),
                                          "INSERT private SQL", timeout=0.15,
                                          query_id="slow-response-attempt")
            except client_module.SQLExecutionError as failure:
                error = failure
            elapsed = time.monotonic() - started
        finally:
            stopped.set()
            server.shutdown()
            thread.join(timeout=2)
        assert elapsed < 1, f"{phase} bypassed total query deadline: {elapsed:.2f}s"
        assert error is not None and error.outcome_unknown and not error.retryable
        assert error.query_id == "slow-response-attempt"
        assert timers and all(not timer.is_alive() for timer in timers)


@pytest.fixture(scope="module")
def tls_server_context(tmp_path_factory):
    import shutil
    import subprocess

    tmp_path = tmp_path_factory.mktemp("local-tls")
    openssl = shutil.which("openssl")
    if openssl is None:
        pytest.skip("local TLS fixture requires openssl")
    certificate, key = tmp_path / "local-cert.pem", tmp_path / "local-key.pem"
    subprocess.run([openssl, "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                    "-subj", "/CN=localhost", "-keyout", str(key), "-out", str(certificate)],
                   check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certificate, key)
    return context


def test_real_https_uses_deadline_connection_and_completes_request(tls_server_context):
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    requests = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_POST(self):
            requests.append(self.rfile.read(int(self.headers["Content-Length"])))
            self.send_response(200)
            self.send_header("Content-Length", "14")
            self.end_headers()
            self.wfile.write(b'{"data":[[7]]}')

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        server.socket = tls_server_context.wrap_socket(server.socket, server_side=True)
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        thread.start()
        try:
            result = client_module.execute_sql(make_clickhouse_config(port=server.server_port, secure=True,
                                                                      verify=False), "SELECT 7", timeout=2)
            assert result == [[7]] and requests == [b"SELECT 7"]
        finally:
            server.shutdown()
            thread.join(timeout=2)


@pytest.mark.parametrize("error", [None, ConnectionResetError("private transport detail")])
def test_query_watchdog_is_reaped_after_success_or_failure(monkeypatch, error):
    import threading

    timers, real_timer = [], threading.Timer

    def make_timer(*args, **kwargs):
        timer = real_timer(*args, **kwargs)
        timers.append(timer)
        return timer

    monkeypatch.setattr(threading, "Timer", make_timer)
    capture_http(monkeypatch, error=error)
    if error is None:
        assert client_module.execute_sql(make_clickhouse_config(), "SELECT 1") == []
    else:
        with pytest.raises(client_module.SQLExecutionError):
            client_module.execute_sql(make_clickhouse_config(), "SELECT 1")
    assert timers and all(not timer.is_alive() for timer in timers)
