"""Debug SQL evidence must not change transport outcomes or ordinary output."""

import io
import json
import logging
import socketserver
import threading
import time
import urllib.error
from types import SimpleNamespace

import pytest
import yaml

from data_converge import main as entry
from data_converge.errors import safe_error_details
from data_converge.sql_execution import client, runner

from .test_sql_execution import capture_http, localhost_clickhouse, make_clickhouse_config
from .test_main import app, local_http_app


def debug_config(**values):
    return dict(make_clickhouse_config(**values), logging={"debug_enabled": True})


@pytest.mark.parametrize("settings", [None, {"debug_enabled": False}])
def test_disabled_debug_does_not_read_error_body_or_log_sql(monkeypatch, caplog, settings):
    class UnreadBody(io.BytesIO):
        def read(self, *_):
            pytest.fail("disabled debugging must not consume the error body")

    body = UnreadBody(b"Table metrics.missing does not exist")
    error = urllib.error.HTTPError("private-url", 404, "missing", {}, body)
    capture_http(monkeypatch, error=error)
    config = dict(make_clickhouse_config(), logging=settings)
    caplog.set_level(logging.DEBUG, logger="data_converge")
    with pytest.raises(client.SQLExecutionError, match="CLICKHOUSE_HTTP_FAILED"):
        client.execute_sql(config, "SELECT private_table", query_id="debug-off")
    assert "private_table" not in caplog.text
    assert "does not exist" not in caplog.text
    assert body.closed


def test_debug_logs_both_actual_cleanup_and_aggregation_requests(tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.DEBUG, logger="data_converge")
    monkeypatch.setattr(client, "system_timezone_name", lambda: "UTC")
    sql = "INSERT INTO metrics.hour SELECT {start_time:UInt32}"
    (tmp_path / "hour.sql").write_text(sql)
    calls, _ = capture_http(monkeypatch)
    task = SimpleNamespace(sql_id="hour", target_table="metrics.hour", cleanup_column="window_time")
    instance = SimpleNamespace(
        task=task, taskname="hour", query_id="attempt-debug",
        data_start="2026-09-09T01:00:00Z", data_end="2026-09-09T02:00:00Z")
    runner.SQLTaskRunner(tmp_path / "app.yaml", tmp_path, config=debug_config())(instance)
    records = [item for item in caplog.records if "SQL_REQUEST" in item.getMessage()]
    assert len(records) == len(calls) == 2
    for record, (request, _) in zip(records, calls):
        assert record.levelno == logging.DEBUG
        assert request.data.decode() in record.getMessage()
        assert "attempt-debug" in record.getMessage()
        assert "1788915600" in record.getMessage()
        assert "1788919200" in record.getMessage()
        assert "UTC" in record.getMessage()
    assert "private test password" not in caplog.text


@pytest.mark.parametrize("status, unknown", [(401, False), (404, True), (500, True)])
def test_real_http_debug_contains_missing_table_without_changing_failure(
        localhost_clickhouse, caplog, status, unknown):
    caplog.set_level(logging.DEBUG, logger="data_converge")
    server = localhost_clickhouse
    server.status = status
    server.body = b"Code: 60. DB::Exception: Table metrics.missing does not exist."
    with pytest.raises(client.SQLExecutionError) as caught:
        client.execute_sql(debug_config(port=server.port), "SELECT * FROM metrics.missing",
                           timeout=2, query_id="missing-table")
    assert "Table metrics.missing does not exist" in caplog.text
    assert "HTTPError" in caplog.text and "Traceback" in caplog.text
    assert "missing-table" in caplog.text
    assert caught.value.outcome_unknown is unknown
    assert caught.value.retryable is False
    assert safe_error_details(caught.value)["http_status"] == status
    assert "missing" not in str(caught.value)
    assert "response_body" not in safe_error_details(caught.value)
    assert len(server.requests) == 1


def test_debug_transport_exception_includes_detail_but_exception_stays_safe(monkeypatch, caplog):
    caplog.set_level(logging.DEBUG, logger="data_converge")
    capture_http(monkeypatch, error=ConnectionResetError("peer closed connection"))
    with pytest.raises(client.SQLExecutionError, match="CLICKHOUSE_OUTCOME_UNKNOWN") as caught:
        client.execute_sql(debug_config(), "SELECT 1", query_id="reset-attempt")
    assert "peer closed connection" in caplog.text
    assert "Traceback" in caplog.text
    assert caught.value.outcome_unknown and not caught.value.retryable
    assert str(caught.value) == "CLICKHOUSE_OUTCOME_UNKNOWN"


def test_debug_records_exception_body_returned_with_http_200(localhost_clickhouse, caplog):
    caplog.set_level(logging.DEBUG, logger="data_converge")
    localhost_clickhouse.body = b'{"exception":"Table metrics.missing does not exist."}'
    with pytest.raises(client.SQLExecutionError, match="CLICKHOUSE_RESPONSE_INVALID") as caught:
        client.execute_sql(debug_config(port=localhost_clickhouse.port), "SELECT 1", timeout=2)
    assert "Table metrics.missing does not exist" in caplog.text
    assert caught.value.outcome_unknown and not caught.value.retryable


@pytest.mark.parametrize("debug_enabled", [False, True])
def test_cli_writes_sql_to_service_log_only_when_enabled(
        local_http_app, monkeypatch, capsys, debug_enabled):
    path, requests = local_http_app
    monkeypatch.setenv("LOG_PATH", str(path.parent / "logs"))
    monkeypatch.setenv("SERVICE_NAME", "data-converge")
    config = yaml.safe_load(path.read_text())
    config["logging"] = {"debug_enabled": debug_enabled}
    path.write_text(yaml.safe_dump(config))
    base = ["--config", str(path)]
    assert entry.main(base + ["--init-state", "--start-time", "2026-10-12T10:00:00+08:00"]) == 0
    capsys.readouterr()
    assert entry.main(base + ["--run-once", "--now", "2026-10-12T10:06:00+08:00"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["instances"][0]["status"] == "SUCCESS"
    assert len(requests) == 2
    text = (path.parent / "logs/data-converge.log").read_text()
    assert ("SQL_REQUEST" in text) is debug_enabled
    for _, sql in requests:
        assert (sql in text) is debug_enabled
    assert "synthetic-integration-secret" not in text


def slow_error_handler(status, stopped):
    class SlowError(socketserver.StreamRequestHandler):
        def handle(self):
            while self.rfile.readline().strip():
                pass
            self.wfile.write(f"HTTP/1.1 {status} Error\r\nContent-Length: 100\r\n\r\n".encode())
            try:
                for _ in range(100):
                    if stopped.wait(0.03):
                        return
                    self.wfile.write(b"x")
            except OSError:
                stopped.set()

    return SlowError


@pytest.mark.parametrize("status, unknown", [(401, False), (500, True)])
def test_debug_error_body_obeys_deadline_without_changing_classification(
        caplog, monkeypatch, status, unknown):
    caplog.set_level(logging.DEBUG, logger="data_converge")
    stopped = threading.Event()
    timers, real_timer = [], threading.Timer

    def track_timer(*args, **kwargs):
        timer = real_timer(*args, **kwargs)
        timers.append(timer)
        return timer

    monkeypatch.setattr(threading, "Timer", track_timer)
    with socketserver.ThreadingTCPServer(("127.0.0.1", 0), slow_error_handler(status, stopped)) as server:
        server.daemon_threads = True
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        thread.start()
        started = time.monotonic()
        try:
            with pytest.raises(client.SQLExecutionError) as caught:
                client.execute_sql(debug_config(port=server.server_address[1]), "SELECT 1",
                                   timeout=0.15, query_id="slow-debug")
            assert time.monotonic() - started < 1
            assert caught.value.outcome_unknown is unknown
            assert not caught.value.retryable
            assert safe_error_details(caught.value)["http_status"] == status
            assert "SQL_ERROR" in caplog.text
            assert timers and all(not timer.is_alive() for timer in timers)
        finally:
            stopped.set()
            server.shutdown()
            thread.join(timeout=2)
