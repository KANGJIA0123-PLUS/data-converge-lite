"""Only the failed HTTP operation emits its redacted SQL at ERROR by default."""

import io
import json
import logging
import urllib.error
import urllib.parse
import urllib.request

import pytest
import yaml

from data_converge import main as entry
from data_converge.sql_execution import client
from data_converge.errors import TaskExecutionError
from .test_sql_debug import debug_config
from .test_sql_execution import capture_http
from .test_sql_runner_debug import make_instance, make_runner
from .test_main import app


AGGREGATION_SQL = (
    "INSERT INTO target SELECT toDateTime({start_time:UInt32}, {system_timezone:String}), "
    "{end_time:UInt32}"
)
CLEANUP_SQL = (
    "ALTER TABLE target_local DELETE WHERE window_time >= {start_time:DateTime} "
    "AND window_time < {end_time:DateTime} SETTINGS mutations_sync=2"
)


def assert_failed_request_sql(request, message, operation):
    parameters = urllib.parse.parse_qs(urllib.parse.urlsplit(request.full_url).query)
    assert parameters["param_start_time"] == ["1792458000"]
    assert parameters["param_end_time"] == ["1792461600"]
    assert parameters["param_system_timezone"] == ["Asia/Kathmandu"]
    assert request.data.decode() == (CLEANUP_SQL if operation == 1 else AGGREGATION_SQL)
    expected = (
        "ALTER TABLE target_local DELETE WHERE window_time >= CAST(1792458000 AS DateTime) "
        "AND window_time < CAST(1792461600 AS DateTime) SETTINGS mutations_sync=2"
        if operation == 1 else
        "INSERT INTO target SELECT toDateTime(CAST(1792458000 AS UInt32), 'Asia/Kathmandu'), "
        "CAST(1792461600 AS UInt32)"
    )
    assert message.split("\nsql=", 1)[1] == expected
    assert "sql_rendered=true" in message and "unresolved_parameters=[]" in message


@pytest.mark.parametrize("failed_operation", [1, 2], ids=["cleanup", "aggregation"])
@pytest.mark.parametrize("debug_enabled", [False, True])
def test_runner_logs_only_the_failed_operation_sql(
        tmp_path, monkeypatch, caplog, failed_operation, debug_enabled):
    task_runner = make_runner(tmp_path, debug_enabled=debug_enabled)
    (tmp_path / "hour.sql").write_text(AGGREGATION_SQL)
    monkeypatch.setattr(client, "system_timezone_name", lambda: "Asia/Kathmandu")
    calls = []

    class Opener:
        def open(self, request, timeout):
            calls.append(request)
            if len(calls) == failed_operation:
                raise urllib.error.HTTPError(request.full_url, 404, "missing table",
                                             {}, io.BytesIO(b"Table target does not exist"))
            return io.BytesIO(b"")

    monkeypatch.setattr(urllib.request, "build_opener", lambda *args: Opener())
    caplog.set_level(logging.DEBUG, logger="data_converge")
    with pytest.raises(TaskExecutionError):
        task_runner(make_instance())
    records = [record for record in caplog.records if "SQL_REQUEST" in record.getMessage()]
    assert len(records) == 1 and records[0].levelno == logging.ERROR
    text = records[0].getMessage()
    assert_failed_request_sql(calls[-1], text, failed_operation)
    assert "runner-attempt" in text
    assert "param_start_time" in text and "param_end_time" in text
    assert "Table target does not exist" in caplog.text
    errors = [record for record in caplog.records if "SQL_ERROR" in record.getMessage()]
    assert len(errors) == 1
    assert "SQL_TASK_FAILED" in caplog.text
    if failed_operation == 2:
        assert "ALTER TABLE target_local DELETE" not in caplog.text
    else:
        assert "INSERT INTO target" not in caplog.text
    assert "runner-secret-password" not in caplog.text


@pytest.mark.parametrize("body", [b"", b'{"data":[[1]]}'])
def test_successful_queries_do_not_log_sql_even_with_debug(monkeypatch, caplog, body):
    capture_http(monkeypatch, body=body)
    caplog.set_level(logging.DEBUG, logger="data_converge")
    client.execute_sql(debug_config(), "SELECT private_success_marker", query_id="successful")
    assert "SQL_REQUEST" not in caplog.text
    assert "private_success_marker" not in caplog.text


@pytest.mark.parametrize("failure", [ConnectionRefusedError("refused"), TimeoutError("timeout")])
def test_failed_transport_logs_request_once(monkeypatch, caplog, failure):
    capture_http(monkeypatch, error=failure)
    caplog.set_level(logging.DEBUG, logger="data_converge")
    with pytest.raises(client.SQLExecutionError):
        client.execute_sql(debug_config(), "SELECT failure_marker", query_id="failed")
    records = [record.getMessage() for record in caplog.records if "SQL_REQUEST" in record.getMessage()]
    assert len(records) == 1 and "SELECT failure_marker" in records[0]
    assert "SQL_ERROR" in caplog.text


@pytest.mark.parametrize("kind", ["invalid", "oversized", "incomplete"])
def test_failed_response_validation_logs_request_once(monkeypatch, caplog, kind):
    body = b"not json" if kind == "invalid" else b"12345" if kind == "oversized" else b""
    capture_http(monkeypatch, body=body)
    if kind == "oversized":
        monkeypatch.setattr(client, "MAX_RESPONSE_BYTES", 4)
    if kind == "incomplete":
        class Incomplete(io.BytesIO):
            length = 5
        opener = type("Opener", (), {"open": lambda *a, **kw: Incomplete()})()
        monkeypatch.setattr(urllib.request, "build_opener", lambda *args: opener)
    caplog.set_level(logging.DEBUG, logger="data_converge")
    with pytest.raises(client.SQLExecutionError):
        client.execute_sql(debug_config(), "SELECT invalid_response_marker", query_id="invalid-response")
    records = [record.getMessage() for record in caplog.records if "SQL_REQUEST" in record.getMessage()]
    assert len(records) == 1 and "SELECT invalid_response_marker" in records[0]


def test_deadline_expiring_after_response_still_logs_failed_request(monkeypatch, caplog):
    capture_http(monkeypatch)
    post = client._post_sql_request

    def expire_after_response(*args):
        result = post(*args)
        args[3].expires_at = 0
        return result

    monkeypatch.setattr(client, "_post_sql_request", expire_after_response)
    caplog.set_level(logging.DEBUG, logger="data_converge")
    with pytest.raises(client.SQLExecutionError, match="CLICKHOUSE_OUTCOME_UNKNOWN") as caught:
        client.execute_sql(debug_config(), "SELECT deadline_marker", query_id="expired")
    assert caught.value.outcome_unknown and not caught.value.retryable
    records = [record.getMessage() for record in caplog.records if "SQL_REQUEST" in record.getMessage()]
    assert len(records) == 1 and "SELECT deadline_marker" in records[0]


@pytest.mark.parametrize("debug_enabled", [False, True])
def test_cli_failure_writes_failed_sql_at_default_and_debug_levels(app, monkeypatch, capsys, debug_enabled):
    monkeypatch.setenv("LOG_PATH", str(app.parent / "logs"))
    monkeypatch.setenv("SERVICE_NAME", "data-converge")
    config = yaml.safe_load(app.read_text())
    config["clickhouse"].pop("password_file")
    config["clickhouse"]["password_env"] = "SQL_DEBUG_TEST_PASSWORD"
    monkeypatch.setenv("SQL_DEBUG_TEST_PASSWORD", "synthetic-test-password")
    config["logging"] = {"debug_enabled": debug_enabled}
    app.write_text(yaml.safe_dump(config))
    error = urllib.error.HTTPError("private-url", 404, "missing table", {},
                                   io.BytesIO(b"Table target_local does not exist"))
    calls, _ = capture_http(monkeypatch, error=error)
    base = ["--config", str(app)]
    assert entry.main(base + ["--init-state", "--start-time", "2026-10-12T10:00:00+08:00"]) == 0
    capsys.readouterr()
    assert entry.main(base + ["--run-once", "--now", "2026-10-12T10:06:00+08:00"]) == 1
    output = capsys.readouterr().out
    assert json.loads(output)["instances"][0]["status"] == "UNKNOWN"
    assert "Table target_local does not exist" not in output and "SQL_REQUEST" not in output
    assert len(calls) == 1
    text = (app.parent / "logs/data-converge.log").read_text()
    assert text.count("SQL_REQUEST") == 1
    assert calls[0][0].data.decode() == CLEANUP_SQL
    assert ("window_time >= CAST(1791770400 AS DateTime) "
            "AND window_time < CAST(1791770700 AS DateTime)") in text
    assert "sql_rendered=true" in text and "{start_time:DateTime}" not in text
    assert "Table target_local does not exist" in text
    assert "INSERT INTO target" not in text and "synthetic-test-password" not in text
