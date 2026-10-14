"""Safe failure evidence survives execution without retaining remote content."""

import errno
import io
import json
import socket
import sqlite3
import ssl
import urllib.error
from contextlib import closing

import pytest

from data_converge import errors
from data_converge.dag import DAGExecutor, TaskStatus
from data_converge.sql_execution import client
from data_converge.state import StateStore

from .test_sql_execution import capture_http, make_clickhouse_config
from .test_state import NOW, graph, open_store


def test_safe_error_details_never_exposes_generic_exception_text():
    details = errors.safe_error_details(ValueError("SELECT password FROM private_table"))
    assert details == {"error_type": "ValueError"}
    assert errors.safe_error_details(ValueError("CLICKHOUSE_CREDENTIAL_MISSING")) == {
        "error_type": "ValueError", "error_code": "CLICKHOUSE_CREDENTIAL_MISSING"}


def test_failure_metadata_is_filtered_and_copied():
    supplied = {"http_status": 503, "password": "private", "sql": "SELECT secret",
                "tls_verify_code": True, "os_errno": "private", "query_id": "secret url /?"}
    error = errors.TaskExecutionError("CLICKHOUSE_HTTP_FAILED", query_id="attempt-1", diagnostic=supplied)
    supplied["http_status"] = 401
    assert errors.safe_error_details(error) == {
        "error_type": "TaskExecutionError", "error_code": "CLICKHOUSE_HTTP_FAILED",
        "query_id": "attempt-1", "http_status": 503}


@pytest.mark.parametrize("header, expected", [("241", 241), ("241 private password", None),
                                              pytest.param("9" * 5000, None, id="oversized"), ("-1", None)])
def test_http_failure_retains_only_numeric_server_evidence(monkeypatch, header, expected):
    response = io.BytesIO(b"SELECT private password")
    failure = urllib.error.HTTPError("private URL", 503, "private password",
                                     {"X-ClickHouse-Exception-Code": header}, response)
    capture_http(monkeypatch, error=failure)
    with pytest.raises(client.SQLExecutionError) as caught:
        client.execute_sql(make_clickhouse_config(), "SELECT private", query_id="attempt-2")
    details = errors.safe_error_details(caught.value)
    assert details["http_status"] == 503
    assert details["error_type"] == "HTTPError"
    assert details.get("clickhouse_exception_code") == expected
    assert details["query_id"] == "attempt-2"
    assert "private" not in json.dumps(details)
    assert caught.value.outcome_unknown and not caught.value.retryable
    assert response.closed


@pytest.mark.parametrize("failure, expected_type, expected_errno, retryable", [
    (socket.gaierror(socket.EAI_NONAME, "private hostname"), "gaierror", socket.EAI_NONAME, True),
    (ConnectionRefusedError(errno.ECONNREFUSED, "private hostname"), "ConnectionRefusedError", errno.ECONNREFUSED, True),
    (TimeoutError("private password"), "TimeoutError", None, False),
])
def test_transport_failure_retains_type_and_errno(monkeypatch, failure, expected_type, expected_errno, retryable):
    capture_http(monkeypatch, error=urllib.error.URLError(failure))
    with pytest.raises(client.SQLExecutionError) as caught:
        client.execute_sql(make_clickhouse_config(), "SELECT private", query_id="attempt-3")
    details = errors.safe_error_details(caught.value)
    assert details["error_type"] == expected_type
    assert details.get("os_errno") == expected_errno
    assert caught.value.retryable is retryable
    assert caught.value.outcome_unknown is not retryable
    assert "private" not in json.dumps(details)


def test_tls_verification_failure_keeps_numeric_code_without_certificate_text(monkeypatch):
    failure = ssl.SSLCertVerificationError(1, "private certificate subject")
    failure.verify_code = 62
    failure.verify_message = "private certificate subject"
    capture_http(monkeypatch, error=urllib.error.URLError(failure))
    with pytest.raises(client.SQLExecutionError) as caught:
        client.execute_sql(make_clickhouse_config(), "SELECT private")
    details = errors.safe_error_details(caught.value)
    assert details["error_type"] == "SSLCertVerificationError"
    assert details["tls_verify_code"] == 62
    assert "private" not in json.dumps(details)
    assert caught.value.outcome_unknown and not caught.value.retryable


def test_executor_persists_sanitized_evidence_for_current_attempt(tmp_path):
    builder, dag = graph()
    instance = builder.create_instance(dag, "root", NOW)

    def fail(item):
        raise client.SQLExecutionError("CLICKHOUSE_HTTP_FAILED", outcome_unknown=True,
                                       query_id=item.query_id,
                                       diagnostic={"http_status": 500, "sql": "private"})

    with closing(open_store(tmp_path / "state.db")) as store:
        DAGExecutor(dag, fail, state_store=store).execute([instance], now=NOW)
        details = store.get_execution_diagnostic(instance.instance_id)
        assert details["http_status"] == 500
        assert details["error_code"] == "CLICKHOUSE_HTTP_FAILED"
        assert details["query_id"] == instance.query_id
        assert "private" not in json.dumps(details)
    with closing(StateStore(tmp_path / "state.db")) as reader:
        assert reader.get_execution_diagnostic(instance.instance_id) == details


def test_old_state_inspection_does_not_create_diagnostic_schema(tmp_path):
    path = tmp_path / "state.db"
    with closing(open_store(path)) as store:
        tables = store._connection().execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        assert "execution_diagnostics" not in {row[0] for row in tables}
        assert store.get_execution_diagnostic("missing") is None
    before = path.read_bytes()
    with closing(StateStore(path)) as reader:
        assert reader.get_execution_diagnostic("missing") is None
    assert path.read_bytes() == before


def test_resolved_or_different_attempt_cannot_reuse_diagnostic(tmp_path):
    builder, dag = graph()
    instance = builder.create_instance(dag, "root", NOW)
    with closing(open_store(tmp_path / "state.db")) as store:
        store.enqueue_and_advance([instance], {})
        store.claim(instance.instance_id, now=NOW)
        store.finish(instance.instance_id, TaskStatus.UNKNOWN,
                     diagnostic={"http_status": 500}, now=NOW)
        assert store.get_execution_diagnostic(instance.instance_id)["http_status"] == 500
        store.resolve(instance.instance_id, "PENDING", "operator verified")
        assert store.get_execution_diagnostic(instance.instance_id) is None
        store.claim(instance.instance_id, now=NOW)
        store.finish(instance.instance_id, TaskStatus.UNKNOWN, now=NOW)
        assert store.get_execution_diagnostic(instance.instance_id) is None


def test_diagnostic_write_failure_rolls_back_task_finish(tmp_path):
    builder, dag = graph()
    instance = builder.create_instance(dag, "root", NOW)
    with closing(open_store(tmp_path / "state.db")) as store:
        store.enqueue_and_advance([instance], {})
        store.claim(instance.instance_id, now=NOW)
        store._connection(write=True).execute("CREATE TABLE execution_diagnostics (invalid TEXT)")
        with pytest.raises(sqlite3.DatabaseError, match="instance_id|query_id|details"):
            store.finish(instance.instance_id, TaskStatus.UNKNOWN,
                         diagnostic={"http_status": 500}, now=NOW)
        assert store.get_status(instance.instance_id) == TaskStatus.RUNNING
