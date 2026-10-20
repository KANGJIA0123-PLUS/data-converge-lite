"""Opt-in SQL diagnostics retain useful evidence while masking credentials."""

import base64
import importlib
import io
import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request

import pytest


MODULE = "data_converge.sql_execution.debug"


@pytest.fixture
def debug():
    try:
        return importlib.import_module(MODULE)
    except ModuleNotFoundError:
        pytest.fail("SQL debug helper has not been implemented")


def debug_config(**settings):
    return {"clickhouse": {"username": "operator", "password": 'p@ss "词"/?'},
            "logging": {"debug_enabled": True, **settings}}


def sql_request(sql="SELECT {start_time:UInt32}"):
    query = urllib.parse.urlencode({
        "database": "metrics", "query_id": "attempt-42", "param_start_time": "1788915600",
        "param_end_time": "1788919200", "param_system_timezone": "Asia/Shanghai",
        "default_format": "JSONCompact", "wait_end_of_query": "1", "max_execution_time": "30",
        "password": "url-secret", "param_password": "parameter-secret", "token": "token-secret"})
    return urllib.request.Request(
        "https://private-host/?" + query, data=sql.encode(),
        headers={"Authorization": "Basic header-secret"})


@pytest.mark.parametrize("flag", [None, False, "true", 1])
def test_disabled_debug_never_reads_or_logs_private_content(debug, caplog, flag):
    config = debug_config(debug_enabled=flag)
    body = io.BytesIO(b"private error body")
    failure = urllib.error.HTTPError("private URL", 500, "private reason", {}, body)
    with caplog.at_level(logging.DEBUG):
        debug.log_request(config, sql_request("SELECT private_sql"))
        debug.log_error(config, "attempt-42", failure, body=b"private response")
        debug.log_http_error(config, "attempt-42", failure)
    assert not debug.enabled(config)
    assert body.tell() == 0 and not body.closed
    assert caplog.records == []


def test_debug_requires_effective_debug_log_level(debug, caplog):
    body = io.BytesIO(b"private error body")
    failure = urllib.error.HTTPError("private URL", 500, "private reason", {}, body)
    with caplog.at_level(logging.INFO, logger=MODULE):
        debug.log_request(debug_config(), sql_request())
        debug.log_http_error(debug_config(), "attempt-42", failure)
        assert not debug.enabled(debug_config())
    assert body.tell() == 0 and caplog.records == []


def test_request_logs_complete_wire_sql_and_only_approved_parameters(debug, caplog):
    sql = "SELECT {start_time:UInt32}\n" + ("-- retain SQL line\n" * 5000) + "-- final SQL line"
    with caplog.at_level(logging.DEBUG, logger=MODULE):
        debug.log_request(debug_config(), sql_request(sql))
    text = caplog.text
    assert "SQL_REQUEST" in text and sql in text
    for value in ("metrics", "attempt-42", "1788915600", "1788919200", "Asia/Shanghai"):
        assert value in text
    for secret in ("private-host", "header-secret", "url-secret", "parameter-secret", "token-secret"):
        assert secret not in text
    assert caplog.records[0].exc_info is None


def test_error_redacts_known_credential_encodings_and_retains_missing_table(debug, caplog):
    config = debug_config()
    password = config["clickhouse"]["password"]
    token = base64.b64encode(f"operator:{password}".encode()).decode()
    secrets = [password, urllib.parse.quote(password, safe=""), urllib.parse.quote_plus(password),
               json.dumps(password)[1:-1], token]
    body = "Table metrics.missing_local does not exist\n" + "\n".join(secrets)
    failure = ValueError("failure contains " + password)
    with caplog.at_level(logging.DEBUG, logger=MODULE):
        debug.log_error(config, "attempt-42", failure, body=body.encode())
    assert "SQL_ERROR" in caplog.text and "metrics.missing_local does not exist" in caplog.text
    assert "ValueError" in caplog.text and "attempt-42" in caplog.text
    assert all(secret not in caplog.text for secret in secrets)
    assert caplog.records[0].exc_info is None


@pytest.mark.parametrize("private_text, secret", [
    ("password=unconfigured-secret", "unconfigured-secret"),
    ('"password": "json-secret with spaces"', "json-secret with spaces"),
    ("access_token: alternate-secret", "alternate-secret"),
    ("Authorization: Bearer remote-token", "remote-token"),
    ("Basic dXNlcjpyZW1vdGU=", "dXNlcjpyZW1vdGU="),
    ("https://remote-user:remote-password@db.example/query", "remote-password"),
])
def test_error_redacts_generic_secret_forms(debug, caplog, private_text, secret):
    body = ("Table metrics.missing does not exist\n" + private_text).encode()
    with caplog.at_level(logging.DEBUG, logger=MODULE):
        debug.log_error(debug_config(), "attempt-42", ValueError("query failed"), body=body)
    assert "Table metrics.missing does not exist" in caplog.text
    assert secret not in caplog.text


def test_traceback_excludes_locals_and_chained_exception(debug, caplog):
    try:
        try:
            raise ValueError("hidden chained detail")
        except ValueError as cause:
            private_local = "hidden local value"
            raise RuntimeError("missing table") from cause
    except RuntimeError as failure:
        with caplog.at_level(logging.DEBUG, logger=MODULE):
            debug.log_error(debug_config(), "attempt-42", failure)
    assert "Traceback" in caplog.text and "RuntimeError: missing table" in caplog.text
    assert private_local not in caplog.text and "hidden chained detail" not in caplog.text
    assert all(record.exc_info is None for record in caplog.records)


@pytest.mark.parametrize("filler", [b"x" * 70000, b"a-" * 35000], ids=["word", "hyphenated"])
def test_http_error_reads_bounded_body_and_marks_truncation(debug, caplog, filler):
    body = io.BytesIO(b"Table metrics.missing does not exist\n" + filler + b"unread-tail")
    failure = urllib.error.HTTPError("private URL", 500, "query failed", {}, body)
    started = time.monotonic()
    with caplog.at_level(logging.DEBUG, logger=MODULE):
        debug.log_http_error(debug_config(), "attempt-42", failure)
    assert time.monotonic() - started < 1, "Redaction must not rescan every suffix of a long word"
    assert body.tell() == 65537 and not body.closed
    assert "Table metrics.missing does not exist" in caplog.text
    assert "body_truncated=True" in caplog.text and "unread-tail" not in caplog.text
    assert failure.code == 500 and failure.reason == "query failed"


def test_http_body_read_failure_preserves_original_exception(debug, caplog):
    class BrokenBody(io.BytesIO):
        def read(self, size):
            raise OSError("read failed with password=unconfigured-secret")

    failure = urllib.error.HTTPError("private URL", 401, "auth failed", {}, BrokenBody())
    with caplog.at_level(logging.DEBUG, logger=MODULE):
        debug.log_http_error(debug_config(), "attempt-42", failure)
    assert failure.code == 401 and failure.reason == "auth failed"
    assert "body_read_failed=OSError" in caplog.text and "HTTP Error 401" in caplog.text
    assert "unconfigured-secret" not in caplog.text


def test_invalid_utf8_error_body_keeps_readable_server_details(debug, caplog):
    failure = urllib.error.HTTPError(
        "private URL", 500, "query failed", {}, io.BytesIO(b"\xffTable metrics.missing does not exist"))
    with caplog.at_level(logging.DEBUG, logger=MODULE):
        debug.log_http_error(debug_config(), "attempt-42", failure)
    assert "Table metrics.missing does not exist" in caplog.text
    assert "\ufffd" in caplog.text


def test_empty_http_body_still_logs_numeric_server_evidence(debug, caplog):
    failure = urllib.error.HTTPError("private URL", 500, "query failed",
                                     {"X-ClickHouse-Exception-Code": "60"}, io.BytesIO())
    with caplog.at_level(logging.DEBUG, logger=MODULE):
        debug.log_http_error(debug_config(), "attempt-42", failure)
    assert '"http_status": 500' in caplog.text
    assert '"clickhouse_exception_code": 60' in caplog.text


def test_transport_error_retains_numeric_error_evidence(debug, caplog):
    failure = ConnectionRefusedError(61, "connection refused")
    with caplog.at_level(logging.DEBUG, logger=MODULE):
        debug.log_error(debug_config(), "attempt-42", failure)
    assert '"os_errno": 61' in caplog.text


@pytest.mark.parametrize("encoding", ["plain", "url", "json", "basic"])
def test_truncation_does_not_expose_partial_known_credentials(debug, caplog, encoding):
    config = debug_config()
    password = config["clickhouse"]["password"]
    secret = {
        "plain": password,
        "url": urllib.parse.quote(password, safe=""),
        "json": json.dumps(password)[1:-1],
        "basic": base64.b64encode(f"operator:{password}".encode()).decode(),
    }[encoding]
    prefix = secret[:-3]
    useful = b"Table metrics.missing does not exist\n"
    padding = b"x" * (65536 - len(useful) - len(prefix.encode()))
    body = useful + padding + secret.encode()
    failure = urllib.error.HTTPError("private URL", 500, "query failed", {}, io.BytesIO(body))
    with caplog.at_level(logging.DEBUG, logger=MODULE):
        debug.log_http_error(config, "attempt-42", failure)
    assert "Table metrics.missing does not exist" in caplog.text
    assert "body_truncated=True" in caplog.text
    assert prefix not in caplog.text
    assert "REDACTED" in caplog.text
