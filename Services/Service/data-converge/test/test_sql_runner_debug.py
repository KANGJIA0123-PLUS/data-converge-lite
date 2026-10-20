"""Runner context and error diagnostics are opt-in and never submit real SQL."""

import logging
from datetime import datetime, timezone

import pytest

from data_converge.dag import TaskDefinition, TaskInstance
from data_converge.errors import TaskExecutionError
from data_converge.sql_execution import runner as runner_module


AGGREGATION_SQL = "INSERT INTO target SELECT 'aggregation marker'"
PASSWORD = "runner-secret-password"


def make_runner(tmp_path, debug_enabled=None):
    config = {"clickhouse": {"host": "localhost", "port": 8123, "database": "metrics",
                             "username": "runner", "password": PASSWORD}}
    if debug_enabled is not None:
        config["logging"] = {"debug_enabled": debug_enabled}
    (tmp_path / "hour.sql").write_text(AGGREGATION_SQL, encoding="utf-8")
    return runner_module.SQLTaskRunner(tmp_path / "app.yaml", tmp_path, config=config)


def make_instance():
    task = TaskDefinition("hourly_task", "", ("source",), "target", "1h",
                          "0 * * * *", (), "hour")
    start = datetime(2026, 10, 20, 1, tzinfo=timezone.utc)
    end = datetime(2026, 10, 20, 2, tzinfo=timezone.utc)
    return TaskInstance(task, "batch", end, start, end, (), query_id="runner-attempt")


def test_debug_context_identifies_each_phase_without_repeating_sql(tmp_path, monkeypatch, caplog):
    runner = make_runner(tmp_path, debug_enabled=True)
    instance = make_instance()
    submissions = []
    monkeypatch.setattr(runner_module, "execute_sql", lambda **values: submissions.append(values))
    caplog.set_level(logging.DEBUG, logger="data_converge")

    runner(instance)

    records = [record for record in caplog.records if record.levelno == logging.DEBUG]
    assert len(records) == 3
    for record, phase in zip(records, ("preflight", "cleanup", "aggregate")):
        message = record.getMessage()
        assert f"phase={phase}" in message
        assert "taskname=hourly_task" in message and "query_id=runner-attempt" in message
        assert f"sql_file={tmp_path / 'hour.sql'}" in message
        assert "window=[2026-10-20 01:00:00+00:00, 2026-10-20 02:00:00+00:00)" in message
        assert PASSWORD not in message and AGGREGATION_SQL not in message
    assert [values["query_id"] for values in submissions] == ["runner-attempt", "runner-attempt"]
    assert submissions[0]["sql"] == (
        "ALTER TABLE target_local DELETE WHERE window_time >= {start_time:DateTime} "
        "AND window_time < {end_time:DateTime} SETTINGS mutations_sync=2")
    assert submissions[1]["sql"] == AGGREGATION_SQL
    assert all(values["start_time"] == instance.data_start for values in submissions)
    assert all(values["end_time"] == instance.data_end for values in submissions)


@pytest.mark.parametrize("invalid, detail", [
    ("missing", "FileNotFoundError: SQL 文件不存在"),
    ("empty", "ValueError: SQL 文件内容为空"),
])
def test_debug_preflight_records_original_error_before_safe_wrapper(
        tmp_path, monkeypatch, caplog, invalid, detail):
    runner = make_runner(tmp_path, debug_enabled=True)
    sql_file = tmp_path / "hour.sql"
    if invalid == "missing":
        sql_file.unlink()
    else:
        sql_file.write_text(" \n", encoding="utf-8")
    monkeypatch.setattr(runner_module, "execute_sql", lambda **_: pytest.fail("SQL submitted"))
    caplog.set_level(logging.DEBUG, logger="data_converge")

    with pytest.raises(TaskExecutionError, match="^SQL_PREFLIGHT_FAILED$") as caught:
        runner(make_instance())

    assert detail in caplog.text and "Traceback (most recent call last)" in caplog.text
    assert f"sql_file={sql_file}" in caplog.text and "phase=preflight" in caplog.text
    assert "phase=cleanup" not in caplog.text and "phase=aggregate" not in caplog.text
    assert "query_id=runner-attempt" in caplog.text
    assert caught.value.query_id == "runner-attempt"
    assert not caught.value.retryable and not caught.value.outcome_unknown
    assert str(sql_file) not in str(caught.value) and caught.value.__suppress_context__


@pytest.mark.parametrize("debug_enabled", [None, False])
def test_default_configuration_suppresses_runner_debug_even_with_debug_logger(
        tmp_path, monkeypatch, caplog, debug_enabled):
    runner = make_runner(tmp_path, debug_enabled)
    monkeypatch.setattr(runner_module, "execute_sql", lambda **_: [])
    caplog.set_level(logging.DEBUG, logger="data_converge")
    runner(make_instance())
    (tmp_path / "hour.sql").unlink()

    with pytest.raises(TaskExecutionError, match="^SQL_PREFLIGHT_FAILED$"):
        runner(make_instance())

    assert not [record for record in caplog.records if record.levelno == logging.DEBUG]
    assert "Traceback" not in caplog.text and PASSWORD not in caplog.text


@pytest.mark.parametrize("debug_enabled", [False, True])
def test_cleanup_error_keeps_raw_details_only_in_opt_in_debug(
        tmp_path, monkeypatch, caplog, debug_enabled):
    runner = make_runner(tmp_path, debug_enabled)
    submissions = []

    def failed_execution(**values):
        submissions.append(values["sql"])
        raise RuntimeError(f"private cleanup diagnostic {PASSWORD}")

    monkeypatch.setattr(runner_module, "execute_sql", failed_execution)
    caplog.set_level(logging.DEBUG, logger="data_converge")

    with pytest.raises(TaskExecutionError, match="^IDEMPOTENT_CLEANUP_FAILED$") as caught:
        runner(make_instance())

    errors = [record.getMessage() for record in caplog.records if record.levelno >= logging.ERROR]
    assert len(errors) == 1 and "RuntimeError" in errors[0]
    assert "private cleanup diagnostic" not in errors[0] and PASSWORD not in caplog.text
    assert ("private cleanup diagnostic" in caplog.text) is debug_enabled
    assert ("Traceback (most recent call last)" in caplog.text) is debug_enabled
    assert caught.value.retryable and caught.value.outcome_unknown
    assert caught.value.query_id == "runner-attempt" and caught.value.__suppress_context__
    assert caught.value.diagnostic == {"error_type": "RuntimeError"}
    assert len(submissions) == 1


def test_cleanup_error_retains_safe_code_and_numeric_evidence(tmp_path, monkeypatch, caplog):
    runner = make_runner(tmp_path)

    def failed_execution(**_):
        raise TaskExecutionError("CLICKHOUSE_HTTP_FAILED", query_id="runner-attempt",
                                 diagnostic={"http_status": 503})

    monkeypatch.setattr(runner_module, "execute_sql", failed_execution)
    caplog.set_level(logging.DEBUG, logger="data_converge")

    with pytest.raises(TaskExecutionError, match="^IDEMPOTENT_CLEANUP_FAILED$") as caught:
        runner(make_instance())

    assert "'http_status': 503" in caplog.text
    assert "'error_code': 'CLICKHOUSE_HTTP_FAILED'" in caplog.text
    assert "'query_id': 'runner-attempt'" in caplog.text
    assert caught.value.diagnostic["http_status"] == 503
