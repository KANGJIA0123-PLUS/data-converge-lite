"""Runner failures include task context while successful phases remain opt-in."""

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


@pytest.mark.parametrize("debug_enabled", [None, False, True])
def test_cleanup_error_logs_redacted_details_without_changing_safe_wrapper(
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
    contexts = [message for message in errors if "SQL_TASK_FAILED" in message]
    assert len(contexts) == 1 and "phase=cleanup" in contexts[0]
    assert "taskname=hourly_task" in contexts[0] and "instance_id=hourly_task@batch" in contexts[0]
    assert "query_id=runner-attempt" in contexts[0] and f"sql_file={tmp_path / 'hour.sql'}" in contexts[0]
    assert "window=[2026-10-20 01:00:00+00:00, 2026-10-20 02:00:00+00:00)" in contexts[0]
    assert "private cleanup diagnostic" in "\n".join(errors)
    assert PASSWORD not in caplog.text and "[REDACTED]" in caplog.text
    assert ("Traceback (most recent call last)" in caplog.text) is bool(debug_enabled)
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


@pytest.mark.parametrize("debug_enabled", [None, False])
@pytest.mark.parametrize("invalid", ["missing", "empty", "changed"])
def test_default_preflight_failure_identifies_task_and_window(
        tmp_path, monkeypatch, caplog, debug_enabled, invalid):
    runner = make_runner(tmp_path, debug_enabled)
    instance = make_instance()
    sql_file = tmp_path / "hour.sql"
    if invalid == "missing":
        sql_file.unlink()
    elif invalid == "empty":
        sql_file.write_text(" \n", encoding="utf-8")
    else:
        runner.expected_sql_hashes = {"hourly_task": "previous-hash"}
    monkeypatch.setattr(runner_module, "execute_sql", lambda **_: pytest.fail("SQL submitted"))
    caplog.set_level(logging.ERROR, logger="data_converge")

    with pytest.raises(TaskExecutionError) as caught:
        runner(instance)

    context = next(record.getMessage() for record in caplog.records
                   if "SQL_TASK_FAILED" in record.getMessage())
    assert "phase=preflight" in context and "taskname=hourly_task" in context
    assert "instance_id=hourly_task@batch" in context and "query_id=runner-attempt" in context
    assert f"sql_file={sql_file}" in context
    assert "window=[2026-10-20 01:00:00+00:00, 2026-10-20 02:00:00+00:00)" in context
    assert not caught.value.retryable and not caught.value.outcome_unknown
    assert str(caught.value) == (
        "SQL_CONFIGURATION_CHANGED" if invalid == "changed" else "SQL_PREFLIGHT_FAILED")
    assert PASSWORD not in caplog.text and AGGREGATION_SQL not in caplog.text
    assert "phase=cleanup" not in caplog.text and "phase=aggregate" not in caplog.text


@pytest.mark.parametrize("debug_enabled", [None, False])
def test_default_aggregate_failure_logs_context_and_preserves_original_error(
        tmp_path, monkeypatch, caplog, debug_enabled):
    runner = make_runner(tmp_path, debug_enabled)
    error = TaskExecutionError("CLICKHOUSE_HTTP_FAILED", retryable=True,
                               query_id="runner-attempt",
                               diagnostic={"http_status": 503, "clickhouse_exception_code": 241})

    def failed_aggregation(**values):
        if values["sql"] == AGGREGATION_SQL:
            raise error

    monkeypatch.setattr(runner_module, "execute_sql", failed_aggregation)
    caplog.set_level(logging.ERROR, logger="data_converge")

    with pytest.raises(TaskExecutionError) as caught:
        runner(make_instance())

    context = next(record.getMessage() for record in caplog.records
                   if "SQL_TASK_FAILED" in record.getMessage())
    assert "phase=aggregate" in context and "taskname=hourly_task" in context
    assert "instance_id=hourly_task@batch" in context and "query_id=runner-attempt" in context
    assert f"sql_file={tmp_path / 'hour.sql'}" in context
    assert "window=[2026-10-20 01:00:00+00:00, 2026-10-20 02:00:00+00:00)" in context
    assert "'http_status': 503" in context and "'clickhouse_exception_code': 241" in context
    assert caught.value is error and error.retryable and not error.outcome_unknown
    assert PASSWORD not in caplog.text and AGGREGATION_SQL not in caplog.text
    assert "phase=preflight" not in caplog.text and "phase=cleanup" not in caplog.text
