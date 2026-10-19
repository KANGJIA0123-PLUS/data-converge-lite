"""Only the approved SQL text may leave a configured task runner."""

import hashlib
from dataclasses import replace
from datetime import timedelta

import pytest

from data_converge.dag import DAGExecutor
from data_converge.errors import TaskExecutionError
from data_converge.sql_execution import runner as runner_module

from .test_sql_execution import capture_http, make_clickhouse_config
from .test_state import NOW, graph, open_store


APPROVED_SQL = "INSERT INTO rollup SELECT 1"
APPROVED_HASH = hashlib.sha256(APPROVED_SQL.encode("utf-8")).hexdigest()


def configured_runner(tmp_path, hashes):
    (tmp_path / "root.sql").write_text(APPROVED_SQL, encoding="utf-8")
    return runner_module.SQLTaskRunner(
        tmp_path / "app.yaml", tmp_path, config=make_clickhouse_config(),
        expected_sql_hashes=hashes)


def test_runtime_sql_replacement_is_rejected_before_submission(tmp_path, monkeypatch):
    runner = configured_runner(tmp_path, {"root": APPROVED_HASH})
    (tmp_path / "root.sql").write_text("INSERT INTO private_table SELECT 2", encoding="utf-8")
    builder, dag = graph()
    instance = builder.create_instance(dag, "root", NOW)
    instance.query_id = "approved-attempt"
    calls, _ = capture_http(monkeypatch)

    with pytest.raises(TaskExecutionError, match="^SQL_CONFIGURATION_CHANGED$") as caught:
        runner(instance)

    assert calls == []
    assert caught.value.query_id == "approved-attempt"
    assert not caught.value.retryable and not caught.value.outcome_unknown
    assert "private_table" not in str(caught.value)


def test_approved_digest_uses_the_stripped_sql_sent_to_clickhouse(tmp_path, monkeypatch):
    runner = configured_runner(tmp_path, {"root": APPROVED_HASH})
    (tmp_path / "root.sql").write_text("\n  " + APPROVED_SQL + " \n", encoding="utf-8")
    builder, dag = graph()
    calls, _ = capture_http(monkeypatch)

    runner(builder.create_instance(dag, "root", NOW))

    assert len(calls) == 1
    assert calls[0][0].data == APPROVED_SQL.encode("utf-8")


def test_runner_executes_the_same_text_it_verified(tmp_path, monkeypatch):
    runner = configured_runner(tmp_path, {"root": APPROVED_HASH})
    builder, dag = graph()
    original_load = runner_module.load_sql
    calls, _ = capture_http(monkeypatch)

    def replace_after_read(path):
        sql = original_load(path)
        path.write_text("INSERT INTO rollup SELECT 2", encoding="utf-8")
        return sql

    monkeypatch.setattr(runner_module, "load_sql", replace_after_read)
    runner(builder.create_instance(dag, "root", NOW))
    assert calls[0][0].data == APPROVED_SQL.encode("utf-8")
    with pytest.raises(TaskExecutionError, match="SQL_CONFIGURATION_CHANGED"):
        runner(builder.create_instance(dag, "root", NOW))
    assert len(calls) == 1


def test_runner_copies_expected_hashes_and_rejects_missing_task(tmp_path, monkeypatch):
    hashes = {"root": APPROVED_HASH}
    runner = configured_runner(tmp_path, hashes)
    hashes["root"] = "unapproved"
    builder, dag = graph()
    calls, _ = capture_http(monkeypatch)
    runner(builder.create_instance(dag, "root", NOW))
    assert len(calls) == 1
    runner = configured_runner(tmp_path, {})
    with pytest.raises(TaskExecutionError, match="SQL_CONFIGURATION_CHANGED"):
        runner(builder.create_instance(dag, "root", NOW))
    assert len(calls) == 1


def test_expected_hashes_are_keyed_by_task_name_not_shared_sql_resource(tmp_path, monkeypatch):
    runner = configured_runner(tmp_path, {"renamed_root": APPROVED_HASH})
    builder, dag = graph()
    instance = builder.create_instance(dag, "root", NOW)
    instance.task = replace(instance.task, taskname="renamed_root")
    calls, _ = capture_http(monkeypatch)

    runner(instance)

    assert len(calls) == 1 and calls[0][0].data == APPROVED_SQL.encode("utf-8")
    with pytest.raises(TaskExecutionError, match="SQL_CONFIGURATION_CHANGED"):
        runner(builder.create_instance(dag, "root", NOW))
    assert len(calls) == 1


def test_sql_configuration_failure_is_terminal_and_preserves_execution_evidence(tmp_path, monkeypatch):
    runner = configured_runner(tmp_path, {"root": APPROVED_HASH})
    (tmp_path / "root.sql").write_text("INSERT INTO rollup SELECT 2", encoding="utf-8")
    calls, _ = capture_http(monkeypatch)
    builder, dag = graph()
    store = open_store(tmp_path / "state.db")
    try:
        executor = DAGExecutor(dag, runner, state_store=store)
        instance = builder.create_instance(dag, "root", NOW)
        report = executor.execute([instance], now=NOW)
        saved = store.get_instance(instance.instance_id)
        assert report.failed == [instance] and not report.unknown
        assert saved["error"] == "SQL_CONFIGURATION_CHANGED"
        assert not saved["retryable"] and saved["next_retry_at"] is None
        assert saved["query_id"] == instance.query_id and instance.query_id
        executor.execute([builder.create_instance(dag, "root", NOW)], now=NOW + timedelta(days=1))
        assert store.get_instance(instance.instance_id)["attempts"] == 1
        assert calls == []
    finally:
        store.close()
