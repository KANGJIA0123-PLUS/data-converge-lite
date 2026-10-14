"""SQL files fail before submission without becoming uncertain writes."""

from datetime import timedelta

import pytest

from data_converge.dag import DAGExecutor, TaskStatus
from data_converge.errors import TaskExecutionError
from data_converge.sql_execution import runner as runner_module

from .test_sql_execution import make_clickhouse_config
from .test_state import NOW, graph, open_store


@pytest.fixture(params=["missing", "empty", "invalid_utf8", "directory"])
def invalid_sql(request, tmp_path):
    path = tmp_path / "root.sql"
    if request.param == "empty":
        path.write_text(" \n\t", encoding="utf-8")
    elif request.param == "invalid_utf8":
        path.write_bytes(b"\xff")
    elif request.param == "directory":
        path.mkdir()
    return path


def test_sql_preflight_failure_is_definite_and_does_not_submit(invalid_sql, monkeypatch):
    builder, dag = graph()
    instance = builder.create_instance(dag, "root", NOW)
    instance.query_id = "preflight-attempt"
    runner = runner_module.SQLTaskRunner(invalid_sql.parent / "app.yaml", invalid_sql.parent,
                                        config=make_clickhouse_config())
    monkeypatch.setattr(runner_module, "execute_sql", lambda **_: pytest.fail("SQL submitted"))

    with pytest.raises(TaskExecutionError, match="^SQL_PREFLIGHT_FAILED$") as caught:
        runner(instance)

    assert not caught.value.retryable and not caught.value.outcome_unknown
    assert caught.value.query_id == "preflight-attempt"
    assert str(invalid_sql) not in str(caught.value)


def test_preflight_failure_is_persisted_without_automatic_retry(invalid_sql, monkeypatch):
    builder, dag = graph()
    runner = runner_module.SQLTaskRunner(invalid_sql.parent / "app.yaml", invalid_sql.parent,
                                        config=make_clickhouse_config())
    monkeypatch.setattr(runner_module, "execute_sql", lambda **_: pytest.fail("SQL submitted"))
    store = open_store(invalid_sql.parent / "state.db")
    try:
        executor = DAGExecutor(dag, runner, state_store=store)
        instance = builder.create_instance(dag, "root", NOW)
        report = executor.execute([instance], now=NOW)
        saved = store.get_instance(instance.instance_id)
        assert report.failed == [instance] and not report.unknown
        assert saved["status"] == "FAILED" and saved["error"] == "SQL_PREFLIGHT_FAILED"
        assert not saved["retryable"] and saved["next_retry_at"] is None
        assert saved["query_id"] == instance.query_id and instance.query_id
        executor.execute([builder.create_instance(dag, "root", NOW)], now=NOW + timedelta(days=1))
        assert store.get_instance(instance.instance_id)["attempts"] == 1
    finally:
        store.close()


@pytest.mark.parametrize("error", [
    TaskExecutionError("CLICKHOUSE_OUTCOME_UNKNOWN", outcome_unknown=True, query_id="sent-query"),
    ValueError("private response"),
    OSError("private transport"),
])
def test_execution_errors_stay_outside_preflight_classification(tmp_path, monkeypatch, error):
    (tmp_path / "root.sql").write_text("INSERT INTO rollup SELECT 1", encoding="utf-8")
    builder, dag = graph()
    runner = runner_module.SQLTaskRunner(tmp_path / "app.yaml", tmp_path,
                                        config=make_clickhouse_config())

    def failed_execution(**_):
        raise error

    monkeypatch.setattr(runner_module, "execute_sql", failed_execution)
    with pytest.raises(type(error)) as caught:
        runner(builder.create_instance(dag, "root", NOW))
    assert caught.value is error
    assert DAGExecutor._classify_error(caught.value)[:2] == (TaskStatus.UNKNOWN, False)
