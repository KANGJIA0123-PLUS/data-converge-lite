"""Execution backends share state transitions without coupling the DAG to SQLite."""

from datetime import timedelta

import pytest

from data_converge.dag import DAGExecutor, TaskStatus
from data_converge.dag.memory import InMemoryExecutionRepository
from data_converge.errors import TaskExecutionError

from .test_state import NOW, graph, open_store


@pytest.fixture(params=["memory", "sqlite"])
def repository(request, tmp_path):
    if request.param == "memory":
        yield InMemoryExecutionRepository()
        return
    store = open_store(tmp_path / "state.db")
    try:
        yield store
    finally:
        store.close()


def test_repository_claim_is_exclusive_and_success_is_terminal(repository):
    builder, dag = graph()
    instance = builder.create_instance(dag, "root", NOW)
    repository.enqueue_and_advance([instance], {})
    assert repository.claim(instance.instance_id, now=NOW)
    assert repository.get_status(instance.instance_id) == TaskStatus.RUNNING
    assert not repository.claim(instance.instance_id, now=NOW)
    repository.finish(instance.instance_id, TaskStatus.SUCCESS, now=NOW)
    assert repository.get_status(instance.instance_id) == TaskStatus.SUCCESS
    assert not repository.claim(instance.instance_id, now=NOW + timedelta(days=1))


def test_repository_unknown_result_cannot_be_retried(repository):
    builder, dag = graph()
    instance = builder.create_instance(dag, "root", NOW)
    repository.enqueue_and_advance([instance], {})
    assert repository.claim(instance.instance_id, now=NOW)
    repository.finish(instance.instance_id, TaskStatus.UNKNOWN, retryable=True, now=NOW)
    assert not repository.claim(instance.instance_id, now=NOW + timedelta(days=1))


def test_repository_status_reports_running_after_retry(repository):
    builder, dag = graph()
    instance = builder.create_instance(dag, "root", NOW)
    repository.enqueue_and_advance([instance], {})
    assert repository.claim(instance.instance_id, now=NOW)
    repository.finish(instance.instance_id, TaskStatus.FAILED, retryable=True, now=NOW)
    assert repository.claim(instance.instance_id, now=NOW + timedelta(seconds=30))
    assert repository.get_status(instance.instance_id) == TaskStatus.RUNNING


def test_repository_evidence_is_sanitized_and_cleared_for_next_attempt(repository):
    builder, dag = graph()
    instance = builder.create_instance(dag, "root", NOW)
    instance.query_id = "caller-attempt"
    repository.enqueue_and_advance([instance], {})
    assert repository.claim(instance.instance_id, now=NOW)
    query_id = repository.get_instance(instance.instance_id)["query_id"]
    repository.finish(instance.instance_id, TaskStatus.FAILED, retryable=True,
                      diagnostic={"http_status": 503, "sql": "private"}, now=NOW)
    assert repository.get_execution_diagnostic(instance.instance_id) == {
        "http_status": 503, "query_id": query_id}
    assert repository.claim(instance.instance_id, now=NOW)
    assert repository.get_execution_diagnostic(instance.instance_id) is None
    repository.finish(instance.instance_id, TaskStatus.UNKNOWN, now=NOW)
    assert repository.get_execution_diagnostic(instance.instance_id) is None


def test_memory_active_claim_survives_registration_of_another_batch():
    builder, dag = graph()
    repository = InMemoryExecutionRepository()
    instance = builder.create_instance(dag, "root", NOW)
    instance.query_id = "existing-query"
    repository.enqueue_and_advance([instance], {})
    assert repository.claim(instance.instance_id, now=NOW)
    before = repository.get_instance(instance.instance_id)
    later = builder.create_instance(dag, "root", NOW + timedelta(minutes=5))
    repository.enqueue_and_advance([later], {})
    assert repository.get_instance(instance.instance_id) == before
    assert not repository.claim(instance.instance_id, now=NOW)
    repository.finish(instance.instance_id, TaskStatus.SUCCESS, now=NOW)
    completed = repository.get_instance(instance.instance_id)
    assert completed["status"] == TaskStatus.SUCCESS
    assert not {"started_at", "finished_at", "query_id"} & completed.keys()


def test_executor_accepts_repository_without_sqlite_specific_methods():
    builder, dag = graph()
    repository = InMemoryExecutionRepository()
    calls = []
    executor = DAGExecutor(dag, lambda instance: calls.append(instance.instance_id),
                           state_store=repository)
    assert executor.state_store is repository
    assert executor.repository is repository
    for _ in range(2):
        executor.execute([builder.create_instance(dag, "root", NOW)], now=NOW)
    assert calls == [builder.create_instance(dag, "root", NOW).instance_id]


def test_executor_restores_success_known_only_to_the_repository():
    builder, dag = graph()
    instance = builder.create_instance(dag, "root", NOW)
    repository = InMemoryExecutionRepository({instance.instance_id: TaskStatus.SUCCESS})
    calls = []
    executor = DAGExecutor(dag, lambda item: calls.append(item.instance_id),
                           state_store=repository)
    report = executor.execute([instance], now=NOW)
    assert calls == []
    assert report.succeeded == [instance]


def test_memory_preserves_shared_statuses_and_legacy_metadata():
    builder, dag = graph()
    known_statuses = {}
    observed = []
    executor = DAGExecutor(dag, lambda instance: observed.append(instance.query_id),
                           known_statuses=known_statuses)
    original = builder.create_instance(dag, "root", NOW)
    original.query_id = "caller-query"
    executor.execute([original], now=NOW)
    restored = builder.create_instance(dag, "root", NOW)
    executor.execute([restored], now=NOW)
    assert observed == ["caller-query"]
    assert known_statuses[original.instance_id] == TaskStatus.SUCCESS
    assert executor._memory[original.instance_id]["status"] == TaskStatus.SUCCESS
    assert restored.query_id is restored.started_at is restored.finished_at is None


def test_memory_retry_restores_failure_and_does_not_create_query_id():
    builder, dag = graph()
    observed = []

    def fail(instance):
        observed.append((instance.error, instance.query_id, instance.started_at))
        raise TaskExecutionError("UNSENT", retryable=True)

    executor = DAGExecutor(dag, fail, retry_delay_seconds=30)
    executor._now = lambda instance: NOW
    for seconds in (0, 30):
        executor.execute([builder.create_instance(dag, "root", NOW)],
                         now=NOW + timedelta(seconds=seconds))
    assert observed == [(None, None, NOW), ("UNSENT", None, NOW + timedelta(seconds=30))]


def test_failed_repository_commit_escapes_and_leaves_claim_running(monkeypatch):
    builder, dag = graph()
    repository = InMemoryExecutionRepository()
    executor = DAGExecutor(dag, lambda instance: None, state_store=repository)

    def fail_commit(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(repository, "finish", fail_commit)
    instance = builder.create_instance(dag, "root", NOW)
    with pytest.raises(OSError, match="disk full"):
        executor.execute([instance], now=NOW)
    assert repository.get_instance(instance.instance_id)["status"] == TaskStatus.RUNNING
