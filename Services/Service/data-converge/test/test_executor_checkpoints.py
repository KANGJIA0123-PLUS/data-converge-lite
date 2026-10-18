"""Dispatch must not cross an uncommitted checkpoint or a stop request."""
from contextlib import closing
from datetime import timedelta

import pytest

from data_converge.dag import DAGExecutor, TaskStatus
from data_converge.state import StateStore

from .test_state import NOW, graph, open_store


def test_dispatch_commit_failure_prevents_runner_call(tmp_path, monkeypatch):
    builder, dag = graph()
    calls = []
    with closing(open_store(tmp_path / 'state.db')) as store:
        def reject_dispatch(*args, **kwargs):
            raise OSError('checkpoint commit failed')

        monkeypatch.setattr(store, 'mark_dispatched', reject_dispatch, raising=False)
        executor = DAGExecutor(dag, lambda item: calls.append(item.query_id), state_store=store)
        instance = builder.create_instance(dag, 'root', NOW)
        with pytest.raises(OSError, match='checkpoint commit failed'):
            executor.execute([instance], now=NOW)
        assert calls == []
        assert store.get_instance(instance.instance_id)['attempts'] == 0
        assert store.recover_running() == 1
        assert store.get_status(instance.instance_id) == TaskStatus.PENDING


def test_runner_observes_committed_attempt_and_success_is_not_resubmitted(tmp_path):
    builder, dag = graph()
    observed = []
    with closing(open_store(tmp_path / 'state.db')) as store:
        def receive(instance):
            with closing(StateStore(store.path)) as reader:
                row = reader.get_instance(instance.instance_id)
                history = reader.list_execution_attempts(instance.instance_id)
                observed.append((row['status'], row['attempts'], history[0]['query_id']))

        executor = DAGExecutor(dag, receive, state_store=store)
        first = builder.create_instance(dag, 'root', NOW)
        executor.execute([first], now=NOW)
        executor.execute([builder.create_instance(dag, 'root', NOW)], now=NOW)
        assert observed == [('RUNNING', 1, first.query_id)]
        assert store.get_status(first.instance_id) == TaskStatus.SUCCESS


def test_stop_after_prepare_leaves_safe_reservation_without_consuming_budget(tmp_path, monkeypatch):
    builder, dag = graph()
    calls, stopped = [], []
    with closing(open_store(tmp_path / 'state.db')) as store:
        prepare = store.prepare_attempt

        def prepare_then_stop(*args, **kwargs):
            claimed = prepare(*args, **kwargs)
            stopped.append(True)
            return claimed

        monkeypatch.setattr(store, 'prepare_attempt', prepare_then_stop)
        executor = DAGExecutor(dag, lambda item: calls.append(item.query_id), state_store=store)
        instance = builder.create_instance(dag, 'root', NOW)
        executor.execute([instance], now=NOW, should_stop=lambda: bool(stopped))
        assert calls == []
        assert store.get_instance(instance.instance_id)['attempts'] == 0
        assert store.recover_running() == 1
        assert store.get_status(instance.instance_id) == TaskStatus.PENDING


def test_stop_between_submissions_finishes_inflight_and_preserves_unsubmitted(tmp_path):
    builder, dag = graph()
    calls = []
    decisions = iter([False, False, True, True])
    with closing(open_store(tmp_path / 'state.db')) as store:
        instances = [builder.create_instance(dag, 'root', NOW + timedelta(minutes=n)) for n in (0, 5)]
        executor = DAGExecutor(dag, lambda item: calls.append(item.instance_id),
                               state_store=store, max_concurrent_tasks=2)
        executor.execute(instances, now=NOW + timedelta(minutes=5),
                         should_stop=lambda: next(decisions, True))
        assert calls == [instances[0].instance_id]
        assert store.get_status(instances[0].instance_id) == TaskStatus.SUCCESS
        assert store.get_instance(instances[1].instance_id)['attempts'] == 0
        store.recover_running()
        assert store.get_status(instances[1].instance_id) == TaskStatus.PENDING


def test_lower_retry_limit_does_not_let_recovered_reservation_starve_fresh_work(tmp_path):
    builder, dag = graph()
    old = builder.create_instance(dag, 'root', NOW)
    fresh = builder.create_instance(dag, 'root', NOW + timedelta(minutes=5))
    calls = []
    with closing(open_store(tmp_path / 'state.db')) as store:
        store.enqueue_and_advance([old, fresh], {})
        assert store.claim(old.instance_id, now=NOW)
        store.finish(old.instance_id, TaskStatus.FAILED, retryable=True, now=NOW)
        assert store.prepare_attempt(old.instance_id, now=NOW, max_attempts=2)
        store.recover_running()
        executor = DAGExecutor(dag, lambda item: calls.append(item.instance_id),
                               state_store=store, max_attempts=1)
        for _ in range(3):
            executor.execute([old, fresh], now=NOW + timedelta(minutes=5), max_executions=1)
        assert calls == [fresh.instance_id]
        assert store.get_instance(old.instance_id)['attempts'] == 1
        assert store.get_status(fresh.instance_id) == TaskStatus.SUCCESS
