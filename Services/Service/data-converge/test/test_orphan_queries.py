"""BUG-1/BUG-3 regressions found in the dual-instance lab (docs/evidence/dual-instance-lab-2026-11-06.md).

BUG-1: an interrupted window must not rerun while the previous leader's query may
still be writing; rerun waits until ClickHouse confirms that query stopped.
BUG-3: a backfill dispatch is published as RUNNING so a takeover treats it as interrupted.
"""
from contextlib import closing
from datetime import timedelta

from data_converge.dag import DAGExecutor, TaskStatus
from data_converge.scheduler.orphans import HOLD_REASON, OrphanQueryGuard
from data_converge.state.shared import _take_over

from .test_mirror_recovery import FakeMirror, attach, pending_instance
from .test_state import NOW, graph, open_store


class RunningMirror(FakeMirror):
    def running_instances(self):
        return [row | {'instance_id': key} for key, row in self.rows.items() if row.get('status') == 'RUNNING']


def interrupted_takeover(tmp_path):
    builder, dag = graph()
    instance = pending_instance(builder, dag)
    mirror = RunningMirror()
    store = open_store(tmp_path / 'state.db')
    attach(store, mirror)
    store.enqueue_and_advance([instance], {})
    mirror.rows[instance.instance_id] |= {'status': 'RUNNING', 'query_id': 'old-query'}
    mirror.cursors = {'root': NOW}
    _take_over(store, mirror, lambda _ids: (_ for _ in ()).throw(OSError('clickhouse down')))
    return dag, instance, store


def guarded_executor(dag, store, calls, stopper):
    executor = DAGExecutor(dag, lambda item: calls.append(item.instance_id), state_store=store)
    guard = OrphanQueryGuard(store, stopper, retry_seconds=0)
    executor.orphan_check = guard.reason
    return executor, guard


def test_failed_kill_holds_rerun_until_clickhouse_confirms(tmp_path):
    dag, instance, store = interrupted_takeover(tmp_path)
    with closing(store):
        assert store.held_orphans() == {instance.instance_id: 'old-query'}
        calls = []

        def unreachable(_ids):
            raise OSError('clickhouse down')
        executor, guard = guarded_executor(dag, store, calls, unreachable)
        assert guard() == 0
        report = executor.execute([instance], now=NOW + timedelta(minutes=5))
        assert calls == [] and report.blocked[0].error == f'{HOLD_REASON}:old-query'
        executor, guard = guarded_executor(dag, store, calls, lambda ids: set(ids))
        assert guard() == 0   # still running in ClickHouse: keep holding
        executor.execute([instance], now=NOW + timedelta(minutes=5))
        assert calls == []
        executor, guard = guarded_executor(dag, store, calls, lambda _ids: set())
        assert guard() == 1 and store.held_orphans() == {}
        executor.execute([instance], now=NOW + timedelta(minutes=5))
        assert calls == [instance.instance_id]
        assert store.get_status(instance.instance_id) == TaskStatus.SUCCESS


def test_backfill_cannot_start_while_window_is_held(tmp_path):
    _dag, instance, store = interrupted_takeover(tmp_path)
    with closing(store):
        result = store.begin_backfill('bf:1', instance, recompute=True, now=NOW + timedelta(minutes=5))
        assert result['status'] == 'BLOCKED' and result['error'] == 'BACKFILL_PREVIOUS_QUERY_RUNNING'


def test_backfill_dispatch_is_published_as_running(tmp_path):
    builder, dag = graph()
    instance = pending_instance(builder, dag)
    mirror = FakeMirror()
    with closing(open_store(tmp_path / 'state.db')) as store:
        attach(store, mirror)
        DAGExecutor(dag, lambda _item: None, state_store=store).execute([instance], now=NOW)
        assert mirror.rows[instance.instance_id]['status'] == 'SUCCESS'
        result = store.begin_backfill('bf:2', instance, recompute=True, now=NOW + timedelta(minutes=5))
        assert result['status'] == 'RUNNING'
        shared = mirror.rows[instance.instance_id]
        assert shared['status'] == 'RUNNING' and shared['query_id'] == result['query_id']


def test_local_restart_publishes_interrupted_backfill(tmp_path):
    builder, dag = graph()
    instance = pending_instance(builder, dag)
    mirror = FakeMirror()
    with closing(open_store(tmp_path / 'state.db')) as store:
        attach(store, mirror)
        store.begin_backfill('bf:3', instance, recompute=True, now=NOW)
    with closing(open_store(tmp_path / 'state.db')) as store:
        attach(store, mirror)
        store.recover_running()
        assert store.get_status(instance.instance_id) == TaskStatus.UNKNOWN
    assert mirror.rows[instance.instance_id]['status'] == 'UNKNOWN'


def fake_clickhouse(monkeypatch, responses):
    """Replace execute_sql: answer SELECT with the next running set, KILL with an optional error."""
    from data_converge.sql_execution import client

    sent = []

    def execute(_config, sql, timeout=30, **_kwargs):
        sent.append(sql.split()[0])
        reply = responses.pop(0)
        if isinstance(reply, Exception):
            raise reply
        return [[value] for value in reply]
    monkeypatch.setattr(client, 'execute_sql', execute)
    return client, sent


def test_stop_queries_skips_kill_when_nothing_runs(monkeypatch):
    client, sent = fake_clickhouse(monkeypatch, [[]])
    assert client.stop_queries({}, ['old-query', "bad'id"]) == set()
    assert sent == ['SELECT']


def test_stop_queries_kills_and_reports_survivors(monkeypatch):
    client, sent = fake_clickhouse(monkeypatch, [['old-query'], [], ['old-query']])
    assert client.stop_queries({}, ['old-query']) == {'old-query'}
    assert sent == ['SELECT', 'KILL', 'SELECT']


def test_stop_queries_tolerates_nothing_to_kill_race(monkeypatch):
    from data_converge.sql_execution.client import SQLExecutionError

    race = SQLExecutionError('CLICKHOUSE_HTTP_FAILED', diagnostic={'clickhouse_exception_code': 785})
    client, _sent = fake_clickhouse(monkeypatch, [['old-query'], race, []])
    assert client.stop_queries({}, ['old-query']) == set()


def test_stop_queries_raises_when_unconfirmed(monkeypatch):
    import pytest
    from data_converge.sql_execution.client import SQLExecutionError

    client, _sent = fake_clickhouse(monkeypatch, [SQLExecutionError('CLICKHOUSE_CONNECT_FAILED')])
    with pytest.raises(SQLExecutionError):
        client.stop_queries({}, ['old-query'])
