"""Mirror write failures: retry, local pending queue, and takeover replay."""

from contextlib import closing
from datetime import timedelta

import pytest

from data_converge.dag import TaskStatus
from data_converge.state.redis_mirror import FencedOut, StateSyncError
from data_converge.state.shared import _take_over

from .test_state import NOW, graph, open_store


class FakeMirror:
    def __init__(self):
        self.rows = {}
        self.cursors = {}
        self.fail = 0
        self.fail_with = None

    def write_instance(self, row, interval):
        if self.fail:
            self.fail -= 1
            raise self.fail_with or StateSyncError('REDIS_STATE_WRITE_FAILED')
        self.rows[row['instance_id']] = dict(row)

    def read_instance(self, instance_id):
        return self.rows.get(instance_id)

    def read_cursors(self):
        return dict(self.cursors)

    def write_cursors(self, cursors):
        self.cursors.update(cursors)


def attach(store, mirror):
    store.attach_mirror(mirror, {'root': 'PT5M'})


def pending_instance(builder, dag, scheduled=NOW):
    return builder.create_instance(dag, 'root', scheduled)


def dispatch(store, instance):
    assert store.prepare_attempt(instance.instance_id, now=NOW)
    store.mark_dispatched(instance.instance_id,
                          store.get_instance(instance.instance_id)['query_id'], now=NOW)


def test_transient_publish_failure_is_retried(tmp_path):
    builder, dag = graph()
    instance = pending_instance(builder, dag)
    mirror = FakeMirror()
    mirror.fail = 1
    with closing(open_store(tmp_path / 'state.db')) as store:
        attach(store, mirror)
        store.enqueue_and_advance([instance], {'root': NOW + timedelta(minutes=5)})
        assert mirror.rows[instance.instance_id]['status'] == 'PENDING'
        assert store.pending_ids() == []


def test_finish_failure_keeps_success_and_replays_on_takeover(tmp_path):
    builder, dag = graph()
    instance = pending_instance(builder, dag)
    mirror = FakeMirror()
    with closing(open_store(tmp_path / 'state.db')) as store:
        attach(store, mirror)
        store.enqueue_and_advance([instance], {})
        dispatch(store, instance)
        mirror.fail = 3
        with pytest.raises(StateSyncError):
            store.finish(instance.instance_id, TaskStatus.SUCCESS, now=NOW)
        assert store.get_status(instance.instance_id) == TaskStatus.SUCCESS
        assert store.pending_ids() == [instance.instance_id]
        assert mirror.rows[instance.instance_id]['status'] == 'RUNNING'
        mirror.fail = 0
        assert store.flush_pending() == 1
        assert mirror.rows[instance.instance_id]['status'] == 'SUCCESS'
        assert store.pending_ids() == []


def test_enqueue_failure_queues_the_whole_batch(tmp_path):
    builder, dag = graph()
    first = pending_instance(builder, dag)
    second = pending_instance(builder, dag, NOW + timedelta(minutes=5))
    mirror = FakeMirror()
    mirror.fail = 3
    with closing(open_store(tmp_path / 'state.db')) as store:
        attach(store, mirror)
        with pytest.raises(StateSyncError):
            store.enqueue_and_advance([first, second], {'root': NOW + timedelta(minutes=10)})
        assert set(store.pending_ids()) == {first.instance_id, second.instance_id}
        assert mirror.cursors == {}
        mirror.fail = 0
        store.flush_pending()
        assert set(mirror.rows) == {first.instance_id, second.instance_id}
        assert mirror.cursors == {}
        assert store.pending_ids() == []


def test_replay_does_not_overwrite_a_newer_terminal(tmp_path):
    builder, dag = graph()
    instance = pending_instance(builder, dag)
    mirror = FakeMirror()
    with closing(open_store(tmp_path / 'state.db')) as store:
        attach(store, mirror)
        store.enqueue_and_advance([instance], {})
        dispatch(store, instance)
        mirror.fail = 3
        with pytest.raises(StateSyncError):
            store.finish(instance.instance_id, TaskStatus.SUCCESS, now=NOW)
        mirror.rows[instance.instance_id] = {'status': 'FAILED', 'query_id': 'new-leader'}
        mirror.fail = 0
        assert store.flush_pending() == 0
        assert mirror.rows[instance.instance_id] == {'status': 'FAILED', 'query_id': 'new-leader'}
        assert store.pending_ids() == []


def test_fenced_out_is_not_queued(tmp_path):
    builder, dag = graph()
    instance = pending_instance(builder, dag)
    mirror = FakeMirror()
    mirror.fail = 1
    mirror.fail_with = FencedOut('REDIS_STATE_FENCED')
    with closing(open_store(tmp_path / 'state.db')) as store:
        attach(store, mirror)
        with pytest.raises(FencedOut):
            store.enqueue_and_advance([instance], {})
        assert store.pending_ids() == []


def test_existing_ledger_upgrades_mirror_pending(tmp_path):
    import sqlite3

    path = tmp_path / 'state.db'
    with closing(open_store(path)) as store:
        store.set_meta('coverage_start', NOW.isoformat())
    with sqlite3.connect(path) as connection:
        connection.execute('DROP TABLE IF EXISTS mirror_pending')
    with closing(open_store(path, create=False)) as store:
        assert store.pending_ids() == []


def test_takeover_flushes_pending_before_scheduling(tmp_path, monkeypatch):
    builder, dag = graph()
    instance = pending_instance(builder, dag)
    mirror = FakeMirror()
    with closing(open_store(tmp_path / 'state.db')) as store:
        attach(store, mirror)
        store.enqueue_and_advance([instance], {})
        dispatch(store, instance)
        mirror.fail = 3
        with pytest.raises(StateSyncError):
            store.finish(instance.instance_id, TaskStatus.SUCCESS, now=NOW)
        mirror.fail = 0
        mirror.cursors = {'root': NOW}
        monkeypatch.setattr(store, 'import_shared_state',
                            lambda: {'interrupted_query_ids': []})
        _take_over(store, mirror, None)
        assert mirror.rows[instance.instance_id]['status'] == 'SUCCESS'
        assert store.pending_ids() == []


def test_finish_writes_ahead_before_mirroring(tmp_path):
    class CrashingMirror(FakeMirror):
        crash = False

        def write_instance(self, row, interval):
            if self.crash:
                raise SystemExit('process died before the mirror write')
            super().write_instance(row, interval)

    builder, dag = graph()
    instance = pending_instance(builder, dag)
    mirror = CrashingMirror()
    with closing(open_store(tmp_path / 'state.db')) as store:
        attach(store, mirror)
        store.enqueue_and_advance([instance], {})
        dispatch(store, instance)
        mirror.crash = True
        with pytest.raises(SystemExit):
            store.finish(instance.instance_id, TaskStatus.SUCCESS, now=NOW)
        assert store.get_status(instance.instance_id) == TaskStatus.SUCCESS
        assert store.pending_ids() == [instance.instance_id]
