"""Shared Redis state: fencing, period aging, takeover import and rerun.

Uses fakeredis (with Lua) when installed; skipped otherwise, so the product
keeps no extra runtime dependency.
"""

from contextlib import closing
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from data_converge.dag import DAGExecutor, TaskStatus
from data_converge.state.redis_mirror import (
    FencedOut, RedisStateMirror, RedisStateSettings,
)
from data_converge.state.shared import _take_over

from .test_state import NOW, graph, open_store

fakeredis = pytest.importorskip('fakeredis')
pytest.importorskip('lupa')

LEADER = 'data_converge:leader:{test}'


def make_mirror(server, token, settings=None):
    client = fakeredis.FakeStrictRedis(server=server)
    return RedisStateMirror(client, client, cluster='test', leader_key=LEADER,
                            token_source=lambda: token[0],
                            settings=settings or RedisStateSettings())


def attach(store, mirror):
    store.attach_mirror(mirror, {'root': 'PT5M'})


def test_settings_follow_design_defaults_and_ranges():
    settings = RedisStateSettings.from_mapping(None)
    assert settings.ttl == {'PT5M': 1, 'PT1H': 1, 'P1D': 3, 'P1M': 3}
    end = datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert settings.expires_at('P1M', end) == end + timedelta(days=93)
    for bad in ({'ttl': {'5min': 8}}, {'ttl': {'day': 11}}, {'ttl': {'month': 13}}, {'x': 1}):
        with pytest.raises(ValueError, match='REDIS_STATE_CONFIG_INVALID'):
            RedisStateSettings.from_mapping(bad)


def test_success_is_published_with_ttl_from_window_end(tmp_path):
    server, token = fakeredis.FakeServer(), ['me']
    builder, dag = graph()
    mirror = make_mirror(server, token)
    mirror.write.set(LEADER, 'me')
    with closing(open_store(tmp_path / 'state.db')) as store:
        attach(store, mirror)
        instance = builder.create_instance(dag, 'root', NOW)
        DAGExecutor(dag, lambda _i: None, state_store=store).execute([instance], now=NOW)
        row = mirror.read_instance(instance.instance_id)
        assert row['status'] == 'SUCCESS'
        expected = max((instance.data_end + timedelta(days=1)).timestamp(),
                       datetime.now(timezone.utc).timestamp() + 6 * 3600)
        assert abs(mirror.write.pexpiretime(mirror.instance_key(instance.instance_id)) / 1000
                   - expected) < 5
        assert not mirror.running_instances()


def test_pending_instances_are_published_for_takeover(tmp_path):
    server, token = fakeredis.FakeServer(), ['leader']
    builder, dag = graph()
    mirror = make_mirror(server, token)
    mirror.write.set(LEADER, 'leader')
    with closing(open_store(tmp_path / 'state.db')) as store:
        attach(store, mirror)
        pending = builder.create_instance(dag, 'root', NOW)
        store.enqueue_and_advance([pending], {'root': NOW + timedelta(minutes=5)})
        row = mirror.read_instance(pending.instance_id)
    assert row is not None and row['status'] == 'PENDING'


def test_takeover_kills_real_running_query_when_local_is_pending(tmp_path):
    from data_converge.state.shared import _take_over

    server, token = fakeredis.FakeServer(), ['leader']
    builder, dag = graph()
    mirror = make_mirror(server, token)
    mirror.write.set(LEADER, 'leader')
    instance = builder.create_instance(dag, 'root', NOW)
    killed = []
    with closing(open_store(tmp_path / 'state.db')) as store:
        attach(store, mirror)
        store.enqueue_and_advance([instance], {})
        row = store.get_instance(instance.instance_id)
        row.update(status='RUNNING', query_id='old-query')
        mirror.write_instance(row, 'PT5M')
        mirror.write_cursors({'root': NOW})
        _take_over(store, mirror, killed.extend)
    assert killed == ['old-query']
    assert mirror.read_instance(instance.instance_id)['status'] == 'PENDING'
    assert not mirror.running_instances()


def test_takeover_kills_real_running_query_before_local_success(tmp_path):
    from data_converge.state.shared import _take_over

    server, token = fakeredis.FakeServer(), ['leader']
    builder, dag = graph()
    mirror = make_mirror(server, token)
    mirror.write.set(LEADER, 'leader')
    instance = builder.create_instance(dag, 'root', NOW)
    killed = []
    with closing(open_store(tmp_path / 'state.db')) as store:
        attach(store, mirror)
        DAGExecutor(dag, lambda _item: None, state_store=store).execute([instance], now=NOW)
        row = store.get_instance(instance.instance_id)
        row.update(status='RUNNING', query_id='old-query')
        mirror.write_instance(row, 'PT5M')
        mirror.write_cursors({'root': NOW})
        _take_over(store, mirror, killed.extend)
        assert store.mirror_conflicts()[0]['shared_status'] == 'RUNNING'
    assert killed == ['old-query']
    assert mirror.read_instance(instance.instance_id)['status'] == 'SUCCESS'
    assert not mirror.running_instances()


def test_recent_window_ages_from_window_end_not_write_time():
    server, token = fakeredis.FakeServer(), ['me']
    mirror = make_mirror(server, token)
    mirror.write.set(LEADER, 'me')
    end = (datetime.now(timezone.utc) + timedelta(days=1)).replace(microsecond=0)
    mirror.write_instance({'instance_id': 'day@x', 'status': 'SUCCESS', 'taskname': 'day',
                           'data_end': end.isoformat()}, 'P1D')
    assert abs(mirror.write.pexpiretime(mirror.instance_key('day@x')) / 1000
               - (end + timedelta(days=3)).timestamp()) < 2


def test_deposed_leader_cannot_overwrite_shared_state(tmp_path):
    server, token = fakeredis.FakeServer(), ['old']
    builder, dag = graph()
    mirror = make_mirror(server, token)
    mirror.write.set(LEADER, 'new-leader')
    with closing(open_store(tmp_path / 'state.db')) as store:
        attach(store, mirror)
        instance = builder.create_instance(dag, 'root', NOW)
        with pytest.raises(FencedOut):
            DAGExecutor(dag, lambda _i: None, state_store=store).execute([instance], now=NOW)
        assert mirror.read_instance(instance.instance_id) is None


def test_standby_takeover_skips_success_and_reruns_interrupted(tmp_path):
    server = fakeredis.FakeServer()
    builder, dag = graph()
    done = builder.create_instance(dag, 'root', NOW)
    cut = builder.create_instance(dag, 'root', NOW + timedelta(minutes=5))
    first = make_mirror(server, ['a'])
    first.write.set(LEADER, 'a')
    with closing(open_store(tmp_path / 'a.db')) as store:
        attach(store, first)
        DAGExecutor(dag, lambda _i: None, state_store=store).execute([done], now=NOW)
        store.enqueue_and_advance([cut], {'root': NOW + timedelta(minutes=10)})
        assert store.prepare_attempt(cut.instance_id, now=NOW)
        store.mark_dispatched(cut.instance_id, store.get_instance(cut.instance_id)['query_id'])
    first.write.set(LEADER, 'b')  # The old lease expired; the standby won.
    second = make_mirror(server, ['b'])
    calls = []
    with closing(open_store(tmp_path / 'b.db')) as store:
        attach(store, second)
        counts = store.import_shared_state()
        assert counts['REQUEUED'] == 1 and counts['IMPORTED'] == 1 and counts['cursors'] == 1
        assert counts['interrupted_query_ids']
        assert store.get_status(done.instance_id) == TaskStatus.SUCCESS
        assert store.get_cursor('root') == NOW + timedelta(minutes=10)
        DAGExecutor(dag, lambda i: calls.append(i.instance_id), state_store=store).execute(
            [done, cut], now=NOW + timedelta(minutes=10))
    assert calls == [cut.instance_id]
    assert second.read_instance(cut.instance_id)['status'] == 'SUCCESS'


def test_dwd_readiness_survives_takeover(tmp_path):
    server = fakeredis.FakeServer()
    first = make_mirror(server, ['a'])
    first.write.set(LEADER, 'a')
    window = NOW.replace(minute=0)
    with closing(open_store(tmp_path / 'a.db')) as store:
        attach(store, first)
        store.record_dwd_signal(window, 'GPU_REALTIME', NOW)
    first.write.set(LEADER, 'b')
    with closing(open_store(tmp_path / 'b.db')) as store:
        attach(store, make_mirror(server, ['b']))
        assert store.import_shared_state()['dwd'] == 1
        assert store.get_dwd_types(window) == {'GPU_REALTIME'}


def test_first_seed_publishes_local_dwd_facts(tmp_path):
    server = fakeredis.FakeServer()
    mirror = make_mirror(server, ['a'])
    mirror.write.set(LEADER, 'a')
    window = NOW.replace(minute=0)
    with closing(open_store(tmp_path / 'state.db')) as store:
        store.record_dwd_signal(window, 'GPU_REALTIME', NOW)
        store.attach_mirror(mirror, {'root': 'PT5M'})
        _take_over(store, mirror, None)
    assert mirror.read_dwd_types(window) == {'GPU_REALTIME'}


def test_cross_period_parent_past_retention_is_not_missing(tmp_path):
    from data_converge.scheduler.cross_period_wait import _aged_out

    server = fakeredis.FakeServer()
    with closing(open_store(tmp_path / 'state.db')) as store:
        store.attach_mirror(make_mirror(server, ['a']), {'root': 'PT5M'})
        old = (datetime.now(timezone.utc) - timedelta(days=2)).replace(second=0, microsecond=0)
        fresh = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        assert _aged_out(store, f'root@{old.isoformat()}')
        assert not _aged_out(store, f'root@{fresh.isoformat()}')
        assert not _aged_out(SimpleNamespace(), 'root@x')
