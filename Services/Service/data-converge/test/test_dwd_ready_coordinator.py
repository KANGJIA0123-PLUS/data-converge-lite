from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

from data_converge.dwd_ready.coordinator import ReadyCoordinator
from data_converge.dwd_ready.message import DWD_TYPES
from data_converge.dwd_ready.redis_source import RedisSignalError
from data_converge.state import StateStore


START = datetime.fromisoformat('2026-10-12T10:00:00+08:00')
NOW = START + timedelta(minutes=6)


class Source:
    def __init__(self, messages=()):
        self.messages = list(messages)
        self.pending = None
        self.acknowledged = []
        self.fail_ack = False
        self.fail_receive = False
        self.closed = False

    def receive(self, block=True):
        if self.fail_receive:
            raise RedisSignalError('DWD_REDIS_UNAVAILABLE')
        if self.pending is None and self.messages:
            self.pending = self.messages.pop(0)
        return self.pending

    def ack(self, raw):
        if self.fail_ack:
            raise RedisSignalError('DWD_REDIS_UNAVAILABLE')
        assert raw == self.pending
        self.acknowledged.append(raw)
        self.pending = None

    def close(self):
        self.closed = True


def signals(start=START):
    return [f'{start:%Y-%m-%dT%H:%M}:{kind}' for kind in sorted(DWD_TYPES)]


@pytest.fixture
def store(tmp_path):
    store = StateStore(tmp_path / 'state.sqlite3', create=True)
    store.acquire_lock()
    yield store
    store.close()


def coordinator(store, source):
    return ReadyCoordinator(store, source, START, batch_size=100, wait_warning_seconds=60)


def instance():
    return SimpleNamespace(task=SimpleNamespace(aggregation_interval='PT5M'),
                           data_start=START, data_end=START + timedelta(minutes=5))


def test_five_signals_release_exact_window_only_after_persistence(store):
    source = Source(signals()[:4])
    ready = coordinator(store, source)
    assert ready.poll(NOW) == []
    assert ready.reason(instance(), NOW).startswith('DWD_NOT_READY:')
    source.messages.extend(signals()[4:])
    assert ready.poll(NOW) == [START]
    assert ready.reason(instance(), NOW) is None
    assert store.get_dwd_types(START) == set(DWD_TYPES)
    assert len(source.acknowledged) == 5


def test_commit_failure_does_not_ack_or_release(store, monkeypatch):
    source = Source(signals())
    ready = coordinator(store, source)

    def fail(*_):
        raise RuntimeError('disk failure')

    monkeypatch.setattr(store, 'record_dwd_signal', fail)
    with pytest.raises(RuntimeError):
        ready.poll(NOW)
    assert source.pending == signals()[0]
    assert source.acknowledged == []


def test_ack_failure_replays_durable_fact_after_restart(store):
    source = Source(signals())
    source.fail_ack = True
    ready = coordinator(store, source)
    assert ready.poll(NOW) == []
    assert len(store.get_dwd_types(START)) == 1
    source.fail_ack = False
    reopened = coordinator(store, source)
    assert reopened.poll(NOW) == [START]
    assert len(store.get_dwd_types(START)) == 5
    assert len(source.acknowledged) == 5


def test_missing_or_zero_signals_timeout_warns_and_never_releases(store, caplog):
    ready = coordinator(store, Source())
    assert ready.reason(instance(), NOW - timedelta(seconds=1))
    assert not caplog.records
    assert ready.reason(instance(), NOW)
    assert 'DWD_WAIT_TIMEOUT' in caplog.text
    assert ready.reason(instance(), NOW + timedelta(days=1))
    assert ready.poll(NOW + timedelta(days=1)) == []


def test_redis_outage_does_not_discard_already_ready_window(store, caplog):
    source = Source(signals())
    ready = coordinator(store, source)
    assert ready.poll(NOW) == [START]
    source.fail_receive = True
    assert ready.poll(NOW) == []
    assert ready.reason(instance(), NOW) is None
    assert 'DWD_REDIS_UNAVAILABLE' in caplog.text


def test_poison_old_and_future_messages_do_not_release_and_are_acknowledged(store, caplog):
    messages = ['secret-in-a-malformed-message', *signals(START - timedelta(minutes=5)),
                *signals(START + timedelta(minutes=10))]
    source = Source(messages)
    assert coordinator(store, source).poll(NOW) == []
    assert not store.get_dwd_types(START)
    assert len(source.acknowledged) == len(messages)
    assert 'secret-in-a-malformed-message' not in caplog.text


def test_bounded_batch_and_stop_do_not_consume_more(store):
    source = Source(signals())
    ready = ReadyCoordinator(store, source, START, batch_size=2, wait_warning_seconds=60)
    assert ready.poll(NOW) == []
    assert len(source.acknowledged) == 2
    ready.should_stop = lambda: True
    assert ready.poll(NOW) == []
    assert len(source.acknowledged) == 2
    ready.close()
    assert source.closed


def test_blocking_receive_across_boundary_uses_arrival_time(store, monkeypatch):
    import data_converge.dwd_ready.coordinator as module
    monotonic = [0.0]
    source = Source(signals())
    receive = source.receive

    def across_boundary(block=True):
        monotonic[0] = 0.6
        return receive(block)

    source.receive = across_boundary
    monkeypatch.setattr(module.time, 'monotonic', lambda: monotonic[0])
    before_end = START + timedelta(minutes=5, milliseconds=-500)
    assert coordinator(store, source).poll(before_end) == [START]
    assert store.get_dwd_types(START) == set(DWD_TYPES)


def test_extreme_future_timestamp_is_poison_not_a_service_crash(store):
    source = Source(['9999-12-31T23:55:GPU_REALTIME'])
    assert coordinator(store, source).poll(NOW) == []
    assert len(source.acknowledged) == 1
