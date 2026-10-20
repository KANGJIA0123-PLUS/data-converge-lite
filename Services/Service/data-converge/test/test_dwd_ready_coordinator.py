from datetime import datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo
import subprocess
import sys
import shlex

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


def test_readiness_core_does_not_load_redis_adapter():
    result = subprocess.run([sys.executable, '-c', '''
import sys
from data_converge.dwd_ready.coordinator import ReadyCoordinator
assert 'data_converge.dwd_ready.redis_source' not in sys.modules
assert 'data_converge.dwd_ready.redis_auth' not in sys.modules
assert 'data_converge.dwd_ready.psm_crypto' not in sys.modules
'''], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize('operation', ['receive', 'ack'])
def test_source_port_failure_preserves_commit_before_ack(store, caplog, operation):
    from data_converge.dwd_ready.ports import SignalSourceError

    class AlternateSource(Source):
        def receive(self, block=True):
            if operation == 'receive':
                raise SignalSourceError('private source details')
            return super().receive(block)

        def ack(self, raw):
            raise SignalSourceError('private acknowledgement details')

    source = AlternateSource(signals())
    assert coordinator(store, source).poll(NOW) == []
    assert len(store.get_dwd_types(START)) == (1 if operation == 'ack' else 0)
    assert source.acknowledged == []
    assert 'private' not in caplog.text


def test_system_clock_in_repeated_hour_preserves_signal_arrival_instant(store, monkeypatch):
    import data_converge.dwd_ready.coordinator as module
    monkeypatch.setattr(module.time, 'monotonic', lambda: 0.0)
    start = datetime.fromisoformat('2026-11-01T14:20:00+08:00')
    now = datetime(2026, 11, 1, 1, 30, tzinfo=ZoneInfo('America/New_York'), fold=1)
    source = Source(signals(start))
    ready = ReadyCoordinator(store, source, start, wait_warning_seconds=60)
    assert ready.poll(now) == [start]
    assert store.get_dwd_types(start) == set(DWD_TYPES)
    assert len(source.acknowledged) == 5


def test_wait_warning_uses_elapsed_seconds_across_repeated_hour(store, caplog):
    zone = ZoneInfo('America/New_York')
    start = datetime(2026, 11, 1, 1, 55, tzinfo=zone)
    end = datetime(2026, 11, 1, 1, 0, tzinfo=zone, fold=1)
    window = SimpleNamespace(task=SimpleNamespace(aggregation_interval='PT5M'),
                             data_start=start, data_end=end)
    ready = ReadyCoordinator(store, Source(), start, wait_warning_seconds=60)
    before = datetime(2026, 11, 1, 1, 59, 59, tzinfo=zone)
    assert ready.reason(window, before)
    assert 'DWD_WAIT_TIMEOUT' not in caplog.text
    assert ready.reason(window, before.replace(minute=1, second=0, fold=1))
    assert 'DWD_WAIT_TIMEOUT' in caplog.text
    assert 'waited_seconds=60' in caplog.text
    assert 'window_end=2026-11-01T01:00:00-05:00' in caplog.text


@pytest.mark.parametrize('received,received_text,missing_text', [
    ([], 'NONE', 'GPU_REALTIME,MODELINSTANCE_REALTIME,MODELMETRIC_REALTIME,NODE_REALTIME,REQUEST_REALTIME'),
    (['NODE_REALTIME', 'GPU_REALTIME'], 'GPU_REALTIME,NODE_REALTIME',
     'MODELINSTANCE_REALTIME,MODELMETRIC_REALTIME,REQUEST_REALTIME'),
])
def test_timeout_warning_explains_local_evidence_without_debug(store, caplog, received, received_text, missing_text):
    for kind in received:
        store.record_dwd_signal(START, kind, NOW)
    ready = ReadyCoordinator(store, Source(), START, wait_warning_seconds=90)
    reason = ready.reason(instance(), NOW + timedelta(seconds=35))
    assert reason == 'DWD_NOT_READY:' + missing_text
    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == 'WARNING'
    message = caplog.records[0].getMessage()
    assert 'window_start=2026-10-12T10:00:00+08:00' in message
    assert 'window_end=2026-10-12T10:05:00+08:00' in message
    assert 'waited_seconds=95' in message
    assert 'timeout_seconds=90' in message
    assert f'received={received_text}' in message
    assert f'missing={missing_text}' in message
    assert 'local readiness evidence is incomplete' in message
    assert 'does not prove upstream data is absent' in message
    assert 'execution remains blocked' in message


def test_timeout_warning_provides_valid_readonly_diagnostic_arguments(store, caplog):
    from data_converge.main import arguments

    coordinator(store, Source()).reason(instance(), NOW)
    message = caplog.records[0].getMessage()
    assert 'diagnostic_args="' in message
    diagnostic_args = message.split('diagnostic_args="', 1)[1].split('"', 1)[0]
    args = arguments(shlex.split(diagnostic_args))
    assert args.dwd_status is True
    assert args.window == '2026-10-12T10:00:00+08:00'
    assert args.format == 'json'
    assert 'same service launcher and --config' in message


def test_timeout_warning_remains_rate_limited_per_window(store, caplog):
    ready = coordinator(store, Source())
    assert ready.reason(instance(), NOW)
    assert ready.reason(instance(), NOW)
    assert ready.reason(instance(), NOW + timedelta(seconds=59))
    assert len(caplog.records) == 1
    assert ready.reason(instance(), NOW + timedelta(seconds=60))
    assert len(caplog.records) == 2
    for kind in DWD_TYPES:
        store.record_dwd_signal(START, kind, NOW)
    assert ready.reason(instance(), NOW + timedelta(seconds=120)) is None
    assert len(caplog.records) == 2
