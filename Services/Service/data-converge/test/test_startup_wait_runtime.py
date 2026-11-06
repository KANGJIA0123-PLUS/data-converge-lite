"""Exercise dependency startup waiting through real HTTP and service lifecycles."""

import signal
import threading
from contextlib import closing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest
import yaml

from data_converge import main as entry, runtime, startup
from data_converge.application import Application
from data_converge.leader_election import LeadershipError
from data_converge.state import StateStore

from .test_dwd_ready_coordinator import Source
from .test_main import app, enable_ready
from .test_writer_coordination import Lease, enable_leadership, leased


class StartupClock:
    def __init__(self):
        self.now = 0
        self.delays = []
        self.after_sleep = None

    def sleep(self, seconds):
        self.now += seconds
        self.delays.append(seconds)
        callback, self.after_sleep = self.after_sleep, None
        if callback is not None:
            callback()


@pytest.fixture
def startup_clock(monkeypatch):
    clock = StartupClock()
    monkeypatch.setattr(startup, 'monotonic', lambda: clock.now)
    monkeypatch.setattr(startup, 'sleep', clock.sleep)
    return clock


def health_snapshot(application):
    return (application.check_health('liveness').exit_code,
            application.check_health('readiness').exit_code)


def startup_handler(state):
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = self.rfile.read(int(self.headers['Content-Length'])).decode()
            state.requests.append((self.path, body))
            state.health.append(health_snapshot(state.application))
            missing = len(state.requests) <= state.failures
            payload = (b'Code: 81. Database example does not exist. (UNKNOWN_DATABASE)'
                       if missing else b'{"data":[[1]]}')
            self.send_response(404 if missing else 200)
            if missing:
                self.send_header('X-ClickHouse-Exception-Code', '81')
            self.send_header('Content-Length', str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *_):
            pass

    return Handler


@pytest.fixture
def waiting_http_app(app, monkeypatch):
    state = SimpleNamespace(requests=[], health=[], failures=2, application=None)
    server = ThreadingHTTPServer(('127.0.0.1', 0), startup_handler(state))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    config = yaml.safe_load(app.read_text())
    config['clickhouse'].pop('password_file')
    config['clickhouse'].update(port=server.server_port, password_env='LITE_TEST_PASSWORD')
    config['startup'] = dict(timeout_seconds=6, initial_retry_delay_seconds=1,
                             max_retry_delay_seconds=2, probe_timeout_seconds=1)
    app.write_text(yaml.safe_dump(config))
    monkeypatch.setenv('LITE_TEST_PASSWORD', 'synthetic-integration-secret')
    state.application = Application(app)
    try:
        yield app, state
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def assert_business_database_probes(requests):
    assert requests
    for path, body in requests:
        assert body == 'SELECT 1'
        assert parse_qs(urlsplit(path).query)['database'] == ['example']


def assert_runtime_resources_closed(path, previous):
    assert not list(path.parent.rglob('heartbeat.json'))
    assert all(signal.getsignal(signum) is handler for signum, handler in previous.items())
    with closing(StateStore(path.parent / 'state/state.sqlite3')) as store:
        store.acquire_lock()


def test_missing_business_database_waits_until_ready_before_starting_once(
        waiting_http_app, startup_clock, monkeypatch):
    path, state = waiting_http_app
    started = []
    monkeypatch.setattr(runtime.TaskScheduler, 'start',
                        lambda self: started.append(health_snapshot(state.application)))

    assert entry.main(['--config', str(path)]) == 0

    assert len(state.requests) == 3
    assert_business_database_probes(state.requests)
    assert state.health == [(0, 1), (0, 1), (0, 1)]
    assert startup_clock.delays
    assert started == [(0, 0)]
    assert health_snapshot(state.application) == (1, 1)


@pytest.mark.parametrize('signum', [signal.SIGTERM, signal.SIGINT])
def test_shutdown_signal_during_wait_stops_cleanly_and_releases_resources(
        waiting_http_app, startup_clock, monkeypatch, signum):
    path, state = waiting_http_app
    enable_ready(path)
    source, started = Source(), []
    previous = {item: signal.getsignal(item) for item in (signal.SIGTERM, signal.SIGINT)}
    state.failures = 1000
    state.application = Application(path, dependencies=runtime.RuntimeDependencies(
        source_factory=lambda _: source))
    startup_clock.after_sleep = lambda: signal.raise_signal(signum)
    monkeypatch.setattr(runtime.TaskScheduler, 'start', lambda self: started.append(True))

    assert state.application.serve().exit_code == 0

    assert len(state.requests) == 1
    assert state.health == [(0, 1)]
    assert source.closed and not started
    assert_runtime_resources_closed(path, previous)


def test_lease_loss_during_wait_preserves_failure_and_releases_resources(
        waiting_http_app, startup_clock, monkeypatch):
    path, state = waiting_http_app
    enable_leadership(path)
    lease, source, started = Lease(), Source(), []
    previous = {item: signal.getsignal(item) for item in (signal.SIGTERM, signal.SIGINT)}
    state.failures = 1000
    adapters = runtime.RuntimeDependencies(source_factory=lambda _: source,
                                          leadership_factory=lambda _: leased(lease))
    state.application = Application(path, dependencies=adapters)
    startup_clock.after_sleep = lambda: setattr(lease, 'lost', True)
    monkeypatch.setattr(runtime.TaskScheduler, 'start', lambda self: started.append(True))

    with pytest.raises(LeadershipError, match='LEADERSHIP_LOST'):
        state.application._serve_term(None, state.application.dependencies)

    assert len(state.requests) == 1
    assert state.health == [(0, 1)]
    assert source.closed and lease.closed and not started
    assert_runtime_resources_closed(path, previous)


def test_startup_deadline_returns_failure_without_starting_scheduler(
        waiting_http_app, startup_clock, monkeypatch, caplog):
    path, state = waiting_http_app
    enable_ready(path)
    source, started = Source(), []
    previous = {item: signal.getsignal(item) for item in (signal.SIGTERM, signal.SIGINT)}
    state.failures = 1000
    monkeypatch.setattr(runtime, 'create_source', lambda _: source)
    monkeypatch.setattr(runtime.TaskScheduler, 'start', lambda self: started.append(True))

    assert entry.main(['--config', str(path)]) == 1

    assert len(state.requests) > 1
    assert_business_database_probes(state.requests)
    assert all(health == (0, 1) for health in state.health)
    assert startup_clock.now == pytest.approx(6)
    assert source.closed and not started
    assert 'CLICKHOUSE_STARTUP_TIMEOUT' in caplog.text
    assert_runtime_resources_closed(path, previous)


def test_lease_loss_while_checking_stop_cannot_be_reported_as_clean_shutdown():
    lease, events = Lease(), []

    class Scheduler:
        @property
        def stop_requested(self):
            lease.lost = True
            return True

        def start(self):
            events.append('scheduler_started')

    heartbeat = SimpleNamespace(set_ready=lambda value: events.append(('ready', value)))
    service = runtime.ServiceRuntime(
        Scheduler(), lambda _: events.append('executed'), heartbeat, lease=lease,
        startup_probe=lambda: pytest.fail('a lost lease must not probe ClickHouse'))

    with pytest.raises(LeadershipError, match='LEADERSHIP_LOST'):
        service.serve()

    assert events == []
