"""Exercise runtime composition through public injection seams and real state."""

import json
import signal
from contextlib import contextmanager
from datetime import datetime, timedelta

import pytest

from data_converge import runtime
from data_converge.application import Application
from data_converge.config.application import load_definition
from data_converge.health import Heartbeat
from data_converge.state import StateStore

from .test_dwd_ready_coordinator import NOW, START, Source, signals
from .test_main import app, enable_ready, local_http_app


@pytest.fixture
def inputs(app):
    enable_ready(app)
    definition = load_definition(app)
    directory = app.parent / 'state'
    store = StateStore(directory / 'state.sqlite3', create=True)
    store.acquire_lock()
    try:
        yield app, definition, store, directory
    finally:
        store.close()


def test_injected_execution_and_source_run_real_scheduler_with_duplicate_suppression(inputs):
    seen = []
    source = Source(signals())
    dependencies = runtime.RuntimeDependencies(
        execution_factory=lambda *_: runtime.ExecutionAdapter(seen.append, lambda: None),
        source_factory=lambda _: source,
    )
    with runtime.open_runtime(*inputs, START, dependencies=dependencies) as service:
        first = service.run_once(NOW)
        service.run_once(NOW)
        legacy = runtime.ServiceRuntime(service.scheduler, service.runner, service.heartbeat)
        legacy.run_task('simple', NOW)
        assert len(first.succeeded) == 1
        assert seen == first.succeeded
        assert len(source.acknowledged) == 5
    assert source.closed
    assert not (inputs[3] / 'heartbeat.json').exists()


def test_injected_probe_and_heartbeat_work_with_runner_without_sql_config(inputs):
    now = datetime.now(START.tzinfo)
    start = now.replace(minute=now.minute - now.minute % 5, second=0, microsecond=0)
    start -= timedelta(minutes=5)
    source = Source(signals(start))
    observed = []

    @contextmanager
    def heartbeat_factory(directory):
        with Heartbeat(directory) as heartbeat:
            yield heartbeat
        assert source.closed
        observed.append('closed')

    def execute(_instance):
        observed.append('run')
        assert json.loads((inputs[3] / 'heartbeat.json').read_text())['ready']
        service.request_stop()

    dependencies = runtime.RuntimeDependencies(
        execution_factory=lambda *_: runtime.ExecutionAdapter(
            execute, lambda: observed.append('probe')),
        source_factory=lambda _: source, heartbeat_factory=heartbeat_factory,
    )
    with runtime.open_runtime(*inputs, start, dependencies=dependencies) as service:
        service.serve()
    assert observed == ['probe', 'run', 'closed']


def test_failing_probe_closes_source_heartbeat_and_restores_signals(inputs):
    source = Source()
    previous = {signum: signal.getsignal(signum) for signum in (signal.SIGTERM, signal.SIGINT)}

    def fail_probe():
        raise OSError('connection unavailable')

    dependencies = runtime.RuntimeDependencies(
        execution_factory=lambda *_: runtime.ExecutionAdapter(lambda _: None, fail_probe),
        source_factory=lambda _: source,
    )
    with pytest.raises(OSError, match='connection unavailable'):
        with runtime.open_runtime(*inputs, START, dependencies=dependencies) as service:
            service.serve()
    assert source.closed
    assert not (inputs[3] / 'heartbeat.json').exists()
    assert all(signal.getsignal(signum) is handler for signum, handler in previous.items())


def test_source_binding_change_allows_runtime_and_retains_original_binding(inputs, caplog):
    source = Source()
    source.identity = 'changed-identity'
    inputs[2].set_meta('dwd_redis_sentinel_binding', 'original-identity')
    dependencies = runtime.RuntimeDependencies(
        execution_factory=lambda *_: runtime.ExecutionAdapter(lambda _: None, lambda: None),
        source_factory=lambda _: source,
    )
    with runtime.open_runtime(*inputs, START, dependencies=dependencies) as service:
        assert service.run_once(NOW).instances
        assert inputs[2].get_meta('dwd_redis_sentinel_binding') == 'original-identity'
        assert not source.closed
    assert source.closed
    assert not (inputs[3] / 'heartbeat.json').exists()
    assert 'DWD_REDIS_BINDING_CHECK_DISABLED' in caplog.text


def test_application_runs_with_injected_adapters_without_external_services(app):
    enable_ready(app)
    seen = []
    source = Source(signals())
    dependencies = runtime.RuntimeDependencies(
        execution_factory=lambda *_: runtime.ExecutionAdapter(seen.append, lambda: None),
        source_factory=lambda _: source,
    )
    application = Application(app, dependencies=dependencies)
    assert application.initialize(START.isoformat()).exit_code == 0
    result = application.run_once(NOW.isoformat())
    assert result.exit_code == 0
    assert len(result.payload['instances']) == len(seen) == 1
    assert result.payload['instances'][0]['status'] == 'SUCCESS'
    assert source.closed


def test_legacy_runtime_constructor_keeps_default_sql_startup_probe(local_http_app, monkeypatch):
    app, queries = local_http_app
    definition = load_definition(app)
    directory = app.parent / 'state'
    store = StateStore(directory / 'state.sqlite3', create=True)
    store.acquire_lock()
    monkeypatch.setattr(runtime.TaskScheduler, 'start', lambda self: self.shutdown(wait=False))
    try:
        with runtime.open_runtime(app, definition, store, directory, START) as service:
            legacy = runtime.ServiceRuntime(service.scheduler, service.runner, service.heartbeat)
            legacy.serve()
            assert legacy.heartbeat.ready and legacy.scheduler.stop_requested
    finally:
        store.close()
    assert [body for _, body in queries] == ['SELECT 1']
