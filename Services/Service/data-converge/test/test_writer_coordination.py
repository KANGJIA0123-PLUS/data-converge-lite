"""Writer admission is checked before state recovery or any external work."""
from contextlib import contextmanager, closing

import pytest
import yaml

from data_converge.application import Application
from data_converge.config.application import load_definition
from data_converge.runtime import ExecutionAdapter, RuntimeDependencies
from data_converge.state import StateStore
from data_converge.leader_election import LeadershipError

from .test_main import app, enable_ready
from .test_dwd_ready_coordinator import Source, START, NOW


class Lease:
    def __init__(self, *, available=True):
        self.available = available
        self.lost = False
        self.closed = False
        self.bindings = []

    def acquire(self):
        return self.available

    def assert_owned(self):
        if self.lost:
            raise LeadershipError('LEADERSHIP_LOST')

    def bind_ledger(self, identity, *, require_existing=False):
        self.bindings.append((identity, require_existing))

    def close(self):
        self.closed = True


@contextmanager
def leased(lease):
    try:
        yield lease
    finally:
        lease.close()


def enable_leadership(path):
    enable_ready(path)
    config = yaml.safe_load(path.read_text())
    config['leadership'] = {'enabled': True, 'cluster_name': 'unit'}
    path.write_text(yaml.safe_dump(config))


def dependencies(lease, seen):
    return RuntimeDependencies(
        execution_factory=lambda *_: ExecutionAdapter(seen.append, lambda: None),
        source_factory=lambda _: Source(),
        leadership_factory=lambda _: leased(lease),
    )


def test_busy_lease_prevents_state_recovery_and_sql(app):
    enable_leadership(app)
    application = Application(app)
    application.initialize(START.isoformat())
    lease, seen = Lease(available=False), []
    candidate = Application(app, dependencies=dependencies(lease, seen))
    with pytest.raises(Exception, match='LEADERSHIP_BUSY'):
        candidate.run_once(NOW.isoformat())
    assert seen == [] and lease.closed
    with closing(StateStore(app.parent / 'state/state.sqlite3')) as store:
        assert store.get_meta('leadership_ledger_id') is None


def test_successive_writers_keep_completed_tasks_without_registry_binding(app):
    from .test_dwd_ready_coordinator import signals

    enable_leadership(app)
    Application(app).initialize(START.isoformat())
    leases, seen = [Lease(), Lease()], []
    for lease in leases:
        adapters = RuntimeDependencies(
            execution_factory=lambda *_: ExecutionAdapter(lambda i: seen.append(i.taskname), lambda: None),
            source_factory=lambda _: Source(signals()), leadership_factory=lambda _: leased(lease))
        assert Application(app, dependencies=adapters).run_once(NOW.isoformat()).exit_code == 0
    assert seen == ['simple']
    assert all(lease.closed and lease.bindings == [] for lease in leases)
    with closing(StateStore(app.parent / 'state/state.sqlite3')) as store:
        assert store.get_meta('leadership_ledger_id') is None
        assert store.get_meta('leadership_cluster') is None


@pytest.mark.parametrize('value', [None, [], {'enabled': 'true'},
    {'enabled': True, 'ttl_seconds': 0}, {'enabled': True, 'cluster_name': ''},
    {'enabled': True, 'ttl_seconds': 5, 'renew_interval_seconds': 5}])
def test_leadership_config_rejects_invalid_settings(app, value):
    enable_ready(app)
    config = yaml.safe_load(app.read_text())
    config['leadership'] = value
    app.write_text(yaml.safe_dump(config))
    with pytest.raises(ValueError, match='LEADERSHIP_CONFIG_INVALID'):
        load_definition(app)


def test_lost_lease_rejects_before_recovery_or_runtime(app, monkeypatch):
    enable_leadership(app)
    Application(app).initialize(START.isoformat())
    lease, seen = Lease(), []

    def forbidden(*args):
        pytest.fail('a lost lease must not recover running tasks')

    lease.lost = True
    monkeypatch.setattr(StateStore, 'recover_running', forbidden)
    with pytest.raises(LeadershipError, match='LEADERSHIP_LOST'):
        Application(app, dependencies=dependencies(lease, seen)).run_once(NOW.isoformat())
    assert seen == [] and lease.closed


def test_lost_lease_blocks_signal_read_ack_and_sql_submission():
    from data_converge.coordination import GuardedRunner, GuardedSignalSource
    from data_converge.errors import TaskExecutionError
    from types import SimpleNamespace

    lease, seen = Lease(), []
    lease.lost = True
    source = Source(['message'])
    guarded = GuardedSignalSource(source, lease)
    with pytest.raises(LeadershipError):
        guarded.receive()
    with pytest.raises(LeadershipError):
        guarded.ack('message')
    with pytest.raises(TaskExecutionError) as caught:
        GuardedRunner(seen.append, lease)(SimpleNamespace(query_id='query'))
    assert caught.value.retryable and not caught.value.outcome_unknown
    assert seen == [] and source.acknowledged == []


def test_historical_coordination_metadata_does_not_require_leadership(app):
    enable_ready(app)
    Application(app).initialize(START.isoformat())
    with closing(StateStore(app.parent / 'state/state.sqlite3')) as store:
        store.acquire_lock()
        store.set_meta('leadership_cluster', 'former-cluster')
        store.set_meta('leadership_ledger_id', 'former-ledger')
    seen = []
    Application(app, dependencies=dependencies(Lease(), seen)).run_once(NOW.isoformat())
    with closing(StateStore(app.parent / 'state/state.sqlite3')) as store:
        assert store.get_meta('leadership_cluster') == 'former-cluster'
        assert store.get_meta('leadership_ledger_id') == 'former-ledger'


def test_lease_loss_drains_completed_sql_and_restart_runs_only_remaining_task(app):
    from data_converge.runtime import RuntimeDependencies
    from .test_dwd_ready_coordinator import signals

    enable_leadership(app)
    rules = app.parent / 'rules.yaml'
    content = yaml.safe_load(rules.read_text())
    child = dict(content['convergence_rules'][0], taskname='child', depends_on=['simple'])
    content['convergence_rules'].append(child)
    rules.write_text(yaml.safe_dump(content))
    Application(app).initialize(START.isoformat())
    lease, seen = Lease(), []

    def execute(instance):
        seen.append(instance.taskname)
        lease.lost = True

    adapters = RuntimeDependencies(
        execution_factory=lambda *_: ExecutionAdapter(execute, lambda: None),
        source_factory=lambda _: Source(signals()), leadership_factory=lambda _: leased(lease))
    with pytest.raises(LeadershipError, match='LOST'):
        Application(app, dependencies=adapters).run_once(NOW.isoformat())
    assert seen == ['simple']
    next_lease = Lease()
    adapters = RuntimeDependencies(
        execution_factory=lambda *_: ExecutionAdapter(lambda i: seen.append(i.taskname), lambda: None),
        source_factory=lambda _: Source(), leadership_factory=lambda _: leased(next_lease))
    result = Application(app, dependencies=adapters).run_once(NOW.isoformat())
    assert result.exit_code == 0 and seen == ['simple', 'child']
    assert next_lease.bindings == lease.bindings == []


def test_standby_has_own_live_but_not_ready_heartbeat(app, monkeypatch):
    from data_converge import runtime

    enable_leadership(app)
    lease = Lease()
    attempts = []
    application = Application(app, dependencies=dependencies(lease, []))

    def acquire():
        attempts.append((application.check_health('liveness').exit_code,
                         application.check_health('readiness').exit_code))
        return len(attempts) == 2

    lease.acquire = acquire
    monkeypatch.setattr('data_converge.coordination.admission.time.sleep', lambda _: None)
    monkeypatch.setattr(runtime.TaskScheduler, 'start', lambda self: None)
    assert application.serve().exit_code == 0
    assert attempts == [(0, 1), (0, 1)]
    assert application.check_health('liveness').exit_code == 1


def test_replica_health_does_not_read_another_replica_heartbeat(app, monkeypatch):
    from data_converge.health import Heartbeat, heartbeat_directory

    enable_leadership(app)
    application = Application(app)
    monkeypatch.setenv('DATA_CONVERGE_INSTANCE_ID', 'active-pod')
    directory = heartbeat_directory(application.directory, application._raw_config)
    directory.mkdir(parents=True)
    with Heartbeat(directory) as heartbeat:
        heartbeat.set_ready(True)
        assert application.check_health('readiness').exit_code == 0
        monkeypatch.setenv('DATA_CONVERGE_INSTANCE_ID', 'standby-pod')
        assert application.check_health('readiness').exit_code == 1
