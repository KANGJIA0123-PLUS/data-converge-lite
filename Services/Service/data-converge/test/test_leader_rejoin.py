"""BUG-2 (leader side): losing coordination ends a term, not the process.

The leader releases everything it owns and competes again as a standby. Only an
operator stop (SIGTERM) or a non-coordination error ends serve().
"""
from contextlib import closing

import pytest

from data_converge.application import Application
from data_converge.leader_election import LeadershipError
from data_converge.state import StateStore
from data_converge.state.redis_mirror import StateSyncError

from .test_dwd_ready_coordinator import START
from .test_main import app  # noqa: F401  (fixture)
from .test_writer_coordination import Lease, dependencies, enable_leadership


class Terms:
    """One fresh lease per term, like the production factory."""

    def __init__(self):
        self.leases = []

    def factory(self, _definition):
        from .test_writer_coordination import leased

        lease = Lease()
        self.leases.append(lease)
        return leased(lease)


def serve_terms(app, monkeypatch, outcomes):  # noqa: F811
    """Each scheduler.start() call consumes one outcome: an exception or 'stop'."""
    from data_converge.scheduler.service import TaskScheduler

    enable_leadership(app)
    Application(app).initialize(START.isoformat())
    terms, seen = Terms(), []
    adapters = dependencies(None, seen)
    adapters = type(adapters)(**{**adapters.__dict__, 'leadership_factory': terms.factory})
    monkeypatch.setattr('data_converge.application.time.sleep', lambda _s: None)

    def start(scheduler):
        outcome = outcomes.pop(0)
        if outcome == 'stop':
            scheduler.shutdown()
            return
        raise outcome
    monkeypatch.setattr(TaskScheduler, 'start', start)
    return Application(app, dependencies=adapters), terms


def test_redis_failure_rejoins_and_operator_stop_ends(app, monkeypatch, caplog):  # noqa: F811
    application, terms = serve_terms(app, monkeypatch, [
        StateSyncError('REDIS_STATE_WRITE_FAILED'), LeadershipError('LEADERSHIP_LOST'), 'stop'])
    caplog.set_level('WARNING')
    assert application.serve().exit_code == 0
    assert len(terms.leases) == 3 and all(lease.closed for lease in terms.leases)
    assert caplog.text.count('LEADERSHIP_TERM_ENDED') == 2
    assert 'REDIS_STATE_WRITE_FAILED' in caplog.text
    # The file lock is released between terms, so another writer could take over.
    with closing(StateStore(app.parent / 'state/state.sqlite3')) as store:
        store.acquire_lock()


def test_non_coordination_error_still_stops_the_process(app, monkeypatch):  # noqa: F811
    application, terms = serve_terms(app, monkeypatch, [RuntimeError('ledger corrupted')])
    with pytest.raises(RuntimeError, match='ledger corrupted'):
        application.serve()
    assert len(terms.leases) == 1 and terms.leases[0].closed


def test_without_leadership_errors_are_not_retried(app, monkeypatch):  # noqa: F811
    from data_converge.runtime import ExecutionAdapter, RuntimeDependencies
    from data_converge.scheduler.service import TaskScheduler

    Application(app).initialize(START.isoformat())

    def start(_scheduler):
        raise StateSyncError('REDIS_STATE_WRITE_FAILED')
    monkeypatch.setattr(TaskScheduler, 'start', start)
    adapters = RuntimeDependencies(execution_factory=lambda *_: ExecutionAdapter(lambda _i: None, lambda: None))
    with pytest.raises(StateSyncError):
        Application(app, dependencies=adapters).serve()
