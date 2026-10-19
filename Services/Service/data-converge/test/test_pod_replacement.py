"""Pod startup is independent of the retired permanent Redis ledger registry."""

from contextlib import closing

import pytest

from data_converge.application import Application
from data_converge.leader_election import LeadershipError
from data_converge.runtime import TaskScheduler
from data_converge.state import StateStore

from .test_main import app
from .test_dwd_ready_coordinator import START
from .test_writer_coordination import Lease, dependencies, enable_leadership


class ForeignRegistryLease(Lease):
    def bind_ledger(self, identity, *, require_existing=False):
        raise LeadershipError('LEADERSHIP_LEDGER_MISMATCH')

    def check_ledger(self, identity, *, require_existing=False):
        raise LeadershipError('LEADERSHIP_LEDGER_MISMATCH')


@pytest.mark.parametrize('metadata', [
    None,
    {'leadership_ledger_id': 'new-pod-ledger'},
    {'leadership_ledger_id': 'old-ledger', 'leadership_cluster': 'former-cluster'},
])
def test_replacement_pod_reaches_ready_without_reading_foreign_registry(app, monkeypatch, metadata):
    enable_leadership(app)
    if metadata is not None:
        Application(app).initialize(START.isoformat())
        with closing(StateStore(app.parent / 'state/state.sqlite3')) as store:
            store.acquire_lock()
            for key, value in metadata.items():
                store.set_meta(key, value)
    lease, seen, ready = ForeignRegistryLease(), [], []
    candidate = Application(app, dependencies=dependencies(lease, seen))
    monkeypatch.setattr(TaskScheduler, 'start',
                        lambda self: ready.append(candidate.check_health('readiness').exit_code))
    assert candidate.serve().exit_code == 0
    assert ready == [0] and lease.closed and seen == []
    with closing(StateStore(candidate.directory / 'state.sqlite3')) as store:
        for key in ('leadership_ledger_id', 'leadership_cluster'):
            assert store.get_meta(key) == (metadata or {}).get(key)


def test_lost_lease_still_prevents_replacement_pod_from_starting(app, monkeypatch):
    enable_leadership(app)
    lease, seen = ForeignRegistryLease(), []
    lease.lost = True
    monkeypatch.setattr(TaskScheduler, 'start', lambda self: pytest.fail('lost lease cannot schedule'))
    with pytest.raises(LeadershipError, match='LEADERSHIP_LOST'):
        Application(app, dependencies=dependencies(lease, seen)).serve()
    assert seen == [] and lease.closed
    assert not (app.parent / 'state/state.sqlite3').exists()
