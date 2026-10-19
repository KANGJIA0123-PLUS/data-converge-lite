"""Legacy recovery respects the existing registration before touching facts."""

from contextlib import closing

import pytest
import yaml

from data_converge.application import Application
from data_converge.leader_election import LeadershipError
from data_converge.state import StateStore

from .test_main import app
from .test_legacy_startup_recovery import clock, legacy
from .test_configuration_migration import dump_database
from .test_writer_coordination import Lease, dependencies, enable_leadership


class RegistryLease(Lease):
    def __init__(self, registry=None):
        super().__init__()
        self.registry = registry
        self.checked = []

    def check_ledger(self, identity, *, require_existing=False):
        self.checked.append((identity, require_existing))
        if self.registry is not None and self.registry != identity:
            raise LeadershipError('LEADERSHIP_LEDGER_MISMATCH')
        if self.registry is None and require_existing:
            raise LeadershipError('LEADERSHIP_REGISTRY_MISSING')

    def bind_ledger(self, identity, *, require_existing=False):
        super().bind_ledger(identity, require_existing=require_existing)
        self.registry = identity


def coordinated_legacy(app, *, bound):
    enable_leadership(app)
    legacy(app)
    if bound:
        with closing(StateStore(app.parent / 'state/state.sqlite3')) as store:
            store.acquire_lock()
            store.set_meta('leadership_cluster', 'unit')
            store.set_meta('leadership_ledger_id', 'original-ledger')


@pytest.mark.parametrize('bound', [False, True])
def test_recovery_checks_registry_before_backup_and_preserves_original_identity(app, clock, monkeypatch, bound):
    coordinated_legacy(app, bound=bound)
    lease, seen = RegistryLease('original-ledger' if bound else None), []
    backup = StateStore.backup_configuration
    def checked_backup(store, destination):
        assert lease.checked == [('original-ledger' if bound else None, bound)]
        assert lease.bindings == []
        return backup(store, destination)
    monkeypatch.setattr(StateStore, 'backup_configuration', checked_backup)
    target = Application(app, dependencies=dependencies(lease, seen))
    assert target.serve().exit_code == 0
    with closing(StateStore(target.directory / 'state.sqlite3')) as store:
        assert store.get_meta('leadership_ledger_id') == lease.registry
        if bound:
            assert lease.registry == 'original-ledger'
    assert seen == []


@pytest.mark.parametrize('bound,remote,code', [
    (False, 'foreign-ledger', 'LEDGER_MISMATCH'),
    (True, 'foreign-ledger', 'LEDGER_MISMATCH'),
    (True, None, 'REGISTRY_MISSING'),
])
def test_registry_conflict_keeps_legacy_ledger_and_creates_no_backup(app, clock, bound, remote, code):
    coordinated_legacy(app, bound=bound)
    before = dump_database(app)
    lease, seen = RegistryLease(remote), []
    with pytest.raises(LeadershipError, match=code):
        Application(app, dependencies=dependencies(lease, seen)).serve()
    assert dump_database(app) == before
    assert not (app.parent / 'configuration-backups').exists()
    assert seen == [] and lease.bindings == []


def test_lease_loss_during_backup_prevents_retirement(app, clock, monkeypatch):
    coordinated_legacy(app, bound=True)
    before = dump_database(app)
    lease, seen = RegistryLease('original-ledger'), []
    backup = StateStore.backup_configuration
    def lost_backup(store, destination):
        result = backup(store, destination)
        lease.lost = True
        return result
    monkeypatch.setattr(StateStore, 'backup_configuration', lost_backup)
    with pytest.raises(LeadershipError, match='LEADERSHIP_LOST'):
        Application(app, dependencies=dependencies(lease, seen)).serve()
    assert dump_database(app) == before and seen == []


def test_transient_binding_failure_after_commit_retries_without_retiring_twice(app, clock):
    coordinated_legacy(app, bound=False)
    lease, seen = RegistryLease(), []
    def unavailable(*args, **kwargs):
        raise LeadershipError('LEADERSHIP_REDIS_FAILED')
    lease.bind_ledger = unavailable
    with pytest.raises(LeadershipError, match='LEADERSHIP_REDIS_FAILED'):
        Application(app, dependencies=dependencies(lease, seen)).serve()
    with closing(StateStore(app.parent / 'state/state.sqlite3')) as store:
        audit, identity = store.legacy_recovery(), store.get_meta('leadership_ledger_id')
    retry = RegistryLease()
    assert Application(app, dependencies=dependencies(retry, seen)).serve().exit_code == 0
    with closing(StateStore(app.parent / 'state/state.sqlite3')) as store:
        assert store.legacy_recovery() == audit
        assert store.get_meta('leadership_ledger_id') == identity == retry.registry
    assert seen == []


@pytest.mark.parametrize('change,code', [('disable', 'REQUIRED_FOR_LEDGER'), ('rename', 'CLUSTER_CHANGED')])
def test_fallback_never_replaces_bound_coordination_identity(app, clock, change, code):
    coordinated_legacy(app, bound=True)
    raw = yaml.safe_load(app.read_text())
    raw['leadership'].update({'enabled': False} if change == 'disable' else {'cluster_name': 'different'})
    app.write_text(yaml.safe_dump(raw))
    before = dump_database(app)
    with pytest.raises(LeadershipError, match=code):
        Application(app, dependencies=dependencies(RegistryLease('original-ledger'), [])).serve()
    assert dump_database(app) == before
