"""Legacy recovery keeps lease fencing without relying on retired identities."""

from contextlib import closing
from pathlib import Path

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

    def check_ledger(self, identity, *, require_existing=False):
        pytest.fail('recovery must not check the retired registry')

    def bind_ledger(self, identity, *, require_existing=False):
        pytest.fail('recovery must not overwrite the retired registry')


def coordinated_legacy(app, *, bound):
    enable_leadership(app)
    legacy(app)
    if bound:
        with closing(StateStore(app.parent / 'state/state.sqlite3')) as store:
            store.acquire_lock()
            store.set_meta('leadership_cluster', 'unit')
            store.set_meta('leadership_ledger_id', 'original-ledger')


@pytest.mark.parametrize('bound,remote', [
    (False, None), (False, 'foreign-ledger'),
    (True, 'original-ledger'), (True, 'foreign-ledger'), (True, None),
])
def test_recovery_ignores_registry_and_preserves_old_metadata(app, clock, bound, remote):
    coordinated_legacy(app, bound=bound)
    lease, seen = RegistryLease(remote), []
    target = Application(app, dependencies=dependencies(lease, seen))
    assert target.serve().exit_code == 0
    with closing(StateStore(target.directory / 'state.sqlite3')) as store:
        assert store.get_meta('leadership_cluster') == ('unit' if bound else None)
        assert store.get_meta('leadership_ledger_id') == ('original-ledger' if bound else None)
        assert Path(store.legacy_recovery()['backup_path']).is_file()
        assert store.retired_status_counts() == {'SUCCESS': 1, 'UNKNOWN': 1, 'RUNNING': 1}
    assert seen == [] and lease.registry == remote and lease.closed


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
    target = Application(app, dependencies=dependencies(lease, seen))
    # A lost term is retried by serve(); this unit keeps the old no-retirement
    # assertion without pretending the process must exit.
    with pytest.raises(LeadershipError, match='LEADERSHIP_LOST'):
        target._serve_term(None, target.dependencies)
    assert dump_database(app) == before and seen == []


def test_restart_ignores_registry_and_does_not_retire_twice(app, clock):
    coordinated_legacy(app, bound=False)
    lease, seen = RegistryLease('old-deployment'), []
    assert Application(app, dependencies=dependencies(lease, seen)).serve().exit_code == 0
    with closing(StateStore(app.parent / 'state/state.sqlite3')) as store:
        audit = store.legacy_recovery()
    retry = RegistryLease('old-deployment')
    assert Application(app, dependencies=dependencies(retry, seen)).serve().exit_code == 0
    with closing(StateStore(app.parent / 'state/state.sqlite3')) as store:
        assert store.legacy_recovery() == audit
        assert store.get_meta('leadership_ledger_id') is None
    assert len(list(Path(audit['backup_path']).parent.glob('*.sqlite3'))) == 1
    assert seen == [] and lease.registry == retry.registry == 'old-deployment'


@pytest.mark.parametrize('change', ['disable', 'rename'])
def test_fallback_preserves_old_metadata_when_coordination_changes(app, clock, change):
    coordinated_legacy(app, bound=True)
    raw = yaml.safe_load(app.read_text())
    raw['leadership'].update({'enabled': False} if change == 'disable' else {'cluster_name': 'different'})
    app.write_text(yaml.safe_dump(raw))
    lease = RegistryLease('original-ledger')
    target = Application(app, dependencies=dependencies(lease, []))
    assert target.serve().exit_code == 0
    with closing(StateStore(target.directory / 'state.sqlite3')) as store:
        assert store.get_meta('leadership_cluster') == 'unit'
        assert store.get_meta('leadership_ledger_id') == 'original-ledger'
        assert store.legacy_recovery() is not None
    assert lease.registry == 'original-ledger'


def test_missing_old_ledger_id_does_not_prevent_recovery(app, clock):
    coordinated_legacy(app, bound=False)
    with closing(StateStore(app.parent / 'state/state.sqlite3')) as store:
        store.acquire_lock()
        store.set_meta('leadership_cluster', 'unit')
    target = Application(app, dependencies=dependencies(RegistryLease('foreign-ledger'), []))
    assert target.serve().exit_code == 0
    with closing(StateStore(target.directory / 'state.sqlite3')) as store:
        assert store.get_meta('leadership_cluster') == 'unit'
        assert store.get_meta('leadership_ledger_id') is None
        assert store.legacy_recovery() is not None
