"""Normal startup preserves unknown history and resumes only new full windows."""

from contextlib import closing
from datetime import datetime, timedelta
from pathlib import Path
import sqlite3

import pytest

from data_converge.application import Application
from data_converge.config.application import StartupError
from data_converge.dag import TaskStatus
from data_converge.runtime import TaskScheduler
from data_converge.state import StateStore

from .test_main import app, local_http_app
from .test_configuration_migration import START, add_task, dump_database
from .test_legacy_retirement import facts

NOW = datetime.fromisoformat('2026-10-19T11:21:00+08:00')
CUTOVER = NOW.replace(minute=25)


def legacy(path, *, seed=True):
    original = Application(path)
    original.initialize(START)
    with closing(StateStore(path.parent / 'state/state.sqlite3')) as store:
        store.acquire_lock()
        if seed:
            for index, status in enumerate((TaskStatus.SUCCESS, TaskStatus.UNKNOWN, TaskStatus.RUNNING)):
                item = original.definition.builder.create_instance(original.definition.dag, 'simple',
                    datetime.fromisoformat(START) + timedelta(minutes=6 + 5 * index))
                store.enqueue_and_advance([item], {'simple': item.scheduled_time})
                store.claim(item.instance_id, now=item.data_end)
                if status != TaskStatus.RUNNING:
                    store.finish(item.instance_id, status, now=item.data_end)
        before = facts(store)
    with closing(sqlite3.connect(path.parent / 'state/state.sqlite3')) as conn, conn:
        conn.execute("DELETE FROM metadata WHERE key IN ('configuration_snapshot','configuration_required')")
        conn.execute('DROP TABLE configuration_history')
    add_task(path)
    return before


@pytest.fixture
def clock(monkeypatch):
    from data_converge.configuration_migration import legacy_recovery
    monkeypatch.setattr(legacy_recovery, '_utc_now', lambda: NOW)
    monkeypatch.setattr(TaskScheduler, 'start', lambda self: None)
    return legacy_recovery


def test_normal_start_retires_old_facts_executes_new_windows_and_restarts_once(local_http_app, clock, monkeypatch, caplog):
    path, requests = local_http_app
    before = legacy(path)
    monkeypatch.setattr(TaskScheduler, 'start', lambda self: self.tick(CUTOVER + timedelta(minutes=6)))
    target = Application(path)
    assert target.serve().exit_code == 0
    with closing(StateStore(target.directory / 'state.sqlite3')) as store:
        audit = store.legacy_recovery()
        for name, rows in before.items():
            assert facts(store)[name][:len(rows)] == rows
        assert store.get_meta('coverage_start') == START
        assert set(store.get_task_effective_starts().values()) == {CUTOVER}
        assert store.active_status_counts() == {'SUCCESS': 2}
        assert store.retired_status_counts() == {'SUCCESS': 1, 'UNKNOWN': 1, 'RUNNING': 1}
        assert Path(audit['backup_path']).is_file()
        assert audit['identity_verified'] is False
        assert store.get_meta('binding') != target.fingerprint  # Old binaries cannot reopen this ledger.
    assert len([body for _, body in requests if body != 'SELECT 1']) == 2
    assert Application(path).serve().exit_code == 0
    with closing(StateStore(target.directory / 'state.sqlite3')) as store:
        assert store.legacy_recovery() == audit
    assert len(list(Path(audit['backup_path']).parent.glob('*.sqlite3'))) == 1
    assert len([body for _, body in requests if body != 'SELECT 1']) == 2
    assert 'LEGACY_NEW_WINDOWS_RECOVERED' in caplog.text
    assert 'synthetic-integration-secret' not in caplog.text


@pytest.mark.parametrize('operation', ['run_once', 'run_task', 'initialize', 'resolve'])
def test_non_service_commands_never_implicitly_retire_legacy_history(local_http_app, operation):
    app, _ = local_http_app
    legacy(app)
    target = Application(app)
    with closing(StateStore(target.directory / 'state.sqlite3')) as store:
        uncertain = next(row['instance_id'] for row in store.list_instances() if row['status'] == 'UNKNOWN')
        binding = store.get_meta('binding')
    arguments = {'run_once': ['2026-10-12T10:06:00+08:00'],
                 'run_task': ['simple', '2026-10-12T10:06:00+08:00'],
                 'initialize': [], 'resolve': [uncertain, 'success', 'confirmed']}
    assert getattr(target, operation)(*arguments[operation]).exit_code == 0
    with closing(StateStore(target.directory / 'state.sqlite3')) as store:
        assert store.legacy_recovery() is None
        assert store.configuration_snapshot() is None
        assert store.get_meta('binding') == binding


def test_new_snapshot_mismatch_can_start_without_rewriting_history(local_http_app, clock, caplog):
    app, _ = local_http_app
    Application(app).initialize(START)
    add_task(app)
    before = dump_database(app)
    assert Application(app).serve().exit_code == 0
    assert dump_database(app) == before
    assert 'CONFIGURATION_CHECK_DISABLED' in caplog.text


def test_lost_new_snapshot_markers_cannot_masquerade_as_legacy(app, clock):
    Application(app).initialize(START)
    with closing(sqlite3.connect(app.parent / 'state/state.sqlite3')) as conn, conn:
        conn.execute("DELETE FROM metadata WHERE key IN ('configuration_snapshot','configuration_required')")
    add_task(app)
    before = dump_database(app)
    with pytest.raises(StartupError, match='LEGACY_RECOVERY_INVALID'):
        Application(app).serve()
    assert dump_database(app) == before


def test_backup_failure_keeps_original_ledger(app, clock, monkeypatch):
    legacy(app)
    before = dump_database(app)
    def fail(*args):
        raise OSError('backup unavailable')
    monkeypatch.setattr(StateStore, 'backup_configuration', fail)
    with pytest.raises(OSError, match='backup unavailable'):
        Application(app).serve()
    assert dump_database(app) == before


def test_cutover_uses_clock_after_slow_backup(local_http_app, clock, monkeypatch):
    path, _ = local_http_app
    legacy(path)
    original = StateStore.backup_configuration
    def slow_backup(store, destination):
        result = original(store, destination)
        monkeypatch.setattr(clock, '_utc_now', lambda: NOW.replace(minute=26))
        return result
    monkeypatch.setattr(StateStore, 'backup_configuration', slow_backup)
    Application(path).serve()
    with closing(StateStore(path.parent / 'state/state.sqlite3')) as store:
        assert set(store.get_task_effective_starts().values()) == {NOW.replace(minute=30)}


@pytest.mark.parametrize('source', ['instance', 'signal'])
def test_future_old_facts_cannot_overlap_new_windows(local_http_app, clock, source):
    path, _ = local_http_app
    legacy(path)
    application = Application(path)
    with closing(StateStore(application.directory / 'state.sqlite3')) as store:
        store.acquire_lock()
        if source == 'instance':
            item = application.definition.builder.create_instance(application.definition.dag, 'simple',
                                                                  NOW.replace(hour=13, minute=6))
            store.enqueue_and_advance([item], {})
        else:
            store.record_dwd_signal(NOW.replace(hour=13, minute=0), 'test', NOW)
    application.serve()
    with closing(StateStore(application.directory / 'state.sqlite3')) as store:
        assert set(store.get_task_effective_starts().values()) == {NOW.replace(hour=13, minute=5)}


def test_target_files_changed_during_backup_do_not_change_ledger(app, clock, monkeypatch):
    legacy(app)
    before = dump_database(app)
    original = StateStore.backup_configuration
    def drift(store, destination):
        result = original(store, destination)
        (app.parent / 'sql/simple.sql').write_text('INSERT INTO target SELECT 999')
        return result
    monkeypatch.setattr(StateStore, 'backup_configuration', drift)
    with pytest.raises(StartupError, match='LEGACY_RECOVERY_CONFIGURATION_CHANGED'):
        Application(app).serve()
    assert dump_database(app) == before


def test_retirement_marker_loss_cannot_resume_or_recover_again(local_http_app, clock):
    path, _ = local_http_app
    legacy(path)
    Application(path).serve()
    with closing(sqlite3.connect(path.parent / 'state/state.sqlite3')) as conn, conn:
        conn.execute("DELETE FROM metadata WHERE key='retirement_required'")
    before = dump_database(path)
    with pytest.raises(ValueError, match='LEGACY_RETIREMENT_INVALID'):
        Application(path).serve()
    assert dump_database(path) == before


def test_ordinary_migration_preserves_fence_and_old_unknown_is_not_a_blocker(local_http_app, clock):
    from data_converge.configuration_migration.service import preview_config, apply_config
    path, _ = local_http_app
    legacy(path)
    Application(path).serve()
    (path.parent / 'sql/simple.sql').write_text('INSERT INTO target SELECT 42')
    target = Application(path)
    plan = preview_config(target, effective_from=CUTOVER.isoformat(), allow_backfill=True)
    assert plan['blockers'] == []
    apply_config(target, plan['plan_id'], effective_from=CUTOVER.isoformat(), allow_backfill=True)
    assert target.serve().exit_code == 0
    with closing(StateStore(target.directory / 'state.sqlite3')) as store:
        store.validate_retirement()
        assert store.get_meta('binding') != target.fingerprint
    assert apply_config(target, plan['plan_id'])['status'] == 'ALREADY_APPLIED'


def test_later_migration_cannot_reopen_the_isolated_historical_range(local_http_app, clock):
    from data_converge.configuration_migration.service import preview_config, apply_config
    path, _ = local_http_app
    legacy(path)
    Application(path).serve()
    (path.parent / 'sql/simple.sql').write_text('INSERT INTO target SELECT 42')
    target = Application(path)
    before = dump_database(path)
    plan = preview_config(target, effective_from=START, allow_backfill=True)
    assert plan['recovery_cutover'] == CUTOVER.isoformat()
    assert {'code': 'CONFIG_EFFECTIVE_BEFORE_COVERAGE'} in plan['blockers']
    with pytest.raises(StartupError, match='CONFIG_MIGRATION_BLOCKED'):
        apply_config(target, plan['plan_id'], effective_from=START, allow_backfill=True)
    assert dump_database(path) == before
