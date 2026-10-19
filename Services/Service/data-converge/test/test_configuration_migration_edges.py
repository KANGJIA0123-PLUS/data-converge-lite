"""Migration rejection and admission leave historical execution facts intact."""

from contextlib import closing
from datetime import datetime
import sqlite3

import pytest
import yaml

from data_converge.application import Application
from data_converge.config.application import StartupError
from data_converge.configuration_migration.service import apply_config, preview_config
from data_converge.dag import TaskStatus
from data_converge.leader_election import LeadershipError
from data_converge.state import StateStore

from .test_main import app, enable_ready
from .test_configuration_migration import START, CUTOVER, add_task, dump_database, preserve_config
from .test_writer_coordination import Lease, dependencies, enable_leadership


def record_instance(path, status):
    definition = Application(path).definition
    item = definition.builder.create_instance(definition.dag, 'simple',
                datetime.fromisoformat('2026-10-12T10:11:00+08:00'))
    with closing(StateStore(path.parent / 'state/state.sqlite3')) as store:
        store.acquire_lock()
        store.enqueue_and_advance([item], {})
        store.claim(item.instance_id, now=item.scheduled_time)
        if status != TaskStatus.RUNNING:
            store.finish(item.instance_id, status)
    return item


def test_global_running_blocks_even_unrelated_addition_without_recovery(app):
    Application(app).initialize(START)
    record_instance(app, TaskStatus.RUNNING)
    add_task(app)
    target = Application(app)
    before = dump_database(app)
    plan = preview_config(target, effective_from=CUTOVER, allow_backfill=True)
    assert plan['blockers'] == [{'code': 'CONFIG_RUNNING_INSTANCES', 'count': 1}]
    with pytest.raises(StartupError, match='CONFIG_MIGRATION_BLOCKED'):
        apply_config(target, plan['plan_id'], effective_from=CUTOVER, allow_backfill=True)
    assert dump_database(app) == before


def test_changed_task_cannot_overlap_successful_old_window(app):
    Application(app).initialize(START)
    record_instance(app, TaskStatus.SUCCESS)
    (app.parent / 'sql/simple.sql').write_text('INSERT INTO target SELECT 42')
    plan = preview_config(Application(app), effective_from=CUTOVER, allow_backfill=True)
    assert plan['blockers'] == [{'code': 'CONFIG_TASK_WINDOW_OVERLAP', 'task': 'simple'}]
    plan = preview_config(Application(app), effective_from='2026-10-12T10:10:00+08:00',
                          allow_backfill=True)
    assert plan['blockers'] == []


@pytest.mark.parametrize('effective,code', [
    (None, 'CONFIG_EFFECTIVE_FROM_REQUIRED'),
    ('2026-10-12T10:05:00', 'CONFIG_EFFECTIVE_FROM_INVALID'),
    ('2026-10-12T10:06:00+08:00', 'CONFIG_EFFECTIVE_FROM_INVALID'),
])
def test_changed_tasks_need_explicit_aligned_aware_start(app, effective, code):
    Application(app).initialize(START)
    add_task(app)
    with pytest.raises(StartupError, match=code):
        preview_config(Application(app), effective_from=effective)


def test_past_cutover_needs_explicit_backfill_and_cannot_precede_coverage(app):
    Application(app).initialize(START)
    add_task(app)
    target = Application(app)
    plan = preview_config(target, effective_from=CUTOVER)
    before = dump_database(app)
    with pytest.raises(StartupError, match='CONFIG_EFFECTIVE_FROM_PAST'):
        apply_config(target, plan['plan_id'], effective_from=CUTOVER,
                     now=datetime.fromisoformat('2026-10-12T11:00:00+08:00'))
    assert dump_database(app) == before
    plan = preview_config(target, effective_from='2026-10-12T09:55:00+08:00', allow_backfill=True)
    assert {'code': 'CONFIG_EFFECTIVE_BEFORE_COVERAGE'} in plan['blockers']


def test_legacy_previous_config_must_reproduce_binding(app):
    Application(app).initialize(START)
    previous = preserve_config(app)
    with closing(sqlite3.connect(app.parent / 'state/state.sqlite3')) as conn, conn:
        conn.execute("DELETE FROM metadata WHERE key IN ('configuration_snapshot','configuration_required')")
    old = yaml.safe_load(previous.read_text())
    old['clickhouse']['database'] = 'other'
    previous.write_text(yaml.safe_dump(old))
    with pytest.raises(StartupError, match='CONFIG_PREVIOUS_BINDING_MISMATCH'):
        preview_config(Application(app), previous_config=previous)


def test_readiness_change_requires_two_confirmations_and_preserves_signals(app):
    enable_ready(app)
    Application(app).initialize(START)
    raw = yaml.safe_load(app.read_text())
    raw['dwd_ready']['queue_key'] = 'another-queue'
    app.write_text(yaml.safe_dump(raw))
    target = Application(app)
    plan = preview_config(target)
    assert plan['required_confirmations'] == ['confirm_same_data', 'confirm_readiness_change']
    with pytest.raises(StartupError, match='CONFIG_CONFIRMATION_REQUIRED'):
        apply_config(target, plan['plan_id'], confirm_same_data=True)
    assert apply_config(target, plan['plan_id'], confirm_same_data=True,
                        confirm_readiness_change=True)['status'] == 'APPLIED'


def test_existing_writer_lock_prevents_apply(app):
    Application(app).initialize(START)
    add_task(app)
    target = Application(app)
    plan = preview_config(target, effective_from=CUTOVER, allow_backfill=True)
    with closing(StateStore(app.parent / 'state/state.sqlite3')) as store:
        store.acquire_lock()
        before = dump_database(app)
        with pytest.raises(RuntimeError, match='Another local writer'):
            apply_config(target, plan['plan_id'], effective_from=CUTOVER, allow_backfill=True)
        assert dump_database(app) == before


def bind_existing_ledger(path):
    with closing(StateStore(path.parent / 'state/state.sqlite3')) as store:
        store.acquire_lock()
        store.set_meta('leadership_cluster', 'unit')
        store.set_meta('leadership_ledger_id', 'existing-ledger')


def test_bound_ledger_keeps_registry_identity_and_failed_registry_changes_nothing(app):
    enable_leadership(app)
    Application(app).initialize(START)
    bind_existing_ledger(app)
    add_task(app)
    lease = Lease()
    target = Application(app, dependencies=dependencies(lease, []))
    plan = preview_config(target, effective_from=CUTOVER, allow_backfill=True)
    before = dump_database(app)

    def reject(identity, *, require_existing):
        assert identity == 'existing-ledger' and require_existing
        raise LeadershipError('LEADERSHIP_LEDGER_MISMATCH')

    lease.bind_ledger = reject
    with pytest.raises(LeadershipError, match='LEADERSHIP_LEDGER_MISMATCH'):
        apply_config(target, plan['plan_id'], effective_from=CUTOVER, allow_backfill=True)
    assert dump_database(app) == before and lease.closed
    assert not (app.parent / 'configuration-backups').exists()


@pytest.mark.parametrize('change', ['disabled', 'renamed'])
def test_bound_ledger_cannot_change_coordination_identity(app, change):
    enable_leadership(app)
    Application(app).initialize(START)
    bind_existing_ledger(app)
    raw = yaml.safe_load(app.read_text())
    if change == 'disabled':
        raw['leadership']['enabled'] = False
    else:
        raw['leadership']['cluster_name'] = 'another'
    app.write_text(yaml.safe_dump(raw))
    plan = preview_config(Application(app))
    assert {'code': 'CONFIG_COORDINATION_CHANGED'} in plan['blockers']


def test_live_facts_make_preview_stale_before_any_backup(app):
    Application(app).initialize(START)
    (app.parent / 'sql/simple.sql').write_text('INSERT INTO target SELECT 42')
    target = Application(app)
    plan = preview_config(target, effective_from=CUTOVER, allow_backfill=True)
    record_instance(app, TaskStatus.SUCCESS)
    before = dump_database(app)
    with pytest.raises(StartupError, match='CONFIG_PLAN_STALE'):
        apply_config(target, plan['plan_id'], effective_from=CUTOVER, allow_backfill=True)
    assert dump_database(app) == before
    assert not (app.parent / 'configuration-backups').exists()


@pytest.mark.parametrize('changed', ['sql', 'rules', 'config'])
def test_reusing_application_cannot_apply_stale_files(app, changed):
    Application(app).initialize(START)
    add_task(app)
    target = Application(app)
    target.manifest  # Deliberately populate the normal execution snapshot cache.
    plan = preview_config(target, effective_from=CUTOVER, allow_backfill=True)
    if changed == 'sql':
        (app.parent / 'sql/added.sql').write_text('INSERT INTO added_target SELECT 99')
    else:
        path = app if changed == 'config' else app.parent / 'rules.yaml'
        raw = yaml.safe_load(path.read_text())
        if changed == 'config':
            raw['clickhouse']['username'] = 'another'
        else:
            raw['convergence_rules'][-1]['target_table'] = 'another'
        path.write_text(yaml.safe_dump(raw))
    before = dump_database(app)
    with pytest.raises(StartupError, match='CONFIG_PLAN_STALE'):
        apply_config(target, plan['plan_id'], effective_from=CUTOVER, allow_backfill=True)
    assert dump_database(app) == before


def test_backup_crossing_cutover_rejects_without_modifying_ledger(app, monkeypatch):
    from data_converge.configuration_migration import service

    Application(app).initialize(START)
    add_task(app)
    target = Application(app)
    plan = preview_config(target, effective_from=CUTOVER)
    before = dump_database(app)
    instant = [datetime.fromisoformat('2026-10-12T10:04:00+08:00')]

    class Clock:
        @staticmethod
        def now(zone):
            return instant[0].astimezone(zone)

    backup = StateStore.backup_configuration

    def slow_backup(self, destination):
        result = backup(self, destination)
        instant[0] = datetime.fromisoformat('2026-10-12T10:06:00+08:00')
        return result

    monkeypatch.setattr(service, 'datetime', Clock)
    monkeypatch.setattr(StateStore, 'backup_configuration', slow_backup)
    with pytest.raises(StartupError, match='CONFIG_EFFECTIVE_FROM_PAST'):
        apply_config(target, plan['plan_id'], effective_from=CUTOVER)
    assert dump_database(app) == before


def test_changed_cron_replans_from_cutover_and_preserves_unaffected_cursor(app):
    from data_converge.runtime import ExecutionAdapter, RuntimeDependencies

    add_task(app)
    rules_path = app.parent / 'rules.yaml'
    rules = yaml.safe_load(rules_path.read_text())
    rules['convergence_rules'][1]['cron'] = '3 * * * *'
    rules_path.write_text(yaml.safe_dump(rules))
    original = Application(app)
    original.initialize(START)
    old_future = datetime.fromisoformat('2026-10-12T12:03:00+08:00')
    old_simple = datetime.fromisoformat('2026-10-12T10:06:00+08:00')
    with closing(StateStore(original.directory / 'state.sqlite3')) as store:
        store.acquire_lock()
        store.enqueue_and_advance([], {'simple': old_simple, 'added': old_future})
    rules['convergence_rules'][1]['cron'] = rules['convergence_rules'][0]['cron']
    rules_path.write_text(yaml.safe_dump(rules))
    seen = []
    target = Application(app, dependencies=RuntimeDependencies(
        execution_factory=lambda *_: ExecutionAdapter(seen.append, lambda: None)))
    options = dict(effective_from='2026-10-12T11:00:00+08:00', allow_backfill=True)
    plan = preview_config(target, **options)
    result = apply_config(target, plan['plan_id'], **options)
    with closing(StateStore(original.directory / 'state.sqlite3')) as store:
        assert store.get_cursor('simple') == old_simple
    target.run_once('2026-10-12T11:16:00+08:00')
    assert [item.data_start.minute for item in seen if item.taskname == 'added'] == [0, 5, 10]
    with closing(StateStore(result['backup_path'])) as old:
        assert old.get_cursor('added') == old_future and old.get_cursor('simple') == old_simple
