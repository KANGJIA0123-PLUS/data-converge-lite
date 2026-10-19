"""Configuration upgrades preserve execution facts and require explicit scope."""

from contextlib import closing
from datetime import datetime
import json
from pathlib import Path
import shutil
import sqlite3

import pytest
import yaml

from data_converge.application import Application
from data_converge.config.application import StartupError
from data_converge.configuration_migration.service import preview_config, apply_config
from data_converge.state import StateStore

from .test_main import app, local_http_app

START = '2026-10-12T10:00:00+08:00'
CUTOVER = '2026-10-12T10:05:00+08:00'


def preserve_config(app):
    directory = app.parent / 'previous'
    directory.mkdir()
    shutil.copytree(app.parent / 'sql', directory / 'sql')
    shutil.copyfile(app.parent / 'rules.yaml', directory / 'rules.yaml')
    raw = yaml.safe_load(app.read_text())
    raw['config'] = {'convergence_rules_path': str(directory / 'rules.yaml'),
                     'sql_directory': str(directory / 'sql')}
    previous = directory / 'app.yaml'
    previous.write_text(yaml.safe_dump(raw))
    return previous


def add_task(app):
    path = app.parent / 'rules.yaml'
    rules = yaml.safe_load(path.read_text())
    rule = dict(rules['convergence_rules'][0], taskname='added', target_table='added_target',
                sql_id='added', depends_on=['simple'])
    rules['convergence_rules'].append(rule)
    path.write_text(yaml.safe_dump(rules))
    (app.parent / 'sql/added.sql').write_text('INSERT INTO added_target SELECT 1')


def dump_database(app):
    path = app.parent / 'state/state.sqlite3'
    with closing(sqlite3.connect(f'{path.as_uri()}?mode=ro', uri=True)) as connection:
        return list(connection.iterdump())


def test_additive_migration_preserves_success_and_uses_explicit_cutover(local_http_app):
    path, requests = local_http_app
    original = Application(path)
    original.initialize(START)
    assert original.run_once('2026-10-12T10:06:00+08:00').exit_code == 0
    before = original.status().payload
    add_task(path)
    target = Application(path)
    plan = preview_config(target, effective_from=CUTOVER, allow_backfill=True)
    assert plan['changes']['added'] == ['added'] and plan['blockers'] == []
    result = apply_config(target, plan['plan_id'], effective_from=CUTOVER, allow_backfill=True)
    assert result['status'] == 'APPLIED' and Path(result['backup_path']).is_file()
    assert target.status().payload == before
    with closing(StateStore(path.parent / 'state/state.sqlite3')) as store:
        starts = store.get_task_effective_starts()
        assert starts['simple'].isoformat() == START
        assert starts['added'].isoformat() == CUTOVER
        assert store.configuration_snapshot()['revision'] == result['revision']
    assert len(requests) == 1
    assert target.run_once('2026-10-12T10:06:00+08:00').exit_code == 0
    assert len(requests) == 1  # The added task must not backfill before its start.
    assert target.run_once('2026-10-12T10:11:00+08:00').exit_code == 0
    assert len(requests) == 3  # Existing and added tasks each execute the new window.


def test_preview_is_read_only_and_sql_change_requires_migration(app, monkeypatch):
    original = Application(app)
    original.initialize(START)
    (app.parent / 'sql/simple.sql').write_text('INSERT INTO target SELECT 42')
    monkeypatch.setenv('LOG_PATH', str(app.parent / 'logs'))
    before = dump_database(app)
    target = Application(app)
    plan = preview_config(target, effective_from=CUTOVER, allow_backfill=True)
    assert plan['changes']['changed'] == ['simple']
    assert plan['blockers'] == [] and dump_database(app) == before
    assert not (app.parent / 'logs').exists()
    with pytest.raises(StartupError, match='CONFIG_MIGRATION_REQUIRED'):
        target.run_once('2026-10-12T10:06:00+08:00')


def test_legacy_ledger_requires_matching_old_config_and_trusted_sql_confirmation(app):
    original = Application(app)
    original.initialize(START)
    previous = preserve_config(app)
    with closing(sqlite3.connect(app.parent / 'state/state.sqlite3')) as connection, connection:
        connection.execute("DELETE FROM metadata WHERE key='configuration_snapshot'")
        connection.execute("DELETE FROM metadata WHERE key='configuration_required'")
        connection.execute('DROP TABLE configuration_history')
    add_task(app)
    target = Application(app)
    with pytest.raises(StartupError, match='CONFIG_PREVIOUS_CONFIG_REQUIRED'):
        preview_config(target, effective_from=CUTOVER, allow_backfill=True)
    plan = preview_config(target, previous_config=previous, effective_from=CUTOVER, allow_backfill=True)
    assert 'accept_legacy_baseline' in plan['required_confirmations']
    before = dump_database(app)
    with pytest.raises(StartupError, match='CONFIG_CONFIRMATION_REQUIRED'):
        apply_config(target, plan['plan_id'], previous_config=previous,
                     effective_from=CUTOVER, allow_backfill=True)
    assert dump_database(app) == before
    result = apply_config(target, plan['plan_id'], previous_config=previous,
                         effective_from=CUTOVER, allow_backfill=True, accept_legacy_baseline=True)
    assert result['status'] == 'APPLIED'


def test_connection_change_requires_same_data_confirmation(app):
    Application(app).initialize(START)
    raw = yaml.safe_load(app.read_text())
    raw['clickhouse']['host'] = 'new-endpoint.invalid'
    raw['clickhouse']['username'] = 'private-account'
    app.write_text(yaml.safe_dump(raw))
    target = Application(app)
    plan = preview_config(target)
    assert plan['changes']['connection_changed'] is True
    assert plan['required_confirmations'] == ['confirm_same_data']
    assert 'private-account' not in json.dumps(plan)
    with pytest.raises(StartupError, match='CONFIG_CONFIRMATION_REQUIRED'):
        apply_config(target, plan['plan_id'])
    assert apply_config(target, plan['plan_id'], confirm_same_data=True)['status'] == 'APPLIED'


def test_dataset_change_cannot_reuse_existing_execution_facts(app):
    raw = yaml.safe_load(app.read_text())
    raw['state']['dataset_id'] = 'business-a'
    app.write_text(yaml.safe_dump(raw))
    Application(app).initialize(START)
    raw['state']['dataset_id'] = 'business-b'
    app.write_text(yaml.safe_dump(raw))
    target = Application(app)
    plan = preview_config(target)
    assert 'CONFIG_DATASET_CHANGED' in [row['code'] for row in plan['blockers']]
    with pytest.raises(StartupError, match='CONFIG_MIGRATION_BLOCKED'):
        apply_config(target, plan['plan_id'], confirm_same_data=True)


def test_changed_task_with_unknown_is_blocked_but_addition_preserves_it(app):
    original = Application(app)
    original.initialize(START)
    item = original.definition.builder.create_instance(
        original.definition.dag, 'simple', datetime.fromisoformat('2026-10-12T10:06:00+08:00'))
    from data_converge.dag import TaskStatus
    with closing(StateStore(app.parent / 'state/state.sqlite3')) as store:
        store.acquire_lock()
        store.enqueue_and_advance([item], {})
        store.claim(item.instance_id, now=item.data_end)
        store.finish(item.instance_id, TaskStatus.UNKNOWN)
    sql = app.parent / 'sql/simple.sql'
    previous_sql = sql.read_text()
    sql.write_text('INSERT INTO target SELECT 42')
    plan = preview_config(Application(app), effective_from=CUTOVER, allow_backfill=True)
    assert 'CONFIG_TASKS_UNRESOLVED' in [row['code'] for row in plan['blockers']]
    sql.write_text(previous_sql)
    add_task(app)
    target = Application(app)
    plan = preview_config(target, effective_from=CUTOVER, allow_backfill=True)
    assert plan['blockers'] == []
    apply_config(target, plan['plan_id'], effective_from=CUTOVER, allow_backfill=True)
    assert target.status().payload['counts']['UNKNOWN'] == 1


def test_stale_plan_and_backup_failure_leave_original_ledger(app, monkeypatch):
    Application(app).initialize(START)
    add_task(app)
    plan = preview_config(Application(app), effective_from=CUTOVER, allow_backfill=True)
    sql = app.parent / 'sql/added.sql'
    sql.write_text('INSERT INTO added_target SELECT 2')
    target = Application(app)
    before = dump_database(app)
    with pytest.raises(StartupError, match='CONFIG_PLAN_STALE'):
        apply_config(target, plan['plan_id'], effective_from=CUTOVER, allow_backfill=True)
    plan = preview_config(target, effective_from=CUTOVER, allow_backfill=True)

    def fail_backup(self, destination):
        raise OSError('backup unavailable')

    monkeypatch.setattr(StateStore, 'backup_configuration', fail_backup)
    with pytest.raises(OSError, match='backup unavailable'):
        apply_config(target, plan['plan_id'], effective_from=CUTOVER, allow_backfill=True)
    assert dump_database(app) == before


def test_repeated_apply_is_idempotent_and_old_snapshot_remains_in_backup(app):
    Application(app).initialize(START)
    before = dump_database(app)
    add_task(app)
    target = Application(app)
    plan = preview_config(target, effective_from=CUTOVER, allow_backfill=True)
    first = apply_config(target, plan['plan_id'], effective_from=CUTOVER, allow_backfill=True)
    after = dump_database(app)
    again = apply_config(target, plan['plan_id'], effective_from=CUTOVER, allow_backfill=True)
    assert again['status'] == 'ALREADY_APPLIED'
    assert again['backup_path'] == first['backup_path'] and dump_database(app) == after
    with closing(sqlite3.connect(first['backup_path'])) as connection:
        assert list(connection.iterdump()) == before
