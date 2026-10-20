"""Migration CLI stays explicit, read-only for plans, and safe in diagnostics."""

from contextlib import closing
from datetime import datetime, timedelta
import json
import sys
from types import ModuleType

import pytest
import yaml

from data_converge import main as entry
from data_converge.application import Application
from data_converge.config.application import StartupError
from data_converge.config.evolution import build_manifest, manifest_digest
from data_converge.errors import safe_error_details
from data_converge.state import StateStore

from .test_diagnostics import run_json, seed
from .test_main import app


def save_snapshot(app, store):
    application = Application(app)
    manifest = build_manifest(application.definition)
    snapshot = store.configuration_snapshot()
    if snapshot is None:
        start = datetime.fromisoformat(store.get_meta('coverage_start'))
        store.apply_configuration(
            expected_binding=application.fingerprint, expected_revision=None, manifest=manifest,
            revision=manifest_digest(manifest), task_starts={'simple': start},
            binding=application.fingerprint, migration_id='cli-baseline', applied_at=start)


def install_service(monkeypatch, **operations):
    module = ModuleType('data_converge.configuration_migration.service')
    for name, operation in operations.items():
        setattr(module, name, operation)
    monkeypatch.setitem(sys.modules, module.__name__, module)


def test_plan_cli_returns_complete_public_plan_without_logs_or_state(app, capsys, monkeypatch):
    expected = {'plan_id': 'a' * 64, 'blockers': [], 'required_confirmations': ['confirm_same_data'],
                'changes': {'added': ['new'], 'changed': [], 'removed': []},
                'task_starts': {'new': '2099-01-01T00:00:00+08:00'}}
    calls = []
    def preview(application, **options):
        calls.append((application.config_path, options))
        return expected
    def forbidden():
        pytest.fail('Configuration preview opened a logging session')
    install_service(monkeypatch, preview_config=preview)
    monkeypatch.setattr(entry, 'LoggingSession', forbidden)
    monkeypatch.setenv('LOG_PATH', str(app.parent / 'logs'))
    code, result = run_json(app, capsys, '--plan-config', '--previous-config', str(app),
                            '--effective-from', '2099-01-01T00:00:00+08:00', '--allow-backfill')
    assert code == 0 and result['command'] == 'plan-config' and result['data'] == expected
    assert calls == [(app, {'previous_config': app, 'effective_from': '2099-01-01T00:00:00+08:00',
                            'allow_backfill': True})]
    assert not (app.parent / 'logs').exists() and not (app.parent / 'state').exists()


def test_plan_cli_blockers_fail_without_hiding_the_reviewable_plan(app, capsys, monkeypatch):
    expected = {'plan_id': 'a' * 64, 'blockers': [{'code': 'CONFIG_TASKS_UNRESOLVED'}],
                'required_confirmations': [], 'changes': {'changed': ['simple']}}
    install_service(monkeypatch, preview_config=lambda *args, **kwargs: expected)
    code, result = run_json(app, capsys, '--plan-config')
    assert code == 1 and result['status'] == 'FAIL' and result['data'] == expected


def test_plan_cli_renders_plan_id_and_required_actions_as_text(app, capsys, monkeypatch):
    expected = {'plan_id': 'b' * 64, 'blockers': [], 'required_confirmations': ['confirm_same_data'],
                'changes': {'added': ['task_new']}, 'task_starts': {'task_new': '2099-01-01T00:00:00+08:00'}}
    install_service(monkeypatch, preview_config=lambda *args, **kwargs: expected)
    assert entry.main(['--config', str(app), '--plan-config']) == 0
    output = capsys.readouterr().out
    assert 'b' * 64 in output and 'confirm_same_data' in output and 'task_new' in output and '只读' in output


def test_apply_cli_forwards_every_authorization_and_returns_json(app, capsys, monkeypatch):
    calls = []
    def apply(application, plan_id, **options):
        calls.append((application.config_path, plan_id, options))
        return {'status': 'APPLIED', 'plan_id': plan_id}
    install_service(monkeypatch, apply_config=apply)
    args = ['--config', str(app), '--apply-config', 'c' * 64, '--previous-config', str(app),
            '--effective-from', '2099-01-01T00:00:00+08:00', '--allow-backfill',
            '--confirm-same-data', '--confirm-readiness-change', '--accept-legacy-baseline',
            '--backup-dir', str(app.parent / 'backup')]
    assert entry.main(args) == 0
    assert json.loads(capsys.readouterr().out) == {'status': 'APPLIED', 'plan_id': 'c' * 64}
    assert calls == [(app, 'c' * 64, {'previous_config': app,
        'effective_from': '2099-01-01T00:00:00+08:00', 'allow_backfill': True,
        'confirm_same_data': True, 'confirm_readiness_change': True,
        'accept_legacy_baseline': True, 'backup_dir': app.parent / 'backup'})]


@pytest.mark.parametrize('options', [
    ['--previous-config', 'old.yaml'], ['--effective-from', '2099-01-01T00:00:00+08:00'],
    ['--allow-backfill'], ['--confirm-same-data'], ['--confirm-readiness-change'],
    ['--accept-legacy-baseline'], ['--backup-dir', 'backup'], ['--apply-config', ''],
    ['--plan-config', '--apply-config', 'id'], ['--plan-config', '--run-once'],
    ['--plan-config', '--confirm-same-data'], ['--plan-config', '--confirm-readiness-change'],
    ['--plan-config', '--accept-legacy-baseline'], ['--plan-config', '--backup-dir', 'backup'],
    ['--plan-config', '--online'], ['--plan-config', '--limit', '2'],
    ['--plan-config', '--scheduled-time', '2099-01-01T00:00:00+08:00'],
    ['--plan-config', '--note', 'note'], ['--plan-config', '--resolution', 'success'],
    ['--plan-config', '--start-time', ''], ['--plan-config', '--now', ''],
    ['--diagnose', '--allow-backfill'], ['--apply-config', 'id', '--format', 'json'],
])
def test_migration_parameters_cannot_be_used_by_another_command(app, options):
    with pytest.raises(SystemExit) as error:
        entry.main(['--config', str(app), *options])
    assert error.value.code == 2
    assert not (app.parent / 'state').exists()


def test_diagnose_warns_on_sql_change_without_exposing_or_overwriting_snapshot(app, capsys):
    store, _ = seed(app, count=1)
    save_snapshot(app, store)
    before = store.path.read_bytes()
    store.close()
    (app.parent / 'sql/simple.sql').write_text('INSERT INTO target SELECT 7')
    _, result = run_json(app, capsys, '--diagnose')
    state = next(item for item in result['checks'] if item['id'] == 'state')
    assert state['status'] == 'WARN'
    assert state['evidence']['configuration_matches'] is False
    assert state['evidence']['configuration_enforced'] is False
    assert state['evidence']['issue'] == 'DC-STATE-002'
    assert 'error_code' not in state['evidence']
    assert state['evidence']['configuration_changes']['changed'] == ['simple']
    assert '--plan-config' in state['action']
    evidence = json.dumps(state['evidence'])
    assert '127.0.0.1' not in evidence and 'example' not in evidence and 'INSERT INTO' not in evidence
    assert (app.parent / 'state/state.sqlite3').read_bytes() == before


def test_inspection_skips_current_dependencies_after_sql_only_change(app, capsys):
    store, instances = seed(app, count=1)
    save_snapshot(app, store)
    store.close()
    (app.parent / 'sql/simple.sql').write_text('INSERT INTO target SELECT 8')
    _, result = run_json(app, capsys, '--inspect-instance', instances[0].instance_id)
    dependencies = next(item for item in result['checks'] if item['id'] == 'dependencies')
    assert dependencies['status'] == 'SKIP'
    assert '临时' in dependencies['action']
    assert 'dependencies' not in result['data'] and 'sql_file' not in result['data']
    assert result['data']['instance']['status'] == 'UNKNOWN'


def test_diagnostics_reject_corrupt_snapshot_revision_without_exposing_it(app, capsys):
    store, _ = seed(app, count=1)
    save_snapshot(app, store)
    snapshot = store.configuration_snapshot()
    snapshot['revision'] = 'private-corrupt-revision'
    store.set_meta('configuration_snapshot', json.dumps(snapshot))
    store.close()
    _, result = run_json(app, capsys, '--diagnose')
    state = next(item for item in result['checks'] if item['id'] == 'state')
    assert state['status'] == 'FAIL' and state['evidence']['error_code'] == 'CONFIG_STATE_INVALID'
    assert 'private-corrupt-revision' not in json.dumps(result)


def test_required_missing_snapshot_cannot_be_diagnosed_as_legacy(app, capsys):
    store, instances = seed(app, count=1)
    store.set_meta('configuration_required', '1')
    with store._connection(write=True) as connection:
        connection.execute("DELETE FROM metadata WHERE key='configuration_snapshot'")
    before = store.path.read_bytes()
    store.close()
    _, result = run_json(app, capsys, '--diagnose')
    state = next(item for item in result['checks'] if item['id'] == 'state')
    assert state['status'] == 'FAIL' and state['evidence']['error_code'] == 'CONFIG_STATE_INVALID'
    _, detail = run_json(app, capsys, '--inspect-instance', instances[0].instance_id)
    dependencies = next(item for item in detail['checks'] if item['id'] == 'dependencies')
    assert dependencies['status'] == 'FAIL'
    assert dependencies['evidence']['error_code'] == 'CONFIG_STATE_INVALID'
    assert 'dependencies' not in detail['data']
    assert (app.parent / 'state/state.sqlite3').read_bytes() == before


def test_invalid_definition_does_not_hide_required_missing_snapshot(app, capsys):
    store, _ = seed(app, count=1)
    store.set_meta('configuration_required', '1')
    with store._connection(write=True) as connection:
        connection.execute("DELETE FROM metadata WHERE key='configuration_snapshot'")
    store.close()
    (app.parent / 'rules.yaml').unlink()
    _, result = run_json(app, capsys, '--diagnose')
    state = next(item for item in result['checks'] if item['id'] == 'state')
    assert state['status'] == 'FAIL' and state['evidence']['error_code'] == 'CONFIG_STATE_INVALID'


@pytest.mark.parametrize('starts', [
    {}, [], None, {'other': '2026-10-12T10:00:00+08:00'},
    {'simple': '2026-10-12T10:00:00'}, {'simple': '2026-10-12T09:55:00+08:00'},
    {'simple': 'private-invalid-time'},
])
def test_diagnostics_and_startup_reject_invalid_task_starts_consistently(app, capsys, starts):
    store, _ = seed(app, count=1)
    snapshot = store.configuration_snapshot()
    snapshot['task_starts'] = starts
    store.set_meta('configuration_snapshot', json.dumps(snapshot))
    before = store.path.read_bytes()
    store.close()
    _, result = run_json(app, capsys, '--diagnose')
    state = next(item for item in result['checks'] if item['id'] == 'state')
    assert state['status'] == 'FAIL' and state['evidence']['error_code'] == 'CONFIG_STATE_INVALID'
    assert 'private-invalid-time' not in json.dumps(result)
    with pytest.raises(StartupError, match='^CONFIG_STATE_INVALID$'):
        Application(app).run_once('2026-10-12T10:06:00+08:00')
    assert (app.parent / 'state/state.sqlite3').read_bytes() == before


def test_migrated_old_instance_does_not_inherit_current_task_dependencies(app, capsys):
    store, instances = seed(app, count=1)
    store.resolve(instances[0].instance_id, 'SUCCESS', 'confirmed historical execution')
    store.close()
    rules_path = app.parent / 'rules.yaml'
    rules = yaml.safe_load(rules_path.read_text())
    parent = dict(rules['convergence_rules'][0], taskname='new_parent', sql_id='new_parent')
    rules['convergence_rules'][0]['depends_on'] = ['new_parent']
    rules['convergence_rules'].append(parent)
    rules_path.write_text(yaml.safe_dump(rules))
    (app.parent / 'sql/new_parent.sql').write_text('INSERT INTO target SELECT 1')
    effective = '2099-01-01T00:00:00+08:00'
    code, plan = run_json(app, capsys, '--plan-config', '--effective-from', effective)
    assert code == 0
    assert entry.main(['--config', str(app), '--apply-config', plan['data']['plan_id'],
                       '--effective-from', effective]) == 0
    capsys.readouterr()
    _, result = run_json(app, capsys, '--inspect-instance', instances[0].instance_id)
    dependency = next(item for item in result['checks'] if item['id'] == 'dependencies')
    assert dependency['status'] == 'SKIP' and '生效' in dependency['summary']
    assert result['data']['instance']['status'] == 'SUCCESS'
    assert 'dependencies' not in result['data'] and 'sql_file' not in result['data']
    current = Application(app).definition
    instance = current.builder.create_instance(current.dag, 'simple',
        datetime.fromisoformat(effective) + timedelta(minutes=6))
    with closing(StateStore(app.parent / 'state/state.sqlite3')) as store:
        store.acquire_lock()
        store.enqueue_and_advance([instance], {})
    _, current_result = run_json(app, capsys, '--inspect-instance', instance.instance_id)
    assert current_result['data']['dependencies']['total'] == 1
    assert current_result['data']['dependencies']['blocked'][0]['status'] == 'NOT_FOUND'


def test_real_cli_plan_is_read_only_then_apply_preserves_success(app, capsys, monkeypatch):
    store, instances = seed(app, count=1)
    save_snapshot(app, store)
    store.resolve(instances[0].instance_id, 'SUCCESS', 'verified earlier execution')
    fact = store.get_instance(instances[0].instance_id)
    before = store.path.read_bytes()
    store.close()
    monkeypatch.setenv('LOG_PATH', str(app.parent / 'logs'))
    (app.parent / 'sql/simple.sql').write_text('INSERT INTO target SELECT 99')
    effective = '2099-01-01T00:00:00+08:00'
    code, result = run_json(app, capsys, '--plan-config', '--effective-from', effective)
    assert code == 0 and result['data']['changes']['changed'] == ['simple']
    assert result['data']['blockers'] == [] and result['data']['required_confirmations'] == []
    assert (app.parent / 'state/state.sqlite3').read_bytes() == before
    assert not (app.parent / 'logs').exists()
    def forbidden(*args, **kwargs):
        pytest.fail('Migration attempted runtime execution or RUNNING recovery')
    monkeypatch.setattr(Application, '_runtime', forbidden)
    monkeypatch.setattr(StateStore, 'recover_running', forbidden)
    plan_id = result['data']['plan_id']
    assert entry.main(['--config', str(app), '--apply-config', plan_id,
                       '--effective-from', effective, '--backup-dir', str(app.parent / 'backups')]) == 0
    applied = json.loads(capsys.readouterr().out)
    assert applied['status'] == 'APPLIED' and applied['plan_id'] == plan_id
    with closing(StateStore(app.parent / 'state/state.sqlite3')) as check_store:
        assert check_store.get_instance(instances[0].instance_id) == fact
        assert check_store.get_task_effective_starts()['simple'] == datetime.fromisoformat(effective)
        assert check_store.configuration_history()[0]['migration_id'] == plan_id
    assert list((app.parent / 'backups').glob('*.sqlite3'))


def test_real_legacy_plan_requires_trusted_previous_config_without_writes(app, capsys, monkeypatch):
    Application(app).initialize('2026-10-12T10:00:00+08:00')
    with closing(StateStore(app.parent / 'state/state.sqlite3')) as store:
        store.acquire_lock()
        with store._connection(write=True) as connection:
            connection.execute("DELETE FROM metadata WHERE key IN ('configuration_snapshot','configuration_required')")
            connection.execute('DROP TABLE IF EXISTS configuration_history')
        before = store.path.read_bytes()
    monkeypatch.setenv('LOG_PATH', str(app.parent / 'logs'))
    code, missing = run_json(app, capsys, '--plan-config')
    assert code == 1
    assert missing['checks'][0]['evidence']['error_code'] == 'CONFIG_PREVIOUS_CONFIG_REQUIRED'
    code, result = run_json(app, capsys, '--plan-config', '--previous-config', str(app))
    assert code == 0 and result['data']['legacy'] is True
    assert result['data']['required_confirmations'] == ['accept_legacy_baseline']
    assert (app.parent / 'state/state.sqlite3').read_bytes() == before
    assert not (app.parent / 'logs').exists()


@pytest.mark.parametrize('code', [
    'DATASET_ID_INVALID', 'CONFIGURATION_MANIFEST_INVALID', 'CONFIGURATION_SQL_UNREADABLE',
    'CONFIG_MIGRATION_REQUIRED', 'CONFIG_PREVIOUS_CONFIG_REQUIRED', 'CONFIG_PREVIOUS_BINDING_MISMATCH',
    'CONFIG_PREVIOUS_RESOURCES_REQUIRED', 'CONFIG_CONFIRMATION_REQUIRED', 'CONFIG_DATASET_CHANGED',
    'CONFIG_TASKS_UNRESOLVED', 'CONFIG_PLAN_STALE', 'CONFIG_MIGRATION_BLOCKED',
    'CONFIG_EFFECTIVE_FROM_REQUIRED', 'CONFIG_EFFECTIVE_FROM_INVALID', 'CONFIG_EFFECTIVE_FROM_PAST',
    'CONFIG_STATE_INVALID', 'CONFIGURATION_STATE_CHANGED', 'TASK_WINDOW_BEFORE_EFFECTIVE_START',
    'CONFIG_COORDINATION_CHANGED', 'CONFIG_RUNNING_INSTANCES', 'CONFIG_EFFECTIVE_BEFORE_COVERAGE',
    'CONFIG_TASK_WINDOW_OVERLAP', 'CONFIGURATION_TRANSACTION_ACTIVE', 'CONFIGURATION_BACKUP_INVALID',
])
def test_migration_errors_have_public_safe_codes(code):
    assert safe_error_details(ValueError(code))['error_code'] == code
