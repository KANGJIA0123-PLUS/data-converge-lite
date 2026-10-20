"""Historical windows remain visible without being presented as active work."""

from contextlib import closing
from datetime import datetime, timedelta
import json

import pytest
import yaml

from data_converge import main as entry
from data_converge.application import Application
from data_converge.config.evolution import manifest_digest
from data_converge.config.state_binding import fenced_binding
from data_converge.dag import TaskStatus
from data_converge.diagnostics.ledger import DiagnosticLedger
from data_converge.diagnostics.windows import list_windows
from data_converge.dwd_ready.message import DWD_TYPES
from data_converge.errors import safe_error_details
from data_converge.state import StateStore

from .test_diagnostics import run_json
from .test_main import app


START = datetime.fromisoformat('2026-10-14T15:15:00+08:00')
CUTOVER = datetime.fromisoformat('2099-01-01T00:00:00+08:00')
HISTORICAL_COUNTS = {'SUCCESS': 1, 'UNKNOWN': 1, 'FAILED': 1,
                     'BLOCKED': 1, 'PENDING': 1, 'RUNNING': 2}


def erase_new_baseline(store):
    with store._connection(write=True) as connection:
        connection.execute("DELETE FROM metadata WHERE key IN ('configuration_snapshot','configuration_required')")
        connection.execute('DROP TABLE IF EXISTS configuration_history')
    store.set_meta('binding', 'a' * 64)


def seed_historical_states(application, store):
    definition = application.definition
    instances = [definition.builder.create_instance(definition.dag, 'simple',
                 START + timedelta(minutes=5 * (index + 1))) for index in range(7)]
    store.enqueue_and_advance(instances, {})
    for index, status in enumerate((TaskStatus.SUCCESS, TaskStatus.UNKNOWN, TaskStatus.FAILED)):
        assert store.claim(instances[index].instance_id, now=instances[index].data_end)
        store.finish(instances[index].instance_id, status, now=instances[index].data_end)
    store.mark_blocked(instances[3].instance_id, 'UPSTREAM_NOT_READY')
    assert store.claim(instances[5].instance_id, now=instances[5].data_end)
    assert store.prepare_attempt(instances[6].instance_id, now=instances[6].data_end)
    return instances


@pytest.fixture
def retired_ledger(app):
    application = Application(app)
    application.initialize(START.isoformat())
    with closing(StateStore(app.parent / 'state/state.sqlite3')) as store:
        store.acquire_lock()
        instances = seed_historical_states(application, store)
        erase_new_baseline(store)
        backup = store.backup_configuration(app.parent / 'before-recovery.sqlite3')
        revision = manifest_digest(application.manifest)
        recovery = dict(schema_version=1, migration_id='b' * 64, cutover=CUTOVER.isoformat(),
            previous_binding='a' * 64, current_binding=fenced_binding(application.fingerprint), revision=revision,
            backup_path=str(backup), recovered_at=CUTOVER.isoformat(), identity_verified=False,
            previous_definition_available=False, historical_counts=HISTORICAL_COUNTS)
        store.apply_configuration(expected_binding='a' * 64, expected_revision=None,
            manifest=application.manifest, revision=revision, task_starts={'simple': CUTOVER},
            binding=fenced_binding(application.fingerprint), migration_id='b' * 64, applied_at=CUTOVER,
            backup_path=backup, retire_existing=True, recovery=recovery)
        yield store, instances


def active_instance(app, store, *, prepared=False):
    definition = Application(app).definition
    instance = definition.builder.create_instance(definition.dag, 'simple', CUTOVER + timedelta(minutes=5))
    store.enqueue_and_advance([instance], {})
    if prepared:
        assert store.prepare_attempt(instance.instance_id, now=instance.data_end)
    else:
        assert store.claim(instance.instance_id, now=instance.data_end)
        store.finish(instance.instance_id, TaskStatus.UNKNOWN, now=instance.data_end)
    return instance


def test_diagnose_separates_active_results_and_historical_running(app, capsys, retired_ledger):
    store, _ = retired_ledger
    before = store.path.read_bytes()
    _, result = run_json(app, capsys, '--diagnose')
    checks = {item['id']: item for item in result['checks']}
    assert checks['tasks']['status'] == 'PASS' and checks['tasks']['evidence'] == {}
    historical = checks['historical-tasks']
    assert historical['status'] == 'WARN' and historical['evidence']['counts'] == HISTORICAL_COUNTS
    assert historical['evidence']['cutover'] == CUTOVER.isoformat()
    assert historical['evidence']['identity_verified'] is False
    assert '当前不会自动处理' in historical['summary']
    assert checks['state']['status'] == 'PASS'
    assert checks['state']['evidence']['counts'] == {}
    assert checks['state']['evidence']['oldest_active_window'] is None
    assert store.path.read_bytes() == before


def test_active_inspection_accepts_fenced_recovered_binding(app, capsys, retired_ledger):
    store, _ = retired_ledger
    instance = active_instance(app, store)
    _, result = run_json(app, capsys, '--inspect-instance', instance.instance_id)
    dependencies = next(item for item in result['checks'] if item['id'] == 'dependencies')
    assert dependencies['status'] == 'PASS'
    assert result['data']['dependencies'] == {
        'total': 0, 'blocked_count': 0, 'blocked': [], 'truncated': False}
    assert 'retirement' not in result['data']


@pytest.mark.parametrize('index', [1, 2, 5, 6])
def test_inspect_marks_historical_and_does_not_recommend_retry(app, capsys, retired_ledger, index):
    store, instances = retired_ledger
    original = store.get_instance(instances[index].instance_id)
    code, result = run_json(app, capsys, '--inspect-instance', instances[index].instance_id)
    checks = {item['id']: item for item in result['checks']}
    assert code == 0 and checks['historical-instance']['status'] == 'WARN'
    assert result['data']['instance']['historical'] is True
    assert result['data']['retirement']['migration_id'] == 'b' * 64
    assert result['data']['instance']['status'] == original['status']
    assert checks['dependencies']['status'] == 'SKIP'
    assert 'dependencies' not in result['data'] and 'sql_file' not in result['data']
    for action in (result['data']['action'], result['data']['instance']['action']):
        assert 'resolve' not in action and 'retry' not in action and '重试' not in action
        assert '当前不会自动处理' in action
    assert store.get_instance(instances[index].instance_id) == original


def test_failures_lists_only_active_problems_and_retains_historical_counts(app, capsys, retired_ledger):
    store, _ = retired_ledger
    code, result = run_json(app, capsys, '--failures')
    assert code == 0 and result['data']['total'] == 0 and result['data']['instances'] == []
    assert result['data']['historical_counts'] == HISTORICAL_COUNTS
    current = active_instance(app, store)
    code, result = run_json(app, capsys, '--failures')
    assert code == 1 and result['data']['total'] == 1
    assert [row['instance_id'] for row in result['data']['instances']] == [current.instance_id]
    assert result['data']['historical_counts'] == HISTORICAL_COUNTS


def test_recovery_plan_never_auto_resumes_historical_prepared_instances(app, capsys, retired_ledger):
    store, instances = retired_ledger
    code, result = run_json(app, capsys, '--recovery-plan')
    assert code == 0 and result['data']['counts'] == {'AUTO_RESUME': 0, 'RECONCILE': 0}
    assert result['data']['candidates'] == [] and result['data']['historical_counts'] == HISTORICAL_COUNTS
    historical = next(item for item in result['checks'] if item['id'] == 'historical-tasks')
    assert historical['status'] == 'WARN'
    current = active_instance(app, store, prepared=True)
    _, active = run_json(app, capsys, '--recovery-plan')
    assert active['data']['counts'] == {'AUTO_RESUME': 1, 'RECONCILE': 0}
    assert [row['instance_id'] for row in active['data']['candidates']] == [current.instance_id]
    assert store.get_instance(instances[6].instance_id)['status'] == 'RUNNING'


@pytest.mark.parametrize('command', ['--diagnose', '--recovery-plan', '--failures'])
def test_missing_retirement_mapping_is_not_reported_as_normal_work(app, capsys, retired_ledger, command):
    store, _ = retired_ledger
    with store._connection(write=True) as connection:
        connection.execute('DROP TABLE retired_instances')
    code, result = run_json(app, capsys, command)
    assert code == 1
    assert any(item['evidence'].get('error_code') == 'LEGACY_RETIREMENT_INVALID'
               for item in result['checks'])


def test_missing_baseline_plan_explains_normal_start_recovery_without_applying(app, capsys, monkeypatch):
    Application(app).initialize(START.isoformat())
    with closing(StateStore(app.parent / 'state/state.sqlite3')) as store:
        store.acquire_lock()
        erase_new_baseline(store)
        before = store.path.read_bytes()
    monkeypatch.setenv('LOG_PATH', str(app.parent / 'logs'))
    code, result = run_json(app, capsys, '--plan-config')
    item = result['checks'][0]
    assert code == 1 and item['evidence']['error_code'] == 'CONFIG_PREVIOUS_CONFIG_REQUIRED'
    assert '正常启动' in item['action'] and '新窗口' in item['action']
    assert '旧窗口' in item['action'] and 'UNKNOWN' in item['action']
    assert 'cutover' not in result['data'] and not (app.parent / 'logs').exists()
    assert (app.parent / 'state/state.sqlite3').read_bytes() == before


def test_legacy_mismatch_diagnose_warns_without_changing_old_ledger(app, capsys):
    Application(app).initialize(START.isoformat())
    with closing(StateStore(app.parent / 'state/state.sqlite3')) as store:
        store.acquire_lock()
        erase_new_baseline(store)
        before = store.path.read_bytes()
    _, result = run_json(app, capsys, '--diagnose')
    state = next(item for item in result['checks'] if item['id'] == 'state')
    assert state['status'] == 'WARN'
    assert state['evidence']['configuration_snapshot_present'] is False
    assert state['evidence']['binding_matches'] is False
    assert state['evidence']['binding_enforced'] is False
    assert state['evidence']['issue'] == 'DC-STATE-002'
    assert (app.parent / 'state/state.sqlite3').read_bytes() == before


@pytest.mark.parametrize('elapsed, expected', [(0, 0), (6, 1), (106, 20)])
def test_recent_dwd_windows_start_at_recovery_cutover(retired_ledger, elapsed, expected):
    store, _ = retired_ledger
    before = store.path.read_bytes()
    with closing(DiagnosticLedger(store.path)) as reader:
        rows = list_windows(reader, {'wait_warning_seconds': 120},
                            now=CUTOVER + timedelta(minutes=elapsed))
    assert len(rows) == expected
    assert all(datetime.fromisoformat(row['window_start']) >= CUTOVER for row in rows)
    assert all(not row.get('historical', False) for row in rows)
    if elapsed == 6:
        assert rows[0]['status'] == 'WAITING' and rows[0]['warning_due'] is False
    assert store.path.read_bytes() == before


def test_exact_dwd_window_retains_historical_signals_without_current_warning(app, capsys, retired_ledger):
    store, _ = retired_ledger
    raw = yaml.safe_load(app.read_text())
    raw['dwd_ready'] = {'enabled': True, 'redis': {'mode': 'standalone', 'host': '127.0.0.1', 'port': 6379,
                                               'password_env': 'UNSET_DIAGNOSTIC_PASSWORD'}}
    app.write_text(yaml.safe_dump(raw))
    kind = sorted(DWD_TYPES)[0]
    store.record_dwd_signal(START, kind, START + timedelta(minutes=6))
    before = store.path.read_bytes()
    code, result = run_json(app, capsys, '--dwd-status', '--window', START.isoformat())
    row = result['data']['windows'][0]
    assert code == 0 and row['historical'] is True
    assert row['status'] == 'WAITING' and row['received'] == [kind]
    assert row['warning_due'] is False and row['recovery_cutover'] == CUTOVER.isoformat()
    assert result['checks'][0]['evidence']['late_windows'] == 0
    assert '历史' in result['checks'][0]['action']
    assert store.path.read_bytes() == before


@pytest.mark.parametrize('code', ['HISTORICAL_INSTANCE_RETIRED', 'LEGACY_RETIREMENT_INVALID',
                                  'LEGACY_RECOVERY_INVALID', 'LEGACY_RECOVERY_CONFIGURATION_CHANGED'])
def test_legacy_recovery_errors_have_safe_public_codes(code):
    assert safe_error_details(ValueError(code))['error_code'] == code
