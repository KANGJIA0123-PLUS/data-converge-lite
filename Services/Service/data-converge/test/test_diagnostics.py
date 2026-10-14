"""Read-only operational commands use the real ledger and never start work."""
import json
from datetime import datetime, timedelta

import pytest
import yaml

from data_converge import main as entry
from data_converge.application import Application
from data_converge.dag import TaskStatus
from data_converge.dwd_ready.message import DWD_TYPES
from data_converge.state import StateStore
from .test_main import app


def run_json(app, capsys, *args):
    code = entry.main(['--config', str(app), *args, '--format', 'json'])
    return code, json.loads(capsys.readouterr().out)


def test_diagnose_missing_config_reports_without_creating_logs(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv('LOG_PATH', str(tmp_path / 'logs'))
    code, result = run_json(tmp_path / 'missing.yaml', capsys, '--diagnose')
    assert code == 1
    assert result['checks'][0]['id'] == 'config'
    assert result['checks'][0]['status'] == 'FAIL'
    assert not (tmp_path / 'logs').exists()


@pytest.mark.parametrize('field, other', [
    ('convergence_rules_path', 'sql_directory'), ('sql_directory', 'convergence_rules_path'),
])
@pytest.mark.parametrize('invalid, error_code', [
    ('', 'CONFIG_PATH_INVALID'),
    ({'password': 'path-secret'}, 'CONFIG_PATH_INVALID'),
    ('$CODECHECK_MISSING_ASSET_PATH', 'CONFIG_PATH_ENV_MISSING'),
])
def test_diagnose_reports_path_errors_without_hiding_primary_error(
        app, capsys, monkeypatch, field, other, invalid, error_code):
    monkeypatch.delenv('CODECHECK_MISSING_ASSET_PATH', raising=False)
    raw = yaml.safe_load(app.read_text())
    raw['clickhouse']['port'] = 0
    raw['config'][field] = invalid
    app.write_text(yaml.safe_dump(raw))
    code, result = run_json(app, capsys, '--diagnose')
    checks = {item['id']: item for item in result['checks']}
    evidence = checks['definition']['evidence']
    assert code == 1 and checks['definition']['status'] == 'FAIL'
    assert evidence['error_code'] == 'CLICKHOUSE_CONNECTION_INVALID'
    assert evidence['path_errors'] == {field: {'error_type': 'StartupError', 'error_code': error_code}}
    assert evidence[other] == raw['config'][other] and evidence['config'] == str(app)
    assert {'logging.settings', 'heartbeat', 'state', 'connections'} <= checks.keys()
    assert 'path-secret' not in json.dumps(result)
    assert not (app.parent / 'state').exists()


def test_diagnose_redacts_multiple_path_exceptions_in_json_and_text(app, capsys, monkeypatch):
    from data_converge.diagnostics import checks
    from data_converge.diagnostics.report import render_text

    def fail_path(value, default, path):
        if default == 'sql':
            raise PermissionError(13, 'password=path-secret')
        raise RuntimeError('password=path-secret')

    raw = yaml.safe_load(app.read_text())
    raw['clickhouse']['port'] = 0
    app.write_text(yaml.safe_dump(raw))
    monkeypatch.setattr(checks, 'asset_path', fail_path)
    code, result = run_json(app, capsys, '--diagnose')
    item = next(item for item in result['checks'] if item['id'] == 'definition')
    assert code == 1 and item['status'] == 'FAIL'
    assert item['evidence']['error_code'] == 'CLICKHOUSE_CONNECTION_INVALID'
    assert item['evidence']['path_errors'] == {
        'convergence_rules_path': {'error_type': 'RuntimeError'},
        'sql_directory': {'error_type': 'PermissionError', 'os_errno': 13},
    }
    for output in (json.dumps(result), render_text(result)):
        assert 'path-secret' not in output
        assert 'RuntimeError' in output and 'PermissionError' in output


def test_diagnose_corrupt_ledger_continues_and_does_not_repair(app, capsys):
    directory = app.parent / 'state'
    directory.mkdir()
    path = directory / 'state.sqlite3'
    path.write_bytes(b'not a database')
    code, result = run_json(app, capsys, '--diagnose')
    checks = {item['id']: item for item in result['checks']}
    assert code == 1 and checks['state']['status'] == 'FAIL'
    assert 'heartbeat' in checks and checks['connections']['status'] == 'SKIP'
    assert path.read_bytes() == b'not a database'


def seed(app, count=25):
    application = Application(app)
    start = datetime.fromisoformat('2026-10-14T15:15:00+08:00')
    application.initialize(start.isoformat())
    store = StateStore(app.parent / 'state/state.sqlite3')
    store.acquire_lock()
    instances = [application.definition.builder.create_instance(
        application.definition.dag, 'simple', start + timedelta(minutes=5 * (i + 1)))
        for i in range(count)]
    store.enqueue_and_advance(instances, {})
    for i, instance in enumerate(instances):
        store.claim(instance.instance_id, now=instance.data_end)
        status = TaskStatus.UNKNOWN if i == 0 else TaskStatus.SUCCESS
        store.finish(instance.instance_id, status, error='CLICKHOUSE_HTTP_FAILED' if i == 0 else None)
    return store, instances


def test_failures_finds_old_unknown_while_writer_owns_lock(app, capsys):
    store, instances = seed(app)
    try:
        before = store.get_instance(instances[0].instance_id)
        code, result = run_json(app, capsys, '--failures', '--limit', '1')
        assert code == 1 and result['data']['total'] == 1
        assert result['data']['instances'][0]['instance_id'] == instances[0].instance_id
        assert 'query_id' in result['data']['instances'][0]
        assert store.get_instance(instances[0].instance_id) == before
        code, result = run_json(app, capsys, '--failures', '--offset', '1')
        assert code == 1 and result['data']['instances'] == []
    finally:
        store.close()


def test_dwd_status_reports_zero_signals_and_later_readiness(app, capsys):
    store, _ = seed(app, count=1)
    start = datetime.fromisoformat('2026-10-14T15:15:00+08:00')
    raw = yaml.safe_load(app.read_text())
    raw['dwd_ready'] = {'enabled': True, 'redis': {'mode': 'standalone', 'host': '127.0.0.1', 'port': 6379,
                                               'password_env': 'UNSET_DIAGNOSTIC_PASSWORD'}}
    app.write_text(yaml.safe_dump(raw))
    try:
        code, result = run_json(app, capsys, '--dwd-status', '--window', start.isoformat())
        row = result['data']['windows'][0]
        assert row['status'] == 'WAITING' and set(row['missing']) == DWD_TYPES
        for kind in DWD_TYPES:
            store.record_dwd_signal(start, kind, start + timedelta(minutes=6))
        code, result = run_json(app, capsys, '--dwd-status', '--window', start.isoformat())
        assert result['data']['windows'][0]['status'] == 'READY'
        assert result['data']['windows'][0]['missing'] == []
    finally:
        store.close()


def test_inspect_instance_does_not_depend_on_valid_rule_files(app, capsys):
    store, instances = seed(app, count=1)
    store.close()
    (app.parent / 'rules.yaml').unlink()
    code, result = run_json(app, capsys, '--inspect-instance', instances[0].instance_id)
    assert code == 1
    assert result['data']['instance']['status'] == 'UNKNOWN'
    assert any(item['id'] == 'dependencies' and item['status'] == 'SKIP' for item in result['checks'])
    assert result['data']['diagnostic'] is None


def test_diagnose_offline_does_not_create_remote_clients(app, monkeypatch, capsys):
    from data_converge.dwd_ready import redis_source
    from data_converge.sql_execution import config

    def forbidden(*args, **kwargs):
        pytest.fail('offline diagnostics must not resolve credentials or connect')

    monkeypatch.setattr(redis_source, 'create_source', forbidden)
    monkeypatch.setattr(config, 'resolve_credentials', forbidden)
    _, result = run_json(app, capsys, '--diagnose')
    assert any(item['id'] == 'connections' and item['status'] == 'SKIP' for item in result['checks'])
    assert not (app.parent / 'state').exists()


@pytest.mark.parametrize('args', [
    ['--online'], ['--status', '--window', 'bad'], ['--failures', '--limit', '0'],
    ['--dwd-status', '--window', '2026-10-14T16:15:00'],
    ['--failures', '--offset', '-1'], ['--diagnose', '--run-once'],
    ['--inspect-instance', ''], ['--inspect-instance', '   '], ['--failures', '--task', ''],
])
def test_diagnostic_options_cannot_accidentally_start_service(app, args):
    with pytest.raises(SystemExit) as error:
        entry.main(['--config', str(app), *args])
    assert error.value.code == 2
    assert not (app.parent / 'state').exists()


def test_inspect_text_exposes_evidence_without_a_large_json_blob(app, capsys):
    store, instances = seed(app, count=1)
    store.close()
    code = entry.main(['--config', str(app), '--inspect-instance', instances[0].instance_id])
    text = capsys.readouterr().out
    assert code == 1 and 'UNKNOWN' in text and instances[0].instance_id in text
    assert 'query_id:' in text
    assert '错误证据: 未记录' in text


def test_window_times_normalize_utc_and_wait_stops_after_ready(app):
    from data_converge.diagnostics.ledger import DiagnosticLedger
    from data_converge.diagnostics.windows import list_windows, parse_window

    store, _ = seed(app, count=1)
    start = parse_window('2026-10-14T07:15:00+00:00')
    for kind in DWD_TYPES:
        store.record_dwd_signal(start, kind, start + timedelta(minutes=6, seconds=6))
    store.close()
    reader = DiagnosticLedger(app.parent / 'state/state.sqlite3')
    try:
        rows = list_windows(reader, {'wait_warning_seconds': 120}, selected=start,
                            now=start + timedelta(hours=1))
        assert rows[0]['window_start'] == '2026-10-14T15:15:00+08:00'
        assert rows[0]['wait_seconds'] == 0 and rows[0]['ready_delay_seconds'] == 66
        assert rows[0]['warning_due'] is False
    finally:
        reader.close()


def test_corrupt_optional_evidence_does_not_hide_instance(app, capsys):
    store, instances = seed(app, count=1)
    row = store.get_instance(instances[0].instance_id)
    connection = store._connection(write=True)
    with connection:
        connection.execute('CREATE TABLE execution_diagnostics '
                           '(instance_id TEXT PRIMARY KEY, query_id TEXT, details TEXT)')
        connection.execute('INSERT INTO execution_diagnostics VALUES (?, ?, ?)',
                           (row['instance_id'], row['query_id'], '{broken json'))
    store.close()
    code, result = run_json(app, capsys, '--inspect-instance', row['instance_id'])
    assert code == 1
    assert result['data']['instance']['query_id'] == row['query_id']
    assert result['data']['instance']['status'] == 'UNKNOWN'
    assert any(item['id'] == 'execution-evidence' and item['status'] == 'FAIL' for item in result['checks'])


def test_invalid_coverage_is_not_reported_as_healthy(app, capsys):
    store, _ = seed(app, count=1)
    store.set_meta('coverage_start', 'invalid-time')
    store.close()
    _, result = run_json(app, capsys, '--diagnose')
    states = [item for item in result['checks'] if item['id'] == 'state']
    assert states and all(item['status'] != 'PASS' for item in states)


def test_diagnose_identifies_missing_sdk_without_importing_it(app, monkeypatch, capsys):
    import importlib.util

    original = importlib.util.find_spec
    monkeypatch.setattr(importlib.util, 'find_spec', lambda name: None if name == 'dsf' else original(name))
    _, result = run_json(app, capsys, '--diagnose')
    check = next(item for item in result['checks'] if item['id'] == 'runtime.modules')
    assert check['status'] == 'FAIL'
    assert check['evidence']['missing'] == ['dsf']


def test_diagnose_validates_logging_policy_without_creating_logs(app, capsys, monkeypatch):
    raw = yaml.safe_load(app.read_text())
    raw['logging'] = {'max_bytes': 0}
    app.write_text(yaml.safe_dump(raw))
    monkeypatch.setenv('LOG_PATH', str(app.parent / 'uncreated-logs'))
    code, result = run_json(app, capsys, '--diagnose')
    item = next(item for item in result['checks'] if item['id'] == 'logging.settings')
    assert code == 1 and item['status'] == 'FAIL'
    assert item['evidence']['error_code'] == 'LOGGING_SETTING_INVALID'
    assert any(item['id'] == 'state' for item in result['checks'])
    assert not (app.parent / 'uncreated-logs').exists()


def test_invalid_dependency_timestamp_does_not_hide_instance(app, capsys):
    store, instances = seed(app, count=1)
    connection = store._connection(write=True)
    with connection:
        connection.execute('UPDATE task_instances SET scheduled_time=?', ('invalid-time',))
    store.close()
    code, result = run_json(app, capsys, '--inspect-instance', instances[0].instance_id)
    assert code == 1 and result['data']['instance']['status'] == 'UNKNOWN'
    assert any(item['id'] == 'dependencies' and item['status'] == 'FAIL' for item in result['checks'])
