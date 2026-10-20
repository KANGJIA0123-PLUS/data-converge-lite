"""Archive diagnostics inspect policy and files without starting maintenance."""

from contextlib import closing
from datetime import datetime, timedelta, timezone
import json
import sqlite3

import pytest

from data_converge.application import Application
from data_converge.diagnostics.archives import archive_check
from data_converge.diagnostics.checks import protected_check
from data_converge.diagnostics.service import diagnose
from data_converge.errors import safe_error_details
from data_converge.state import StateStore

from .test_main import app
from .timezone_fixtures import deployment_zone

NOW = datetime(2026, 10, 19, 4, 0, tzinfo=timezone.utc)


def make_archive(directory, instant=NOW):
    if not (directory / 'state.sqlite3').exists():
        with closing(StateStore(directory / 'state.sqlite3', create=True)) as store:
            store.acquire_lock()
    directory = directory.parent / (directory.name + '-archives')
    directory.mkdir(exist_ok=True)
    name = 'daily-' + instant.strftime('%Y%m%dT%H%M%S%fZ') + '-' + 'a' * 32 + '.sqlite3.gz'
    path = directory / name
    path.write_bytes(b'file-presence-only-diagnostic')
    return path


def write_status(path, *, checked_at=NOW, status='PASS', **extra):
    value = dict(schema_version=1, checked_at=checked_at.isoformat(), status=status,
                 created=1, compressed=0, removed=0, latest_archive=str(path),
                 latest_snapshot_at=NOW.isoformat(), **extra)
    target = path.parent / 'maintenance.json'
    target.write_text(json.dumps(value))
    return target


def test_missing_archives_warn_without_creating_state(tmp_path):
    directory = tmp_path / 'state'
    result = archive_check({}, directory, now=NOW)
    assert result['status'] == 'WARN'
    assert result['evidence']['retention_days'] == 30
    assert result['evidence']['daily_archive_count'] == 0
    assert list(tmp_path.iterdir()) == []


def test_disabled_archives_are_skipped_without_reading_files(tmp_path):
    result = archive_check({'state': {'archives': {'enabled': False}}}, tmp_path / 'state', now=NOW)
    assert result['status'] == 'SKIP'
    assert result['evidence']['enabled'] is False
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize('value', [0, -1, True, '30'])
def test_invalid_retention_is_failure(tmp_path, value):
    result = protected_check('state.archives', archive_check,
                             {'state': {'archives': {'retention_days': value}}}, tmp_path / 'state')
    assert result['status'] == 'FAIL'
    assert result['evidence']['error_code'] == 'ARCHIVE_SETTINGS_INVALID'


def test_current_archive_and_recent_maintenance_are_reported_without_decompression(tmp_path):
    directory = tmp_path / 'state'
    path = make_archive(directory)
    status = write_status(path)
    before = {item: item.read_bytes() for item in path.parent.iterdir()}
    result = archive_check({}, directory, now=NOW)
    assert result['status'] == 'PASS'
    assert result['evidence']['latest_archive'] == str(path)
    assert result['evidence']['availability_check'] == 'regular_file_only'
    assert result['evidence']['maintenance']['status'] == 'PASS'
    assert {item: item.read_bytes() for item in path.parent.iterdir()} == before
    assert status.exists()


def test_stale_worker_status_warns_even_with_today_archive(tmp_path):
    path = make_archive(tmp_path / 'state')
    write_status(path, checked_at=NOW - timedelta(days=1))
    result = archive_check({}, tmp_path / 'state', now=NOW)
    assert result['status'] == 'WARN'
    assert result['evidence']['maintenance_stale'] is True


def test_daily_status_remains_current_without_minute_heartbeats(tmp_path, deployment_zone):
    deployment_zone('Asia/Shanghai')
    path = make_archive(tmp_path / 'state')
    write_status(path, checked_at=NOW - timedelta(hours=6))
    result = archive_check({}, tmp_path / 'state', now=NOW)
    assert result['status'] == 'PASS'
    assert result['evidence']['maintenance_stale'] is False


def test_lost_database_does_not_hide_existing_restore_archives(tmp_path):
    directory = tmp_path / 'state'
    path = make_archive(directory)
    write_status(path)
    (directory / 'state.sqlite3').unlink()
    result = archive_check({}, directory, now=NOW)
    assert result['evidence']['daily_archive_count'] == 1
    assert result['evidence']['latest_archive'] == str(path)
    assert not (directory / 'state.sqlite3').exists()


@pytest.mark.parametrize('content', ['{broken-json', 'x' * 65537, '["untrusted-status-body"]'])
def test_bad_worker_status_is_bounded_warning(tmp_path, content):
    path = make_archive(tmp_path / 'state')
    (path.parent / 'maintenance.json').write_text(content)
    result = archive_check({}, tmp_path / 'state', now=NOW)
    assert result['status'] == 'WARN'
    assert result['evidence']['error_code'] == 'ARCHIVE_STATUS_INVALID'
    assert content not in json.dumps(result)


def test_worker_error_is_safe_and_does_not_hide_archive(tmp_path):
    path = make_archive(tmp_path / 'state')
    write_status(path, status='WARN', error_code='ARCHIVE_TIMEOUT', password='never-show-this')
    result = archive_check({}, tmp_path / 'state', now=NOW)
    assert result['status'] == 'WARN'
    assert result['evidence']['maintenance']['error_code'] == 'ARCHIVE_TIMEOUT'
    assert result['evidence']['latest_archive'] == str(path)
    assert 'never-show-this' not in json.dumps(result)


def test_worker_stage_errors_are_bounded_and_sanitized(tmp_path):
    path = make_archive(tmp_path / 'state')
    errors = [{'code': 'ARCHIVE_MAINTENANCE_FAILED', 'stage': 'migration_backup',
               'error_type': 'OSError', 'password': 'never-show-this'}] * 30
    write_status(path, status='WARN', errors=errors)
    result = archive_check({}, tmp_path / 'state', now=NOW)
    shown = result['evidence']['maintenance']['errors']
    assert len(shown) == 20 and shown[0]['stage'] == 'migration_backup'
    assert shown[0]['error_code'] == 'ARCHIVE_MAINTENANCE_FAILED'
    assert 'never-show-this' not in json.dumps(result)


@pytest.mark.parametrize('field', ['schema_version', 'created', 'compressed', 'removed'])
@pytest.mark.parametrize('value', [True, 1.0])
def test_worker_status_integer_fields_reject_booleans_and_floats(tmp_path, field, value):
    path = make_archive(tmp_path / 'state')
    status = write_status(path)
    content = json.loads(status.read_text())
    content[field] = value
    status.write_text(json.dumps(content))
    result = archive_check({}, tmp_path / 'state', now=NOW)
    assert result['status'] == 'WARN'
    assert result['evidence']['error_code'] == 'ARCHIVE_STATUS_INVALID'


def test_status_symlink_is_not_followed(tmp_path):
    path = make_archive(tmp_path / 'state')
    secret = tmp_path / 'secret'
    secret.write_text('secret-diagnostic-text')
    (path.parent / 'maintenance.json').symlink_to(secret)
    result = archive_check({}, tmp_path / 'state', now=NOW)
    assert result['status'] == 'WARN'
    assert result['evidence']['error_code'] == 'ARCHIVE_STATUS_INVALID'
    assert 'secret-diagnostic-text' not in json.dumps(result)


def test_diagnose_contains_archive_check_without_writes(app):
    Application(app).initialize('2026-10-19T10:00:00+08:00')
    database = app.parent / 'state/state.sqlite3'
    before = database.read_bytes()
    result = diagnose(app)
    archive = next(item for item in result['checks'] if item['id'] == 'state.archives')
    assert archive['status'] == 'WARN'
    assert database.read_bytes() == before
    assert not (app.parent / 'state-archives').exists()


def set_migration_backup(app, instant):
    Application(app).initialize('2026-10-19T10:00:00+08:00')
    original = app.parent / 'configuration-backups' / ('b' * 16 + '-' + 'c' * 32 + '.sqlite3')
    original.parent.mkdir()
    database = app.parent / 'state/state.sqlite3'
    with closing(StateStore(database)) as store:
        store.acquire_lock()
        with store._connection(write=True) as connection:
            connection.execute('UPDATE configuration_history SET backup_path=?, applied_at=?',
                               (str(original), instant.isoformat()))
    return original, database


def test_compressed_migration_reference_is_reported_without_rewriting_audit(app):
    original, database = set_migration_backup(app, NOW - timedelta(days=2))
    compressed = original.with_suffix('.sqlite3.gz')
    compressed.write_bytes(b'compressed-presence-only')
    before = database.read_bytes()
    result = archive_check({}, database.parent, now=NOW)
    row = result['evidence']['migration_backups'][0]
    assert row['original_path'] == str(original)
    assert row['path'] == str(compressed) and row['status'] == 'compressed'
    assert database.read_bytes() == before


def test_expired_migration_reference_is_explained_without_claiming_deletion(app):
    original, database = set_migration_backup(app, NOW - timedelta(days=31))
    before = database.read_bytes()
    result = archive_check({}, database.parent, now=NOW)
    row = result['evidence']['migration_backups'][0]
    assert row['original_path'] == str(original) and row['status'] == 'expired_or_missing'
    assert database.read_bytes() == before


def add_backup_references(database, paths, instant):
    with closing(StateStore(database)) as store:
        store.acquire_lock()
        with store._connection(write=True) as connection:
            for index, path in enumerate(paths):
                connection.execute('INSERT INTO configuration_history SELECT ?, previous_revision, '
                    'revision, ?, ?, manifest, task_starts FROM configuration_history LIMIT 1',
                    (f'reference-{index}', instant.isoformat(), str(path)))


def test_migration_backup_diagnostics_does_not_rescan_audit_for_each_file(app, monkeypatch):
    original, database = set_migration_backup(app, NOW - timedelta(days=2))
    paths = [original.with_name('b' * 16 + f'-{index:032x}.sqlite3') for index in range(19)]
    add_backup_references(database, paths, NOW - timedelta(days=2))
    statements, connect = [], sqlite3.connect

    def traced_connect(*args, **kwargs):
        connection = connect(*args, **kwargs)
        connection.set_trace_callback(statements.append)
        return connection

    monkeypatch.setattr(sqlite3, 'connect', traced_connect)
    result = archive_check({}, database.parent, now=NOW)
    records = result['evidence']['migration_backups']
    assert len(records) == 20 and all(row['status'] == 'missing' and row['managed'] for row in records)
    reads = [sql for sql in statements if 'from configuration_history' in sql.lower()]
    assert len(reads) <= 2  # Recent records plus one full ownership lookup, independent of file count.


def test_backup_ownership_uses_audit_entries_outside_recent_page(app):
    original, database = set_migration_backup(app, NOW - timedelta(days=1))
    add_backup_references(database, [original] * 21, NOW - timedelta(days=2))
    result = archive_check({}, database.parent, now=NOW)
    records = result['evidence']['migration_backups']
    assert len(records) == 20 and result['evidence']['migration_backups_truncated'] is True
    assert all(row['managed'] is False and row['status'] == 'unmanaged' for row in records)


@pytest.mark.parametrize('code', ['ARCHIVE_SETTINGS_INVALID', 'ARCHIVE_FILE_INVALID',
    'ARCHIVE_CONTENT_INVALID', 'ARCHIVE_FILE_CHANGED', 'ARCHIVE_CANCELLED', 'ARCHIVE_TIMEOUT',
    'ARCHIVE_DIRECTORY_INVALID', 'ARCHIVE_BUSY', 'ARCHIVE_FAILED', 'ARCHIVE_STATUS_INVALID',
    'ARCHIVE_STATUS_WRITE_FAILED'])
def test_archive_errors_have_safe_public_codes(code):
    assert safe_error_details(ValueError(code))['error_code'] == code
