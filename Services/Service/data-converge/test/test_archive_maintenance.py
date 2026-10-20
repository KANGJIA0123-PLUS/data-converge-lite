"""Retention acts only on verified snapshots and this ledger's audited backups."""

from contextlib import closing
from datetime import datetime, timedelta, timezone
import gzip
import os
from pathlib import Path
import shutil
import sqlite3

import pytest

from data_converge.archives import maintenance
from data_converge.archives.settings import ArchiveSettings
from data_converge.state import StateStore

from .timezone_fixtures import deployment_zone

NOW = datetime(2026, 10, 19, 16, 30, tzinfo=timezone.utc)


@pytest.fixture
def database(tmp_path):
    path = tmp_path / 'state/state.sqlite3'
    with closing(StateStore(path, create=True)) as store:
        store.acquire_lock()
        store.set_meta('binding', 'a' * 64)
        store.set_meta('coverage_start', '2026-10-19T00:00:00+08:00')
    return path


def run(database, now=NOW, check=lambda: None):
    return maintenance.run_maintenance(database, ArchiveSettings(), now=now, check=check)


def add_migration(database, when, *, parent=None, name=None):
    directory = parent or database.parent.parent / 'configuration-backups'
    directory.mkdir(exist_ok=True)
    path = directory / (name or ('a' * 16 + '-' + 'b' * 32 + '.sqlite3'))
    shutil.copyfile(database, path)
    with sqlite3.connect(database) as connection:
        connection.execute('CREATE TABLE IF NOT EXISTS configuration_history '
                           '(backup_path TEXT, applied_at TEXT)')
        connection.execute('INSERT INTO configuration_history VALUES (?, ?)',
                           (str(path), when.isoformat()))
    return path


def test_daily_snapshot_is_idempotent_by_beijing_date_and_preserves_database(database, deployment_zone):
    deployment_zone('Asia/Shanghai')
    before = database.read_bytes()
    first = run(database)
    second = run(database, NOW + timedelta(hours=3))
    third = run(database, NOW + timedelta(days=1))
    assert first['status'] == second['status'] == third['status'] == 'PASS'
    assert (first['created'], second['created'], third['created']) == (1, 0, 1)
    archives = maintenance.daily_archives(database)
    assert len(archives) == 2 and database.read_bytes() == before
    assert all(item['path'].stat().st_mode & 0o777 == 0o600 for item in archives)
    assert maintenance.archive_directory(database).stat().st_mode & 0o777 == 0o700


def test_beijing_midnight_creates_new_snapshot(database, deployment_zone):
    deployment_zone('Asia/Shanghai')
    before_midnight = NOW.replace(hour=15, minute=59)
    assert run(database, before_midnight)['created'] == 1
    assert run(database, before_midnight + timedelta(minutes=1))['created'] == 1


def test_retention_uses_filename_time_and_keeps_unknown_files(database):
    old = run(database, NOW - timedelta(days=30))['latest_archive']
    recent = run(database, NOW - timedelta(days=29))['latest_archive']
    directory = maintenance.archive_directory(database)
    unknown = directory / 'something.sqlite3.gz'
    unknown.write_bytes(b'keep')
    os.utime(old, None)
    report = run(database)
    assert report['removed'] == 1 and not Path(old).exists()
    assert Path(recent).exists() and unknown.read_bytes() == b'keep'


def test_failed_fresh_snapshot_never_prunes_existing_archives(database, monkeypatch):
    old = Path(run(database, NOW - timedelta(days=40))['latest_archive'])
    monkeypatch.setattr(maintenance.files, 'create_snapshot',
                        lambda *args: (_ for _ in ()).throw(OSError('private path')))
    report = run(database)
    assert report['status'] == 'WARN' and report['removed'] == 0 and old.exists()
    assert 'private path' not in str(report)


def test_invalid_today_snapshot_does_not_suppress_new_backup(database):
    first = Path(run(database)['latest_archive'])
    first.write_bytes(b'not gzip')
    report = run(database)
    assert report['created'] == 1 and Path(report['latest_archive']) != first
    assert first.exists()  # Corruption is visible until its own retention expires.


def test_migration_snapshot_compresses_after_24_hours_then_expires(database):
    path = add_migration(database, NOW - timedelta(days=2))
    report = run(database)
    compressed = Path(str(path) + '.gz')
    assert report['compressed'] == 1 and compressed.exists() and not path.exists()
    with gzip.open(compressed, 'rb') as stream:
        assert stream.read(16) == b'SQLite format 3\x00'
    availability = maintenance.backup_availability(
        database, str(path), NOW - timedelta(days=2), ArchiveSettings(), NOW)
    assert availability['status'] == 'compressed' and availability['path'] == str(compressed)
    run(database, NOW + timedelta(days=28))
    assert not compressed.exists()
    availability = maintenance.backup_availability(
        database, str(path), NOW - timedelta(days=2), ArchiveSettings(), NOW + timedelta(days=28))
    assert availability['status'] == 'expired_or_missing'


def test_migration_compression_failure_preserves_raw_source(database, monkeypatch):
    path = add_migration(database, NOW - timedelta(days=2))
    run(database)
    path.write_bytes(gzip.decompress(Path(str(path) + '.gz').read_bytes()))
    Path(str(path) + '.gz').unlink()
    monkeypatch.setattr(maintenance.files, 'compress_snapshot',
                        lambda *args: (_ for _ in ()).throw(OSError('failure')))
    report = run(database)
    assert report['status'] == 'WARN' and path.exists()


def test_unreferenced_external_malformed_and_link_files_remain(database, tmp_path):
    directory = database.parent.parent / 'configuration-backups'
    directory.mkdir()
    unknown = directory / ('c' * 16 + '-' + 'd' * 32 + '.sqlite3')
    shutil.copyfile(database, unknown)
    external = add_migration(database, NOW - timedelta(days=40), parent=tmp_path / 'custom')
    malformed = add_migration(database, NOW - timedelta(days=40), name='foreign.sqlite3')
    linked = add_migration(database, NOW - timedelta(days=40))
    linked.unlink()
    linked.symlink_to(database)
    report = run(database)
    assert report['removed'] == 0
    assert all(path.exists() for path in (unknown, external, malformed, linked, database))
    assert maintenance.backup_availability(
        database, str(external), NOW, ArchiveSettings(), NOW)['managed'] is False


def test_hardlinks_and_archive_directory_symlinks_are_rejected(database, tmp_path):
    directory = maintenance.archive_directory(database)
    target = tmp_path / 'foreign'
    target.mkdir()
    directory.symlink_to(target, target_is_directory=True)
    with pytest.raises(ValueError, match='ARCHIVE_DIRECTORY_INVALID'):
        run(database)
    assert list(target.iterdir()) == []


def test_cancellation_propagates_and_leaves_old_snapshot(database):
    old = Path(run(database, NOW - timedelta(days=40))['latest_archive'])
    def cancelled():
        raise RuntimeError('cancelled')
    with pytest.raises(RuntimeError, match='cancelled'):
        run(database, check=cancelled)
    assert old.exists()


def test_disabled_policy_and_readonly_listing_create_no_files(database):
    directory = maintenance.archive_directory(database)
    assert maintenance.daily_archives(database) == []
    result = maintenance.run_maintenance(
        database, ArchiveSettings(enabled=False), now=NOW, check=lambda: None)
    assert result['status'] == 'SKIP' and not directory.exists()


def test_migration_24_hour_boundary_and_audit_are_preserved(database):
    created = NOW - timedelta(days=1) + timedelta(microseconds=1)
    path = add_migration(database, created)
    with sqlite3.connect(database) as connection:
        audit = connection.execute('SELECT * FROM configuration_history').fetchall()
    assert run(database)['compressed'] == 0 and path.exists()
    report = run(database, NOW + timedelta(microseconds=1))
    assert report['compressed'] == 1 and not path.exists()
    with sqlite3.connect(database) as connection:
        assert connection.execute('SELECT * FROM configuration_history').fetchall() == audit


def test_expired_raw_migration_backup_is_removed_only_after_fresh_daily(database):
    path = add_migration(database, NOW - timedelta(days=30))
    report = run(database)
    assert report['created'] == 1 and report['removed'] == 1
    assert report['compressed'] == 0 and not path.exists()


def test_existing_matching_compressed_migration_finishes_interrupted_cleanup(database):
    path = add_migration(database, NOW - timedelta(days=2))
    maintenance.files.compress_snapshot(path, Path(str(path) + '.gz'), lambda: None)
    assert run(database)['compressed'] == 1 and not path.exists()


def test_existing_different_compressed_migration_does_not_delete_raw(database):
    path = add_migration(database, NOW - timedelta(days=2))
    maintenance.files.compress_snapshot(path, Path(str(path) + '.gz'), lambda: None)
    with sqlite3.connect(path) as connection:
        connection.execute("INSERT INTO metadata VALUES ('changed', 'yes')")
    report = run(database)
    assert report['status'] == 'WARN' and report['compressed'] == 0 and path.exists()


def test_hardlinked_daily_and_migration_files_are_untouched(database, tmp_path):
    old = Path(run(database, NOW - timedelta(days=40))['latest_archive'])
    linked_daily = tmp_path / 'linked-daily'
    os.link(old, linked_daily)
    migration = add_migration(database, NOW - timedelta(days=40))
    linked_migration = tmp_path / 'linked-migration'
    os.link(migration, linked_migration)
    report = run(database)
    assert report['created'] == 1 and report['removed'] == 0
    assert all(path.exists() for path in (old, linked_daily, migration, linked_migration))


def test_migration_directory_symlink_cannot_expose_external_backups(database, tmp_path):
    original = add_migration(database, NOW - timedelta(days=40))
    directory = original.parent
    renamed = tmp_path / 'external-backups'
    directory.rename(renamed)
    directory.symlink_to(renamed, target_is_directory=True)
    report = run(database)
    assert report['created'] == 1 and report['removed'] == 0
    assert original.exists() and (renamed / original.name).exists()


def test_availability_remains_read_only_when_entire_backup_directory_missing(database):
    path = add_migration(database, NOW - timedelta(days=40))
    path.unlink()
    path.parent.rmdir()
    result = maintenance.backup_availability(
        database, str(path), NOW - timedelta(days=40), ArchiveSettings(), NOW)
    assert result['status'] == 'expired_or_missing' and result['managed'] is True
    assert not path.parent.exists() and not maintenance.archive_directory(database).exists()


def test_flock_conflict_does_not_start_second_archive_round(database):
    directory = maintenance.archive_directory(database)
    with maintenance._archive_lock(directory):
        with pytest.raises(BlockingIOError):
            run(database)
    assert maintenance.daily_archives(database) == []


def test_readonly_archives_remain_visible_when_live_database_is_missing(database):
    created = run(database)['latest_archive']
    migration = add_migration(database, NOW - timedelta(days=2))
    run(database)
    database.unlink()
    assert maintenance.daily_archives(database)[0]['path'] == Path(created)
    available = maintenance.backup_availability(
        database, str(migration), NOW - timedelta(days=2), ArchiveSettings(), NOW)
    assert available['status'] == 'compressed'
    assert available['managed'] is False  # No live audit can establish managed ownership.
    assert not database.exists()


def age_file(path, age):
    timestamp = (NOW - age).timestamp()
    os.utime(path, (timestamp, timestamp))


def test_old_owned_temporary_files_are_reaped_without_touching_unknown_files(database):
    directory = maintenance.archive_directory(database)
    directory.mkdir()
    abandoned = directory / ('.snapshot-' + 'f' * 32 + '.sqlite3')
    abandoned.write_bytes(b'abandoned')
    stale_compressed = directory / '.archive-ab12_cd3'
    stale_compressed.write_bytes(b'abandoned')
    recent = directory / '.archive-recent12'
    recent.write_bytes(b'keep')
    unknown = directory / '.archive-not-a-temp-name'
    unknown.write_bytes(b'keep')
    for path in (abandoned, stale_compressed, unknown):
        age_file(path, timedelta(days=2))
    report = run(database)
    assert report['created'] == 1 and not abandoned.exists() and not stale_compressed.exists()
    assert recent.exists() and unknown.exists()


def test_old_publish_link_pair_recovers_daily_without_external_hardlink_cleanup(database):
    final = Path(run(database, NOW - timedelta(hours=25))['latest_archive'])
    temporary = final.parent / '.archive-ab12_cd3'
    os.link(final, temporary)
    age_file(temporary, timedelta(hours=25))
    external = final.parent / '.archive-external'
    external.write_bytes(b'keep')
    other_directory = database.parent / 'external-link'
    os.link(external, other_directory)
    age_file(external, timedelta(days=2))
    report = run(database)
    assert report['created'] == 1 and final.exists() and final.stat().st_nlink == 1
    assert not temporary.exists() and external.exists() and other_directory.exists()
    assert len(maintenance.daily_archives(database)) == 2


def test_hidden_temporary_hardlink_to_live_database_is_preserved(database):
    directory = maintenance.archive_directory(database)
    directory.mkdir()
    temporary = directory / '.archive-ab12_cd3'
    os.link(database, temporary)
    age_file(temporary, timedelta(days=2))
    report = run(database)
    assert report['status'] == 'WARN'
    assert temporary.exists() and database.exists() and database.stat().st_nlink == 2


def test_old_snapshot_publish_pair_is_recovered_and_reaped(database):
    directory = maintenance.archive_directory(database)
    directory.mkdir()
    final = directory / ('.snapshot-' + 'e' * 32 + '.sqlite3')
    final.write_bytes(b'abandoned snapshot')
    temporary = directory / '.archive-ab12_cd3'
    os.link(final, temporary)
    age_file(temporary, timedelta(days=2))
    report = run(database)
    assert report['created'] == 1 and not final.exists() and not temporary.exists()


def test_missing_live_database_cannot_create_or_prune_snapshots(database):
    old = Path(run(database, NOW - timedelta(days=40))['latest_archive'])
    database.unlink()
    with pytest.raises(ValueError, match='ARCHIVE_FILE_INVALID'):
        run(database)
    assert old.exists() and len(maintenance.daily_archives(database)) == 1


def test_failed_today_backup_preserves_recoverable_interrupted_raw_snapshot(database, monkeypatch):
    directory = maintenance.archive_directory(database)
    directory.mkdir()
    pending = directory / ('.snapshot-' + 'f' * 32 + '.sqlite3')
    shutil.copyfile(database, pending)
    age_file(pending, timedelta(days=2))
    before = pending.read_bytes()
    def unavailable(*args):
        raise OSError('full archive disk')
    monkeypatch.setattr(maintenance.files, 'create_snapshot', unavailable)
    report = run(database)
    assert report['status'] == 'WARN' and report['removed'] == 0
    assert pending.read_bytes() == before


@pytest.mark.parametrize('change', ['remove', 'replace'])
def test_live_database_disappearing_during_validation_preserves_all_archives(database, monkeypatch, change):
    old = Path(run(database, NOW - timedelta(days=40))['latest_archive'])
    maintenance.run_maintenance(database, ArchiveSettings(retention_days=90), now=NOW, check=lambda: None)
    original_verify = maintenance.files.verify_compressed
    def lose_database(path, check):
        database.unlink()
        if change == 'replace':
            with sqlite3.connect(database) as connection:
                connection.execute('CREATE TABLE unrelated (id INTEGER)')
        return original_verify(path, check)
    monkeypatch.setattr(maintenance.files, 'verify_compressed', lose_database)
    with pytest.raises(ValueError, match='ARCHIVE_FILE_CHANGED'):
        run(database)
    assert old.is_file() and len(maintenance.daily_archives(database)) == 2
