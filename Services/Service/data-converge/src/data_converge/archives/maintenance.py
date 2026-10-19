"""Manage compressed snapshots without mutating active or historical ledger facts."""

from contextlib import closing, contextmanager
from datetime import datetime, timedelta, timezone
import fcntl
import os
from pathlib import Path
import re
import sqlite3
import stat
import uuid

from . import files

_DAILY = re.compile(r'daily-(\d{8}T\d{12}Z)-[0-9a-f]{32}\.sqlite3\.gz\Z')
_MIGRATION = re.compile(r'[0-9a-f]{16}-[0-9a-f]{32}\.sqlite3\Z')
_SNAPSHOT = re.compile(r'\.snapshot-[0-9a-f]{32}\.sqlite3\Z')
_TEMPORARY = re.compile(r'\.archive-[a-z0-9_]{8}\Z')
_BEIJING = timezone(timedelta(hours=8))


class _Guard:
    def __init__(self, callback, database, identity):
        self.callback = callback
        self.database = database
        self.identity = identity
        self.cancelled = False

    def __call__(self):
        try:
            self.callback()
            current = _live_identity(self.database)
            if current is None or not _same_file(current, self.identity):
                raise ValueError('ARCHIVE_FILE_CHANGED')
        except BaseException:
            self.cancelled = True
            raise


def _instant(value):
    result = datetime.fromisoformat(value) if isinstance(value, str) else value
    if not isinstance(result, datetime) or result.utcoffset() is None:
        raise ValueError('ARCHIVE_TIMESTAMP_INVALID')
    return result.astimezone(timezone.utc)


def archive_directory(database) -> Path:
    database = Path(database).resolve()
    return database.parent.parent / (database.parent.name + '-archives')


def _directory_identity(directory):
    try:
        identity = directory.lstat()
    except FileNotFoundError:
        return None
    if not stat.S_ISDIR(identity.st_mode):
        raise ValueError('ARCHIVE_DIRECTORY_INVALID')
    return identity


def _same_file(first, second):
    return (first.st_dev, first.st_ino) == (second.st_dev, second.st_ino)


def _live_identity(database):
    try:
        return Path(database).stat()
    except FileNotFoundError:
        return None


def _safe_file(path, database):
    identity = files.regular_file(path)
    active = _live_identity(database)
    if identity is not None and active is not None and _same_file(identity, active):
        raise ValueError('ARCHIVE_FILE_INVALID')
    return identity


def daily_archives(database) -> list[dict]:
    directory = archive_directory(database)
    if _directory_identity(directory) is None:
        return []
    archives = []
    for path in directory.iterdir():
        match = _DAILY.fullmatch(path.name)
        if match is None:
            continue
        try:
            created = datetime.strptime(match[1], '%Y%m%dT%H%M%S%fZ').replace(tzinfo=timezone.utc)
            if _safe_file(path, database) is not None:
                archives.append({'path': path, 'created_at': created})
        except (ValueError, FileNotFoundError):
            continue
    return sorted(archives, key=lambda item: (item['created_at'], item['path'].name))


@contextmanager
def _archive_lock(directory):
    directory.mkdir(mode=0o700, exist_ok=True)
    identity = _directory_identity(directory)
    directory_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, 0o700)
    descriptor = None
    try:
        if not _same_file(identity, os.fstat(directory_fd)):
            raise ValueError('ARCHIVE_DIRECTORY_INVALID')
        os.fchmod(directory_fd, 0o700)
        descriptor = os.open('.maintenance.lock', os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW,
                             0o600, dir_fd=directory_fd)
        locked = os.fstat(descriptor)
        if not stat.S_ISREG(locked.st_mode) or locked.st_nlink != 1:
            raise ValueError('ARCHIVE_LOCK_INVALID')
        os.fchmod(descriptor, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield
    finally:
        if descriptor is not None:
            os.close(descriptor)
        os.close(directory_fd)


def _error(report, stage, error, check):
    if check.cancelled:
        raise error
    report['status'] = 'WARN'
    report['errors'].append({'code': 'ARCHIVE_MAINTENANCE_FAILED', 'stage': stage,
                             'error_type': type(error).__name__})



def _file_version(identity):
    return (identity.st_dev, identity.st_ino, identity.st_size,
            identity.st_mtime_ns, identity.st_nlink)


def _paired_final(directory, temporary, expected):
    if not _TEMPORARY.fullmatch(temporary.name) or expected.st_nlink != 2:
        return None
    for candidate in directory.iterdir():
        if not (_DAILY.fullmatch(candidate.name) or _SNAPSHOT.fullmatch(candidate.name)):
            continue
        identity = candidate.lstat()
        if stat.S_ISREG(identity.st_mode) and _file_version(identity) == _file_version(expected):
            return candidate
    return None


def _recover_publish_pair(temporary, final, expected, check):
    descriptor = os.open(temporary.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, 0o700)
    try:
        check()
        first = os.stat(temporary.name, dir_fd=descriptor, follow_symlinks=False)
        second = os.stat(final.name, dir_fd=descriptor, follow_symlinks=False)
        if (_file_version(first) != _file_version(expected)
                or _file_version(second) != _file_version(expected)):
            return False
        os.unlink(temporary.name, dir_fd=descriptor)
        os.fsync(descriptor)
        return True
    finally:
        os.close(descriptor)


def _reap_temporary(database, path, cutoff, check, report, redundant_only):
    identity = path.lstat()
    if not stat.S_ISREG(identity.st_mode) or identity.st_mtime > cutoff.timestamp():
        return
    active = _live_identity(database)
    if active is not None and _same_file(identity, active):
        return
    check()
    if identity.st_nlink == 1:
        if not redundant_only:
            report['removed'] += int(files.remove_file(path, expected=identity))
    else:
        final = _paired_final(path.parent, path, identity)
        if final is not None:
            report['removed'] += int(_recover_publish_pair(path, final, identity, check))


def _reap_temporaries(database, now, check, report, *, redundant_only=False):
    directory = archive_directory(database)
    # Recover publication links first; then old standalone raw snapshots can expire.
    paths = sorted(directory.iterdir(), key=lambda path: not bool(_TEMPORARY.fullmatch(path.name)))
    for path in paths:
        if not (_TEMPORARY.fullmatch(path.name) or _SNAPSHOT.fullmatch(path.name)):
            continue
        try:
            _reap_temporary(database, path, now - timedelta(days=1), check, report, redundant_only)
        except FileNotFoundError:
            continue
        except Exception as error:
            _error(report, 'reap_temporary', error, check)


def _valid_today(database, now, check, report):
    today = now.astimezone(_BEIJING).date()
    for item in reversed(daily_archives(database)):
        if item['created_at'] > now or item['created_at'].astimezone(_BEIJING).date() != today:
            continue
        try:
            check()
            files.verify_compressed(item['path'], check)
            return item
        except Exception as error:
            _error(report, 'verify_daily', error, check)
    return None


def _create_daily(database, now, check):
    directory = archive_directory(database)
    suffix = uuid.uuid4().hex
    name = 'daily-' + now.strftime('%Y%m%dT%H%M%S%fZ') + '-' + suffix + '.sqlite3'
    temporary = directory / ('.snapshot-' + suffix + '.sqlite3')
    destination = directory / (name + '.gz')
    identity = None
    try:
        check()
        files.create_snapshot(database, temporary, check)
        identity = _safe_file(temporary, database)
        files.compress_snapshot(temporary, destination, check)
        check()
        return {'path': destination, 'created_at': now}
    finally:
        if identity is not None:
            files.remove_file(temporary, expected=identity)


def _remove(path, database, check, report):
    check()
    identity = _safe_file(path, database)
    if identity is not None:
        check()
        report['removed'] += int(files.remove_file(path, expected=identity))


def _prune_daily(database, cutoff, check, report):
    for item in daily_archives(database):
        if item['created_at'] > cutoff:
            continue
        try:
            _remove(item['path'], database, check, report)
        except Exception as error:
            _error(report, 'prune_daily', error, check)


def migration_references(database):
    """Read the latest audited timestamp for every backup path, without page limits."""
    uri = Path(database).resolve().as_uri() + '?mode=ro'
    references = {}
    with closing(sqlite3.connect(uri, uri=True)) as connection:
        exists = connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                                    "AND name='configuration_history'").fetchone()
        if exists is None:
            return references
        for path, applied in connection.execute('SELECT backup_path, applied_at FROM configuration_history'):
            if not isinstance(path, str):
                continue
            try:
                instant = _instant(applied)
            except (ValueError, TypeError):
                continue
            references[path] = max(instant, references.get(path, instant))
    return references


def _managed_migration(database, path):
    directory = Path(database).resolve().parent.parent / 'configuration-backups'
    path = Path(path)
    if path.parent != directory or not _MIGRATION.fullmatch(path.name):
        return False
    _directory_identity(directory)
    return True


def _maintain_migration(database, path, applied, now, retention, check, report):
    if not _managed_migration(database, path):
        return
    original = Path(path)
    compressed = Path(path + '.gz')
    if applied <= now - timedelta(days=retention):
        for candidate in (original, compressed):
            _remove(candidate, database, check, report)
    elif applied <= now - timedelta(days=1):
        identity = _safe_file(original, database)
        if identity is not None:
            check()
            files.compress_snapshot(original, compressed, check)
            check()
            if files.remove_file(original, expected=identity):
                report['compressed'] += 1


def _maintain_migrations(database, now, settings, check, report):
    try:
        references = migration_references(database)
    except Exception as error:
        _error(report, 'read_migration_audit', error, check)
        return
    for path, applied in references.items():
        try:
            check()
            _maintain_migration(database, path, applied, now, settings.retention_days, check, report)
        except Exception as error:
            _error(report, 'migration_backup', error, check)


def _report():
    return {'status': 'PASS', 'created': 0, 'compressed': 0, 'removed': 0,
            'latest_archive': None, 'latest_snapshot_at': None, 'errors': []}


def run_maintenance(database, settings, *, now, check) -> dict:
    report = _report()
    if not settings.enabled:
        report['status'] = 'SKIP'
        return report
    now = _instant(now)
    database = Path(database).resolve()
    identity = _live_identity(database)
    if identity is None:
        raise ValueError('ARCHIVE_FILE_INVALID')
    check = _Guard(check, database, identity)
    check()
    with _archive_lock(archive_directory(database)):
        _reap_temporaries(database, now, check, report, redundant_only=True)
        latest = _valid_today(database, now, check, report)
        if latest is None:
            try:
                latest = _create_daily(database, now, check)
                report['created'] = 1
            except Exception as error:
                _error(report, 'create_daily', error, check)
                return report
        report['latest_archive'] = str(latest['path'])
        report['latest_snapshot_at'] = latest['created_at'].isoformat()
        _reap_temporaries(database, now, check, report)
        _prune_daily(database, now - timedelta(days=settings.retention_days), check, report)
        _maintain_migrations(database, now, settings, check, report)
    return report


def _availability_paths(database, original_path):
    original = Path(original_path)
    for status, candidate in (('original', original), ('compressed', Path(str(original) + '.gz'))):
        try:
            if _safe_file(candidate, database) is not None:
                return {'status': status, 'path': str(candidate)}
        except (ValueError, FileNotFoundError):
            continue
    return None


def backup_availability(database, original_path, created_at, settings, now, *, references=None) -> dict:
    """Report immutable audit references without creating files or asserting deletion."""
    now, created = _instant(now), _instant(created_at)
    if references is None:
        references = migration_references(database) if _live_identity(database) else {}
    managed = (isinstance(original_path, str) and original_path in references
               and references[original_path] == created
               and _managed_migration(database, original_path))
    result = {'managed': managed, 'retention_days': settings.retention_days}
    if not managed:
        available = (_availability_paths(database, original_path)
                     if _managed_migration(database, original_path) else None)
        return dict(result, **(available or {'status': 'unmanaged'}))
    result['expires_at'] = (created + timedelta(days=settings.retention_days)).isoformat()
    available = _availability_paths(database, original_path)
    if available is not None:
        return dict(result, **available)
    expired = created <= now - timedelta(days=settings.retention_days)
    result['status'] = 'expired_or_missing' if settings.enabled and expired else 'missing'
    return result
