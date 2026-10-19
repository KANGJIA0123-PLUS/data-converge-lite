"""Read archive availability without running maintenance or changing ledger audit."""

from contextlib import closing
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import stat
from zoneinfo import ZoneInfo

from data_converge.archives.maintenance import archive_directory, backup_availability, daily_archives
from data_converge.archives.settings import ArchiveSettings
from data_converge.errors import safe_diagnostic_details, safe_error_details

from .ledger import DiagnosticLedger
from .report import check

TZ = ZoneInfo('Asia/Shanghai')
ACTION = ('正常服务启动后后台维护归档；检查目录权限、剩余空间和归档维护日志。'
          '本检查只确认文件可用性，不重新解压校验，不会创建备份或清理文件。')
STAGES = {'create_daily', 'verify_daily', 'prune_daily', 'read_migration_audit', 'migration_backup'}


def _instant(value):
    instant = datetime.fromisoformat(value)
    if instant.tzinfo is None or instant.utcoffset() is None:
        raise ValueError('ARCHIVE_STATUS_INVALID')
    return instant


def _read_status(path):
    try:
        if path.parent.is_symlink():
            raise ValueError('ARCHIVE_STATUS_INVALID')
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor, 'rb') as stream:
            if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                raise ValueError('ARCHIVE_STATUS_INVALID')
            content = stream.read(65537)
        if len(content) > 65536:
            raise ValueError('ARCHIVE_STATUS_INVALID')
        return _status_fields(json.loads(content))
    except FileNotFoundError:
        return None
    except (OSError, KeyError, TypeError, ValueError):
        raise ValueError('ARCHIVE_STATUS_INVALID') from None


def _status_fields(value):
    if (not isinstance(value, dict) or type(value.get('schema_version')) is not int or
            value['schema_version'] != 1 or value.get('status') not in ('PASS', 'WARN')):
        raise ValueError('ARCHIVE_STATUS_INVALID')
    result = {name: value[name] for name in ('schema_version', 'status', 'checked_at')}
    _instant(result['checked_at'])
    for name in ('created', 'compressed', 'removed'):
        count = value.get(name)
        if type(count) is not int or not 0 <= count < 2**63:
            raise ValueError('ARCHIVE_STATUS_INVALID')
        result[name] = count
    snapshot = value.get('latest_snapshot_at')
    result['latest_snapshot_at'] = _instant(snapshot).isoformat() if snapshot is not None else None
    result['errors'] = _stage_errors(value.get('errors', []))
    if value.get('error_code') is not None:
        code = safe_error_details(ValueError(value['error_code'])).get('error_code')
        result['error_code'] = code if code and code.startswith('ARCHIVE_') else 'ARCHIVE_FAILED'
    return result


def _stage_errors(errors):
    if not isinstance(errors, list):
        raise ValueError('ARCHIVE_STATUS_INVALID')
    result = []
    for error in errors[:20]:
        if not isinstance(error, dict):
            raise ValueError('ARCHIVE_STATUS_INVALID')
        code = safe_error_details(ValueError(error.get('error_code', error.get('code')))).get('error_code')
        row = safe_diagnostic_details({'error_type': error.get('error_type'), 'os_errno': error.get('os_errno')})
        row['error_code'] = code if code and code.startswith('ARCHIVE_') else 'ARCHIVE_FAILED'
        if error.get('stage') in STAGES:
            row['stage'] = error['stage']
        result.append(row)
    return result


def _daily_evidence(database, evidence, now):
    archives = daily_archives(database)
    latest = max(archives, key=lambda row: row['created_at']) if archives else None
    evidence.update(daily_archive_count=len(archives), latest_archive=None, latest_snapshot_at=None)
    if latest is not None:
        evidence.update(latest_archive=str(latest['path']), latest_snapshot_at=latest['created_at'].isoformat())
    return (latest is not None and latest['created_at'] <= now and
            latest['created_at'].astimezone(TZ).date() == now.astimezone(TZ).date())


def _maintenance_evidence(directory, evidence, now):
    status = _read_status(directory / 'maintenance.json')
    evidence['maintenance'] = status
    if status is None:
        evidence['maintenance_stale'] = True
        return False
    checked = _instant(status['checked_at'])
    evidence['maintenance_stale'] = checked > now or checked.astimezone(TZ).date() != now.astimezone(TZ).date()
    return status['status'] == 'PASS' and not evidence['maintenance_stale']


def _backup_references(database, settings, now):
    if not database.is_file():
        return [], False
    with closing(DiagnosticLedger(database)) as store:
        records = store.configuration_history(limit=21)
        truncated = len(records) > 20
        records = records[:20]
        recovery = store.legacy_recovery()
        if recovery and not any(row['migration_id'] == recovery['migration_id'] for row in records):
            records.append(dict(migration_id=recovery['migration_id'], backup_path=recovery['backup_path'],
                                applied_at=recovery['recovered_at']))
    result = []
    for row in records:
        if row['backup_path'] is not None:
            available = backup_availability(database, row['backup_path'], _instant(row['applied_at']), settings, now)
            result.append(dict(migration_id=row['migration_id'], original_path=row['backup_path'], **available))
    return result, truncated


def _archive_status(database, directory, settings, now, evidence):
    ready = True
    for callback, arguments in ((_daily_evidence, (database, evidence, now)),
                                (_maintenance_evidence, (directory, evidence, now))):
        try:
            ready = callback(*arguments) and ready
        except Exception as error:
            evidence.update(safe_error_details(error))
            ready = False
    try:
        references, truncated = _backup_references(database, settings, now)
        evidence.update(migration_backups=references, migration_backups_truncated=truncated)
        ready = ready and all(row['status'] != 'missing' for row in references)
    except Exception as error:
        evidence['migration_backup_error'] = safe_error_details(error)
        ready = False
    return 'PASS' if ready else 'WARN'


def archive_check(raw, directory, *, now=None):
    section = raw.get('state', {})
    if not isinstance(section, dict):
        raise ValueError('ARCHIVE_SETTINGS_INVALID')
    settings = ArchiveSettings.from_mapping(section.get('archives'))
    database = Path(directory) / 'state.sqlite3'
    archive_dir = archive_directory(database)
    evidence = dict(enabled=settings.enabled, retention_days=settings.retention_days,
                    directory=str(archive_dir), timezone='Asia/Shanghai',
                    availability_check='regular_file_only')
    if not settings.enabled:
        return check('state.archives', 'SKIP', '状态库归档维护已关闭', evidence)
    status = _archive_status(database, archive_dir, settings, now or datetime.now(timezone.utc), evidence)
    return check('state.archives', status, '每日压缩备份与保留期限', evidence, ACTION)
