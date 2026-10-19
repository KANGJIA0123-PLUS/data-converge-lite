"""Run cancellable archive maintenance outside the SQL scheduling thread."""

from datetime import datetime, timedelta, timezone
import json
import logging
import os
from pathlib import Path
import tempfile
import threading
import time
from zoneinfo import ZoneInfo

from data_converge.errors import safe_error_details
from data_converge.leader_election import LeadershipError

from .maintenance import archive_directory, run_maintenance

logger = logging.getLogger(__name__)
TZ = ZoneInfo('Asia/Shanghai')


def _next_day(now):
    local = now.astimezone(TZ)
    return (local.replace(hour=0, minute=0, second=0, microsecond=0)
            + timedelta(days=1)).astimezone(timezone.utc)


def _persist_status(directory, report):
    if directory.is_symlink() or not directory.is_dir():
        raise ValueError('ARCHIVE_DIRECTORY_INVALID')
    destination = directory / 'maintenance.json'
    if destination.is_symlink():
        raise ValueError('ARCHIVE_FILE_INVALID')
    descriptor, temporary = tempfile.mkstemp(prefix='.maintenance-', dir=directory)
    try:
        with os.fdopen(descriptor, 'w', encoding='utf-8') as stream:
            json.dump(report, stream, ensure_ascii=True, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, destination)
        descriptor = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        Path(temporary).unlink(missing_ok=True)


class ArchiveWorker:
    def __init__(self, database, settings, *, lease=None):
        self.database = Path(database)
        self.settings = settings
        self.lease = lease
        self._stop = threading.Event()
        self._thread = None
        self._deadline = 0.0

    def __enter__(self):
        if self.settings.enabled:
            self._thread = threading.Thread(target=self._run, name='state-archives', daemon=True)
            try:
                self._thread.start()
            except Exception as error:
                self._thread = None
                logger.warning('STATE_ARCHIVE_FAILED diagnostic=%s',
                               json.dumps(safe_error_details(error), ensure_ascii=True))
        return self

    def __exit__(self, *_):
        self._stop.set()
        if self._thread is not None:
            self._thread.join()

    def _check(self):
        if self._stop.is_set():
            raise RuntimeError('ARCHIVE_CANCELLED')
        if time.monotonic() >= self._deadline:
            raise RuntimeError('ARCHIVE_TIMEOUT')
        if self.lease is not None:
            self.lease.assert_owned()

    def _run(self):
        next_attempt = datetime.min.replace(tzinfo=timezone.utc)
        while not self._stop.is_set():
            now = datetime.now(timezone.utc)
            if now >= next_attempt:
                try:
                    next_attempt = self._cycle(now)
                except LeadershipError:
                    return
                except RuntimeError as error:
                    if str(error) == 'ARCHIVE_CANCELLED':
                        return
                    raise
            self._stop.wait(min(60, max(0.1, (next_attempt - now).total_seconds())))

    def _cycle(self, now):
        self._deadline = time.monotonic() + 300
        try:
            self._check()
            report = run_maintenance(self.database, self.settings, now=now, check=self._check)
        except LeadershipError:
            raise
        except Exception as error:
            if str(error) == 'ARCHIVE_CANCELLED':
                raise
            report = dict(status='WARN', created=0, compressed=0, removed=0, errors=[],
                          latest_archive=None, latest_snapshot_at=None, **safe_error_details(error))
        report = dict(report, schema_version=1, checked_at=now.isoformat())
        if not self._write_status(report):
            report.update(status='WARN', error_code='ARCHIVE_STATUS_WRITE_FAILED')
        failed = report['status'] != 'PASS'
        event = 'STATE_ARCHIVE_FAILED' if failed else 'STATE_ARCHIVE_COMPLETED'
        logger.log(logging.WARNING if failed else logging.INFO,
                   '%s diagnostic=%s', event, json.dumps(report, ensure_ascii=True))
        return now + timedelta(minutes=5) if failed else _next_day(now)

    def _write_status(self, report):
        try:
            _persist_status(archive_directory(self.database), report)
            return True
        except Exception as error:
            logger.warning('STATE_ARCHIVE_STATUS_FAILED diagnostic=%s',
                           json.dumps(safe_error_details(error), ensure_ascii=True))
            return False
