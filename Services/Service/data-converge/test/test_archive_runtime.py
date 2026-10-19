"""Daily archives are optional background maintenance, not task identity or execution."""

from contextlib import closing
from datetime import datetime, timezone
import json
import threading
import time

import pytest
import yaml

from data_converge.application import Application
from data_converge.runtime import TaskScheduler
from data_converge.state import StateStore

from .test_main import app, local_http_app
from .test_configuration_migration import START, dump_database


def archive_config(path, **settings):
    config = yaml.safe_load(path.read_text())
    config['state']['archives'] = settings
    path.write_text(yaml.safe_dump(config))


@pytest.mark.parametrize('settings', [dict(enabled='false'), dict(enabled=0), dict(enabled=1), dict(retention_days=0),
    dict(retention_days=-1), dict(retention_days=True), dict(retention_days=1.5), dict(unknown=30)])
def test_bad_archive_policy_rejected_before_creating_state(app, settings):
    archive_config(app, **settings)
    with pytest.raises(ValueError, match='ARCHIVE_SETTINGS_INVALID'):
        Application(app).validate()
    assert not (app.parent / 'state').exists()


def test_archive_settings_do_not_change_binding_or_approved_task_revision(app):
    original = Application(app)
    original.initialize(START)
    old_fingerprint, old_manifest = original.fingerprint, original.manifest
    before = dump_database(app)
    archive_config(app, enabled=False, retention_days=45)
    current = Application(app)
    assert current.validate().exit_code == 0
    assert current.fingerprint == old_fingerprint and current.manifest == old_manifest
    assert dump_database(app) == before


def test_normal_serve_creates_one_verified_archive_without_changing_live_facts(local_http_app, monkeypatch):
    from data_converge.archives.maintenance import archive_directory, daily_archives
    from data_converge.archives.files import verify_compressed
    path, requests = local_http_app
    application = Application(path)
    application.initialize(START)
    before = dump_database(path)
    directory = archive_directory(application.directory / 'state.sqlite3')
    def wait_for_archive(self):
        deadline = time.monotonic() + 5
        while not (directory / 'maintenance.json').exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert (directory / 'maintenance.json').is_file()
    monkeypatch.setattr(TaskScheduler, 'start', wait_for_archive)
    application.serve()
    archives = daily_archives(application.directory / 'state.sqlite3')
    assert len(archives) == 1
    assert verify_compressed(archives[0]['path'], lambda: None)['original_bytes'] > 0
    application.serve()
    assert len(daily_archives(application.directory / 'state.sqlite3')) == 1
    assert dump_database(path) == before
    assert all(body == 'SELECT 1' for _, body in requests)
    assert json.loads((directory / 'maintenance.json').read_text())['status'] == 'PASS'


def test_run_once_and_disabled_serve_do_not_start_archiving(local_http_app, monkeypatch):
    from data_converge.archives.maintenance import archive_directory
    path, _ = local_http_app
    application = Application(path)
    application.initialize(START)
    assert application.run_once('2026-10-12T10:06:00+08:00').exit_code == 0
    assert not archive_directory(application.directory / 'state.sqlite3').exists()
    archive_config(path, enabled=False)
    monkeypatch.setattr(TaskScheduler, 'start', lambda self: None)
    assert Application(path).serve().exit_code == 0
    assert not archive_directory(application.directory / 'state.sqlite3').exists()


def test_archive_failure_is_reported_without_stopping_service(local_http_app, monkeypatch, caplog):
    from data_converge.archives import worker
    path, requests = local_http_app
    failure_seen = threading.Event()
    def unavailable(*args, **kwargs):
        failure_seen.set()
        raise OSError(28, 'private-secret-path')
    monkeypatch.setattr(worker, 'run_maintenance', unavailable)
    monkeypatch.setattr(TaskScheduler, 'start', lambda self: failure_seen.wait(3))
    assert Application(path).serve().exit_code == 0
    assert failure_seen.is_set() and 'STATE_ARCHIVE_FAILED' in caplog.text
    assert 'private-secret-path' not in caplog.text
    assert [body for _, body in requests] == ['SELECT 1']


def test_background_archive_does_not_block_task_transactions_and_stops_before_exit(app, monkeypatch):
    from data_converge.archives import worker
    from data_converge.archives.settings import ArchiveSettings
    application = Application(app)
    application.initialize(START)
    started, stopped = threading.Event(), threading.Event()
    def cancellable(database, settings, *, now, check):
        started.set()
        try:
            while True:
                check()
                time.sleep(0.005)
        finally:
            stopped.set()
    monkeypatch.setattr(worker, 'run_maintenance', cancellable)
    with closing(StateStore(application.directory / 'state.sqlite3')) as store:
        store.acquire_lock()
        with worker.ArchiveWorker(store.path, ArchiveSettings()):
            assert started.wait(2)
            store.set_meta('background-write-test', 'committed')
            assert store.get_meta('background-write-test') == 'committed'
        assert stopped.is_set()


def test_worker_schedules_next_beijing_day_and_retries_failures(app, monkeypatch):
    from data_converge.archives import worker
    from data_converge.archives.settings import ArchiveSettings
    calls = []
    def successful(*args, **kwargs):
        calls.append(kwargs['now'])
        return dict(status='PASS', created=0, compressed=0, removed=0, errors=[],
                    latest_archive=None, latest_snapshot_at=None)
    monkeypatch.setattr(worker, 'run_maintenance', successful)
    service = worker.ArchiveWorker(app.parent / 'state/state.sqlite3', ArchiveSettings())
    monkeypatch.setattr(service, '_write_status', lambda report: True)
    now = datetime(2026, 10, 19, 15, 59, tzinfo=timezone.utc)
    assert service._cycle(now) == datetime(2026, 10, 19, 16, tzinfo=timezone.utc)
    def unavailable(*args, **kwargs):
        raise OSError('private-error')
    monkeypatch.setattr(worker, 'run_maintenance', unavailable)
    assert service._cycle(now) == datetime(2026, 10, 19, 16, 4, tzinfo=timezone.utc)
    assert len(calls) == 1


def test_status_write_failure_is_warning_and_retried_before_tomorrow(app, monkeypatch, caplog):
    from data_converge.archives import worker
    from data_converge.archives.settings import ArchiveSettings
    def completed(*args, **kwargs):
        return dict(status='PASS', created=0, compressed=0, removed=0, errors=[],
                    latest_archive=None, latest_snapshot_at=None)
    def no_permission(*args):
        raise PermissionError('private-status-path')
    monkeypatch.setattr(worker, 'run_maintenance', completed)
    monkeypatch.setattr(worker, '_persist_status', no_permission)
    service = worker.ArchiveWorker(app.parent / 'state/state.sqlite3', ArchiveSettings())
    now = datetime(2026, 10, 19, 4, tzinfo=timezone.utc)
    assert service._cycle(now) == datetime(2026, 10, 19, 4, 5, tzinfo=timezone.utc)
    assert 'STATE_ARCHIVE_FAILED' in caplog.text and 'ARCHIVE_STATUS_WRITE_FAILED' in caplog.text
    assert 'private-status-path' not in caplog.text


def test_archive_worker_cancels_immediately_after_lease_loss(app, monkeypatch):
    from data_converge.archives import worker
    from data_converge.archives.settings import ArchiveSettings
    from .test_writer_coordination import Lease
    started, stopped = threading.Event(), threading.Event()
    def cancellable(database, settings, *, now, check):
        started.set()
        try:
            while True:
                check()
                time.sleep(0.005)
        finally:
            stopped.set()
    monkeypatch.setattr(worker, 'run_maintenance', cancellable)
    lease = Lease()
    with worker.ArchiveWorker(app.parent / 'state/state.sqlite3', ArchiveSettings(), lease=lease):
        assert started.wait(2)
        lease.lost = True
        assert stopped.wait(2)
