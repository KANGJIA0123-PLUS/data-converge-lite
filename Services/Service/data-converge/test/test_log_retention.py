"""Exercise retention with actual files and POSIX process locks."""

import logging
import multiprocessing
import os
from pathlib import Path
import time

import pytest

from data_converge.observability.handlers import RetentionFileHandler
from data_converge.observability.retention import RetentionPolicy
from data_converge.observability.settings import LoggingSettings


def emit(handler, message):
    handler.handle(logging.LogRecord('test', logging.INFO, __file__, 0, message, (), None))


def archive(path, text, age_days):
    target = RetentionPolicy(path, LoggingSettings()).archive_path(time.time())
    target.write_text(text, encoding='utf-8')
    timestamp = time.time() - age_days * 86400
    os.utime(target, (timestamp, timestamp))
    return target


@pytest.mark.parametrize('value', [True, 0, -1, 1.5, '7'])
def test_settings_reject_non_positive_integers(value):
    for name in ('max_bytes', 'backup_count', 'retention_days'):
        with pytest.raises(ValueError, match='^LOGGING_SETTING_INVALID$'):
            LoggingSettings.from_mapping({name: value})


def test_settings_defaults_and_mapping():
    assert LoggingSettings.from_mapping(None) == LoggingSettings(20 * 1024 * 1024, 10, 7)
    assert LoggingSettings.from_mapping({'backup_count': 3}).backup_count == 3


def test_bootstrap_appends_without_rotation_or_retention(tmp_path):
    path = tmp_path / 'service.log'
    path.write_text('existing\n')
    os.utime(path, (0, 0))
    old = archive(path, 'past default retention', 8)
    handler = RetentionFileHandler(path, None)
    try:
        emit(handler, 'configuration error')
    finally:
        handler.close()
    assert path.read_text() == 'existing\nconfiguration error\n'
    assert old.exists()
    assert list(tmp_path.glob('*.archive')) == [old]


def test_rotation_without_policy_preserves_the_open_log(tmp_path):
    path = tmp_path / 'service.log'
    handler = RetentionFileHandler(path, None)
    try:
        emit(handler, 'before')
        stream = handler.stream
        with pytest.raises(RuntimeError, match='^LOGGING_POLICY_REQUIRED$'):
            handler._rotate(time.time())
        assert handler.stream is stream and not stream.closed
        emit(handler, 'after')
    finally:
        handler.close()
    assert path.read_text() == 'before\nafter\n'
    assert not list(tmp_path.glob('*.archive'))


def test_size_rotation_counts_utf8_bytes_and_keeps_oversized_record(tmp_path):
    path = tmp_path / 'service.log'
    handler = RetentionFileHandler(path, LoggingSettings(max_bytes=8))
    try:
        emit(handler, '中文')
        emit(handler, '第二条超过限制')
        emit(handler, 'end')
    finally:
        handler.close()
    saved = sorted(p.read_text() for p in tmp_path.glob('*.archive'))
    assert saved == sorted(['中文\n', '第二条超过限制\n'])
    assert path.read_text() == 'end\n'


def test_startup_removes_only_owned_expired_and_excess_archives(tmp_path):
    path = tmp_path / 'service.log'
    old = archive(path, 'expired', 8)
    oldest = archive(path, 'excess', 3)
    kept = [archive(path, 'recent', age) for age in (2, 1)]
    unrelated = tmp_path / 'start.log'
    unrelated.write_text('platform log')
    malformed = tmp_path / 'service.log.external.archive'
    malformed.write_text('external log')
    linked = RetentionPolicy(path, LoggingSettings()).archive_path(time.time())
    linked.symlink_to(unrelated)
    handler = RetentionFileHandler(path, LoggingSettings(backup_count=2))
    handler.close()
    assert not old.exists() and not oldest.exists()
    assert all(p.exists() for p in kept)
    assert unrelated.read_text() == 'platform log'
    assert malformed.exists() and linked.is_symlink()


def test_daily_rotation_preserves_old_mtime_for_expiry(tmp_path):
    path = tmp_path / 'service.log'
    path.write_text('old day\n')
    old_time = time.time() - 2 * 86400
    os.utime(path, (old_time, old_time))
    handler = RetentionFileHandler(path, LoggingSettings())
    try:
        emit(handler, 'today')
    finally:
        handler.close()
    saved, = tmp_path.glob('*.archive')
    assert saved.read_text() == 'old day\n'
    assert saved.stat().st_mtime == pytest.approx(old_time)
    assert path.read_text() == 'today\n'


def test_external_rotation_and_repeated_close(tmp_path):
    path = tmp_path / 'service.log'
    handler = RetentionFileHandler(path, LoggingSettings())
    emit(handler, 'before')
    path.rename(tmp_path / 'service.log.1')
    emit(handler, 'after')
    lock_fd = handler._lock_fd
    handler.close()
    handler.close()
    with pytest.raises(OSError):
        os.fstat(lock_fd)
    assert path.read_text() == 'after\n'
    assert (tmp_path / 'service.log.1').read_text() == 'before\n'


def test_failed_initialization_closes_open_descriptors(tmp_path, monkeypatch):
    real_open = os.open
    descriptors = []

    def track_open(*args, **kwargs):
        descriptor = real_open(*args, **kwargs)
        descriptors.append(descriptor)
        return descriptor

    def fail_cleanup(self, now):
        raise PermissionError('cannot remove archive')

    monkeypatch.setattr(os, 'open', track_open)
    monkeypatch.setattr(RetentionPolicy, 'cleanup', fail_cleanup)
    with pytest.raises(PermissionError):
        RetentionFileHandler(tmp_path / 'service.log', LoggingSettings())
    assert len(descriptors) == 2
    for descriptor in descriptors:
        with pytest.raises(OSError):
            os.fstat(descriptor)


def test_write_failure_reports_only_safe_error_code(tmp_path, monkeypatch, capsys):
    handler = RetentionFileHandler(tmp_path / 'service.log', LoggingSettings())

    def fail_reopen():
        raise OSError('secret from downstream')

    monkeypatch.setattr(handler, 'reopenIfNeeded', fail_reopen)
    try:
        emit(handler, 'record contents are not echoed on failure')
    finally:
        handler.close()
    assert capsys.readouterr().err == 'LOGGING_WRITE_FAILED\n'


def write_process_logs(filename, worker):
    handler = RetentionFileHandler(Path(filename), LoggingSettings(max_bytes=40, backup_count=100))
    try:
        for index in range(30):
            emit(handler, f'{worker}:{index}')
    finally:
        handler.close()


def test_processes_serialize_rotation_and_preserve_all_records(tmp_path):
    path = tmp_path / 'service.log'
    context = multiprocessing.get_context('spawn')
    processes = [context.Process(target=write_process_logs, args=(str(path), i)) for i in range(3)]
    try:
        for process in processes:
            process.start()
        for process in processes:
            process.join(10)
            assert process.exitcode == 0
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join(5)
    lines = [line for p in [path, *tmp_path.glob('*.archive')] for line in p.read_text().splitlines()]
    assert sorted(lines) == sorted(f'{worker}:{index}' for worker in range(3) for index in range(30))
