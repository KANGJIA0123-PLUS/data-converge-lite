"""Serialize append, rotation and retention across service and CLI processes."""

from contextlib import contextmanager, suppress
import fcntl
from logging.handlers import WatchedFileHandler
import os
from pathlib import Path
import stat
import sys
import time

from data_converge.observability.retention import RetentionPolicy
from data_converge.observability.settings import LoggingSettings


class RetentionFileHandler(WatchedFileHandler):
    """Keep whole records, including single records larger than the size limit."""

    def __init__(self, filename: Path, settings: LoggingSettings | None):
        self._lock_fd = None
        self.policy = RetentionPolicy(filename.absolute(), settings) if settings else None
        self._last_cleanup = 0.0
        super().__init__(filename, mode='a', encoding='utf-8', delay=True)
        try:
            flags = os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC
            self._lock_fd = os.open(f'{self.baseFilename}.lock', flags, 0o600)
            with self._file_lock():
                self._open_stream()
                self._cleanup(time.time(), rotated=True)
        except Exception:
            self.close()
            raise

    def emit(self, record):
        if self._closed:
            return
        try:
            message = self.format(record) + self.terminator
            record_bytes = len(message.encode('utf-8'))
            with self._file_lock():
                if self.stream is None:
                    self._open_stream()
                self.reopenIfNeeded()
                metadata = os.fstat(self.stream.fileno())
                now = time.time()
                rotated = self.policy is not None and self.policy.should_rotate(
                    metadata.st_size, metadata.st_mtime, record_bytes, now
                )
                if rotated:
                    self._rotate(now)
                self.stream.write(message)
                self.flush()
                self._cleanup(now, rotated)
        except Exception:
            self.handleError(record)

    def handleError(self, record):
        # Never dump the record, SQL, SDK response, or credentials on I/O failure.
        with suppress(OSError):
            sys.stderr.write('LOGGING_WRITE_FAILED\n')

    def close(self):
        self.acquire()
        try:
            try:
                super().close()
            finally:
                if self._lock_fd is not None:
                    os.close(self._lock_fd)
                    self._lock_fd = None
        finally:
            self.release()

    @contextmanager
    def _file_lock(self):
        fcntl.flock(self._lock_fd, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(self._lock_fd, fcntl.LOCK_UN)

    def _open_stream(self):
        self.stream = self._open()
        self._statstream()

    def _open(self):
        return open(
            self.baseFilename, self.mode, encoding=self.encoding, errors=self.errors,
            opener=lambda path, flags: os.open(path, flags | os.O_NOFOLLOW, 0o600),
        )

    def _rotate(self, now: float):
        assert self.policy is not None
        self.stream.close()
        self.stream = None
        try:
            metadata = self.policy.filename.stat(follow_symlinks=False)
            if not stat.S_ISREG(metadata.st_mode):
                raise ValueError('LOGGING_FILE_INVALID')
            self.policy.filename.rename(self.policy.archive_path(now))
        except FileNotFoundError:
            # Platform log rotation does not participate in our process lock.
            pass
        finally:
            self._open_stream()

    def _cleanup(self, now: float, rotated: bool):
        if self.policy is None:
            return
        if rotated or now - self._last_cleanup >= 60 or now < self._last_cleanup:
            self.policy.cleanup(now)
            self._last_cleanup = now
