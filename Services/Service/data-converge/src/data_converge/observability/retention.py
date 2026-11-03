"""Decide when to rotate, how to compress, and which owned archives to retain."""

from datetime import datetime, timezone
import gzip
import os
from pathlib import Path
import re
import shutil
import stat
from uuid import uuid4

from data_converge.observability.settings import LoggingSettings

ROTATING_SUFFIX = '.rotating'


class RetentionPolicy:
    def __init__(self, filename: Path, settings: LoggingSettings):
        self.filename = filename
        self.settings = settings
        stem = re.escape(filename.name) + r'\.\d{8}T\d{12}Z\.[0-9a-f]{32}'
        # Legacy uncompressed archives are aged together with new gzip ones.
        self._archive_name = re.compile(stem + r'\.(?:gz|archive)')
        self._rotating_name = re.compile(stem + re.escape(ROTATING_SUFFIX))

    def should_rotate(self, size: int, modified: float, record_bytes: int, now: float) -> bool:
        return size > 0 and size + record_bytes > self.settings.max_bytes

    def archive_path(self, now: float) -> Path:
        stamp = datetime.fromtimestamp(now, timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
        return self.filename.with_name(f'{self.filename.name}.{stamp}.{uuid4().hex}.gz')

    def rotating_path(self, now: float) -> Path:
        archive = self.archive_path(now)
        return archive.with_name(archive.name[:-len('.gz')] + ROTATING_SUFFIX)

    def compress_pending(self) -> int:
        """Compress every renamed file, including leftovers from an interrupted rotation."""
        produced = 0
        for path in sorted(self.filename.parent.iterdir()):
            if self._rotating_name.fullmatch(path.name) and _regular(path):
                compress_file(path)
                produced += 1
        return produced

    def cleanup(self, now: float) -> None:
        """Keep the newest archives within both the count and total size limits."""
        archives = sorted(self._archives(), key=lambda item: (item[0], item[2].name), reverse=True)
        total = 0
        for index, (_modified, size, path) in enumerate(archives):
            total += size
            if index >= self.settings.backup_count or total > self.settings.max_total_bytes:
                self._remove_archive(path)

    def _archives(self) -> list[tuple[float, int, Path]]:
        archives = []
        for path in self.filename.parent.iterdir():
            if self._archive_name.fullmatch(path.name) is None:
                continue
            try:
                metadata = path.stat(follow_symlinks=False)
            except FileNotFoundError:
                continue
            if stat.S_ISREG(metadata.st_mode):
                archives.append((metadata.st_mtime, metadata.st_size, path))
        return archives

    @staticmethod
    def _remove_archive(path: Path) -> None:
        try:
            if stat.S_ISREG(path.stat(follow_symlinks=False).st_mode):
                path.unlink()
        except FileNotFoundError:
            pass


def _regular(path: Path) -> bool:
    try:
        return stat.S_ISREG(path.stat(follow_symlinks=False).st_mode)
    except FileNotFoundError:
        return False


def compress_file(source: Path) -> Path:
    """Gzip through a temporary name so a crash never leaves a truncated archive."""
    target = source.with_name(source.name[:-len(ROTATING_SUFFIX)] + '.gz')
    partial = target.with_name(target.name + '.partial')
    metadata = source.stat(follow_symlinks=False)
    with open(source, 'rb') as reader, open(partial, 'wb') as raw:
        with gzip.GzipFile(filename='', mode='wb', fileobj=raw, mtime=0) as writer:
            shutil.copyfileobj(reader, writer, 1024 * 1024)
    os.chmod(partial, 0o600)
    # Keep the original write time so retention orders archives by content age.
    os.utime(partial, (metadata.st_atime, metadata.st_mtime))
    os.replace(partial, target)
    source.unlink()
    return target
