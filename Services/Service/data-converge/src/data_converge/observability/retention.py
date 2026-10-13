"""Decide when to rotate and which application-owned archives to retain."""

from datetime import datetime, timezone
from pathlib import Path
import re
import stat
from uuid import uuid4

from data_converge.observability.settings import LoggingSettings


class RetentionPolicy:
    def __init__(self, filename: Path, settings: LoggingSettings):
        self.filename = filename
        self.settings = settings
        self._archive_name = re.compile(
            re.escape(filename.name) + r'\.\d{8}T\d{12}Z\.[0-9a-f]{32}\.archive'
        )

    def should_rotate(self, size: int, modified: float, record_bytes: int, now: float) -> bool:
        if size == 0:
            return False
        previous_day = datetime.fromtimestamp(modified, timezone.utc).date()
        current_day = datetime.fromtimestamp(now, timezone.utc).date()
        return size + record_bytes > self.settings.max_bytes or previous_day < current_day

    def archive_path(self, now: float) -> Path:
        stamp = datetime.fromtimestamp(now, timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
        return self.filename.with_name(f'{self.filename.name}.{stamp}.{uuid4().hex}.archive')

    def cleanup(self, now: float) -> None:
        cutoff = now - self.settings.retention_days * 86400
        archives = sorted(self._archives(), key=lambda item: (item[0], item[1].name), reverse=True)
        for index, (modified, path) in enumerate(archives):
            if modified < cutoff or index >= self.settings.backup_count:
                self._remove_archive(path)

    def _archives(self) -> list[tuple[float, Path]]:
        archives = []
        for path in self.filename.parent.iterdir():
            if self._archive_name.fullmatch(path.name) is None:
                continue
            try:
                metadata = path.stat(follow_symlinks=False)
            except FileNotFoundError:
                continue
            if stat.S_ISREG(metadata.st_mode):
                archives.append((metadata.st_mtime, path))
        return archives

    @staticmethod
    def _remove_archive(path: Path) -> None:
        try:
            if stat.S_ISREG(path.stat(follow_symlinks=False).st_mode):
                path.unlink()
        except FileNotFoundError:
            pass
