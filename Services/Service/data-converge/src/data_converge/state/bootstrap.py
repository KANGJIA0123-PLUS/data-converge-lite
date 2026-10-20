"""Admit startup while preserving evidence when the local ledger is missing."""

import os
import tempfile
from pathlib import Path


class StateBootstrapError(ValueError):
    """A fixed, public startup diagnostic without database or credential data."""


def _sync_directory(directory: Path) -> None:
    descriptor = os.open(directory, os.O_RDONLY, 0o700)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class StateBootstrap:
    """Use only while owning the database's existing single-writer file lock.

    The marker survives deletion of the state subdirectory, not loss of its
    parent volume. A legacy deployment without a marker cannot prove prior use.
    """

    def __init__(self, database: Path):
        self.database = database
        self.directory = database.parent
        self.marker = self.directory.parent / f".{self.directory.name}.initialized"
        self.recreated = False

    def prepare(self, lock_path: Path, *, create: bool = True) -> bool:
        if self.database.exists():
            if not self.database.is_file():
                raise StateBootstrapError("STATE_DATABASE_INVALID")
            return False
        if not create:
            raise FileNotFoundError(f"State database missing: {self.database}")
        if self.marker.is_symlink() or (self.marker.exists() and not self.marker.is_file()):
            raise StateBootstrapError("STATE_DATABASE_INVALID")
        self.recreated = self.marker.exists() or any(
            entry != lock_path for entry in self.directory.iterdir())
        if not self.marker.exists():
            # Persist intent before SQLite can create even an empty database file.
            with self.marker.open("x", encoding="ascii") as stream:
                stream.write("pending\n")
                stream.flush()
                os.fsync(stream.fileno())
            _sync_directory(self.marker.parent)
        self._archive_sidecars()
        return True

    def _archive_sidecars(self) -> None:
        sidecars = [Path(str(self.database) + suffix) for suffix in ("-wal", "-shm", "-journal")]
        present = [path for path in sidecars if path.exists() or path.is_symlink()]
        if not present:
            return
        archive = Path(tempfile.mkdtemp(prefix=f".{self.database.name}.orphans.", dir=self.directory))
        for path in present:
            os.rename(path, archive / path.name)
        _sync_directory(archive)
        _sync_directory(self.directory)

    def mark_ready(self) -> None:
        _sync_directory(self.directory)
        descriptor, name = tempfile.mkstemp(prefix=self.marker.name + ".", dir=self.marker.parent)
        try:
            try:
                # Retain ownership even when constructing the stream fails.
                with os.fdopen(descriptor, "w", encoding="ascii", closefd=False) as stream:
                    stream.write("ready\n")
                    stream.flush()
                    os.fsync(stream.fileno())
            finally:
                os.close(descriptor)
            os.replace(name, self.marker)
            _sync_directory(self.marker.parent)
        finally:
            try:
                os.unlink(name)
            except FileNotFoundError:
                pass  # Successful replacement has already moved this path.
