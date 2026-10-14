"""Admit first startup without treating a lost ledger as a new deployment."""

import os
import tempfile
from pathlib import Path


class StateBootstrapError(ValueError):
    """A fixed, public startup diagnostic without database or credential data."""


def _sync_directory(directory: Path) -> None:
    descriptor = os.open(directory, os.O_RDONLY)
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

    def prepare(self, lock_path: Path) -> bool:
        if self.database.exists():
            if not self.database.is_file():
                raise StateBootstrapError("STATE_DATABASE_INVALID")
            return False
        if self.marker.exists() or self.marker.is_symlink():
            raise StateBootstrapError("STATE_DATABASE_MISSING")
        if any(entry != lock_path for entry in self.directory.iterdir()):
            raise StateBootstrapError("STATE_DIRECTORY_NOT_EMPTY")
        # Persist intent before SQLite can create even an empty database file.
        with self.marker.open("x", encoding="ascii") as stream:
            stream.write("pending\n")
            stream.flush()
            os.fsync(stream.fileno())
        _sync_directory(self.marker.parent)
        return True

    def mark_ready(self) -> None:
        _sync_directory(self.directory)
        descriptor, name = tempfile.mkstemp(prefix=self.marker.name + ".", dir=self.marker.parent)
        temporary = Path(name)
        try:
            with os.fdopen(descriptor, "w", encoding="ascii") as stream:
                stream.write("ready\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.marker)
            _sync_directory(self.marker.parent)
        finally:
            temporary.unlink(missing_ok=True)
