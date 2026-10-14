"""Own the ledger's SQLite connection and lifetime single-writer file lock."""

from __future__ import annotations

import fcntl
import sqlite3
from pathlib import Path
from typing import TextIO

from .bootstrap import StateBootstrap, StateBootstrapError


class LedgerDatabase:
    """Manage resources only; callers own business transactions on the connection.

    Readers open without a lock or schema changes. A writer keeps the same file
    lock until its connection has closed, including startup failure cleanup.
    """

    def __init__(self, path: str | Path, create: bool = False):
        self.path = Path(path).resolve()
        self.lock_path = Path(str(self.path) + ".lock")
        self._create = create
        self._lock_file: TextIO | None = None
        self._conn: sqlite3.Connection | None = None
        self._startup_created: bool | None = None
        if not create and not self.path.is_file():
            raise FileNotFoundError(f"State database missing: {self.path}; use --init-state explicitly")

    def acquire_lock(self) -> None:
        if self._lock_file is not None:
            return
        if self._create:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self._acquire_file_lock()
        try:
            self._open_database(create=self._create, upgrade=True)
        except BaseException:
            self.close()
            raise

    def acquire_startup_lock(self) -> bool:
        """Admit an unmarked, otherwise empty directory; never repair a ledger."""
        if self._lock_file is not None:
            if self._startup_created is None:
                raise RuntimeError("State startup requires its own lock acquisition")
            return self._startup_created
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._acquire_file_lock()
        try:
            created = StateBootstrap(self.path).prepare(self.lock_path)
            self._open_database(create=created, upgrade=created)
            self._validate_startup_schema()
            self._startup_created = created
            return created
        except sqlite3.DatabaseError:
            self.close()
            raise StateBootstrapError("STATE_DATABASE_INVALID") from None
        except BaseException:
            self.close()
            raise

    def _acquire_file_lock(self) -> None:
        lock_file = self.lock_path.open("a+")
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            lock_file.close()
            raise RuntimeError(f"Another local writer owns the state lock: {self.lock_path}") from exc
        except BaseException:
            lock_file.close()
            raise
        self._lock_file = lock_file

    def _connect(self, mode: str) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path.as_uri() + f"?mode={mode}", uri=True)
        try:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA busy_timeout=5000")
            return connection
        except BaseException:
            connection.close()
            raise

    def _open_database(self, *, create: bool, upgrade: bool) -> None:
        self._close_connection()
        self._conn = self._connect("rwc" if create else "rw")
        self._conn.execute("PRAGMA synchronous=FULL")
        if create:
            self._create_ledger_schema()
        self._conn.execute("SELECT key, value FROM metadata LIMIT 1").fetchall()
        self._conn.execute("SELECT instance_id FROM task_instances LIMIT 1").fetchall()
        self._conn.execute("SELECT next_fire_time FROM schedule_cursors LIMIT 1").fetchall()
        if upgrade:
            with self._conn:
                self._conn.execute("""CREATE TABLE IF NOT EXISTS dwd_ready_signals (
                    window_start TEXT NOT NULL,
                    dwd_type TEXT NOT NULL,
                    received_at TEXT NOT NULL,
                    PRIMARY KEY (window_start, dwd_type)
                )""")

    def _validate_startup_schema(self) -> None:
        self._conn.execute("""SELECT instance_id, taskname, scheduled_time,
            data_start, data_end, status, attempts, retryable, next_retry_at,
            started_at, finished_at, error, query_id, resolution_note, resolved_at
            FROM task_instances LIMIT 1""").fetchall()
        self._conn.execute("SELECT taskname, next_fire_time FROM schedule_cursors LIMIT 1").fetchall()
        self._conn.execute("SELECT window_start, dwd_type, received_at FROM dwd_ready_signals LIMIT 1").fetchall()

    def _create_ledger_schema(self) -> None:
        """Create the base ledger after the caller admits state initialization."""
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY, value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS schedule_cursors (
                taskname TEXT PRIMARY KEY, next_fire_time TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS task_instances (
                instance_id TEXT PRIMARY KEY,
                taskname TEXT NOT NULL,
                scheduled_time TEXT NOT NULL,
                data_start TEXT NOT NULL,
                data_end TEXT NOT NULL,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                retryable INTEGER NOT NULL DEFAULT 0,
                next_retry_at TEXT,
                started_at TEXT,
                finished_at TEXT,
                error TEXT,
                query_id TEXT,
                resolution_note TEXT,
                resolved_at TEXT
            );
            CREATE INDEX IF NOT EXISTS task_instances_status
                ON task_instances(status, data_end);
        """)

    def connection(self, *, write: bool = False) -> sqlite3.Connection:
        if write and self._lock_file is None:
            raise RuntimeError("State mutation requires acquire_lock()")
        if self._conn is None:
            self._conn = self._connect("ro")
        return self._conn

    def mark_ready(self) -> None:
        """Persist the marker after StateStore has verified application binding."""
        self.connection(write=True)
        StateBootstrap(self.path).mark_ready()

    def _close_connection(self) -> None:
        connection, self._conn = self._conn, None
        if connection is not None:
            connection.close()

    def close(self) -> None:
        self._startup_created = None
        try:
            self._close_connection()
        finally:
            lock_file, self._lock_file = self._lock_file, None
            if lock_file is not None:
                try:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
                finally:
                    lock_file.close()
