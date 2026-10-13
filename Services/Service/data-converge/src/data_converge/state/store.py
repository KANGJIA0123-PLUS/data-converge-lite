from __future__ import annotations

import fcntl
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping

from data_converge.dag.models import TaskInstance, TaskStatus
from data_converge.dag.retry import can_start_attempt


def _stamp(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Persistent timestamps must include a timezone")
    return value.isoformat(timespec="microseconds")


def _window_key(value: datetime) -> str:
    _stamp(value)
    if value.minute % 5 or value.second or value.microsecond:
        raise ValueError("DWD window_start must align to a five-minute boundary")
    return _stamp(value.astimezone(timezone.utc))


class StateStore:
    """A single local writer owns the lock for the lifetime of the service.

    Read-only inspection is allowed without the lock. Every mutation requires
    acquire_lock(). No SQLite transaction is held while executing external SQL.
    """

    def __init__(self, path: str | Path, create: bool = False):
        self.path = Path(path).resolve()
        self.lock_path = Path(str(self.path) + ".lock")
        self._create = create
        self._lock_file = None
        self._conn: sqlite3.Connection | None = None
        if not create and not self.path.is_file():
            raise FileNotFoundError(f"State database missing: {self.path}; use --init-state explicitly")

    def acquire_lock(self) -> None:
        if self._lock_file is not None:
            return
        if self._create:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        lock_file = self.lock_path.open("a+")
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            lock_file.close()
            raise RuntimeError(f"Another local writer owns the state lock: {self.lock_path}") from exc
        self._lock_file = lock_file
        try:
            if self._conn is not None:
                self._conn.close()
            mode = "rwc" if self._create else "rw"
            self._conn = sqlite3.connect(self.path.as_uri() + f"?mode={mode}", uri=True)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA busy_timeout=5000")
            self._conn.execute("PRAGMA synchronous=FULL")
            if self._create:
                self._create_ledger_schema()
            # An empty or unrelated database must not silently become a new ledger.
            self._conn.execute("SELECT key, value FROM metadata LIMIT 1").fetchall()
            self._conn.execute("SELECT instance_id FROM task_instances LIMIT 1").fetchall()
            self._conn.execute("SELECT next_fire_time FROM schedule_cursors LIMIT 1").fetchall()
            # Add the ready facts only after validating the existing ledger.
            with self._conn:
                self._conn.execute("""CREATE TABLE IF NOT EXISTS dwd_ready_signals (
                    window_start TEXT NOT NULL,
                    dwd_type TEXT NOT NULL,
                    received_at TEXT NOT NULL,
                    PRIMARY KEY (window_start, dwd_type)
                )""")
        except BaseException:
            self.close()
            raise

    def _create_ledger_schema(self) -> None:
        """Create the base ledger only during explicit state initialization."""
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

    def _connection(self, *, write: bool = False) -> sqlite3.Connection:
        if write and self._lock_file is None:
            raise RuntimeError("State mutation requires acquire_lock()")
        if self._conn is None:
            self._conn = sqlite3.connect(self.path.as_uri() + "?mode=ro", uri=True)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA busy_timeout=5000")
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
        if self._lock_file is not None:
            fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_UN)
            self._lock_file.close()
            self._lock_file = None

    def get_meta(self, key: str, default: str | None = None) -> str | None:
        row = self._connection().execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
        return row[0] if row else default

    def set_meta(self, key: str, value: str) -> None:
        conn = self._connection(write=True)
        with conn:
            conn.execute("INSERT INTO metadata VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))

    def get_cursor(self, taskname: str) -> datetime | None:
        row = self._connection().execute("SELECT next_fire_time FROM schedule_cursors WHERE taskname=?", (taskname,)).fetchone()
        return datetime.fromisoformat(row[0]) if row else None

    def enqueue_and_advance(self, instances: Iterable[TaskInstance], next_cursors: Mapping[str, datetime]) -> None:
        conn = self._connection(write=True)
        with conn:
            for instance in instances:
                conn.execute("""INSERT OR IGNORE INTO task_instances
                    (instance_id, taskname, scheduled_time, data_start, data_end, status)
                    VALUES (?, ?, ?, ?, ?, ?)""",
                    (instance.instance_id, instance.taskname, _stamp(instance.scheduled_time),
                     _stamp(instance.data_start), _stamp(instance.data_end), TaskStatus.PENDING.value))
            for taskname, next_fire_time in next_cursors.items():
                conn.execute("""INSERT INTO schedule_cursors VALUES (?, ?)
                    ON CONFLICT(taskname) DO UPDATE SET next_fire_time=excluded.next_fire_time""",
                    (taskname, _stamp(next_fire_time)))

    def get_instance(self, instance_id: str) -> dict | None:
        row = self._connection().execute("SELECT * FROM task_instances WHERE instance_id=?", (instance_id,)).fetchone()
        return dict(row) if row else None

    def get_status(self, instance_id: str) -> TaskStatus | None:
        row = self._connection().execute("SELECT status FROM task_instances WHERE instance_id=?", (instance_id,)).fetchone()
        return TaskStatus(row[0]) if row else None

    def list_pending(self) -> list[dict]:
        # Include all blocked rows so a missing old dependency cannot starve new work.
        rows = self._connection().execute("""SELECT * FROM task_instances
            WHERE status IN ('PENDING', 'BLOCKED') OR (status='FAILED' AND retryable=1)
            ORDER BY data_end, taskname""").fetchall()
        return [dict(row) for row in rows]

    def status_counts(self) -> dict[str, int]:
        return dict(self._connection().execute("SELECT status, COUNT(*) FROM task_instances GROUP BY status").fetchall())

    def list_instances(self, limit: int = 20) -> list[dict]:
        if limit < 1:
            raise ValueError("limit must be positive")
        rows = self._connection().execute("SELECT * FROM task_instances ORDER BY data_end DESC, taskname LIMIT ?", (limit,)).fetchall()
        return [dict(row) for row in rows]

    def record_dwd_signal(self, window_start: datetime, dwd_type: str, received_at: datetime) -> None:
        if not isinstance(dwd_type, str) or not dwd_type.strip():
            raise ValueError("dwd_type must be non-empty")
        key = _window_key(window_start)
        _stamp(received_at)
        observed = _stamp(received_at.astimezone(timezone.utc))
        conn = self._connection(write=True)
        with conn:
            conn.execute("INSERT OR IGNORE INTO dwd_ready_signals VALUES (?, ?, ?)", (key, dwd_type, observed))

    def _has_dwd_table(self) -> bool:
        return self._connection().execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='dwd_ready_signals'").fetchone() is not None

    def get_dwd_types(self, window_start: datetime) -> set[str]:
        key = _window_key(window_start)
        if not self._has_dwd_table():
            return set()
        rows = self._connection().execute("SELECT dwd_type FROM dwd_ready_signals WHERE window_start=?", (key,)).fetchall()
        return {row[0] for row in rows}

    def list_dwd_windows(self, limit: int = 20) -> list[dict]:
        if limit < 1:
            raise ValueError("limit must be positive")
        if not self._has_dwd_table():
            return []
        rows = self._connection().execute("""SELECT window_start, MIN(received_at) AS received_at,
            MAX(received_at) AS last_received_at FROM dwd_ready_signals
            GROUP BY window_start ORDER BY window_start DESC LIMIT ?""", (limit,)).fetchall()
        return [dict(row) | {"types": sorted(self.get_dwd_types(datetime.fromisoformat(row["window_start"])))} for row in rows]

    def claim(self, instance_id: str, *, now: datetime | None = None, max_attempts: int = 3) -> bool:
        now = now or datetime.now(timezone.utc)
        conn = self._connection(write=True)
        with conn:
            row = conn.execute("SELECT * FROM task_instances WHERE instance_id=?", (instance_id,)).fetchone()
            if not row:
                raise KeyError(instance_id)
            status = TaskStatus(row["status"])
            if not can_start_attempt(status, row["attempts"], row["retryable"],
                                     row["next_retry_at"] or None, now=now,
                                     max_attempts=max_attempts):
                return False
            updated = conn.execute("""UPDATE task_instances SET status='RUNNING',
                attempts=attempts+1, started_at=?, finished_at=NULL, error=NULL,
                retryable=0, next_retry_at=NULL, query_id=? WHERE instance_id=? AND status=?""",
                (_stamp(now), str(uuid.uuid4()), instance_id, status.value))
            return updated.rowcount == 1

    def finish(self, instance_id: str, status: TaskStatus, *, error: str | None = None,
               retryable: bool = False, next_retry_at: datetime | None = None,
               now: datetime | None = None) -> None:
        if status not in (TaskStatus.SUCCESS, TaskStatus.FAILED, TaskStatus.UNKNOWN):
            raise ValueError("finish requires SUCCESS, FAILED or UNKNOWN")
        conn = self._connection(write=True)
        with conn:
            result = conn.execute("""UPDATE task_instances SET status=?, error=?,
                retryable=?, next_retry_at=?, finished_at=? WHERE instance_id=? AND status='RUNNING'""",
                (status.value, error, int(retryable and status == TaskStatus.FAILED),
                 _stamp(next_retry_at) if next_retry_at else None,
                 _stamp(now or datetime.now(timezone.utc)), instance_id))
            if result.rowcount != 1:
                raise RuntimeError(f"Cannot finish an unclaimed instance: {instance_id}")

    def mark_blocked(self, instance_id: str, reason: str) -> None:
        conn = self._connection(write=True)
        with conn:
            conn.execute("""UPDATE task_instances SET status='BLOCKED', error=?
                WHERE instance_id=? AND status IN ('PENDING', 'BLOCKED', 'FAILED')
                AND (status!='BLOCKED' OR error IS NOT ?)""", (reason, instance_id, reason))

    def recover_running(self) -> int:
        conn = self._connection(write=True)
        with conn:
            result = conn.execute("""UPDATE task_instances SET status='UNKNOWN', retryable=0,
                next_retry_at=NULL, error='Process stopped before execution outcome was durably recorded'
                WHERE status='RUNNING'""")
            return result.rowcount

    def resolve(self, instance_id: str, resolution: str, note: str) -> None:
        resolution = resolution.upper()
        if resolution not in ("SUCCESS", "PENDING"):
            raise ValueError("resolution must be SUCCESS or PENDING")
        if not note.strip():
            raise ValueError("A non-empty operator note is required")
        conn = self._connection(write=True)
        with conn:
            result = conn.execute("""UPDATE task_instances SET status=?, retryable=0,
                next_retry_at=NULL, resolution_note=?, resolved_at=?, error=NULL,
                attempts=CASE WHEN ?='PENDING' THEN 0 ELSE attempts END
                WHERE instance_id=? AND (status='UNKNOWN' OR (?='PENDING' AND status='FAILED'))""",
                (resolution, note.strip(), _stamp(datetime.now(timezone.utc)), resolution, instance_id, resolution))
            if result.rowcount != 1:
                raise ValueError(f"Only UNKNOWN can become SUCCESS; only UNKNOWN/FAILED can retry: {instance_id}")
