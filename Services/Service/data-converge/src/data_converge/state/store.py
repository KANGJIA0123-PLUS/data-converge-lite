from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping

from data_converge.dag.models import TaskInstance, TaskStatus
from data_converge.dag.retry import can_start_attempt
from data_converge.errors import safe_diagnostic_details

from .bootstrap import StateBootstrapError
from .database import LedgerDatabase
from . import history


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
        self._database = LedgerDatabase(path, create=create)

    @property
    def path(self) -> Path:
        return self._database.path

    @property
    def lock_path(self) -> Path:
        return self._database.lock_path

    def acquire_lock(self) -> None:
        self._database.acquire_lock()

    def acquire_startup_lock(self) -> bool:
        return self._database.acquire_startup_lock()

    def mark_initialized(self) -> None:
        """Record successful application binding only while holding its lock."""
        self._connection(write=True)
        if not self.get_meta("binding") or not self.get_meta("coverage_start"):
            raise StateBootstrapError("STATE_NOT_INITIALIZED")
        self._database.mark_ready()

    def _connection(self, *, write: bool = False) -> sqlite3.Connection:
        return self._database.connection(write=write)

    def close(self) -> None:
        self._database.close()

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

    def history_available(self) -> bool:
        return history.available(self._connection())

    def list_execution_attempts(self, instance_id: str | None = None, *,
                                taskname: str | None = None, limit: int = 20,
                                offset: int = 0) -> list[dict]:
        return history.list_attempts(self._connection(), instance_id, taskname=taskname,
                                     limit=limit, offset=offset)

    def list_legacy_resolutions(self, instance_id: str | None = None, *,
                                taskname: str | None = None, limit: int = 20,
                                offset: int = 0) -> list[dict]:
        return history.list_legacy_resolutions(self._connection(), instance_id, taskname=taskname,
                                               limit=limit, offset=offset)

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
            if updated.rowcount == 1:
                history.record_claim(conn, instance_id)
            return updated.rowcount == 1

    def finish(self, instance_id: str, status: TaskStatus, *, error: str | None = None,
               retryable: bool = False, next_retry_at: datetime | None = None,
               now: datetime | None = None, diagnostic: dict | None = None) -> None:
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
            if diagnostic and status != TaskStatus.SUCCESS:
                self._save_execution_diagnostic(conn, instance_id, diagnostic)
            history.record_finish(conn, instance_id, diagnostic)

    @staticmethod
    def _save_execution_diagnostic(conn, instance_id: str, diagnostic: dict) -> None:
        """Add evidence atomically with failure, without upgrading old ledgers on read."""
        details = safe_diagnostic_details(diagnostic)
        query_id = conn.execute("SELECT query_id FROM task_instances WHERE instance_id=?", (instance_id,)).fetchone()[0]
        details["query_id"] = query_id
        conn.execute("""CREATE TABLE IF NOT EXISTS execution_diagnostics (
            instance_id TEXT PRIMARY KEY, query_id TEXT NOT NULL, details TEXT NOT NULL
        )""")
        conn.execute("""INSERT INTO execution_diagnostics (instance_id, query_id, details)
            VALUES (?, ?, ?) ON CONFLICT(instance_id) DO UPDATE SET
            query_id=excluded.query_id, details=excluded.details""",
            (instance_id, query_id, json.dumps(details, sort_keys=True)))

    def get_execution_diagnostic(self, instance_id: str) -> dict | None:
        conn = self._connection()
        if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='execution_diagnostics'").fetchone():
            return None
        row = conn.execute("""SELECT d.details FROM execution_diagnostics d
            JOIN task_instances t ON t.instance_id=d.instance_id AND t.query_id=d.query_id
            WHERE d.instance_id=? AND t.status IN ('FAILED', 'UNKNOWN')""", (instance_id,)).fetchone()
        if row is None:
            return None
        return safe_diagnostic_details(json.loads(row[0]))

    def mark_blocked(self, instance_id: str, reason: str) -> None:
        conn = self._connection(write=True)
        with conn:
            conn.execute("""UPDATE task_instances SET status='BLOCKED', error=?
                WHERE instance_id=? AND status IN ('PENDING', 'BLOCKED', 'FAILED')
                AND (status!='BLOCKED' OR error IS NOT ?)""", (reason, instance_id, reason))

    def recover_running(self) -> int:
        conn = self._connection(write=True)
        with conn:
            history.record_recovery(conn, _stamp(datetime.now(timezone.utc)))
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
            history.record_resolution(conn, instance_id)
