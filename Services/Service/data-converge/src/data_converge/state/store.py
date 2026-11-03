from __future__ import annotations

import json
import logging
import sqlite3
import uuid
from datetime import datetime, timezone as datetime_timezone
from pathlib import Path
from typing import Iterable, Mapping

from data_converge.dag.models import TaskInstance, TaskStatus
from data_converge.dag.retry import can_start_attempt
from data_converge.errors import safe_diagnostic_details

from .bootstrap import StateBootstrapError
from . import cross_period_waits as wait_ledger
from . import preaggregate_coverage
from .database import LedgerDatabase
from . import backfill, checkpoints, configuration, events, history, retirement, preaggregate, preaggregate_protection


logger = logging.getLogger(__name__)


def _stamp(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Persistent timestamps must include a timezone")
    return value.isoformat(timespec="microseconds")


def _window_key(value: datetime) -> str:
    _stamp(value)
    if value.minute % 5 or value.second or value.microsecond:
        raise ValueError("DWD window_start must align to a five-minute boundary")
    return _stamp(value.astimezone(datetime_timezone.utc))


class StateStore:
    """A single local writer owns the lock for the lifetime of the service.

    Read-only inspection is allowed without the lock. Every mutation requires
    acquire_lock(). No SQLite transaction is held while executing external SQL.
    """

    def __init__(self, path: str | Path, create: bool = False):
        self._database = LedgerDatabase(path, create=create)
        self.mirror = None
        self._intervals: dict[str, str] = {}

    def attach_mirror(self, mirror, intervals: Mapping[str, str]) -> None:
        """Publish every committed transition to shared Redis (fenced by the lease)."""
        self.mirror = mirror
        self._intervals = dict(intervals)

    def _publish(self, instance_id: str) -> None:
        """Mirror after the local commit; a fencing failure stops this writer."""
        if self.mirror is None:
            return
        row = self.get_instance(instance_id)
        if row is not None and self.get_retirement(instance_id) is not None:
            return
        interval = self._intervals.get(row['taskname']) if row else None
        if interval is not None:
            self.mirror.write_instance(row, interval)

    def import_shared_state(self) -> dict:
        """Merge the previous leader's published state before scheduling starts."""
        from . import redis_import

        if self.mirror is None:
            return {}
        now = _stamp(datetime.now(datetime_timezone.utc))
        counts = {'cursors': 0, 'IMPORTED': 0, 'REQUEUED': 0, 'dwd': 0}
        conn = self._connection(write=True)
        cursors = self.mirror.read_cursors()
        instances = [row for row in self.mirror.scan_instances() if row.get('taskname') in self._intervals]
        dwd = list(self.mirror.scan_dwd())
        changed = []
        with conn:
            counts['cursors'] = redis_import.import_cursors(conn, cursors)
            for row in instances:
                action = redis_import.import_instance(conn, row)
                if action is not None:
                    counts[action] += 1
                    changed.append((row['instance_id'], action, row.get('query_id')))
            for start, types in dwd:
                counts['dwd'] += redis_import.import_dwd(conn, _window_key(start), types, now)
        for instance_id, action, _query_id in changed:
            if action == 'REQUEUED':
                self._publish(instance_id)  # Clear the shared RUNNING marker.
        counts['interrupted_query_ids'] = [query for _, action, query in changed
                                           if action == 'REQUEUED' and query]
        return counts

    def publish_local_state(self) -> int:
        """Seed Redis from a ledger that predates shared state (first upgrade)."""
        if self.mirror is None:
            return 0
        rows = self._connection().execute(
            "SELECT instance_id FROM task_instances WHERE status IN "
            "('SUCCESS','FAILED','UNKNOWN','PENDING','BLOCKED') ORDER BY data_end DESC LIMIT 20000").fetchall()
        for row in rows:
            self._publish(row[0])
        cursors = self._connection().execute("SELECT taskname, next_fire_time FROM schedule_cursors").fetchall()
        self.mirror.write_cursors({name: datetime.fromisoformat(value) for name, value in cursors})
        return len(rows)

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

    def configuration_snapshot(self) -> dict | None:
        return configuration.snapshot(self._connection())

    def configuration_history(self, limit: int = 20) -> list[dict]:
        return configuration.history(self._connection(), limit)

    def configuration_migration(self, migration_id: str) -> dict | None:
        return configuration.migration(self._connection(), migration_id)

    def get_task_effective_starts(self) -> dict[str, datetime]:
        return configuration.effective_starts(self._connection())

    def configuration_constraints(self, tasknames: Iterable[str]) -> dict:
        return configuration.constraints(self._connection(), tasknames)

    def backup_configuration(self, destination: Path) -> Path:
        return configuration.backup(self._connection(write=True), destination)

    def apply_configuration(self, *, expected_binding, expected_revision, manifest, revision,
                            task_starts, binding, migration_id, applied_at, backup_path=None,
                            reset_task_cursors=(), retire_existing=False, recovery=None) -> None:
        configuration.apply(self._connection(write=True), expected_binding=expected_binding,
                            expected_revision=expected_revision, manifest=manifest, revision=revision,
                            task_starts=task_starts, binding=binding, migration_id=migration_id,
                            applied_at=applied_at, backup_path=backup_path,
                            reset_task_cursors=reset_task_cursors,
                            retire_existing=retire_existing, recovery=recovery)

    def legacy_window_summary(self) -> dict:
        return retirement.summary(self._connection())

    def legacy_recovery(self) -> dict | None:
        return retirement.recovery(self._connection())

    def get_retirement(self, instance_id: str) -> dict | None:
        return retirement.get(self._connection(), instance_id)

    def retired_status_counts(self) -> dict:
        return retirement.status_counts(self._connection(), retired=True)

    def active_status_counts(self) -> dict:
        return retirement.status_counts(self._connection())

    def validate_retirement(self) -> None:
        retirement.validate(self._connection())

    def get_cursor(self, taskname: str) -> datetime | None:
        row = self._connection().execute("SELECT next_fire_time FROM schedule_cursors WHERE taskname=?", (taskname,)).fetchone()
        return datetime.fromisoformat(row[0]) if row else None

    def enqueue_and_advance(self, instances: Iterable[TaskInstance], next_cursors: Mapping[str, datetime],
                            *, cross_period_waits=()) -> None:
        conn = self._connection(write=True)
        prepared = list(instances)
        for instance in prepared:
            retirement.assert_active(conn, instance.instance_id)
        starts = self.get_task_effective_starts()
        configuration.validate_windows((item for item in prepared
            if starts.get(item.taskname) is None or item.data_start >= starts[item.taskname]
            or not preaggregate_coverage.covers_window(conn, item)), starts)
        with conn:
            inserted_ids = set()
            for instance in prepared:
                inserted = conn.execute("""INSERT OR IGNORE INTO task_instances
                    (instance_id, taskname, scheduled_time, data_start, data_end, status)
                    VALUES (?, ?, ?, ?, ?, ?)""",
                    (instance.instance_id, instance.taskname, _stamp(instance.scheduled_time),
                     _stamp(instance.data_start), _stamp(instance.data_end), TaskStatus.PENDING.value))
                if inserted.rowcount == 1:
                    inserted_ids.add(instance.instance_id)
            wait_ledger.register(conn, (row for row in cross_period_waits if row['instance_id'] in inserted_ids))
            for taskname, next_fire_time in next_cursors.items():
                conn.execute("""INSERT INTO schedule_cursors VALUES (?, ?)
                    ON CONFLICT(taskname) DO UPDATE SET next_fire_time=excluded.next_fire_time""",
                    (taskname, _stamp(next_fire_time)))
            preaggregate_protection.validate_cursors(conn)
        if self.mirror is not None:
            self.mirror.write_cursors(dict(next_cursors))

    def get_cross_period_wait(self, instance_id: str) -> dict | None:
        return wait_ledger.get(self._connection(), instance_id)

    def record_cross_period_wait(self, instance_id: str, *, status, missing, now) -> bool:
        conn = self._connection(write=True)
        with conn:
            return wait_ledger.decide(conn, instance_id, status=status, missing=missing, now=now)

    def get_instance(self, instance_id: str) -> dict | None:
        row = self._connection().execute("SELECT * FROM task_instances WHERE instance_id=?", (instance_id,)).fetchone()
        return dict(row) if row else None

    def get_backfill(self, item_id: str) -> dict | None:
        return backfill.get(self._connection(), item_id)

    def begin_backfill(self, item_id: str, instance: TaskInstance, *,
                       recompute: bool, now: datetime | None = None) -> dict:
        return backfill.begin(self._connection(write=True), item_id, instance,
                              recompute=recompute, now=now or datetime.now(datetime_timezone.utc))

    def validate_preaggregate_configuration(self, manifest, task_starts, coverage, *,
                                            timezone=None, cleanup_columns=None) -> None:
        preaggregate_protection.validate(self._connection(), manifest, task_starts, coverage,
                                         timezone=timezone, cleanup_columns=cleanup_columns)

    def preaggregate_recovery_coverage(self, instance: TaskInstance, now: datetime) -> dict | None:
        return preaggregate_coverage.admission(self._connection(), instance, now)

    def pending_preaggregate_coverages(self) -> list[dict]:
        return preaggregate_coverage.pending(self._connection())

    def preaggregate_covers_window(self, instance: TaskInstance) -> bool:
        return preaggregate_coverage.covers_window(self._connection(), instance)

    def get_preaggregate(self, item_id: str) -> dict | None:
        return preaggregate.get(self._connection(), item_id)

    def begin_preaggregate(self, item: dict, instance: TaskInstance,
                           now: datetime | None = None) -> dict:
        return preaggregate.begin(self._connection(write=True), item, instance,
                                  now or datetime.now(datetime_timezone.utc))

    def finish_preaggregate(self, item_id: str, status, error=None,
                            diagnostic=None, now: datetime | None = None) -> None:
        preaggregate.finish(self._connection(write=True), item_id, status,
                            error=error, diagnostic=diagnostic, now=now)

    def latest_preaggregate(self, instance_id: str) -> dict | None:
        return preaggregate.latest(self._connection(), instance_id)

    def list_preaggregates(self, instance_id=None, limit=20, offset=0, *,
                           taskname=None, only_unresolved=False, problems_only=False) -> list[dict]:
        return preaggregate.list_attempts(self._connection(), instance_id, limit, offset,
                                         taskname=taskname, only_unresolved=only_unresolved,
                                         problems_only=problems_only)

    def resolve_preaggregate(self, item_id: str, resolution: str, note: str) -> None:
        preaggregate.resolve(self._connection(write=True), item_id, resolution, note,
                             datetime.now(datetime_timezone.utc))

    def preaggregate_blocker(self, instance_id: str) -> str | None:
        return preaggregate.blocker(self._connection(), instance_id)

    def preaggregate_formal_blocker(self, instance_id: str, now: datetime | None = None) -> str | None:
        return preaggregate_protection.formal_blocker(
            self._connection(), instance_id, now or datetime.now(datetime_timezone.utc))

    def preaggregate_schedule_blocker(self, scheduled_until: datetime, now: datetime) -> str | None:
        return preaggregate_protection.schedule_blocker(self._connection(), scheduled_until, now)

    def get_status(self, instance_id: str) -> TaskStatus | None:
        if self.get_retirement(instance_id) is not None:
            return None
        row = self._connection().execute("SELECT status FROM task_instances WHERE instance_id=?", (instance_id,)).fetchone()
        return TaskStatus(row[0]) if row else None

    def list_pending(self) -> list[dict]:
        # Include all blocked rows so a missing old dependency cannot starve new work.
        conn = self._connection()
        active = retirement.active_filter(conn)
        rows = conn.execute(f"""SELECT * FROM task_instances
            WHERE (status IN ('PENDING', 'BLOCKED') OR (status='FAILED' AND retryable=1))
            AND {active} ORDER BY data_end, taskname""").fetchall()
        return [dict(row) for row in rows]

    def status_counts(self) -> dict[str, int]:
        return dict(self._connection().execute("SELECT status, COUNT(*) FROM task_instances GROUP BY status").fetchall())

    def list_instances(self, limit: int = 20) -> list[dict]:
        if limit < 1:
            raise ValueError("limit must be positive")
        rows = self._connection().execute(
            "SELECT * FROM task_instances ORDER BY julianday(data_end) DESC, taskname LIMIT ?", (limit,)).fetchall()
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

    def list_recovery_candidates(self, limit: int = 20, offset: int = 0) -> list[dict]:
        return checkpoints.recovery_rows(self._connection(), limit=limit, offset=offset)

    def recovery_counts(self) -> dict[str, int]:
        return checkpoints.recovery_counts(self._connection())

    def record_dwd_signal(self, window_start: datetime, dwd_type: str, received_at: datetime) -> None:
        if not isinstance(dwd_type, str) or not dwd_type.strip():
            raise ValueError("dwd_type must be non-empty")
        key = _window_key(window_start)
        _stamp(received_at)
        observed = _stamp(received_at.astimezone(datetime_timezone.utc))
        conn = self._connection(write=True)
        with conn:
            conn.execute("INSERT OR IGNORE INTO dwd_ready_signals VALUES (?, ?, ?)", (key, dwd_type, observed))
        if self.mirror is not None:
            self.mirror.write_dwd(window_start, dwd_type)

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
        now = now or datetime.now(datetime_timezone.utc)
        conn = self._connection(write=True)
        retirement.assert_active(conn, instance_id)
        if preaggregate_protection.formal_blocker(conn, instance_id, now):
            return False
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
                query_id = self.get_instance(instance_id)['query_id']
                wait_ledger.attach_query(conn, instance_id, query_id)
                events.append_event(conn, "DISPATCHED", instance_id, now)
        if updated.rowcount == 1:
            self._publish(instance_id)
        return updated.rowcount == 1

    def prepare_attempt(self, instance_id: str, *, now: datetime | None = None,
                        max_attempts: int = 3) -> bool:
        """Reserve an eligible instance without allowing external execution yet."""
        now = now or datetime.now(datetime_timezone.utc)
        conn = self._connection(write=True)
        retirement.assert_active(conn, instance_id)
        if preaggregate_protection.formal_blocker(conn, instance_id, now):
            return False
        with conn:
            row = conn.execute("SELECT * FROM task_instances WHERE instance_id=?", (instance_id,)).fetchone()
            if row is None:
                raise KeyError(instance_id)
            status = TaskStatus(row["status"])
            if row["attempts"] >= max_attempts or not can_start_attempt(
                    status, row["attempts"], row["retryable"], row["next_retry_at"],
                    now=now, max_attempts=max_attempts):
                return False
            query_id = str(uuid.uuid4())
            result = conn.execute("""UPDATE task_instances SET status='RUNNING',
                started_at=NULL, finished_at=NULL, error=NULL, retryable=0,
                next_retry_at=NULL, query_id=? WHERE instance_id=? AND status=?""",
                (query_id, instance_id, status.value))
            if result.rowcount != 1:
                return False
            checkpoints.record_prepared(conn, instance_id, query_id, _stamp(now), max_attempts)
            events.append_event(conn, "PREPARED", instance_id, now)
            return True

    def mark_dispatched(self, instance_id: str, query_id: str, *, now: datetime | None = None) -> None:
        """Commit dispatch intent and its attempt before the runner can be called."""
        now = now or datetime.now(datetime_timezone.utc)
        conn = self._connection(write=True)
        retirement.assert_active(conn, instance_id)
        preaggregate_protection.assert_formal_allowed(conn, instance_id, now)
        with conn:
            checkpoints.mark_dispatched(conn, instance_id, query_id, _stamp(now))
            result = conn.execute("""UPDATE task_instances SET attempts=attempts+1, started_at=?
                WHERE instance_id=? AND query_id=? AND status='RUNNING'""",
                (_stamp(now), instance_id, query_id))
            if result.rowcount != 1:
                raise RuntimeError(f"Prepared reservation changed: {instance_id}")
            history.record_claim(conn, instance_id)
            wait_ledger.attach_query(conn, instance_id, query_id)
            events.append_event(conn, "DISPATCHED", instance_id, now)
        # Publish RUNNING before SQL is sent, so a takeover knows to rerun it.
        self._publish(instance_id)

    def finish(self, instance_id: str, status: TaskStatus, *, error: str | None = None,
               retryable: bool = False, next_retry_at: datetime | None = None,
               now: datetime | None = None, diagnostic: dict | None = None) -> None:
        if status not in (TaskStatus.SUCCESS, TaskStatus.FAILED, TaskStatus.UNKNOWN):
            raise ValueError("finish requires SUCCESS, FAILED or UNKNOWN")
        now = now or datetime.now(datetime_timezone.utc)
        conn = self._connection(write=True)
        retirement.assert_active(conn, instance_id)
        if status == TaskStatus.SUCCESS:
            preaggregate_protection.assert_formal_allowed(conn, instance_id, now)
        with conn:
            checkpoints.assert_finish_allowed(conn, instance_id)
            result = conn.execute("""UPDATE task_instances SET status=?, error=?,
                retryable=?, next_retry_at=?, finished_at=? WHERE instance_id=? AND status='RUNNING'""",
                (status.value, error, int(retryable and status == TaskStatus.FAILED),
                 _stamp(next_retry_at) if next_retry_at else None,
                 _stamp(now), instance_id))
            if result.rowcount != 1:
                raise RuntimeError(f"Cannot finish an unclaimed instance: {instance_id}")
            if diagnostic and status != TaskStatus.SUCCESS:
                self._save_execution_diagnostic(conn, instance_id, diagnostic)
            history.record_finish(conn, instance_id, diagnostic)
            backfill.record_finish(conn, instance_id)
            if status == TaskStatus.SUCCESS:
                preaggregate.mark_covered(conn, instance_id, now)
            event = {TaskStatus.SUCCESS: "SUCCEEDED", TaskStatus.FAILED: "FAILED",
                     TaskStatus.UNKNOWN: "OUTCOME_UNKNOWN"}[status]
            events.append_event(conn, event, instance_id, now)
        self._publish(instance_id)

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
        retirement.assert_active(conn, instance_id)
        with conn:
            conn.execute("""UPDATE task_instances SET status='BLOCKED', error=?
                WHERE instance_id=? AND status IN ('PENDING', 'BLOCKED', 'FAILED')
                AND (status!='BLOCKED' OR error IS NOT ?)""", (reason, instance_id, reason))

    def recover_running(self) -> int:
        conn = self._connection(write=True)
        with conn:
            now = _stamp(datetime.now(datetime_timezone.utc))
            backfill.recover_running(conn, datetime.fromisoformat(now))
            preaggregate.recover_running(conn, datetime.fromisoformat(now))
            rows = checkpoints.classified_rows(conn, include_unknown=False)

            # 自动恢复所有 RUNNING 任务（包括 PREPARED 和 DISPATCHED）
            # 配合 D3 幂等 DELETE，可以安全地重新执行任何阶段崩溃的任务
            recovered_count = 0
            for row in rows:
                if row["action"] in ("AUTO_RESUME", "RECONCILE"):
                    try:
                        checkpoints.recover_with_idempotent_protection(conn, row, now)
                        events.append_event(
                            conn,
                            "IDEMPOTENT_RECOVERED",
                            row["instance_id"],
                            now
                        )
                        recovered_count += 1
                    except RuntimeError as e:
                        # 如果恢复失败，记录错误但继续处理其他任务
                        logger.error(f"Failed to recover {row['instance_id']}: {e}")
                        continue

            history.record_recovery(conn, now)
        for row in rows:
            if row["action"] in ("AUTO_RESUME", "RECONCILE"):
                self._publish(row["instance_id"])
        return recovered_count

    def resolve(self, instance_id: str, resolution: str, note: str) -> None:
        resolution = resolution.upper()
        if resolution not in ("SUCCESS", "PENDING"):
            raise ValueError("resolution must be SUCCESS or PENDING")
        if not note.strip():
            raise ValueError("A non-empty operator note is required")
        now = _stamp(datetime.now(datetime_timezone.utc))
        conn = self._connection(write=True)
        retirement.assert_active(conn, instance_id)
        preaggregate.assert_unblocked(conn, instance_id)
        if resolution == "SUCCESS":
            preaggregate_protection.assert_formal_allowed(conn, instance_id, datetime.fromisoformat(now))
        with conn:
            result = conn.execute("""UPDATE task_instances SET status=?, retryable=0,
                next_retry_at=NULL, resolution_note=?, resolved_at=?, error=NULL,
                attempts=CASE WHEN ?='PENDING' THEN 0 ELSE attempts END
                WHERE instance_id=? AND (status='UNKNOWN' OR (?='PENDING' AND status='FAILED'))""",
                (resolution, note.strip(), now, resolution, instance_id, resolution))
            if result.rowcount != 1:
                raise ValueError(f"Only UNKNOWN can become SUCCESS; only UNKNOWN/FAILED can retry: {instance_id}")
            history.record_resolution(conn, instance_id)
            event = "RESOLVED_SUCCESS" if resolution == "SUCCESS" else "RETRY_AUTHORIZED"
            events.append_event(conn, event, instance_id, now)
        self._publish(instance_id)
