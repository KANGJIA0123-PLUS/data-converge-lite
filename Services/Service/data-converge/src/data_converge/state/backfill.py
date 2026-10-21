"""Atomically journal operator-authorized executions in the writer's ledger."""

from datetime import datetime
import uuid

from data_converge.dag.builder import timeline_time
from . import checkpoints, events, history, retirement, preaggregate_protection


def available(conn):
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                        "AND name='backfill_attempts'").fetchone() is not None


def get(conn, item_id):
    if not available(conn):
        return None
    row = conn.execute("SELECT * FROM backfill_attempts WHERE item_id=?", (item_id,)).fetchone()
    return dict(row) if row is not None else None


def _create_schema(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS backfill_attempts (
        item_id TEXT PRIMARY KEY, instance_id TEXT NOT NULL,
        query_id TEXT, status TEXT NOT NULL, error TEXT, created_at TEXT NOT NULL
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS backfill_attempts_query ON backfill_attempts(query_id)")


def _existing_decision(conn, instance, recompute, now):
    reason = preaggregate_protection.formal_blocker(conn, instance.instance_id, now)
    if reason:
        return "BLOCKED", reason
    if retirement.get(conn, instance.instance_id) is not None:
        return "BLOCKED", "BACKFILL_RETIRED_INSTANCE"
    row = conn.execute("SELECT * FROM task_instances WHERE instance_id=?",
                       (instance.instance_id,)).fetchone()
    if row is None:
        return None
    for field in ("data_start", "data_end"):
        if timeline_time(datetime.fromisoformat(row[field])) != timeline_time(getattr(instance, field)):
            return "BLOCKED", "BACKFILL_WINDOW_CHANGED"
    if row["status"] in ("UNKNOWN", "RUNNING"):
        return "BLOCKED", "BACKFILL_INSTANCE_" + row["status"]
    if row["status"] == "SUCCESS" and not recompute:
        return "SKIPPED", None
    return None


def begin(conn, item_id, instance, *, recompute, now):
    """Commit dispatch before SQL; repeated item IDs return the original fact."""
    previous = get(conn, item_id)
    if previous is not None:
        return previous
    _create_schema(conn)
    stamp = now.isoformat(timespec="microseconds")
    with conn:
        decision = _existing_decision(conn, instance, recompute, now)
        query_id = None
        status, error = decision or ("RUNNING", None)
        if decision is None:
            query_id = _dispatch(conn, instance, now)
        conn.execute("INSERT INTO backfill_attempts VALUES (?, ?, ?, ?, ?, ?)",
                     (item_id, instance.instance_id, query_id, status, error, stamp))
    return get(conn, item_id)


def _dispatch(conn, instance, now):
    stamp = now.isoformat(timespec="microseconds")
    conn.execute("""INSERT OR IGNORE INTO task_instances
        (instance_id, taskname, scheduled_time, data_start, data_end, status)
        VALUES (?, ?, ?, ?, ?, 'PENDING')""",
        (instance.instance_id, instance.taskname, instance.scheduled_time.isoformat(),
         instance.data_start.isoformat(), instance.data_end.isoformat()))
    query_id = str(uuid.uuid4())
    conn.execute("""UPDATE task_instances SET status='RUNNING', query_id=?,
        started_at=?, finished_at=NULL, error=NULL, retryable=0, next_retry_at=NULL,
        resolution_note=NULL, resolved_at=NULL WHERE instance_id=?""",
        (query_id, stamp, instance.instance_id))
    attempts = conn.execute("SELECT attempts FROM task_instances WHERE instance_id=?",
                            (instance.instance_id,)).fetchone()[0]
    checkpoints.record_prepared(conn, instance.instance_id, query_id, stamp, attempts + 1)
    checkpoints.mark_dispatched(conn, instance.instance_id, query_id, stamp)
    conn.execute("UPDATE task_instances SET attempts=attempts+1 WHERE instance_id=?",
                 (instance.instance_id,))
    history.record_claim(conn, instance.instance_id)
    events.append_event(conn, "BACKFILL_DISPATCHED", instance.instance_id, now)
    return query_id


def record_finish(conn, instance_id):
    if not available(conn):
        return
    row = conn.execute("SELECT query_id, status, error FROM task_instances WHERE instance_id=?",
                       (instance_id,)).fetchone()
    conn.execute("""UPDATE backfill_attempts SET status=?, error=?
        WHERE instance_id=? AND query_id=? AND status='RUNNING'""",
        (row["status"], row["error"], instance_id, row["query_id"]))


def recover_running(conn, now):
    """Never replay a manually dispatched write after losing its outcome."""
    if not available(conn):
        return
    rows = conn.execute("SELECT * FROM backfill_attempts WHERE status='RUNNING'").fetchall()
    for row in rows:
        stamp = now.isoformat(timespec="microseconds")
        changed = conn.execute("""UPDATE task_instances SET status='UNKNOWN',
            error='EXECUTION_INTERRUPTED', retryable=0, next_retry_at=NULL, finished_at=?
            WHERE instance_id=? AND query_id=? AND status='RUNNING'""",
            (stamp, row["instance_id"], row["query_id"]))
        if changed.rowcount:
            history.record_finish(conn, row["instance_id"], None)
            events.append_event(conn, "OUTCOME_UNKNOWN", row["instance_id"], now)
        conn.execute("""UPDATE backfill_attempts SET status='UNKNOWN', error='EXECUTION_INTERRUPTED'
            WHERE item_id=?""", (row["item_id"],))
