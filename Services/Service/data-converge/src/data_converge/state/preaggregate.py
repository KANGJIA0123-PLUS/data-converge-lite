"""Independent receipts for early writes and their human reconciliation."""
from datetime import datetime, timezone
import json
import uuid

from data_converge.dag.builder import timeline_time
from data_converge.errors import safe_diagnostic_details
from . import retirement


def available(conn):
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                        "AND name='preaggregate_attempts'").fetchone() is not None


def stamp(value):
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError("PREAGGREGATE_TIMEZONE_REQUIRED")
    return value.isoformat(timespec="microseconds")


def _create_schema(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS preaggregate_attempts (
        item_id TEXT PRIMARY KEY, instance_id TEXT NOT NULL, taskname TEXT NOT NULL,
        revision TEXT NOT NULL, config_evidence TEXT NOT NULL,
        formal_scheduled_time TEXT NOT NULL, data_start TEXT NOT NULL, data_end TEXT NOT NULL,
        query_id TEXT, status TEXT NOT NULL, created_at TEXT NOT NULL, started_at TEXT,
        finished_at TEXT, error TEXT, diagnostic TEXT, resolution TEXT,
        resolution_note TEXT, resolved_at TEXT, covered_at TEXT
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS preaggregate_instance "
                 "ON preaggregate_attempts(instance_id, created_at)")


def _record(row):
    result = dict(row)
    result["config_evidence"] = json.loads(result["config_evidence"])
    result["diagnostic"] = json.loads(result["diagnostic"]) if result["diagnostic"] else None
    result["effective_status"] = {"success": "SUCCESS", "retry": "FAILED"}.get(
        result["resolution"], result["status"])
    return result


def get(conn, item_id):
    if not available(conn):
        return None
    row = conn.execute("SELECT * FROM preaggregate_attempts WHERE item_id=?", (item_id,)).fetchone()
    return _record(row) if row is not None else None


def list_attempts(conn, instance_id=None, limit=20, offset=0, *, taskname=None, only_unresolved=False,
                  problems_only=False):
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
        raise ValueError("limit must be an integer between 1 and 1000")
    if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
        raise ValueError("offset must be a nonnegative integer")
    if not available(conn):
        return []
    clauses, values = [], []
    for field, value in (("instance_id", instance_id), ("taskname", taskname)):
        if value is not None:
            clauses.append(field + "=?")
            values.append(value)
    if only_unresolved:
        clauses.append("status IN ('RUNNING', 'UNKNOWN') AND resolution IS NULL")
    if problems_only:
        clauses.append("covered_at IS NULL AND resolution IS NULL "
                       "AND status IN ('FAILED', 'BLOCKED', 'UNKNOWN')")
    condition = " AND ".join(clauses) if clauses else "1=1"
    rows = conn.execute(f"SELECT * FROM preaggregate_attempts WHERE {condition} "
                        "ORDER BY rowid DESC LIMIT ? OFFSET ?",
                        (*values, limit, offset)).fetchall()
    return [_record(row) for row in rows]


def latest(conn, instance_id):
    rows = list_attempts(conn, instance_id, limit=1)
    return rows[0] if rows else None


def blocker(conn, instance_id):
    rows = list_attempts(conn, instance_id, limit=1, only_unresolved=True)
    if rows:
        return "PREAGGREGATE_UNRESOLVED:" + rows[0]["item_id"]
    return None


def assert_unblocked(conn, instance_id):
    reason = blocker(conn, instance_id)
    if reason:
        raise ValueError(reason)


def _decision(conn, instance, now):
    if timeline_time(now) >= timeline_time(instance.data_end):
        return "SKIPPED", "PREAGGREGATE_WINDOW_EXPIRED"
    if timeline_time(now) < timeline_time(instance.data_start):
        return "BLOCKED", "PREAGGREGATE_WINDOW_NOT_STARTED"
    if retirement.get(conn, instance.instance_id) is not None:
        return "BLOCKED", "PREAGGREGATE_RETIRED_INSTANCE"
    reason = blocker(conn, instance.instance_id)
    if reason:
        return "BLOCKED", reason
    row = conn.execute("SELECT status FROM task_instances WHERE instance_id=?",
                       (instance.instance_id,)).fetchone()
    if row is not None:
        return "BLOCKED", "PREAGGREGATE_FORMAL_" + row[0]
    return "RUNNING", None


def begin(conn, item, instance, now):
    previous = get(conn, item["item_id"])
    if previous is not None:
        return previous
    evidence = json.dumps(item["config_evidence"], sort_keys=True, allow_nan=False)
    due = stamp(datetime.fromisoformat(item["formal_scheduled_time"]))
    current = stamp(now)
    with conn:
        _create_schema(conn)
        status, error = _decision(conn, instance, now)
        query_id = str(uuid.uuid4()) if status == "RUNNING" else None
        conn.execute("""INSERT INTO preaggregate_attempts
            (item_id,instance_id,taskname,revision,config_evidence,formal_scheduled_time,
             data_start,data_end,query_id,status,created_at,started_at,finished_at,error)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (item["item_id"], instance.instance_id, instance.taskname, item["revision"], evidence,
             due, stamp(instance.data_start), stamp(instance.data_end), query_id, status, current,
             current if query_id else None, None if query_id else current, error))
    return get(conn, item["item_id"])


def finish(conn, item_id, status, error=None, diagnostic=None, now=None):
    status = getattr(status, "value", status)
    if status not in ("SUCCESS", "FAILED", "UNKNOWN"):
        raise ValueError("preaggregate finish requires SUCCESS, FAILED or UNKNOWN")
    details = safe_diagnostic_details(diagnostic) if diagnostic else None
    if details:
        details["query_id"] = get(conn, item_id)["query_id"]
    with conn:
        result = conn.execute("""UPDATE preaggregate_attempts SET status=?,error=?,diagnostic=?,
            finished_at=? WHERE item_id=? AND status='RUNNING'""",
            (status, error, json.dumps(details) if details else None,
             stamp(now or datetime.now(timezone.utc)), item_id))
        if result.rowcount != 1:
            raise RuntimeError("PREAGGREGATE_NOT_RUNNING:" + item_id)


def recover_running(conn, now):
    if not available(conn):
        return 0
    result = conn.execute("""UPDATE preaggregate_attempts SET status='UNKNOWN',
        error='EXECUTION_INTERRUPTED', finished_at=? WHERE status='RUNNING'""", (stamp(now),))
    return result.rowcount


def resolve(conn, item_id, resolution, note, now):
    if resolution not in ("success", "retry"):
        raise ValueError("preaggregate resolution must be success or retry")
    if not isinstance(note, str) or not note.strip():
        raise ValueError("Resolution note must be non-empty")
    if not available(conn):
        raise ValueError("PREAGGREGATE_NOT_UNKNOWN:" + item_id)
    with conn:
        result = conn.execute("""UPDATE preaggregate_attempts SET resolution=?,
            resolution_note=?,resolved_at=? WHERE item_id=? AND status='UNKNOWN'
            AND resolution IS NULL""", (resolution, note.strip(), stamp(now), item_id))
        if result.rowcount != 1:
            raise ValueError("PREAGGREGATE_NOT_UNKNOWN:" + item_id)


def mark_covered(conn, instance_id, now):
    if available(conn):
        conn.execute("""UPDATE preaggregate_attempts SET covered_at=?
            WHERE instance_id=? AND query_id IS NOT NULL AND covered_at IS NULL""",
            (stamp(now), instance_id))
