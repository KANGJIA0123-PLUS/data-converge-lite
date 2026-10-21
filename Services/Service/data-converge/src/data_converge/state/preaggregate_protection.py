"""Keep dispatched early writes reachable by an unchanged formal task."""
from datetime import datetime
import json

from data_converge.dag.builder import timeline_time
from . import preaggregate, retirement


def pending(conn):
    if not preaggregate.available(conn):
        return []
    return conn.execute("""SELECT * FROM preaggregate_attempts WHERE query_id IS NOT NULL
        AND covered_at IS NULL ORDER BY rowid""").fetchall()


def _instant(value):
    value = datetime.fromisoformat(value) if isinstance(value, str) else value
    preaggregate.stamp(value)
    return timeline_time(value)


def _reject(row, reason):
    raise ValueError("PREAGGREGATE_COVERAGE_" + reason + ":" + row["item_id"])


def _validate_identity(row, manifest, timezone, cleanup_columns):
    evidence = json.loads(row["config_evidence"])
    taskname = row["taskname"]
    task = manifest.get("tasks", {}).get(taskname)
    if task is None or evidence.get("task") != task:
        _reject(row, "TASK_CHANGED")
    for field in ("connection", "dataset_id"):
        if evidence.get(field) != manifest.get(field):
            _reject(row, "DATASET_CHANGED")
    if timezone is not None and evidence.get("timezone") != timezone:
        _reject(row, "TIMEZONE_CHANGED")
    if cleanup_columns is not None and evidence.get("cleanup_column") != cleanup_columns.get(taskname):
        _reject(row, "CLEANUP_CHANGED")


def _validate_window(conn, row, task_starts, coverage):
    start = _instant(row["data_start"])
    if coverage is None or _instant(coverage) > start:
        _reject(row, "START_CHANGED")
    effective = task_starts.get(row["taskname"], coverage)
    if _instant(effective) > start:
        _reject(row, "EFFECTIVE_CHANGED")
    if retirement.get(conn, row["instance_id"]) is not None:
        _reject(row, "RETIRED")


def validate(conn, manifest, task_starts, coverage, *, timezone=None, cleanup_columns=None):
    records = pending(conn)
    for row in records:
        _validate_identity(row, manifest, timezone, cleanup_columns)
        _validate_window(conn, row, task_starts, coverage)
    validate_cursors(conn, records)


def validate_cursors(conn, records=None):
    for row in pending(conn) if records is None else records:
        formal = conn.execute("SELECT 1 FROM task_instances WHERE instance_id=?",
                              (row["instance_id"],)).fetchone()
        if formal is not None:
            continue
        cursor = conn.execute("SELECT next_fire_time FROM schedule_cursors WHERE taskname=?",
                              (row["taskname"],)).fetchone()
        if cursor is not None and _instant(cursor[0]) > _instant(row["formal_scheduled_time"]):
            _reject(row, "CURSOR_PASSED")


def formal_blocker(conn, instance_id, now):
    reason = preaggregate.blocker(conn, instance_id)
    if reason or not preaggregate.available(conn):
        return reason
    rows = conn.execute("""SELECT data_end FROM preaggregate_attempts
        WHERE instance_id=? AND query_id IS NOT NULL AND covered_at IS NULL""",
        (instance_id,)).fetchall()
    if any(_instant(now) < _instant(row[0]) for row in rows):
        return "PREAGGREGATE_FORMAL_WINDOW_NOT_ENDED"
    return None


def assert_formal_allowed(conn, instance_id, now):
    reason = formal_blocker(conn, instance_id, now)
    if reason:
        raise ValueError(reason)


def schedule_blocker(conn, scheduled_until, now):
    current, requested = _instant(now), _instant(scheduled_until)
    if requested <= current:
        return None
    if any(current < _instant(row["data_end"]) <= requested for row in pending(conn)):
        return "PREAGGREGATE_FORMAL_WINDOW_NOT_ENDED"
    return None
