"""Attempt history shares the state writer's transactions and read-only connection."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from data_converge.errors import safe_diagnostic_details

from . import retirement


def available(conn) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                        "AND name='execution_attempts'").fetchone() is not None


def create_schema(conn) -> None:
    """Use individual statements: executescript would commit the caller's transaction."""
    conn.execute("""CREATE TABLE IF NOT EXISTS execution_attempts (
        query_id TEXT PRIMARY KEY, instance_id TEXT NOT NULL, taskname TEXT NOT NULL,
        scheduled_time TEXT NOT NULL, data_start TEXT NOT NULL, data_end TEXT NOT NULL,
        attempt_number INTEGER NOT NULL, status TEXT NOT NULL, started_at TEXT NOT NULL,
        finished_at TEXT, recovered_at TEXT, error TEXT, retryable INTEGER NOT NULL DEFAULT 0,
        next_retry_at TEXT, diagnostic TEXT NOT NULL DEFAULT '{}'
    )""")
    conn.execute("""CREATE INDEX IF NOT EXISTS execution_attempts_instance
        ON execution_attempts(instance_id, started_at DESC)""")
    conn.execute("""CREATE INDEX IF NOT EXISTS execution_attempts_task
        ON execution_attempts(taskname, started_at DESC)""")
    conn.execute("""CREATE INDEX IF NOT EXISTS execution_attempts_recent
        ON execution_attempts(started_at DESC)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS execution_resolutions (
        resolution_id INTEGER PRIMARY KEY, query_id TEXT UNIQUE, instance_id TEXT NOT NULL,
        resolution TEXT NOT NULL, resolved_at TEXT NOT NULL, note TEXT NOT NULL
    )""")


def record_claim(conn, instance_id: str) -> None:
    create_schema(conn)
    row = conn.execute("SELECT started_at FROM task_instances WHERE instance_id=?", (instance_id,)).fetchone()
    started = datetime.fromisoformat(row['started_at']).astimezone(timezone.utc)
    conn.execute("""INSERT INTO execution_attempts
        (query_id, instance_id, taskname, scheduled_time, data_start, data_end,
         attempt_number, status, started_at)
        SELECT query_id, instance_id, taskname, scheduled_time, data_start, data_end,
               attempts, status, ? FROM task_instances WHERE instance_id=?""",
        (started.isoformat(timespec='microseconds'), instance_id))


def public_error(value):
    if not value:
        return None
    code = str(value).split(':', 1)[0]
    return safe_diagnostic_details({'error_code': code}).get('error_code', 'ERROR_DETAIL_NOT_RECORDED')


def record_finish(conn, instance_id: str, diagnostic: dict | None) -> None:
    if not available(conn):
        return  # An old running attempt was never recorded; do not invent its history.
    row = conn.execute("SELECT * FROM task_instances WHERE instance_id=?", (instance_id,)).fetchone()
    details = safe_diagnostic_details(diagnostic) if row['status'] != 'SUCCESS' else {}
    if details:
        details['query_id'] = row['query_id']
    conn.execute("""UPDATE execution_attempts SET status=?, finished_at=?, error=?,
        retryable=?, next_retry_at=?, diagnostic=? WHERE query_id=? AND status='RUNNING'""",
        (row['status'], row['finished_at'], public_error(row['error']), row['retryable'],
         row['next_retry_at'], json.dumps(details, sort_keys=True), row['query_id']))


def record_recovery(conn, recovered_at: str) -> None:
    if not available(conn):
        return
    active = retirement.active_filter(conn)
    conn.execute(f"""UPDATE execution_attempts SET status='UNKNOWN', retryable=0,
        next_retry_at=NULL, error='EXECUTION_INTERRUPTED', recovered_at=?
        WHERE status='RUNNING' AND query_id IN
            (SELECT query_id FROM task_instances WHERE status='RUNNING' AND {active})""", (recovered_at,))


def record_resolution(conn, instance_id: str) -> None:
    create_schema(conn)
    conn.execute("""INSERT INTO execution_resolutions
        (query_id, instance_id, resolution, resolved_at, note)
        SELECT query_id, instance_id, status, resolved_at, resolution_note
        FROM task_instances WHERE instance_id=?""", (instance_id,))


def list_attempts(conn, instance_id=None, *, taskname=None, limit=20, offset=0):
    if type(limit) is not int or not 1 <= limit <= 100 or type(offset) is not int or offset < 0:
        raise ValueError('limit must be between 1 and 100; offset must be non-negative')
    if not available(conn):
        return []
    clauses, parameters = [], []
    for field, value in (('instance_id', instance_id), ('taskname', taskname)):
        if value is not None:
            clauses.append(f'a.{field}=?')
            parameters.append(value)
    where = ' WHERE ' + ' AND '.join(clauses) if clauses else ''
    rows = conn.execute("""SELECT a.*, r.resolution, r.resolved_at,
        CASE WHEN r.note IS NOT NULL AND r.note!='' THEN 1 ELSE 0 END AS note_recorded
        FROM execution_attempts a LEFT JOIN execution_resolutions r ON r.query_id=a.query_id"""
        + where + ' ORDER BY a.started_at DESC, a.rowid DESC LIMIT ? OFFSET ?',
        [*parameters, limit, offset]).fetchall()
    return [_public_attempt(row) for row in rows]


def _public_attempt(row):
    value = dict(row)
    value['error'] = public_error(value['error'])
    value['diagnostic'] = safe_diagnostic_details(json.loads(value['diagnostic']))
    resolution = value.pop('resolution')
    resolved_at, note_recorded = value.pop('resolved_at'), value.pop('note_recorded')
    value['resolution'] = None if resolution is None else dict(
        resolution=resolution, resolved_at=resolved_at, note_recorded=bool(note_recorded))
    return value


def list_legacy_resolutions(conn, instance_id=None, *, taskname=None, limit=20, offset=0):
    """Expose new operator actions on legacy instances without fabricating attempts."""
    if type(limit) is not int or not 1 <= limit <= 100 or type(offset) is not int or offset < 0:
        raise ValueError('limit must be between 1 and 100; offset must be non-negative')
    if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                        "AND name='execution_resolutions'").fetchone():
        return []
    clauses, parameters = [], []
    if available(conn):
        clauses.append('NOT EXISTS (SELECT 1 FROM execution_attempts a WHERE a.query_id=r.query_id)')
    for field, value in (('r.instance_id', instance_id), ('t.taskname', taskname)):
        if value is not None:
            clauses.append(f'{field}=?')
            parameters.append(value)
    where = ' WHERE ' + ' AND '.join(clauses) if clauses else ''
    rows = conn.execute("""SELECT r.query_id, r.instance_id, t.taskname, r.resolution, r.resolved_at,
        CASE WHEN r.note!='' THEN 1 ELSE 0 END AS note_recorded
        FROM execution_resolutions r LEFT JOIN task_instances t ON t.instance_id=r.instance_id"""
        + where + ' ORDER BY r.resolved_at DESC, r.resolution_id DESC LIMIT ? OFFSET ?',
        [*parameters, limit, offset]).fetchall()
    return [dict(row) | {'note_recorded': bool(row['note_recorded'])} for row in rows]
