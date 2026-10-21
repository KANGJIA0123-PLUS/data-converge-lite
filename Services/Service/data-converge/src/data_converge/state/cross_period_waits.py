"""Durable normal-schedule wait anchors and bounded release evidence."""

from datetime import datetime
import json


_FIELDS = ('missing_count', 'missing_status_counts', 'missing_samples',
           'missing_truncated', 'decision_reason')
_EMPTY = dict(missing_count=0, missing_status_counts={}, missing_samples=[],
              missing_truncated=False, decision_reason=None)


def available(conn) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                        "AND name='cross_period_waits'").fetchone() is not None


def register(conn, records) -> None:
    """Insert new normal-schedule anchors inside the caller's enqueue transaction."""
    records = list(records)
    if not records:
        return
    conn.execute('''CREATE TABLE IF NOT EXISTS cross_period_waits (
        instance_id TEXT PRIMARY KEY, scheduled_due_at TEXT NOT NULL,
        wait_seconds INTEGER NOT NULL, deadline_at TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'WAITING', checked_at TEXT,
        evidence TEXT, query_id TEXT
    )''')
    for record in records:
        conn.execute('''INSERT INTO cross_period_waits
            (instance_id, scheduled_due_at, wait_seconds, deadline_at)
            VALUES (?, ?, ?, ?) ON CONFLICT(instance_id) DO NOTHING''', (record['instance_id'],
                record['scheduled_due_at'].isoformat(), record['wait_seconds'],
                record['deadline_at'].isoformat()))


def get(conn, instance_id: str) -> dict | None:
    """Read old ledgers without implicitly creating or upgrading a table."""
    if not available(conn):
        return None
    row = conn.execute('SELECT * FROM cross_period_waits WHERE instance_id=?',
                       (instance_id,)).fetchone()
    if row is None:
        return None
    result = dict(row)
    evidence = result.pop('evidence')
    result.update(json.loads(evidence) if evidence else _EMPTY)
    return result


def decide(conn, instance_id: str, *, status: str, missing: dict, now: datetime) -> bool:
    """Keep original release evidence after dispatch or timeout; return transitions."""
    previous = get(conn, instance_id)
    if previous is None:
        raise KeyError(instance_id)
    if previous['query_id'] is not None or previous['status'] == 'TIMED_OUT':
        return False
    changed = previous['checked_at'] is None or previous['status'] != status
    evidence = {key: missing[key] for key in _FIELDS}
    conn.execute('''UPDATE cross_period_waits SET status=?, checked_at=?, evidence=?
        WHERE instance_id=?''', (status, now.isoformat(),
                                json.dumps(evidence, sort_keys=True), instance_id))
    return changed


def attach_query(conn, instance_id: str, query_id: str) -> None:
    if available(conn):
        conn.execute('''UPDATE cross_period_waits SET query_id=COALESCE(query_id, ?)
            WHERE instance_id=?''', (query_id, instance_id))
