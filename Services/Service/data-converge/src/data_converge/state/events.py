"""Append safe execution facts inside the state writer's existing transaction."""

from datetime import datetime, timezone


EVENT_TYPES = frozenset({
    'PREPARED', 'DISPATCHED', 'SUCCEEDED', 'FAILED', 'OUTCOME_UNKNOWN',
    'RECOVERED_UNSUBMITTED', 'RECOVERY_REQUIRED', 'RESOLVED_SUCCESS', 'RETRY_AUTHORIZED',
})
_FIELDS = ('event_id', 'schema_version', 'occurred_at', 'event_type', 'instance_id',
           'taskname', 'query_id', 'attempt_number', 'status')


def available(conn) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                        "AND name='execution_events'").fetchone() is not None


def _utc_stamp(value: datetime | str) -> str:
    try:
        instant = datetime.fromisoformat(value) if isinstance(value, str) else value
        if not isinstance(instant, datetime) or instant.tzinfo is None or instant.utcoffset() is None:
            raise ValueError
        return instant.astimezone(timezone.utc).isoformat(timespec='microseconds')
    except (TypeError, ValueError):
        raise ValueError('EXECUTION_EVENT_TIME_INVALID') from None


def append_event(conn, event_type: str, instance_id: str, occurred_at: datetime | str) -> None:
    """Snapshot the current instance; never begin or commit the caller's transaction."""
    if not isinstance(event_type, str) or event_type not in EVENT_TYPES:
        raise ValueError('EXECUTION_EVENT_TYPE_INVALID')
    stamp = _utc_stamp(occurred_at)
    if not conn.in_transaction:
        raise RuntimeError('EXECUTION_EVENT_REQUIRES_TRANSACTION')
    row = conn.execute('SELECT instance_id, taskname, query_id, attempts, status '
                       'FROM task_instances WHERE instance_id=?', (instance_id,)).fetchone()
    if row is None:
        raise KeyError('EXECUTION_EVENT_INSTANCE_MISSING')
    # Individual statements keep schema creation within the caller's transaction.
    conn.execute('''CREATE TABLE IF NOT EXISTS execution_events (
        event_id INTEGER PRIMARY KEY AUTOINCREMENT, schema_version INTEGER NOT NULL,
        occurred_at TEXT NOT NULL, event_type TEXT NOT NULL, instance_id TEXT NOT NULL,
        taskname TEXT NOT NULL, query_id TEXT, attempt_number INTEGER NOT NULL, status TEXT NOT NULL
    )''')
    conn.execute('''INSERT INTO execution_events
        (schema_version, occurred_at, event_type, instance_id, taskname, query_id, attempt_number, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)''', (1, stamp, event_type, *row))


def list_events(conn, after_event: int = 0, limit: int = 20) -> list[dict]:
    """Return a bounded sequence projection without migrating or repairing old ledgers."""
    if type(after_event) is not int or after_event < 0 or type(limit) is not int or not 1 <= limit <= 100:
        raise ValueError('EXECUTION_EVENT_PAGE_INVALID')
    # SQLite sequence values cannot exceed its signed 64-bit INTEGER range.
    if after_event > 2**63 - 1 or not available(conn):
        return []
    rows = conn.execute('''SELECT event_id, schema_version, occurred_at, event_type,
        instance_id, taskname, query_id, attempt_number, status FROM execution_events
        WHERE event_id>? ORDER BY event_id LIMIT ?''', (after_event, limit)).fetchall()
    return [dict(zip(_FIELDS, row)) for row in rows]
