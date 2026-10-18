"""Persist submission boundaries and classify recovery from current evidence."""

from __future__ import annotations


def available(conn) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                        "AND name='dispatch_checkpoints'").fetchone() is not None


def record_prepared(conn, instance_id, query_id, prepared_at, max_attempts):
    """Called after reservation DML, inside the state writer's transaction."""
    conn.execute("""CREATE TABLE IF NOT EXISTS dispatch_checkpoints (
        query_id TEXT PRIMARY KEY, instance_id TEXT NOT NULL,
        phase TEXT NOT NULL CHECK (phase IN ('PREPARED', 'DISPATCHED', 'NOT_SUBMITTED')),
        prepared_at TEXT NOT NULL, dispatched_at TEXT, recovered_at TEXT,
        max_attempts INTEGER NOT NULL CHECK (max_attempts > 0)
    )""")
    conn.execute("""INSERT INTO dispatch_checkpoints
        (query_id, instance_id, phase, prepared_at, max_attempts)
        VALUES (?, ?, 'PREPARED', ?, ?)""",
        (query_id, instance_id, prepared_at, max_attempts))


def mark_dispatched(conn, instance_id, query_id, dispatched_at):
    if not available(conn):
        raise RuntimeError(f"No prepared reservation: {instance_id}")
    result = conn.execute("""UPDATE dispatch_checkpoints
        SET phase='DISPATCHED', dispatched_at=?
        WHERE instance_id=? AND query_id=? AND phase='PREPARED'
        AND EXISTS (SELECT 1 FROM task_instances t
            WHERE t.instance_id=dispatch_checkpoints.instance_id
            AND t.query_id=dispatch_checkpoints.query_id AND t.status='RUNNING'
            AND t.attempts < dispatch_checkpoints.max_attempts)""",
        (dispatched_at, instance_id, query_id))
    if result.rowcount != 1:
        raise RuntimeError(f"No current prepared reservation: {instance_id}")


def assert_finish_allowed(conn, instance_id):
    if not available(conn):
        return
    row = conn.execute("""SELECT 1 FROM task_instances t JOIN dispatch_checkpoints c
        ON c.instance_id=t.instance_id AND c.query_id=t.query_id
        WHERE t.instance_id=? AND t.status='RUNNING' AND c.phase='PREPARED'""",
        (instance_id,)).fetchone()
    if row is not None:
        raise RuntimeError(f"Cannot finish an undispatched reservation: {instance_id}")


def _recovery_source(conn, include_unknown=True):
    """Keep enumeration and SQL totals on the same current-reservation proof."""
    if available(conn):
        source = """FROM task_instances t LEFT JOIN dispatch_checkpoints c
            ON c.instance_id=t.instance_id AND c.query_id=t.query_id"""
        phase = "c.phase"
    else:
        source = "FROM task_instances t"
        phase = "NULL"
    statuses = "t.status IN ('RUNNING', 'UNKNOWN')" if include_unknown else "t.status='RUNNING'"
    automatic = f"t.status='RUNNING' AND {phase}='PREPARED'"
    return f"{source} WHERE {statuses}", phase, automatic


def _recovery_query(conn, include_unknown=True):
    source, phase, automatic = _recovery_source(conn, include_unknown)
    fields = "t.instance_id, t.taskname, t.status, t.query_id, t.attempts"
    return (f"SELECT {fields}, {phase} AS phase, "
            f"CASE WHEN {automatic} THEN 1 ELSE 0 END AS auto_resume {source}"
            " ORDER BY t.data_end, t.instance_id")


def classify(row):
    """Only a matching PREPARED current reservation proves no dispatch."""
    value = dict(row)
    if value.pop("auto_resume"):
        action, reason = "AUTO_RESUME", "PREPARED_NOT_DISPATCHED"
    elif value["status"] == "UNKNOWN":
        action, reason = "RECONCILE", "OUTCOME_UNCONFIRMED"
    elif value["phase"] == "DISPATCHED":
        action, reason = "RECONCILE", "DISPATCH_COMMITTED"
    else:
        action, reason = "RECONCILE", "NO_PRE_SUBMISSION_PROOF"
    return value | {"action": action, "reason": reason}


def classified_rows(conn, *, include_unknown=True):
    """Share the read-only classification between recovery and diagnostics."""
    rows = conn.execute(_recovery_query(conn, include_unknown)).fetchall()
    return [classify(row) for row in rows]


def recovery_rows(conn, limit=20, offset=0):
    if type(limit) is not int or not 1 <= limit <= 100 or type(offset) is not int or offset < 0:
        raise ValueError("limit must be between 1 and 100; offset must be non-negative")
    if offset > 2**63 - 1:
        return []
    rows = conn.execute(_recovery_query(conn) + " LIMIT ? OFFSET ?", (limit, offset)).fetchall()
    return [classify(row) for row in rows]


def recovery_counts(conn):
    source, _, automatic = _recovery_source(conn)
    total, resumable = conn.execute(
        f"SELECT COUNT(*), COALESCE(SUM(CASE WHEN {automatic} THEN 1 ELSE 0 END), 0) "
        f"{source}").fetchone()
    return {"AUTO_RESUME": resumable, "RECONCILE": total - resumable}


def recover_unsubmitted(conn, row, recovered_at):
    """Requeue the current reservation only; retain consumed attempt budget."""
    result = conn.execute("""UPDATE task_instances SET status='PENDING',
        started_at=NULL, finished_at=NULL, retryable=0, next_retry_at=NULL, error=NULL
        WHERE instance_id=? AND query_id=? AND status='RUNNING'
        AND EXISTS (SELECT 1 FROM dispatch_checkpoints c
            WHERE c.instance_id=task_instances.instance_id
            AND c.query_id=task_instances.query_id AND c.phase='PREPARED')""",
        (row["instance_id"], row["query_id"]))
    if result.rowcount != 1:
        raise RuntimeError(f"Prepared reservation changed: {row['instance_id']}")
    conn.execute("""UPDATE dispatch_checkpoints SET phase='NOT_SUBMITTED', recovered_at=?
        WHERE instance_id=? AND query_id=? AND phase='PREPARED'""",
        (recovered_at, row["instance_id"], row["query_id"]))
