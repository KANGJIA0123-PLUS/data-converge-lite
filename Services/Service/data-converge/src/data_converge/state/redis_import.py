"""Import the shared Redis state into the local ledger when this process becomes leader.

Redis is the source of truth across instances; the local SQLite ledger keeps the
detailed audit (events, attempts, backfill). On takeover we merge what the
previous leader published so cursors, SUCCESS deduplication, DWD readiness and
interrupted work continue without gaps.
"""

from __future__ import annotations

from datetime import datetime
import logging

from data_converge.dag.models import TaskStatus

logger = logging.getLogger(__name__)

TERMINAL = {TaskStatus.SUCCESS.value, TaskStatus.FAILED.value, TaskStatus.UNKNOWN.value}


def _later(left: datetime | None, right: datetime) -> bool:
    return left is None or right.timestamp() > left.timestamp()


def import_cursors(conn, cursors: dict[str, datetime]) -> int:
    """Advance local cursors to the shared ones; never move a cursor backwards."""
    changed = 0
    for taskname, value in cursors.items():
        row = conn.execute('SELECT next_fire_time FROM schedule_cursors WHERE taskname=?',
                           (taskname,)).fetchone()
        current = datetime.fromisoformat(row[0]) if row else None
        if _later(current, value):
            conn.execute('''INSERT INTO schedule_cursors VALUES (?, ?)
                ON CONFLICT(taskname) DO UPDATE SET next_fire_time=excluded.next_fire_time''',
                         (taskname, value.isoformat(timespec='microseconds')))
            changed += 1
    return changed


def import_instance(conn, row: dict) -> str | None:
    """Merge one shared instance; returns the action taken, or None when unchanged.

    Shared SUCCESS always wins (prevents re-execution). A shared RUNNING row
    means the previous leader was interrupted; it is requeued as PENDING so the
    idempotent cleanup+insert reruns it. Local SUCCESS is never downgraded.
    """
    status = row.get('status')
    was_running = status == TaskStatus.RUNNING.value
    local = conn.execute('SELECT status FROM task_instances WHERE instance_id=?',
                         (row['instance_id'],)).fetchone()
    if local is not None and local[0] == TaskStatus.SUCCESS.value:
        return None
    if status == TaskStatus.RUNNING.value:
        status, action = TaskStatus.PENDING.value, 'REQUEUED'
    elif status in TERMINAL or status in (TaskStatus.PENDING.value, TaskStatus.BLOCKED.value):
        action = 'IMPORTED'
    else:
        return None
    retryable = int(status == TaskStatus.FAILED.value and
                    str(row.get('retryable', '0')) == '1')
    # Redis 会把 NULL hash 值序列化为空字符串，这里还原为 None，保留立即重试语义。
    next_retry_at = (row.get('next_retry_at') or None) if retryable else None
    if local is None:
        conn.execute('''INSERT INTO task_instances (instance_id, taskname, scheduled_time,
            data_start, data_end, status, attempts, retryable, next_retry_at, query_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                     (row['instance_id'], row['taskname'], row['scheduled_time'], row['data_start'],
                      row['data_end'], status, int(row.get('attempts') or 0), retryable,
                      next_retry_at, row.get('query_id') or None))
        return action
    if local[0] == status:
        conn.execute('''UPDATE task_instances SET attempts=MAX(attempts, ?),
            query_id=COALESCE(?, query_id), retryable=?, next_retry_at=?
            WHERE instance_id=?''',
                     (int(row.get('attempts') or 0), row.get('query_id') or None,
                      retryable, next_retry_at, row['instance_id']))
        return 'REQUEUED' if was_running else None
    conn.execute('''UPDATE task_instances SET status=?, attempts=MAX(attempts, ?),
        query_id=COALESCE(?, query_id), retryable=?, next_retry_at=?
        WHERE instance_id=?''',
                 (status, int(row.get('attempts') or 0), row.get('query_id') or None,
                  retryable, next_retry_at, row['instance_id']))
    return action


def import_dwd(conn, window_key: str, types: set[str], received_at: str) -> int:
    """Insert shared readiness facts; the caller supplies normalized UTC keys."""
    added = 0
    for dwd_type in types:
        added += conn.execute('INSERT OR IGNORE INTO dwd_ready_signals VALUES (?, ?, ?)',
                              (window_key, dwd_type, received_at)).rowcount
    return added
