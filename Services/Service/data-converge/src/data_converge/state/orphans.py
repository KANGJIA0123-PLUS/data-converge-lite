"""Queries left by a previous leader that are not yet confirmed stopped in ClickHouse.

Takeover reruns an interrupted window with an idempotent DELETE + INSERT. If the
old INSERT is still running, it writes after the DELETE and duplicates the window
(ClickHouse keeps running ``wait_end_of_query`` INSERTs after the client dies).
Each interrupted window is therefore held here until ClickHouse confirms that its
previous query_id is no longer running; only then may it be dispatched again.
"""

from __future__ import annotations

from datetime import datetime, timezone


def _ensure(conn) -> None:
    conn.execute("""CREATE TABLE IF NOT EXISTS orphan_queries (
        instance_id TEXT PRIMARY KEY, query_id TEXT NOT NULL, detected_at TEXT NOT NULL
    )""")


def available(conn) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                        "AND name='orphan_queries'").fetchone() is not None


def hold(conn, pairs) -> int:
    """Record (instance_id, query_id) pairs; the newest unconfirmed query wins."""
    pairs = [(instance_id, query_id) for instance_id, query_id in pairs if instance_id and query_id]
    if not pairs:
        return 0
    _ensure(conn)
    stamp = datetime.now(timezone.utc).isoformat(timespec="microseconds")
    conn.executemany("""INSERT INTO orphan_queries VALUES (?, ?, ?)
        ON CONFLICT(instance_id) DO UPDATE SET query_id=excluded.query_id,
        detected_at=excluded.detected_at""", [(i, q, stamp) for i, q in pairs])
    return len(pairs)


def held(conn) -> dict[str, str]:
    if not available(conn):
        return {}
    return {row[0]: row[1] for row in conn.execute("SELECT instance_id, query_id FROM orphan_queries")}


def release(conn, instance_ids) -> int:
    if not available(conn):
        return 0
    return conn.executemany("DELETE FROM orphan_queries WHERE instance_id=?",
                            [(value,) for value in instance_ids]).rowcount
