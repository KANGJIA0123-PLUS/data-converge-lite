"""Optional configuration snapshots and atomic migration audit persistence."""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path

from . import retirement


SNAPSHOT_KEY = "configuration_snapshot"


def _timestamp(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Persistent timestamps must include a timezone")
    return value.isoformat(timespec="microseconds")


def snapshot(conn) -> dict | None:
    row = conn.execute("SELECT value FROM metadata WHERE key=?", (SNAPSHOT_KEY,)).fetchone()
    return json.loads(row[0]) if row else None


def effective_starts(conn) -> dict[str, datetime]:
    current = snapshot(conn)
    if current is None:
        return {}
    starts = {name: datetime.fromisoformat(value) for name, value in current["task_starts"].items()}
    for value in starts.values():
        _timestamp(value)
    return starts


def _history_available(conn) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                        "AND name='configuration_history'").fetchone() is not None


def _history_record(row) -> dict:
    value = dict(row)
    value["manifest"] = json.loads(value["manifest"])
    value["task_starts"] = json.loads(value["task_starts"])
    return value


def history(conn, limit: int = 20) -> list[dict]:
    if type(limit) is not int or not 1 <= limit <= 100:
        raise ValueError("limit must be between 1 and 100")
    if not _history_available(conn):
        return []
    rows = conn.execute("SELECT * FROM configuration_history ORDER BY rowid DESC LIMIT ?", (limit,))
    return [_history_record(row) for row in rows]


def migration(conn, migration_id: str) -> dict | None:
    if not _history_available(conn):
        return None
    row = conn.execute("SELECT * FROM configuration_history WHERE migration_id=?", (migration_id,)).fetchone()
    return _history_record(row) if row else None


def constraints(conn, tasknames) -> dict:
    tasks = {name: {"unresolved": 0, "latest_end": None} for name in tasknames}
    running = 0
    active = retirement.active_filter(conn)
    for row in conn.execute(f"SELECT taskname, status, data_end FROM task_instances WHERE {active}"):
        running += int(row["status"] == "RUNNING")
        constraint = tasks.get(row["taskname"])
        if constraint is None:
            continue
        constraint["unresolved"] += int(row["status"] != "SUCCESS")
        end = datetime.fromisoformat(row["data_end"])
        _timestamp(end)
        if constraint["latest_end"] is None or end > constraint["latest_end"]:
            constraint["latest_end"] = end
    return {"running": running, "tasks": tasks}


def _assert_idle(conn) -> None:
    if conn.in_transaction:
        raise RuntimeError("CONFIGURATION_TRANSACTION_ACTIVE")


def _verify_backup(conn) -> None:
    if [row[0] for row in conn.execute("PRAGMA quick_check")] != ["ok"]:
        raise sqlite3.DatabaseError("CONFIGURATION_BACKUP_INVALID")


def _copy_backup(source, destination: Path) -> None:
    target = sqlite3.connect(destination.as_uri() + "?mode=rw", uri=True)
    try:
        source.backup(target)
        _verify_backup(target)
    finally:
        target.close()


def _remove_owned_backup(destination: Path, identity) -> None:
    try:
        current = destination.lstat()
        if (current.st_dev, current.st_ino) == (identity.st_dev, identity.st_ino):
            destination.unlink()
    except FileNotFoundError:
        pass


def _sync_directory(directory: Path) -> None:
    descriptor = os.open(directory, os.O_RDONLY, 0o700)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def backup(conn, destination: Path) -> Path:
    _assert_idle(conn)
    destination = Path(destination).absolute()
    descriptor = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    identity = os.fstat(descriptor)
    try:
        os.fchmod(descriptor, 0o600)
        _copy_backup(conn, destination)
        os.fsync(descriptor)
        _sync_directory(destination.parent)
    except BaseException:
        _remove_owned_backup(destination, identity)
        raise
    finally:
        os.close(descriptor)
    return destination


def _assert_expected(conn, binding, revision) -> None:
    row = conn.execute("SELECT value FROM metadata WHERE key='binding'").fetchone()
    current = snapshot(conn)
    actual_revision = current["revision"] if current else None
    if (row[0] if row else None) != binding or actual_revision != revision:
        raise RuntimeError("CONFIGURATION_STATE_CHANGED")


def _create_history(conn) -> None:
    conn.execute("""CREATE TABLE IF NOT EXISTS configuration_history (
        migration_id TEXT PRIMARY KEY, previous_revision TEXT, revision TEXT NOT NULL,
        applied_at TEXT NOT NULL, backup_path TEXT, manifest TEXT NOT NULL,
        task_starts TEXT NOT NULL
    )""")


def _json(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _write_metadata(conn, key, value) -> None:
    conn.execute("""INSERT INTO metadata VALUES (?, ?) ON CONFLICT(key)
        DO UPDATE SET value=excluded.value""", (key, value))


def apply(conn, *, expected_binding, expected_revision, manifest, revision,
          task_starts, binding, migration_id, applied_at, backup_path=None,
          reset_task_cursors=(), retire_existing=False, recovery=None) -> None:
    _assert_idle(conn)
    cursor_names = tuple(reset_task_cursors)
    starts = {name: _timestamp(value) for name, value in task_starts.items()}
    current = {"manifest": manifest, "revision": revision, "task_starts": starts}
    record = (migration_id, expected_revision, revision, _timestamp(applied_at),
              str(backup_path) if backup_path is not None else None, _json(manifest), _json(starts))
    with conn:
        conn.execute("BEGIN IMMEDIATE")
        _assert_expected(conn, expected_binding, expected_revision)
        retirement.validate(conn)
        _configure_retirement(conn, current, retire_existing, recovery, expected_revision, record, expected_binding, binding)
        _create_history(conn)
        _write_metadata(conn, "configuration_required", "1")
        _write_metadata(conn, SNAPSHOT_KEY, _json(current))
        _write_metadata(conn, "binding", binding)
        conn.executemany("DELETE FROM schedule_cursors WHERE taskname=?",
                         ((name,) for name in cursor_names))
        conn.execute("""INSERT INTO configuration_history
            (migration_id, previous_revision, revision, applied_at, backup_path, manifest, task_starts)
            VALUES (?, ?, ?, ?, ?, ?, ?)""", record)
        retirement.validate(conn)


def _configure_retirement(conn, current, retiring, recovery, expected_revision, record, old_binding, binding):
    previous = snapshot(conn)
    if retiring:
        required = conn.execute("SELECT 1 FROM metadata WHERE key='configuration_required'").fetchone()
        if expected_revision is not None or previous is not None or required is not None:
            raise ValueError("LEGACY_RETIREMENT_INVALID")
        expected = {"migration_id": record[0], "previous_binding": old_binding,
                    "current_binding": binding, "revision": record[2], "backup_path": record[4]}
        if not isinstance(recovery, dict) or retirement.instant(recovery.get("recovered_at")) != retirement.instant(record[3]):
            raise ValueError("LEGACY_RETIREMENT_INVALID")
        current["retirement_id"] = retirement.create(conn, recovery, expected=expected, starts=current["task_starts"])
    elif recovery is not None:
        raise ValueError("LEGACY_RETIREMENT_INVALID")
    elif previous is not None and "retirement_id" in previous:
        cutover = retirement.instant(retirement.recovery(conn)["cutover"])
        if any(retirement.instant(start) < cutover for start in current["task_starts"].values()):
            raise ValueError("LEGACY_RETIREMENT_INVALID")
        current["retirement_id"] = previous["retirement_id"]


def validate_windows(instances, starts) -> None:
    for instance in instances:
        start = starts.get(instance.taskname)
        if start is not None and instance.data_start < start:
            raise ValueError("TASK_WINDOW_BEFORE_EFFECTIVE_START")
