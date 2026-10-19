"""Preserve historical execution facts while isolating them from current work."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from datetime import datetime, timedelta


_FIELDS = {"schema_version", "migration_id", "cutover", "previous_binding", "current_binding",
           "revision", "backup_path", "recovered_at", "identity_verified",
           "previous_definition_available", "historical_counts"}
_RECORDED_FIELDS = _FIELDS | {"retired_count", "retired_id_digest"}
_STATUSES = {"PENDING", "RUNNING", "SUCCESS", "FAILED", "BLOCKED", "UNKNOWN"}


def _require(condition):
    if not condition:
        raise ValueError("LEGACY_RETIREMENT_INVALID")


def instant(value):
    try:
        result = datetime.fromisoformat(value) if isinstance(value, str) else value
        _require(isinstance(result, datetime) and result.tzinfo is not None and result.utcoffset() is not None)
        return result
    except (TypeError, ValueError):
        raise ValueError("LEGACY_RETIREMENT_INVALID") from None


def _table(conn, name):
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone() is not None


def _metadata(conn):
    return dict(conn.execute("SELECT key, value FROM metadata WHERE key IN "
        "('retirement_required','legacy_recovery','configuration_snapshot','configuration_required')"))


def _decoded(value):
    try:
        result = json.loads(value)
        _require(isinstance(result, dict))
        return result
    except (TypeError, ValueError):
        raise ValueError("LEGACY_RETIREMENT_INVALID") from None


def _check_record(record, *, persisted):
    _require(isinstance(record, dict) and record.keys() == (_RECORDED_FIELDS if persisted else _FIELDS))
    _require(type(record["schema_version"]) is int and record["schema_version"] == 1)
    _require(record["identity_verified"] is False and record["previous_definition_available"] is False)
    for key in ("previous_binding", "current_binding", "revision"):
        _require(isinstance(record[key], str) and re.fullmatch(r"[a-f0-9]{64}", record[key]) is not None)
    for key in ("migration_id", "backup_path"):
        _require(isinstance(record[key], str) and bool(record[key]) and not any(ord(c) < 32 for c in record[key]))
    cutoff = instant(record["cutover"])
    _require(not (cutoff.minute % 5 or cutoff.second or cutoff.microsecond))
    _require(instant(record["recovered_at"]) <= cutoff)
    counts = record["historical_counts"]
    _require(isinstance(counts, dict) and all(k in _STATUSES and type(v) is int and v >= 0
                                            for k, v in counts.items()))
    if persisted:
        _require(type(record["retired_count"]) is int and record["retired_count"] >= 0)
        _require(isinstance(record["retired_id_digest"], str) and
                 re.fullmatch(r"[a-f0-9]{64}", record["retired_id_digest"]) is not None)


def _mode(conn):
    values = _metadata(conn)
    required, audit = values.get("retirement_required"), values.get("legacy_recovery")
    exists = _table(conn, "retired_instances")
    try:
        current = _decoded(values["configuration_snapshot"]) if "configuration_snapshot" in values else {}
    except ValueError:
        if required is None and audit is None and not exists:
            return None  # The configuration validator owns unrelated snapshot corruption.
        raise
    if required is None and audit is None and not exists and "retirement_id" not in current:
        return None
    _require(required is not None and audit is not None and exists)
    record = _decoded(audit)
    _check_record(record, persisted=True)
    _require(required == record["migration_id"] == current.get("retirement_id"))
    _require(values.get("configuration_required") == "1")
    count = conn.execute("SELECT COUNT(*) FROM retired_instances").fetchone()[0]
    _require(count == record["retired_count"])
    return record


def recovery(conn):
    return _mode(conn)


def get(conn, instance_id):
    if _mode(conn) is None:
        return None
    row = conn.execute("SELECT migration_id, retired_at FROM retired_instances WHERE instance_id=?",
                       (instance_id,)).fetchone()
    return dict(row) if row else None


def assert_active(conn, instance_id):
    if get(conn, instance_id) is not None:
        raise ValueError("HISTORICAL_INSTANCE_RETIRED")


def active_filter(conn, column="task_instances.instance_id"):
    if _mode(conn) is None:
        return "1"
    return f"NOT EXISTS (SELECT 1 FROM retired_instances retired WHERE retired.instance_id={column})"


def status_counts(conn, *, retired=False):
    active = active_filter(conn)
    if active == "1" and retired:
        return {}
    condition = f"NOT ({active})" if retired else active
    return dict(conn.execute(f"SELECT status, COUNT(*) FROM task_instances WHERE {condition} GROUP BY status"))


def summary(conn):
    counts, latest = {}, None
    for row in conn.execute("SELECT status, data_end FROM task_instances"):
        _require(row["status"] in _STATUSES)
        end = instant(row["data_end"])
        counts[row["status"]] = counts.get(row["status"], 0) + 1
        latest = max(latest, end) if latest is not None else end
    signal_end = None
    if _table(conn, "dwd_ready_signals"):
        for row in conn.execute("SELECT window_start FROM dwd_ready_signals"):
            end = instant(row[0]) + timedelta(minutes=5)
            signal_end = max(signal_end, end) if signal_end is not None else end
    return {"counts": counts, "latest_end": latest, "latest_signal_end": signal_end}


def _id_digest(rows):
    digest, count = hashlib.sha256(), 0
    for row in rows:
        encoded = row[0].encode("utf-8")
        digest.update(str(len(encoded)).encode("ascii") + b":" + encoded + b"\0")
        count += 1
    return count, digest.hexdigest()


def _validate_history(conn, record):
    _require(_table(conn, "configuration_history"))
    row = conn.execute("SELECT * FROM configuration_history WHERE migration_id=?",
                       (record["migration_id"],)).fetchone()
    _require(row is not None and row["previous_revision"] is None and row["revision"] == record["revision"])
    _require(row["backup_path"] == record["backup_path"])
    _require(instant(row["applied_at"]) == instant(record["recovered_at"]))
    starts = _decoded(row["task_starts"])
    _require(bool(starts) and all(instant(value) == instant(record["cutover"]) for value in starts.values()))


def _validate_mapping(conn, record):
    rows = conn.execute("SELECT instance_id FROM retired_instances ORDER BY instance_id")
    count, digest = _id_digest(rows)
    _require(count == record["retired_count"] and digest == record["retired_id_digest"])
    rows = conn.execute("""SELECT r.migration_id, r.retired_at, t.instance_id, t.status
        FROM retired_instances r LEFT JOIN task_instances t ON t.instance_id=r.instance_id""")
    counts = {}
    for row in rows:
        _require(row["instance_id"] is not None and row["migration_id"] == record["migration_id"])
        _require(instant(row["retired_at"]) == instant(record["recovered_at"]))
        counts[row["status"]] = counts.get(row["status"], 0) + 1
    _require(counts == record["historical_counts"])


def validate(conn):
    try:
        record = _mode(conn)
        if record is not None:
            _validate_history(conn, record)
            _validate_mapping(conn, record)
            starts = _decoded(_metadata(conn)["configuration_snapshot"])["task_starts"]
            _require(isinstance(starts, dict))
            cutover = instant(record["cutover"])
            _require(all(instant(value) >= cutover for value in starts.values()))
    except (sqlite3.DatabaseError, TypeError, KeyError, ValueError):
        raise ValueError("LEGACY_RETIREMENT_INVALID") from None


def _request(conn, record, expected, starts):
    _check_record(record, persisted=False)
    _require(all(record[key] == value for key, value in expected.items()))
    _require(bool(starts) and all(instant(value) == instant(record["cutover"]) for value in starts.values()))
    facts = summary(conn)
    _require(facts["counts"] == record["historical_counts"])
    limits = [value for value in (facts["latest_end"], facts["latest_signal_end"]) if value is not None]
    row = conn.execute("SELECT value FROM metadata WHERE key='coverage_start'").fetchone()
    _require(row is not None)
    limits.append(instant(row[0]))
    _require(all(instant(record["cutover"]) >= value for value in limits))


def create(conn, record, *, expected, starts):
    _require(_mode(conn) is None)
    _request(conn, record, expected, starts)
    count, digest = _id_digest(conn.execute("SELECT instance_id FROM task_instances ORDER BY instance_id"))
    stored = dict(record, retired_count=count, retired_id_digest=digest)
    conn.execute("""CREATE TABLE retired_instances (
        instance_id TEXT PRIMARY KEY NOT NULL, migration_id TEXT NOT NULL, retired_at TEXT NOT NULL)""")
    conn.execute("""INSERT INTO retired_instances SELECT instance_id, ?, ? FROM task_instances""",
                 (record["migration_id"], record["recovered_at"]))
    conn.executemany("INSERT INTO metadata VALUES (?, ?)", (
        ("retirement_required", record["migration_id"]),
        ("legacy_recovery", json.dumps(stored, sort_keys=True, separators=(",", ":")))))
    conn.execute("DELETE FROM schedule_cursors")
    return record["migration_id"]
