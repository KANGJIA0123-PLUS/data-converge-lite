"""Durable attempts retain their outcomes independently of current task state."""

import json
import sqlite3
from contextlib import closing
from datetime import timedelta, timezone

import pytest

from data_converge.dag import TaskStatus
from data_converge.state import StateStore
from .test_state import NOW, graph, open_store


def enqueue(store, *, minutes=0):
    builder, dag = graph()
    instance = builder.create_instance(dag, "root", NOW + timedelta(minutes=minutes))
    store.enqueue_and_advance([instance], {})
    return instance.instance_id


def test_each_retry_survives_reopen_without_reusing_current_evidence(tmp_path):
    path = tmp_path / "state.db"
    with closing(open_store(path)) as store:
        instance_id = enqueue(store)
        store.claim(instance_id, now=NOW)
        first = store.get_instance(instance_id)["query_id"]
        store.finish(instance_id, TaskStatus.FAILED, error="FIRST_FAILURE:private",
                     retryable=True, diagnostic={"http_status": 503, "sql": "private"}, now=NOW)
        store.claim(instance_id, now=NOW + timedelta(seconds=1))
        second = store.get_instance(instance_id)["query_id"]
        store.finish(instance_id, TaskStatus.SUCCESS, now=NOW + timedelta(seconds=2))
    with closing(StateStore(path)) as reader:
        rows = reader.list_execution_attempts(instance_id)
        assert [row["query_id"] for row in rows] == [second, first]
        assert [row["status"] for row in rows] == ["SUCCESS", "FAILED"]
        assert [row["attempt_number"] for row in rows] == [2, 1]
        assert rows[1]["error"] == "FIRST_FAILURE"
        assert rows[1]["diagnostic"] == {"http_status": 503, "query_id": first}
        assert "private" not in json.dumps(rows)
        assert reader.get_execution_diagnostic(instance_id) is None


def test_resolution_does_not_rewrite_attempt_outcome_or_expose_note(tmp_path):
    with closing(open_store(tmp_path / "state.db")) as store:
        instance_id = enqueue(store)
        store.claim(instance_id, now=NOW)
        store.finish(instance_id, TaskStatus.UNKNOWN, now=NOW)
        store.resolve(instance_id, "PENDING", "private operator note")
        store.claim(instance_id, now=NOW + timedelta(seconds=1))
        store.finish(instance_id, TaskStatus.UNKNOWN, now=NOW)
        store.resolve(instance_id, "SUCCESS", "second private note")
        rows = store.list_execution_attempts(instance_id)
        assert [row["status"] for row in rows] == ["UNKNOWN", "UNKNOWN"]
        assert [row["attempt_number"] for row in rows] == [1, 1]
        assert [row["resolution"]["resolution"] for row in rows] == ["SUCCESS", "PENDING"]
        assert all(row["resolution"]["note_recorded"] for row in rows)
        assert "private" not in json.dumps(rows)
        assert store.get_status(instance_id) == TaskStatus.SUCCESS


def test_recovery_keeps_query_id_without_inventing_finish_time(tmp_path):
    path = tmp_path / "state.db"
    with closing(open_store(path)) as store:
        instance_id = enqueue(store)
        store.claim(instance_id, now=NOW)
        query_id = store.get_instance(instance_id)["query_id"]
    with closing(open_store(path, create=False)) as store:
        assert store.recover_running() == 1
        row = store.list_execution_attempts(instance_id)[0]
        assert row["query_id"] == query_id and row["status"] == "UNKNOWN"
        assert row["finished_at"] is None and row["recovered_at"] is not None
        assert row["error"] == "EXECUTION_INTERRUPTED"
        assert store.recover_running() == 0
        assert store.list_execution_attempts(instance_id) == [row]


def test_old_ledger_read_does_not_create_or_backfill_history(tmp_path):
    path = tmp_path / "state.db"
    with closing(open_store(path)) as store:
        instance_id = enqueue(store)
    before = path.read_bytes()
    with closing(StateStore(path)) as reader:
        assert not reader.history_available()
        assert reader.list_execution_attempts(instance_id) == []
        assert reader.list_legacy_resolutions(instance_id) == []
    assert path.read_bytes() == before


def test_attempts_filter_and_paginate_stably(tmp_path):
    with closing(open_store(tmp_path / "state.db")) as store:
        ids = [enqueue(store, minutes=minutes) for minutes in (0, 5, 10)]
        for instance_id in ids:
            store.claim(instance_id, now=NOW)
            store.finish(instance_id, TaskStatus.SUCCESS, now=NOW)
        assert store.history_available()
        rows = store.list_execution_attempts(taskname="root", limit=1, offset=1)
        assert [row["instance_id"] for row in rows] == ids[1:2]
        assert store.list_execution_attempts(taskname="missing") == []
        assert len(store.list_execution_attempts(ids[0])) == 1


@pytest.mark.parametrize("limit, offset", [(0, 0), (101, 0), (20, -1)])
def test_history_rejects_invalid_pagination(tmp_path, limit, offset):
    with closing(open_store(tmp_path / "state.db")) as store:
        with pytest.raises(ValueError):
            store.list_execution_attempts(limit=limit, offset=offset)


def test_failed_attempt_history_write_rolls_back_current_finish(tmp_path):
    with closing(open_store(tmp_path / "state.db")) as store:
        instance_id = enqueue(store)
        store.claim(instance_id, now=NOW)
        conn = store._connection(write=True)
        conn.execute("""CREATE TRIGGER reject_history BEFORE UPDATE ON execution_attempts
            BEGIN SELECT RAISE(ABORT, 'history unavailable'); END""")
        with pytest.raises(sqlite3.DatabaseError, match="history unavailable"):
            store.finish(instance_id, TaskStatus.SUCCESS, now=NOW)
        assert store.get_status(instance_id) == TaskStatus.RUNNING
        assert store.list_execution_attempts(instance_id)[0]["status"] == "RUNNING"


def test_attempt_order_compares_instants_across_timezones(tmp_path):
    with closing(open_store(tmp_path / "state.db")) as store:
        earlier, later = enqueue(store), enqueue(store, minutes=5)
        store.claim(earlier, now=NOW)
        store.claim(later, now=NOW.astimezone(timezone.utc) + timedelta(seconds=1))
        assert [row["instance_id"] for row in store.list_execution_attempts()] == [later, earlier]


def test_failed_history_insert_rolls_back_claim(tmp_path):
    with closing(open_store(tmp_path / "state.db")) as store:
        first, second = enqueue(store), enqueue(store, minutes=5)
        store.claim(first, now=NOW)
        conn = store._connection(write=True)
        conn.execute("""CREATE TRIGGER reject_claim BEFORE INSERT ON execution_attempts
            BEGIN SELECT RAISE(ABORT, 'history unavailable'); END""")
        with pytest.raises(sqlite3.DatabaseError, match="history unavailable"):
            store.claim(second, now=NOW)
        assert store.get_status(second) == TaskStatus.PENDING
        assert store.get_instance(second)["query_id"] is None
        assert store.list_execution_attempts(second) == []


def test_failed_resolution_insert_rolls_back_manual_resolution(tmp_path):
    with closing(open_store(tmp_path / "state.db")) as store:
        instance_id = enqueue(store)
        store.claim(instance_id, now=NOW)
        store.finish(instance_id, TaskStatus.UNKNOWN, now=NOW)
        conn = store._connection(write=True)
        conn.execute("""CREATE TRIGGER reject_resolution BEFORE INSERT ON execution_resolutions
            BEGIN SELECT RAISE(ABORT, 'resolution unavailable'); END""")
        with pytest.raises(sqlite3.DatabaseError, match="resolution unavailable"):
            store.resolve(instance_id, "SUCCESS", "operator verified")
        assert store.get_status(instance_id) == TaskStatus.UNKNOWN
        assert store.list_execution_attempts(instance_id)[0]["resolution"] is None


def test_failed_recovery_history_write_preserves_both_running_states(tmp_path):
    with closing(open_store(tmp_path / "state.db")) as store:
        instance_id = enqueue(store)
        store.claim(instance_id, now=NOW)
        conn = store._connection(write=True)
        conn.execute("""CREATE TRIGGER reject_recovery BEFORE UPDATE ON execution_attempts
            BEGIN SELECT RAISE(ABORT, 'history unavailable'); END""")
        with pytest.raises(sqlite3.DatabaseError, match="history unavailable"):
            store.recover_running()
        assert store.get_status(instance_id) == TaskStatus.RUNNING
        assert store.list_execution_attempts(instance_id)[0]["status"] == "RUNNING"


def test_legacy_running_attempt_is_not_backfilled_by_recovery(tmp_path):
    with closing(open_store(tmp_path / "state.db")) as store:
        legacy_id, new_id = enqueue(store), enqueue(store, minutes=5)
        conn = store._connection(write=True)
        with conn:
            conn.execute("""UPDATE task_instances SET status='RUNNING', attempts=1,
                query_id='legacy-query', started_at=? WHERE instance_id=?""",
                (NOW.isoformat(), legacy_id))
        store.claim(new_id, now=NOW)
        assert store.recover_running() == 2
        assert store.get_status(legacy_id) == TaskStatus.UNKNOWN
        assert store.list_execution_attempts(legacy_id) == []
        assert len(store.list_execution_attempts(new_id)) == 1


def seed_legacy(store, *, minutes=0, status="UNKNOWN", query_id=None):
    instance_id = enqueue(store, minutes=minutes)
    conn = store._connection(write=True)
    with conn:
        conn.execute("UPDATE task_instances SET status=?, attempts=1, query_id=? WHERE instance_id=?",
                     (status, query_id, instance_id))
    return instance_id


@pytest.mark.parametrize("status, query_id, resolution", [
    ("UNKNOWN", "legacy-query", "SUCCESS"), ("FAILED", "legacy-query", "PENDING"),
    ("UNKNOWN", None, "SUCCESS"),
])
def test_legacy_resolution_survives_reopen_without_inventing_attempts(
        tmp_path, status, query_id, resolution):
    path = tmp_path / "state.db"
    with closing(open_store(path)) as store:
        instance_id = seed_legacy(store, status=status, query_id=query_id)
        store.resolve(instance_id, resolution, "private operator note")
        resolved_at = store.get_instance(instance_id)["resolved_at"]
    before = path.read_bytes()
    with closing(StateStore(path)) as reader:
        assert reader.list_legacy_resolutions(instance_id) == [{
            "query_id": query_id, "instance_id": instance_id, "taskname": "root",
            "resolution": resolution, "resolved_at": resolved_at, "note_recorded": True}]
        assert reader.list_execution_attempts(instance_id) == []
    assert path.read_bytes() == before


def test_legacy_resolution_pagination_keeps_null_ids_and_excludes_recorded_attempts(tmp_path):
    with closing(open_store(tmp_path / "state.db")) as store:
        legacy = [seed_legacy(store, minutes=minutes) for minutes in (0, 5)]
        current = enqueue(store, minutes=10)
        store.claim(current, now=NOW)
        store.finish(current, TaskStatus.UNKNOWN, now=NOW)
        for instance_id in [*legacy, current]:
            store.resolve(instance_id, "SUCCESS", "private note")
        conn = store._connection(write=True)
        with conn:
            conn.execute("UPDATE execution_resolutions SET resolved_at=?", (NOW.isoformat(),))
        assert [row["instance_id"] for row in store.list_legacy_resolutions()] == legacy[::-1]
        rows = store.list_legacy_resolutions(taskname="root", limit=1, offset=1)
        assert [row["instance_id"] for row in rows] == legacy[:1]
        assert store.list_legacy_resolutions(current) == []
        assert store.list_legacy_resolutions(taskname="missing") == []
        assert "private" not in json.dumps(rows)


@pytest.mark.parametrize("limit, offset", [(0, 0), (101, 0), (20, -1)])
def test_legacy_resolutions_reject_invalid_pagination(tmp_path, limit, offset):
    with closing(open_store(tmp_path / "state.db")) as store:
        with pytest.raises(ValueError):
            store.list_legacy_resolutions(limit=limit, offset=offset)
