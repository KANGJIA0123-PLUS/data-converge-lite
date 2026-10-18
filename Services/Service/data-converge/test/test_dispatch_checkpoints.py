"""Submission checkpoints prove only which reservations never reached dispatch."""

import sqlite3
from contextlib import closing
from datetime import timedelta

import pytest

from data_converge.dag import TaskStatus
from data_converge.state import StateStore
from .test_execution_history import enqueue
from .test_state import NOW, open_store


def checkpoint(store, query_id):
    return dict(store._connection().execute(
        "SELECT * FROM dispatch_checkpoints WHERE query_id=?", (query_id,)).fetchone())


def event_types(store):
    return [row[0] for row in store._connection().execute(
        "SELECT event_type FROM execution_events ORDER BY event_id")]


def prepare(store, instance_id):
    assert store.prepare_attempt(instance_id, now=NOW)
    return store.get_instance(instance_id)["query_id"]


def test_prepare_reserves_without_consuming_budget_or_inventing_attempt(tmp_path):
    with closing(open_store(tmp_path / "state.db")) as store:
        instance_id = enqueue(store)
        query_id = prepare(store, instance_id)
        row = store.get_instance(instance_id)
        assert row["status"] == "RUNNING" and row["attempts"] == 0
        assert row["started_at"] is row["finished_at"] is None
        assert query_id and checkpoint(store, query_id)["phase"] == "PREPARED"
        assert store.list_execution_attempts(instance_id) == []
        assert event_types(store) == ["PREPARED"]
        assert not store.prepare_attempt(instance_id, now=NOW)


def test_dispatch_consumes_budget_once_and_persists_attempt_identity(tmp_path):
    with closing(open_store(tmp_path / "state.db")) as store:
        instance_id = enqueue(store)
        query_id = prepare(store, instance_id)
        store.mark_dispatched(instance_id, query_id, now=NOW)
        assert store.get_instance(instance_id)["attempts"] == 1
        attempt = store.list_execution_attempts(instance_id)[0]
        assert attempt["query_id"] == query_id and attempt["attempt_number"] == 1
        assert attempt["status"] == "RUNNING"
        assert checkpoint(store, query_id)["phase"] == "DISPATCHED"
        with pytest.raises(RuntimeError):
            store.mark_dispatched(instance_id, query_id, now=NOW)
        assert store.get_instance(instance_id)["attempts"] == 1
        assert event_types(store) == ["PREPARED", "DISPATCHED"]


def test_dispatch_rejects_stale_query_id_without_mutating_reservation(tmp_path):
    with closing(open_store(tmp_path / "state.db")) as store:
        instance_id = enqueue(store)
        query_id = prepare(store, instance_id)
        with pytest.raises(RuntimeError):
            store.mark_dispatched(instance_id, "stale-query", now=NOW)
        assert store.get_instance(instance_id)["attempts"] == 0
        assert checkpoint(store, query_id)["phase"] == "PREPARED"
        assert store.list_execution_attempts(instance_id) == []


@pytest.mark.parametrize("status", [TaskStatus.SUCCESS, TaskStatus.FAILED, TaskStatus.UNKNOWN])
def test_finish_rejects_prepared_reservation_without_inventing_execution(tmp_path, status):
    with closing(open_store(tmp_path / "state.db")) as store:
        instance_id = enqueue(store)
        query_id = prepare(store, instance_id)
        with pytest.raises(RuntimeError):
            store.finish(instance_id, status, now=NOW)
        assert store.get_status(instance_id) == TaskStatus.RUNNING
        assert checkpoint(store, query_id)["phase"] == "PREPARED"
        assert store.list_execution_attempts(instance_id) == []
        assert event_types(store) == ["PREPARED"]


def test_prepared_recovery_requeues_without_changing_consumed_attempts(tmp_path):
    path = tmp_path / "state.db"
    with closing(open_store(path)) as store:
        instance_id = enqueue(store)
        query_id = prepare(store, instance_id)
    with closing(open_store(path, create=False)) as store:
        assert store.recover_running() == 1
        row = store.get_instance(instance_id)
        assert row["status"] == "PENDING" and row["attempts"] == 0
        assert row["started_at"] is row["finished_at"] is None
        assert checkpoint(store, query_id)["phase"] == "NOT_SUBMITTED"
        assert store.list_execution_attempts(instance_id) == []
        assert event_types(store) == ["PREPARED", "RECOVERED_UNSUBMITTED"]
        assert store.recover_running() == 0
        assert prepare(store, instance_id) != query_id


def test_dispatched_and_legacy_claim_recovery_remain_unknown(tmp_path):
    with closing(open_store(tmp_path / "state.db")) as store:
        dispatched, legacy = enqueue(store), enqueue(store, minutes=5)
        query_id = prepare(store, dispatched)
        store.mark_dispatched(dispatched, query_id, now=NOW)
        assert store.claim(legacy, now=NOW)
        assert store.recover_running() == 2
        for instance_id in (dispatched, legacy):
            assert store.get_status(instance_id) == TaskStatus.UNKNOWN
            assert store.get_instance(instance_id)["attempts"] == 1
            assert store.list_execution_attempts(instance_id)[0]["status"] == "UNKNOWN"
            assert not store.prepare_attempt(instance_id, now=NOW)
        assert event_types(store).count("RECOVERY_REQUIRED") == 2


def test_legacy_claim_after_prepared_recovery_cannot_reuse_its_proof(tmp_path):
    with closing(open_store(tmp_path / "state.db")) as store:
        instance_id = enqueue(store)
        prepared_query = prepare(store, instance_id)
        store.recover_running()
        assert store.claim(instance_id, now=NOW)
        dispatched_query = store.get_instance(instance_id)["query_id"]
        assert dispatched_query != prepared_query
        assert store.list_recovery_candidates()[0]["action"] == "RECONCILE"
        assert store.recover_running() == 1
        assert store.get_status(instance_id) == TaskStatus.UNKNOWN
        assert checkpoint(store, prepared_query)["phase"] == "NOT_SUBMITTED"
        assert [row["query_id"] for row in store.list_execution_attempts()] == [dispatched_query]


def test_old_prepared_checkpoint_cannot_reset_a_different_current_attempt(tmp_path):
    with closing(open_store(tmp_path / "state.db")) as store:
        instance_id = enqueue(store)
        old_query = prepare(store, instance_id)
        conn = store._connection(write=True)
        with conn:
            conn.execute("UPDATE task_instances SET query_id='new-query' WHERE instance_id=?",
                         (instance_id,))
        assert store.recover_running() == 1
        assert store.get_status(instance_id) == TaskStatus.UNKNOWN
        assert checkpoint(store, old_query)["phase"] == "PREPARED"


@pytest.mark.parametrize("status", [TaskStatus.SUCCESS, TaskStatus.UNKNOWN])
def test_recovery_leaves_existing_terminal_states_and_history_unchanged(tmp_path, status):
    with closing(open_store(tmp_path / "state.db")) as store:
        instance_id = enqueue(store)
        query_id = prepare(store, instance_id)
        store.mark_dispatched(instance_id, query_id, now=NOW)
        store.finish(instance_id, status, now=NOW)
        before = store.get_instance(instance_id), store.list_execution_attempts(instance_id)
        before_events = event_types(store)
        assert store.recover_running() == 0
        assert (store.get_instance(instance_id), store.list_execution_attempts(instance_id)) == before
        assert event_types(store) == before_events


def test_prepared_retry_recovery_keeps_budget_and_honors_reduced_limit(tmp_path):
    with closing(open_store(tmp_path / "state.db")) as store:
        instance_id = enqueue(store)
        store.claim(instance_id, now=NOW)
        store.finish(instance_id, TaskStatus.FAILED, retryable=True, now=NOW)
        assert store.prepare_attempt(instance_id, now=NOW, max_attempts=2)
        assert store.recover_running() == 1
        assert store.get_instance(instance_id)["attempts"] == 1
        assert not store.prepare_attempt(instance_id, now=NOW, max_attempts=1)
        assert store.prepare_attempt(instance_id, now=NOW, max_attempts=2)
        query_id = store.get_instance(instance_id)["query_id"]
        store.mark_dispatched(instance_id, query_id, now=NOW)
        store.finish(instance_id, TaskStatus.FAILED, retryable=True, now=NOW)
        assert not store.prepare_attempt(instance_id, now=NOW, max_attempts=2)
        assert [row["attempt_number"] for row in store.list_execution_attempts()] == [2, 1]


def test_preparation_preserves_retry_delay_and_missing_instance_errors(tmp_path):
    with closing(open_store(tmp_path / "state.db")) as store:
        instance_id = enqueue(store)
        store.claim(instance_id, now=NOW)
        later = NOW + timedelta(seconds=30)
        store.finish(instance_id, TaskStatus.FAILED, retryable=True, next_retry_at=later, now=NOW)
        assert not store.prepare_attempt(instance_id, now=NOW)
        assert store.prepare_attempt(instance_id, now=later)
        with pytest.raises(KeyError):
            store.prepare_attempt("missing", now=NOW)


def test_read_only_recovery_plan_classifies_current_evidence_without_writes(tmp_path):
    path = tmp_path / "state.db"
    with closing(open_store(path)) as store:
        prepared, dispatched, unknown = [enqueue(store, minutes=n) for n in (0, 5, 10)]
        prepare(store, prepared)
        query_id = prepare(store, dispatched)
        store.mark_dispatched(dispatched, query_id, now=NOW)
        store.claim(unknown, now=NOW)
        store.finish(unknown, TaskStatus.UNKNOWN, now=NOW)
    before = path.read_bytes()
    with closing(StateStore(path)) as reader:
        rows = reader.list_recovery_candidates(limit=2)
        assert [row["instance_id"] for row in rows] == [prepared, dispatched]
        assert [row["action"] for row in rows] == ["AUTO_RESUME", "RECONCILE"]
        assert [row["phase"] for row in rows] == ["PREPARED", "DISPATCHED"]
        assert reader.recovery_counts() == {"AUTO_RESUME": 1, "RECONCILE": 2}
        assert reader.list_recovery_candidates(limit=2, offset=2)[0]["instance_id"] == unknown
    assert path.read_bytes() == before


def test_old_ledger_recovery_plan_is_conservative_without_creating_tables(tmp_path):
    path = tmp_path / "state.db"
    with closing(open_store(path)) as store:
        instance_id = enqueue(store)
        with store._connection(write=True) as conn:
            conn.execute("UPDATE task_instances SET status='RUNNING', query_id='legacy'")
    before = path.read_bytes()
    with closing(StateStore(path)) as reader:
        row = reader.list_recovery_candidates()[0]
        assert row["instance_id"] == instance_id and row["phase"] is None
        assert row["action"] == "RECONCILE"
        assert reader.recovery_counts() == {"AUTO_RESUME": 0, "RECONCILE": 1}
    assert path.read_bytes() == before


@pytest.mark.parametrize("limit,offset", [(0, 0), (101, 0), (20, -1), (True, 0)])
def test_recovery_plan_rejects_invalid_pagination(tmp_path, limit, offset):
    with closing(open_store(tmp_path / "state.db")) as store:
        with pytest.raises(ValueError):
            store.list_recovery_candidates(limit=limit, offset=offset)


def test_recovery_plan_offset_past_sqlite_integer_range_is_empty(tmp_path):
    with closing(open_store(tmp_path / "state.db")) as store:
        prepare(store, enqueue(store))
        assert store.list_recovery_candidates(offset=2**63) == []


def seed_unknown_instances(store, count=1000):
    stamp = NOW.isoformat()
    with store._connection(write=True) as conn:
        conn.executemany("""INSERT INTO task_instances
            (instance_id, taskname, scheduled_time, data_start, data_end, status)
            VALUES (?, 'old', ?, ?, ?, 'UNKNOWN')""",
            ((f"old-{index}", stamp, stamp, stamp) for index in range(count)))


def count_materialized_rows(conn, counter):
    def row_factory(cursor, row):
        counter.append(1)
        return sqlite3.Row(cursor, row)
    conn.row_factory = row_factory


@pytest.mark.parametrize("with_checkpoint", [False, True])
def test_recovery_counts_do_not_materialize_retained_unknown_rows(tmp_path, with_checkpoint):
    with closing(open_store(tmp_path / "state.db")) as store:
        seed_unknown_instances(store)
        if with_checkpoint:
            prepare(store, enqueue(store))
        conn, materialized = store._connection(), []
        original = conn.row_factory
        count_materialized_rows(conn, materialized)
        try:
            counts = store.recovery_counts()
        finally:
            conn.row_factory = original
        assert counts == {"AUTO_RESUME": int(with_checkpoint), "RECONCILE": 1000}
        assert len(materialized) <= 4


def test_startup_recovery_does_not_load_retained_unknown_instances(tmp_path):
    with closing(open_store(tmp_path / "state.db")) as store:
        seed_unknown_instances(store)
        instance_id = enqueue(store)
        prepare(store, instance_id)
        conn, materialized = store._connection(), []
        original = conn.row_factory
        count_materialized_rows(conn, materialized)
        try:
            recovered = store.recover_running()
        finally:
            conn.row_factory = original
        assert recovered == 1 and store.get_status(instance_id) == TaskStatus.PENDING
        assert store.status_counts() == {"PENDING": 1, "UNKNOWN": 1000}
        assert len(materialized) <= 8


def reject_events(store):
    store._connection(write=True).execute("""CREATE TRIGGER reject_event
        BEFORE INSERT ON execution_events
        BEGIN SELECT RAISE(ABORT, 'event unavailable'); END""")


def test_failed_preparation_event_rolls_back_reservation(tmp_path):
    with closing(open_store(tmp_path / "state.db")) as store:
        first, second = enqueue(store), enqueue(store, minutes=5)
        prepare(store, first)
        reject_events(store)
        with pytest.raises(sqlite3.DatabaseError, match="event unavailable"):
            store.prepare_attempt(second, now=NOW)
        row = store.get_instance(second)
        assert row["status"] == "PENDING" and row["query_id"] is None
        assert store.list_recovery_candidates()[0]["instance_id"] == first


def test_failed_dispatch_event_rolls_back_checkpoint_budget_and_history(tmp_path):
    with closing(open_store(tmp_path / "state.db")) as store:
        instance_id = enqueue(store)
        query_id = prepare(store, instance_id)
        reject_events(store)
        with pytest.raises(sqlite3.DatabaseError, match="event unavailable"):
            store.mark_dispatched(instance_id, query_id, now=NOW)
        assert checkpoint(store, query_id)["phase"] == "PREPARED"
        assert store.get_instance(instance_id)["attempts"] == 0
        assert store.list_execution_attempts(instance_id) == []


def test_failed_dispatch_history_insert_preserves_prepared_reservation(tmp_path):
    with closing(open_store(tmp_path / "state.db")) as store:
        earlier, current = enqueue(store), enqueue(store, minutes=5)
        store.claim(earlier, now=NOW)
        query_id = prepare(store, current)
        store._connection(write=True).execute("""CREATE TRIGGER reject_attempt
            BEFORE INSERT ON execution_attempts
            BEGIN SELECT RAISE(ABORT, 'history unavailable'); END""")
        with pytest.raises(sqlite3.DatabaseError, match="history unavailable"):
            store.mark_dispatched(current, query_id, now=NOW)
        assert checkpoint(store, query_id)["phase"] == "PREPARED"
        assert store.get_instance(current)["attempts"] == 0
        assert store.list_execution_attempts(current) == []
        assert event_types(store) == ["DISPATCHED", "PREPARED"]


def test_failed_recovery_event_preserves_prepared_state(tmp_path):
    with closing(open_store(tmp_path / "state.db")) as store:
        instance_id = enqueue(store)
        query_id = prepare(store, instance_id)
        reject_events(store)
        with pytest.raises(sqlite3.DatabaseError, match="event unavailable"):
            store.recover_running()
        assert store.get_status(instance_id) == TaskStatus.RUNNING
        assert checkpoint(store, query_id)["phase"] == "PREPARED"


def test_failed_mixed_recovery_rolls_back_safe_requeue_and_prior_events(tmp_path):
    with closing(open_store(tmp_path / "state.db")) as store:
        prepared, dispatched = enqueue(store), enqueue(store, minutes=5)
        query_id = prepare(store, prepared)
        store.claim(dispatched, now=NOW)
        before_events = event_types(store)
        store._connection(write=True).execute("""CREATE TRIGGER reject_recovery
            BEFORE UPDATE ON execution_attempts
            BEGIN SELECT RAISE(ABORT, 'history unavailable'); END""")
        with pytest.raises(sqlite3.DatabaseError, match="history unavailable"):
            store.recover_running()
        assert store.get_status(prepared) == store.get_status(dispatched) == TaskStatus.RUNNING
        assert checkpoint(store, query_id)["phase"] == "PREPARED"
        assert store.list_execution_attempts(dispatched)[0]["status"] == "RUNNING"
        assert event_types(store) == before_events


@pytest.mark.parametrize("operation", ["finish", "resolve"])
def test_failed_terminal_event_rolls_back_outcome_and_history(tmp_path, operation):
    with closing(open_store(tmp_path / "state.db")) as store:
        instance_id = enqueue(store)
        store.claim(instance_id, now=NOW)
        if operation == "resolve":
            store.finish(instance_id, TaskStatus.UNKNOWN, now=NOW)
        before = store.get_instance(instance_id), store.list_execution_attempts(instance_id)
        reject_events(store)
        with pytest.raises(sqlite3.DatabaseError, match="event unavailable"):
            if operation == "finish":
                store.finish(instance_id, TaskStatus.SUCCESS, now=NOW)
            else:
                store.resolve(instance_id, "SUCCESS", "operator verified")
        assert (store.get_instance(instance_id), store.list_execution_attempts(instance_id)) == before
