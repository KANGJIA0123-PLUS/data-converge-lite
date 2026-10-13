"""Durable duplicate suppression, conservative recovery and bounded retries."""

from datetime import datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from data_converge.dag import DAGBuilder, DAGExecutor, TaskStatus


TZ = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 10, 12, 10, 6, tzinfo=TZ)


def graph():
    rules = [dict(taskname="root", source_tables=["raw"], target_table="rollup",
                  aggregation_interval="PT5M", cron="1,6,11,16,21,26,31,36,41,46,51,56 * * * *",
                  depends_on=[], sql_id="root")]
    builder = DAGBuilder(SimpleNamespace(validate=lambda: [], get_all_rules=lambda: rules))
    return builder, builder.build()


def open_store(path, create=True):
    from data_converge.state import StateStore
    store = StateStore(path, create=create)
    store.acquire_lock()
    return store


def test_missing_state_requires_explicit_initialization(tmp_path):
    from data_converge.state import StateStore
    with pytest.raises(FileNotFoundError, match="init-state"):
        StateStore(tmp_path / "state.db")
    assert not (tmp_path / "state.db").exists()


def test_lock_prevents_second_local_writer(tmp_path):
    from data_converge.state import StateStore
    first = open_store(tmp_path / "state.db")
    second = StateStore(tmp_path / "state.db")
    try:
        with pytest.raises(RuntimeError, match="lock|锁"):
            second.acquire_lock()
    finally:
        first.close()
        second.close()


def test_success_survives_reopen_and_is_not_submitted_again(tmp_path):
    builder, dag = graph()
    calls = []
    for create in (True, False):
        store = open_store(tmp_path / "state.db", create=create)
        try:
            executor = DAGExecutor(dag, lambda i: calls.append(i.instance_id), state_store=store)
            result = executor.execute([builder.create_instance(dag, "root", NOW)])
            assert len(result.succeeded) == 1
        finally:
            store.close()
    assert calls == ["root@2026-10-12T10:05:00+08:00"]


def test_abandoned_running_is_unknown_and_requires_explicit_resolution(tmp_path):
    builder, dag = graph()
    instance = builder.create_instance(dag, "root", NOW)
    store = open_store(tmp_path / "state.db")
    store.enqueue_and_advance([instance], {"root": NOW + timedelta(minutes=5)})
    assert store.claim(instance.instance_id, now=NOW, max_attempts=3)
    store.close()
    store = open_store(tmp_path / "state.db", create=False)
    try:
        assert store.recover_running() == 1
        assert store.get_status(instance.instance_id) == TaskStatus.UNKNOWN
        calls = []
        executor = DAGExecutor(dag, lambda i: calls.append(i.instance_id), state_store=store)
        executor.execute([builder.create_instance(dag, "root", NOW)])
        assert calls == []
        with pytest.raises(ValueError, match="note|备注"):
            store.resolve(instance.instance_id, "SUCCESS", "")
        store.resolve(instance.instance_id, "SUCCESS", "Verified target and completed query manually")
        assert store.get_status(instance.instance_id) == TaskStatus.SUCCESS
        with pytest.raises(ValueError):
            store.resolve(instance.instance_id, "PENDING", "Cannot override success")
    finally:
        store.close()


def test_cursor_and_window_registration_rollback_together(tmp_path):
    builder, dag = graph()
    store = open_store(tmp_path / "state.db")
    try:
        instance = builder.create_instance(dag, "root", NOW)
        with pytest.raises((TypeError, ValueError, AttributeError)):
            store.enqueue_and_advance([instance], {"root": object()})
        assert store.get_status(instance.instance_id) is None
        assert store.get_cursor("root") is None
        store.enqueue_and_advance([instance], {"root": NOW})
        store.enqueue_and_advance([instance], {"root": NOW + timedelta(minutes=5)})
        assert len(store.list_pending()) == 1
        assert store.get_cursor("root") == NOW + timedelta(minutes=5)
    finally:
        store.close()


def test_only_known_unsent_failures_retry_with_limit_and_delay(tmp_path):
    from data_converge.sql_execution.client import SQLExecutionError
    builder, dag = graph()
    store = open_store(tmp_path / "state.db")
    calls = []

    def unsent(instance):
        calls.append(instance.instance_id)
        raise SQLExecutionError("connect failed before sending", retryable=True)

    try:
        executor = DAGExecutor(dag, unsent, state_store=store, max_attempts=3, retry_delay_seconds=30)
        for seconds in (0, 1, 30, 60, 90):
            executor._now = lambda _: NOW + timedelta(seconds=seconds)
            executor.execute([builder.create_instance(dag, "root", NOW)], now=NOW + timedelta(seconds=seconds))
        assert len(calls) == 3
        assert store.get_status(calls[0]) == TaskStatus.FAILED
    finally:
        store.close()


@pytest.mark.parametrize("known", [False, True])
def test_uncertain_execution_is_never_automatically_retried(tmp_path, known):
    from data_converge.sql_execution.client import SQLExecutionError
    builder, dag = graph()
    store = open_store(tmp_path / "state.db")
    calls = []

    def uncertain(instance):
        calls.append(instance.instance_id)
        if known:
            raise SQLExecutionError("response lost", retryable=True, outcome_unknown=True)
        raise RuntimeError("unexpected runner failure")

    try:
        executor = DAGExecutor(dag, uncertain, state_store=store)
        for seconds in (0, 300):
            result = executor.execute([builder.create_instance(dag, "root", NOW)], now=NOW + timedelta(seconds=seconds))
        assert len(calls) == 1
        assert result.instances[calls[0]].status == TaskStatus.UNKNOWN
    finally:
        store.close()


def test_success_persistence_failure_escapes_instead_of_becoming_retry(tmp_path, monkeypatch):
    builder, dag = graph()
    store = open_store(tmp_path / "state.db")
    calls = []
    executor = DAGExecutor(dag, lambda i: calls.append(i.instance_id), state_store=store)
    try:
        def disk_failure(*args, **kwargs):
            raise OSError("disk full")
        monkeypatch.setattr(store, "finish", disk_failure)
        with pytest.raises(OSError, match="disk full"):
            executor.execute([builder.create_instance(dag, "root", NOW)])
        assert len(calls) == 1
        assert store.get_status(calls[0]) == TaskStatus.RUNNING
    finally:
        store.close()


def test_query_id_is_durable_before_sql_and_changes_only_on_manual_retry(tmp_path):
    builder, dag = graph()
    store = open_store(tmp_path / "state.db")
    observed = []

    # The runner executes on a worker; inspect persisted identity independently.
    from data_converge.state import StateStore
    def observe(instance):
        reader = StateStore(store.path)
        try:
            saved = reader.get_instance(instance.instance_id)
            observed.append((instance.query_id, saved["query_id"]))
        finally:
            reader.close()
        raise RuntimeError("unknown response")

    try:
        executor = DAGExecutor(dag, observe, state_store=store)
        instance = builder.create_instance(dag, "root", NOW)
        executor.execute([instance])
        store.resolve(instance.instance_id, "PENDING", "Verified no insertion and no active query")
        executor.execute([builder.create_instance(dag, "root", NOW)])
        assert len(observed) == 2
        assert observed[0][0] and observed[0][0] == observed[0][1]
        assert observed[1][0] and observed[1][0] == observed[1][1]
        assert observed[0][0] != observed[1][0]
    finally:
        store.close()


def test_status_reader_does_not_need_writer_lock_and_cannot_mutate(tmp_path):
    from data_converge.state import StateStore
    store = open_store(tmp_path / "state.db")
    builder, dag = graph()
    instance = builder.create_instance(dag, "root", NOW)
    store.set_meta("coverage_start", NOW.isoformat())
    store.enqueue_and_advance([instance], {})
    reader = StateStore(store.path)
    try:
        assert reader.status_counts() == {"PENDING": 1}
        assert reader.list_instances()[0]["instance_id"] == instance.instance_id
        assert reader.get_meta("coverage_start") == NOW.isoformat()
        with pytest.raises(RuntimeError, match="lock"):
            reader.recover_running()
    finally:
        reader.close()
        store.close()


def test_retry_delay_begins_after_failure_finishes_not_before_slow_attempt(tmp_path):
    from data_converge.sql_execution.client import SQLExecutionError
    builder, dag = graph()
    store = open_store(tmp_path / "state.db")
    calls = []

    def unsent(instance):
        calls.append(instance.instance_id)
        raise SQLExecutionError("unsent", retryable=True)

    try:
        executor = DAGExecutor(dag, unsent, state_store=store, retry_delay_seconds=30)
        executor._now = lambda _: NOW + timedelta(minutes=2)
        executor.execute([builder.create_instance(dag, "root", NOW)], now=NOW)
        executor.execute([builder.create_instance(dag, "root", NOW)], now=NOW + timedelta(minutes=2))
        assert len(calls) == 1
    finally:
        store.close()


@pytest.mark.parametrize("retryable", [True, False])
def test_operator_can_retry_definite_failed_window_with_a_fresh_attempt_budget(tmp_path, retryable):
    from data_converge.sql_execution.client import SQLExecutionError
    builder, dag = graph()
    store = open_store(tmp_path / "state.db")
    calls = []

    def fail(instance):
        calls.append(instance.instance_id)
        raise SQLExecutionError("CLICKHOUSE_CONNECT_FAILED" if retryable else "CLICKHOUSE_AUTH_FAILED",
                                retryable=retryable)

    try:
        executor = DAGExecutor(dag, fail, state_store=store, max_attempts=2, retry_delay_seconds=30)
        instance = builder.create_instance(dag, "root", NOW)
        for seconds in (0, 30, 60):
            executor._now = lambda _: NOW + timedelta(seconds=seconds)
            executor.execute([builder.create_instance(dag, "root", NOW)], now=NOW + timedelta(seconds=seconds))
        before = len(calls)
        assert before == (2 if retryable else 1)
        with pytest.raises(ValueError):
            store.resolve(instance.instance_id, "SUCCESS", "Cannot declare a failed query successful")
        with pytest.raises(ValueError):
            store.resolve(instance.instance_id, "PENDING", " ")
        store.resolve(instance.instance_id, "PENDING", "Fixed connection or credentials; confirmed no insertion")
        assert store.get_instance(instance.instance_id)["attempts"] == 0
        executor.execute([builder.create_instance(dag, "root", NOW)], now=NOW + timedelta(seconds=90))
        assert len(calls) == before + 1
        if retryable:
            executor._now = lambda _: NOW + timedelta(seconds=120)
            executor.execute([builder.create_instance(dag, "root", NOW)], now=NOW + timedelta(seconds=120))
            assert len(calls) == before + 2
    finally:
        store.close()


def test_unclassified_exception_body_is_not_saved_in_state_or_report(tmp_path):
    builder, dag = graph()
    store = open_store(tmp_path / "state.db")

    def fail(instance):
        raise RuntimeError("INSERT INTO private_table; password=DO_NOT_PERSIST")

    try:
        executor = DAGExecutor(dag, fail, state_store=store)
        report = executor.execute([builder.create_instance(dag, "root", NOW)])
        assert report.unknown[0].error == "UNEXPECTED_RUNNER_ERROR:RuntimeError"
        record = store.get_instance(report.unknown[0].instance_id)
        assert "private_table" not in record["error"]
        assert "DO_NOT_PERSIST" not in record["error"]
    finally:
        store.close()


def test_dwd_signals_are_durable_idempotent_and_match_equivalent_timezones(tmp_path):
    from datetime import timezone
    from data_converge.state import StateStore
    path = tmp_path / "state.db"
    store = open_store(path)
    start = NOW.replace(minute=0)
    store.record_dwd_signal(start, "GPU_REALTIME", NOW)
    store.record_dwd_signal(start.astimezone(timezone.utc), "GPU_REALTIME", NOW)
    store.record_dwd_signal(start, "NODE_REALTIME", NOW)
    store.close()
    reader = StateStore(path)
    try:
        assert reader.get_dwd_types(start) == {"GPU_REALTIME", "NODE_REALTIME"}
        windows = reader.list_dwd_windows()
        assert len(windows) == 1
        assert set(windows[0]["types"]) == {"GPU_REALTIME", "NODE_REALTIME"}
        assert datetime.fromisoformat(windows[0]["window_start"]) == start
    finally:
        reader.close()


def test_dwd_table_migrates_existing_valid_ledger_without_changing_state(tmp_path):
    import sqlite3
    path = tmp_path / "state.db"
    store = open_store(path)
    store.set_meta("coverage_start", NOW.isoformat())
    store.close()
    with sqlite3.connect(path) as conn:
        conn.execute("DROP TABLE IF EXISTS dwd_ready_signals")
    store = open_store(path, create=False)
    try:
        store.record_dwd_signal(NOW.replace(minute=0), "GPU_REALTIME", NOW)
        assert store.get_meta("coverage_start") == NOW.isoformat()
    finally:
        store.close()


def test_dwd_migration_does_not_turn_an_invalid_database_into_a_ledger(tmp_path):
    import sqlite3
    from data_converge.state import StateStore
    path = tmp_path / "state.db"
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE metadata(key TEXT PRIMARY KEY, value TEXT)")
    store = StateStore(path)
    try:
        with pytest.raises(sqlite3.DatabaseError):
            store.acquire_lock()
    finally:
        store.close()
    with sqlite3.connect(path) as conn:
        assert not conn.execute("SELECT name FROM sqlite_master WHERE name='dwd_ready_signals'").fetchall()


def test_mark_blocked_only_writes_when_status_or_reason_changes(tmp_path):
    builder, dag = graph()
    instance = builder.create_instance(dag, "root", NOW)
    store = open_store(tmp_path / "state.db")
    try:
        store.enqueue_and_advance([instance], {})
        assert store.claim(instance.instance_id, now=NOW)
        store.finish(instance.instance_id, TaskStatus.FAILED, error="WAITING_DWD",
                     retryable=True, next_retry_at=NOW + timedelta(seconds=30), now=NOW)
        previous = store.get_instance(instance.instance_id)
        changes = store._connection().total_changes

        for reason, expected_changes in (("WAITING_DWD", 1), ("WAITING_DWD", 1), ("WAITING_PARENT", 2)):
            store.mark_blocked(instance.instance_id, reason)
            assert store._connection().total_changes == changes + expected_changes
            assert store.get_instance(instance.instance_id) == previous | {"status": "BLOCKED", "error": reason}
    finally:
        store.close()


def test_readiness_gate_blocks_claim_then_releases_persisted_window(tmp_path):
    builder, dag = graph()
    store = open_store(tmp_path / "state.db")
    calls = []
    start = NOW.replace(minute=0)

    def check(instance, now):
        return None if "GPU_REALTIME" in store.get_dwd_types(instance.data_start) else "WAITING_DWD:GPU_REALTIME"

    try:
        executor = DAGExecutor(dag, lambda i: calls.append(i.instance_id), state_store=store, readiness_check=check)
        result = executor.execute([builder.create_instance(dag, "root", NOW)], now=NOW)
        assert result.blocked[0].error == "WAITING_DWD:GPU_REALTIME"
        assert store.get_instance(result.blocked[0].instance_id)["attempts"] == 0
        assert calls == []
        store.record_dwd_signal(start, "GPU_REALTIME", NOW)
        result = executor.execute([builder.create_instance(dag, "root", NOW)], now=NOW)
        assert len(result.succeeded) == 1
        assert len(calls) == 1
        executor.readiness_check = lambda *_: "WAITING_DWD"
        result = executor.execute([builder.create_instance(dag, "root", NOW)], now=NOW)
        assert len(result.succeeded) == 1
        assert len(calls) == 1
    finally:
        store.close()


def test_readiness_gate_cannot_downgrade_unknown_or_make_it_retry(tmp_path):
    builder, dag = graph()
    store = open_store(tmp_path / "state.db")
    calls = []

    def unknown(instance):
        calls.append(instance.instance_id)
        raise RuntimeError("unknown")

    try:
        executor = DAGExecutor(dag, unknown, state_store=store, readiness_check=lambda *_: None)
        executor.execute([builder.create_instance(dag, "root", NOW)])
        executor.readiness_check = lambda *_: "WAITING_DWD"
        result = executor.execute([builder.create_instance(dag, "root", NOW)])
        assert len(result.unknown) == 1
        assert len(calls) == 1
    finally:
        store.close()
