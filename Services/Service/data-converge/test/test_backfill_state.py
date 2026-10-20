from datetime import timedelta

import pytest

from data_converge.dag import TaskStatus
from .test_state import NOW, graph, open_store


def test_explicit_recompute_records_new_attempt_and_preserves_cursor(tmp_path):
    builder, dag = graph()
    instance = builder.create_instance(dag, "root", NOW)
    with_store = open_store(tmp_path / "state.db")
    try:
        with_store.enqueue_and_advance([instance], {"root": NOW})
        with_store.claim(instance.instance_id, now=NOW)
        with_store.finish(instance.instance_id, TaskStatus.SUCCESS, now=NOW)
        assert hasattr(with_store, "begin_backfill")
        skipped = with_store.begin_backfill("job:0", instance, recompute=False, now=NOW)
        assert skipped["status"] == "SKIPPED"
        started = with_store.begin_backfill("job:1", instance, recompute=True, now=NOW)
        assert started["status"] == "RUNNING"
        with_store.finish(instance.instance_id, TaskStatus.SUCCESS, now=NOW)
        assert with_store.get_backfill("job:1")["status"] == "SUCCESS"
        assert len(with_store.list_execution_attempts()) == 2
        assert with_store.get_cursor("root") == NOW
    finally:
        with_store.close()


def test_backfill_running_is_unknown_on_restart_not_automatically_requeued(tmp_path):
    builder, dag = graph()
    instance = builder.create_instance(dag, "root", NOW)
    store = open_store(tmp_path / "state.db")
    try:
        assert hasattr(store, "begin_backfill")
        first = store.begin_backfill("job:0", instance, recompute=True, now=NOW)
        store.recover_running()
        assert store.get_instance(instance.instance_id)["status"] == "UNKNOWN"
        assert store.get_backfill("job:0")["status"] == "UNKNOWN"
        assert store.list_pending() == []
        assert store.list_execution_attempts()[0]["status"] == "UNKNOWN"
        assert store.begin_backfill("job:0", instance, recompute=True, now=NOW)["query_id"] == first["query_id"]
    finally:
        store.close()


def test_manual_historical_window_does_not_change_coverage_or_cursors(tmp_path):
    builder, dag = graph()
    instance = builder.create_instance(dag, "root", NOW - timedelta(days=2))
    store = open_store(tmp_path / "state.db")
    try:
        store.set_meta("coverage_start", NOW.isoformat())
        assert hasattr(store, "begin_backfill")
        result = store.begin_backfill("job:0", instance, recompute=False, now=NOW)
        assert result["status"] == "RUNNING"
        assert store.get_meta("coverage_start") == NOW.isoformat()
        assert store.get_cursor("root") is None
    finally:
        store.close()


@pytest.mark.parametrize("status", [TaskStatus.UNKNOWN, TaskStatus.RUNNING])
def test_unsafe_existing_instance_cannot_be_overwritten(tmp_path, status):
    builder, dag = graph()
    instance = builder.create_instance(dag, "root", NOW)
    store = open_store(tmp_path / "state.db")
    try:
        store.enqueue_and_advance([instance], {})
        store.claim(instance.instance_id, now=NOW)
        if status == TaskStatus.UNKNOWN:
            store.finish(instance.instance_id, status, now=NOW)
        before = store.get_instance(instance.instance_id)
        assert hasattr(store, "begin_backfill")
        result = store.begin_backfill("job:0", instance, recompute=True, now=NOW)
        assert result["status"] == "BLOCKED"
        assert store.get_instance(instance.instance_id) == before
    finally:
        store.close()


@pytest.mark.parametrize("fold", [0, 1])
def test_existing_repeated_dst_hour_compares_by_instant(tmp_path, fold):
    from datetime import datetime
    from zoneinfo import ZoneInfo
    builder, dag = graph()
    when = datetime(2026, 11, 1, 1, 10, tzinfo=ZoneInfo("America/New_York"), fold=fold)
    instance = builder.create_instance(dag, "root", when)
    store = open_store(tmp_path / "state.db")
    try:
        store.enqueue_and_advance([instance], {})
        result = store.begin_backfill("job:0", instance, recompute=True, now=NOW)
        assert result["status"] == "RUNNING"
    finally:
        store.close()
