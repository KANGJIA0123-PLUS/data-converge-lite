"""Independent early writes cannot become or bypass formal completion."""
from contextlib import closing
from datetime import timedelta
import sqlite3

import pytest

from data_converge.dag import TaskStatus
from data_converge.state import StateStore
from .test_state import NOW, graph, open_store


def candidate(item_id="early:0"):
    builder, dag = graph()
    instance = builder.create_instance(dag, "root", NOW + timedelta(minutes=5))
    item = {"item_id": item_id, "revision": "request-revision",
            "formal_scheduled_time": instance.scheduled_time.isoformat(),
            "config_evidence": {"task": {"revision": "task-revision"},
                                "connection": {}, "dataset_id": None,
                                "timezone": "Asia/Shanghai", "cleanup_column": "window_time"}}
    return item, instance


def test_early_success_is_separate_and_replayed_receipt_keeps_query(tmp_path):
    item, instance = candidate()
    with closing(open_store(tmp_path / "state.db")) as store:
        assert hasattr(store, "begin_preaggregate")
        first = store.begin_preaggregate(item, instance, now=NOW)
        assert first["status"] == "RUNNING"
        assert first["query_id"]
        assert store.get_status(instance.instance_id) is None
        store.finish_preaggregate(item["item_id"], TaskStatus.SUCCESS, now=NOW)
        repeated = store.begin_preaggregate(item, instance, now=NOW)
        assert repeated["status"] == "SUCCESS"
        assert repeated["query_id"] == first["query_id"]
        assert store.get_cursor("root") is None
        assert store.list_execution_attempts() == []
        store.enqueue_and_advance([instance], {})
        assert store.claim(instance.instance_id, now=instance.data_end)
        store.finish(instance.instance_id, TaskStatus.SUCCESS, now=instance.data_end)
        assert store.get_preaggregate(item["item_id"])["covered_at"] is not None


def test_recovery_preserves_unknown_without_replaying_or_formal_row(tmp_path):
    item, instance = candidate()
    path = tmp_path / "state.db"
    with closing(open_store(path)) as store:
        assert hasattr(store, "begin_preaggregate")
        first = store.begin_preaggregate(item, instance, now=NOW)
    with closing(open_store(path, create=False)) as store:
        store.recover_running()
        record = store.get_preaggregate(item["item_id"])
        assert record["status"] == "UNKNOWN"
        assert record["error"] == "EXECUTION_INTERRUPTED"
        assert store.begin_preaggregate(item, instance, now=NOW)["query_id"] == first["query_id"]
        assert store.get_status(instance.instance_id) is None
        assert store.preaggregate_blocker(instance.instance_id)
        assert store.list_preaggregates(only_unresolved=True) == [record]


@pytest.mark.parametrize("status", [TaskStatus.RUNNING, TaskStatus.UNKNOWN])
def test_unresolved_early_write_blocks_every_formal_write_entry(tmp_path, status):
    item, instance = candidate()
    with closing(open_store(tmp_path / "state.db")) as store:
        assert hasattr(store, "begin_preaggregate")
        store.begin_preaggregate(item, instance, now=NOW)
        if status == TaskStatus.UNKNOWN:
            store.finish_preaggregate(item["item_id"], status, now=NOW)
        store.enqueue_and_advance([instance], {})
        assert not store.claim(instance.instance_id, now=NOW)
        assert not store.prepare_attempt(instance.instance_id, now=NOW)
        assert store.begin_backfill("formal:0", instance, recompute=True)["status"] == "BLOCKED"
        with pytest.raises(ValueError, match="PREAGGREGATE"):
            store.resolve(instance.instance_id, "SUCCESS", "formal cannot skip early unknown")
        assert store.get_status(instance.instance_id) == TaskStatus.PENDING


@pytest.mark.parametrize("resolution,effective", [("success", "SUCCESS"), ("retry", "FAILED")])
def test_resolution_retains_unknown_and_releases_only_the_barrier(tmp_path, resolution, effective):
    item, instance = candidate()
    with closing(open_store(tmp_path / "state.db")) as store:
        assert hasattr(store, "begin_preaggregate")
        store.begin_preaggregate(item, instance, now=NOW)
        store.finish_preaggregate(item["item_id"], TaskStatus.UNKNOWN, error="lost response", now=NOW)
        store.resolve_preaggregate(item["item_id"], resolution, "Checked cleanup and query ended")
        record = store.latest_preaggregate(instance.instance_id)
        assert record["status"] == "UNKNOWN"
        assert record["effective_status"] == effective
        assert record["resolution"] == resolution
        assert record["error"] == "lost response"
        assert store.preaggregate_blocker(instance.instance_id) is None
        assert store.get_status(instance.instance_id) is None
        assert store.list_preaggregates(only_unresolved=True) == []
        with pytest.raises(ValueError):
            store.resolve_preaggregate(item["item_id"], resolution, "same evidence")


def test_latest_attempt_does_not_skip_newer_failed_or_expired_item(tmp_path):
    item, instance = candidate()
    with closing(open_store(tmp_path / "state.db")) as store:
        assert hasattr(store, "begin_preaggregate")
        store.begin_preaggregate(item, instance, now=NOW)
        store.finish_preaggregate(item["item_id"], TaskStatus.SUCCESS, now=NOW)
        later = item | {"item_id": "early:1"}
        store.begin_preaggregate(later, instance, now=NOW)
        store.finish_preaggregate(later["item_id"], TaskStatus.FAILED, now=NOW)
        assert store.latest_preaggregate(instance.instance_id)["status"] == "FAILED"
        expired = item | {"item_id": "early:2"}
        record = store.begin_preaggregate(expired, instance, now=instance.data_end)
        assert record["status"] == "SKIPPED"
        assert record["query_id"] is None
        assert store.latest_preaggregate(instance.instance_id)["status"] == "SKIPPED"
        assert len(store.list_preaggregates(taskname="root", limit=1, offset=1)) == 1
        assert store.list_preaggregates(taskname="missing") == []


def test_old_ledger_reads_are_side_effect_free(tmp_path):
    path = tmp_path / "state.db"
    open_store(path).close()
    before = path.read_bytes()
    with closing(StateStore(path)) as store:
        assert hasattr(store, "list_preaggregates")
        assert store.list_preaggregates() == []
        assert store.latest_preaggregate("missing") is None
        assert store.get_preaggregate("missing") is None
        assert store.preaggregate_blocker("missing") is None
    assert path.read_bytes() == before
    with sqlite3.connect(path) as conn:
        assert not conn.execute("SELECT name FROM sqlite_master WHERE name='preaggregate_attempts'").fetchall()


def test_problem_filter_excludes_all_reviewed_unknowns(tmp_path):
    item, instance = candidate()
    with closing(open_store(tmp_path / "state.db")) as store:
        store.begin_preaggregate(item, instance, now=NOW)
        store.finish_preaggregate(item["item_id"], TaskStatus.UNKNOWN, now=NOW)
        store.resolve_preaggregate(item["item_id"], "success", "Verified completion")
        retry = item | {"item_id": "retry:0"}
        store.begin_preaggregate(retry, instance, now=NOW)
        store.finish_preaggregate(retry["item_id"], TaskStatus.UNKNOWN, now=NOW)
        store.resolve_preaggregate(retry["item_id"], "retry", "Verified stopped")
        assert store.list_preaggregates(problems_only=True) == []


def test_current_problems_exclude_reviewed_or_formally_covered_attempts(tmp_path):
    item, instance = candidate()
    with closing(open_store(tmp_path / "state.db")) as store:
        store.begin_preaggregate(item, instance, now=NOW)
        store.finish_preaggregate(item["item_id"], TaskStatus.UNKNOWN, now=NOW)
        store.resolve_preaggregate(item["item_id"], "retry", "Verified query ended")
        assert store.list_preaggregates(problems_only=True) == []
        failed = item | {"item_id": "early:1"}
        store.begin_preaggregate(failed, instance, now=NOW)
        store.finish_preaggregate(failed["item_id"], TaskStatus.FAILED, now=NOW)
        assert len(store.list_preaggregates(problems_only=True)) == 1
        store.enqueue_and_advance([instance], {})
        store.claim(instance.instance_id, now=instance.data_end)
        store.finish(instance.instance_id, TaskStatus.SUCCESS, now=instance.data_end)
        assert store.list_preaggregates(problems_only=True) == []


@pytest.mark.parametrize("formal", [TaskStatus.PENDING, TaskStatus.BLOCKED, TaskStatus.FAILED])
def test_preaggregate_cannot_overwrite_any_existing_formal_record(tmp_path, formal):
    item, instance = candidate()
    with closing(open_store(tmp_path / "state.db")) as store:
        store.enqueue_and_advance([instance], {})
        if formal == TaskStatus.BLOCKED:
            store.mark_blocked(instance.instance_id, "WAITING_DEPENDENCY")
        elif formal == TaskStatus.FAILED:
            store.claim(instance.instance_id, now=instance.data_end)
            store.finish(instance.instance_id, formal, now=NOW)
        previous = store.get_instance(instance.instance_id)
        result = store.begin_preaggregate(item, instance, now=NOW)
        assert result["status"] == "BLOCKED"
        assert result["query_id"] is None
        assert store.get_instance(instance.instance_id) == previous


def test_latest_receipt_uses_dispatch_sequence_when_wall_clock_moves_back(tmp_path):
    item, instance = candidate()
    with closing(open_store(tmp_path / "state.db")) as store:
        store.begin_preaggregate(item, instance, now=NOW + timedelta(seconds=30))
        store.finish_preaggregate(item["item_id"], TaskStatus.SUCCESS, now=NOW + timedelta(seconds=31))
        later = item | {"item_id": "early:1"}
        store.begin_preaggregate(later, instance, now=NOW)
        store.finish_preaggregate(later["item_id"], TaskStatus.FAILED, now=NOW)
        assert store.latest_preaggregate(instance.instance_id)["item_id"] == "early:1"
