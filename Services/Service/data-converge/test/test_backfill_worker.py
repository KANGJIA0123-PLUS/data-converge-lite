"""Background requests share the live writer without replaying ambiguous work."""
from importlib.util import find_spec

import pytest

from data_converge.backfill.plan import build_plan
from data_converge.backfill.queue import Queue
from data_converge.dag import TaskStatus
from data_converge.errors import TaskExecutionError
from .test_backfill_plan import definition_for
from .test_main import app
from .test_state import open_store


@pytest.fixture
def setup(app):
    definition = definition_for(app, ("PT5M", "PT5M"), dependency=True)
    store = open_store(app.parent / "state.sqlite3")
    queue = Queue(app.parent / "backfill.sqlite3", create=True)
    try:
        yield definition, store, queue
    finally:
        queue.close()
        store.close()


def submit(setup, *, recompute=False, task=None):
    definition, _, queue = setup
    plan = build_plan(definition, "2026-10-19T10:00:00+08:00",
                      "2026-10-19T10:10:00+08:00", task=task,
                      all_tasks=task is None, recompute=recompute)
    return queue.submit(plan)


def worker(setup, runner, stop_check=lambda: False):
    assert find_spec("data_converge.backfill.worker") is not None
    from data_converge.backfill.worker import BackfillWorker
    definition, store, _ = setup
    return BackfillWorker(definition, store, runner, stop_check=stop_check)


def test_service_drains_one_window_at_a_time_and_preserves_dependency_order(setup):
    job = submit(setup)
    calls = []
    service = worker(setup, lambda instance: calls.append(instance.instance_id))
    for count in range(4):
        service.run_one(setup[2])
        assert len(calls) == count + 1
    service.run_one(setup[2])
    assert len(calls) == 4
    assert [name.split("@")[0] for name in calls] == ["task_0"] * 2 + ["task_1"] * 2
    assert setup[2].status(job)["counts"] == {"SUCCESS": 4}
    assert setup[1].get_cursor("task_0") is None


def test_queue_write_failure_after_success_does_not_reexecute_sql(setup, monkeypatch):
    job = submit(setup, task="task_0", recompute=True)
    calls = []
    service = worker(setup, lambda instance: calls.append(instance.query_id))
    queue = setup[2]
    complete = queue.complete
    monkeypatch.setattr(queue, "complete", lambda *a, **kw: (_ for _ in ()).throw(OSError("full")))
    with pytest.raises(OSError):
        service.run_one(queue)
    assert len(calls) == 1
    monkeypatch.setattr(queue, "complete", complete)
    service.run_one(queue)
    assert len(calls) == 1
    assert queue.status(job)["counts"] == {"QUEUED": 1, "SUCCESS": 1}


def test_failed_manual_attempt_is_not_normal_retry_and_blocks_same_period_child(setup):
    job = submit(setup, recompute=True)

    def fail(instance):
        raise TaskExecutionError("SQL_PREFLIGHT_FAILED", retryable=True, outcome_unknown=False)

    service = worker(setup, fail)
    for _ in range(4):
        service.run_one(setup[2])
    assert setup[2].status(job)["counts"] == {"FAILED": 2, "BLOCKED": 2}
    assert setup[1].list_pending() == []


def test_restart_after_dispatch_is_unknown_and_never_runs_again(setup):
    job = submit(setup, task="task_0")
    queue = setup[2]
    item = queue.next_item()
    definition, store, _ = setup
    from datetime import datetime
    instance = definition.builder.create_instance(
        definition.dag, item["taskname"], datetime.fromisoformat(item["scheduled_time"]))
    store.begin_backfill(item["item_id"], instance, recompute=False)
    store.recover_running()
    calls = []
    worker(setup, lambda i: calls.append(i)).run_one(queue)
    assert calls == []
    assert queue.status(job)["counts"] == {"UNKNOWN": 1, "QUEUED": 1}


def test_stop_does_not_claim_a_manual_window(setup):
    job = submit(setup)
    worker(setup, lambda i: pytest.fail("must not execute"), lambda: True).run_one(setup[2])
    assert setup[1].list_instances() == []
    assert setup[2].status(job)["counts"] == {"QUEUED": 4}


def test_different_live_snapshot_blocks_request_without_sql(setup):
    from dataclasses import replace
    from types import SimpleNamespace
    job = submit(setup)
    definition, store, queue = setup
    changed = replace(definition, options=SimpleNamespace(timezone="UTC"))
    worker((changed, store, queue), lambda i: pytest.fail("must not execute")).run_one(queue)
    first = queue.status(job)["items"][0]
    assert first["status"] == "BLOCKED"
    assert first["error"] == "BACKFILL_CONFIGURATION_CHANGED"


def test_successful_child_is_skipped_even_when_old_parent_history_is_missing(setup):
    from datetime import datetime
    definition, store, queue = setup
    job = submit(setup, task="task_1")
    item = queue.next_item()
    instance = definition.builder.create_instance(
        definition.dag, item["taskname"], datetime.fromisoformat(item["scheduled_time"]))
    store.enqueue_and_advance([instance], {})
    store.claim(instance.instance_id)
    store.finish(instance.instance_id, TaskStatus.SUCCESS)
    worker(setup, lambda i: pytest.fail("must skip")).run_one(queue)
    assert queue.status(job)["items"][0]["status"] == "SKIPPED"


def test_cross_period_failures_do_not_gate_hour_backfill(app):
    from contextlib import closing
    definition = definition_for(app, ("PT5M", "PT1H"), dependency=True)
    plan = build_plan(definition, "2026-10-19T10:00:00+08:00",
                      "2026-10-19T11:00:00+08:00", all_tasks=True)
    calls = []

    def run(instance):
        calls.append(instance.taskname)
        if instance.taskname == "task_0":
            raise TaskExecutionError("SQL_PREFLIGHT_FAILED", outcome_unknown=False)

    with closing(open_store(app.parent / "state.sqlite3")) as store:
        with Queue(app.parent / "backfill.sqlite3", create=True) as queue:
            job = queue.submit(plan)
            service = worker((definition, store, queue), run)
            for _ in range(13):
                service.run_one(queue)
            assert calls == ["task_0"] * 12 + ["task_1"]
            assert queue.status(job)["counts"] == {"FAILED": 12, "SUCCESS": 1}
