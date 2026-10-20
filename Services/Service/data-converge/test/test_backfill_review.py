"""Independent checks around the two-database commit and live-runtime boundary."""

from dataclasses import replace
import sqlite3
import threading

import pytest

from data_converge.config.application import SchedulerSettings
from data_converge.dag import TaskStatus
from data_converge.runtime import build_scheduler
from data_converge.state import backfill
from .test_backfill_worker import setup, submit, worker
from .test_main import app


def test_replayed_queue_ack_keeps_original_query_after_later_recompute(setup, monkeypatch):
    submit(setup, task="task_0", recompute=True)
    _, store, queue = setup
    calls = []
    service = worker(setup, lambda instance: calls.append(instance.query_id))
    item = queue.next_item()
    complete = queue.complete
    monkeypatch.setattr(queue, "complete", lambda *a, **k: (_ for _ in ()).throw(OSError("full")))
    with pytest.raises(OSError):
        service.run_one(queue)
    original = store.get_backfill(item["item_id"])
    instance = service._instance(item)
    later = store.begin_backfill("later-authorized-request:0", instance, recompute=True)
    store.finish(instance.instance_id, TaskStatus.SUCCESS)
    assert original["query_id"] != later["query_id"]
    monkeypatch.setattr(queue, "complete", complete)

    service.run_one(queue)

    result = queue.status(item["job_id"])["items"][0]
    assert result["status"] == "SUCCESS" and result["query_id"] == original["query_id"]
    assert len(calls) == 1
    assert store.get_instance(instance.instance_id)["query_id"] == later["query_id"]


def test_finish_commit_failure_preserves_unknown_recovery_without_second_sql(setup, monkeypatch):
    job = submit(setup, task="task_0", recompute=True)
    _, store, queue = setup
    calls = []
    service = worker(setup, lambda instance: calls.append(instance.query_id))
    item = queue.next_item()
    original_finish = backfill.record_finish
    monkeypatch.setattr(backfill, "record_finish",
                        lambda *args: (_ for _ in ()).throw(sqlite3.OperationalError("disk full")))
    with pytest.raises(sqlite3.OperationalError):
        service.run_one(queue, tolerate_queue_errors=True)
    assert store.get_instance(item["instance_id"])["status"] == "RUNNING"
    assert store.get_backfill(item["item_id"])["status"] == "RUNNING"
    assert store.list_execution_attempts()[0]["status"] == "RUNNING"
    monkeypatch.setattr(backfill, "record_finish", original_finish)

    store.recover_running()
    service.run_one(queue)

    assert len(calls) == 1
    assert queue.status(job)["items"][0]["status"] == "UNKNOWN"
    assert store.list_pending() == []


def test_dispatch_audit_failure_rolls_back_every_main_ledger_fact(setup):
    submit(setup, task="task_0")
    _, store, queue = setup
    calls = []
    service = worker(setup, lambda instance: calls.append(instance.query_id))
    connection = store._connection(write=True)
    backfill._create_schema(connection)
    connection.execute("""CREATE TRIGGER reject_backfill BEFORE INSERT ON backfill_attempts
        BEGIN SELECT RAISE(ABORT, 'audit unavailable'); END""")
    item = queue.next_item()

    with pytest.raises(sqlite3.IntegrityError):
        service.run_one(queue, tolerate_queue_errors=True)

    assert calls == []
    assert store.get_instance(item["instance_id"]) is None
    assert store.get_backfill(item["item_id"]) is None
    assert store.list_execution_attempts() == []
    connection.execute("DROP TRIGGER reject_backfill")
    service.run_one(queue)
    assert len(calls) == 1
    assert store.get_backfill(item["item_id"])["status"] == "SUCCESS"


def test_runtime_wires_the_same_runner_and_sqlite_owner_thread(setup):
    definition, store, queue = setup
    definition = replace(definition, options=SchedulerSettings())
    submit((definition, store, queue), task="task_0")
    sequence = []
    owner = threading.get_ident()

    def runner(instance):
        sequence.append(("manual", threading.get_ident()))

    scheduler, _ = build_scheduler(definition, runner, store, None)
    scheduler.start_time = scheduler._normalize_time(None)
    scheduler.tick = lambda: sequence.append(("normal", threading.get_ident()))
    background = scheduler.background_worker
    assert background.runner is runner

    def one_window():
        background()
        scheduler.shutdown()

    scheduler.background_worker = one_window
    scheduler.start()
    assert sequence == [("normal", owner), ("manual", owner)]
