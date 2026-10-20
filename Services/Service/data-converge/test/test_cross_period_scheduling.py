from datetime import datetime, timedelta

import pytest

from data_converge.dag import TaskStatus

from .test_scheduler import SHANGHAI, make_rules, persistent_scheduler


COVERAGE = datetime(2026, 10, 1, tzinfo=SHANGHAI)
PERIODS = [
    ("PT5M", "PT1H", datetime(2026, 10, 2, 1, 3, tzinfo=SHANGHAI)),
    ("PT1H", "P1D", datetime(2026, 10, 2, 0, 7, tzinfo=SHANGHAI)),
    ("P1D", "P1M", datetime(2026, 11, 1, 0, 11, tzinfo=SHANGHAI)),
]


def period_rules(parent_interval, child_interval):
    rules = make_rules()
    rules[0]["aggregation_interval"] = parent_interval
    rules[1]["aggregation_interval"] = child_interval
    return rules


def persist_parent_status(scheduler, store, scheduled, status):
    parent = scheduler.dag_builder.create_instance(scheduler.dag, "task_5min", scheduled)
    if status is not None:
        store.enqueue_and_advance([parent], {})
        assert store.claim(parent.instance_id, now=scheduled)
        store.finish(parent.instance_id, status, now=scheduled)
    return parent


@pytest.mark.parametrize("parent_interval, child_interval, scheduled", PERIODS)
@pytest.mark.parametrize("parent_status", [None, TaskStatus.FAILED, TaskStatus.UNKNOWN])
def test_cross_period_runs_despite_missing_or_unsuccessful_parent(
        tmp_path, parent_interval, child_interval, scheduled, parent_status):
    calls = []
    rules = period_rules(parent_interval, child_interval)
    scheduler, store = persistent_scheduler(tmp_path, calls, COVERAGE, rules=rules)
    try:
        parent = persist_parent_status(scheduler, store, scheduled, parent_status)
        report = scheduler.run_task("task_hour", scheduled)
        assert len(report.succeeded) == 1
        child_id = report.succeeded[0].instance_id
        assert calls == [child_id]
        assert store.get_instance(child_id)["attempts"] == 1
        assert store.get_status(parent.instance_id) == parent_status
    finally:
        store.close()


@pytest.mark.parametrize("parent_status", [None, TaskStatus.FAILED, TaskStatus.UNKNOWN])
def test_same_period_missing_or_unsuccessful_parent_still_blocks(tmp_path, parent_status):
    calls = []
    scheduled = datetime(2026, 10, 2, 1, 3, tzinfo=SHANGHAI)
    rules = period_rules("PT5M", "PT5M")
    scheduler, store = persistent_scheduler(tmp_path, calls, COVERAGE, rules=rules)
    try:
        persist_parent_status(scheduler, store, scheduled, parent_status)
        report = scheduler.run_task("task_hour", scheduled)
        assert len(report.blocked) == 1
        assert store.get_instance(report.blocked[0].instance_id)["attempts"] == 0
        assert calls == []
    finally:
        store.close()


@pytest.mark.parametrize("parent_interval, child_interval, scheduled", PERIODS)
def test_old_cross_period_blocked_window_resumes_after_restart(
        tmp_path, parent_interval, child_interval, scheduled):
    calls = []
    rules = period_rules(parent_interval, child_interval)
    scheduler, store = persistent_scheduler(tmp_path, calls, COVERAGE, rules=rules)
    child = scheduler.dag_builder.create_instance(scheduler.dag, "task_hour", scheduled)
    parent = scheduler.dag_builder.create_instance(scheduler.dag, "task_5min", scheduled)
    child.upstream_instance_ids = (parent.instance_id,)
    try:
        report = scheduler.dag_executor.execute([child], now=scheduled)
        assert len(report.blocked) == 1
        assert store.get_status(child.instance_id) == TaskStatus.BLOCKED
        assert store.get_instance(child.instance_id)["attempts"] == 0
        assert calls == []
    finally:
        store.close()
    scheduler, store = persistent_scheduler(tmp_path, calls, COVERAGE, create=False, rules=rules)
    try:
        report = scheduler.run_pending(now=scheduled)
        assert [instance.instance_id for instance in report.succeeded] == [child.instance_id]
        assert calls == [child.instance_id]
        assert store.get_status(child.instance_id) == TaskStatus.SUCCESS
        assert store.get_instance(child.instance_id)["attempts"] == 1
        assert store.get_instance(child.instance_id)["error"] is None
    finally:
        store.close()


def test_hour_waits_for_its_cron_but_not_blocked_five_minute_windows(tmp_path):
    coverage = datetime(2026, 10, 12, 10, 0, tzinfo=SHANGHAI)
    calls = []
    scheduler, store = persistent_scheduler(tmp_path, calls, coverage)
    scheduler.dag_executor.readiness_check = lambda instance, now: (
        "WAITING_DWD" if instance.taskname == "task_5min" else None)
    try:
        scheduler.tick(coverage + timedelta(hours=1, minutes=2))
        assert calls == []
        assert store.get_instance("task_hour@2026-10-12T11:00:00+08:00") is None
        scheduler.tick(coverage + timedelta(hours=1, minutes=3))
        assert calls == ["task_hour@2026-10-12T11:00:00+08:00"]
        assert store.status_counts() == {"BLOCKED": 12, "SUCCESS": 1}
    finally:
        store.close()


def test_late_five_minute_window_does_not_replay_successful_hour_after_restart(tmp_path):
    coverage = datetime(2026, 10, 12, 10, 0, tzinfo=SHANGHAI)
    scheduled = coverage + timedelta(hours=1, minutes=3)
    calls = []
    scheduler, store = persistent_scheduler(tmp_path, calls, coverage)
    try:
        scheduler.run_task("task_hour", scheduled)
        assert calls == ["task_hour@2026-10-12T11:00:00+08:00"]
    finally:
        store.close()
    scheduler, store = persistent_scheduler(tmp_path, calls, coverage, create=False)
    try:
        scheduler.run_task("task_5min", coverage + timedelta(hours=1, minutes=1))
        scheduler.run_task("task_hour", scheduled)
        scheduler.run_pending(now=scheduled)
        assert calls == [
            "task_hour@2026-10-12T11:00:00+08:00",
            "task_5min@2026-10-12T11:00:00+08:00",
        ]
        assert store.get_instance(calls[0])["attempts"] == 1
    finally:
        store.close()
