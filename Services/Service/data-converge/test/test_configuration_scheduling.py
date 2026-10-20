"""Task activation windows constrain every scheduling entrance."""

from contextlib import contextmanager
from datetime import datetime, timedelta

import pytest

from data_converge.dag import DAGBuilder, DAGExecutor
from data_converge.scheduler import TaskScheduler

from .test_scheduler import FakeConfigLoader, SHANGHAI, make_rules
from .test_state import open_store


COVERAGE = datetime(2020, 1, 1, tzinfo=SHANGHAI)
EFFECTIVE = datetime(2026, 10, 19, 10, tzinfo=SHANGHAI)


def save_configuration(store, starts, coverage):
    store.set_meta("binding", "old")
    store.set_meta("coverage_start", coverage.isoformat())
    store.apply_configuration(
        expected_binding="old", expected_revision=None,
        manifest={"schema_version": 1}, revision="approved", task_starts=starts,
        binding="old", migration_id="first", applied_at=EFFECTIVE)


@contextmanager
def configured_scheduler(tmp_path, starts, *, coverage=COVERAGE, rules=None):
    store = open_store(tmp_path / "state.db")
    try:
        if starts is not None:
            save_configuration(store, starts, coverage)
        builder = DAGBuilder(FakeConfigLoader(rules or make_rules()[:1]))
        dag = builder.build()
        calls = []
        executor = DAGExecutor(dag, lambda i: calls.append(i.instance_id), state_store=store)
        scheduler = TaskScheduler(dag, builder, executor, start_time=coverage, catchup_limit=2)
        scheduler.register_jobs()
        yield scheduler, store, calls
    finally:
        store.close()


@pytest.mark.parametrize("cursor", [None, COVERAGE + timedelta(minutes=1)])
def test_cron_starts_at_effective_time_without_spending_catchup_on_old_windows(tmp_path, cursor):
    with configured_scheduler(tmp_path, {"task_5min": EFFECTIVE}) as (scheduler, store, calls):
        plan = scheduler._plan_due({"task_5min": cursor}, EFFECTIVE + timedelta(minutes=11))
        assert [i.instance_id for i in plan.instances] == ["task_5min@2026-10-19T10:05:00+08:00"]
        assert plan.next_cursors == {"task_5min": EFFECTIVE + timedelta(minutes=11)}
        assert store.get_cursor("task_5min") is None
        assert store.status_counts() == {} and calls == []


def test_cron_uses_global_coverage_if_it_is_later_than_task_start(tmp_path):
    coverage = EFFECTIVE + timedelta(minutes=10)
    with configured_scheduler(tmp_path, {"task_5min": EFFECTIVE}, coverage=coverage) as (scheduler, _, _):
        plan = scheduler._plan_due({}, coverage + timedelta(minutes=6))
        assert [i.instance_id for i in plan.instances] == ["task_5min@2026-10-19T10:15:00+08:00"]


@pytest.mark.parametrize("interval, cron, now, expected_start", [
    ("P1D", "3 0 * * *", "2026-10-21T00:03:00+08:00", "2026-10-20T00:00:00+08:00"),
    ("P1M", "3 0 1 * *", "2026-12-01T00:03:00+08:00", "2026-11-01T00:00:00+08:00"),
])
def test_calendar_tasks_wait_for_a_complete_window_after_activation(tmp_path, interval, cron, now, expected_start):
    rule = dict(make_rules()[0], aggregation_interval=interval, cron=cron)
    with configured_scheduler(tmp_path, {"task_5min": EFFECTIVE}, rules=[rule]) as (scheduler, _, _):
        plan = scheduler._plan_due({}, datetime.fromisoformat(now))
        assert len(plan.instances) == 1
        assert plan.instances[0].data_start == datetime.fromisoformat(expected_start)


def test_late_dwd_events_cannot_enqueue_new_task_before_its_activation(tmp_path):
    first = make_rules()[0]
    second = dict(first, taskname="new_task", sql_id="new_task")
    starts = {"task_5min": COVERAGE, "new_task": EFFECTIVE}
    with configured_scheduler(tmp_path, starts, rules=[first, second]) as (scheduler, store, calls):
        scheduler.enqueue_ready_windows(
            [EFFECTIVE - timedelta(minutes=5), EFFECTIVE], now=EFFECTIVE + timedelta(minutes=5))
        assert {row["instance_id"] for row in store.list_pending()} == {
            "task_5min@2026-10-19T10:00:00+08:00",
            "task_5min@2026-10-19T10:05:00+08:00",
            "new_task@2026-10-19T10:05:00+08:00",
        }
        assert store.get_cursor("new_task") is None and calls == []


@pytest.mark.parametrize("starts, coverage", [
    ({"task_5min": EFFECTIVE}, COVERAGE),
    ({"task_5min": COVERAGE}, EFFECTIVE),
])
def test_manual_window_is_rejected_before_polling_or_creating_records(tmp_path, starts, coverage):
    with configured_scheduler(tmp_path, starts, coverage=coverage) as (scheduler, store, calls):
        scheduler.ready_poller = lambda now: pytest.fail("Readiness polled for an invalid window")
        with pytest.raises(ValueError, match="coverage_start|TASK_WINDOW_BEFORE_EFFECTIVE_START"):
            scheduler.run_task("task_5min", EFFECTIVE + timedelta(minutes=1))
        assert store.status_counts() == {} and calls == []


def test_manual_window_at_task_activation_is_allowed(tmp_path):
    with configured_scheduler(tmp_path, {"task_5min": EFFECTIVE}) as (scheduler, store, calls):
        report = scheduler.run_task("task_5min", EFFECTIVE + timedelta(minutes=6))
        assert len(report.succeeded) == 1
        assert calls == ["task_5min@2026-10-19T10:05:00+08:00"]
        assert store.status_counts() == {"SUCCESS": 1}


@pytest.mark.parametrize("scope", ["task", "coverage"])
def test_pending_old_window_is_retained_without_blocking_current_work(tmp_path, caplog, scope):
    with configured_scheduler(tmp_path, None) as (scheduler, store, calls):
        instance = scheduler.dag_builder.create_instance(scheduler.dag, "task_5min", EFFECTIVE)
        store.enqueue_and_advance([instance], {})
        if scope == "task":
            save_configuration(store, {"task_5min": EFFECTIVE}, COVERAGE)
        scheduler = TaskScheduler(scheduler.dag, scheduler.dag_builder, scheduler.dag_executor,
                                  start_time=EFFECTIVE if scope == "coverage" else COVERAGE)
        valid = scheduler.dag_builder.create_instance(
            scheduler.dag, "task_5min", EFFECTIVE + timedelta(minutes=6))
        store.enqueue_and_advance([valid], {})
        scheduler.run_pending(EFFECTIVE + timedelta(minutes=6))
        scheduler.run_pending(EFFECTIVE + timedelta(minutes=6))
        saved = store.get_instance(instance.instance_id)
        assert saved["status"] == "PENDING" and saved["attempts"] == 0
        assert calls == ["task_5min@2026-10-19T10:05:00+08:00"]
        assert caplog.text.count("PERSISTED_WINDOW_SKIPPED") == 1


def test_legacy_ledger_without_task_starts_retains_coverage_planning(tmp_path):
    with configured_scheduler(tmp_path, None, coverage=EFFECTIVE) as (scheduler, store, calls):
        scheduler.tick(EFFECTIVE + timedelta(minutes=6))
        assert calls == ["task_5min@2026-10-19T10:05:00+08:00"]
        assert store.status_counts() == {"SUCCESS": 1}
