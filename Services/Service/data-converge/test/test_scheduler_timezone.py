"""Scheduler progression must distinguish both occurrences of a repeated hour."""

from contextlib import closing
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from data_converge.dag import DAGBuilder, DAGExecutor
from data_converge.scheduler import TaskScheduler
from data_converge.state import StateStore

from .test_dag_timezone import local_time
from .test_scheduler import FakeConfigLoader, make_rules
from .test_system_timezone import run_in_timezone


@pytest.mark.parametrize("zone, expected", [
    ("UTC", "task@2026-10-20T00:00:00+00:00"),
    ("Asia/Tokyo", "task@2026-10-20T09:00:00+09:00"),
    ("America/New_York", "task@2026-10-19T20:00:00-04:00"),
])
def test_default_scheduler_creates_windows_in_deployment_timezone(zone, expected):
    actual = run_in_timezone(zone, """
import json
from datetime import datetime
from data_converge.dag import DAG, DAGBuilder, DAGExecutor, TaskDefinition
from data_converge.scheduler import TaskScheduler
task = TaskDefinition('task', '', ('source',), 'target', 'PT5M',
                      '1,6,11,16,21,26,31,36,41,46,51,56 * * * *', (), 'task')
dag = DAG({'task': task}, {'task': ()}, ('task',))
calls = []
executor = DAGExecutor(dag, lambda instance: calls.append(instance.instance_id))
scheduler = TaskScheduler(dag, DAGBuilder(None), executor)
scheduler.run_task('task', datetime.fromisoformat('2026-10-20T00:01:00+00:00'))
print(json.dumps(calls))
""")
    assert actual == [expected]


def timezone_scheduler(tmp_path, interval="PT1H", cron="3 * * * *"):
    store = StateStore(tmp_path / "timezone.db", create=True)
    store.acquire_lock()
    rule = make_rules()[0]
    rule.update(aggregation_interval=interval, cron=cron)
    builder = DAGBuilder(FakeConfigLoader([rule]))
    dag = builder.build()
    calls = []
    executor = DAGExecutor(dag, lambda instance: calls.append(instance.instance_id),
                           state_store=store)
    scheduler = TaskScheduler(
        dag, builder, executor, timezone="America/New_York", state_store=store,
        start_time=local_time("2026-11-01T00:00:00-04:00"))
    scheduler.register_jobs()
    return scheduler, store, calls


def test_planner_does_not_dispatch_second_hour_during_first_hour(tmp_path):
    scheduler, store, _ = timezone_scheduler(tmp_path)
    try:
        plan = scheduler._plan_due({}, local_time("2026-11-01T01:30:00-04:00"))
        assert [item.instance_id for item in plan.instances] == [
            "task_5min@2026-11-01T01:00:00-04:00"]
        assert plan.next_cursors["task_5min"].isoformat() == "2026-11-01T01:03:00-05:00"
    finally:
        store.close()


def test_persisted_cursor_advances_once_through_repeated_hour(tmp_path):
    scheduler, store, calls = timezone_scheduler(tmp_path)
    try:
        scheduler.tick(local_time("2026-11-01T01:30:00-04:00"))
        scheduler.tick(local_time("2026-11-01T01:30:00-05:00"))
        scheduler.tick(local_time("2026-11-01T02:30:00-05:00"))
        assert calls == [
            "task_5min@2026-11-01T01:00:00-04:00",
            "task_5min@2026-11-01T01:00:00-05:00",
            "task_5min@2026-11-01T02:00:00-05:00",
        ]
        assert store.get_cursor("task_5min").isoformat() == "2026-11-01T03:03:00-05:00"
    finally:
        store.close()


def test_ready_window_crosses_repeated_hour_without_waiting_an_extra_hour(tmp_path):
    scheduler, store, _ = timezone_scheduler(tmp_path, "PT5M", "1,6,11,16,21,26,31,36,41,46,51,56 * * * *")
    try:
        scheduler.enqueue_ready_windows(
            [local_time("2026-11-01T01:55:00-04:00")],
            now=local_time("2026-11-01T01:00:00-05:00"))
        records = store.list_pending()
        assert [record["instance_id"] for record in records] == [
            "task_5min@2026-11-01T01:00:00-05:00"]
        assert local_time(records[0]["data_start"]).isoformat() == "2026-11-01T01:55:00-04:00"
    finally:
        store.close()


def test_restored_pending_repeated_hour_matches_its_persisted_window(tmp_path):
    scheduler, store, calls = timezone_scheduler(tmp_path)
    instance = scheduler.dag_builder.create_instance(
        scheduler.dag, "task_5min", local_time("2026-11-01T01:03:00-05:00"))
    try:
        store.enqueue_and_advance([instance], {})
        scheduler.run_pending(local_time("2026-11-01T02:30:00-05:00"))
        assert calls == ["task_5min@2026-11-01T01:00:00-05:00"]
    finally:
        store.close()


def test_effective_start_distinguishes_the_two_repeated_hours(tmp_path):
    scheduler, store, _ = timezone_scheduler(tmp_path)
    try:
        scheduler.start_time = local_time("2026-11-01T01:00:00-05:00")
        scheduler.task_starts = {"task_5min": local_time("2026-11-01T01:30:00-04:00")}
        plan = scheduler._plan_due({}, local_time("2026-11-01T02:30:00-05:00"))
        assert [item.instance_id for item in plan.instances] == [
            "task_5min@2026-11-01T02:00:00-05:00"]
    finally:
        store.close()


def test_ready_poll_crossing_clock_rollback_advances_by_elapsed_time(tmp_path, monkeypatch):
    from data_converge.scheduler import service
    scheduler, store, _ = timezone_scheduler(tmp_path, "PT5M")
    monotonic = [100.0]
    monkeypatch.setattr(service.time, "monotonic", lambda: monotonic[0])

    def poll(now):
        monotonic[0] += 0.6
        return [local_time("2026-11-01T01:55:00-04:00")]

    scheduler.ready_poller = poll
    try:
        actual = scheduler._poll_ready(local_time("2026-11-01T01:59:59.500000-04:00"))
        assert actual.isoformat() == "2026-11-01T01:00:00.100000-05:00"
        assert [record["instance_id"] for record in store.list_pending()] == [
            "task_5min@2026-11-01T01:00:00-05:00"]
    finally:
        store.close()


@pytest.mark.parametrize("zone, start, now, expected", [
    ("America/New_York", "2026-03-07", "2026-03-10T00:30:00",
     ["2026-03-08T00:00:00-05:00", "2026-03-09T00:00:00-04:00", "2026-03-10T00:00:00-04:00"]),
    ("Europe/Berlin", "2026-03-28", "2026-03-31T00:30:00",
     ["2026-03-29T00:00:00+01:00", "2026-03-30T00:00:00+02:00", "2026-03-31T00:00:00+02:00"]),
    ("Australia/Lord_Howe", "2026-10-03", "2026-10-06T00:30:00",
     ["2026-10-04T00:00:00+10:30", "2026-10-05T00:00:00+11:00", "2026-10-06T00:00:00+11:00"]),
    ("America/Santiago", "2026-09-05", "2026-09-07T00:30:00",
     ["2026-09-06T01:00:00-03:00", "2026-09-07T00:00:00-03:00"]),
])
def test_calendar_cron_does_not_skip_days_across_clock_changes(zone, start, now, expected):
    rule = make_rules()[0]
    rule.update(aggregation_interval="P1D", cron="10 0 * * *")
    builder = DAGBuilder(FakeConfigLoader([rule]))
    dag = builder.build()
    timezone = ZoneInfo(zone)
    scheduler = TaskScheduler(
        dag, builder, DAGExecutor(dag, lambda instance: None), timezone=zone,
        start_time=datetime.fromisoformat(start).replace(tzinfo=timezone))
    scheduler.register_jobs()
    plan = scheduler._plan_due({}, datetime.fromisoformat(now).replace(tzinfo=timezone))
    assert [instance.data_end.isoformat() for instance in plan.instances] == expected


@pytest.mark.parametrize("zone, cron, start, before, after, cursor, expected", [
    ("America/Santiago", "10 0 * * *", "2026-09-05T00:00:00-04:00",
     "2026-09-05T23:59:00-04:00", "2026-09-06T01:01:00-03:00",
     "2026-09-06T01:00:00-03:00", "task_5min@2026-09-06T01:00:00-03:00"),
    ("America/New_York", "10 2 * * *", "2026-03-07T00:00:00-05:00",
     "2026-03-08T01:59:00-05:00", "2026-03-08T03:01:00-04:00",
     "2026-03-08T03:00:00-04:00", "task_5min@2026-03-08T00:00:00-05:00"),
    ("Australia/Lord_Howe", "10 2 * * *", "2026-10-03T00:00:00+10:30",
     "2026-10-04T01:59:00+10:30", "2026-10-04T02:31:00+11:00",
     "2026-10-04T02:30:00+11:00", "task_5min@2026-10-04T00:00:00+10:30"),
])
def test_persisted_calendar_cursor_keeps_shifted_missing_time(
        tmp_path, zone, cron, start, before, after, cursor, expected):
    rule = dict(make_rules()[0], aggregation_interval="P1D", cron=cron)
    builder = DAGBuilder(FakeConfigLoader([rule]))
    dag = builder.build()
    calls = []
    with closing(StateStore(tmp_path / "calendar.db", create=True)) as store:
        store.acquire_lock()
        executor = DAGExecutor(dag, lambda item: calls.append(item.instance_id), state_store=store)
        scheduler = TaskScheduler(dag, builder, executor, timezone=zone,
                                  start_time=datetime.fromisoformat(start))
        scheduler.tick(datetime.fromisoformat(before))
        assert calls == []
        assert store.get_cursor("task_5min").isoformat() == cursor
        scheduler.tick(datetime.fromisoformat(after))
        scheduler.tick(datetime.fromisoformat(after))
        assert calls == [expected]
        assert store.get_instance(expected)["status"] == "SUCCESS"


@pytest.mark.parametrize("zone, start, now", [
    ("America/New_York", "2026-03-07", "2026-03-10T00:30:00"),
    ("Australia/Lord_Howe", "2026-04-04", "2026-04-07T00:30:00"),
    ("Australia/Lord_Howe", "2026-10-03", "2026-10-06T00:30:00"),
    ("America/Santiago", "2026-09-05", "2026-09-08T00:30:00"),
])
def test_hour_schedule_tiles_calendar_days_and_creates_every_required_parent(zone, start, now):
    rules = make_rules()
    rules[0].update(aggregation_interval="PT1H", cron="6 * * * *")
    rules[1].update(aggregation_interval="P1D", cron="10 0 * * *")
    builder = DAGBuilder(FakeConfigLoader(rules))
    dag = builder.build()
    timezone = ZoneInfo(zone)
    scheduler = TaskScheduler(
        dag, builder, DAGExecutor(dag, lambda instance: None), timezone=zone,
        start_time=datetime.fromisoformat(start).replace(tzinfo=timezone))
    scheduler.register_jobs()
    plan = scheduler._plan_due({}, datetime.fromisoformat(now).replace(tzinfo=timezone))
    hours = [instance for instance in plan.instances if instance.taskname == "task_5min"]
    days = [instance for instance in plan.instances if instance.taskname == "task_hour"]
    assert len(days) == 3
    for previous, current in zip(hours, hours[1:]):
        assert previous.data_end.timestamp() == current.data_start.timestamp()
    hour_ids = {instance.instance_id for instance in hours}
    for day in days:
        assert set(day.upstream_instance_ids) <= hour_ids


@pytest.mark.parametrize("now, count, last", [
    ("2026-04-05T02:46:00+12:45", 1, "2026-04-05T02:45:00+12:45"),
    ("2026-04-05T03:41:00+12:45", 12, "2026-04-05T03:40:00+12:45"),
])
def test_chatham_rollback_keeps_all_second_occurrence_five_minute_windows(now, count, last):
    zone = ZoneInfo("Pacific/Chatham")
    start = datetime.fromisoformat("2026-04-05T03:40:00+13:45").astimezone(zone)
    builder = DAGBuilder(FakeConfigLoader(make_rules()[:1]))
    dag = builder.build()
    scheduler = TaskScheduler(
        dag, builder, DAGExecutor(dag, lambda instance: None),
        timezone=zone.key, start_time=start)
    scheduler.register_jobs()
    plan = scheduler._plan_due({}, datetime.fromisoformat(now).astimezone(zone))
    assert len(plan.instances) == count
    assert plan.instances[0].data_start.isoformat() == "2026-04-05T03:40:00+13:45"
    assert plan.instances[0].data_end.isoformat() == "2026-04-05T02:45:00+12:45"
    assert plan.instances[-1].data_end.isoformat() == last
    for previous, current in zip(plan.instances, plan.instances[1:]):
        assert previous.data_end.timestamp() == current.data_start.timestamp()
