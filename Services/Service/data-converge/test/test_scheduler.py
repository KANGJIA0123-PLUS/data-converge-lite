from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
from apscheduler.triggers.cron import CronTrigger

from data_converge.dag import DAGBuilder, DAGExecutor, TaskStatus
from data_converge.scheduler import TaskScheduler

from .paths import RESOURCES_ROOT, SOURCE_ROOT


SHANGHAI = ZoneInfo("Asia/Shanghai")


class FakeConfigLoader:
    def __init__(self, rules):
        self.rules = rules

    def validate(self):
        return []

    def get_all_rules(self):
        return self.rules


def make_rules():
    return [
        {
            "taskname": "task_5min",
            "description": "5分钟聚合任务",
            "source_tables": ["realtime_table"],
            "target_table": "five_minute_table",
            "aggregation_interval": "PT5M",
            "cron": "1,6,11,16,21,26,31,36,41,46,51,56 * * * *",
            "depends_on": [],
            "sql_id": "task_5min",
        },
        {
            "taskname": "task_hour",
            "description": "小时聚合任务",
            "source_tables": ["five_minute_table"],
            "target_table": "hour_table",
            "aggregation_interval": "PT1H",
            "cron": "3 * * * *",
            "depends_on": ["task_5min"],
            "sql_id": "task_hour",
        },
    ]


def build_scheduler(runner):
    builder = DAGBuilder(FakeConfigLoader(make_rules()))
    dag = builder.build()
    executor = DAGExecutor(dag=dag, runner=runner)

    scheduler = TaskScheduler(
        dag=dag,
        dag_builder=builder,
        dag_executor=executor,
        timezone="Asia/Shanghai",
    )

    return scheduler


def test_registers_one_job_for_each_cron_group():
    """相同 Cron 的任务应合并到同一个调度任务组。"""
    scheduler = build_scheduler(lambda instance: None)

    scheduler.register_jobs()

    jobs = scheduler.get_jobs()

    # 测试配置中有两个不同的 Cron：
    # 5分钟任务一个，小时任务一个。
    assert len(jobs) == 2

    registered_task_groups = {
        tuple(job.kwargs["tasknames"])
        for job in jobs
    }

    assert registered_task_groups == {
        ("task_5min",),
        ("task_hour",),
    }

    for job in jobs:
        assert isinstance(job.trigger, CronTrigger)


def test_register_jobs_is_idempotent_and_returns_a_snapshot():
    scheduler = build_scheduler(lambda instance: None)
    scheduler.register_jobs()
    scheduler.register_jobs()

    jobs = scheduler.get_jobs()
    assert [job.id for job in jobs] == ["cron_group_001", "cron_group_002"]
    jobs.clear()
    assert len(scheduler.get_jobs()) == 2
    with pytest.raises(TypeError):
        scheduler.get_jobs()[0].kwargs["tasknames"] = ("different_task",)


def build_same_cron_scheduler(execution_order):
    rules = make_rules()

    # 让小时任务和5分钟任务使用相同Cron、相同窗口类型，
    # 模拟同一触发时间的上下游。
    rules[1]["aggregation_interval"] = "PT5M"
    rules[1]["cron"] = rules[0]["cron"]

    builder = DAGBuilder(FakeConfigLoader(rules))
    dag = builder.build()

    def runner(instance):
        execution_order.append(instance.taskname)

    executor = DAGExecutor(
        dag=dag,
        runner=runner,
        max_concurrent_tasks=2,
    )

    scheduler = TaskScheduler(
        dag=dag,
        dag_builder=builder,
        dag_executor=executor,
        timezone="Asia/Shanghai",
    )
    return scheduler


def test_same_cron_tasks_execute_as_one_dependency_group():
    """相同 Cron 的上下游应进入同一批次按依赖执行。"""
    execution_order = []
    scheduler = build_same_cron_scheduler(execution_order)

    scheduler.register_jobs()

    jobs = scheduler.get_jobs()

    # 两个任务Cron相同，因此只注册一个任务组。
    assert len(jobs) == 1
    assert tuple(jobs[0].kwargs["tasknames"]) == (
        "task_5min",
        "task_hour",
    )

    report = scheduler.run_group(
        tasknames=jobs[0].kwargs["tasknames"],
        scheduled_time=datetime(
            2026,
            9,
            9,
            10,
            1,
            tzinfo=SHANGHAI,
        ),
    )

    assert len(report.succeeded) == 2
    assert len(report.failed) == 0
    assert len(report.blocked) == 0

    assert execution_order == [
        "task_5min",
        "task_hour",
    ]


def test_five_minute_task_uses_aligned_data_window():
    """10:06 触发的 5 分钟任务应处理 [10:00, 10:05)。"""
    executed_instances = []

    scheduler = build_scheduler(
        lambda instance: executed_instances.append(instance)
    )

    report = scheduler.run_task(
        taskname="task_5min",
        scheduled_time=datetime(
            2026,
            9,
            9,
            10,
            6,
            tzinfo=SHANGHAI,
        ),
    )

    assert len(report.succeeded) == 1
    assert len(executed_instances) == 1

    instance = executed_instances[0]

    assert instance.data_start == datetime(
        2026,
        9,
        9,
        10,
        0,
        tzinfo=SHANGHAI,
    )
    assert instance.data_end == datetime(
        2026,
        9,
        9,
        10,
        5,
        tzinfo=SHANGHAI,
    )
    assert instance.status == TaskStatus.SUCCESS


def test_hour_task_stays_blocked_when_only_last_upstream_window_succeeded():
    """最后一个5分钟成功不足以证明整小时上游完整。"""
    execution_order = []

    def runner(instance):
        execution_order.append(instance.taskname)

    scheduler = build_scheduler(runner)

    # 02:01 触发最后一个 5 分钟窗口：
    # 数据窗口为 [01:55, 02:00)，batch_id 对应 02:00。
    parent_report = scheduler.run_task(
        taskname="task_5min",
        scheduled_time=datetime(
            2026,
            9,
            9,
            2,
            1,
            tzinfo=SHANGHAI,
        ),
    )

    # 02:03 触发小时任务：
    # 数据窗口为 [01:00, 02:00)，batch_id 同样对应 02:00。
    child_report = scheduler.run_task(
        taskname="task_hour",
        scheduled_time=datetime(
            2026,
            9,
            9,
            2,
            3,
            tzinfo=SHANGHAI,
        ),
    )

    assert len(parent_report.succeeded) == 1
    assert len(child_report.blocked) == 1

    assert execution_order == [
        "task_5min",
    ]

    parent = parent_report.succeeded[0]
    child = child_report.blocked[0]

    assert parent.batch_id == child.batch_id
    assert parent.instance_id in child.upstream_instance_ids


def test_hour_task_is_blocked_when_upstream_has_not_succeeded():
    """同批次上游尚未成功时，小时任务应被阻塞。"""
    executed_instances = []

    scheduler = build_scheduler(
        lambda instance: executed_instances.append(instance)
    )

    report = scheduler.run_task(
        taskname="task_hour",
        scheduled_time=datetime(
            2026,
            9,
            9,
            2,
            3,
            tzinfo=SHANGHAI,
        ),
    )

    assert len(report.succeeded) == 0
    assert len(report.failed) == 0
    assert len(report.blocked) == 1

    blocked = report.blocked[0]

    assert blocked.taskname == "task_hour"
    assert blocked.status == TaskStatus.BLOCKED

    # 被阻塞的任务不能真正调用 SQL runner。
    assert executed_instances == []


def test_upstream_sql_failure_blocks_hour_task(caplog):
    """上游执行失败后，同批次小时任务应被阻塞。"""
    def failing_runner(instance):
        if instance.taskname == "task_5min":
            raise RuntimeError("模拟上游 SQL 执行失败")

    scheduler = build_scheduler(failing_runner)

    parent_report = scheduler.run_task(
        taskname="task_5min",
        scheduled_time=datetime(
            2026,
            9,
            9,
            2,
            1,
            tzinfo=SHANGHAI,
        ),
    )

    child_report = scheduler.run_task(
        taskname="task_hour",
        scheduled_time=datetime(
            2026,
            9,
            9,
            2,
            3,
            tzinfo=SHANGHAI,
        ),
    )

    assert len(parent_report.unknown) == 1
    assert parent_report.unknown[0].error == "UNEXPECTED_RUNNER_ERROR:RuntimeError"
    assert "status=UNKNOWN" in caplog.text

    assert len(child_report.blocked) == 1
    assert child_report.blocked[0].status == TaskStatus.BLOCKED


def test_invalid_cron_expression_is_rejected():
    """非法 Cron 应在注册阶段明确指出对应任务。"""
    rules = make_rules()
    rules[0]["cron"] = "invalid cron"

    builder = DAGBuilder(FakeConfigLoader(rules))
    dag = builder.build()
    executor = DAGExecutor(
        dag=dag,
        runner=lambda instance: None,
    )

    scheduler = TaskScheduler(
        dag=dag,
        dag_builder=builder,
        dag_executor=executor,
        timezone="Asia/Shanghai",
    )

    with pytest.raises(
        ValueError,
        match="Cron 表达式无效.*task_5min",
    ):
        scheduler.register_jobs()


def persistent_scheduler(tmp_path, calls, coverage_start, *, create=True, catchup_limit=100):
    from data_converge.state import StateStore
    store = StateStore(tmp_path / "state.db", create=create)
    store.acquire_lock()
    builder = DAGBuilder(FakeConfigLoader(make_rules()))
    dag = builder.build()
    executor = DAGExecutor(dag, lambda i: calls.append(i.instance_id), state_store=store)
    scheduler = TaskScheduler(dag, builder, executor, state_store=store,
                              start_time=coverage_start, catchup_limit=catchup_limit,
                              timezone="Asia/Shanghai")
    return scheduler, store


def test_plan_due_requires_initialized_coverage_start():
    scheduler = build_scheduler(lambda instance: None)
    with pytest.raises(ValueError, match="coverage_start"):
        scheduler._plan_due({}, datetime(2026, 10, 12, 10, 6, tzinfo=SHANGHAI))


def test_plan_due_preserves_cursor_snapshot_without_persisting_or_executing(tmp_path):
    coverage = datetime(2026, 10, 12, 10, 3, tzinfo=SHANGHAI)
    calls = []
    scheduler, store = persistent_scheduler(tmp_path, calls, coverage, catchup_limit=2)
    scheduler.register_jobs()
    cursors = {"task_5min": None, "task_hour": None}
    try:
        plan = scheduler._plan_due(cursors, coverage + timedelta(minutes=18))
        assert [i.instance_id for i in plan.instances] == [
            "task_5min@2026-10-12T10:10:00+08:00",
        ]
        assert plan.next_cursors == {
            "task_5min": datetime(2026, 10, 12, 10, 16, tzinfo=SHANGHAI),
            "task_hour": datetime(2026, 10, 12, 11, 3, tzinfo=SHANGHAI),
        }
        assert cursors == {"task_5min": None, "task_hour": None}
        assert store.get_cursor("task_5min") is None
        assert store.status_counts() == {}
        assert calls == []
    finally:
        store.close()


def test_missed_crons_recovered_from_durable_cursor_without_replaying_success(tmp_path):
    coverage = datetime(2026, 10, 12, 10, 0, tzinfo=SHANGHAI)
    calls = []
    scheduler, store = persistent_scheduler(tmp_path, calls, coverage)
    scheduler.tick(coverage + timedelta(minutes=6))
    store.close()
    scheduler, store = persistent_scheduler(tmp_path, calls, coverage, create=False)
    try:
        scheduler.tick(coverage + timedelta(minutes=21))
        scheduler.tick(coverage + timedelta(minutes=21))
        assert calls == [
            "task_5min@2026-10-12T10:05:00+08:00",
            "task_5min@2026-10-12T10:10:00+08:00",
            "task_5min@2026-10-12T10:15:00+08:00",
            "task_5min@2026-10-12T10:20:00+08:00",
        ]
    finally:
        store.close()


def test_partial_window_before_coverage_start_is_not_executed(tmp_path):
    coverage = datetime(2026, 10, 12, 10, 3, tzinfo=SHANGHAI)
    calls = []
    scheduler, store = persistent_scheduler(tmp_path, calls, coverage)
    try:
        scheduler.tick(coverage + timedelta(minutes=8))
        assert calls == ["task_5min@2026-10-12T10:10:00+08:00"]
    finally:
        store.close()


def test_bounded_catchup_continues_without_advancing_over_unplanned_windows(tmp_path):
    coverage = datetime(2026, 10, 12, 10, 0, tzinfo=SHANGHAI)
    calls = []
    scheduler, store = persistent_scheduler(tmp_path, calls, coverage, catchup_limit=2)
    try:
        for _ in range(4):
            scheduler.tick(coverage + timedelta(minutes=21))
        assert len(calls) == 4
        assert len(set(calls)) == 4
        assert store.get_cursor("task_5min") > coverage + timedelta(minutes=21)
    finally:
        store.close()


def test_old_blocked_window_does_not_starve_new_ready_work(tmp_path):
    coverage = datetime(2026, 10, 12, 10, 0, tzinfo=SHANGHAI)
    calls = []
    scheduler, store = persistent_scheduler(tmp_path, calls, coverage, catchup_limit=1)
    try:
        missing = scheduler.dag_builder.create_instance(scheduler.dag, "task_hour", coverage + timedelta(hours=1, minutes=3))
        store.enqueue_and_advance([missing], {})
        for _ in range(4):
            scheduler.tick(coverage + timedelta(minutes=11))
        assert calls == ["task_5min@2026-10-12T10:05:00+08:00", "task_5min@2026-10-12T10:10:00+08:00"]
        assert store.get_status(missing.instance_id) == TaskStatus.BLOCKED
    finally:
        store.close()


def test_hour_waits_for_all_persisted_upstream_windows(tmp_path):
    coverage = datetime(2026, 10, 12, 10, 0, tzinfo=SHANGHAI)
    calls = []
    scheduler, store = persistent_scheduler(tmp_path, calls, coverage)
    try:
        scheduler.tick(coverage + timedelta(hours=1, minutes=3))
        assert calls[-1] == "task_hour@2026-10-12T11:00:00+08:00"
        assert len(calls) == 13
    finally:
        store.close()


def test_shutdown_finishes_inflight_sql_but_does_not_dispatch_another_wave(tmp_path):
    coverage = datetime(2026, 10, 12, 10, 0, tzinfo=SHANGHAI)
    calls = []
    scheduler, store = persistent_scheduler(tmp_path, calls, coverage)

    def stop_after_first(instance):
        calls.append(instance.instance_id)
        scheduler.shutdown()

    scheduler.dag_executor.runner = stop_after_first
    try:
        scheduler.tick(coverage + timedelta(minutes=16))
        assert calls == ["task_5min@2026-10-12T10:05:00+08:00"]
        assert store.status_counts() == {"PENDING": 2, "SUCCESS": 1}
    finally:
        store.close()


def test_signal_shutdown_cannot_deadlock_when_wait_condition_is_locked():
    """Deliver SIGTERM precisely inside Event.wait's non-reentrant lock."""
    import os
    from pathlib import Path
    import subprocess
    import sys
    import textwrap

    code = textwrap.dedent('''
        import os
        import signal
        import threading
        import time
        from datetime import datetime
        from types import SimpleNamespace
        from zoneinfo import ZoneInfo
        from data_converge.dag import DAGBuilder, DAGExecutor
        from data_converge.scheduler import TaskScheduler

        builder = DAGBuilder(SimpleNamespace(validate=lambda: [], get_all_rules=lambda: []))
        dag = builder.build()
        scheduler = TaskScheduler(dag, builder, DAGExecutor(dag, lambda _: None))
        scheduler.state_store = object()
        scheduler.start_time = datetime(2026, 10, 12, tzinfo=ZoneInfo('Asia/Shanghai'))
        scheduler.tick = lambda: None
        guard = getattr(scheduler, '_stop_event', threading.Event())
        signal.signal(signal.SIGTERM, lambda *_: scheduler.shutdown())

        def interrupted_wait(self, timeout=None):
            with self._cond:
                os.kill(os.getpid(), signal.SIGTERM)
            return True

        def interrupted_sleep(timeout):
            with guard._cond:
                os.kill(os.getpid(), signal.SIGTERM)

        threading.Event.wait = interrupted_wait
        time.sleep = interrupted_sleep
        scheduler.start()
        print('stopped')
    ''')
    env = dict(os.environ, PYTHONPATH=str(SOURCE_ROOT))
    result = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True,
                            text=True, timeout=3)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "stopped"


def test_ready_event_registers_all_fourteen_five_minute_tasks_before_cron(tmp_path):
    from pathlib import Path
    from data_converge.config import ConfigLoader
    from data_converge.state import StateStore
    builder = DAGBuilder(ConfigLoader(RESOURCES_ROOT / "config/convergence_rules.yaml", RESOURCES_ROOT / "sql"))
    dag = builder.build()
    store = StateStore(tmp_path / "state.db", create=True)
    store.acquire_lock()
    calls = []
    start = datetime(2026, 10, 12, 10, 0, tzinfo=SHANGHAI)
    executor = DAGExecutor(dag, lambda i: calls.append(i.instance_id), state_store=store)
    scheduler = TaskScheduler(dag, builder, executor, state_store=store, start_time=start,
                              ready_poller=lambda now: [start])
    try:
        scheduler.enqueue_ready_windows([start], now=start + timedelta(minutes=5))
        assert store.get_cursor("task_gpu_realtime_to_5min_dws") is None
        scheduler.tick(start + timedelta(minutes=5))
        assert len(calls) == 14
        assert all(dag.get_task(instance_id.rsplit("@", 1)[0]).aggregation_interval == "PT5M" for instance_id in calls)
        scheduler.tick(start + timedelta(minutes=6))
        assert len(calls) == 14
        assert store.status_counts() == {"SUCCESS": 14}
    finally:
        store.close()


def test_cron_and_manual_paths_wait_for_ready_and_duplicate_event_survives_restart(tmp_path):
    start = datetime(2026, 10, 12, 10, 0, tzinfo=SHANGHAI)
    calls = []
    scheduler, store = persistent_scheduler(tmp_path, calls, start)
    scheduler.dag_executor.readiness_check = lambda instance, now: (
        None if "GPU_REALTIME" in store.get_dwd_types(instance.data_start) else "WAITING_DWD")
    polls = []
    scheduler.ready_poller = lambda now: polls.append(now) or []
    scheduler.tick(start + timedelta(minutes=6))
    scheduler.run_task("task_5min", start + timedelta(minutes=6))
    assert len(polls) == 2
    assert calls == []
    assert store.status_counts() == {"BLOCKED": 1}
    store.record_dwd_signal(start, "GPU_REALTIME", start + timedelta(minutes=6))
    scheduler.ready_poller = lambda now: [start, start]
    scheduler.tick(start + timedelta(minutes=6))
    store.close()
    scheduler, store = persistent_scheduler(tmp_path, calls, start, create=False)
    scheduler.dag_executor.readiness_check = lambda instance, now: (
        None if "GPU_REALTIME" in store.get_dwd_types(instance.data_start) else "WAITING_DWD")
    scheduler.ready_poller = lambda now: [start]
    try:
        scheduler.tick(start + timedelta(minutes=11))
        assert calls == ["task_5min@2026-10-12T10:05:00+08:00"]
        assert store.status_counts() == {"BLOCKED": 1, "SUCCESS": 1}
    finally:
        store.close()


def test_ready_event_rejects_misalignment_and_excludes_partial_or_future_windows(tmp_path):
    start = datetime(2026, 10, 12, 10, 0, tzinfo=SHANGHAI)
    scheduler, store = persistent_scheduler(tmp_path, [], start)
    try:
        with pytest.raises(ValueError):
            scheduler.enqueue_ready_windows([start.replace(tzinfo=None)], now=start + timedelta(minutes=5))
        with pytest.raises(ValueError):
            scheduler.enqueue_ready_windows([start + timedelta(minutes=1)], now=start + timedelta(minutes=6))
        scheduler.enqueue_ready_windows([start - timedelta(minutes=5), start + timedelta(minutes=5)],
                                        now=start + timedelta(minutes=5))
        assert store.status_counts() == {}
    finally:
        store.close()


def test_stop_requested_during_ready_poll_prevents_manual_submission(tmp_path):
    start = datetime(2026, 10, 12, 10, 0, tzinfo=SHANGHAI)
    calls = []
    scheduler, store = persistent_scheduler(tmp_path, calls, start)

    def poll(now):
        scheduler.shutdown()
        return []

    scheduler.ready_poller = poll
    try:
        scheduler.run_task("task_5min", start + timedelta(minutes=6))
        assert calls == []
    finally:
        store.close()


@pytest.mark.parametrize("scheduled_offset", [6, 66])
def test_manual_schedule_uses_actual_clock_for_current_signals_and_readiness(tmp_path, monkeypatch, scheduled_offset):
    from data_converge.scheduler import service as scheduler_module
    start = datetime(2026, 10, 12, 10, 0, tzinfo=SHANGHAI)
    actual_now = start + timedelta(minutes=16)
    scheduler, store = persistent_scheduler(tmp_path, [], start)
    normalize = scheduler._normalize_time
    monkeypatch.setattr(scheduler, "_normalize_time", lambda value: actual_now if value is None else normalize(value))
    monkeypatch.setattr(scheduler_module.time, "monotonic", lambda: 100.0)
    polled_times = []
    checked_times = []

    def poll(now):
        polled_times.append(now)
        return [start + timedelta(minutes=10), start + timedelta(minutes=20)]

    def check(instance, now):
        checked_times.append(now)
        return "WAITING_DWD"

    scheduler.ready_poller = poll
    scheduler.dag_executor.readiness_check = check
    try:
        scheduler.run_task("task_5min", start + timedelta(minutes=scheduled_offset))
        # Historical manual execution still registers a current completed event.
        assert store.get_instance("task_5min@2026-10-12T10:15:00+08:00") is not None
        # A future manual scheduled_time must not admit a not-yet-finished event.
        assert store.get_instance("task_5min@2026-10-12T10:25:00+08:00") is None
        assert polled_times == [actual_now]
        assert checked_times == [actual_now]
    finally:
        store.close()


@pytest.mark.parametrize("mode", ["tick", "manual"])
def test_ready_poll_crossing_window_boundary_advances_planning_and_gate_clock(tmp_path, monkeypatch, mode):
    from data_converge.scheduler import service as scheduler_module
    start = datetime(2026, 10, 12, 10, 0, tzinfo=SHANGHAI)
    before_poll = start + timedelta(minutes=4, seconds=59, microseconds=500000)
    after_poll = start + timedelta(minutes=5, microseconds=100000)
    calls = []
    scheduler, store = persistent_scheduler(tmp_path, calls, start)
    monotonic = [100.0]
    monkeypatch.setattr(scheduler_module.time, "monotonic", lambda: monotonic[0])
    normalize = scheduler._normalize_time
    monkeypatch.setattr(scheduler, "_normalize_time", lambda value: before_poll if value is None else normalize(value))
    checked_times = []

    def poll(now):
        assert now == before_poll
        monotonic[0] += 0.6
        return [start]

    def check(instance, now):
        checked_times.append(now)
        return "WAITING_WINDOW_END" if instance.data_end > now else None

    scheduler.ready_poller = poll
    scheduler.dag_executor.readiness_check = check
    try:
        if mode == "tick":
            scheduler.tick(before_poll)
        else:
            scheduler.run_task("task_5min", start + timedelta(minutes=6))
        assert calls == ["task_5min@2026-10-12T10:05:00+08:00"]
        assert checked_times == [after_poll]
        assert store.status_counts() == {"SUCCESS": 1}
    finally:
        store.close()
