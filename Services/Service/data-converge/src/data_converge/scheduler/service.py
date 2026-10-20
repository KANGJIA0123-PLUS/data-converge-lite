"""Schedule DAG task groups from Cron expressions."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Callable, Iterable, Mapping
from zoneinfo import ZoneInfo

from apscheduler.triggers.cron import CronTrigger

from data_converge.config.cron import CronGroup, compile_cron_groups
from data_converge.dag import (
    DAG,
    DAGBuilder,
    DAGExecutor,
    ExecutionReport,
)
from data_converge.dag.builder import shift_elapsed, timeline_time
from data_converge.time_policy import system_timezone
from .cron import next_fire_time


logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from data_converge.dag import TaskInstance
    from data_converge.state import StateStore


@dataclass(frozen=True)
class SchedulePlan:
    instances: tuple[TaskInstance, ...]
    next_cursors: Mapping[str, datetime]


class TaskScheduler:
    """Group tasks by Cron and execute each due group as one DAG batch."""

    def __init__(
        self,
        dag: DAG,
        dag_builder: DAGBuilder,
        dag_executor: DAGExecutor,
        timezone: str | None = None,
        misfire_grace_time: int = 120,
        *,
        state_store: StateStore | None = None,
        start_time: datetime | None = None,
        poll_interval_seconds: float = 5,
        catchup_limit: int = 100,
        ready_poller: Callable[[datetime], Iterable[datetime]] | None = None,
    ):
        self.dag = dag
        self.dag_builder = dag_builder
        self.dag_executor = dag_executor
        self.timezone = ZoneInfo(timezone) if timezone is not None else system_timezone()
        self.misfire_grace_time = misfire_grace_time
        self.state_store = state_store or dag_executor.state_store
        if self.state_store is not dag_executor.state_store:
            raise ValueError("Scheduler and executor must share the same StateStore")
        if poll_interval_seconds <= 0 or catchup_limit <= 0:
            raise ValueError("poll_interval_seconds and catchup_limit must be positive")
        if start_time is None and self.state_store:
            stored_start = self.state_store.get_meta("coverage_start")
            start_time = datetime.fromisoformat(stored_start) if stored_start else None
        if start_time is not None and start_time.tzinfo is None:
            raise ValueError("coverage_start must include a timezone")
        self.start_time = start_time.astimezone(self.timezone) if start_time else None
        self.task_starts = self.state_store.get_task_effective_starts() if self.state_store else {}
        self.poll_interval_seconds = poll_interval_seconds
        self.catchup_limit = catchup_limit
        self.ready_poller = ready_poller
        self._stop_requested = False
        self.stop_check: Callable[[], bool] | None = None
        self._triggers: dict[str, CronTrigger] = {}
        self._wall_triggers: dict[str, CronTrigger] = {}
        self._jobs: tuple[CronGroup, ...] = ()
        self._registered = False

    def register_jobs(self) -> None:
        """Compile cron metadata for inspection and durable cursor planning."""
        jobs = compile_cron_groups(self.dag, self.timezone)
        self._triggers = {
            taskname: job.trigger
            for job in jobs
            for taskname in job.tasknames
        }
        self._jobs = jobs
        self._wall_triggers = {
            taskname: CronTrigger.from_crontab(self.dag.get_task(taskname).cron, timezone="UTC")
            for taskname in self._triggers
        }
        for job in jobs:
            logger.info(
                "已注册Cron任务组：job_id=%s, name=%s, "
                "task_count=%s, tasks=%s",
                job.id,
                job.name,
                len(job.tasknames),
                job.tasknames,
            )
        self._registered = True

    def run_group(
        self,
        tasknames: Iterable[str],
        scheduled_time: datetime | None = None,
    ) -> ExecutionReport:
        """Create and execute all instances in one Cron group."""
        normalized_time = self._normalize_time(
            scheduled_time
        )
        instances = [
            self.dag_builder.create_instance(
                dag=self.dag,
                taskname=taskname,
                scheduled_time=normalized_time,
            )
            for taskname in tasknames
        ]
        for instance in instances:
            self._validate_window_start(instance)
        actual_now = self._poll_ready(self._normalize_time(None))

        logger.info(
            "开始执行Cron任务组：scheduled_time=%s, "
            "task_count=%s, tasks=%s",
            normalized_time,
            len(instances),
            [instance.taskname for instance in instances],
        )

        report = self.dag_executor.execute(instances, now=actual_now,
                                           should_stop=lambda: self.stop_requested)
        self._log_group_result(normalized_time, report)
        return report

    def _effective_start(self, taskname: str) -> datetime | None:
        task_start = self.task_starts.get(taskname)
        if self.start_time is None:
            return task_start
        return (max(self.start_time, task_start, key=timeline_time)
                if task_start is not None else self.start_time)

    def _validate_window_start(self, instance: TaskInstance) -> None:
        if self.start_time and timeline_time(instance.data_start) < timeline_time(self.start_time):
            raise ValueError("Requested window begins before the initialized coverage_start")
        task_start = self.task_starts.get(instance.taskname)
        if task_start and timeline_time(instance.data_start) < timeline_time(task_start):
            raise ValueError("TASK_WINDOW_BEFORE_EFFECTIVE_START")

    def _log_group_result(self, scheduled_time: datetime, report: ExecutionReport) -> None:
        logger.info(
            "Cron任务组执行结束：scheduled_time=%s, "
            "success=%s, failed=%s, blocked=%s, unknown=%s",
            scheduled_time,
            [i.taskname for i in report.succeeded],
            [i.taskname for i in report.failed],
            [i.taskname for i in report.blocked],
            [i.taskname for i in report.unknown],
        )
        self._log_execution_errors(report)
        for instance in report.blocked:
            logger.warning(
                "任务执行阻塞：taskname=%s, "
                "instance_id=%s, reason=%s",
                instance.taskname,
                instance.instance_id,
                instance.error,
            )

    def run_task(
        self,
        taskname: str,
        scheduled_time: datetime | None = None,
    ) -> ExecutionReport:
        """Manually execute one task.

        This method is retained for tests and manual debugging.
        """
        return self.run_group(
            tasknames=[taskname],
            scheduled_time=scheduled_time,
        )

    def _normalize_time(
        self,
        scheduled_time: datetime | None,
    ) -> datetime:
        if scheduled_time is None:
            return datetime.now(self.timezone)

        if scheduled_time.tzinfo is None:
            return scheduled_time.replace(
                tzinfo=self.timezone
            )

        return scheduled_time.astimezone(
            self.timezone
        )

    def start(self) -> None:
        """Plan every due cron occurrence from the durable cursor, then execute."""
        if self.state_store is None or self.start_time is None:
            raise ValueError("Persistent state and initialized coverage_start are required; use --init-state")
        if not self._registered:
            self.register_jobs()

        logger.info(
            "DAG定时调度器启动：timezone=%s, "
            "cron_group_count=%s",
            self.timezone.key,
            len(self._jobs),
        )

        while not self.stop_requested:
            self.tick()
            deadline = time.monotonic() + self.poll_interval_seconds
            while not self.stop_requested:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                time.sleep(min(0.2, remaining))

    def tick(self, now: datetime | None = None) -> ExecutionReport:
        if self.state_store is None or self.start_time is None:
            raise ValueError("Persistent state and initialized coverage_start are required; use --init-state")
        if not self._registered:
            self.register_jobs()
        now = self._normalize_time(now)
        now = self._poll_ready(now)
        cursors = {
            taskname: self.state_store.get_cursor(taskname)
            for taskname in self.dag.topological_order
        }
        plan = self._plan_due(cursors, now)
        self.state_store.enqueue_and_advance(plan.instances, plan.next_cursors)
        return self.run_pending(now=now)

    def _plan_due(self, cursors: Mapping[str, datetime | None], now: datetime) -> SchedulePlan:
        """Calculate complete due windows without changing state or executing work."""
        if self.start_time is None:
            raise ValueError("Initialized coverage_start is required; use --init-state")
        instances = []
        next_cursors = {}
        for taskname in self.dag.topological_order:
            trigger = self._triggers[taskname]
            wall_trigger = self._wall_triggers[taskname]
            calendar = self.dag.get_task(taskname).aggregation_interval in ("P1D", "P1M")
            effective_start = self._effective_start(taskname)
            cursor = cursors.get(taskname)
            if cursor is not None:
                cursor = cursor.astimezone(self.timezone)
            if cursor is None or timeline_time(cursor) < timeline_time(effective_start):
                cursor = next_fire_time(
                    trigger, wall_trigger, effective_start, move_missing=calendar)
            count = 0
            while (cursor is not None and timeline_time(cursor) <= timeline_time(now)
                   and count < self.catchup_limit):
                instance = self.dag_builder.create_instance(self.dag, taskname, cursor.astimezone(self.timezone))
                # First deployment starts only with whole new windows. It never
                # silently replays the partial hour/day/month before the cutover.
                if timeline_time(instance.data_start) >= timeline_time(effective_start):
                    instances.append(instance)
                count += 1
                cursor = next_fire_time(
                    trigger, wall_trigger, shift_elapsed(cursor, timedelta(microseconds=1)),
                    previous=cursor, move_missing=calendar)
            if cursor is not None:
                next_cursors[taskname] = cursor
        return SchedulePlan(tuple(instances), next_cursors)

    def _poll_ready(self, now: datetime) -> datetime:
        if self.ready_poller is not None and not self.stop_requested:
            started = time.monotonic()
            windows = list(self.ready_poller(now))
            # Blocking Redis reads may cross a window boundary. Advance the
            # supplied clock, including an explicit --now, by elapsed time.
            now = shift_elapsed(now, timedelta(seconds=max(0.0, time.monotonic() - started)))
            self.enqueue_ready_windows(windows, now=now)
        return now

    def enqueue_ready_windows(self, window_starts: Iterable[datetime], *,
                              now: datetime | None = None) -> None:
        if self.state_store is None or self.start_time is None:
            raise ValueError("Ready events require persistent state and initialized coverage_start")
        now = self._normalize_time(now)
        instances = []
        for start in window_starts:
            if start.tzinfo is None or start.utcoffset() is None:
                raise ValueError("DWD window_start must include a timezone")
            start = start.astimezone(self.timezone)
            if start.minute % 5 or start.second or start.microsecond:
                raise ValueError("DWD window_start must align to a five-minute boundary")
            end = shift_elapsed(start, timedelta(minutes=5))
            if (timeline_time(start) < timeline_time(self.start_time)
                    or timeline_time(end) > timeline_time(now)):
                continue
            for taskname in self.dag.topological_order:
                if (self.dag.tasks[taskname].aggregation_interval == "PT5M"
                        and timeline_time(start) >= timeline_time(self._effective_start(taskname))):
                    instances.append(self.dag_builder.create_instance(self.dag, taskname, end))
        # Events may precede cron by a minute, but share the same window identity.
        # Never advance cron cursors here: they still enumerate missing windows.
        self.state_store.enqueue_and_advance(instances, {})

    def run_pending(self, now: datetime | None = None) -> ExecutionReport:
        if self.state_store is None:
            raise ValueError("run_pending requires persistent state")
        now = self._normalize_time(now)
        instances = []
        for record in self.state_store.list_pending():
            scheduled = self._normalize_time(datetime.fromisoformat(record["scheduled_time"]))
            instance = self.dag_builder.create_instance(self.dag, record["taskname"], scheduled)
            if (instance.instance_id != record["instance_id"]
                    or timeline_time(instance.data_start)
                    != timeline_time(datetime.fromisoformat(record["data_start"]))
                    or timeline_time(instance.data_end)
                    != timeline_time(datetime.fromisoformat(record["data_end"]))):
                raise ValueError(f"Persisted window no longer matches task definition: {record['instance_id']}")
            self._validate_window_start(instance)
            instances.append(instance)
        # Read all pending rows, but bound actual submissions. Old BLOCKED rows
        # cannot monopolize a LIMIT query and starve unrelated ready windows.
        report = self.dag_executor.execute(instances, now=now, max_executions=self.catchup_limit,
                                           should_stop=lambda: self.stop_requested)
        self._log_execution_errors(report)
        return report

    @staticmethod
    def _log_execution_errors(report: ExecutionReport) -> None:
        for instance in report.failed + report.unknown:
            logger.error("任务执行结果：instance=%s status=%s query_id=%s error=%s",
                         instance.instance_id, instance.status.value, instance.query_id, instance.error)

    @property
    def stop_requested(self) -> bool:
        """Expose shutdown state without sharing the scheduler's internal storage."""
        return self._stop_requested or (self.stop_check is not None and self.stop_check())

    def shutdown(self, wait: bool = True) -> None:
        """Signal-safe request: no locks, logging or I/O in this call."""
        self._stop_requested = True

    def get_jobs(self) -> list[CronGroup]:
        return list(self._jobs)
