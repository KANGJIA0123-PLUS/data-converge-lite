"""Construct and own the resources needed to run a validated application."""

from __future__ import annotations

import signal
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from data_converge.config.application import ApplicationDefinition
from data_converge.dag import DAGExecutor, ExecutionReport
from data_converge.dwd_ready.coordinator import ReadyCoordinator
from data_converge.dwd_ready.redis_source import bind_sentinel_identity, create_source
from data_converge.health import Heartbeat
from data_converge.scheduler import TaskScheduler
from data_converge.sql_execution import SQLTaskRunner
from data_converge.sql_execution.client import execute_sql
from data_converge.state import StateStore


@dataclass
class ServiceRuntime:
    scheduler: TaskScheduler
    runner: SQLTaskRunner
    heartbeat: Heartbeat

    def run_task(self, taskname: str, scheduled_time: datetime) -> ExecutionReport:
        return self.scheduler.run_task(taskname, scheduled_time)

    def run_once(self, now: datetime | None = None) -> ExecutionReport:
        return self.scheduler.tick(now)

    def serve(self) -> None:
        execute_sql(self.runner.config, 'SELECT 1', timeout=10)
        if not self.scheduler.stop_requested:
            self.heartbeat.set_ready(True)
            self.scheduler.start()

    def request_stop(self) -> None:
        self.scheduler.shutdown(wait=False)


@contextmanager
def open_runtime(config_path: Path, definition: ApplicationDefinition, store: StateStore,
                 directory: Path, coverage_start: datetime):
    """Release resources and restore handlers after successful or partial startup."""
    with Heartbeat(directory) as heartbeat, ExitStack() as cleanup:
        runtime = None

        def stop(signum, _frame):
            # Signal handlers must not acquire locks or perform file I/O.
            heartbeat.ready = False
            if runtime is None:
                raise SystemExit(128 + signum)
            runtime.request_stop()

        for signum in (signal.SIGTERM, signal.SIGINT):
            previous = signal.signal(signum, stop)
            cleanup.callback(signal.signal, signum, previous)

        config, options = definition.config, definition.options
        runner = SQLTaskRunner(config_path, definition.sql_dir,
                               timeout=options.default_timeout_seconds, config=config)
        executor = DAGExecutor(definition.dag, runner, state_store=store,
                               max_concurrent_tasks=options.max_concurrent_tasks,
                               max_attempts=options.max_attempts,
                               retry_delay_seconds=options.retry_delay_seconds)
        scheduler = TaskScheduler(
            definition.dag, definition.builder, executor, timezone=options.timezone,
            state_store=store, start_time=coverage_start,
            poll_interval_seconds=options.poll_interval_seconds, catchup_limit=options.catchup_limit,
        )
        if config['dwd_ready']['enabled']:
            ready = config['dwd_ready']
            source = create_source(ready)
            cleanup.callback(source.close)
            bind_sentinel_identity(store, getattr(source, 'identity', None))
            coordinator = ReadyCoordinator(
                store, source, coverage_start, batch_size=ready['batch_size'],
                wait_warning_seconds=ready['wait_warning_seconds'],
                should_stop=lambda: scheduler.stop_requested,
            )
            executor.readiness_check = coordinator.reason
            scheduler.ready_poller = coordinator.poll
        runtime = ServiceRuntime(scheduler, runner, heartbeat)
        yield runtime
