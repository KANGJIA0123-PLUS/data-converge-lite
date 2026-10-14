"""Construct and own the resources needed to run a validated application."""

from __future__ import annotations

import signal
from contextlib import AbstractContextManager, ExitStack, contextmanager, nullcontext
from dataclasses import dataclass, field
from datetime import datetime
from functools import partial
from pathlib import Path
from typing import Callable

from data_converge.config.application import ApplicationDefinition
from data_converge.dag import DAGExecutor, ExecutionReport
from data_converge.dag.ports import TaskRunner
from data_converge.dwd_ready.coordinator import ReadyCoordinator
from data_converge.dwd_ready.ports import ReadySignalSource
from data_converge.dwd_ready.redis_source import bind_sentinel_identity, create_source
from data_converge.health import Heartbeat, heartbeat_directory
from data_converge.scheduler import TaskScheduler
from data_converge.sql_execution import SQLTaskRunner
from data_converge.sql_execution.client import execute_sql
from data_converge.state import StateStore
from data_converge.coordination import create_leadership, GuardedRunner, GuardedSignalSource, WriterLease


@dataclass(frozen=True)
class ExecutionAdapter:
    """A task runner and its startup check belong to the same connection snapshot."""

    runner: TaskRunner
    probe: Callable[[], None]


def _probe_clickhouse(config: dict) -> None:
    execute_sql(config, 'SELECT 1', timeout=10)


def _legacy_sql_probe(runner: SQLTaskRunner) -> None:
    """Preserve the original three-argument runtime's deferred SQL probe."""
    _probe_clickhouse(runner.config)


def _create_sql_execution(config_path: Path, definition: ApplicationDefinition) -> ExecutionAdapter:
    runner = SQLTaskRunner(config_path, definition.sql_dir,
                           timeout=definition.options.default_timeout_seconds,
                           config=definition.config)
    return ExecutionAdapter(runner, partial(_probe_clickhouse, runner.config))


def _create_ready_source(config: dict) -> ReadySignalSource:
    return create_source(config)


def _create_heartbeat(directory: Path) -> AbstractContextManager[Heartbeat]:
    return Heartbeat(directory)


@dataclass(frozen=True)
class RuntimeDependencies:
    """Explicit adapter factories; defaults are resolved when a runtime opens."""

    execution_factory: Callable[[Path, ApplicationDefinition], ExecutionAdapter] = _create_sql_execution
    source_factory: Callable[[dict], ReadySignalSource] = _create_ready_source
    heartbeat_factory: Callable[[Path], AbstractContextManager[Heartbeat]] = _create_heartbeat
    leadership_factory: Callable[[ApplicationDefinition], AbstractContextManager[WriterLease]] = create_leadership


@contextmanager
def service_heartbeat(definition, directory, dependencies):
    if not definition.leadership.enabled:
        yield None
        return
    directory = heartbeat_directory(directory, definition.config)
    directory.mkdir(parents=True, exist_ok=True)
    with dependencies.heartbeat_factory(directory) as heartbeat:
        yield heartbeat


@dataclass
class ServiceRuntime:
    scheduler: TaskScheduler
    runner: TaskRunner
    heartbeat: Heartbeat
    startup_probe: Callable[[], None] | None = field(default=None, repr=False)
    lease: WriterLease | None = field(default=None, repr=False)
    _probe: Callable[[], None] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._probe = (self.startup_probe if self.startup_probe is not None
                       else partial(_legacy_sql_probe, self.runner))

    def run_task(self, taskname: str, scheduled_time: datetime) -> ExecutionReport:
        self._assert_owned()
        result = self.scheduler.run_task(taskname, scheduled_time)
        self._assert_owned()
        return result

    def run_once(self, now: datetime | None = None) -> ExecutionReport:
        self._assert_owned()
        result = self.scheduler.tick(now)
        self._assert_owned()
        return result

    def serve(self) -> None:
        self._assert_owned()
        self._probe()
        self._assert_owned()
        if not self.scheduler.stop_requested:
            self.heartbeat.set_ready(True)
            self.scheduler.start()
        self._assert_owned()

    def _assert_owned(self) -> None:
        if self.lease is not None:
            self.lease.assert_owned()

    def request_stop(self) -> None:
        self.scheduler.shutdown(wait=False)


def build_scheduler(definition: ApplicationDefinition, runner: TaskRunner,
                    store: StateStore, coverage_start: datetime) -> tuple[TaskScheduler, DAGExecutor]:
    """Assemble task execution and scheduling from one validated configuration."""
    options = definition.options
    executor = DAGExecutor(definition.dag, runner, state_store=store,
                           max_concurrent_tasks=options.max_concurrent_tasks,
                           max_attempts=options.max_attempts,
                           retry_delay_seconds=options.retry_delay_seconds)
    scheduler = TaskScheduler(
        definition.dag, definition.builder, executor, timezone=options.timezone,
        state_store=store, start_time=coverage_start,
        poll_interval_seconds=options.poll_interval_seconds, catchup_limit=options.catchup_limit,
    )
    return scheduler, executor


def attach_readiness(ready_config: dict, store: StateStore, coverage_start: datetime,
                     scheduler: TaskScheduler, executor: DAGExecutor, cleanup: ExitStack,
                     source_factory: Callable[[dict], ReadySignalSource] = _create_ready_source,
                     lease=None) -> None:
    """Attach the readiness adapter; register cleanup before binding can fail."""
    if not ready_config['enabled']:
        return
    source = source_factory(ready_config)
    cleanup.callback(source.close)
    bind_sentinel_identity(store, getattr(source, 'identity', None))
    if lease is not None:
        source = GuardedSignalSource(source, lease)
    coordinator = ReadyCoordinator(
        store, source, coverage_start, batch_size=ready_config['batch_size'],
        wait_warning_seconds=ready_config['wait_warning_seconds'],
        should_stop=lambda: scheduler.stop_requested,
    )
    executor.readiness_check = coordinator.reason
    scheduler.ready_poller = coordinator.poll


@contextmanager
def open_runtime(config_path: Path, definition: ApplicationDefinition, store: StateStore,
                 directory: Path, coverage_start: datetime, *,
                 dependencies: RuntimeDependencies | None = None, lease=None, heartbeat=None):
    """Release resources and restore handlers after successful or partial startup."""
    dependencies = dependencies if dependencies is not None else RuntimeDependencies()
    directory = heartbeat_directory(directory, definition.config)
    if definition.leadership.enabled:
        directory.mkdir(parents=True, exist_ok=True)
    owned_heartbeat = nullcontext(heartbeat) if heartbeat is not None else dependencies.heartbeat_factory(directory)
    with owned_heartbeat as heartbeat, ExitStack() as cleanup:
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

        execution = dependencies.execution_factory(config_path, definition)
        runner = GuardedRunner(execution.runner, lease) if lease is not None else execution.runner
        scheduler, executor = build_scheduler(definition, runner, store, coverage_start)
        if lease is not None:
            scheduler.stop_check = lambda: lease.lost
            heartbeat.readiness_check = lambda: not lease.lost
        attach_readiness(definition.config['dwd_ready'], store, coverage_start,
                         scheduler, executor, cleanup, dependencies.source_factory, lease)
        runtime = ServiceRuntime(scheduler, runner, heartbeat, execution.probe, lease)
        yield runtime
