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
from data_converge.config.startup import StartupSettings
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
from data_converge.infrastructure.redis import create_connection
from data_converge.scheduler.skip import queue_resume
from data_converge.sql_execution.client import kill_queries, stop_queries
from data_converge.scheduler.orphans import OrphanQueryGuard
from data_converge.sql_execution.pool import ClickHousePool
from data_converge.sql_execution.config import resolve_credentials
from data_converge.archives.settings import ArchiveSettings
from data_converge.archives.worker import ArchiveWorker
from data_converge.backfill.worker import BackfillWorker
from data_converge.startup import wait_for_clickhouse


@dataclass(frozen=True)
class ExecutionAdapter:
    """A task runner and its startup check belong to the same connection snapshot."""

    runner: TaskRunner
    probe: Callable[[], None]
    probe_with_timeout: Callable[[float], None] | None = None


def _probe_clickhouse(config: dict, timeout: float = 10.0) -> None:
    execute_sql(config, 'SELECT 1', timeout=timeout)


def _legacy_sql_probe(runner: SQLTaskRunner, timeout: float = 10.0) -> None:
    """Preserve the original three-argument runtime's deferred SQL probe."""
    _probe_clickhouse(runner.config, timeout)


def _create_sql_execution(config_path: Path, definition: ApplicationDefinition) -> ExecutionAdapter:
    optional = {} if definition.sql_hashes is None else {'expected_sql_hashes': definition.sql_hashes}
    pool = ClickHousePool.from_mapping(definition.config.get('clickhouse_pool'))
    runner = SQLTaskRunner(config_path, definition.sql_dir,
                           timeout=definition.options.task_timeout_seconds,
                           config=definition.config, pool=pool, **optional)
    probe = partial(_probe_clickhouse, runner.config)
    return ExecutionAdapter(runner, probe, probe)


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
    redis_connection_factory: Callable[[dict], object] = create_connection

    def query_killer(self, definition: ApplicationDefinition):
        """Cancel interrupted ClickHouse queries by id during takeover."""
        def kill(query_ids):
            kill_queries(resolve_credentials(definition.config), query_ids)
        return kill

    def query_stopper(self, definition: ApplicationDefinition):
        """Synchronously cancel queries; return the ids ClickHouse still runs."""
        config = resolve_credentials(definition.config)

        def stop(query_ids):
            return stop_queries(config, query_ids, timeout=10)
        return stop


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
    maintenance: AbstractContextManager | None = field(default=None, repr=False)
    startup_settings: StartupSettings = field(default_factory=StartupSettings)
    probe_with_timeout: Callable[[float], None] | None = field(default=None, repr=False)
    _probe: Callable[[], None] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._probe = (self.startup_probe if self.startup_probe is not None
                       else partial(_legacy_sql_probe, self.runner))
        if self.probe_with_timeout is None:
            self.probe_with_timeout = (partial(_legacy_sql_probe, self.runner)
                                       if self.startup_probe is None else lambda _timeout: self._probe())

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
        if not wait_for_clickhouse(self.probe_with_timeout, self.startup_settings,
                                  lambda: self.scheduler.stop_requested, self._assert_owned):
            self._assert_owned()
            return
        self._assert_owned()
        if not self.scheduler.stop_requested:
            with self.maintenance or nullcontext():
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
                           task_parallelism=options.task_parallelism,
                           max_attempts=options.max_attempts,
                           retry_delay_seconds=options.retry_delay_seconds)
    scheduler = TaskScheduler(
        definition.dag, definition.builder, executor, timezone=options.timezone,
        state_store=store, start_time=coverage_start,
        poll_interval_seconds=options.poll_interval_seconds, catchup_limit=options.catchup_limit,
        cross_period_wait_enabled=options.cross_period_wait_enabled,
        cross_period_wait_seconds=options.cross_period_wait_seconds,
        cross_period_wait_overrides=options.cross_period_wait_overrides,
    )
    scheduler.background_worker = BackfillWorker(
        definition, store, runner, stop_check=lambda: scheduler.stop_requested)
    if options.skip_batches:
        scheduler.skip_options = options
        scheduler.skip_resume = partial(queue_resume, definition, store.path.parent)
    return scheduler, executor


def attach_orphan_guard(definition, store, scheduler, executor, dependencies) -> None:
    """Hold reruns of interrupted windows until ClickHouse confirms their old query stopped."""
    stopper = getattr(dependencies, 'query_stopper', None)
    if stopper is None:
        return
    guard = OrphanQueryGuard(store, _lazy(stopper, definition))
    executor.orphan_check = guard.reason
    scheduler.orphan_guard = guard


def _lazy(factory, definition):
    """Resolve credentials on first use, so offline paths never decrypt."""
    resolved = []

    def call(query_ids):
        if not resolved:
            resolved.append(factory(definition))
        return resolved[0](query_ids)
    return call


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
        attach_orphan_guard(definition, store, scheduler, executor, dependencies)
        if lease is not None:
            scheduler.stop_check = lambda: lease.lost
        attach_readiness(definition.config['dwd_ready'], store, coverage_start,
                         scheduler, executor, cleanup, dependencies.source_factory, lease)
        policy = ArchiveSettings.from_mapping(definition.config.get('state', {}).get('archives'))
        maintenance = ArchiveWorker(store.path, policy, lease=lease)
        runtime = ServiceRuntime(
            scheduler, runner, heartbeat, execution.probe, lease, maintenance,
            startup_settings=StartupSettings.from_mapping(definition.config.get('startup')),
            probe_with_timeout=execution.probe_with_timeout)
        yield runtime
