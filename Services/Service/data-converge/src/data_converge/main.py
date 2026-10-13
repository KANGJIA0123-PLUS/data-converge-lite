"""Company-layout service entry point, with explicit local state initialization."""
from __future__ import annotations

import argparse
import json
import logging
import os
import signal
from contextlib import ExitStack, closing, contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from data_converge.config.application import (
    DEFAULT_APP_CONFIG_PATH,
    ApplicationDefinition,
    StartupError,
    binding,
    load_app_config,
    load_definition,
    state_directory,
)
from data_converge.dag import DAGExecutor, ExecutionReport, TaskStatus
from data_converge.dwd_ready.coordinator import ReadyCoordinator
from data_converge.dwd_ready.redis_source import bind_sentinel_identity, create_source
from data_converge.health import Heartbeat, healthcheck
from data_converge.scheduler import TaskScheduler
from data_converge.sql_execution import SQLTaskRunner
from data_converge.sql_execution.client import execute_sql
from data_converge.state import StateStore

logger = logging.getLogger(__name__)


def parse_time(value):
    try:
        instant = datetime.fromisoformat(value)
        if instant.tzinfo is None or instant.utcoffset() is None:
            raise ValueError
        return instant.astimezone(ZoneInfo('Asia/Shanghai'))
    except (ValueError, TypeError):
        raise StartupError('TIME_REQUIRES_ISO8601_WITH_TIMEZONE') from None


def next_boundary(value):
    floor = value.replace(minute=value.minute - value.minute % 5, second=0, microsecond=0)
    return floor if floor == value else floor + timedelta(minutes=5)


def initialize_state(store, fingerprint, start_time):
    existing = store.get_meta('binding')
    if existing and existing != fingerprint:
        raise StartupError('STATE_DEPLOYMENT_BINDING_CHANGED')
    previous = store.get_meta('coverage_start')
    if previous:
        if start_time and start_time.isoformat() != previous:
            raise StartupError('STATE_START_CANNOT_BE_CHANGED')
    else:
        start_time = start_time or next_boundary(datetime.now(ZoneInfo('Asia/Shanghai')))
        store.set_meta('coverage_start', start_time.isoformat())
    store.set_meta('binding', fingerprint)


def build_task_scheduler(app_config_path: Path, definition: ApplicationDefinition, store: StateStore):
    config, options = definition.config, definition.options
    builder, dag = definition.builder, definition.dag
    runner = SQLTaskRunner(app_config_path, definition.sql_dir,
                           timeout=options['default_timeout_seconds'], config=config)
    executor = DAGExecutor(dag, runner, state_store=store,
                           max_concurrent_tasks=options['max_concurrent_tasks'],
                           max_attempts=options['max_attempts'],
                           retry_delay_seconds=options['retry_delay_seconds'])
    scheduler = TaskScheduler(dag, builder, executor, timezone=options['timezone'], state_store=store,
                              start_time=parse_time(store.get_meta('coverage_start')),
                              poll_interval_seconds=options['poll_interval_seconds'],
                              catchup_limit=options['catchup_limit'])
    coordinator = None
    if config['dwd_ready']['enabled']:
        ready = config['dwd_ready']
        source = create_source(ready)
        try:
            bind_sentinel_identity(store, getattr(source, 'identity', None))
            coordinator = ReadyCoordinator(store, source, parse_time(store.get_meta('coverage_start')),
                                           batch_size=ready['batch_size'],
                                           wait_warning_seconds=ready['wait_warning_seconds'],
                                           should_stop=lambda: scheduler.stop_requested)
            executor.readiness_check = coordinator.reason
            scheduler.ready_poller = coordinator.poll
        except BaseException:
            source.close()
            raise
    return scheduler, runner, coordinator


def arguments(argv):
    parser = argparse.ArgumentParser(description='Data Converge Lite')
    parser.add_argument('--config', type=Path, default=DEFAULT_APP_CONFIG_PATH)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument('--validate-only', action='store_true')
    modes.add_argument('--init-state', action='store_true')
    modes.add_argument('--status', action='store_true')
    modes.add_argument('--healthcheck', choices=['liveness', 'readiness'])
    modes.add_argument('--run-once', action='store_true')
    modes.add_argument('--run-task')
    modes.add_argument('--resolve-instance')
    parser.add_argument('--start-time', help='First complete data window; only with --init-state')
    parser.add_argument('--scheduled-time')
    parser.add_argument('--now', help='Explicit evaluation time; only with --run-once')
    parser.add_argument('--resolution', choices=['success', 'retry'])
    parser.add_argument('--note')
    args = parser.parse_args(argv)
    if args.start_time and not args.init_state:
        parser.error('--start-time requires --init-state')
    if args.now and not args.run_once:
        parser.error('--now requires --run-once')
    if args.run_task and not args.scheduled_time:
        parser.error('--run-task requires --scheduled-time')
    if args.resolve_instance and (not args.resolution or not args.note):
        parser.error('--resolve-instance requires --resolution and --note')
    return args


def output(value):
    print(json.dumps(value, ensure_ascii=False, default=str))


@contextmanager
def service_runtime(config_path: Path, definition: ApplicationDefinition,
                    store: StateStore, directory: Path):
    """Own runtime resources and restore signal handlers even if cleanup fails."""
    with Heartbeat(directory) as heartbeat, ExitStack() as cleanup:
        scheduler = None

        def stop(signum, _frame):
            # Signal handlers must not acquire locks or perform file I/O.
            heartbeat.ready = False
            if scheduler is None:
                raise SystemExit(128 + signum)
            scheduler.shutdown(wait=False)

        for signum in (signal.SIGTERM, signal.SIGINT):
            previous = signal.signal(signum, stop)
            cleanup.callback(signal.signal, signum, previous)
        scheduler, runner, coordinator = build_task_scheduler(config_path, definition, store)
        if coordinator is not None:
            cleanup.callback(coordinator.close)
        yield scheduler, runner, heartbeat


def report_result(report: ExecutionReport) -> int:
    output({'instances': [{'id': item.instance_id, 'status': item.status.value,
                           'query_id': item.query_id}
                          for item in report.instances.values()]})
    return int(any(item.status != TaskStatus.SUCCESS for item in report.instances.values()))


def execute_mode(args, scheduler: TaskScheduler, runner: SQLTaskRunner,
                 heartbeat: Heartbeat) -> int:
    if args.run_task:
        scheduled = parse_time(args.scheduled_time)
        report = scheduler.run_task(args.run_task, scheduled)
        return report_result(report)
    if args.run_once:
        report = scheduler.tick(parse_time(args.now) if args.now else None)
        return report_result(report)
    execute_sql(runner.config, 'SELECT 1', timeout=10)
    if not scheduler.stop_requested:
        heartbeat.set_ready(True)
        scheduler.start()
    return 0


def run(args) -> int:
    raw = load_app_config(args.config)
    directory = state_directory(raw, args.config)
    if args.healthcheck:
        return 0 if healthcheck(directory, args.healthcheck) else 1
    if args.status:
        with closing(StateStore(directory / 'state.sqlite3')) as store:
            output({'coverage_start': store.get_meta('coverage_start'),
                    'counts': store.status_counts(), 'instances': store.list_instances(limit=20),
                    'dwd_windows': store.list_dwd_windows(limit=20)})
        return 0
    definition = load_definition(args.config, raw_config=raw)
    fingerprint = binding(definition.config, definition.dag)
    if args.validate_only:
        output({'status': 'VALID', 'tasks': len(definition.dag.tasks)})
        return 0
    start_time = parse_time(args.start_time) if args.start_time else None
    if start_time and next_boundary(start_time) != start_time:
        raise StartupError('START_TIME_MUST_BE_A_FIVE_MINUTE_BOUNDARY')
    with closing(StateStore(directory / 'state.sqlite3', create=args.init_state)) as store:
        store.acquire_lock()
        if args.init_state:
            initialize_state(store, fingerprint, start_time)
            output({'status': 'INITIALIZED', 'coverage_start': store.get_meta('coverage_start')})
            return 0
        if store.get_meta('binding') != fingerprint or not store.get_meta('coverage_start'):
            raise StartupError('STATE_DEPLOYMENT_BINDING_CHANGED_OR_UNINITIALIZED')
        store.recover_running()
        if args.resolve_instance:
            store.resolve(args.resolve_instance, 'SUCCESS' if args.resolution == 'success' else 'PENDING', args.note)
            output({'status': 'RESOLVED', 'instance_id': args.resolve_instance})
            return 0
        if args.run_task:
            candidate = definition.builder.create_instance(
                definition.dag, args.run_task, parse_time(args.scheduled_time))
            if candidate.data_start < parse_time(store.get_meta('coverage_start')):
                raise StartupError('WINDOW_PRECEDES_COVERAGE_START')
        with service_runtime(args.config, definition, store, directory) as (scheduler, runner, heartbeat):
            return execute_mode(args, scheduler, runner, heartbeat)


def main(argv=None):
    os.umask(0o077)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
    args = arguments(argv)
    try:
        return run(args)
    except (Exception, KeyboardInterrupt) as exc:
        # Untrusted SDK/server/SQL exception bodies never reach application logs.
        detail = str(exc) if isinstance(exc, StartupError) else type(exc).__name__
        logger.error('SERVICE_FAILED: %s', detail)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
