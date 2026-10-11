"""Company-layout service entry point, with explicit local state initialization."""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import signal
from dataclasses import asdict
from datetime import datetime, timedelta
from importlib.resources import files
from pathlib import Path
from zoneinfo import ZoneInfo

import yaml

from data_converge.config import ConfigLoader
from data_converge.dag import DAGBuilder, DAGExecutor
from data_converge.dwd_ready.config import load_ready_config
from data_converge.dwd_ready.coordinator import ReadyCoordinator
from data_converge.dwd_ready.redis_source import create_source
from data_converge.health import Heartbeat, healthcheck
from data_converge.scheduler import TaskScheduler
from data_converge.sql_execution import SQLTaskRunner
from data_converge.sql_execution.client import execute_sql, load_config
from data_converge.state import StateStore

PROJECT_ROOT = Path(__file__).resolve().parents[2]
_source_resources = PROJECT_ROOT / 'resources'
RESOURCES = _source_resources if _source_resources.is_dir() else Path(str(files('data_converge_assets')))
DEFAULT_APP_CONFIG_PATH = RESOURCES / 'config/app_config.yaml'
logger = logging.getLogger(__name__)


class StartupError(ValueError):
    """An intentionally public, credential-free startup diagnostic."""


def load_app_config(path):
    try:
        value = yaml.safe_load(Path(path).read_text())
        if not isinstance(value, dict):
            raise ValueError
        return value
    except (OSError, ValueError, yaml.YAMLError):
        raise StartupError('CONFIG_UNREADABLE') from None


def resolve_path(value, base):
    if not isinstance(value, str) or not value:
        raise StartupError('CONFIG_PATH_INVALID')
    value = os.path.expandvars(value)
    if '$' in value:
        raise StartupError('CONFIG_PATH_ENV_MISSING')
    path = Path(value).expanduser()
    return path if path.is_absolute() else Path(base) / path


def state_directory(config, config_path):
    return resolve_path(config.get('state', {}).get('directory', '/srv/its/MindOpsBOSE/data-converge/data'),
                        Path(config_path).parent)


def asset_path(value, fallback, config_path):
    if value is None:
        return RESOURCES / fallback
    # Preserve the company's resources/... convention, including installed wheels.
    if isinstance(value, str) and value.startswith('resources/'):
        return RESOURCES / value[len('resources/'):]
    return resolve_path(value, Path(config_path).parent)


def settings(config):
    values = {'timezone': 'Asia/Shanghai', 'default_timeout_seconds': 300,
              'max_concurrent_tasks': 4, 'poll_interval_seconds': 5,
              'catchup_limit': 100, 'max_attempts': 3, 'retry_delay_seconds': 30}
    values.update(config.get('scheduler', {}))
    if values['timezone'] != 'Asia/Shanghai':
        raise StartupError('TIMEZONE_MUST_BE_ASIA_SHANGHAI')
    for key in ('default_timeout_seconds', 'max_concurrent_tasks', 'poll_interval_seconds',
                'catchup_limit', 'max_attempts', 'retry_delay_seconds'):
        if type(values[key]) is not int or values[key] <= 0:
            raise StartupError('SCHEDULER_SETTING_INVALID')
    if values['default_timeout_seconds'] > 300:
        raise StartupError('SQL_TIMEOUT_MUST_NOT_EXCEED_300_SECONDS')
    return values


def load_definition(config_path):
    config = load_config(Path(config_path), resolve_password=False)
    config['dwd_ready'] = load_ready_config(config, Path(config_path).parent)
    options = settings(config)
    section = config.get('config', {})
    rules = asset_path(section.get('convergence_rules_path'), 'config/convergence_rules.yaml', config_path)
    sql_dir = asset_path(section.get('sql_directory'), 'sql', config_path)
    builder = DAGBuilder(ConfigLoader(rules, sql_dir=sql_dir))
    dag = builder.build()
    if not dag.tasks:
        raise StartupError('NO_TASKS_CONFIGURED')
    if config['dwd_ready']['enabled'] and any(
            not task.depends_on and task.aggregation_interval != 'PT5M' for task in dag.tasks.values()):
        raise StartupError('DWD_READY_REQUIRES_FIVE_MINUTE_ROOT_TASKS')
    anchor = datetime(2000, 3, 1, tzinfo=ZoneInfo(options['timezone']))
    for taskname in dag.topological_order:
        builder.create_instance(dag, taskname, anchor)
    # Cron and interval validation runs without a runner, state directory or SDK.
    preview = TaskScheduler(dag, builder, DAGExecutor(dag, runner=lambda _: None),
                            timezone=options['timezone'])
    preview.register_jobs()
    return config, options, builder, dag, sql_dir


def binding(config, dag):
    from data_converge.sql_execution.client import _connection
    endpoint = _connection(config['clickhouse'])[0]
    value = {'endpoint': endpoint, 'database': config['clickhouse']['database'],
             'username': config['clickhouse']['username'],
             'tasks': [{key: item for key, item in asdict(dag.tasks[name]).items()
                        if key != 'description'} for name in sorted(dag.tasks)]}
    ready = config['dwd_ready']
    value['dwd_ready'] = {'enabled': ready['enabled']}
    if ready['enabled']:
        value['dwd_ready'].update(queue_key=ready['queue_key'], redis={
            key: ready['redis'].get(key) for key in ('host', 'port', 'mode', 'db')})
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


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


def build_task_scheduler(app_config_path, definition, store):
    config, options, builder, dag, sql_dir = definition
    runner = SQLTaskRunner(Path(app_config_path), sql_dir, timeout=options['default_timeout_seconds'])
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
            coordinator = ReadyCoordinator(store, source, parse_time(store.get_meta('coverage_start')),
                                           batch_size=ready['batch_size'],
                                           wait_warning_seconds=ready['wait_warning_seconds'],
                                           should_stop=lambda: scheduler._stop_requested)
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


def run(args):
    raw = load_app_config(args.config)
    directory = state_directory(raw, args.config)
    if args.healthcheck:
        return 0 if healthcheck(directory, args.healthcheck) else 1
    if args.status:
        store = StateStore(directory / 'state.sqlite3')
        try:
            output({'coverage_start': store.get_meta('coverage_start'),
                    'counts': store.status_counts(), 'instances': store.list_instances(limit=20),
                    'dwd_windows': store.list_dwd_windows(limit=20)})
        finally:
            store.close()
        return 0
    definition = load_definition(args.config)
    config, _, _, dag, _ = definition
    fingerprint = binding(config, dag)
    if args.validate_only:
        output({'status': 'VALID', 'tasks': len(dag.tasks)})
        return 0
    start_time = parse_time(args.start_time) if args.start_time else None
    if start_time and next_boundary(start_time) != start_time:
        raise StartupError('START_TIME_MUST_BE_A_FIVE_MINUTE_BOUNDARY')
    store = StateStore(directory / 'state.sqlite3', create=args.init_state)
    try:
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
        with Heartbeat(directory) as heartbeat:
            scheduler = None
            coordinator = None
            previous = {}
            stopping = False

            def stop(signum, _frame):
                nonlocal stopping
                # Python handlers may interrupt code holding a non-reentrant lock.
                # Keep this path free of file I/O and Event/Condition locks.
                stopping = True
                heartbeat.ready = False
                if scheduler is None:
                    raise SystemExit(128 + signum)
                scheduler.shutdown(wait=False)

            for signum in (signal.SIGTERM, signal.SIGINT):
                previous[signum] = signal.signal(signum, stop)
            try:
                scheduler, runner, coordinator = build_task_scheduler(args.config, definition, store)
                if args.run_task:
                    scheduled = parse_time(args.scheduled_time)
                    candidate = scheduler.dag_builder.create_instance(dag, args.run_task, scheduled)
                    if candidate.data_start < parse_time(store.get_meta('coverage_start')):
                        raise StartupError('WINDOW_PRECEDES_COVERAGE_START')
                    report = scheduler.run_task(args.run_task, scheduled)
                elif args.run_once:
                    report = scheduler.tick(parse_time(args.now) if args.now else None)
                else:
                    execute_sql(runner.config, 'SELECT 1', timeout=10)
                    if stopping:
                        return 0
                    heartbeat.set_ready(True)
                    scheduler.start()
                    return 0
                output({'instances': [{'id': item.instance_id, 'status': item.status.value,
                                       'query_id': getattr(item, 'query_id', None)}
                                      for item in report.instances.values()]})
                return int(any(item.status.value != 'SUCCESS' for item in report.instances.values()))
            finally:
                if coordinator is not None:
                    coordinator.close()
                for signum, handler in previous.items():
                    signal.signal(signum, handler)
    finally:
        store.close()


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
