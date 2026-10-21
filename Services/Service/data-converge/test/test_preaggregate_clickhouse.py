"""Opt in with RUN_CLICKHOUSE_INTEGRATION=1; owns and removes one isolated container."""

from datetime import datetime, timedelta
import os
import subprocess
import time
from types import SimpleNamespace
import urllib.error
import urllib.request
import uuid
from zoneinfo import ZoneInfo

import pytest
import yaml

from data_converge.backfill.plan import build_plan
from data_converge.backfill.queue import Queue
from data_converge.backfill.worker import BackfillWorker
from data_converge.config.application import load_definition
from data_converge.dag import TaskStatus
from data_converge.runtime import build_scheduler
from data_converge.sql_execution import SQLTaskRunner
from data_converge.sql_execution.client import execute_sql
from .test_state import open_store


pytestmark = pytest.mark.skipif(os.environ.get('RUN_CLICKHOUSE_INTEGRATION') != '1',
                                reason='Set RUN_CLICKHOUSE_INTEGRATION=1 for isolated Docker ClickHouse')
IMAGE = 'clickhouse/clickhouse-server:25.8.7.3'
PASSWORD = 'preaggregate-integration-only'
TASKS = ('hour_dws', 'hour_ads')


def docker(*arguments, check=True):
    return subprocess.run(['docker', *arguments], check=check, capture_output=True,
                           text=True, timeout=45)


def server_configuration(path):
    path.write_text(f'''<clickhouse>
      <logger><level>information</level><console>1</console></logger>
      <max_thread_pool_size>512</max_thread_pool_size>
      <background_schedule_pool_size>8</background_schedule_pool_size>
      <background_pool_size>16</background_pool_size>
      <background_move_pool_size>2</background_move_pool_size>
      <remote_servers><integration><shard><replica>
        <host>127.0.0.1</host><port>9000</port>
        <user>integration</user><password>{PASSWORD}</password>
      </replica></shard></integration></remote_servers>
    </clickhouse>''')


def wait_for_server(port, name):
    deadline = time.monotonic() + 35
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    while time.monotonic() < deadline:
        try:
            with opener.open(f'http://127.0.0.1:{port}/ping', timeout=1) as response:
                if response.read() == b'Ok.\n':
                    return
        except (OSError, urllib.error.URLError):
            if docker('inspect', '--format', '{{.State.Running}}', name).stdout.strip() == 'false':
                break
            time.sleep(0.2)
    log = docker('logs', '--tail', '50', name)
    pytest.fail('Isolated ClickHouse did not start: ' + log.stdout + log.stderr)


@pytest.fixture(scope='module')
def clickhouse(tmp_path_factory):
    directory = tmp_path_factory.mktemp('preaggregate-clickhouse')
    limits = directory / 'limits.xml'
    server_configuration(limits)
    name = 'codex-preaggregate-it-' + uuid.uuid4().hex[:12]
    try:
        docker('run', '--detach', '--name', name, '--memory', '1g', '--cpus', '2',
               '--publish', '127.0.0.1::8123', '--env', 'CLICKHOUSE_USER=integration',
               '--env', 'CLICKHOUSE_PASSWORD=' + PASSWORD, '--env', 'CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1',
               '--volume', str(limits) + ':/etc/clickhouse-server/config.d/integration.xml:ro', IMAGE)
        port = int(docker('inspect', '--format',
                          '{{(index (index .NetworkSettings.Ports "8123/tcp") 0).HostPort}}', name).stdout)
        wait_for_server(port, name)
        yield {'clickhouse': dict(host='127.0.0.1', port=port, database='default',
                                 username='integration', password=PASSWORD)}
    finally:
        removed = docker('rm', '--force', '--volumes', name, check=False)
        assert removed.returncode == 0, removed.stderr
        assert docker('inspect', name, check=False).returncode != 0
        print('CLICKHOUSE_INTEGRATION_REMOVED ' + name)


def create_tables(config):
    execute_sql(config, 'CREATE TABLE source (event_time DateTime, value UInt64) '
                'ENGINE=MergeTree ORDER BY event_time')
    database = config['clickhouse']['database']
    for target in ('dws', 'ads'):
        execute_sql(config, f'CREATE TABLE {target}_local (window_time DateTime, '
                    'samples UInt64, total UInt64) ENGINE=MergeTree ORDER BY window_time')
        execute_sql(config, f"CREATE TABLE {target} AS {target}_local "
                    f"ENGINE=Distributed('integration', '{database}', '{target}_local', rand())")


def write_definition(directory, config, interval):
    sql = directory / 'sql'
    sql.mkdir()
    rules = []
    for index, target in enumerate(('dws', 'ads')):
        source = 'source' if index == 0 else 'dws'
        column = 'event_time' if index == 0 else 'window_time'
        totals = 'count(), sum(value)' if index == 0 else 'sum(samples), sum(total)'
        (sql / (TASKS[index] + '.sql')).write_text(
            f'INSERT INTO {target} SELECT {{start_time:DateTime}}, {totals} FROM {source} '
            f'WHERE {column} >= {{start_time:DateTime}} AND {column} < {{end_time:DateTime}}')
        cron = f'{3 + 2 * index} ' + ('* * * *' if interval == 'PT1H' else '0 * * *')
        rules.append(dict(taskname=TASKS[index], source_tables=[source], target_table=target,
                          aggregation_interval=interval, cron=cron, sql_id=TASKS[index],
                          depends_on=[] if index == 0 else [TASKS[0]]))
    rules_path = directory / 'rules.yaml'
    rules_path.write_text(yaml.safe_dump({'convergence_rules': rules}))
    connection = {key: value for key, value in config['clickhouse'].items() if key != 'password'}
    connection['password_env'] = 'PREAGGREGATE_TEST_PASSWORD'
    path = directory / 'app.yaml'
    path.write_text(yaml.safe_dump(dict(clickhouse=connection, dwd_ready={'enabled': False},
                    config=dict(convergence_rules_path=str(rules_path), sql_directory=str(sql)))))
    return path, load_definition(path)


@pytest.fixture
def scenario(request, tmp_path, monkeypatch, clickhouse):
    database = 'preaggregate_' + uuid.uuid4().hex[:12]
    execute_sql(clickhouse, 'CREATE DATABASE ' + database)
    config = {'clickhouse': dict(clickhouse['clickhouse'], database=database)}
    monkeypatch.setenv('PREAGGREGATE_TEST_PASSWORD', PASSWORD)
    store = queue = None
    try:
        create_tables(config)
        path, definition = write_definition(tmp_path, config, request.param)
        start = datetime.now(ZoneInfo(definition.options.timezone)).replace(minute=0, second=0, microsecond=0)
        if request.param == 'P1D':
            start = start.replace(hour=0)
        end = start + (timedelta(hours=1) if request.param == 'PT1H' else timedelta(days=1))
        store = open_store(tmp_path / 'state.sqlite3')
        store.set_meta('coverage_start', start.isoformat())
        queue = Queue(tmp_path / 'backfill.sqlite3', create=True)
        runner = SQLTaskRunner(path, definition.sql_dir, config=definition.config)
        yield SimpleNamespace(config=config, definition=definition, runner=runner, store=store,
                               queue=queue, start=start, end=end)
    finally:
        if queue is not None:
            queue.close()
        if store is not None:
            store.close()
        execute_sql(clickhouse, 'DROP DATABASE ' + database)


def insert_source(config, entries):
    values = ','.join(f'(toDateTime({int(instant.timestamp())}),{value})' for instant, value in entries)
    execute_sql(config, 'INSERT INTO source VALUES ' + values)


def assert_targets(config, samples, total):
    for target in ('dws', 'ads'):
        rows = execute_sql(config, f'SELECT count(), sum(samples), sum(total) FROM {target}')
        assert [[int(value) for value in row] for row in rows] == [[1, samples, total]]


def assert_phases(config, query_ids):
    execute_sql(config, 'SYSTEM FLUSH LOGS')
    for query_id in query_ids:
        rows = execute_sql(config, "SELECT query_kind FROM system.query_log "
                            f"WHERE query_id='{query_id}' AND type='QueryFinish' "
                            "AND query_kind IN ('Alter', 'Insert') ORDER BY query_start_time_microseconds")
        assert rows == [['Alter'], ['Insert']]


@pytest.mark.parametrize('scenario', ['PT1H', 'P1D'], indirect=True)
def test_early_chain_is_overwritten_by_formal_same_window_without_accumulation(scenario, monkeypatch):
    s = scenario
    insert_source(s.config, [(s.start - timedelta(seconds=1), 1000), (s.start, 2),
                             (s.start + timedelta(minutes=10), 5), (s.end, 2000)])
    early = s.start + timedelta(minutes=30)
    plan = build_plan(s.definition, s.start.isoformat(), s.end.isoformat(), all_tasks=True,
                      now=early, preaggregate=True, store=s.store)
    job = s.queue.submit(plan)
    worker = BackfillWorker(s.definition, s.store, s.runner, clock=lambda: early)
    worker.run_one(s.queue)
    worker.run_one(s.queue)
    assert s.queue.status(job)['counts'] == {'SUCCESS': 2}
    assert_targets(s.config, 2, 7)
    ids = [item['instance_id'] for item in plan['items']]
    initial = [s.store.latest_preaggregate(instance_id) for instance_id in ids]
    assert all(s.store.get_status(instance_id) is None for instance_id in ids)
    assert all(s.store.get_cursor(task) is None for task in TASKS)
    assert all(row['covered_at'] is None for row in initial)
    insert_source(s.config, [(s.start + timedelta(minutes=40), 11)])
    scheduler, executor = build_scheduler(s.definition, s.runner, s.store, s.start)
    due = s.end + timedelta(minutes=5)
    monkeypatch.setattr(executor, '_now', lambda instance: due)
    report = scheduler.tick(due)
    assert {instance.instance_id for instance in report.succeeded} == set(ids)
    assert_targets(s.config, 3, 18)
    assert all(s.store.get_status(instance_id) == TaskStatus.SUCCESS for instance_id in ids)
    assert all(s.store.latest_preaggregate(instance_id)['covered_at'] for instance_id in ids)
    formal = [s.store.get_instance(instance_id) for instance_id in ids]
    query_ids = [row['query_id'] for row in initial + formal]
    assert len(set(query_ids)) == 4
    assert_phases(s.config, query_ids)
    scheduler.tick(due)
    assert_targets(s.config, 3, 18)
    assert all(s.store.get_instance(instance_id)['attempts'] == 1 for instance_id in ids)
