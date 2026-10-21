"""Shipped monthly tasks can consume existing daily data before month close."""
from dataclasses import replace
from datetime import datetime
from types import SimpleNamespace

import pytest
import yaml

from data_converge.backfill.plan import build_plan
from data_converge.backfill.queue import Queue
from data_converge.backfill.worker import BackfillWorker
from data_converge.config.application import load_definition
from data_converge.config.evolution import build_manifest, manifest_digest
from data_converge.dag.builder import required_cross_period_ids
from .paths import RESOURCES_ROOT
from .test_main import app
from .test_state import open_store


START = '2026-10-01T00:00:00+08:00'
END = '2026-11-01T00:00:00+08:00'
NOW = datetime.fromisoformat('2026-10-21T10:55:00+08:00')
CUTOVER = datetime.fromisoformat('2026-10-21T10:50:00+08:00')
COVERAGE = '2026-10-16T19:35:00+08:00'
MONTH_TASKS = (
    'task_gpu_day_to_month_dws', 'task_node_day_to_month_dws',
    'task_node_month_ads', 'task_resource_month_ads',
    'task_model_instance_month_dws', 'task_model_instance_month_ads',
    'task_model_server_month_ads', 'task_model_metric_instance_month_dws',
    'task_model_metric_instance_month_ads', 'task_model_metric_server_month_ads',
    'task_resource_server_month_ads', 'task_request_month_dws',
    'task_model_metric_node_month_ads', 'task_request_month_agent_ads',
    'task_request_month_server_ads',
)
# Independent contracts from the shipped SQL input chains; daily inputs already exist.
SAME_MONTH_PARENTS = {
    'task_node_month_ads': ('task_gpu_day_to_month_dws', 'task_node_day_to_month_dws'),
    'task_resource_month_ads': ('task_node_month_ads',),
    'task_model_instance_month_ads': ('task_model_instance_month_dws', 'task_gpu_day_to_month_dws'),
    'task_model_server_month_ads': ('task_model_instance_month_dws', 'task_gpu_day_to_month_dws'),
    'task_model_metric_instance_month_ads': ('task_model_metric_instance_month_dws',
                                           'task_model_instance_month_ads'),
    'task_model_metric_server_month_ads': ('task_model_metric_instance_month_ads',),
    'task_resource_server_month_ads': ('task_model_metric_server_month_ads',),
    'task_request_month_agent_ads': ('task_request_month_dws',),
    'task_request_month_server_ads': ('task_request_month_dws',),
}
DAILY_PARENTS = {
    'task_gpu_day_to_month_dws': 'task_gpu_hour_to_day_dws',
    'task_node_day_to_month_dws': 'task_node_hour_to_day_dws',
    'task_model_instance_month_dws': 'task_model_instance_day_dws',
    'task_model_metric_instance_month_dws': 'task_model_metric_instance_day_dws',
    'task_request_month_dws': 'task_request_day_dws',
    'task_model_metric_node_month_ads': 'task_model_metric_node_day_ads',
}


@pytest.fixture
def monthly(app):
    config = yaml.safe_load(app.read_text())
    config['config'] = {'convergence_rules_path': str(RESOURCES_ROOT / 'config/convergence_rules.yaml'),
                        'sql_directory': str(RESOURCES_ROOT / 'sql')}
    definition = load_definition(app, raw_config=config)
    definition = replace(definition, options=SimpleNamespace(timezone='Asia/Shanghai'))
    store = open_store(app.parent / 'state.sqlite3')
    store.set_meta('coverage_start', COVERAGE)
    queue = Queue(app.parent / 'backfill.sqlite3', create=True)
    try:
        yield definition, store, queue
    finally:
        queue.close()
        store.close()


def install_recovery(definition, store):
    manifest = build_manifest(definition)
    revision = manifest_digest(manifest)
    store.set_meta('binding', 'a' * 64)
    record = dict(schema_version=1, migration_id='month-catalog', cutover=CUTOVER.isoformat(),
                  previous_binding='a' * 64, current_binding='b' * 64, revision=revision,
                  backup_path='/test/before.sqlite3', recovered_at=CUTOVER.isoformat(),
                  identity_verified=False, previous_definition_available=False,
                  historical_counts=store.status_counts())
    store.apply_configuration(expected_binding='a' * 64, expected_revision=None,
        manifest=manifest, revision=revision, task_starts={name: CUTOVER for name in definition.dag.tasks},
        binding='b' * 64, migration_id='month-catalog', applied_at=CUTOVER,
        backup_path='/test/before.sqlite3', retire_existing=True, recovery=record)


def plan_month(monthly, taskname):
    definition, store, _ = monthly
    return build_plan(definition, START, END, task=taskname, now=NOW,
                      preaggregate=True, store=store)


@pytest.mark.parametrize('taskname', MONTH_TASKS)
def test_every_shipped_month_task_can_plan_before_coverage_and_cutover(monthly, taskname):
    definition, store, _ = monthly
    install_recovery(definition, store)
    item = plan_month(monthly, taskname)['items'][0]
    assert item['execution_kind'] == 'PREAGGREGATE'
    assert (item['data_start'], item['data_end']) == (START, END)
    assert item['formal_scheduled_time'] == '2026-11-01T00:20:00+08:00'
    assert store.get_meta('coverage_start') == COVERAGE
    assert store.get_instance(item['instance_id']) is None
    assert store.get_cursor(taskname) is None


@pytest.mark.parametrize('recovered', [False, True])
def test_all_fifteen_month_tasks_execute_in_dependency_order(monthly, recovered):
    definition, store, queue = monthly
    if recovered:
        install_recovery(definition, store)
    names = tuple(name for name in definition.dag.topological_order
                  if definition.dag.tasks[name].aggregation_interval == 'P1M')
    assert names == MONTH_TASKS
    before = store.configuration_snapshot()
    jobs = [queue.submit(plan_month(monthly, name)) for name in names]
    calls = []
    worker = BackfillWorker(definition, store, lambda instance: calls.append(instance), clock=lambda: NOW)
    for _ in names:
        worker.run_one(queue)
    assert [instance.taskname for instance in calls] == list(MONTH_TASKS)
    assert all(queue.status(job)['counts'] == {'SUCCESS': 1} for job in jobs)
    assert all((instance.data_start.isoformat(), instance.data_end.isoformat()) == (START, END)
               for instance in calls)
    assert all(store.get_instance(instance.instance_id) is None for instance in calls)
    assert all(store.latest_preaggregate(instance.instance_id)['status'] == 'SUCCESS' for instance in calls)
    assert all(store.get_cursor(name) is None for name in names)
    assert store.configuration_snapshot() == before


@pytest.mark.parametrize('taskname', MONTH_TASKS)
def test_month_instance_readiness_tracks_sql_producer_periods(monthly, taskname):
    definition, _, _ = monthly
    instance = definition.builder.create_instance(definition.dag, taskname, datetime.fromisoformat(END))
    assert instance.upstream_instance_ids == tuple(
        name + '@' + END for name in SAME_MONTH_PARENTS.get(taskname, ()))
    cross = required_cross_period_ids(definition.dag, instance.task, instance.data_start, instance.data_end)
    if taskname in DAILY_PARENTS:
        parent = DAILY_PARENTS[taskname]
        assert len(cross) == 31
        assert cross[0] == parent + '@2026-10-02T00:00:00+08:00'
        assert cross[-1] == parent + '@' + END
    else:
        assert cross == ()


def test_node_month_runs_from_existing_day_ads_without_unrelated_month_receipt(monthly):
    definition, store, queue = monthly
    store.set_meta('coverage_start', START)
    job = queue.submit(plan_month(monthly, 'task_model_metric_node_month_ads'))
    calls = []
    BackfillWorker(definition, store, lambda instance: calls.append(instance.taskname),
                   clock=lambda: NOW).run_one(queue)
    assert queue.status(job)['counts'] == {'SUCCESS': 1}
    assert calls == ['task_model_metric_node_month_ads']
    assert store.latest_preaggregate('task_model_metric_instance_month_dws@' + END) is None


def test_month_ads_waits_for_successful_monthly_dws_preaggregates(monthly):
    definition, store, queue = monthly
    store.set_meta('coverage_start', START)
    calls = []
    worker = BackfillWorker(definition, store, lambda instance: calls.append(instance.taskname),
                            clock=lambda: NOW)
    early = queue.submit(plan_month(monthly, 'task_node_month_ads'))
    worker.run_one(queue)
    assert queue.status(early)['counts'] == {'BLOCKED': 1}
    assert calls == []
    for name in ('task_gpu_day_to_month_dws', 'task_node_day_to_month_dws'):
        queue.submit(plan_month(monthly, name))
        worker.run_one(queue)
    retry = queue.submit(plan_month(monthly, 'task_node_month_ads'))
    worker.run_one(queue)
    assert queue.status(retry)['counts'] == {'SUCCESS': 1}
    assert calls == ['task_gpu_day_to_month_dws', 'task_node_day_to_month_dws', 'task_node_month_ads']
