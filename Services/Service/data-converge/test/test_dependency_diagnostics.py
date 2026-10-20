"""Inspection distinguishes scheduling prerequisites from static data lineage."""

from contextlib import closing
from datetime import datetime

import pytest
import yaml

from data_converge.application import Application
from data_converge.dag import TaskStatus
from data_converge.diagnostics.report import render_text
from data_converge.diagnostics.service import inspect_instance
from data_converge.state import StateStore

from .test_main import app


START = '2026-10-14T10:00:00+08:00'
DUE = datetime.fromisoformat('2026-10-14T11:06:00+08:00')


@pytest.fixture
def dependency_ledger(app):
    path = app.parent / 'rules.yaml'
    rules = yaml.safe_load(path.read_text())
    root = rules['convergence_rules'][0]
    for name, parents in [('hour_source', ['simple']),
                          ('hour_result', ['simple', 'hour_source'])]:
        rules['convergence_rules'].append(dict(
            root, taskname=name, target_table=name, aggregation_interval='PT1H',
            cron='6 * * * *', depends_on=parents))
    path.write_text(yaml.safe_dump(rules))
    application = Application(app)
    application.initialize(START)
    definition = application.definition
    instances = {name: definition.builder.create_instance(definition.dag, name, DUE)
                 for name in ('hour_source', 'hour_result')}
    with closing(StateStore(app.parent / 'state/state.sqlite3')) as store:
        store.acquire_lock()
        store.enqueue_and_advance(instances.values(), {})
        yield app, definition, store, instances


@pytest.mark.parametrize('status', [None, TaskStatus.FAILED, TaskStatus.UNKNOWN, TaskStatus.SUCCESS])
def test_cross_period_sources_do_not_warn_or_require_historical_success(dependency_ledger, status):
    path, definition, store, instances = dependency_ledger
    if status is not None:
        parent = definition.builder.create_instance(definition.dag, 'simple', DUE.replace(minute=1))
        store.enqueue_and_advance([parent], {})
        assert store.claim(parent.instance_id, now=DUE)
        store.finish(parent.instance_id, status, now=DUE)
    before = list(store._connection().iterdump())
    result = inspect_instance(path, instances['hour_source'].instance_id)
    check = next(item for item in result['checks'] if item['id'] == 'dependencies')
    assert check['status'] == 'PASS'
    assert result['data']['dependencies'] == {
        'total': 0, 'blocked_count': 0, 'blocked': [], 'truncated': False}
    assert result['data']['lineage'] == {
        'upstream_tasks': ['simple'], 'cross_period_tasks': ['simple']}
    output = render_text(result)
    assert '跨周期数据来源' in output and 'simple' in output
    assert '同周期执行前置实例: 0' in output
    assert list(store._connection().iterdump()) == before


def test_only_unfinished_same_period_source_is_reported_as_blocking(dependency_ledger):
    path, _, store, instances = dependency_ledger
    result = inspect_instance(path, instances['hour_result'].instance_id)
    check = next(item for item in result['checks'] if item['id'] == 'dependencies')
    assert check['status'] == 'WARN'
    assert result['data']['dependencies'] == {
        'total': 1, 'blocked_count': 1, 'truncated': False,
        'blocked': [{'instance_id': instances['hour_source'].instance_id, 'status': 'PENDING'}]}
    assert result['data']['lineage'] == {
        'upstream_tasks': ['simple', 'hour_source'], 'cross_period_tasks': ['simple']}
    assert '同周期' in check['summary']
    assert store.get_instance(instances['hour_result'].instance_id)['status'] == 'PENDING'


def test_inspection_preserves_old_blocked_evidence_after_prerequisite_is_removed(dependency_ledger):
    path, _, store, instances = dependency_ledger
    instance_id = instances['hour_source'].instance_id
    store.mark_blocked(instance_id, 'Upstream task instances are not successful: simple=NOT_FOUND')
    before = store.get_instance(instance_id)
    result = inspect_instance(path, instance_id)
    check = next(item for item in result['checks'] if item['id'] == 'dependencies')
    assert check['status'] == 'PASS'
    assert result['data']['instance']['status'] == 'BLOCKED'
    assert result['data']['dependencies']['blocked_count'] == 0
    assert store.get_instance(instance_id) == before
