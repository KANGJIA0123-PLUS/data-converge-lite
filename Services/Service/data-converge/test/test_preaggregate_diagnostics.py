"""Preliminary attempts remain visible without inventing a formal success."""
from contextlib import closing
from datetime import datetime, timedelta

import pytest

from data_converge.application import Application
from data_converge.config.evolution import build_manifest
from data_converge.dag import TaskStatus
from data_converge.diagnostics import service
from data_converge.diagnostics.recovery import recovery_plan
from data_converge.diagnostics.report import render_text
from data_converge.state import StateStore

from .test_main import app


@pytest.fixture
def early_ledger(app):
    application = Application(app)
    application.initialize('2026-10-21T10:00:00+08:00')
    definition = application.definition
    now = datetime.fromisoformat('2026-10-21T10:02:00+08:00')
    due = now.replace(minute=6)
    instance = definition.builder.create_instance(definition.dag, 'simple', due)
    manifest = build_manifest(definition)
    evidence = {key: manifest[key] for key in ('connection', 'dataset_id')}
    evidence.update(task=manifest['tasks']['simple'], timezone=definition.options.timezone,
                    cleanup_column='window_time')
    item = dict(item_id='bf_diagnostic:0', revision='test', config_evidence=evidence,
                formal_scheduled_time=due.isoformat())
    with closing(StateStore(app.parent / 'state/state.sqlite3')) as store:
        store.acquire_lock()
        store.begin_preaggregate(item, instance, now=now)
        yield app, store, item, instance, now


def test_inspect_preaggregate_before_formal_instance_exists(early_ledger):
    path, store, item, instance, now = early_ledger
    store.finish_preaggregate(item['item_id'], TaskStatus.SUCCESS, now=now)
    before = list(store._connection().iterdump())
    result = service.inspect_instance(path, instance.instance_id)
    assert result['status'] == 'PASS'
    assert result['data']['formal_instance_registered'] is False
    assert result['data']['preaggregate']['attempts'][0]['status'] == 'SUCCESS'
    assert '正式实例尚未入账' in render_text(result)
    assert store.get_status(instance.instance_id) is None
    assert list(store._connection().iterdump()) == before


def test_history_displays_early_unknown_and_redacts_resolution_note(early_ledger):
    path, store, item, instance, now = early_ledger
    store.finish_preaggregate(item['item_id'], TaskStatus.UNKNOWN,
                              error='CLICKHOUSE_HTTP_FAILED', now=now)
    store.resolve_preaggregate(item['item_id'], 'success', 'private reconciliation evidence')
    result = service.history(path, task='simple')
    row = result['data']['preaggregate']['attempts'][0]
    assert row['status'] == 'UNKNOWN' and row['effective_status'] == 'SUCCESS'
    assert row['resolution']['note_recorded'] is True
    assert 'private reconciliation evidence' not in str(result)
    assert 'config_evidence' not in str(result)
    assert store.get_status(instance.instance_id) is None


def test_early_unknown_visible_in_failures_and_recovery(early_ledger):
    path, store, item, _, now = early_ledger
    store.finish_preaggregate(item['item_id'], TaskStatus.UNKNOWN, now=now)
    before = list(store._connection().iterdump())
    failures = service.failures(path, task='simple')
    assert failures['status'] == 'FAIL'
    assert failures['data']['preaggregate']['attempts'][0]['item_id'] == item['item_id']
    recovery = recovery_plan(path)
    assert recovery['status'] == 'WARN'
    assert recovery['data']['preaggregate']['attempts'][0]['status'] == 'UNKNOWN'
    assert '--resolve-preaggregate' in render_text(recovery)
    assert list(store._connection().iterdump()) == before


def test_preaggregate_pages_honor_task_and_offset(early_ledger):
    path, store, item, instance, now = early_ledger
    store.finish_preaggregate(item['item_id'], TaskStatus.SUCCESS, now=now)
    second = item | {'item_id': 'bf_diagnostic:1'}
    store.begin_preaggregate(second, instance, now=now)
    store.finish_preaggregate(second['item_id'], TaskStatus.FAILED, now=now)
    first = service.history(path, task='simple', limit=1)['data']['preaggregate']
    assert first['has_more'] and first['attempts'][0]['item_id'] == second['item_id']
    next_page = service.history(path, task='simple', limit=1, offset=1)['data']['preaggregate']
    assert not next_page['has_more'] and next_page['attempts'][0]['item_id'] == item['item_id']
    assert service.history(path, task='missing')['data']['preaggregate']['attempts'] == []


def test_wait_timeout_evidence_survives_success_and_inspection_is_readonly(early_ledger):
    path, store, item, instance, now = early_ledger
    store.finish_preaggregate(item['item_id'], TaskStatus.SUCCESS, now=now)
    due = instance.scheduled_time
    record = dict(instance_id=instance.instance_id, scheduled_due_at=due,
                  wait_seconds=300, deadline_at=due + timedelta(minutes=5))
    store.enqueue_and_advance([instance], {}, cross_period_waits=[record])
    missing = dict(missing_count=1, missing_status_counts={'NOT_FOUND': 1},
                   missing_samples=[dict(instance_id='old_parent', status='NOT_FOUND')],
                   missing_truncated=False, decision_reason='DEADLINE_REACHED')
    store.record_cross_period_wait(instance.instance_id, status='TIMED_OUT', missing=missing, now=due)
    assert store.claim(instance.instance_id, now=due)
    store.finish(instance.instance_id, TaskStatus.SUCCESS, now=due)
    before = list(store._connection().iterdump())
    result = service.inspect_instance(path, instance.instance_id)
    wait = result['data']['cross_period_wait']
    assert wait['status'] == 'TIMED_OUT' and wait['missing_count'] == 1
    assert result['data']['instance']['status'] == 'SUCCESS'
    assert result['data']['preaggregate']['attempts'][0]['covered_at']
    assert '超时执行不代表源数据完整' in render_text(result)
    assert list(store._connection().iterdump()) == before


def test_inspect_current_wait_policy_keeps_saved_release_evidence(early_ledger):
    path, store, item, instance, now = early_ledger
    store.finish_preaggregate(item['item_id'], TaskStatus.SUCCESS, now=now)
    due = instance.scheduled_time
    record = dict(instance_id=instance.instance_id, scheduled_due_at=due,
                  wait_seconds=300, deadline_at=due + timedelta(minutes=5))
    store.enqueue_and_advance([instance], {}, cross_period_waits=[record])
    result = service.inspect_instance(path, instance.instance_id)
    wait = result['data']['cross_period_wait']
    assert wait['enabled_now'] is True
    assert wait['current_missing']['missing_count'] == 0
    assert wait['checked_at'] is None
