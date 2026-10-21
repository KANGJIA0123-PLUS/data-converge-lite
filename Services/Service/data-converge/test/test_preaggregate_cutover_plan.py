"""Explicit early writes can carry a durable first-window recovery obligation."""
from dataclasses import replace
from datetime import datetime, timedelta

import pytest

from data_converge.backfill.plan import build_plan
from data_converge.backfill.queue import Queue
from data_converge.backfill.worker import BackfillWorker
from data_converge.config.evolution import build_manifest, manifest_digest
from .test_backfill_plan import definition_for
from .test_main import app
from .test_state import open_store


CUTOVER = datetime.fromisoformat('2026-10-21T10:50:00+08:00')
NOW = CUTOVER + timedelta(minutes=5)
COVERAGE = '2026-10-16T19:35:00+08:00'


def recover(store, definition):
    manifest = build_manifest(definition)
    revision = manifest_digest(manifest)
    store.set_meta('binding', 'a' * 64)
    store.set_meta('coverage_start', COVERAGE)
    record = dict(schema_version=1, migration_id='cutover-test', cutover=CUTOVER.isoformat(),
                  previous_binding='a' * 64, current_binding='b' * 64, revision=revision,
                  backup_path='/test/before.sqlite3', recovered_at=CUTOVER.isoformat(),
                  identity_verified=False, previous_definition_available=False,
                  historical_counts=store.status_counts())
    store.apply_configuration(expected_binding='a' * 64, expected_revision=None,
        manifest=manifest, revision=revision, task_starts={'task_0': CUTOVER}, binding='b' * 64,
        migration_id='cutover-test', applied_at=CUTOVER, backup_path='/test/before.sqlite3',
        retire_existing=True, recovery=record)


@pytest.fixture(params=['PT1H', 'P1D'])
def cutover(app, request):
    definition = definition_for(app, (request.param,))
    cron = '6 * * * *' if request.param == 'PT1H' else '10 0 * * *'
    task = replace(definition.dag.tasks['task_0'], cron=cron)
    definition = replace(definition, dag=replace(definition.dag, tasks={'task_0': task}))
    store = open_store(app.parent / 'state.sqlite3')
    recover(store, definition)
    start = CUTOVER.replace(minute=0) if request.param == 'PT1H' else CUTOVER.replace(hour=0, minute=0)
    end = start + (timedelta(hours=1) if request.param == 'PT1H' else timedelta(days=1))
    try:
        yield definition, store, start, end
    finally:
        store.close()


def plan_for(cutover, **options):
    definition, store, start, end = cutover
    return build_plan(definition, start.isoformat(), end.isoformat(), task='task_0',
                      now=NOW, preaggregate=True, store=store, **options)


def test_cross_cutover_plan_records_recovery_without_rewriting_boundaries(cutover):
    definition, store, start, end = cutover
    before = store.configuration_snapshot()
    item = plan_for(cutover)['items'][0]
    assert item['data_start'] == start.isoformat() and item['data_end'] == end.isoformat()
    assert item['execution_kind'] == 'PREAGGREGATE'
    marker = item['config_evidence']['recovery_coverage']
    assert marker['migration_id'] == store.legacy_recovery()['migration_id']
    assert datetime.fromisoformat(marker['cutover']) == CUTOVER
    assert datetime.fromisoformat(marker['task_effective_start']) == CUTOVER
    assert datetime.fromisoformat(marker['coverage_start']) == datetime.fromisoformat(COVERAGE)
    assert store.configuration_snapshot() == before
    assert store.get_cursor('task_0') is None
    store.validate_retirement()


def test_cross_cutover_worker_persists_obligation_before_running_sql(cutover):
    definition, store, _, _ = cutover
    plan = plan_for(cutover)
    calls = []
    def run(instance):
        records = store.pending_preaggregate_coverages()
        assert len(records) == 1 and records[0]['status'] == 'RUNNING'
        assert records[0]['query_id'] == instance.query_id
        calls.append(instance.instance_id)
    with Queue(store.path.parent / 'backfill.sqlite3', create=True) as queue:
        job = queue.submit(plan)
        BackfillWorker(definition, store, run, clock=lambda: NOW).run_one(queue)
        assert queue.status(job)['counts'] == {'SUCCESS': 1}
    assert calls == [plan['items'][0]['instance_id']]
    assert store.get_instance(calls[0]) is None
    assert store.latest_preaggregate(calls[0])['covered_at'] is None


def test_cross_cutover_plan_survives_already_advanced_ordinary_cursor(cutover):
    _, store, _, end = cutover
    store.enqueue_and_advance([], {'task_0': end + timedelta(days=2)})
    item = plan_for(cutover)['items'][0]
    due = datetime.fromisoformat(item['formal_scheduled_time'])
    assert end <= due < end + timedelta(minutes=15)


def test_cross_cutover_does_not_allow_window_before_global_coverage(cutover):
    _, store, start, _ = cutover
    store.set_meta('coverage_start', (start + timedelta(minutes=5)).isoformat())
    with pytest.raises(ValueError, match='BACKFILL_PREAGGREGATE_BEFORE_COVERAGE'):
        plan_for(cutover)


def test_cross_cutover_request_cannot_run_before_recovery_takes_effect(cutover):
    definition, store, start, end = cutover
    with pytest.raises(ValueError, match='BACKFILL_PREAGGREGATE_BEFORE_EFFECTIVE_START'):
        build_plan(definition, start.isoformat(), end.isoformat(), task='task_0',
                   now=CUTOVER - timedelta(minutes=1), preaggregate=True, store=store)
