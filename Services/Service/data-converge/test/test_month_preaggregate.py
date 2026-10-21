"""Current-month previews retain an exact durable obligation for the formal run."""
from dataclasses import replace
from copy import deepcopy
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

from data_converge.backfill.plan import build_plan
from data_converge.backfill.queue import Queue
from data_converge.backfill.worker import BackfillWorker
from data_converge.config.evolution import build_manifest, manifest_digest
from data_converge.dag import TaskStatus
from data_converge.errors import TaskExecutionError
from .test_backfill_plan import definition_for
from .test_main import app
from .test_preaggregate_cutover_scheduler import (
    COVERAGE, CUTOVER, reopen, retire_empty_history, scheduler_for, tick,
)
from .test_state import open_store


START = datetime.fromisoformat('2026-10-01T00:00:00+08:00')
END = datetime.fromisoformat('2026-11-01T00:00:00+08:00')
DUE = END + timedelta(minutes=10)
NOW = CUTOVER + timedelta(minutes=5)


def initialize_month(store, definition, mode):
    if mode == 'recovered':
        retire_empty_history(store, definition)
        return
    store.set_meta('coverage_start', COVERAGE.isoformat())
    if mode == 'initialized':
        manifest = build_manifest(definition)
        store.apply_configuration(expected_binding=None, expected_revision=None,
            manifest=manifest, revision=manifest_digest(manifest), binding='b' * 64,
            task_starts={name: COVERAGE for name in definition.dag.tasks},
            migration_id='initial-month', applied_at=COVERAGE)


@pytest.fixture(params=['initial', 'initialized', 'recovered'])
def month(app, request):
    definition = definition_for(app, ('P1D', 'P1M'), dependency=True)
    task = replace(definition.dag.tasks['task_1'], cron='10 0 1 * *')
    dag = replace(definition.dag, tasks=dict(definition.dag.tasks, task_1=task))
    definition = replace(definition, dag=dag)
    store = open_store(app.parent / 'state.sqlite3')
    initialize_month(store, definition, request.param)
    setup = SimpleNamespace(definition=definition, store=store, calls=[], due=DUE,
                            current=NOW, mode=request.param)
    setup.instance = definition.builder.create_instance(dag, 'task_1', DUE)
    try:
        yield setup
    finally:
        setup.store.close()


def month_plan(setup, now=NOW):
    return build_plan(setup.definition, START.isoformat(), END.isoformat(),
                      task='task_1', now=now, preaggregate=True, store=setup.store)


def dispatch_month(setup, status=TaskStatus.SUCCESS):
    plan = month_plan(setup)
    setup.item = dict(plan['items'][0], item_id='month:0', revision=plan['revision'])
    result = setup.store.begin_preaggregate(setup.item, setup.instance, now=NOW)
    assert result['status'] == 'RUNNING'
    if status != TaskStatus.RUNNING:
        setup.store.finish_preaggregate(setup.item['item_id'], status, now=NOW)


def advance_month_cursors(setup):
    future = datetime.fromisoformat('2026-12-01T00:10:00+08:00')
    setup.store.enqueue_and_advance([], {name: future for name in setup.definition.dag.tasks})
    return future


def test_month_plan_accepts_midmonth_coverage_without_changing_boundaries(month):
    before = month.store.configuration_snapshot()
    item = month_plan(month)['items'][0]
    assert item['execution_kind'] == 'PREAGGREGATE'
    assert (item['data_start'], item['data_end']) == (START.isoformat(), END.isoformat())
    assert item['formal_scheduled_time'] == DUE.isoformat()
    assert item['upstream_instance_ids'] == []
    assert month.store.configuration_snapshot() == before
    assert month.store.get_meta('coverage_start') == COVERAGE.isoformat()
    assert month.store.get_cursor('task_1') is None
    if month.mode != 'recovered':
        assert month.store.legacy_recovery() is None


def test_month_preview_records_formal_obligation_before_external_sql(month):
    plan = month_plan(month)
    calls = []
    def run(instance):
        pending = month.store.pending_preaggregate_coverages()
        assert len(pending) == 1 and pending[0]['status'] == 'RUNNING'
        assert pending[0]['query_id'] == instance.query_id
        assert instance.data_start == START and instance.data_end == END
        calls.append(instance.instance_id)
    with Queue(month.store.path.parent / 'backfill.sqlite3', create=True) as queue:
        job = queue.submit(plan)
        BackfillWorker(month.definition, month.store, run, clock=lambda: NOW).run_one(queue)
        assert queue.status(job)['counts'] == {'SUCCESS': 1}
    assert calls == [month.instance.instance_id]
    assert month.store.get_instance(month.instance.instance_id) is None
    assert month.store.latest_preaggregate(month.instance.instance_id)['covered_at'] is None


def test_month_plan_keeps_original_due_even_when_cursor_has_advanced(month):
    future = advance_month_cursors(month)
    item = month_plan(month)['items'][0]
    assert item['formal_scheduled_time'] == DUE.isoformat()
    assert month.store.get_cursor('task_1') == future


def test_month_wait_starts_at_actual_cron_and_survives_restart(month):
    dispatch_month(month)
    future = advance_month_cursors(month)
    scheduler = reopen(month)
    tick(scheduler, END)
    tick(scheduler, DUE - timedelta(seconds=1))
    assert month.store.get_instance(month.instance.instance_id) is None
    tick(scheduler, DUE)
    record = month.store.get_instance(month.instance.instance_id)
    assert record['status'] == 'BLOCKED' and record['attempts'] == 0
    wait = month.store.get_cross_period_wait(month.instance.instance_id)
    assert wait['wait_seconds'] == 300
    assert datetime.fromisoformat(wait['scheduled_due_at']) == DUE
    assert datetime.fromisoformat(wait['deadline_at']) == DUE + timedelta(minutes=5)
    scheduler = reopen(month)
    tick(scheduler, DUE + timedelta(minutes=4))
    assert month.calls == []
    assert month.store.get_cross_period_wait(month.instance.instance_id)['deadline_at'] == wait['deadline_at']
    tick(scheduler, DUE + timedelta(minutes=5))
    tick(scheduler, DUE + timedelta(minutes=6))
    assert month.calls == [month.instance.instance_id]
    assert month.store.get_instance(month.instance.instance_id)['attempts'] == 1
    assert month.store.get_preaggregate('month:0')['covered_at'] is not None
    assert month.store.get_cursor('task_1') == future
    assert month.store.get_meta('coverage_start') == COVERAGE.isoformat()
    month.store.validate_retirement()


def test_month_restart_after_deadline_executes_formal_window_past_cursor(month):
    dispatch_month(month)
    future = advance_month_cursors(month)
    tick(reopen(month), DUE + timedelta(minutes=20))
    assert month.calls == [month.instance.instance_id]
    wait = month.store.get_cross_period_wait(month.instance.instance_id)
    assert datetime.fromisoformat(wait['deadline_at']) == DUE + timedelta(minutes=5)
    assert wait['status'] == 'TIMED_OUT'
    assert month.store.get_preaggregate('month:0')['covered_at'] is not None
    assert month.store.get_cursor('task_1') == future


def test_month_failed_preview_still_requires_successful_formal_replacement(month):
    dispatch_month(month, TaskStatus.FAILED)
    tick(scheduler_for(month), DUE + timedelta(minutes=5))
    assert month.calls == [month.instance.instance_id]
    assert month.store.get_preaggregate('month:0')['status'] == 'FAILED'
    assert month.store.get_preaggregate('month:0')['covered_at'] is not None


def test_month_unknown_preview_survives_restart_and_blocks_formal_sql(month):
    dispatch_month(month, TaskStatus.RUNNING)
    advance_month_cursors(month)
    scheduler = reopen(month)
    month.store.recover_running()
    tick(scheduler, DUE + timedelta(minutes=5))
    assert month.calls == []
    assert month.store.get_instance(month.instance.instance_id)['attempts'] == 0
    assert month.store.get_preaggregate('month:0')['status'] == 'UNKNOWN'
    assert month.store.get_preaggregate('month:0')['covered_at'] is None
    month.store.resolve_preaggregate('month:0', 'retry', 'Confirmed both remote operations ended')
    tick(scheduler, DUE + timedelta(minutes=6))
    assert month.calls == [month.instance.instance_id]
    assert month.store.get_preaggregate('month:0')['covered_at'] is not None


def test_month_failed_formal_attempt_retries_after_restart(month):
    dispatch_month(month)
    scheduler = scheduler_for(month)
    def fail(instance):
        raise TaskExecutionError('SQL_FAILED', retryable=True)
    scheduler.dag_executor.runner = fail
    tick(scheduler, DUE + timedelta(minutes=5))
    first = month.store.get_instance(month.instance.instance_id)
    assert first['status'] == 'FAILED' and first['attempts'] == 1
    assert month.store.get_preaggregate('month:0')['covered_at'] is None
    tick(reopen(month), DUE + timedelta(minutes=6))
    assert month.calls == [month.instance.instance_id]
    second = month.store.get_instance(month.instance.instance_id)
    assert second['status'] == 'SUCCESS' and second['attempts'] == 2
    assert second['query_id'] != first['query_id']
    assert month.store.get_preaggregate('month:0')['covered_at'] is not None


def test_month_unknown_formal_attempt_is_never_replayed_automatically(month):
    dispatch_month(month)
    scheduler = scheduler_for(month)
    def fail(instance):
        raise RuntimeError('remote result unavailable')
    scheduler.dag_executor.runner = fail
    tick(scheduler, DUE + timedelta(minutes=5))
    first = month.store.get_instance(month.instance.instance_id)
    assert first['status'] == 'UNKNOWN' and first['attempts'] == 1
    tick(reopen(month), DUE + timedelta(minutes=6))
    assert month.calls == []
    assert month.store.get_instance(month.instance.instance_id) == first
    assert month.store.get_preaggregate('month:0')['covered_at'] is None


def test_month_without_dispatched_preview_does_not_bypass_coverage(month):
    month_plan(month)
    tick(scheduler_for(month), DUE + timedelta(minutes=5))
    assert month.calls == []
    assert month.store.get_instance(month.instance.instance_id) is None


def test_month_preview_cannot_run_before_coverage_begins(month):
    with pytest.raises(ValueError, match='BACKFILL_PREAGGREGATE_BEFORE_'):
        month_plan(month, COVERAGE - timedelta(seconds=1))
    assert month.store.pending_preaggregate_coverages() == []


@pytest.mark.parametrize('month', ['recovered'], indirect=True)
def test_month_recovered_preview_cannot_run_before_cutover(month):
    with pytest.raises(ValueError, match='BACKFILL_PREAGGREGATE_BEFORE_'):
        month_plan(month, CUTOVER - timedelta(seconds=1))
    assert month.store.pending_preaggregate_coverages() == []


@pytest.mark.parametrize('change', ['missing', 'tampered'])
def test_month_dispatch_rejects_missing_or_tampered_obligation(month, change):
    plan = month_plan(month)
    item = dict(plan['items'][0], item_id='month:0', revision=plan['revision'])
    item['config_evidence'] = deepcopy(item['config_evidence'])
    if change == 'missing':
        item['config_evidence'].pop('recovery_coverage', None)
    else:
        item['config_evidence']['recovery_coverage'] = {'tampered': True}
    with pytest.raises(ValueError, match='PREAGGREGATE_.*INVALID'):
        month.store.begin_preaggregate(item, month.instance, now=NOW)
    assert month.store.get_preaggregate('month:0') is None
    assert month.store.pending_preaggregate_coverages() == []


@pytest.mark.parametrize('change', ['coverage', 'effective', 'manifest'])
def test_month_configuration_cannot_abandon_dispatched_obligation(month, change):
    dispatch_month(month)
    manifest = build_manifest(month.definition)
    starts = month.store.get_task_effective_starts()
    coverage = month.store.get_meta('coverage_start')
    before = month.store.configuration_snapshot()
    if change == 'coverage':
        coverage = (COVERAGE + timedelta(minutes=1)).isoformat()
    elif change == 'effective':
        starts['task_1'] = NOW
    else:
        manifest['tasks']['task_1']['revision'] = 'changed-month-sql'
    with pytest.raises(ValueError, match='PREAGGREGATE_COVERAGE'):
        month.store.validate_preaggregate_configuration(manifest, starts, coverage,
            timezone='Asia/Shanghai', cleanup_columns={'task_1': month.instance.task.cleanup_column})
    assert month.store.configuration_snapshot() == before
    assert month.store.get_preaggregate('month:0')['covered_at'] is None


@pytest.mark.parametrize('field', ['data_start', 'data_end', 'scheduled_time'])
def test_month_obligation_only_covers_exact_formal_window(month, field):
    dispatch_month(month)
    candidate = deepcopy(month.instance)
    setattr(candidate, field, getattr(candidate, field) + timedelta(minutes=1))
    assert not month.store.preaggregate_covers_window(candidate)
    assert month.store.preaggregate_covers_window(month.instance)
