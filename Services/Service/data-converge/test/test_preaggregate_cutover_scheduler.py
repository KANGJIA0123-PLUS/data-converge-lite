"""A dispatched crossing-window preview remains due across scheduler restarts."""
from dataclasses import replace
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

from data_converge.backfill.preaggregate import configuration_evidence
from data_converge.config.evolution import build_manifest, manifest_digest
from data_converge.dag import DAGExecutor, TaskStatus
from data_converge.scheduler import TaskScheduler
from .test_backfill_plan import definition_for
from .test_main import app
from .test_state import open_store


COVERAGE = datetime.fromisoformat('2026-10-16T19:35:00+08:00')
CUTOVER = datetime.fromisoformat('2026-10-21T10:50:00+08:00')


def retire_empty_history(store, definition):
    manifest = build_manifest(definition)
    revision = manifest_digest(manifest)
    store.set_meta('coverage_start', COVERAGE.isoformat())
    store.set_meta('binding', 'a' * 64)
    recovery = dict(schema_version=1, migration_id='cutover-1', cutover=CUTOVER.isoformat(),
                    previous_binding='a' * 64, current_binding='b' * 64, revision=revision,
                    backup_path='/backups/before.db', recovered_at=CUTOVER.isoformat(),
                    identity_verified=False, previous_definition_available=False,
                    historical_counts={})
    store.apply_configuration(expected_binding='a' * 64, expected_revision=None,
        manifest=manifest, revision=revision, task_starts={name: CUTOVER for name in definition.dag.tasks},
        binding='b' * 64, migration_id='cutover-1', applied_at=CUTOVER,
        backup_path='/backups/before.db', retire_existing=True, recovery=recovery)


def scheduler_for(setup, **options):
    definition, store = setup.definition, setup.store
    executor = DAGExecutor(definition.dag, lambda i: setup.calls.append(i.instance_id), state_store=store)
    executor.readiness_check = lambda instance, now: (
        'WAITING_DWD' if instance.taskname == 'task_0' else None)
    return TaskScheduler(definition.dag, definition.builder, executor, timezone='Asia/Shanghai',
                         state_store=store, **options)


@pytest.fixture(params=['PT1H', 'P1D'])
def crossing(app, request):
    interval = request.param
    parent = 'PT5M' if interval == 'PT1H' else 'PT1H'
    definition = definition_for(app, (parent, interval), dependency=True)
    child = replace(definition.dag.tasks['task_1'], cron='6 * * * *' if interval == 'PT1H' else '10 0 * * *')
    tasks = dict(definition.dag.tasks, task_1=child)
    definition = replace(definition, dag=replace(definition.dag, tasks=tasks))
    due = datetime.fromisoformat('2026-10-21T11:06:00+08:00' if interval == 'PT1H'
                                 else '2026-10-22T00:10:00+08:00')
    store = open_store(app.parent / 'state.sqlite3')
    retire_empty_history(store, definition)
    setup = SimpleNamespace(definition=definition, store=store, calls=[], due=due,
                            current=CUTOVER + timedelta(minutes=5))
    setup.instance = definition.builder.create_instance(definition.dag, 'task_1', due)
    evidence = configuration_evidence(definition, 'task_1')
    evidence['recovery_coverage'] = dict(migration_id='cutover-1', cutover=CUTOVER.isoformat(),
        task_effective_start=CUTOVER.isoformat(), coverage_start=COVERAGE.isoformat())
    setup.item = dict(item_id='early:0', revision='request-revision', config_evidence=evidence,
                      formal_scheduled_time=due.isoformat())
    try:
        yield setup
    finally:
        setup.store.close()


def dispatch_preview(setup, status=TaskStatus.SUCCESS):
    setup.store.begin_preaggregate(setup.item, setup.instance, now=setup.current)
    if status != TaskStatus.RUNNING:
        setup.store.finish_preaggregate(setup.item['item_id'], status, now=setup.current)


def reopen(setup):
    path = setup.store.path
    setup.store.close()
    setup.store = open_store(path, create=False)
    return scheduler_for(setup)


def tick(scheduler, now):
    scheduler.dag_executor._now = lambda instance: now
    return scheduler.tick(now)


def test_crossing_preview_waits_for_due_then_default_five_minutes(crossing):
    dispatch_preview(crossing)
    scheduler = scheduler_for(crossing)
    tick(scheduler, crossing.due - timedelta(seconds=1))
    assert crossing.store.get_instance(crossing.instance.instance_id) is None
    tick(scheduler, crossing.due)
    record = crossing.store.get_instance(crossing.instance.instance_id)
    assert record is not None and record['status'] == 'BLOCKED' and record['attempts'] == 0
    wait = crossing.store.get_cross_period_wait(crossing.instance.instance_id)
    assert wait['wait_seconds'] == 300
    assert datetime.fromisoformat(wait['deadline_at']) == crossing.due + timedelta(minutes=5)
    tick(scheduler, crossing.due + timedelta(minutes=5))
    assert crossing.calls == [crossing.instance.instance_id]
    assert crossing.store.get_preaggregate('early:0')['covered_at'] is not None
    assert crossing.store.get_task_effective_starts()['task_1'] == CUTOVER
    crossing.store.validate_retirement()


def test_restart_after_due_reconstructs_formal_window_past_cursor(crossing):
    dispatch_preview(crossing)
    future = crossing.due + timedelta(days=2)
    crossing.store.enqueue_and_advance([], {name: future for name in crossing.definition.dag.tasks})
    scheduler = reopen(crossing)
    tick(scheduler, crossing.due + timedelta(minutes=20))
    assert crossing.calls == [crossing.instance.instance_id]
    wait = crossing.store.get_cross_period_wait(crossing.instance.instance_id)
    assert datetime.fromisoformat(wait['scheduled_due_at']) == crossing.due
    assert datetime.fromisoformat(wait['deadline_at']) == crossing.due + timedelta(minutes=5)
    assert wait['status'] == 'TIMED_OUT'
    assert crossing.store.get_preaggregate('early:0')['covered_at'] is not None
    assert crossing.store.get_cursor('task_1') == future


def test_restart_during_wait_preserves_anchor_and_executes_once(crossing):
    dispatch_preview(crossing)
    tick(scheduler_for(crossing), crossing.due)
    scheduler = reopen(crossing)
    tick(scheduler, crossing.due + timedelta(minutes=4))
    assert crossing.calls == []
    tick(scheduler, crossing.due + timedelta(minutes=5))
    tick(scheduler, crossing.due + timedelta(minutes=6))
    assert crossing.calls == [crossing.instance.instance_id]
    assert crossing.store.get_instance(crossing.instance.instance_id)['attempts'] == 1


@pytest.mark.parametrize('options', [
    {'cross_period_wait_enabled': False},
    {'cross_period_wait_seconds': 0},
    {'cross_period_wait_overrides': {'task_1': 0}},
])
def test_crossing_coverage_honors_existing_wait_configuration(crossing, options):
    dispatch_preview(crossing)
    tick(scheduler_for(crossing, **options), crossing.due)
    assert crossing.calls == [crossing.instance.instance_id]
    assert crossing.store.get_preaggregate('early:0')['covered_at'] is not None


def test_crossing_recovered_unknown_requires_resolution_before_formal_write(crossing):
    dispatch_preview(crossing, TaskStatus.RUNNING)
    scheduler = reopen(crossing)
    crossing.store.recover_running()
    tick(scheduler, crossing.due + timedelta(minutes=5))
    assert crossing.calls == []
    assert crossing.store.get_instance(crossing.instance.instance_id)['attempts'] == 0
    assert crossing.store.get_preaggregate('early:0')['status'] == 'UNKNOWN'
    crossing.store.resolve_preaggregate('early:0', 'retry', 'Verified both remote operations ended')
    tick(scheduler, crossing.due + timedelta(minutes=6))
    assert crossing.calls == [crossing.instance.instance_id]
    assert crossing.store.get_preaggregate('early:0')['covered_at'] is not None


def test_same_cutover_without_dispatched_preview_stays_skipped(crossing):
    tick(scheduler_for(crossing), crossing.due + timedelta(minutes=5))
    assert crossing.calls == []
    assert crossing.store.get_instance(crossing.instance.instance_id) is None


def test_failed_early_write_still_requires_formal_coverage(crossing):
    dispatch_preview(crossing, TaskStatus.FAILED)
    tick(scheduler_for(crossing), crossing.due + timedelta(minutes=5))
    assert crossing.calls == [crossing.instance.instance_id]
    assert crossing.store.get_preaggregate('early:0')['covered_at'] is not None


def test_disabled_wait_does_not_advance_data_end_or_original_cron(crossing):
    dispatch_preview(crossing)
    scheduler = scheduler_for(crossing, cross_period_wait_enabled=False)
    tick(scheduler, crossing.instance.data_end - timedelta(seconds=1))
    tick(scheduler, crossing.instance.data_end + timedelta(seconds=1))
    assert crossing.calls == []
    assert crossing.store.get_instance(crossing.instance.instance_id) is None
    tick(scheduler, crossing.due)
    assert crossing.calls == [crossing.instance.instance_id]


def add_same_period_parent(setup):
    dag = setup.definition.dag
    peer = replace(dag.tasks['task_1'], taskname='peer', depends_on=())
    child = replace(dag.tasks['task_1'], depends_on=('task_0', 'peer'))
    tasks = dict(dag.tasks, peer=peer, task_1=child)
    dag = replace(dag, tasks=tasks, dependencies={name: task.depends_on for name, task in tasks.items()},
                  topological_order=('task_0', 'peer', 'task_1'))
    setup.definition = replace(setup.definition, dag=dag)
    manifest = build_manifest(setup.definition)
    previous = setup.store.configuration_snapshot()
    setup.store.apply_configuration(expected_binding=setup.store.get_meta('binding'),
        expected_revision=previous['revision'], manifest=manifest, revision=manifest_digest(manifest),
        task_starts={name: CUTOVER for name in dag.tasks}, binding='b' * 64,
        migration_id='with-peer', applied_at=CUTOVER)
    setup.instance = setup.definition.builder.create_instance(dag, 'task_1', setup.due)
    marker = setup.item['config_evidence']['recovery_coverage']
    setup.item['config_evidence'] = configuration_evidence(setup.definition, 'task_1')
    setup.item['config_evidence']['recovery_coverage'] = marker
    return setup.definition.builder.create_instance(dag, 'peer', setup.due)


def test_crossing_formal_coverage_retains_same_period_unknown_gate(crossing):
    peer = add_same_period_parent(crossing)
    dispatch_preview(crossing)
    crossing.store.begin_backfill('peer:0', peer, recompute=True, now=crossing.due)
    crossing.store.finish(peer.instance_id, TaskStatus.UNKNOWN, now=crossing.due)
    tick(scheduler_for(crossing), crossing.due + timedelta(minutes=20))
    assert crossing.calls == []
    record = crossing.store.get_instance(crossing.instance.instance_id)
    assert record['status'] == 'BLOCKED' and record['attempts'] == 0
    assert peer.instance_id in record['error']
    assert crossing.store.get_cross_period_wait(crossing.instance.instance_id)['checked_at'] is None
    assert crossing.store.get_preaggregate('early:0')['covered_at'] is None


def test_crossing_failed_formal_attempt_retries_after_restart(crossing):
    from data_converge.errors import TaskExecutionError
    dispatch_preview(crossing)
    scheduler = scheduler_for(crossing)
    def fail(instance):
        raise TaskExecutionError('SQL_FAILED', retryable=True)
    scheduler.dag_executor.runner = fail
    tick(scheduler, crossing.due + timedelta(minutes=5))
    first = crossing.store.get_instance(crossing.instance.instance_id)
    assert first['status'] == 'FAILED' and first['attempts'] == 1
    assert crossing.store.get_preaggregate('early:0')['covered_at'] is None
    scheduler = reopen(crossing)
    tick(scheduler, crossing.due + timedelta(minutes=6))
    assert crossing.calls == [crossing.instance.instance_id]
    second = crossing.store.get_instance(crossing.instance.instance_id)
    assert second['status'] == 'SUCCESS' and second['attempts'] == 2
    assert second['query_id'] != first['query_id']
    assert crossing.store.get_preaggregate('early:0')['covered_at'] is not None


def test_crossing_unknown_formal_attempt_is_not_replayed_after_restart(crossing):
    dispatch_preview(crossing)
    scheduler = scheduler_for(crossing)
    def fail(instance):
        raise RuntimeError('remote outcome lost')
    scheduler.dag_executor.runner = fail
    tick(scheduler, crossing.due + timedelta(minutes=5))
    first = crossing.store.get_instance(crossing.instance.instance_id)
    assert first['status'] == 'UNKNOWN' and first['attempts'] == 1
    scheduler = reopen(crossing)
    tick(scheduler, crossing.due + timedelta(minutes=6))
    assert crossing.calls == []
    assert crossing.store.get_instance(crossing.instance.instance_id) == first
    assert crossing.store.get_preaggregate('early:0')['covered_at'] is None
