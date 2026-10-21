"""Recovery-bound early writes durably authorize one exact formal window."""
from contextlib import closing
from copy import deepcopy
from datetime import datetime, timedelta

import pytest

from data_converge.dag.models import TaskDefinition, TaskInstance, TaskStatus
from data_converge.state import StateStore
from .test_state import open_store

CUTOVER = datetime.fromisoformat('2026-10-21T10:50:00+08:00')
COVERAGE = datetime.fromisoformat('2026-10-16T19:35:00+08:00')
NOW = CUTOVER + timedelta(minutes=5)


def candidate(interval='P1D', item_id='early:0'):
    start = CUTOVER.replace(minute=0)
    end = start + timedelta(hours=1)
    delay = 6
    if interval == 'P1D':
        start = start.replace(hour=0)
        end, delay = start + timedelta(days=1), 10
    task = TaskDefinition('root', '', ('raw',), 'rollup', interval,
                          '6 * * * *' if interval == 'PT1H' else '10 0 * * *', (), 'root')
    instance = TaskInstance(task, end.isoformat(), end + timedelta(minutes=delay), start, end, ())
    evidence = {'task': {'definition': {'taskname': 'root', 'aggregation_interval': interval},
                         'revision': 'task-v1'}, 'connection': {}, 'dataset_id': None,
                'timezone': 'Asia/Shanghai', 'cleanup_column': 'window_time'}
    item = {'item_id': item_id, 'revision': 'request-v1', 'config_evidence': evidence,
            'formal_scheduled_time': instance.scheduled_time.isoformat()}
    return item, instance


def recover_store(store, manifest, cutover=CUTOVER, coverage=COVERAGE):
    store.set_meta('binding', 'a' * 64)
    store.set_meta('coverage_start', coverage.isoformat())
    recovery = dict(schema_version=1, migration_id='recovery-1', cutover=cutover.isoformat(),
                    previous_binding='a' * 64, current_binding='b' * 64, revision='c' * 64,
                    backup_path='/backups/before.sqlite3', recovered_at=cutover.isoformat(),
                    identity_verified=False, previous_definition_available=False,
                    historical_counts=store.status_counts())
    store.apply_configuration(expected_binding='a' * 64, expected_revision=None,
        manifest=manifest, revision='c' * 64, task_starts={name: cutover for name in manifest['tasks']},
        binding='b' * 64, migration_id='recovery-1', applied_at=cutover,
        backup_path=recovery['backup_path'], retire_existing=True, recovery=recovery)


def recovered(tmp_path, interval='P1D'):
    store = open_store(tmp_path / 'state.sqlite3')
    item, instance = candidate(interval)
    evidence = item['config_evidence']
    manifest = {key: evidence[key] for key in ('connection', 'dataset_id')}
    manifest['tasks'] = {'root': evidence['task']}
    recover_store(store, manifest)
    item['config_evidence']['recovery_coverage'] = {
        'migration_id': 'recovery-1', 'cutover': CUTOVER.isoformat(),
        'task_effective_start': CUTOVER.isoformat(), 'coverage_start': COVERAGE.isoformat()}
    return store, item, instance, manifest


def dispatch(store, item, instance, status=TaskStatus.SUCCESS):
    assert store.begin_preaggregate(item, instance, now=NOW)['status'] == 'RUNNING'
    store.finish_preaggregate(item['item_id'], status, now=NOW)


@pytest.mark.parametrize('interval', ['PT1H', 'P1D'])
def test_recovery_admission_uses_actual_cutover_without_changing_ledger(tmp_path, interval):
    store, item, instance, _ = recovered(tmp_path, interval)
    with closing(store):
        before = list(store._connection().iterdump())
        assert store.preaggregate_recovery_coverage(instance, NOW) == item['config_evidence']['recovery_coverage']
        assert list(store._connection().iterdump()) == before


@pytest.mark.parametrize('interval', ['PT1H', 'P1D'])
def test_dispatched_coverage_survives_restart_and_cursor_advancement(tmp_path, interval):
    store, item, instance, manifest = recovered(tmp_path, interval)
    with closing(store):
        dispatch(store, item, instance)
        store.enqueue_and_advance([], {'root': instance.scheduled_time + timedelta(days=2)})
    with closing(open_store(store.path, create=False)) as reopened:
        assert reopened.preaggregate_covers_window(instance)
        rows = reopened.pending_preaggregate_coverages()
        assert [row['item_id'] for row in rows] == [item['item_id']]
        reopened.validate_preaggregate_configuration(manifest, reopened.get_task_effective_starts(),
            reopened.get_meta('coverage_start'), timezone='Asia/Shanghai', cleanup_columns={'root': 'window_time'})
        reopened.enqueue_and_advance([instance], {})
        assert reopened.claim(instance.instance_id, now=instance.scheduled_time)
        reopened.finish(instance.instance_id, TaskStatus.SUCCESS, now=instance.scheduled_time)
        assert reopened.pending_preaggregate_coverages() == []
        assert not reopened.preaggregate_covers_window(instance)
        assert reopened.get_preaggregate(item['item_id'])['covered_at']
        assert reopened.get_task_effective_starts()['root'] == CUTOVER
        reopened.validate_retirement()


def test_coverage_requires_dispatch_and_exact_formal_instance(tmp_path):
    store, item, instance, _ = recovered(tmp_path)
    with closing(store):
        assert store.pending_preaggregate_coverages() == []
        assert not store.preaggregate_covers_window(instance)
        with pytest.raises(ValueError, match='TASK_WINDOW_BEFORE_EFFECTIVE_START'):
            store.enqueue_and_advance([instance], {})
        dispatch(store, item, instance)
        other = deepcopy(instance)
        other.scheduled_time += timedelta(minutes=1)
        assert not store.preaggregate_covers_window(other)
        other = deepcopy(instance)
        other.data_start += timedelta(minutes=1)
        assert not store.preaggregate_covers_window(other)


def test_multiple_attempts_authorize_one_window_and_unknown_remains_blocked(tmp_path):
    store, item, instance, _ = recovered(tmp_path)
    with closing(store):
        dispatch(store, item, instance)
        second = item | {'item_id': 'early:1'}
        dispatch(store, second, instance, TaskStatus.UNKNOWN)
        assert len(store.pending_preaggregate_coverages()) == 1
        store.enqueue_and_advance([instance], {})
        assert not store.claim(instance.instance_id, now=instance.scheduled_time)
        assert store.preaggregate_covers_window(instance)
        store.resolve_preaggregate(second['item_id'], 'retry', 'Both remote queries have ended')
        assert store.claim(instance.instance_id, now=instance.scheduled_time)


@pytest.mark.parametrize('field', ['migration_id', 'cutover', 'task_effective_start', 'coverage_start'])
def test_tampered_coverage_marker_cannot_dispatch(tmp_path, field):
    store, item, instance, _ = recovered(tmp_path)
    with closing(store):
        item['config_evidence']['recovery_coverage'][field] = 'wrong'
        with pytest.raises(ValueError, match='PREAGGREGATE_RECOVERY_COVERAGE_INVALID'):
            store.begin_preaggregate(item, instance, now=NOW)
        assert store.get_preaggregate(item['item_id']) is None


@pytest.mark.parametrize('changed', ['effective', 'coverage', 'manifest'])
def test_configuration_change_cannot_abandon_recovery_coverage(tmp_path, changed):
    store, item, instance, manifest = recovered(tmp_path)
    with closing(store):
        dispatch(store, item, instance)
        starts, coverage = store.get_task_effective_starts(), store.get_meta('coverage_start')
        if changed == 'effective':
            starts['root'] += timedelta(minutes=5)
        elif changed == 'coverage':
            coverage = (COVERAGE + timedelta(minutes=5)).isoformat()
        else:
            manifest['tasks']['root']['revision'] = 'different'
        with pytest.raises(ValueError, match='PREAGGREGATE_COVERAGE'):
            store.validate_preaggregate_configuration(manifest, starts, coverage)


def test_missing_recovery_or_unsupported_interval_cannot_authorize(tmp_path):
    item, instance = candidate()
    with closing(open_store(tmp_path / 'plain.sqlite3')) as store:
        store.set_meta('coverage_start', COVERAGE.isoformat())
        assert store.preaggregate_recovery_coverage(instance, NOW) is None
    store, item, instance, _ = recovered(tmp_path)
    with closing(store):
        instance.task = TaskDefinition('root', '', ('raw',), 'rollup', 'P1M', '10 0 1 * *', (), 'root')
        assert store.preaggregate_recovery_coverage(instance, NOW) is None
        instance.task = candidate()[1].task
        assert store.preaggregate_recovery_coverage(instance, CUTOVER - timedelta(seconds=1)) is None
        assert store.preaggregate_recovery_coverage(instance, instance.data_end) is None


def test_retired_identity_cannot_receive_recovery_permission(tmp_path):
    item, instance = candidate()
    with closing(open_store(tmp_path / 'state.sqlite3')) as store:
        # An old task definition may already have recorded this canonical ID.
        retired = deepcopy(instance)
        retired.data_end = CUTOVER
        retired.data_start = CUTOVER - timedelta(hours=1)
        store.enqueue_and_advance([retired], {})
        evidence = item['config_evidence']
        manifest = {'tasks': {'root': evidence['task']}, 'connection': {}, 'dataset_id': None}
        recover_store(store, manifest)
        assert store.get_retirement(instance.instance_id)
        assert store.preaggregate_recovery_coverage(instance, NOW) is None
        assert store.begin_preaggregate(item, instance, now=NOW)['status'] == 'BLOCKED'
        assert store.pending_preaggregate_coverages() == []


@pytest.mark.parametrize('change', ['coverage_inside_window', 'later_task_start', 'window_after_cutover'])
def test_recovery_permission_does_not_widen_other_boundaries(tmp_path, change):
    store, item, instance, _ = recovered(tmp_path)
    with closing(store):
        if change == 'coverage_inside_window':
            store.set_meta('coverage_start', (instance.data_start + timedelta(hours=1)).isoformat())
        elif change == 'later_task_start':
            current = store.configuration_snapshot()
            current['task_starts']['root'] = (CUTOVER + timedelta(minutes=5)).isoformat()
            import json
            store.set_meta('configuration_snapshot', json.dumps(current))
        else:
            instance.data_start = CUTOVER
        assert store.preaggregate_recovery_coverage(instance, NOW) is None


def test_failed_dispatch_remains_obliged_but_expired_never_dispatched_item_does_not(tmp_path):
    store, item, instance, _ = recovered(tmp_path)
    with closing(store):
        dispatch(store, item, instance, TaskStatus.FAILED)
        expired = item | {'item_id': 'expired:0'}
        result = store.begin_preaggregate(expired, instance, now=instance.data_end)
        assert result['status'] == 'SKIPPED'
        assert result['query_id'] is None
        assert [row['item_id'] for row in store.pending_preaggregate_coverages()] == [item['item_id']]


def test_plain_ledger_coverage_reads_are_side_effect_free(tmp_path):
    path = tmp_path / 'state.sqlite3'
    open_store(path).close()
    before = path.read_bytes()
    with closing(StateStore(path)) as store:
        assert store.pending_preaggregate_coverages() == []
        assert not store.preaggregate_covers_window(candidate()[1])
    assert path.read_bytes() == before
