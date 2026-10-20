"""Manual ranges select complete data windows in the deployment timezone."""

from dataclasses import replace
from datetime import datetime, timedelta
import json
from types import SimpleNamespace

import pytest

from .test_configuration_manifest import definition_with_tasks, task_rule
from .test_main import app


NOW = datetime.fromisoformat('2026-12-31T00:00:00+00:00')


def definition_for(app, intervals=('PT5M',), *, zone='Asia/Shanghai', dependency=False):
    rules = []
    for index, interval in enumerate(intervals):
        rule = task_rule(f'task_{index}', ['task_0'] if dependency and index else [])
        rule['aggregation_interval'] = interval
        rule['cron'] = {'PT5M': '*/5 * * * *', 'PT1H': '0 * * * *',
                        'P1D': '0 0 * * *', 'P1M': '0 0 1 * *'}[interval]
        rules.append(rule)
    definition = definition_with_tasks(app, rules)
    return replace(definition, options=SimpleNamespace(timezone=zone))


def plan(definition, start, end, **kwargs):
    from data_converge.backfill.plan import build_plan

    return build_plan(definition, start, end, now=NOW, **kwargs)


def test_single_task_uses_half_open_data_range_and_serializable_manifest_revision(app):
    from data_converge.backfill.plan import plan_revision

    definition = definition_for(app)
    result = plan(definition, '2026-10-20T10:00:00+08:00',
                  '2026-10-20T10:10:00+08:00', task='task_0', recompute=True)
    assert result['schema_version'] == 1
    assert result['revision'] == plan_revision(definition)
    assert result['recompute'] is True
    assert result['selected_tasks'] == ['task_0']
    assert result['omitted_tasks'] == []
    assert result['counts'] == {'task_0': 2}
    assert result['total'] == 2
    assert result['items'] == [{
        'ordinal': index, 'taskname': 'task_0',
        'instance_id': f'task_0@2026-10-20T10:{end:02}:00+08:00',
        'scheduled_time': f'2026-10-20T10:{end:02}:00+08:00',
        'data_start': f'2026-10-20T10:{end - 5:02}:00+08:00',
        'data_end': f'2026-10-20T10:{end:02}:00+08:00',
        'upstream_instance_ids': [],
    } for index, end in enumerate((5, 10))]
    assert json.loads(json.dumps(result)) == result
    assert not (app.parent / 'state').exists()


def test_offsets_are_converted_to_deployment_timezone(app):
    result = plan(definition_for(app), '2026-10-20T02:00:00Z',
                  '2026-10-20T02:05:00+00:00', task='task_0')
    assert result['start'] == '2026-10-20T10:00:00+08:00'
    assert result['end'] == '2026-10-20T10:05:00+08:00'
    assert result['items'][0]['data_start'] == result['start']
    assert result['items'][0]['data_end'] == result['end']
    assert result['recompute'] is False


@pytest.mark.parametrize('kwargs, code', [
    ({}, 'BACKFILL_SELECTION_REQUIRED'),
    ({'task': 'task_0', 'all_tasks': True}, 'BACKFILL_SELECTION_CONFLICT'),
    ({'task': 'missing'}, 'BACKFILL_TASK_NOT_FOUND'),
])
def test_invalid_task_selection_is_rejected(app, kwargs, code):
    with pytest.raises(ValueError, match=code):
        plan(definition_for(app), '2026-10-20T10:00:00+08:00',
             '2026-10-20T11:00:00+08:00', **kwargs)


@pytest.mark.parametrize('start, end, code', [
    ('2026-10-20T10:00:00', '2026-10-20T11:00:00+08:00',
     'BACKFILL_TIMEZONE_REQUIRED'),
    ('2026-10-20T10:00:00+08:00', '2026-10-20T11:00:00',
     'BACKFILL_TIMEZONE_REQUIRED'),
    ('not-a-time', '2026-10-20T11:00:00+08:00', 'BACKFILL_TIME_INVALID'),
    ('2026-10-20T11:00:00+08:00', '2026-10-20T10:00:00+08:00',
     'BACKFILL_RANGE_INVALID'),
    ('2026-10-20T10:00:00+08:00', '2026-10-20T10:00:00+08:00',
     'BACKFILL_RANGE_INVALID'),
    ('2026-12-31T00:00:00+00:00', '2026-12-31T00:05:00+00:00',
     'BACKFILL_END_IN_FUTURE'),
])
def test_invalid_times_are_rejected(app, start, end, code):
    with pytest.raises(ValueError, match=code):
        plan(definition_for(app), start, end, task='task_0')


@pytest.mark.parametrize('interval, start, end', [
    ('PT5M', '2026-10-20T10:01:00+08:00', '2026-10-20T10:10:00+08:00'),
    ('PT5M', '2026-10-20T10:00:00+08:00', '2026-10-20T10:06:00+08:00'),
    ('PT1H', '2026-10-20T10:00:00+08:00', '2026-10-20T11:00:01+08:00'),
    ('P1D', '2026-10-20T01:00:00+08:00', '2026-10-21T00:00:00+08:00'),
    ('P1M', '2026-10-01T00:00:00+08:00', '2026-10-20T00:00:00+08:00'),
])
def test_single_task_rejects_partial_windows(app, interval, start, end):
    with pytest.raises(ValueError, match='BACKFILL_BOUNDARY_NOT_ALIGNED'):
        plan(definition_for(app, (interval,)), start, end, task='task_0')


def test_all_tasks_omits_incomplete_larger_periods_without_expanding_range(app):
    definition = definition_for(app, ('PT5M', 'PT1H', 'P1D', 'P1M'))
    result = plan(definition, '2026-10-20T10:01:00+08:00',
                  '2026-10-20T11:06:00+08:00', all_tasks=True)
    assert result['selected_tasks'] == ['task_0', 'task_1', 'task_2', 'task_3']
    assert result['omitted_tasks'] == ['task_1', 'task_2', 'task_3']
    assert result['counts'] == {'task_0': 12, 'task_1': 0, 'task_2': 0, 'task_3': 0}
    assert result['items'][0]['data_start'] == '2026-10-20T10:05:00+08:00'
    assert result['items'][-1]['data_end'] == '2026-10-20T11:05:00+08:00'


def test_empty_all_task_range_is_explicit_error(app):
    with pytest.raises(ValueError, match='BACKFILL_EMPTY_RANGE'):
        plan(definition_for(app), '2026-10-20T10:01:00+08:00',
             '2026-10-20T10:04:00+08:00', all_tasks=True)


def test_tasks_run_in_topological_then_window_order_and_keep_same_period_dependencies(app):
    definition = definition_for(app, ('PT5M', 'PT5M'), dependency=True)
    result = plan(definition, '2026-10-20T10:00:00+08:00',
                  '2026-10-20T10:10:00+08:00', all_tasks=True)
    assert [item['taskname'] for item in result['items']] == [
        'task_0', 'task_0', 'task_1', 'task_1']
    assert [item['ordinal'] for item in result['items']] == list(range(4))
    assert result['items'][2]['upstream_instance_ids'] == [
        'task_0@2026-10-20T10:05:00+08:00']


def test_selecting_one_task_does_not_implicitly_recompute_its_dependencies(app):
    definition = definition_for(app, ('PT5M', 'PT1H'), dependency=True)
    result = plan(definition, '2026-10-20T10:00:00+08:00',
                  '2026-10-20T11:00:00+08:00', task='task_1')
    assert result['selected_tasks'] == ['task_1']
    assert result['total'] == 1
    assert result['items'][0]['upstream_instance_ids'] == []


@pytest.mark.parametrize('start, end, count', [
    ('2026-03-08T00:00:00-05:00', '2026-03-09T00:00:00-04:00', 23),
    ('2026-11-01T00:00:00-04:00', '2026-11-02T00:00:00-05:00', 25),
])
def test_hour_windows_follow_real_instants_across_dst(app, start, end, count):
    definition = definition_for(app, ('PT1H',), zone='America/New_York')
    result = plan(definition, start, end, task='task_0')
    assert result['total'] == count
    assert len({item['instance_id'] for item in result['items']}) == count
    assert result['items'][0]['data_start'] == start
    assert result['items'][-1]['data_end'] == end
    for before, after in zip(result['items'], result['items'][1:]):
        assert before['data_end'] == after['data_start']


def test_five_minute_windows_keep_both_repeated_hours(app):
    definition = definition_for(app, zone='America/New_York')
    result = plan(definition, '2026-11-01T01:00:00-04:00',
                  '2026-11-01T02:00:00-05:00', task='task_0')
    assert result['total'] == 24
    assert result['items'][11]['data_end'] == '2026-11-01T01:00:00-05:00'
    assert len({item['instance_id'] for item in result['items']}) == 24


@pytest.mark.parametrize('interval, start, end, expected', [
    ('P1D', '2026-03-08T00:00:00-05:00', '2026-03-10T00:00:00-04:00', 2),
    ('P1M', '2026-02-01T00:00:00-05:00', '2026-04-01T00:00:00-04:00', 2),
])
def test_calendar_windows_use_local_boundaries(app, interval, start, end, expected):
    definition = definition_for(app, (interval,), zone='America/New_York')
    result = plan(definition, start, end, task='task_0')
    assert result['total'] == expected
    assert result['items'][0]['data_start'] == start
    assert result['items'][-1]['data_end'] == end


def test_range_exceeding_ten_thousand_windows_is_rejected(app):
    definition = definition_for(app)
    start = datetime.fromisoformat('2026-01-01T00:00:00+08:00')
    with pytest.raises(ValueError, match='BACKFILL_ITEM_LIMIT_EXCEEDED'):
        plan(definition, start.isoformat(), (start + timedelta(minutes=50005)).isoformat(),
             task='task_0')


def test_total_limit_counts_every_selected_task(app):
    definition = definition_for(app, ('PT5M', 'PT5M'))
    start = datetime.fromisoformat('2026-01-01T00:00:00+08:00')
    with pytest.raises(ValueError, match='BACKFILL_ITEM_LIMIT_EXCEEDED'):
        plan(definition, start.isoformat(), (start + timedelta(minutes=25005)).isoformat(),
             all_tasks=True)


def test_revision_changes_with_execution_timezone_and_cleanup_column(app):
    from data_converge.backfill.plan import plan_revision

    definition = definition_for(app)
    original = plan_revision(definition)
    changed_zone = replace(definition, options=SimpleNamespace(timezone='UTC'))
    assert plan_revision(changed_zone) != original
    task = replace(definition.dag.tasks['task_0'], cleanup_column='record_time')
    changed_cleanup = replace(definition, dag=replace(definition.dag, tasks={'task_0': task}))
    assert plan_revision(changed_cleanup) != original


def test_revision_tracks_sql_and_database_but_excludes_runtime_secrets(app):
    from data_converge.backfill.plan import plan_revision

    definition = definition_for(app)
    original = plan_revision(definition)
    definition.config['logging'] = {'debug_enabled': True}
    definition.config['clickhouse']['password'] = 'not-a-persisted-secret'
    assert plan_revision(definition) == original
    definition.config['clickhouse']['database'] = 'different_dataset'
    assert plan_revision(definition) != original
    definition.config['clickhouse']['database'] = 'example'
    (definition.sql_dir / 'task_0.sql').write_text('INSERT INTO target SELECT 42')
    assert plan_revision(definition) != original


def test_ten_thousand_windows_exactly_is_allowed(app):
    definition = definition_for(app)
    start = datetime.fromisoformat('2026-01-01T00:00:00+08:00')
    result = plan(definition, start.isoformat(), (start + timedelta(minutes=50000)).isoformat(),
                  task='task_0')
    assert result['total'] == 10000
    assert result['items'][-1]['ordinal'] == 9999


@pytest.mark.parametrize('zone, interval, start, end, count', [
    ('America/Santiago', 'P1D', '2026-09-05T00:00:00-04:00',
     '2026-09-07T00:00:00-03:00', 2),
    ('Pacific/Apia', 'P1D', '2011-12-29T00:00:00-10:00',
     '2012-01-01T00:00:00+14:00', 2),
    ('Australia/Lord_Howe', 'PT1H', '2026-10-04T00:00:00+10:30',
     '2026-10-04T04:00:00+11:00', 3),
])
def test_clock_gaps_never_invent_windows(app, zone, interval, start, end, count):
    definition = definition_for(app, (interval,), zone=zone)
    result = plan(definition, start, end, task='task_0')
    assert result['total'] == count
    assert result['items'][0]['data_start'] == start
    assert result['items'][-1]['data_end'] == end
    for before, after in zip(result['items'], result['items'][1:]):
        assert before['data_end'] == after['data_start']
