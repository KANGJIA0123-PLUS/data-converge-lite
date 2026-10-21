"""Early plans retain full windows and require eventual formal coverage."""
from datetime import datetime
from dataclasses import replace

import pytest

from data_converge.backfill.plan import build_plan
from .test_backfill_plan import definition_for
from .test_main import app
from .test_state import open_store

START = '2026-10-21T10:00:00+08:00'
END = '2026-10-21T11:00:00+08:00'
NOW = datetime.fromisoformat('2026-10-21T10:30:00+08:00')


@pytest.fixture
def early(app):
    definition = definition_for(app, ('PT1H',))
    store = open_store(app.parent / 'state.sqlite3')
    store.set_meta('coverage_start', '2026-10-20T00:00:00+08:00')
    try:
        yield definition, store
    finally:
        store.close()


def early_plan(early, start=START, end=END, **kwargs):
    definition, store = early
    return build_plan(definition, start, end, task='task_0', now=NOW,
                      preaggregate=True, store=store, **kwargs)


def test_mixed_plan_classifies_ended_current_and_omits_future_windows(early):
    result = early_plan(early, '2026-10-21T09:00:00+08:00', '2026-10-21T13:00:00+08:00')
    assert result['schema_version'] == 2
    assert [item['execution_kind'] for item in result['items']] == ['BACKFILL', 'PREAGGREGATE']
    assert result['total'] == 2
    assert result['omitted_future_count'] == 2
    assert result['omitted_future'][0]['reason'] == 'BACKFILL_WINDOW_NOT_STARTED'
    item = result['items'][1]
    assert (item['data_start'], item['data_end']) == (START, END)
    assert item['formal_scheduled_time'] == END
    assert item['config_evidence']['task']['definition']['taskname'] == 'task_0'


def test_future_remains_rejected_without_explicit_mode(early):
    with pytest.raises(ValueError, match='BACKFILL_END_IN_FUTURE'):
        build_plan(early[0], START, END, task='task_0', now=NOW)


def test_only_unstarted_windows_have_no_executable_plan(early):
    with pytest.raises(ValueError, match='BACKFILL_EMPTY_RANGE'):
        early_plan(early, END, '2026-10-21T12:00:00+08:00')


@pytest.mark.parametrize('change, reason', [
    ('coverage', 'BACKFILL_PREAGGREGATE_BEFORE_COVERAGE'),
    ('cursor', 'BACKFILL_PREAGGREGATE_CURSOR_PASSED'),
    ('cron', 'BACKFILL_PREAGGREGATE_NO_FORMAL_WINDOW'),
    ('formal', 'BACKFILL_PREAGGREGATE_FORMAL_EXISTS'),
])
def test_invalid_future_formal_coverage_is_rejected(early, change, reason):
    definition, store = early
    if change == 'coverage':
        store.set_meta('coverage_start', '2026-10-21T10:15:00+08:00')
    elif change == 'cursor':
        store.enqueue_and_advance([], {'task_0': datetime.fromisoformat('2026-10-21T12:00:00+08:00')})
    elif change == 'cron':
        task = replace(definition.dag.tasks['task_0'], cron='0 0 * * *')
        definition = replace(definition, dag=replace(definition.dag, tasks={'task_0': task}))
    else:
        instance = definition.builder.create_instance(definition.dag, 'task_0', datetime.fromisoformat(END))
        store.enqueue_and_advance([instance], {})
    with pytest.raises(ValueError, match=reason):
        early_plan((definition, store))


def test_delayed_cron_proves_exact_window_not_data_end_as_trigger(early):
    definition, store = early
    task = replace(definition.dag.tasks['task_0'], cron='10 * * * *')
    definition = replace(definition, dag=replace(definition.dag, tasks={'task_0': task}))
    item = early_plan((definition, store))['items'][0]
    assert item['formal_scheduled_time'] == '2026-10-21T11:10:00+08:00'
    assert item['scheduled_time'] == END


@pytest.mark.parametrize('current, expected', [(START, 'PREAGGREGATE'), (END, 'BACKFILL')])
def test_exact_start_and_end_instant_keep_classification_fixed(early, current, expected):
    definition, store = early
    result = build_plan(definition, START, END, task='task_0', preaggregate=True,
                        store=store, now=datetime.fromisoformat(current))
    assert result['items'][0]['execution_kind'] == expected


def test_effective_start_does_not_allow_partial_first_window(early):
    import json
    early[1].set_meta('configuration_snapshot', json.dumps({
        'task_starts': {'task_0': '2026-10-21T10:15:00+08:00'}}))
    with pytest.raises(ValueError, match='BACKFILL_PREAGGREGATE_BEFORE_EFFECTIVE_START'):
        early_plan(early)


@pytest.mark.parametrize('interval, zone, start, end, current, due', [
    ('P1D', 'America/New_York', '2026-03-08T00:00:00-05:00',
     '2026-03-09T00:00:00-04:00', '2026-03-08T12:00:00-04:00', '2026-03-09T00:10:00-04:00'),
    ('P1D', 'America/New_York', '2026-11-01T00:00:00-04:00',
     '2026-11-02T00:00:00-05:00', '2026-11-01T12:00:00-05:00', '2026-11-02T00:10:00-05:00'),
    ('P1M', 'Asia/Shanghai', '2026-02-01T00:00:00+08:00',
     '2026-03-01T00:00:00+08:00', '2026-02-15T12:00:00+08:00', '2026-03-01T00:10:00+08:00'),
])
def test_calendar_early_coverage_keeps_real_boundaries(app, interval, zone, start, end, current, due):
    definition = definition_for(app, (interval,), zone=zone)
    cron = '10 0 * * *' if interval == 'P1D' else '10 0 1 * *'
    task = replace(definition.dag.tasks['task_0'], cron=cron)
    definition = replace(definition, dag=replace(definition.dag, tasks={'task_0': task}))
    store = open_store(app.parent / 'state.sqlite3')
    try:
        store.set_meta('coverage_start', start)
        result = build_plan(definition, start, end, task='task_0', preaggregate=True,
                            store=store, now=datetime.fromisoformat(current))
        item = result['items'][0]
        assert (item['data_start'], item['data_end']) == (start, end)
        assert item['formal_scheduled_time'] == due
    finally:
        store.close()
