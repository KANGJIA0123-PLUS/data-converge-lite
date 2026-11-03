"""Batch skipping after a long outage: aligned jump, logging, bounded resume."""

from contextlib import closing
from datetime import datetime, timedelta
import logging
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from data_converge.config.application import settings
from data_converge.dag import DAGBuilder
from data_converge.scheduler.skip import plan_skip, windows_behind

TZ = ZoneInfo('Asia/Shanghai')
NOW = datetime(2026, 10, 20, 10, 7, tzinfo=TZ)


def chain():
    rules = [
        dict(taskname='m5', source_tables=['raw'], target_table='t5', aggregation_interval='PT5M',
             cron='1,6,11,16,21,26,31,36,41,46,51,56 * * * *', depends_on=[], sql_id='m5'),
        dict(taskname='h1', source_tables=['t5'], target_table='th', aggregation_interval='PT1H',
             cron='6 * * * *', depends_on=['m5'], sql_id='h1'),
    ]
    builder = DAGBuilder(SimpleNamespace(validate=lambda: [], get_all_rules=lambda: rules))
    return builder, builder.build()


def options(**values):
    return settings({'scheduler': {'skip_batches': True, **values}})


def test_disabled_by_default_never_skips():
    _, dag = chain()
    cursors = {'m5': NOW - timedelta(days=2), 'h1': NOW - timedelta(days=2)}
    assert plan_skip(dag, cursors, NOW, settings({})) is None


def test_short_restart_below_threshold_does_not_skip():
    _, dag = chain()
    cursors = {'m5': NOW - timedelta(minutes=30), 'h1': NOW - timedelta(hours=1)}
    assert windows_behind(cursors['m5'], NOW, 'PT5M') == 6
    assert plan_skip(dag, cursors, NOW, options()) is None


def test_long_outage_jumps_all_tasks_to_one_aligned_boundary(caplog):
    _, dag = chain()
    cursors = {'m5': NOW - timedelta(days=2), 'h1': NOW - timedelta(days=2)}
    decision = plan_skip(dag, cursors, NOW, options())
    assert decision.jump_to == datetime(2026, 10, 20, 9, 0, tzinfo=TZ)
    assert set(decision.skipped) == {'m5', 'h1'}
    assert decision.resume_from is None  # current_only drops history
    from data_converge.scheduler.skip import log_skip
    with caplog.at_level(logging.WARNING):
        log_skip(decision, 'current_only')
    assert 'dconv --backfill --task h1' in caplog.text


def test_resume_mode_replays_at_most_the_configured_days():
    _, dag = chain()
    old = NOW - timedelta(days=60)
    decision = plan_skip(dag, {'m5': old, 'h1': old}, NOW,
                         options(skip_mode='current_then_resume'))
    assert decision.resume_from == NOW - timedelta(days=31)
    recent = NOW - timedelta(days=2)
    decision = plan_skip(dag, {'m5': recent, 'h1': recent}, NOW,
                         options(skip_mode='current_then_resume'))
    assert decision.resume_from == recent


def test_scheduler_runs_only_current_batches_after_skip(tmp_path):
    from data_converge.dag import DAGExecutor
    from data_converge.scheduler.service import TaskScheduler
    from .test_state import open_store

    builder, dag = chain()
    start = NOW - timedelta(days=2)
    calls = []
    with closing(open_store(tmp_path / 'state.db')) as store:
        store.set_meta('coverage_start', start.replace(minute=0).isoformat())
        executor = DAGExecutor(dag, lambda i: calls.append(i.instance_id), state_store=store,
                               max_concurrent_tasks=4)
        scheduler = TaskScheduler(dag, builder, executor, state_store=store,
                                  timezone='Asia/Shanghai', catchup_limit=1000,
                                  cross_period_wait_seconds=0)
        scheduler.skip_options = options()
        scheduler.tick(NOW)
    assert 'h1@2026-10-20T10:00:00+08:00' in calls
    assert all(item.split('@')[1] >= '2026-10-20T09' for item in calls)
    assert len(calls) <= 20
