"""Monthly promises retain canonical calendar bounds across SQLite restarts."""
from contextlib import closing
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from data_converge.backfill.plan import build_plan
from data_converge.backfill.queue import Queue
from data_converge.backfill.worker import BackfillWorker
from .test_backfill_plan import definition_for
from .test_main import app
from .test_state import open_store


CALENDAR_MONTHS = [
    pytest.param('America/Asuncion', '2023-10-01T01:00:00-03:00',
                 '2023-11-01T00:00:00-03:00', '2023-10-16T12:00:00-03:00',
                 '2023-10-20T12:00:00-03:00', 743, id='missing-midnight'),
    pytest.param('America/New_York', '2026-03-01T00:00:00-05:00',
                 '2026-04-01T00:00:00-04:00', '2026-03-16T12:00:00-04:00',
                 '2026-03-20T12:00:00-04:00', 743, id='dst-offset-change'),
    pytest.param('Asia/Shanghai', '2024-02-01T00:00:00+08:00',
                 '2024-03-01T00:00:00+08:00', '2024-02-16T12:00:00+08:00',
                 '2024-02-20T12:00:00+08:00', 696, id='leap-february'),
    pytest.param('Asia/Shanghai', '2026-12-01T00:00:00+08:00',
                 '2027-01-01T00:00:00+08:00', '2026-12-16T12:00:00+08:00',
                 '2026-12-20T12:00:00+08:00', 744, id='year-crossing'),
]


@pytest.mark.parametrize('zone,start,end,coverage,current,hours', CALENDAR_MONTHS)
def test_calendar_month_obligation_survives_restart_after_month_end(
        app, zone, start, end, coverage, current, hours):
    definition = definition_for(app, ('P1M',), zone=zone)
    path = app.parent / 'state.sqlite3'
    now = datetime.fromisoformat(current)
    calls = []
    with closing(open_store(path)) as store:
        store.set_meta('coverage_start', coverage)
        plan = build_plan(definition, start, end, task='task_0', now=now,
                          preaggregate=True, store=store)
        item = plan['items'][0]
        assert (item['data_start'], item['data_end']) == (start, end)
        assert item['formal_scheduled_time'] == end
        with Queue(app.parent / 'backfill.sqlite3', create=True) as queue:
            job = queue.submit(plan)
            BackfillWorker(definition, store, calls.append, clock=lambda: now).run_one(queue)
            assert queue.status(job)['counts'] == {'SUCCESS': 1}
        assert len(calls) == 1
        assert (calls[0].data_start.isoformat(), calls[0].data_end.isoformat()) == (start, end)
        assert (calls[0].data_end.timestamp() - calls[0].data_start.timestamp()) / 3600 == hours
        assert store.get_instance(item['instance_id']) is None
    with closing(open_store(path, create=False)) as reopened:
        due = datetime.fromisoformat(end).astimezone(ZoneInfo(zone))
        formal = definition.builder.create_instance(definition.dag, 'task_0', due)
        assert reopened.preaggregate_recovery_coverage(formal, due + timedelta(days=1)) is None
        pending = reopened.pending_preaggregate_coverages()
        assert len(pending) == 1 and pending[0]['status'] == 'SUCCESS'
        assert pending[0]['covered_at'] is None
        assert reopened.preaggregate_covers_window(formal)
        assert reopened.get_instance(formal.instance_id) is None
        assert reopened.get_meta('coverage_start') == coverage
