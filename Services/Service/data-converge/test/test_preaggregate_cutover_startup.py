"""Exercise the normal application startup gates with a saved coverage promise."""
from contextlib import closing
from datetime import timedelta

import pytest

from data_converge.application import Application
from data_converge.backfill.plan import build_plan
from data_converge.backfill.queue import Queue
from data_converge.backfill.worker import BackfillWorker
from data_converge.dag import DAGExecutor
from data_converge.runtime import ExecutionAdapter, RuntimeDependencies
from data_converge.scheduler import TaskScheduler
from data_converge.state import StateStore
from .test_backfill_plan import definition_for
from .test_main import app
from .test_preaggregate_cutover_plan import CUTOVER, NOW, recover
from .test_state import open_store


def prepare_recovered_early_write(app, interval):
    definition = definition_for(app, (interval,))
    start = CUTOVER.replace(minute=0)
    if interval == 'P1D':
        start = start.replace(hour=0)
    end = start + (timedelta(hours=1) if interval == 'PT1H' else timedelta(days=1))
    path = app.parent / 'state' / 'state.sqlite3'
    with closing(open_store(path)) as store:
        recover(store, definition)
        store.mark_initialized()
        plan = build_plan(definition, start.isoformat(), end.isoformat(), task='task_0',
                          now=NOW, preaggregate=True, store=store)
        with Queue(path.parent / 'backfill.sqlite3', create=True) as queue:
            queue.submit(plan)
            BackfillWorker(definition, store, lambda _: None, clock=lambda: NOW).run_one(queue)
        store.enqueue_and_advance([], {'task_0': end + timedelta(days=3)})
    return path, plan['items'][0]['instance_id'], end + timedelta(minutes=15)


@pytest.mark.parametrize('interval', ['PT1H', 'P1D'])
def test_normal_service_restart_validates_and_executes_saved_coverage(app, interval, monkeypatch):
    path, instance_id, due = prepare_recovered_early_write(app, interval)
    calls = []
    dependencies = RuntimeDependencies(execution_factory=lambda *_: ExecutionAdapter(calls.append, lambda: None))
    monkeypatch.setattr(TaskScheduler, 'start', lambda scheduler: scheduler.tick(due))
    monkeypatch.setattr(DAGExecutor, '_now', staticmethod(lambda _: due))
    assert Application(app, dependencies=dependencies).serve().exit_code == 0
    assert [instance.instance_id for instance in calls] == [instance_id]
    with closing(StateStore(path)) as store:
        store.validate_retirement()
        assert store.get_instance(instance_id)['status'] == 'SUCCESS'
        assert store.latest_preaggregate(instance_id)['covered_at']
        assert store.get_task_effective_starts()['task_0'] == CUTOVER
    assert Application(app, dependencies=dependencies).serve().exit_code == 0
    assert len(calls) == 1
