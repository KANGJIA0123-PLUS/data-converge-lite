"""Independent early attempts never replace formal success or replay ambiguous SQL."""
from datetime import datetime

import pytest

from data_converge.backfill.plan import build_plan
from data_converge.backfill.queue import Queue
from data_converge.backfill.worker import BackfillWorker
from data_converge.dag import TaskStatus
from data_converge.errors import TaskExecutionError
from .test_backfill_plan import definition_for
from .test_main import app
from .test_preaggregate_plan import START, END, NOW
from .test_state import open_store


@pytest.fixture
def setup(app):
    definition = definition_for(app, ('PT1H', 'PT1H'), dependency=True)
    store = open_store(app.parent / 'state.sqlite3')
    store.set_meta('coverage_start', '2026-10-20T00:00:00+08:00')
    queue = Queue(app.parent / 'backfill.sqlite3', create=True)
    try:
        yield definition, store, queue
    finally:
        queue.close()
        store.close()


def submit(setup, task=None, request_id=None):
    definition, store, queue = setup
    plan = build_plan(definition, START, END, task=task, all_tasks=task is None,
                      now=NOW, preaggregate=True, store=store)
    return queue.submit(plan, request_id=request_id)


def service(setup, runner, now=NOW):
    definition, store, _ = setup
    return BackfillWorker(definition, store, runner, clock=lambda: now)


def test_early_chain_uses_full_windows_without_formal_status_or_cursor(setup):
    job = submit(setup)
    calls = []
    worker = service(setup, lambda instance: calls.append(instance))
    worker.run_one(setup[2])
    worker.run_one(setup[2])
    assert [value.taskname for value in calls] == ['task_0', 'task_1']
    assert all(value.data_start.isoformat() == START and value.data_end.isoformat() == END for value in calls)
    assert setup[2].status(job)['counts'] == {'SUCCESS': 2}
    assert all(setup[1].get_status(value.instance_id) is None for value in calls)
    assert setup[1].get_cursor('task_0') is None
    assert setup[1].latest_preaggregate(calls[0].instance_id)['status'] == 'SUCCESS'


def test_expired_queued_item_is_skipped_without_formal_write(setup):
    job = submit(setup, 'task_0')
    worker = service(setup, lambda i: pytest.fail('expired item executed'), datetime.fromisoformat(END))
    worker.run_one(setup[2])
    item = setup[2].status(job)['items'][0]
    assert item['status'] == 'SKIPPED' and item['error'] == 'PREAGGREGATE_WINDOW_EXPIRED'
    assert setup[1].get_status(item['instance_id']) is None


def test_queue_sync_failure_reuses_main_ledger_result(setup, monkeypatch):
    job = submit(setup, 'task_0')
    calls = []
    worker = service(setup, lambda i: calls.append(i.query_id))
    queue = setup[2]
    complete = queue.complete
    monkeypatch.setattr(queue, 'complete', lambda *a, **kw: (_ for _ in ()).throw(OSError('full')))
    with pytest.raises(OSError):
        worker.run_one(queue)
    monkeypatch.setattr(queue, 'complete', complete)
    worker.run_one(queue)
    assert len(calls) == 1
    assert queue.status(job)['counts'] == {'SUCCESS': 1}


def test_same_batch_failure_cannot_use_older_success(setup):
    submit(setup, 'task_0')
    service(setup, lambda i: None).run_one(setup[2])
    job = submit(setup)
    calls = []
    def fail(instance):
        calls.append(instance.taskname)
        raise TaskExecutionError('SQL_PREFLIGHT_FAILED', outcome_unknown=False)
    worker = service(setup, fail)
    worker.run_one(setup[2])
    worker.run_one(setup[2])
    assert calls == ['task_0']
    assert setup[2].status(job)['counts'] == {'FAILED': 1, 'BLOCKED': 1}


@pytest.mark.parametrize('latest', ['SUCCESS', 'FAILED', 'UNKNOWN'])
def test_cross_batch_dependency_uses_only_latest_same_revision(setup, latest):
    submit(setup, 'task_0')
    service(setup, lambda i: None).run_one(setup[2])
    if latest != 'SUCCESS':
        submit(setup, 'task_0')
        def fail(instance):
            raise TaskExecutionError('SQL_PREFLIGHT_FAILED', outcome_unknown=latest == 'UNKNOWN')
        service(setup, fail).run_one(setup[2])
    job = submit(setup, 'task_1')
    calls = []
    service(setup, lambda i: calls.append(i)).run_one(setup[2])
    assert bool(calls) == (latest == 'SUCCESS')
    expected = 'SUCCESS' if latest == 'SUCCESS' else 'BLOCKED'
    assert setup[2].status(job)['counts'] == {expected: 1}


def test_dispatch_rechecks_cursor_after_submit(setup):
    job = submit(setup, 'task_0')
    setup[1].enqueue_and_advance([], {'task_0': datetime.fromisoformat('2026-10-21T12:00:00+08:00')})
    service(setup, lambda i: pytest.fail('cursor-passed item executed')).run_one(setup[2])
    item = setup[2].status(job)['items'][0]
    assert item['status'] == 'BLOCKED'
    assert item['error'] == 'BACKFILL_PREAGGREGATE_CURSOR_PASSED'


def test_recovery_unknown_is_synced_without_sql(setup):
    job = submit(setup, 'task_0')
    definition, store, queue = setup
    item = queue.next_item()
    instance = definition.builder.create_instance(definition.dag, 'task_0', datetime.fromisoformat(END))
    store.begin_preaggregate(item, instance, now=NOW)
    store.recover_running()
    service(setup, lambda i: pytest.fail('unknown item replayed')).run_one(queue)
    assert queue.status(job)['counts'] == {'UNKNOWN': 1}
    assert store.get_status(instance.instance_id) is None


def test_skipped_early_upstream_is_not_success_even_with_old_formal_success(setup):
    job = submit(setup)
    definition, store, queue = setup
    item = queue.next_item()
    instance = definition.builder.create_instance(definition.dag, 'task_0', datetime.fromisoformat(END))
    store.enqueue_and_advance([instance], {})
    store.claim(instance.instance_id)
    store.finish(instance.instance_id, TaskStatus.SUCCESS)
    queue.complete(item['item_id'], 'SKIPPED', error='PREAGGREGATE_WINDOW_EXPIRED')
    calls = []
    service(setup, lambda i: calls.append(i)).run_one(queue)
    assert calls == []
    assert queue.status(job)['items'][1]['status'] == 'BLOCKED'


def test_latest_expired_attempt_blocks_cross_batch_dependency(setup):
    submit(setup, 'task_0')
    service(setup, lambda i: None).run_one(setup[2])
    submit(setup, 'task_0')
    service(setup, lambda i: pytest.fail('expired executed'), datetime.fromisoformat(END)).run_one(setup[2])
    job = submit(setup, 'task_1')
    service(setup, lambda i: pytest.fail('used older early success')).run_one(setup[2])
    assert setup[2].status(job)['counts'] == {'BLOCKED': 1}


def test_cross_batch_success_with_changed_revision_is_not_accepted(setup):
    submit(setup, 'task_0')
    service(setup, lambda i: None).run_one(setup[2])
    (setup[0].sql_dir / 'task_0.sql').write_text('INSERT INTO target SELECT 2')
    job = submit(setup, 'task_1')
    service(setup, lambda i: pytest.fail('used old revision')).run_one(setup[2])
    assert setup[2].status(job)['counts'] == {'BLOCKED': 1}


def test_dispatch_rechecks_live_sql_snapshot_after_worker_created(setup):
    job = submit(setup, 'task_0')
    worker = service(setup, lambda i: pytest.fail('changed SQL dispatched'))
    (setup[0].sql_dir / 'task_0.sql').write_text('INSERT INTO target SELECT 2')
    worker.run_one(setup[2])
    item = setup[2].status(job)['items'][0]
    assert item['status'] == 'BLOCKED'
    assert item['error'] == 'BACKFILL_CONFIGURATION_CHANGED'


def test_queue_dependency_reads_crossing_end_skip_before_claim(setup, monkeypatch):
    job = submit(setup, 'task_1')
    definition, store, queue = setup
    parent = definition.builder.create_instance(definition.dag, 'task_0', datetime.fromisoformat(END))
    store.enqueue_and_advance([parent], {})
    store.claim(parent.instance_id)
    store.finish(parent.instance_id, TaskStatus.SUCCESS)
    times = iter([NOW, datetime.fromisoformat(END)])
    worker = BackfillWorker(definition, store, lambda i: pytest.fail('late dispatch'), clock=lambda: next(times))
    worker.run_one(queue)
    assert queue.status(job)['counts'] == {'SKIPPED': 1}


def test_unknown_barrier_is_rechecked_after_submission(setup):
    job = submit(setup, 'task_0')
    definition, store, queue = setup
    item = dict(queue.next_item(), item_id='external:1')
    instance = definition.builder.create_instance(definition.dag, 'task_0', datetime.fromisoformat(END))
    store.begin_preaggregate(item, instance, now=NOW)
    store.finish_preaggregate(item['item_id'], TaskStatus.UNKNOWN, now=NOW)
    service(setup, lambda i: pytest.fail('unknown barrier bypassed')).run_one(queue)
    result = queue.status(job)['items'][0]
    assert result['status'] == 'BLOCKED' and result['error'] == 'BACKFILL_PREAGGREGATE_UNCONFIRMED'


def test_expiry_is_skipped_even_when_unrelated_live_revision_changed(setup):
    job = submit(setup, 'task_0')
    (setup[0].sql_dir / 'task_1.sql').write_text('INSERT INTO target SELECT 2')
    service(setup, lambda i: pytest.fail('expired changed request executed'),
            datetime.fromisoformat(END)).run_one(setup[2])
    result = setup[2].status(job)['items'][0]
    assert result['status'] == 'SKIPPED'
    assert setup[1].latest_preaggregate(result['instance_id'])['status'] == 'SKIPPED'
