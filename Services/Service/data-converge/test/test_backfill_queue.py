"""Backfill requests persist independently of the running ledger writer."""
from concurrent.futures import ThreadPoolExecutor
import json
import sqlite3

import pytest

from data_converge.backfill.queue import BackfillQueueError, Queue


def plan(*names, recompute=False):
    return {'schema_version': 1, 'revision': 'revision-1', 'recompute': recompute, 'total': len(names),
            'items': [{'ordinal': index, 'instance_id': name, 'taskname': name.split('@')[0],
                       'scheduled_time': '2026-10-12T10:05:00+08:00',
                       'data_start': '2026-10-12T10:00:00+08:00',
                       'data_end': '2026-10-12T10:05:00+08:00', 'upstream_instance_ids': []}
                      for index, name in enumerate(names)]}


def test_missing_queue_read_never_creates_directories(tmp_path):
    path = tmp_path / 'missing/backfill.sqlite3'
    with pytest.raises(FileNotFoundError):
        Queue(path)
    assert not path.parent.exists()


def test_queue_persists_request_progress_and_resumes_without_claim(tmp_path):
    path = tmp_path / 'backfill.sqlite3'
    with Queue(path, create=True) as queue:
        job = queue.submit(plan('parent@1', 'child@1', recompute=True), request_id='run-1')
        first = queue.next_item()
        assert first['instance_id'] == 'parent@1' and first['recompute'] is True
        assert first['revision'] == 'revision-1' and first['job_id'] == job
        assert queue.next_item() == first
        assert queue.status(job)['status'] == 'QUEUED'
        queue.complete(first['item_id'], 'SUCCESS', query_id='query-1')
    with Queue(path) as queue:
        assert queue.item_status(job, 'parent@1') == 'SUCCESS'
        assert queue.item_status(job, 'absent') is None
        assert queue.status(job)['status'] == 'IN_PROGRESS'
        assert queue.status(job)['completed'] == 1
        second = queue.next_item()
        assert second['instance_id'] == 'child@1'
        queue.complete(second['item_id'], 'BLOCKED', error='BACKFILL_UPSTREAM_FAILED')
        result = queue.status(job, limit=1, offset=1)
        assert result['status'] == 'COMPLETED_WITH_ERRORS'
        assert result['counts'] == {'SUCCESS': 1, 'BLOCKED': 1}
        assert result['items'][0]['error'] == 'BACKFILL_UPSTREAM_FAILED'
        assert result['items'][0]['ordinal'] == 1
        assert not result['has_more'] and queue.next_item() is None


def test_requests_use_stable_fifo_and_completed_jobs_allow_next_job(tmp_path):
    with Queue(tmp_path / 'backfill.sqlite3', create=True) as queue:
        first = queue.submit(plan('a@1', 'a@2'))
        second = queue.submit(plan('b@1'))
        assert queue.status(first, limit=1)['has_more']
        for _ in range(2):
            item = queue.next_item()
            assert item['job_id'] == first
            queue.complete(item['item_id'], 'SKIPPED')
        assert queue.status(first)['status'] == 'COMPLETED'
        assert queue.next_item()['job_id'] == second


def test_request_id_is_idempotent_but_cannot_change_contents(tmp_path):
    with Queue(tmp_path / 'backfill.sqlite3', create=True) as queue:
        expected = plan('a@1')
        job = queue.submit(expected, request_id='incident-20261020')
        assert queue.submit(dict(reversed(list(expected.items()))),
                            request_id='incident-20261020') == job
        assert queue.status(job)['total'] == 1
        with pytest.raises(BackfillQueueError, match='BACKFILL_REQUEST_ID_CONFLICT'):
            queue.submit(plan('b@1'), request_id='incident-20261020')


def test_concurrent_submissions_deduplicate_one_request(tmp_path):
    path = tmp_path / 'backfill.sqlite3'
    with Queue(path, create=True):
        pass
    def submit(_):
        with Queue(path) as queue:
            return queue.submit(plan('a@1'), request_id='same-request')
    with ThreadPoolExecutor(max_workers=4) as pool:
        ids = list(pool.map(submit, range(12)))
    assert len(set(ids)) == 1
    with Queue(path) as queue:
        assert len(queue.jobs()) == 1
        assert queue.status(ids[0])['total'] == 1


def test_status_readonly_connection_cannot_modify_queue(tmp_path):
    path = tmp_path / 'backfill.sqlite3'
    with Queue(path, create=True) as queue:
        job = queue.submit(plan('a@1'))
    before = path.read_bytes()
    with Queue(path, read_only=True) as queue:
        assert queue.status(job)['status'] == 'QUEUED'
        with pytest.raises(sqlite3.OperationalError, match='readonly'):
            queue.submit(plan('b@1'))
    assert path.read_bytes() == before


def test_completion_is_immutable_and_unknown_ids_are_explicit(tmp_path):
    with Queue(tmp_path / 'backfill.sqlite3', create=True) as queue:
        job = queue.submit(plan('a@1'))
        item = queue.next_item()
        queue.complete(item['item_id'], 'UNKNOWN', query_id='q')
        queue.complete(item['item_id'], 'UNKNOWN', query_id='q')
        with pytest.raises(BackfillQueueError, match='BACKFILL_ITEM_ALREADY_COMPLETED'):
            queue.complete(item['item_id'], 'SUCCESS')
        with pytest.raises(BackfillQueueError, match='BACKFILL_JOB_NOT_FOUND'):
            queue.status('bf_missing')
        with pytest.raises(BackfillQueueError, match='BACKFILL_ITEM_NOT_FOUND'):
            queue.complete(job + ':9', 'SUCCESS')
        assert queue.status(job)['counts'] == {'UNKNOWN': 1}


@pytest.mark.parametrize('value', ['', '../bad', 'with space', 'x\n', 'x' * 129])
def test_request_ids_must_be_safe_bounded_tokens(tmp_path, value):
    with Queue(tmp_path / 'backfill.sqlite3', create=True) as queue:
        with pytest.raises(BackfillQueueError, match='BACKFILL_REQUEST_ID_INVALID'):
            queue.submit(plan('a@1'), request_id=value)
        assert queue.jobs() == []


@pytest.mark.parametrize('value', [plan(), plan('same', 'same'), {'items': []}])
def test_invalid_plans_do_not_leave_partial_jobs(tmp_path, value):
    with Queue(tmp_path / 'backfill.sqlite3', create=True) as queue:
        with pytest.raises(BackfillQueueError, match='BACKFILL_PLAN_INVALID'):
            queue.submit(value)
        assert queue.jobs() == []


@pytest.mark.parametrize('status', ['QUEUED', 'RUNNING', 'invalid'])
def test_only_terminal_item_outcomes_can_be_completed(tmp_path, status):
    with Queue(tmp_path / 'backfill.sqlite3', create=True) as queue:
        job = queue.submit(plan('a@1'))
        with pytest.raises(BackfillQueueError, match='BACKFILL_STATUS_INVALID'):
            queue.complete(queue.next_item()['item_id'], status)
        assert queue.status(job)['counts'] == {'QUEUED': 1}


@pytest.mark.parametrize('serialized', [
    '{broken', 'null', '[]', '42', '{}',
    json.dumps(dict(plan('a@1'), schema_version=0)),
    json.dumps(dict(plan('a@1'), recompute='false')),
    json.dumps(dict(plan('a@1'), revision=None)),
    json.dumps(dict(plan('a@1'), items=None)),
    json.dumps(dict(plan('a@1'), total=True)),
])
def test_malformed_stored_plan_is_a_public_queue_error(tmp_path, serialized):
    with Queue(tmp_path / 'backfill.sqlite3', create=True) as queue:
        job = queue.submit(plan('a@1'))
        queue.connection.execute('UPDATE backfill_jobs SET plan=? WHERE job_id=?', (serialized, job))
        with pytest.raises(BackfillQueueError, match='^BACKFILL_QUEUE_INVALID$'):
            queue.next_item()
        assert queue.connection.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'


@pytest.mark.parametrize('serialized', [
    '{broken', 'null', '[]', '42', '{}',
    json.dumps(dict(plan('a@1')['items'][0], taskname=None)),
    json.dumps(dict(plan('a@1')['items'][0], scheduled_time=[])),
    json.dumps(dict(plan('a@1')['items'][0], upstream_instance_ids='parent@1')),
    json.dumps(dict(plan('a@1')['items'][0], upstream_instance_ids=[{}])),
    json.dumps(dict(plan('a@1')['items'][0], ordinal=True)),
])
def test_malformed_stored_item_is_a_public_queue_error(tmp_path, serialized):
    with Queue(tmp_path / 'backfill.sqlite3', create=True) as queue:
        job = queue.submit(plan('a@1'))
        queue.connection.execute('UPDATE backfill_items SET payload=? WHERE job_id=?', (serialized, job))
        with pytest.raises(BackfillQueueError, match='^BACKFILL_QUEUE_INVALID$'):
            queue.next_item()
        assert queue.connection.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
