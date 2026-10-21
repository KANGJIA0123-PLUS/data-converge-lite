"""Versioned early jobs cannot silently become old formal jobs."""
from copy import deepcopy

import pytest

from data_converge.backfill.queue import Queue, BackfillQueueError
from .test_backfill_queue import plan


def early_plan():
    result = plan('task@1')
    result.update(schema_version=2, preaggregate=True)
    result['items'][0].update(execution_kind='PREAGGREGATE', config_evidence={'task': {}},
                              formal_scheduled_time='2026-10-12T10:05:00+08:00')
    return result


def test_v2_roundtrip_request_id_dedup_and_public_execution_kind(tmp_path):
    with Queue(tmp_path / 'queue.db', create=True) as queue:
        value = early_plan()
        job = queue.submit(value, request_id='early-1')
        assert queue.submit(value, request_id='early-1') == job
        assert queue.next_item()['execution_kind'] == 'PREAGGREGATE'
        assert queue.status(job)['items'][0]['execution_kind'] == 'PREAGGREGATE'


def test_old_item_is_explicit_formal_in_pending_and_status(tmp_path):
    with Queue(tmp_path / 'queue.db', create=True) as queue:
        job = queue.submit(plan('task@1'))
        assert queue.next_item()['execution_kind'] == 'BACKFILL'
        assert queue.status(job)['items'][0]['execution_kind'] == 'BACKFILL'


@pytest.mark.parametrize('mutation', ['downgrade', 'missing', 'unknown'])
def test_execution_type_cannot_be_downgraded_or_implicitly_defaulted_in_v2(tmp_path, mutation):
    value = deepcopy(early_plan())
    if mutation == 'downgrade':
        value['schema_version'] = 1
    elif mutation == 'missing':
        del value['items'][0]['execution_kind']
    else:
        value['items'][0]['execution_kind'] = 'UNKNOWN'
    with Queue(tmp_path / 'queue.db', create=True) as queue:
        with pytest.raises(BackfillQueueError, match='BACKFILL_PLAN_INVALID'):
            queue.submit(value)
        assert queue.jobs() == []
