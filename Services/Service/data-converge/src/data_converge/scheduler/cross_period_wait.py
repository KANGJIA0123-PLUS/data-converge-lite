"""Nonblocking cross-period completeness checks with fixed Cron deadlines."""

from collections import Counter
from datetime import datetime, timedelta
import logging

from data_converge.dag.builder import required_cross_period_ids, shift_elapsed, timeline_time
from data_converge.dag.models import TaskStatus


logger = logging.getLogger(__name__)


def _aged_out(store, parent_id: str) -> bool:
    """A parent older than its Redis retention was finished history, not missing."""
    mirror = getattr(store, 'mirror', None)
    if mirror is None:
        return False
    taskname, _, batch = parent_id.partition('@')
    interval = getattr(store, '_intervals', {}).get(taskname)
    if interval is None:
        return False
    end = datetime.fromisoformat(batch)
    return timeline_time(mirror.settings.expires_at(interval, end)) <= timeline_time(datetime.now(end.tzinfo))


def missing_summary(dag, store, instance) -> dict:
    missing = []
    statuses = Counter()
    count = 0
    for parent in required_cross_period_ids(dag, instance.task, instance.data_start, instance.data_end):
        status = store.get_status(parent)
        if status == TaskStatus.SUCCESS or (status is None and _aged_out(store, parent)):
            continue
        label = status.value if status is not None else 'NOT_FOUND'
        statuses[label] += 1
        count += 1
        if len(missing) < 100:
            missing.append({'instance_id': parent, 'status': label})
    return dict(missing_count=count, missing_status_counts=dict(statuses),
                missing_samples=missing, missing_truncated=count > len(missing))


class CrossPeriodWait:
    def __init__(self, dag, store, *, enabled=True, seconds=300, overrides=None):
        self.dag = dag
        self.store = store
        self.enabled = enabled
        self.seconds = seconds
        self.overrides = dict(overrides or {})
        self._legacy_reported = set()

    def records(self, instances) -> list[dict]:
        result = []
        for instance in instances:
            if not self._parents(instance):
                continue
            seconds = self.wait_seconds(instance)
            result.append(dict(instance_id=instance.instance_id,
                               scheduled_due_at=instance.scheduled_time, wait_seconds=seconds,
                               deadline_at=shift_elapsed(instance.scheduled_time,
                                                         timedelta(seconds=seconds))))
        return result

    def wait_seconds(self, instance) -> int:
        """Task override, then the child's period, then a legacy single value."""
        if instance.taskname in self.overrides:
            return self.overrides[instance.taskname]
        if isinstance(self.seconds, dict):
            return self.seconds.get(instance.task.aggregation_interval, 0)
        return self.seconds

    def _parents(self, instance):
        return required_cross_period_ids(self.dag, instance.task, instance.data_start, instance.data_end)

    def reason(self, instance, now: datetime) -> str | None:
        if not self._parents(instance):
            return None
        record = self.store.get_cross_period_wait(instance.instance_id)
        if record is None:
            self._legacy(instance)
            return None
        if record['status'] == 'TIMED_OUT' or record['query_id'] is not None:
            return None
        missing = missing_summary(self.dag, self.store, instance)
        status, reason = self._decision(record, missing, now)
        missing['decision_reason'] = reason
        changed = self.store.record_cross_period_wait(
            instance.instance_id, status=status, missing=missing, now=now)
        if changed:
            self._log(instance, record, status, missing)
        return 'CROSS_PERIOD_WAITING' if status == 'WAITING' else None

    def _decision(self, record, missing, now):
        if not self.enabled:
            return 'READY', 'WAIT_DISABLED'
        if record['wait_seconds'] == 0:
            return 'READY', 'WAIT_ZERO'
        if not missing['missing_count']:
            return 'READY', 'UPSTREAM_COMPLETE'
        deadline = datetime.fromisoformat(record['deadline_at'])
        if timeline_time(now) >= timeline_time(deadline):
            return 'TIMED_OUT', 'DEADLINE_REACHED'
        return 'WAITING', 'WAITING_FOR_UPSTREAM'

    def _legacy(self, instance):
        if instance.instance_id not in self._legacy_reported:
            self._legacy_reported.add(instance.instance_id)
            logger.info('CROSS_PERIOD_WAIT_NO_TRUSTED_ANCHOR instance_id=%s; legacy immediate execution',
                        instance.instance_id)

    @staticmethod
    def _log(instance, record, status, missing):
        log = logger.warning if status == 'TIMED_OUT' else logger.info
        log('CROSS_PERIOD_WAIT_%s instance_id=%s scheduled_due_at=%s deadline_at=%s '
            'missing_count=%s missing_status_counts=%s missing_samples=%s missing_truncated=%s reason=%s',
            status, instance.instance_id, record['scheduled_due_at'], record['deadline_at'],
            missing['missing_count'], missing['missing_status_counts'], missing['missing_samples'],
            missing['missing_truncated'], missing['decision_reason'])
