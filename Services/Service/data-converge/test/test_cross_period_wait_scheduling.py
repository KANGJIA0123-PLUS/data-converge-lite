"""Real ledger cases for nonblocking, anchored cross-period waiting."""

from datetime import datetime, timedelta
import logging

from data_converge.dag import TaskStatus
from .test_scheduler import SHANGHAI, make_rules, persistent_scheduler


COVERAGE = datetime(2026, 10, 1, tzinfo=SHANGHAI)
DUE = datetime(2026, 10, 2, 0, 10, tzinfo=SHANGHAI)
CHILD = 'task_hour@2026-10-02T00:00:00+08:00'


def waiting_scheduler(tmp_path, calls, *, create=True):
    rules = make_rules()
    rules[0].update(aggregation_interval='PT1H', cron='0 * * * *')
    rules[1].update(aggregation_interval='P1D', cron='10 0 * * *')
    scheduler, store = persistent_scheduler(tmp_path, calls, COVERAGE,
                                           rules=rules, create=create)
    scheduler.dag_executor.readiness_check = lambda instance, now: (
        'WAITING_DWD' if instance.taskname == 'task_5min' else None)
    return scheduler, store


def mark_parent_successes(scheduler, store, *, count=24):
    for hour in range(1, count + 1):
        end = COVERAGE + timedelta(hours=hour)
        parent = scheduler.dag_builder.create_instance(scheduler.dag, 'task_5min', end)
        assert store.claim(parent.instance_id, now=end)
        store.finish(parent.instance_id, TaskStatus.SUCCESS, now=end)


def test_daily_wait_uses_original_cron_and_does_not_consume_attempts(tmp_path):
    calls = []
    scheduler, store = waiting_scheduler(tmp_path, calls)
    try:
        report = scheduler.tick(DUE)
        assert CHILD not in calls
        assert CHILD in report.instances
        assert store.get_instance(CHILD)['attempts'] == 0
        wait = store.get_cross_period_wait(CHILD)
        assert wait['scheduled_due_at'] == '2026-10-02T00:10:00+08:00'
        assert wait['deadline_at'] == '2026-10-02T00:15:00+08:00'
        assert wait['status'] == 'WAITING'
        assert wait['missing_count'] == 24
    finally:
        store.close()


def test_upstream_completeness_releases_before_deadline(tmp_path):
    calls = []
    scheduler, store = waiting_scheduler(tmp_path, calls)
    try:
        scheduler.tick(DUE)
        assert CHILD not in calls
        mark_parent_successes(scheduler, store)
        scheduler.run_pending(DUE + timedelta(minutes=3))
        assert calls == [CHILD]
        assert store.get_cross_period_wait(CHILD)['status'] == 'READY'
    finally:
        store.close()


def test_restart_preserves_deadline_timeout_audit_and_query_id(tmp_path, caplog):
    calls = []
    scheduler, store = waiting_scheduler(tmp_path, calls)
    scheduler.tick(DUE)
    assert CHILD not in calls
    store.close()
    scheduler, store = waiting_scheduler(tmp_path, calls, create=False)
    try:
        scheduler.run_pending(DUE + timedelta(minutes=2))
        assert calls == []
        with caplog.at_level(logging.INFO):
            scheduler.run_pending(DUE + timedelta(minutes=5))
            scheduler.run_pending(DUE + timedelta(minutes=6))
        assert calls == [CHILD]
        wait = store.get_cross_period_wait(CHILD)
        assert wait['status'] == 'TIMED_OUT' and wait['missing_count'] == 24
        assert wait['query_id'] == store.get_instance(CHILD)['query_id']
        assert wait['deadline_at'] == '2026-10-02T00:15:00+08:00'
        assert sum('CROSS_PERIOD_WAIT_TIMED_OUT' in r.message for r in caplog.records) == 1
    finally:
        store.close()


def test_legacy_pending_window_never_acquires_a_new_wait_anchor(tmp_path):
    calls = []
    scheduler, store = waiting_scheduler(tmp_path, calls)
    try:
        child = scheduler.dag_builder.create_instance(scheduler.dag, 'task_hour', DUE)
        store.enqueue_and_advance([child], {})
        scheduler.tick(DUE)
        assert calls == [CHILD]
        assert store.get_cross_period_wait(CHILD) is None
    finally:
        store.close()


def test_disabled_then_reenabled_preserves_original_unconsumed_deadline(tmp_path):
    calls = []
    scheduler, store = waiting_scheduler(tmp_path, calls)
    try:
        scheduler.tick(DUE)
        child = scheduler._pending_instance(store.get_instance(CHILD))
        wait = scheduler.cross_period_wait
        wait.enabled = False
        assert wait.reason(child, DUE + timedelta(minutes=1)) is None
        assert store.get_cross_period_wait(CHILD)['decision_reason'] == 'WAIT_DISABLED'
        wait.enabled = True
        assert wait.reason(child, DUE + timedelta(minutes=2)) == 'CROSS_PERIOD_WAITING'
        assert store.get_cross_period_wait(CHILD)['deadline_at'] == '2026-10-02T00:15:00+08:00'
        scheduler.run_pending(DUE + timedelta(minutes=5))
        assert calls == [CHILD]
    finally:
        store.close()


def test_zero_and_task_override_control_actual_cron_wait(tmp_path):
    calls = []
    scheduler, store = waiting_scheduler(tmp_path, calls)
    try:
        scheduler.cross_period_wait.seconds = 900
        scheduler.cross_period_wait.overrides = {'task_hour': 0}
        scheduler.tick(DUE)
        assert calls == [CHILD]
        wait = store.get_cross_period_wait(CHILD)
        assert wait['wait_seconds'] == 0
        assert wait['deadline_at'] == '2026-10-02T00:10:00+08:00'
        assert wait['decision_reason'] == 'WAIT_ZERO'
    finally:
        store.close()


def test_overdue_recovery_uses_due_instead_of_recovery_clock(tmp_path):
    calls = []
    scheduler, store = waiting_scheduler(tmp_path, calls)
    try:
        scheduler.tick(DUE + timedelta(minutes=20))
        assert calls == [CHILD]
        wait = store.get_cross_period_wait(CHILD)
        assert wait['status'] == 'TIMED_OUT'
        assert wait['deadline_at'] == '2026-10-02T00:15:00+08:00'
    finally:
        store.close()


def test_existing_wait_snapshot_survives_duplicate_enqueue_and_policy_changes(tmp_path):
    calls = []
    scheduler, store = waiting_scheduler(tmp_path, calls)
    try:
        scheduler.tick(DUE)
        child = scheduler._pending_instance(store.get_instance(CHILD))
        child.scheduled_time += timedelta(minutes=7)
        scheduler.cross_period_wait.seconds = 3600
        records = scheduler.cross_period_wait.records([child])
        store.enqueue_and_advance([child], {}, cross_period_waits=records)
        wait = store.get_cross_period_wait(CHILD)
        assert wait['wait_seconds'] == 300
        assert wait['scheduled_due_at'] == '2026-10-02T00:10:00+08:00'
        assert wait['deadline_at'] == '2026-10-02T00:15:00+08:00'
    finally:
        store.close()


def test_sql_execution_time_advances_deadline_within_one_tick(tmp_path, monkeypatch):
    calls = []
    scheduler, store = waiting_scheduler(tmp_path, calls)
    elapsed = [0.0]
    monkeypatch.setattr('data_converge.dag.executor.time.monotonic', lambda: elapsed[0])
    first_end = COVERAGE + timedelta(hours=1)
    scheduler.dag_executor.readiness_check = lambda instance, now: (
        'WAITING_DWD' if instance.taskname == 'task_5min' and instance.data_end != first_end else None)

    def execute(instance):
        calls.append(instance.instance_id)
        elapsed[0] = 301.0

    scheduler.dag_executor.runner = execute
    try:
        scheduler.tick(DUE)
        assert calls == ['task_5min@2026-10-01T01:00:00+08:00', CHILD]
        assert store.get_cross_period_wait(CHILD)['status'] == 'TIMED_OUT'
        assert store.get_cross_period_wait(CHILD)['missing_count'] == 23
    finally:
        store.close()


def test_dwd_hard_gate_cannot_be_bypassed_at_deadline(tmp_path):
    calls = []
    scheduler, store = waiting_scheduler(tmp_path, calls)
    scheduler.dag_executor.readiness_check = lambda instance, now: 'WAITING_DWD'
    try:
        scheduler.tick(DUE + timedelta(minutes=5))
        assert calls == []
        assert store.get_instance(CHILD)['attempts'] == 0
        assert store.get_instance(CHILD)['error'] == 'WAITING_DWD'
    finally:
        store.close()


def test_timeout_samples_capped_and_success_preserves_incomplete_evidence(tmp_path):
    calls = []
    rules = make_rules()
    rules[1].update(aggregation_interval='P1D', cron='10 0 * * *')
    scheduler, store = persistent_scheduler(tmp_path, calls, COVERAGE, rules=rules)
    scheduler.dag_executor.readiness_check = lambda instance, now: (
        'WAITING_DWD' if instance.taskname == 'task_5min' else None)
    try:
        scheduler.tick(DUE + timedelta(minutes=5))
        assert calls == [CHILD]
        wait = store.get_cross_period_wait(CHILD)
        assert wait['missing_count'] == 288
        assert sum(wait['missing_status_counts'].values()) == 288
        assert len(wait['missing_samples']) == 100 and wait['missing_truncated']
        assert store.get_instance(CHILD)['status'] == 'SUCCESS'
        scheduler.run_pending(DUE + timedelta(hours=1))
        assert store.get_cross_period_wait(CHILD) == wait
    finally:
        store.close()


def test_wait_registration_rolls_back_with_task_and_cursor(tmp_path):
    import sqlite3
    import pytest
    calls = []
    scheduler, store = waiting_scheduler(tmp_path, calls)
    child = scheduler.dag_builder.create_instance(scheduler.dag, 'task_hour', DUE)
    records = scheduler.cross_period_wait.records([child])
    records[0]['wait_seconds'] = None
    try:
        with pytest.raises(sqlite3.IntegrityError):
            store.enqueue_and_advance([child], {'task_hour': DUE}, cross_period_waits=records)
        assert store.get_instance(CHILD) is None
        assert store.get_cursor('task_hour') is None
        assert store.get_cross_period_wait(CHILD) is None
    finally:
        store.close()


def test_legacy_read_does_not_create_cross_period_table(tmp_path):
    from data_converge.state import StateStore
    import sqlite3
    calls = []
    scheduler, store = waiting_scheduler(tmp_path, calls)
    path = store.path
    store.close()
    with sqlite3.connect(path) as conn:
        before = list(conn.iterdump())
    reader = StateStore(path)
    try:
        assert reader.get_cross_period_wait(CHILD) is None
    finally:
        reader.close()
    with sqlite3.connect(path) as conn:
        assert list(conn.iterdump()) == before


def test_same_period_unknown_blocks_even_when_cross_period_deadline_expired(tmp_path):
    calls = []
    rules = make_rules()
    rules[0].update(aggregation_interval='PT1H', cron='0 * * * *')
    rules[1].update(aggregation_interval='P1D', cron='10 0 * * *',
                    depends_on=['task_5min', 'same_day'])
    rules.append(dict(rules[1], taskname='same_day', depends_on=[]))
    scheduler, store = persistent_scheduler(tmp_path, calls, COVERAGE, rules=rules)
    scheduler.dag_executor.readiness_check = lambda instance, now: (
        'WAITING_DWD' if instance.taskname == 'task_5min' else None)

    def fail_same_day(instance):
        calls.append(instance.taskname)
        raise RuntimeError('unknown outcome')

    scheduler.dag_executor.runner = fail_same_day
    try:
        scheduler.tick(DUE + timedelta(minutes=5))
        assert calls == ['same_day']
        assert store.get_instance(CHILD)['attempts'] == 0
        assert store.get_instance(CHILD)['status'] == 'BLOCKED'
        assert store.get_cross_period_wait(CHILD)['checked_at'] is None
    finally:
        store.close()


def test_timeout_retry_preserves_release_audit_and_attempt_limit(tmp_path, monkeypatch):
    from data_converge.errors import TaskExecutionError
    calls = []
    scheduler, store = waiting_scheduler(tmp_path, calls)
    now = [DUE + timedelta(minutes=5)]
    monkeypatch.setattr(scheduler.dag_executor, '_now', lambda instance: now[0])

    def fail_retryable(instance):
        calls.append(instance.instance_id)
        raise TaskExecutionError('SQL_FAILED', retryable=True)

    scheduler.dag_executor.runner = fail_retryable
    try:
        scheduler.tick(now[0])
        first = store.get_cross_period_wait(CHILD)
        for _ in range(4):
            now[0] += timedelta(seconds=30)
            scheduler.run_pending(now[0])
        assert calls == [CHILD] * 3
        assert store.get_instance(CHILD)['attempts'] == 3
        assert store.get_cross_period_wait(CHILD) == first
    finally:
        store.close()
