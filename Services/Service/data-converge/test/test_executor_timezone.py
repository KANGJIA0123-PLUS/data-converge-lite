"""Execution order and retry delays follow elapsed time through repeated hours."""

from datetime import datetime
from zoneinfo import ZoneInfo

from data_converge.dag import DAGBuilder, DAGExecutor, TaskStatus
from data_converge.dag.retry import can_start_attempt
from data_converge.errors import TaskExecutionError

from .test_dag import FakeConfigLoader


def test_repeated_hour_is_executed_in_absolute_time_order():
    builder = DAGBuilder(FakeConfigLoader())
    dag = builder.build()
    zone = ZoneInfo('America/New_York')
    times = [datetime(2026, 11, 1, 1, 6, tzinfo=zone, fold=fold) for fold in (1, 0)]
    instances = [builder.create_instance(dag, 'five_min', instant) for instant in times]
    seen = []
    DAGExecutor(dag, lambda instance: seen.append(instance.instance_id)).execute(
        instances, max_executions=1)
    assert seen == ['five_min@2026-11-01T01:05:00-04:00']


def test_retry_delay_crosses_repeated_hour_by_elapsed_seconds(monkeypatch):
    builder = DAGBuilder(FakeConfigLoader())
    dag = builder.build()
    zone = ZoneInfo('America/New_York')
    finished = datetime(2026, 11, 1, 1, 59, 50, tzinfo=zone)
    instance = builder.create_instance(dag, 'five_min', finished)

    def fail(_):
        raise TaskExecutionError('CLICKHOUSE_CONNECT_FAILED', retryable=True)

    executor = DAGExecutor(dag, fail, retry_delay_seconds=30)
    monkeypatch.setattr(executor, '_now', lambda _: finished)
    executor.execute([instance], now=finished)
    record = executor.repository.get_instance(instance.instance_id)
    assert record['next_retry_at'].isoformat() == '2026-11-01T01:00:20-05:00'


def test_retry_is_due_after_clock_moves_back():
    zone = ZoneInfo('America/New_York')
    retry = datetime(2026, 11, 1, 1, 50, tzinfo=zone)
    now = datetime(2026, 11, 1, 1, 10, tzinfo=zone, fold=1)
    assert can_start_attempt(TaskStatus.FAILED, 1, True, retry, now=now, max_attempts=3)


def test_empty_retry_deadline_is_treated_as_none():
    now = datetime(2026, 11, 1, 1, 10, tzinfo=ZoneInfo('UTC'))
    assert can_start_attempt(TaskStatus.FAILED, 1, True, '', now=now, max_attempts=3)
