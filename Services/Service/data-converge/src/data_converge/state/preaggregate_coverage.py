"""Durable, narrowly scoped formal coverage for the first recovered hour/day."""
from datetime import datetime
import json

from data_converge.dag.builder import timeline_time
from . import retirement


def _time(value):
    result = datetime.fromisoformat(value) if isinstance(value, str) else value
    if not isinstance(result, datetime) or result.utcoffset() is None:
        raise ValueError('PREAGGREGATE_TIMEZONE_REQUIRED')
    return result


def _limits(conn):
    values = dict(conn.execute("SELECT key,value FROM metadata WHERE key IN "
                               "('configuration_snapshot','coverage_start')"))
    snapshot = json.loads(values.get('configuration_snapshot', '{}'))
    return snapshot.get('task_starts', {}), values.get('coverage_start')


def _expected(conn, taskname, interval, start, end, instance_id, starts, coverage):
    recovery = retirement.recovery(conn)
    effective = starts.get(taskname)
    if recovery is None or interval not in ('PT1H', 'P1D') or effective is None or coverage is None:
        return None
    start, end = _time(start), _time(end)
    cutover, effective, coverage = map(_time, (recovery['cutover'], effective, coverage))
    if not (timeline_time(coverage) <= timeline_time(start) < timeline_time(cutover) < timeline_time(end)):
        return None
    if timeline_time(effective) != timeline_time(cutover) or retirement.get(conn, instance_id) is not None:
        return None
    return {'migration_id': recovery['migration_id'], 'cutover': cutover.isoformat(),
            'task_effective_start': effective.isoformat(), 'coverage_start': coverage.isoformat()}


def admission(conn, instance, now):
    starts, coverage = _limits(conn)
    expected = _expected(conn, instance.taskname, instance.task.aggregation_interval,
                         instance.data_start, instance.data_end, instance.instance_id, starts, coverage)
    if expected is None or not (timeline_time(_time(expected['cutover'])) <= timeline_time(_time(now))
                                < timeline_time(instance.data_end)):
        return None
    return expected


def _evidence(row):
    value = row['config_evidence']
    return json.loads(value) if isinstance(value, str) else value


def valid_record(conn, row, *, task_starts=None, coverage=None):
    evidence = _evidence(row)
    marker = evidence.get('recovery_coverage')
    if marker is None or row['query_id'] is None or row['covered_at'] is not None:
        return False
    if task_starts is None:
        task_starts, coverage = _limits(conn)
    try:
        interval = evidence['task']['definition']['aggregation_interval']
        expected = _expected(conn, row['taskname'], interval, row['data_start'], row['data_end'],
                             row['instance_id'], task_starts, coverage)
        return expected is not None and marker == expected
    except (KeyError, TypeError, ValueError):
        return False


def pending(conn, instance_id=None):
    exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                          "AND name='preaggregate_attempts'").fetchone()
    if exists is None:
        return []
    condition = ' AND instance_id=?' if instance_id is not None else ''
    values = (instance_id,) if instance_id is not None else ()
    rows = conn.execute("SELECT * FROM preaggregate_attempts WHERE query_id IS NOT NULL "
                        "AND covered_at IS NULL" + condition + " ORDER BY rowid", values).fetchall()
    result = {}
    for row in rows:
        if 'recovery_coverage' not in _evidence(row):
            continue
        if not valid_record(conn, row):
            raise ValueError('PREAGGREGATE_RECOVERY_COVERAGE_INVALID:' + row['item_id'])
        record = dict(row)
        record['config_evidence'] = _evidence(row)
        result[row['instance_id']] = record
    return list(result.values())


def covers_window(conn, instance):
    for row in pending(conn, instance.instance_id):
        if row['instance_id'] != instance.instance_id or row['taskname'] != instance.taskname:
            continue
        pairs = ((row['data_start'], instance.data_start), (row['data_end'], instance.data_end),
                 (row['formal_scheduled_time'], instance.scheduled_time))
        interval = row['config_evidence']['task']['definition']['aggregation_interval']
        return (interval == instance.task.aggregation_interval
                and all(timeline_time(_time(left)) == timeline_time(right) for left, right in pairs))
    return False


def validate_dispatch(conn, item, instance, now):
    marker = item['config_evidence'].get('recovery_coverage')
    if marker is not None and marker != admission(conn, instance, now):
        raise ValueError('PREAGGREGATE_RECOVERY_COVERAGE_INVALID')
