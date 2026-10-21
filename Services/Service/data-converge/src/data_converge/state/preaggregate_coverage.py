"""Durable formal coverage for recovered hours/days and explicit current months."""
from datetime import datetime
import json
from zoneinfo import ZoneInfo

from data_converge.dag.builder import align_data_end, subtract_interval, timeline_time
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


def _expected(conn, taskname, interval, start, end, instance_id, starts, coverage, *, zone=None):
    recovery = retirement.recovery(conn)
    effective = starts.get(taskname)
    if interval == 'P1M':
        return _month_expected(conn, start, end, instance_id, effective, coverage, recovery, zone)
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


def _calendar_month(start, end, zone):
    if zone is not None:
        start, end = (value.astimezone(ZoneInfo(zone)) for value in (start, end))
    if any(timeline_time(value) != timeline_time(align_data_end(value, 'P1M'))
           for value in (start, end)):
        return False
    return timeline_time(subtract_interval(end, 'P1M')) == timeline_time(start)


def _month_expected(conn, start, end, instance_id, effective, coverage, recovery, zone):
    if coverage is None or retirement.get(conn, instance_id) is not None:
        return None
    start, end, coverage = map(_time, (start, end, coverage))
    if not _calendar_month(start, end, zone):
        return None
    effective = _time(effective) if effective is not None else None
    cutover = _time(recovery['cutover']) if recovery is not None else None
    if any(timeline_time(value) >= timeline_time(end)
           for value in (coverage, effective, cutover) if value is not None):
        return None
    return {'kind': 'current_month', 'coverage_start': coverage.isoformat(),
            'task_effective_start': effective.isoformat() if effective is not None else None,
            'migration_id': recovery['migration_id'] if recovery is not None else None,
            'cutover': cutover.isoformat() if cutover is not None else None}


def admission(conn, instance, now):
    starts, coverage = _limits(conn)
    expected = _expected(conn, instance.taskname, instance.task.aggregation_interval,
                         instance.data_start, instance.data_end, instance.instance_id, starts, coverage,
                         zone=getattr(instance.data_start.tzinfo, 'key', None))
    if expected is None:
        return None
    current = timeline_time(_time(now))
    fields = ('coverage_start', 'task_effective_start', 'cutover') if expected.get('kind') else ('cutover',)
    if (not timeline_time(instance.data_start) <= current < timeline_time(instance.data_end)
            or any(timeline_time(_time(expected[field])) > current
                   for field in fields if expected[field] is not None)):
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
                             row['instance_id'], task_starts, coverage, zone=evidence.get('timezone'))
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
    if marker != admission(conn, instance, now):
        raise ValueError('PREAGGREGATE_RECOVERY_COVERAGE_INVALID')
