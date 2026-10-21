"""Build bounded, side-effect-free plans for manual historical aggregation."""

from __future__ import annotations

from datetime import datetime
import hashlib
import json
from zoneinfo import ZoneInfo

from data_converge.config.application import ApplicationDefinition
from data_converge.config.evolution import build_manifest, manifest_digest
from data_converge.dag.builder import align_data_end, subtract_interval, timeline_time


MAX_PLAN_ITEMS = 10000


def plan_revision(definition: ApplicationDefinition) -> str:
    """Bind queued work to SQL, dataset, timezone and destructive cleanup policy."""
    identity = {
        'manifest': manifest_digest(build_manifest(definition)),
        'timezone': definition.options.timezone,
        'cleanup_columns': {name: task.cleanup_column
                            for name, task in definition.dag.tasks.items()},
    }
    encoded = json.dumps(identity, sort_keys=True, separators=(',', ':')).encode('utf-8')
    return hashlib.sha256(encoded).hexdigest()


def _parse_time(value: str, zone: ZoneInfo) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        raise ValueError('BACKFILL_TIME_INVALID') from None
    if parsed.utcoffset() is None:
        raise ValueError('BACKFILL_TIMEZONE_REQUIRED')
    try:
        return parsed.astimezone(zone)
    except (OverflowError, ValueError):
        raise ValueError('BACKFILL_TIME_INVALID') from None


def _range(definition, start: str, end: str, now: datetime, preaggregate=False):
    zone = ZoneInfo(definition.options.timezone)
    lower, upper = _parse_time(start, zone), _parse_time(end, zone)
    current = now
    if not isinstance(current, datetime) or current.utcoffset() is None:
        raise ValueError('BACKFILL_TIMEZONE_REQUIRED')
    if timeline_time(lower) >= timeline_time(upper):
        raise ValueError('BACKFILL_RANGE_INVALID')
    if not preaggregate and timeline_time(upper) > timeline_time(current):
        raise ValueError('BACKFILL_END_IN_FUTURE')
    return lower, upper


def _selection(definition, task: str | None, all_tasks: bool) -> list[str]:
    if task is not None and all_tasks:
        raise ValueError('BACKFILL_SELECTION_CONFLICT')
    if task is None and not all_tasks:
        raise ValueError('BACKFILL_SELECTION_REQUIRED')
    if task is not None and task not in definition.dag.tasks:
        raise ValueError('BACKFILL_TASK_NOT_FOUND')
    return [name for name in definition.dag.topological_order
            if all_tasks or name == task]


def _check_alignment(definition, task: str, start: datetime, end: datetime):
    interval = definition.dag.get_task(task).aggregation_interval
    if any(timeline_time(align_data_end(value, interval)) != timeline_time(value)
           for value in (start, end)):
        raise ValueError('BACKFILL_BOUNDARY_NOT_ALIGNED')


def _windows(definition, task: str, start: datetime, end: datetime):
    interval = definition.dag.get_task(task).aggregation_interval
    cursor = align_data_end(end, interval)
    while timeline_time(cursor) > timeline_time(start):
        previous = subtract_interval(cursor, interval)
        if timeline_time(previous) < timeline_time(start):
            break
        instance = definition.builder.create_instance(definition.dag, task, cursor)
        if (timeline_time(instance.data_start) != timeline_time(previous)
                or timeline_time(instance.data_end) != timeline_time(cursor)
                or timeline_time(cursor) > timeline_time(end)
                or timeline_time(previous) >= timeline_time(cursor)):
            raise ValueError('BACKFILL_WINDOW_INVALID')
        yield instance
        cursor = previous


def _item(instance, ordinal: int) -> dict:
    return {
        'ordinal': ordinal, 'taskname': instance.taskname,
        'instance_id': instance.instance_id,
        'scheduled_time': instance.scheduled_time.isoformat(),
        'data_start': instance.data_start.isoformat(),
        'data_end': instance.data_end.isoformat(),
        'upstream_instance_ids': list(instance.upstream_instance_ids),
    }


def _items(definition, selected: list[str], start: datetime, end: datetime):
    items, counts = [], {}
    for name in selected:
        windows = []
        for instance in _windows(definition, name, start, end):
            if len(items) + len(windows) >= MAX_PLAN_ITEMS:
                raise ValueError('BACKFILL_ITEM_LIMIT_EXCEEDED')
            windows.append(instance)
        counts[name] = len(windows)
        for instance in reversed(windows):
            items.append(_item(instance, len(items)))
    if not items:
        raise ValueError('BACKFILL_EMPTY_RANGE')
    return items, counts


def build_plan(
    definition: ApplicationDefinition, start: str, end: str, *,
    task: str | None = None, all_tasks: bool = False, recompute: bool = False,
    now: datetime | None = None, preaggregate: bool = False, store=None,
) -> dict:
    """Select complete windows in [start, end), never widening the request."""
    now = datetime.now(ZoneInfo(definition.options.timezone)) if now is None else now
    lower, upper = _range(definition, start, end, now, preaggregate)
    selected = _selection(definition, task, all_tasks)
    if task is not None:
        _check_alignment(definition, task, lower, upper)
    items, counts = _items(definition, selected, lower, upper)
    omitted = []
    if preaggregate:
        from .preaggregate import classify_plan
        items, omitted = classify_plan(definition, store, items, now)
        counts = {name: sum(item['taskname'] == name for item in items) for name in selected}
    result = {
        'schema_version': 2 if preaggregate else 1,
        'revision': plan_revision(definition),
        'start': lower.isoformat(), 'end': upper.isoformat(),
        'recompute': recompute, 'selected_tasks': selected,
        'omitted_tasks': [name for name in selected if counts[name] == 0],
        'counts': counts, 'total': len(items), 'items': items,
    }
    if preaggregate:
        result.update(preaggregate=True, omitted_future_count=len(omitted), omitted_future=omitted)
    return result
