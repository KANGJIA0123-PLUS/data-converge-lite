"""Read-only admission checks for a test write that formal scheduling must cover."""
from datetime import datetime
from zoneinfo import ZoneInfo

from apscheduler.triggers.cron import CronTrigger

from data_converge.config.evolution import build_manifest
from data_converge.dag.builder import timeline_time
from data_converge.scheduler.cron import next_fire_time


def configuration_evidence(definition, taskname, manifest=None):
    manifest = build_manifest(definition) if manifest is None else manifest
    return {'task': manifest['tasks'][taskname], 'connection': manifest['connection'],
            'dataset_id': manifest['dataset_id'], 'timezone': definition.options.timezone,
            'cleanup_column': definition.dag.tasks[taskname].cleanup_column}


def formal_scheduled_time(definition, instance, cursor=None):
    task = definition.dag.get_task(instance.taskname)
    zone = ZoneInfo(definition.options.timezone)
    trigger = CronTrigger.from_crontab(task.cron, timezone=zone)
    wall = CronTrigger.from_crontab(task.cron, timezone='UTC')
    lower = max((instance.data_end, cursor or instance.data_end), key=timeline_time)
    due = next_fire_time(trigger, wall, lower.astimezone(zone),
                         move_missing=task.aggregation_interval in ('P1D', 'P1M'))
    if due is None:
        raise ValueError('BACKFILL_PREAGGREGATE_NO_FORMAL_WINDOW')
    actual = definition.builder.create_instance(definition.dag, instance.taskname, due)
    if actual.instance_id != instance.instance_id:
        code = 'CURSOR_PASSED' if cursor and timeline_time(cursor) > timeline_time(instance.data_end) else 'NO_FORMAL_WINDOW'
        raise ValueError('BACKFILL_PREAGGREGATE_' + code)
    return due


def validate_eligibility(definition, store, instance, now, item=None):
    if timeline_time(instance.data_end) <= timeline_time(now):
        raise ValueError('BACKFILL_PREAGGREGATE_EXPIRED')
    if timeline_time(instance.data_start) > timeline_time(now):
        raise ValueError('BACKFILL_WINDOW_NOT_STARTED')
    if store is None or store.get_meta('coverage_start') is None:
        raise ValueError('BACKFILL_PREAGGREGATE_COVERAGE_MISSING')
    coverage = datetime.fromisoformat(store.get_meta('coverage_start'))
    if timeline_time(instance.data_start) < timeline_time(coverage):
        raise ValueError('BACKFILL_PREAGGREGATE_BEFORE_COVERAGE')
    effective = store.get_task_effective_starts().get(instance.taskname)
    if effective and timeline_time(instance.data_start) < timeline_time(effective):
        raise ValueError('BACKFILL_PREAGGREGATE_BEFORE_EFFECTIVE_START')
    if store.get_retirement(instance.instance_id) is not None:
        raise ValueError('BACKFILL_PREAGGREGATE_RETIRED')
    if store.get_instance(instance.instance_id) is not None:
        raise ValueError('BACKFILL_PREAGGREGATE_FORMAL_EXISTS')
    blocker = store.preaggregate_blocker(instance.instance_id)
    if blocker:
        raise ValueError('BACKFILL_PREAGGREGATE_UNCONFIRMED')
    due = formal_scheduled_time(definition, instance, store.get_cursor(instance.taskname))
    if item is not None:
        if (item.get('config_evidence') != configuration_evidence(definition, instance.taskname)
                or item.get('formal_scheduled_time') != due.isoformat()):
            raise ValueError('BACKFILL_CONFIGURATION_CHANGED')
    return due


def classify_plan(definition, store, items, now):
    selected, omitted = [], []
    manifest = build_manifest(definition)
    zone = ZoneInfo(definition.options.timezone)
    for item in items:
        instance = definition.builder.create_instance(
            definition.dag, item['taskname'], datetime.fromisoformat(item['scheduled_time']).astimezone(zone))
        if timeline_time(instance.data_start) > timeline_time(now):
            omitted.append(dict(item, reason='BACKFILL_WINDOW_NOT_STARTED'))
            continue
        item = dict(item, ordinal=len(selected), execution_kind='BACKFILL')
        if timeline_time(instance.data_end) > timeline_time(now):
            due = validate_eligibility(definition, store, instance, now)
            item.update(execution_kind='PREAGGREGATE', formal_scheduled_time=due.isoformat(),
                        config_evidence=configuration_evidence(definition, instance.taskname, manifest))
        selected.append(item)
    if not selected:
        raise ValueError('BACKFILL_EMPTY_RANGE')
    return selected, omitted
