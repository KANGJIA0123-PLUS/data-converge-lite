"""Explicit, credential-free configuration manifests and task revisions."""

from __future__ import annotations

from datetime import datetime
from graphlib import CycleError, TopologicalSorter
import hashlib
import json
import re
from zoneinfo import ZoneInfo

from data_converge.sql_execution.client import load_sql
from data_converge.sql_execution.config import connection_origin

from .application import ApplicationDefinition, StartupError

_MANIFEST_FIELDS = {'schema_version', 'dataset_id', 'connection', 'readiness',
                    'coordination', 'tasks'}
_DEFINITION_FIELDS = {'taskname', 'source_tables', 'target_table', 'aggregation_interval',
                      'cron', 'depends_on', 'sql_id'}
_DIRECT_REDIS_FIELDS = ('mode', 'db', 'host', 'port')
_SENTINEL_REDIS_FIELDS = ('mode', 'db', 'service_name', 'username', 'paas_name', 'password_file')


def _digest(value: dict) -> str:
    serialized = json.dumps(value, sort_keys=True, ensure_ascii=True, separators=(',', ':'))
    return hashlib.sha256(serialized.encode('utf-8')).hexdigest()


def _dataset_id(value, code: str):
    if value is not None and (not isinstance(value, str) or
                              re.fullmatch(r'[A-Za-z0-9_.-]{1,128}', value) is None):
        raise StartupError(code)
    return value


def _readiness_manifest(ready: dict) -> dict:
    result = {'enabled': ready['enabled']}
    if ready['enabled']:
        redis = ready['redis']
        fields = _SENTINEL_REDIS_FIELDS if redis['mode'] == 'sentinel' else _DIRECT_REDIS_FIELDS
        result.update(queue_key=ready['queue_key'], redis={name: redis.get(name) for name in fields})
    return result


def _task_manifest(task, sql_directory) -> dict:
    definition = {
        'taskname': task.taskname, 'source_tables': list(task.source_tables),
        'target_table': task.target_table, 'aggregation_interval': task.aggregation_interval,
        'cron': task.cron, 'depends_on': sorted(task.depends_on), 'sql_id': task.sql_id,
    }
    try:
        sql = load_sql(sql_directory / (task.sql_id + '.sql'))
    except (OSError, ValueError):
        raise StartupError('CONFIGURATION_SQL_UNREADABLE') from None
    return {'definition': definition, 'sql_sha256': hashlib.sha256(sql.encode('utf-8')).hexdigest()}


def _task_revisions(tasks: dict) -> dict[str, str]:
    dependencies = {name: task['definition']['depends_on'] for name, task in tasks.items()}
    revisions = {}
    for name in TopologicalSorter(dependencies).static_order():
        task = tasks[name]
        revisions[name] = _digest({
            'definition': task['definition'], 'sql_sha256': task['sql_sha256'],
            'upstream_revisions': {parent: revisions[parent] for parent in sorted(dependencies[name])},
        })
    return revisions


def build_manifest(definition: ApplicationDefinition) -> dict:
    """Read approved SQL bytes and copy only explicit persistent identity fields."""
    state = definition.config.get('state', {})
    if not isinstance(state, dict):
        raise StartupError('DATASET_ID_INVALID')
    dataset = _dataset_id(state.get('dataset_id'), 'DATASET_ID_INVALID')
    connection = definition.config['clickhouse']
    tasks = {name: _task_manifest(definition.dag.tasks[name], definition.sql_dir)
             for name in sorted(definition.dag.tasks)}
    for name, revision in _task_revisions(tasks).items():
        tasks[name]['revision'] = revision
    manifest = {
        'schema_version': 1, 'dataset_id': dataset,
        'connection': {'endpoint': connection_origin(connection),
                       'database': connection['database'], 'username': connection['username']},
        'readiness': _readiness_manifest(definition.config['dwd_ready']),
        'coordination': {'enabled': definition.leadership.enabled,
                         'cluster_name': definition.leadership.cluster_name},
        'tasks': tasks,
    }
    validate_manifest(manifest)
    return manifest


def manifest_digest(manifest: dict) -> str:
    """Return a stable SHA-256 independent of dictionary insertion order."""
    validate_manifest(manifest)
    return _digest(manifest)


def compare_manifests(previous: dict, current: dict) -> dict:
    """Return safe change categories; downstream revisions include upstream changes."""
    validate_manifest(previous)
    validate_manifest(current)
    old, new = previous['tasks'], current['tasks']
    return {
        'added': sorted(new.keys() - old.keys()),
        'changed': sorted(name for name in new.keys() & old.keys()
                          if new[name]['revision'] != old[name]['revision']),
        'removed': sorted(old.keys() - new.keys()),
        'connection_changed': previous['connection'] != current['connection'],
        'readiness_changed': previous['readiness'] != current['readiness'],
        'coordination_changed': previous['coordination'] != current['coordination'],
        'dataset_changed': previous['dataset_id'] != current['dataset_id'],
    }


def _require(condition: bool) -> None:
    if not condition:
        raise StartupError('CONFIGURATION_MANIFEST_INVALID')


def _require_fields(value, expected) -> None:
    _require(isinstance(value, dict) and value.keys() == set(expected))


def _text(value) -> bool:
    return isinstance(value, str) and bool(value) and not any(ord(char) < 32 for char in value)


def _validate_readiness(ready: dict) -> None:
    _require(isinstance(ready, dict) and isinstance(ready.get('enabled'), bool))
    if not ready['enabled']:
        _require_fields(ready, {'enabled'})
        return
    _require_fields(ready, {'enabled', 'queue_key', 'redis'})
    _require(_text(ready['queue_key']))
    redis = ready['redis']
    _require(isinstance(redis, dict))
    mode = redis.get('mode')
    _require(mode in ('standalone', 'cluster', 'sentinel'))
    fields = _SENTINEL_REDIS_FIELDS if mode == 'sentinel' else _DIRECT_REDIS_FIELDS
    _require_fields(redis, fields)
    _require(isinstance(redis['db'], int) and not isinstance(redis['db'], bool) and redis['db'] >= 0)
    if mode == 'sentinel':
        _require(all(_text(redis[key]) for key in ('service_name', 'username', 'password_file')))
        _require(redis['paas_name'] is None or _text(redis['paas_name']))
    else:
        _require(_text(redis['host']) and isinstance(redis['port'], int)
                 and not isinstance(redis['port'], bool) and 1 <= redis['port'] <= 65535)
        _require(mode != 'cluster' or redis['db'] == 0)


def _validate_task(name: str, task: dict) -> None:
    _require(_text(name))
    _require_fields(task, {'definition', 'sql_sha256', 'revision'})
    definition = task['definition']
    _require_fields(definition, _DEFINITION_FIELDS)
    _require(definition['taskname'] == name)
    for key in _DEFINITION_FIELDS - {'source_tables', 'depends_on'}:
        _require(_text(definition[key]))
    for key in ('source_tables', 'depends_on'):
        values = definition[key]
        _require(isinstance(values, list) and all(_text(value) for value in values))
    _require(bool(definition['source_tables']))
    for key in ('sql_sha256', 'revision'):
        value = task[key]
        _require(isinstance(value, str) and re.fullmatch(r'[a-f0-9]{64}', value) is not None)


def validate_manifest(manifest: dict) -> None:
    """Reject unsupported, malformed or inconsistent stored snapshots with a safe code."""
    _require_fields(manifest, _MANIFEST_FIELDS)
    _require(isinstance(manifest['schema_version'], int) and not isinstance(manifest['schema_version'], bool)
             and manifest['schema_version'] == 1)
    _dataset_id(manifest['dataset_id'], 'CONFIGURATION_MANIFEST_INVALID')
    connection = manifest['connection']
    _require_fields(connection, {'endpoint', 'database', 'username'})
    _require(all(_text(value) for value in connection.values()))
    _validate_readiness(manifest['readiness'])
    coordination = manifest['coordination']
    _require_fields(coordination, {'enabled', 'cluster_name'})
    _require(isinstance(coordination['enabled'], bool) and _text(coordination['cluster_name']))
    tasks = manifest['tasks']
    _require(isinstance(tasks, dict) and bool(tasks))
    for name, task in tasks.items():
        _validate_task(name, task)
    try:
        expected = _task_revisions(tasks)
    except (KeyError, CycleError):
        raise StartupError('CONFIGURATION_MANIFEST_INVALID') from None
    _require(all(task['revision'] == expected[name] for name, task in tasks.items()))


def _snapshot_start(value: str) -> datetime:
    instant = datetime.fromisoformat(value)
    if instant.tzinfo is None or instant.utcoffset() is None:
        raise ValueError
    return instant.astimezone(ZoneInfo('Asia/Shanghai'))


def validate_configuration_snapshot(snapshot: dict, coverage: datetime | None = None) -> tuple:
    """Use the same persisted policy checks for startup, migration and diagnostics."""
    try:
        manifest = snapshot['manifest']
        revision = manifest_digest(manifest)
        values = snapshot['task_starts']
        if snapshot['revision'] != revision or not isinstance(values, dict):
            raise ValueError
        if values.keys() != manifest['tasks'].keys():
            raise ValueError
        if coverage is not None and (not isinstance(coverage, datetime) or coverage.utcoffset() is None):
            raise ValueError
        starts = {name: _snapshot_start(value) for name, value in values.items()}
        if coverage is not None and any(value < coverage for value in starts.values()):
            raise ValueError
    except (KeyError, TypeError, ValueError):
        raise StartupError('CONFIG_STATE_INVALID') from None
    return manifest, revision, starts
