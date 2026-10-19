"""Build deterministic, sanitized migration plans from one ledger snapshot."""

from dataclasses import dataclass
from datetime import datetime
import hashlib
import json

from data_converge.application import next_boundary, parse_time
from data_converge.config.application import StartupError
from data_converge.config.evolution import compare_manifests, manifest_digest

from .baseline import load_baseline


@dataclass(frozen=True)
class MigrationPlan:
    public: dict
    manifest: dict
    binding: str
    previous_binding: str
    previous_revision: str | None
    task_starts: dict[str, datetime]


def _effective_start(value, required):
    if value is None:
        if required:
            raise StartupError('CONFIG_EFFECTIVE_FROM_REQUIRED')
        return None
    try:
        result = parse_time(value)
        if next_boundary(result) != result:
            raise ValueError
    except ValueError:
        raise StartupError('CONFIG_EFFECTIVE_FROM_INVALID') from None
    return result


def _coordination_blockers(store, current):
    previous = store.get_meta('leadership_cluster')
    coordination = current['coordination']
    if previous is not None and (not coordination['enabled'] or previous != coordination['cluster_name']):
        return [{'code': 'CONFIG_COORDINATION_CHANGED'}]
    if previous is not None and not store.get_meta('leadership_ledger_id'):
        return [{'code': 'CONFIG_STATE_INVALID'}]
    return []


def _requirements(previous, current, changes, legacy):
    required = ['accept_legacy_baseline'] if legacy else []
    if changes['connection_changed'] or changes['readiness_changed'] or changes['dataset_changed']:
        required.append('confirm_same_data')
    if changes['readiness_changed']:
        required.append('confirm_readiness_change')
    blockers = []
    if previous['dataset_id'] is not None and previous['dataset_id'] != current['dataset_id']:
        blockers.append({'code': 'CONFIG_DATASET_CHANGED'})
    return required, blockers


def _task_blockers(constraints, changes, effective, coverage):
    blockers = []
    if constraints['running']:
        blockers.append({'code': 'CONFIG_RUNNING_INSTANCES', 'count': constraints['running']})
    if effective is not None and effective < coverage:
        blockers.append({'code': 'CONFIG_EFFECTIVE_BEFORE_COVERAGE'})
    for name, values in constraints['tasks'].items():
        if values['unresolved']:
            blockers.append({'code': 'CONFIG_TASKS_UNRESOLVED', 'task': name,
                             'count': values['unresolved']})
        if name not in changes['removed'] and effective is not None:
            if values['latest_end'] is not None and effective < values['latest_end']:
                blockers.append({'code': 'CONFIG_TASK_WINDOW_OVERLAP', 'task': name})
    return blockers


def _serialized_constraints(constraints):
    return {'running': constraints['running'], 'tasks': {
        name: {'unresolved': values['unresolved'],
               'latest_end': values['latest_end'].isoformat() if values['latest_end'] else None}
        for name, values in sorted(constraints['tasks'].items())}}


def _plan_id(public, previous_binding, baseline):
    value = dict(public, previous_binding=previous_binding, baseline=manifest_digest(baseline))
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def build_plan(application, store, *, previous_config=None, effective_from=None, allow_backfill=False):
    previous_binding, coverage_value = store.get_meta('binding'), store.get_meta('coverage_start')
    if not previous_binding or not coverage_value:
        raise StartupError('CONFIG_STATE_INVALID')
    coverage = parse_time(coverage_value)
    previous, previous_revision, old_starts = load_baseline(store, coverage, previous_config)
    current = application.manifest
    changes = compare_manifests(previous, current)
    effective = _effective_start(effective_from, changes['added'] or changes['changed'])
    affected = sorted(set(changes['added'] + changes['changed'] + changes['removed']))
    constraints = store.configuration_constraints(affected)
    starts = {name: effective if name in changes['added'] + changes['changed'] else old_starts[name]
              for name in current['tasks']}
    required, blockers = _requirements(previous, current, changes, previous_revision is None)
    blockers += _coordination_blockers(store, current)
    blockers += _task_blockers(constraints, changes, effective, coverage)
    public = {'legacy': previous_revision is None, 'previous_revision': previous_revision,
              'revision': manifest_digest(current), 'coverage_start': coverage.isoformat(),
              'changes': changes, 'required_confirmations': required, 'blockers': blockers,
              'task_starts': {name: value.isoformat() for name, value in sorted(starts.items())},
              'restart_cursors': sorted(changes['added'] + changes['changed']),
              'effective_from': effective.isoformat() if effective else None,
              'allow_backfill': bool(allow_backfill), 'constraints': _serialized_constraints(constraints)}
    public['plan_id'] = _plan_id(public, previous_binding, previous)
    return MigrationPlan(public, current, application.fingerprint, previous_binding, previous_revision, starts)
