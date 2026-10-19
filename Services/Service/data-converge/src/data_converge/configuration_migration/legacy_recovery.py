"""Resume new windows when a legacy definition cannot be reconstructed."""

from datetime import datetime, timezone
import json
import logging
import re
from uuid import uuid4

from data_converge.application import next_boundary, parse_time
from data_converge.config.application import StartupError
from data_converge.config.evolution import manifest_digest
from data_converge.config.state_binding import fenced_binding
from data_converge.leader_election import LeadershipError

from .service import _assert_files_unchanged, _backup

logger = logging.getLogger(__name__)


def _utc_now():
    return datetime.now(timezone.utc)


def _candidate(application, store):
    if (store.get_meta('configuration_snapshot') is not None or
            store.get_meta('configuration_required') is not None):
        return None
    store.validate_retirement()
    if store.configuration_history(limit=1):
        raise StartupError('LEGACY_RECOVERY_INVALID')
    old = store.get_meta('binding')
    if not isinstance(old, str) or re.fullmatch(r'[a-f0-9]{64}', old) is None or old == application.fingerprint:
        return None
    coverage = store.get_meta('coverage_start')
    if not coverage:
        return None
    start = parse_time(coverage)
    if next_boundary(start) != start:
        raise StartupError('LEGACY_RECOVERY_INVALID')
    return old, start


def _check_registry(application, store, lease):
    previous = store.get_meta('leadership_cluster')
    if lease is None:
        if previous is not None:
            raise LeadershipError('LEADERSHIP_REQUIRED_FOR_LEDGER')
        return
    if previous is not None and previous != application.definition.leadership.cluster_name:
        raise LeadershipError('LEADERSHIP_CLUSTER_CHANGED')
    identity = store.get_meta('leadership_ledger_id')
    if previous is not None and not identity:
        raise StartupError('LEGACY_RECOVERY_INVALID')
    lease.assert_owned()
    lease.check_ledger(identity, require_existing=previous is not None)
    lease.assert_owned()


def _record(application, store, old, coverage, migration_id, backup_path):
    summary = store.legacy_window_summary()
    instant = _utc_now()
    limits = [instant, coverage, summary['latest_end'], summary['latest_signal_end']]
    cutover = next_boundary(max(value for value in limits if value is not None))
    return dict(schema_version=1, migration_id=migration_id, cutover=cutover.isoformat(),
                previous_binding=old, current_binding=fenced_binding(application.fingerprint),
                revision=manifest_digest(application.manifest), backup_path=str(backup_path),
                recovered_at=instant.isoformat(), identity_verified=False,
                previous_definition_available=False, historical_counts=summary['counts'])


def recover_legacy(application, store, lease):
    candidate = _candidate(application, store)
    if candidate is None:
        return
    old, coverage = candidate
    _check_registry(application, store, lease)
    migration_id = uuid4().hex
    backup_path = _backup(application, store, migration_id, None)
    try:
        _assert_files_unchanged(application)
    except StartupError:
        raise StartupError('LEGACY_RECOVERY_CONFIGURATION_CHANGED') from None
    record = _record(application, store, old, coverage, migration_id, backup_path)
    if lease is not None:
        lease.assert_owned()
    store.apply_configuration(expected_binding=old, expected_revision=None,
        manifest=application.manifest, revision=record['revision'],
        task_starts={name: parse_time(record['cutover']) for name in application.definition.dag.tasks},
        binding=record['current_binding'], migration_id=migration_id,
        applied_at=parse_time(record['recovered_at']), backup_path=backup_path,
        retire_existing=True, recovery=record)
    logger.warning('LEGACY_NEW_WINDOWS_RECOVERED diagnostic=%s', json.dumps(record, ensure_ascii=True))


def log_retained_history(store):
    record = store.legacy_recovery()
    if record is not None:
        evidence = {key: record[key] for key in ('migration_id', 'cutover', 'backup_path', 'historical_counts')}
        logger.warning('LEGACY_HISTORY_RETAINED diagnostic=%s', json.dumps(evidence, ensure_ascii=True))
