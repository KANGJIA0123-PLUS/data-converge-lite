"""Read-only previews and lease-protected, backed-up configuration migration."""

from contextlib import closing
from datetime import datetime, timezone
import logging
import os
from pathlib import Path
import re
from uuid import uuid4

from data_converge.application import Application, parse_time
from data_converge.config.application import StartupError
from data_converge.config.evolution import manifest_digest
from data_converge.config.state_binding import ledger_binding
from data_converge.coordination import admit_writer
from data_converge.diagnostics.ledger import DiagnosticLedger
from data_converge.runtime import RuntimeDependencies
from data_converge.state import StateStore

from .planner import build_plan
from .baseline import stored_baseline

logger = logging.getLogger(__name__)


def _fresh_application(application):
    # Migration always reviews files on disk, even if a caller reuses an Application.
    current = Application(application.config_path, dependencies=application.dependencies)
    if current.directory.resolve() != application.directory.resolve():
        raise StartupError('CONFIG_PLAN_STALE')
    current.manifest
    current.fingerprint
    return current


def _assert_files_unchanged(application):
    current = _fresh_application(application)
    if (current.manifest != application.manifest or current.fingerprint != application.fingerprint or
            current.definition.leadership != application.definition.leadership):
        raise StartupError('CONFIG_PLAN_STALE')
    return current


def preview_config(application, *, previous_config=None, effective_from=None, allow_backfill=False):
    application = _fresh_application(application)
    with closing(DiagnosticLedger(application.directory / 'state.sqlite3')) as store:
        return build_plan(application, store, previous_config=previous_config,
                          effective_from=effective_from, allow_backfill=allow_backfill).public


def _already_applied(application, store, plan_id):
    store.validate_retirement()
    record = store.configuration_migration(plan_id)
    if record is None:
        return None
    stored_baseline(store, parse_time(store.get_meta('coverage_start')))
    snapshot = store.configuration_snapshot()
    if (snapshot is None or snapshot['revision'] != record['revision'] or
            snapshot['task_starts'] != record['task_starts'] or
            record['revision'] != manifest_digest(application.manifest) or
            record['revision'] != manifest_digest(record['manifest']) or
            store.get_meta('binding') != ledger_binding(store, application.fingerprint)):
        raise StartupError('CONFIG_PLAN_STALE')
    return {'status': 'ALREADY_APPLIED', 'plan_id': plan_id, 'revision': record['revision'],
            'backup_path': record['backup_path'], 'task_starts': record['task_starts']}


def _validate_apply(plan, plan_id, confirmations, now):
    if plan.public['plan_id'] != plan_id:
        raise StartupError('CONFIG_PLAN_STALE')
    if plan.public['blockers']:
        raise StartupError('CONFIG_MIGRATION_BLOCKED')
    if any(not confirmations.get(name, False) for name in plan.public['required_confirmations']):
        raise StartupError('CONFIG_CONFIRMATION_REQUIRED')
    _validate_time(plan, now)


def _validate_time(plan, now):
    effective = plan.public['effective_from']
    if effective is not None and not plan.public['allow_backfill'] and parse_time(effective) < now:
        raise StartupError('CONFIG_EFFECTIVE_FROM_PAST')


def _prepare_directory(directory):
    missing, parent = [], directory.absolute()
    while not parent.exists():
        missing.append(parent)
        parent = parent.parent
    for path in reversed(missing):
        path.mkdir(mode=0o700, exist_ok=True)
        descriptor = os.open(path.parent, os.O_RDONLY, 0o700)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def _backup(application, store, plan_id, backup_dir):
    directory = Path(backup_dir) if backup_dir else application.directory.parent / 'configuration-backups'
    _prepare_directory(directory)
    return store.backup_configuration(directory / f'{plan_id[:16]}-{uuid4().hex}.sqlite3')


def _commit(application, store, lease, plan, backup_dir, now):
    plan_id = plan.public['plan_id']
    application._validate_preaggregate_configuration(store, task_starts=plan.task_starts)
    backup_path = _backup(application, store, plan_id, backup_dir)
    _assert_files_unchanged(application)
    if lease is not None:
        lease.assert_owned()
    instant = now or datetime.now(timezone.utc)
    _validate_time(plan, instant)
    store.apply_configuration(expected_binding=plan.previous_binding,
        expected_revision=plan.previous_revision, manifest=plan.manifest, revision=plan.public['revision'],
        task_starts=plan.task_starts, binding=plan.binding, migration_id=plan_id,
        applied_at=instant, backup_path=backup_path,
        reset_task_cursors=plan.public['restart_cursors'])
    logger.info('CONFIGURATION_MIGRATED plan_id=%s revision=%s added=%d changed=%d removed=%d',
                plan_id, plan.public['revision'], len(plan.public['changes']['added']),
                len(plan.public['changes']['changed']), len(plan.public['changes']['removed']))
    return {'status': 'APPLIED', 'plan_id': plan_id, 'revision': plan.public['revision'],
            'backup_path': str(backup_path), 'changes': plan.public['changes'],
            'task_starts': plan.public['task_starts']}


def apply_config(application, plan_id, *, previous_config=None, effective_from=None,
                 allow_backfill=False, confirm_same_data=False, confirm_readiness_change=False,
                 accept_legacy_baseline=False, backup_dir=None, now=None):
    if not isinstance(plan_id, str) or re.fullmatch(r'[a-f0-9]{64}', plan_id) is None:
        raise StartupError('CONFIG_PLAN_STALE')
    application = _fresh_application(application)
    dependencies = application.dependencies or RuntimeDependencies()
    confirmations = {'confirm_same_data': confirm_same_data,
                     'confirm_readiness_change': confirm_readiness_change,
                     'accept_legacy_baseline': accept_legacy_baseline}
    with (
        admit_writer(application.definition, factory=dependencies.leadership_factory) as lease,
        closing(StateStore(application.directory / 'state.sqlite3')) as store,
    ):
        store.acquire_startup_lock()
        application = _assert_files_unchanged(application)
        repeated = _already_applied(application, store, plan_id)
        if repeated is not None:
            return repeated
        plan = build_plan(application, store, previous_config=previous_config,
                          effective_from=effective_from, allow_backfill=allow_backfill)
        _validate_apply(plan, plan_id, confirmations, now or datetime.now(timezone.utc))
        if lease is not None:
            lease.assert_owned()
        return _commit(application, store, lease, plan, backup_dir, now)
