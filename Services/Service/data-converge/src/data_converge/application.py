"""Application operations over one configuration snapshot and local ledger."""

from __future__ import annotations

import json
import logging
import time
import uuid
from contextlib import closing, contextmanager
from dataclasses import dataclass, replace
from datetime import datetime
from functools import cached_property
from pathlib import Path
from typing import Any

from data_converge.config.application import (
    ApplicationDefinition,
    StartupError,
    binding,
    load_app_config,
    load_definition,
    state_directory,
)
from data_converge.config.evolution import (
    build_manifest, compare_manifests, manifest_digest, validate_configuration_snapshot,
)
from data_converge.config.state_binding import ledger_binding
from data_converge.dag import ExecutionReport, TaskStatus
from data_converge.dag.builder import timeline_time
from data_converge.health import healthcheck, heartbeat_directory
from data_converge.observability.state import binding_details
from data_converge.runtime import RuntimeDependencies, open_runtime, service_heartbeat
from data_converge.state import StateStore
from data_converge.coordination import admit_writer, acquire_state
from data_converge.time_policy import system_timezone

logger = logging.getLogger(__name__)


class StopFlag:
    """Remember whether a term ended because of SIGTERM/SIGINT rather than lease loss."""

    def __init__(self):
        self._runtime = None

    def watch(self, runtime) -> None:
        self._runtime = runtime

    @property
    def requested(self) -> bool:
        scheduler = getattr(self._runtime, 'scheduler', None)
        return bool(getattr(scheduler, '_stop_requested', False))


@dataclass(frozen=True)
class CommandResult:
    """An operation's public data and exit status, independent of CLI output."""

    exit_code: int = 0
    payload: dict[str, Any] | None = None


def parse_time(value: str) -> datetime:
    try:
        instant = datetime.fromisoformat(value)
        if instant.tzinfo is None or instant.utcoffset() is None:
            raise ValueError
    except (ValueError, TypeError):
        raise StartupError('TIME_REQUIRES_ISO8601_WITH_TIMEZONE') from None
    return instant.astimezone(system_timezone())


def next_boundary(value: datetime) -> datetime:
    floor = value.replace(minute=value.minute - value.minute % 5, second=0, microsecond=0)
    return floor if floor == value else datetime.fromtimestamp(floor.timestamp() + 300, value.tzinfo)


def initialize_state(store: StateStore, fingerprint: str, start_time: datetime | None) -> None:
    existing = store.get_meta('binding')
    if existing and existing != fingerprint:
        logger.warning('STATE_BINDING_CHECK_DISABLED: issue=DC-STATE-002')
    previous = store.get_meta('coverage_start')
    if previous:
        if start_time and start_time.isoformat() != previous:
            raise StartupError('STATE_START_CANNOT_BE_CHANGED')
    else:
        start_time = start_time or next_boundary(datetime.now(system_timezone()))
        store.set_meta('coverage_start', start_time.isoformat())
    if not existing:
        store.set_meta('binding', fingerprint)


def report_result(report: ExecutionReport) -> CommandResult:
    return CommandResult(
        exit_code=int(any(item.status != TaskStatus.SUCCESS for item in report.instances.values())),
        payload={'instances': [{'id': item.instance_id, 'status': item.status.value,
                                'query_id': item.query_id}
                               for item in report.instances.values()]},
    )


class Application:
    """Use cases share validated inputs; only execution opens remote resources."""

    def __init__(self, config_path: Path, *, raw_config: dict[str, Any] | None = None,
                 dependencies: RuntimeDependencies | None = None):
        self.config_path = Path(config_path)
        self._raw_config = load_app_config(self.config_path) if raw_config is None else raw_config
        self.directory = state_directory(self._raw_config, self.config_path)
        self.dependencies = dependencies

    @cached_property
    def definition(self) -> ApplicationDefinition:
        return load_definition(self.config_path, raw_config=self._raw_config)

    @cached_property
    def fingerprint(self) -> str:
        return binding(self.definition.config, self.definition.dag)

    @cached_property
    def manifest(self) -> dict:
        return build_manifest(self.definition)

    def validate(self) -> CommandResult:
        # Validate the same deployment identity used by initialization/execution.
        self.fingerprint
        self.manifest
        return CommandResult(payload={'status': 'VALID', 'tasks': len(self.definition.dag.tasks)})

    def check_health(self, mode: str) -> CommandResult:
        directory = heartbeat_directory(self.directory, self._raw_config)
        return CommandResult(exit_code=0 if healthcheck(directory, mode) else 1)

    def status(self) -> CommandResult:
        with closing(StateStore(self.directory / 'state.sqlite3')) as store:
            store.validate_retirement()
            payload = {
                'coverage_start': store.get_meta('coverage_start'),
                'counts': store.status_counts(), 'instances': store.list_instances(limit=20),
                'dwd_windows': store.list_dwd_windows(limit=20),
                'mirror': store.mirror_status(),
            }
            recovery = store.legacy_recovery()
            if recovery is not None:
                payload.update(legacy_recovery=recovery, active_counts=store.active_status_counts(),
                               historical_counts=store.retired_status_counts())
            return CommandResult(payload=payload)

    def initialize(self, start_time: str | None = None) -> CommandResult:
        fingerprint = self.fingerprint
        self.manifest
        start = parse_time(start_time) if start_time else None
        if start and next_boundary(start) != start:
            raise StartupError('START_TIME_MUST_BE_A_FIVE_MINUTE_BOUNDARY')
        with closing(StateStore(self.directory / 'state.sqlite3', create=True)) as store:
            created = store.acquire_startup_lock()
            if created:
                self._initialize_configuration(store, fingerprint, start)
            else:
                if not store.get_meta('binding') or not store.get_meta('coverage_start'):
                    raise StartupError('STATE_NOT_INITIALIZED')
                self._validate_configuration(store)
                initialize_state(store, ledger_binding(store, fingerprint), start)
            store.mark_initialized()
            return CommandResult(payload={
                'status': 'INITIALIZED', 'coverage_start': store.get_meta('coverage_start'),
            })

    def _initialize_configuration(self, store: StateStore, fingerprint: str,
                                  start_time: datetime | None) -> None:
        store.set_meta('configuration_required', '1')
        initialize_state(store, fingerprint, start_time)
        coverage = parse_time(store.get_meta('coverage_start'))
        store.apply_configuration(
            expected_binding=fingerprint, expected_revision=None, manifest=self.manifest,
            revision=manifest_digest(self.manifest),
            task_starts={name: coverage for name in self.definition.dag.tasks},
            binding=fingerprint, migration_id=uuid.uuid4().hex,
            applied_at=datetime.now(system_timezone()),
        )

    def _validate_configuration(self, store: StateStore) -> None:
        try:
            snapshot = store.configuration_snapshot()
        except (TypeError, ValueError):
            raise StartupError('CONFIG_STATE_INVALID') from None
        store.validate_retirement()
        if snapshot is None:
            if store.get_meta('configuration_required') is not None:
                raise StartupError('CONFIG_STATE_INVALID')
            self._validate_preaggregate_configuration(store)
            return
        coverage_value = store.get_meta('coverage_start')
        coverage = parse_time(coverage_value) if coverage_value else None
        previous, previous_revision, starts = validate_configuration_snapshot(snapshot, coverage)
        self._validate_preaggregate_configuration(store, task_starts=starts)
        current_revision = manifest_digest(self.manifest)
        if current_revision != previous_revision:
            diagnostic = dict(previous_revision=previous_revision, current_revision=current_revision,
                              changes=compare_manifests(previous, self.manifest),
                              configuration_enforced=False, issue='DC-STATE-002')
            logger.warning('CONFIGURATION_CHECK_DISABLED diagnostic=%s',
                           json.dumps(diagnostic, ensure_ascii=True))

    def _validate_preaggregate_configuration(self, store: StateStore, *, task_starts=None) -> None:
        starts = store.get_task_effective_starts() if task_starts is None else task_starts
        cleanup = {name: task.cleanup_column for name, task in self.definition.dag.tasks.items()}
        try:
            store.validate_preaggregate_configuration(
                self.manifest, starts, store.get_meta('coverage_start'),
                timezone=self.definition.options.timezone, cleanup_columns=cleanup)
        except ValueError as exc:
            raise StartupError(str(exc)) from None

    @contextmanager
    def _active_state(self, *, bootstrap: bool = False):
        from data_converge.configuration_migration.legacy_recovery import recover_legacy, log_retained_history

        fingerprint = self.fingerprint
        self.manifest
        dependencies = self.dependencies or RuntimeDependencies()
        with (
            admit_writer(self.definition, wait=bootstrap, factory=dependencies.leadership_factory) as lease,
            closing(StateStore(self.directory / 'state.sqlite3', create=bootstrap)) as store,
        ):
            created = acquire_state(store, bootstrap=bootstrap, lease=lease)
            if created:
                self._initialize_configuration(store, fingerprint, None)
            elif bootstrap:
                recover_legacy(self, store, lease)
            self._validate_state_binding(store, ledger_binding(store, fingerprint))
            store.mark_initialized()
            if lease is not None:
                lease.assert_owned()
                if bootstrap:
                    logger.warning('LEADERSHIP_LEDGER_CHECK_DISABLED: issue=DC-STATE-001')
            if created:
                logger.info('STATE_INITIALIZED: coverage_start=%s', store.get_meta('coverage_start'))
            store.recover_running()
            log_retained_history(store)
            with self._shared_state(store, lease, dependencies):
                yield store, lease

    def _shared_state(self, store, lease, dependencies):
        """Fenced Redis state for this leadership term (no-op without leadership)."""
        from data_converge.state.shared import attach_shared_state

        return attach_shared_state(self.definition, store, lease,
                                   connection_factory=dependencies.redis_connection_factory,
                                   killer=dependencies.query_killer(self.definition))

    def _validate_state_binding(self, store: StateStore, fingerprint: str) -> None:
        existing = store.get_meta('binding')
        coverage = store.get_meta('coverage_start')
        evidence = binding_details(
            config_path=self.config_path, state_path=store.path,
            task_count=len(self.definition.dag.tasks), current=fingerprint,
            stored=existing, coverage=coverage,
        )
        evidence.update(binding_enforced=False, configuration_enforced=False, issue='DC-STATE-002')
        # Escape paths and control characters; never serialize arbitrary metadata.
        diagnostic = json.dumps(evidence, ensure_ascii=True)
        try:
            self._validate_configuration(store)
        except StartupError:
            logger.error('STATE_BINDING_REJECTED diagnostic=%s', diagnostic)
            raise
        if not existing or not coverage:
            logger.error('STATE_BINDING_REJECTED diagnostic=%s', diagnostic)
            raise StartupError('STATE_DEPLOYMENT_BINDING_CHANGED_OR_UNINITIALIZED')
        parse_time(coverage)
        if existing != fingerprint:
            logger.warning('STATE_BINDING_CHECK_DISABLED diagnostic=%s', diagnostic)
        else:
            logger.info('STATE_BINDING_OBSERVED diagnostic=%s', diagnostic)

    def resolve(self, instance_id: str, resolution: str, note: str) -> CommandResult:
        with self._active_state() as (store, lease):
            if lease is not None:
                lease.assert_owned()
            store.resolve(instance_id, 'SUCCESS' if resolution == 'success' else 'PENDING', note)
        return CommandResult(payload={'status': 'RESOLVED', 'instance_id': instance_id})

    def resolve_mirror_conflict(self, instance_id: str, disposition: str,
                                note: str, actor: str = 'operator') -> CommandResult:
        with self._active_state() as (store, lease):
            if lease is not None:
                lease.assert_owned()
            store.resolve_mirror_conflict(instance_id, disposition.upper(), note, actor)
        return CommandResult(payload={'status': disposition.upper(), 'instance_id': instance_id,
                                      'disposition_by': actor})

    def resolve_preaggregate(self, item_id: str, resolution: str, note: str) -> CommandResult:
        with self._active_state() as (store, lease):
            if lease is not None:
                lease.assert_owned()
            store.resolve_preaggregate(item_id, resolution, note)
        return CommandResult(payload={'status': 'RESOLVED', 'item_id': item_id,
                                      'execution_kind': 'PREAGGREGATE', 'resolution': resolution})

    def _runtime(self, store: StateStore, lease=None, heartbeat=None):
        optional = {} if self.dependencies is None else {'dependencies': self.dependencies}
        if lease is not None:
            optional['lease'] = lease
        if heartbeat is not None:
            optional['heartbeat'] = heartbeat
        definition = replace(self.definition, sql_hashes={
            name: task['sql_sha256'] for name, task in self.manifest['tasks'].items()})
        return open_runtime(self.config_path, definition, store, self.directory,
                            parse_time(store.get_meta('coverage_start')), **optional)

    def run_task(self, taskname: str, scheduled_time: str) -> CommandResult:
        with self._active_state() as (store, lease):
            scheduled = parse_time(scheduled_time)
            candidate = self.definition.builder.create_instance(self.definition.dag, taskname, scheduled)
            if timeline_time(candidate.data_start) < timeline_time(parse_time(store.get_meta('coverage_start'))):
                raise StartupError('WINDOW_PRECEDES_COVERAGE_START')
            task_start = store.get_task_effective_starts().get(taskname)
            if task_start is not None and timeline_time(candidate.data_start) < timeline_time(task_start):
                raise StartupError('TASK_WINDOW_BEFORE_EFFECTIVE_START')
            reason = store.preaggregate_formal_blocker(candidate.instance_id, datetime.now(system_timezone()))
            if reason:
                raise StartupError(reason)
            with self._runtime(store, lease) as runtime:
                return report_result(runtime.run_task(taskname, scheduled))

    def run_once(self, now: str | None = None) -> CommandResult:
        with self._active_state() as (store, lease):
            requested = parse_time(now) if now else None
            if requested is not None:
                reason = store.preaggregate_schedule_blocker(requested, datetime.now(system_timezone()))
                if reason:
                    raise StartupError(reason)
            with self._runtime(store, lease) as runtime:
                return report_result(runtime.run_once(requested))

    def serve(self) -> CommandResult:
        logger.info('SYSTEM_TIMEZONE_SELECTED: timezone=%s', self.definition.options.timezone)
        dependencies = self.dependencies or RuntimeDependencies()
        with service_heartbeat(self.definition, self.directory, dependencies) as heartbeat:
            while True:
                try:
                    stopped, lease_lost = self._serve_term(heartbeat, dependencies)
                except Exception as error:
                    if not self._rejoin_after(error):
                        raise
                    continue
                if stopped or not self.definition.leadership.enabled or not lease_lost:
                    return CommandResult()
                # The term ended without an error: the lease expired while idle.
                logger.warning('LEADERSHIP_TERM_ENDED: reason=LEADERSHIP_LOST; rejoining as standby')
                time.sleep(self.definition.leadership.standby_poll_seconds)

    def _serve_term(self, heartbeat, dependencies) -> tuple[bool, bool]:
        """Serve one term; report operator stop and whether the lease was lost."""
        stop = StopFlag()
        with (
            self._active_state(bootstrap=True) as (store, lease),
            self._runtime(store, lease, heartbeat) as runtime,
        ):
            stop.watch(runtime)
            runtime.serve()
            lease_lost = lease is None or lease.lost
        if heartbeat is not None:
            heartbeat.set_ready(False)
        return stop.requested, lease_lost

    def _rejoin_after(self, error: BaseException) -> bool:
        """Return to standby after a recoverable coordination failure.

        Every context above has exited: in-flight SQL drained, the Redis lease
        released (or left to expire), and the local file lock closed, so the
        next term starts from a clean admission exactly like a fresh process.
        """
        from data_converge.coordination.terms import is_term_end, reason

        if not self.definition.leadership.enabled or not is_term_end(error):
            return False
        logger.warning('LEADERSHIP_TERM_ENDED: reason=%s; released local state, rejoining as standby',
                       reason(error))
        time.sleep(self.definition.leadership.standby_poll_seconds)
        return True
