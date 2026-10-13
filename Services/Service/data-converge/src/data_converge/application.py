"""Application operations over one configuration snapshot and local ledger."""

from __future__ import annotations

from contextlib import closing, contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import cached_property
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from data_converge.config.application import (
    ApplicationDefinition,
    StartupError,
    binding,
    load_app_config,
    load_definition,
    state_directory,
)
from data_converge.dag import ExecutionReport, TaskStatus
from data_converge.health import healthcheck
from data_converge.runtime import open_runtime
from data_converge.state import StateStore


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
        return instant.astimezone(ZoneInfo('Asia/Shanghai'))
    except (ValueError, TypeError):
        raise StartupError('TIME_REQUIRES_ISO8601_WITH_TIMEZONE') from None


def next_boundary(value: datetime) -> datetime:
    floor = value.replace(minute=value.minute - value.minute % 5, second=0, microsecond=0)
    return floor if floor == value else floor + timedelta(minutes=5)


def initialize_state(store: StateStore, fingerprint: str, start_time: datetime | None) -> None:
    existing = store.get_meta('binding')
    if existing and existing != fingerprint:
        raise StartupError('STATE_DEPLOYMENT_BINDING_CHANGED')
    previous = store.get_meta('coverage_start')
    if previous:
        if start_time and start_time.isoformat() != previous:
            raise StartupError('STATE_START_CANNOT_BE_CHANGED')
    else:
        start_time = start_time or next_boundary(datetime.now(ZoneInfo('Asia/Shanghai')))
        store.set_meta('coverage_start', start_time.isoformat())
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

    def __init__(self, config_path: Path, *, raw_config: dict[str, Any] | None = None):
        self.config_path = Path(config_path)
        self._raw_config = load_app_config(self.config_path) if raw_config is None else raw_config
        self.directory = state_directory(self._raw_config, self.config_path)

    @cached_property
    def definition(self) -> ApplicationDefinition:
        return load_definition(self.config_path, raw_config=self._raw_config)

    @cached_property
    def fingerprint(self) -> str:
        return binding(self.definition.config, self.definition.dag)

    def validate(self) -> CommandResult:
        # Validate the same deployment identity used by initialization/execution.
        self.fingerprint
        return CommandResult(payload={'status': 'VALID', 'tasks': len(self.definition.dag.tasks)})

    def check_health(self, mode: str) -> CommandResult:
        return CommandResult(exit_code=0 if healthcheck(self.directory, mode) else 1)

    def status(self) -> CommandResult:
        with closing(StateStore(self.directory / 'state.sqlite3')) as store:
            return CommandResult(payload={
                'coverage_start': store.get_meta('coverage_start'),
                'counts': store.status_counts(), 'instances': store.list_instances(limit=20),
                'dwd_windows': store.list_dwd_windows(limit=20),
            })

    def initialize(self, start_time: str | None = None) -> CommandResult:
        fingerprint = self.fingerprint
        start = parse_time(start_time) if start_time else None
        if start and next_boundary(start) != start:
            raise StartupError('START_TIME_MUST_BE_A_FIVE_MINUTE_BOUNDARY')
        with closing(StateStore(self.directory / 'state.sqlite3', create=True)) as store:
            store.acquire_lock()
            initialize_state(store, fingerprint, start)
            return CommandResult(payload={
                'status': 'INITIALIZED', 'coverage_start': store.get_meta('coverage_start'),
            })

    @contextmanager
    def _active_state(self):
        fingerprint = self.fingerprint
        with closing(StateStore(self.directory / 'state.sqlite3')) as store:
            store.acquire_lock()
            if store.get_meta('binding') != fingerprint or not store.get_meta('coverage_start'):
                raise StartupError('STATE_DEPLOYMENT_BINDING_CHANGED_OR_UNINITIALIZED')
            store.recover_running()
            yield store

    def resolve(self, instance_id: str, resolution: str, note: str) -> CommandResult:
        with self._active_state() as store:
            store.resolve(instance_id, 'SUCCESS' if resolution == 'success' else 'PENDING', note)
        return CommandResult(payload={'status': 'RESOLVED', 'instance_id': instance_id})

    def _runtime(self, store: StateStore):
        return open_runtime(self.config_path, self.definition, store, self.directory,
                            parse_time(store.get_meta('coverage_start')))

    def run_task(self, taskname: str, scheduled_time: str) -> CommandResult:
        with self._active_state() as store:
            scheduled = parse_time(scheduled_time)
            candidate = self.definition.builder.create_instance(self.definition.dag, taskname, scheduled)
            if candidate.data_start < parse_time(store.get_meta('coverage_start')):
                raise StartupError('WINDOW_PRECEDES_COVERAGE_START')
            with self._runtime(store) as runtime:
                return report_result(runtime.run_task(taskname, scheduled))

    def run_once(self, now: str | None = None) -> CommandResult:
        with self._active_state() as store, self._runtime(store) as runtime:
            return report_result(runtime.run_once(parse_time(now) if now else None))

    def serve(self) -> CommandResult:
        with self._active_state() as store, self._runtime(store) as runtime:
            runtime.serve()
        return CommandResult()
