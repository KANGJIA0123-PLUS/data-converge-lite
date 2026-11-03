"""Load one application snapshot and validate its task graph without side effects."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import yaml

from data_converge.dag import DAG, DAGBuilder
from data_converge.dwd_ready.config import load_ready_config
from data_converge.sql_execution.config import connection_origin, validate_config
from data_converge.archives.settings import ArchiveSettings
from data_converge.infrastructure.redis.pools import RedisPools
from data_converge.state.redis_mirror import RedisStateSettings
from data_converge.sql_execution.pool import ClickHousePool

from .loader import ConfigLoader
from .cron import compile_cron_groups
from .leadership import LeadershipSettings
from .startup import StartupSettings
from data_converge.time_policy import system_timezone_name

RESOURCES = Path(__file__).resolve().parents[3] / 'resources'
DEFAULT_APP_CONFIG_PATH = RESOURCES / 'config/app_config.yaml'


class StartupError(ValueError):
    """An intentionally public, credential-free startup diagnostic."""


PERIODS = ('PT5M', 'PT1H', 'P1D', 'P1M')
PERIOD_NAMES = {'5min': 'PT5M', 'hour': 'PT1H', 'day': 'P1D', 'month': 'P1M'}
# Design table: default and inclusive range per aggregation period.
TIMEOUT_DEFAULTS = {'PT5M': 120, 'PT1H': 600, 'P1D': 1800, 'P1M': 1800}
TIMEOUT_RANGES = {'PT5M': (10, 300), 'PT1H': (10, 1800), 'P1D': (10, 3600), 'P1M': (10, 3600)}
CROSS_WAIT_DEFAULTS = {'PT1H': 300, 'P1D': 300, 'P1M': 300}


def _period_mapping(values, defaults, name):
    """Accept {5min|hour|day|month: seconds}; omitted periods keep defaults."""
    if values is None:
        return dict(defaults)
    # dataclasses.replace() revalidates already normalized period keys.
    names = {**PERIOD_NAMES, **{period: period for period in PERIODS}}
    if not isinstance(values, dict) or set(values) - set(names):
        raise StartupError('SCHEDULER_SETTING_INVALID')
    result = dict(defaults)
    for key, value in values.items():
        period = names[key]
        if period not in defaults:
            raise StartupError(name)
        result[period] = value
    return result


def _is_int(value) -> bool:
    return type(value) is int


@dataclass(frozen=True)
class SchedulerSettings:
    """Validated scheduling policy; YAML dictionaries stop at this boundary."""

    timezone: str = field(default_factory=system_timezone_name, init=False)
    max_concurrent_tasks: int = 4
    task_parallelism: int = 1
    poll_interval_seconds: int = 5
    catchup_limit: int = 100
    max_attempts: int = 3
    retry_delay_seconds: int = 30
    task_timeout_seconds: dict | None = None
    cross_period_wait_enabled: bool = True
    cross_period_wait_seconds: dict | int | None = None
    cross_period_wait_overrides: dict[str, int] = field(default_factory=dict)
    skip_batches: bool = False
    skip_mode: str = 'current_only'
    skip_threshold_batches: dict | None = None
    skip_max_resume_days: int = 31
    default_timeout_seconds: int | None = None

    def __post_init__(self) -> None:
        for name in ('max_concurrent_tasks', 'poll_interval_seconds', 'catchup_limit',
                     'max_attempts', 'retry_delay_seconds'):
            value = getattr(self, name)
            if not _is_int(value) or value <= 0:
                raise StartupError('SCHEDULER_SETTING_INVALID')
        if not _is_int(self.task_parallelism) or not 1 <= self.task_parallelism <= 3:
            raise StartupError('TASK_PARALLELISM_OUT_OF_RANGE')
        self._normalize_timeouts()
        self._normalize_cross_period_wait()
        self._validate_skip()

    def _normalize_timeouts(self) -> None:
        defaults = dict(TIMEOUT_DEFAULTS)
        if self.default_timeout_seconds is not None and self.task_timeout_seconds is None:
            # Legacy single value: applies to every period unless overridden.
            if not _is_int(self.default_timeout_seconds) or self.default_timeout_seconds <= 0:
                raise StartupError('SCHEDULER_SETTING_INVALID')
            defaults = dict.fromkeys(PERIODS, self.default_timeout_seconds)
        values = _period_mapping(self.task_timeout_seconds, defaults, 'SCHEDULER_SETTING_INVALID')
        for period, value in values.items():
            low, high = TIMEOUT_RANGES[period]
            if not _is_int(value) or not low <= value <= high:
                raise StartupError('TASK_TIMEOUT_OUT_OF_RANGE')
        object.__setattr__(self, 'task_timeout_seconds', values)

    def _normalize_cross_period_wait(self) -> None:
        raw = self.cross_period_wait_seconds
        values = (dict.fromkeys(CROSS_WAIT_DEFAULTS, raw) if _is_int(raw)
                  else _period_mapping(raw, CROSS_WAIT_DEFAULTS, 'CROSS_PERIOD_WAIT_PERIOD_INVALID'))
        overrides = self.cross_period_wait_overrides
        if not isinstance(self.cross_period_wait_enabled, bool) or not isinstance(overrides, dict):
            raise StartupError('SCHEDULER_SETTING_INVALID')
        if any(not isinstance(name, str) or not name for name in overrides):
            raise StartupError('SCHEDULER_SETTING_INVALID')
        for value in (*values.values(), *overrides.values()):
            if not _is_int(value) or value < 0:
                raise StartupError('SCHEDULER_SETTING_INVALID')
        object.__setattr__(self, 'cross_period_wait_seconds', values)

    def _validate_skip(self) -> None:
        if not isinstance(self.skip_batches, bool):
            raise StartupError('SCHEDULER_SETTING_INVALID')
        if self.skip_mode not in ('current_only', 'current_then_resume'):
            raise StartupError('SKIP_MODE_INVALID')
        thresholds = _period_mapping(self.skip_threshold_batches,
                                     {'PT5M': 12, 'PT1H': 3, 'P1D': 2, 'P1M': 2},
                                     'SCHEDULER_SETTING_INVALID')
        if any(not _is_int(value) or value < 1 for value in thresholds.values()):
            raise StartupError('SCHEDULER_SETTING_INVALID')
        object.__setattr__(self, 'skip_threshold_batches', thresholds)
        if not _is_int(self.skip_max_resume_days) or not 1 <= self.skip_max_resume_days <= 31:
            raise StartupError('SCHEDULER_SETTING_INVALID')

    def timeout_for(self, interval: str) -> int:
        return self.task_timeout_seconds[interval]

    def cross_period_wait_for(self, interval: str) -> int:
        return self.cross_period_wait_seconds.get(interval, 0)


@dataclass(frozen=True)
class ApplicationDefinition:
    """Validated inputs shared by deployment binding and runtime construction."""

    config: dict[str, Any] = field(repr=False)
    options: SchedulerSettings
    builder: DAGBuilder
    dag: DAG
    sql_dir: Path
    leadership: LeadershipSettings = field(default_factory=LeadershipSettings)
    sql_hashes: dict[str, str] | None = field(default=None, repr=False)


def load_app_config(path: Path) -> dict[str, Any]:
    try:
        value = yaml.safe_load(Path(path).read_text(encoding='utf-8'))
        if not isinstance(value, dict):
            raise ValueError
        return value
    except (OSError, ValueError, yaml.YAMLError):
        raise StartupError('CONFIG_UNREADABLE') from None


def resolve_path(value: str, base: Path) -> Path:
    if not isinstance(value, str) or not value:
        raise StartupError('CONFIG_PATH_INVALID')
    value = os.path.expandvars(value)
    if '$' in value:
        raise StartupError('CONFIG_PATH_ENV_MISSING')
    path = Path(value).expanduser()
    return path if path.is_absolute() else Path(base) / path


def state_directory(config: dict[str, Any], config_path: Path) -> Path:
    return resolve_path(config.get('state', {}).get('directory', '/srv/its/MindOpsBOSE/data-converge/data'),
                        Path(config_path).parent)


def asset_path(value: str | None, fallback: str, config_path: Path) -> Path:
    if value is None:
        return RESOURCES / fallback
    # The company bytecode build copies resources alongside src.
    if isinstance(value, str) and value.startswith('resources/'):
        return RESOURCES / value[len('resources/'):]
    return resolve_path(value, Path(config_path).parent)


def settings(config: dict[str, Any]) -> SchedulerSettings:
    values = config.get('scheduler', {})
    if not isinstance(values, dict):
        raise StartupError('SCHEDULER_SETTING_INVALID')
    return SchedulerSettings(**{
        option.name: values[option.name]
        for option in fields(SchedulerSettings) if option.init and option.name in values
    })


def load_definition(config_path: Path, *, raw_config: dict[str, Any] | None = None) -> ApplicationDefinition:
    raw_config = load_app_config(config_path) if raw_config is None else raw_config
    config = validate_config(raw_config, config_path, resolve_password=False)
    StartupSettings.from_mapping(config.get('startup'))
    ArchiveSettings.from_mapping(config.get('state', {}).get('archives'))
    try:
        RedisPools.from_mapping(config.get('redis_pools'))
        RedisStateSettings.from_mapping(config.get('redis_state'))
        ClickHousePool.from_mapping(config.get('clickhouse_pool'))
    except ValueError as error:
        raise StartupError(str(error)) from None
    config['dwd_ready'] = load_ready_config(config, Path(config_path).parent)
    options = settings(config)
    section = config.get('config', {})
    rules = asset_path(section.get('convergence_rules_path'), 'config/convergence_rules.yaml', config_path)
    sql_dir = asset_path(section.get('sql_directory'), 'sql', config_path)
    builder = DAGBuilder(ConfigLoader(rules, sql_dir=sql_dir))
    dag = builder.build()
    if options.cross_period_wait_overrides.keys() - dag.tasks.keys():
        raise StartupError('CROSS_PERIOD_WAIT_TASK_UNKNOWN')
    if not dag.tasks:
        raise StartupError('NO_TASKS_CONFIGURED')
    if config['dwd_ready']['enabled'] and any(
            not task.depends_on and task.aggregation_interval != 'PT5M' for task in dag.tasks.values()):
        raise StartupError('DWD_READY_REQUIRES_FIVE_MINUTE_ROOT_TASKS')
    anchor = datetime(2000, 3, 1, tzinfo=ZoneInfo(options.timezone))
    for taskname in dag.topological_order:
        builder.create_instance(dag, taskname, anchor)
    compile_cron_groups(dag, options.timezone)
    return ApplicationDefinition(config, options, builder, dag, sql_dir,
                                 LeadershipSettings.from_config(config))


def binding(config: dict[str, Any], dag: DAG) -> str:
    """Keep the existing ledger identity stable across internal refactoring."""
    value = {'endpoint': connection_origin(config['clickhouse']),
             'database': config['clickhouse']['database'],
             'username': config['clickhouse']['username'],
             'tasks': [{key: item for key, item in asdict(dag.tasks[name]).items()
                        if key != 'description'} for name in sorted(dag.tasks)]}
    ready = config['dwd_ready']
    value['dwd_ready'] = {'enabled': ready['enabled']}
    if ready['enabled']:
        redis_fields = ('host', 'port', 'mode', 'db')
        if ready['redis']['mode'] == 'sentinel':
            redis_fields = ('mode', 'db', 'service_name', 'username', 'paas_name', 'password_file')
        value['dwd_ready'].update(queue_key=ready['queue_key'], redis={
            key: ready['redis'].get(key) for key in redis_fields})
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()
