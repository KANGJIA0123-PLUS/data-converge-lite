"""Load one application snapshot and validate its task graph without side effects."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import yaml

from data_converge.dag import DAG, DAGBuilder, DAGExecutor
from data_converge.dwd_ready.config import load_ready_config
from data_converge.scheduler import TaskScheduler
from data_converge.sql_execution.client import connection_origin, validate_config

from .loader import ConfigLoader

RESOURCES = Path(__file__).resolve().parents[3] / 'resources'
DEFAULT_APP_CONFIG_PATH = RESOURCES / 'config/app_config.yaml'


class StartupError(ValueError):
    """An intentionally public, credential-free startup diagnostic."""


@dataclass(frozen=True)
class ApplicationDefinition:
    """Validated inputs shared by deployment binding and runtime construction."""

    config: dict[str, Any] = field(repr=False)
    options: dict[str, Any]
    builder: DAGBuilder
    dag: DAG
    sql_dir: Path


def load_app_config(path: Path) -> dict[str, Any]:
    try:
        value = yaml.safe_load(Path(path).read_text(encoding='utf-8'))
        if not isinstance(value, dict):
            raise ValueError
        return value
    except (OSError, UnicodeError, ValueError, yaml.YAMLError):
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


def settings(config: dict[str, Any]) -> dict[str, Any]:
    values = {'timezone': 'Asia/Shanghai', 'default_timeout_seconds': 300,
              'max_concurrent_tasks': 4, 'poll_interval_seconds': 5,
              'catchup_limit': 100, 'max_attempts': 3, 'retry_delay_seconds': 30}
    values.update(config.get('scheduler', {}))
    if values['timezone'] != 'Asia/Shanghai':
        raise StartupError('TIMEZONE_MUST_BE_ASIA_SHANGHAI')
    for key in ('default_timeout_seconds', 'max_concurrent_tasks', 'poll_interval_seconds',
                'catchup_limit', 'max_attempts', 'retry_delay_seconds'):
        if type(values[key]) is not int or values[key] <= 0:
            raise StartupError('SCHEDULER_SETTING_INVALID')
    if values['default_timeout_seconds'] > 300:
        raise StartupError('SQL_TIMEOUT_MUST_NOT_EXCEED_300_SECONDS')
    return values


def load_definition(config_path: Path, *, raw_config: dict[str, Any] | None = None) -> ApplicationDefinition:
    raw_config = load_app_config(config_path) if raw_config is None else raw_config
    config = validate_config(raw_config, config_path, resolve_password=False)
    config['dwd_ready'] = load_ready_config(config, Path(config_path).parent)
    options = settings(config)
    section = config.get('config', {})
    rules = asset_path(section.get('convergence_rules_path'), 'config/convergence_rules.yaml', config_path)
    sql_dir = asset_path(section.get('sql_directory'), 'sql', config_path)
    builder = DAGBuilder(ConfigLoader(rules, sql_dir=sql_dir))
    dag = builder.build()
    if not dag.tasks:
        raise StartupError('NO_TASKS_CONFIGURED')
    if config['dwd_ready']['enabled'] and any(
            not task.depends_on and task.aggregation_interval != 'PT5M' for task in dag.tasks.values()):
        raise StartupError('DWD_READY_REQUIRES_FIVE_MINUTE_ROOT_TASKS')
    anchor = datetime(2000, 3, 1, tzinfo=ZoneInfo(options['timezone']))
    for taskname in dag.topological_order:
        builder.create_instance(dag, taskname, anchor)
    preview = TaskScheduler(dag, builder, DAGExecutor(dag, runner=lambda _: None),
                            timezone=options['timezone'])
    preview.register_jobs()
    return ApplicationDefinition(config, options, builder, dag, sql_dir)


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
        fields = ('host', 'port', 'mode', 'db')
        if ready['redis']['mode'] == 'sentinel':
            fields = ('mode', 'db', 'service_name', 'username', 'paas_name', 'password_file')
        value['dwd_ready'].update(queue_key=ready['queue_key'], redis={
            key: ready['redis'].get(key) for key in fields})
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()
