"""Independent local checks; failure in one component does not hide the others."""
import json
import importlib.util
import math
import os
from pathlib import Path
import time
import sys

from data_converge.config.application import asset_path, binding, load_definition
from data_converge.errors import safe_error_details
from data_converge.observability.logging import log_file_path
from data_converge.observability.settings import LoggingSettings
from .report import LAUNCHER, check
from .windows import parse_window


def failure(identifier, error, summary, action, **evidence):
    return check(identifier, 'FAIL', summary, {**evidence, **safe_error_details(error)}, action)


def protected_check(identifier, callback, *args):
    try:
        return callback(*args)
    except Exception as error:
        return failure(identifier, error, '该检查未完成', '根据错误码检查此组件配置、文件和权限。')


def logging_settings_check(raw):
    settings = LoggingSettings.from_mapping(raw.get('logging'))
    return check('logging.settings', 'PASS', '日志轮转与老化配置有效',
                 dict(max_bytes=settings.max_bytes, backup_count=settings.backup_count,
                      retention_days=settings.retention_days))


def definition_check(path, raw):
    try:
        definition = load_definition(path, raw_config=raw)
        return definition, check('definition', 'PASS', '配置、SQL 文件、依赖关系和调度规则有效',
                                 {'task_count': len(definition.dag.tasks)})
    except Exception as error:
        section = raw.get('config', {})
        paths = {'config': str(path)}
        if isinstance(section, dict):
            for field, default in (('convergence_rules_path', 'config/convergence_rules.yaml'),
                                   ('sql_directory', 'sql')):
                try:
                    paths[field] = str(asset_path(section.get(field), default, path))
                except Exception:
                    pass
        return None, failure('definition', error, '业务配置或 SQL 文件检查失败',
                             '核对证据中的文件路径、挂载和权限，再运行 --validate-only。', **paths)


def state_check(store, definition):
    coverage = store.get_meta('coverage_start')
    counts = store.status_counts()
    evidence = dict(path=str(store.path), coverage_start=coverage, counts=counts, **store.progress())
    if not coverage or not store.get_meta('binding'):
        return check('state', 'FAIL', '状态库未完成初始化', evidence,
                     '核对部署与原状态目录；不要删除已有状态库来绕过初始化检查。')
    try:
        parse_window(coverage)
    except (ValueError, TypeError):
        return check('state', 'FAIL', '状态库覆盖起点无效', evidence,
                     '核对原账本与部署版本；不要重置覆盖起点来绕过检查。')
    if definition is not None and binding(definition.config, definition.dag) != store.get_meta('binding'):
        return check('state', 'FAIL', '当前配置与状态库绑定不一致', evidence,
                     '核对数据库、账号、任务配置和状态卷是否属于同一部署；不要重建状态库。')
    return check('state', 'PASS', '状态库可只读查询', evidence)


def task_check(counts):
    failed = counts.get('UNKNOWN', 0) + counts.get('FAILED', 0)
    blocked = counts.get('BLOCKED', 0)
    status = 'FAIL' if failed else 'WARN' if blocked else 'PASS'
    return check('tasks', status, '任务历史状态统计', counts,
                 f'{LAUNCHER} --failures' if failed or blocked else '')


def heartbeat_check(directory):
    path = directory / 'heartbeat.json'
    try:
        with path.open(encoding='utf-8') as stream:
            content = stream.read(65537)
        if len(content) > 65536:
            raise ValueError('HEARTBEAT_INVALID')
        value = json.loads(content)
        pid, stamp = value['pid'], value['time']
        if type(pid) is not int or pid <= 0 or type(stamp) not in (int, float) or not math.isfinite(stamp):
            raise ValueError('HEARTBEAT_INVALID')
        age = time.time() - stamp
        os.kill(pid, 0)
        evidence = dict(path=str(path), pid=pid, age_seconds=round(age, 1), ready=value.get('ready') is True)
        status = 'PASS' if 0 <= age <= 75 and evidence['ready'] else 'FAIL'
        return check('heartbeat', status, '进程心跳与启动就绪状态', evidence,
                     '心跳不证明调度线程或远端连接正常；结合任务进展及 --diagnose --online 检查。')
    except Exception as error:
        return failure('heartbeat', error, '进程心跳缺失、无效或进程不存在',
                       '核对服务进程、状态目录挂载和 data-converge.log 启动错误。', path=str(path))


def file_check(identifier, path, *, directory=False):
    path = Path(path)
    exists = path.is_dir() if directory else path.is_file()
    readable = exists and os.access(path, os.R_OK | (os.X_OK if directory else 0))
    return check(identifier, 'PASS' if readable else 'FAIL', '文件可读性检查',
                 dict(path=str(path), exists=exists, readable=readable),
                 '' if readable else '核对挂载路径、运行账号和读取权限；不要输出密钥内容。')


def credential_files(definition):
    config = definition.config
    files = []
    ch = config['clickhouse']
    if ch.get('password_file'):
        files.append(protected_check('clickhouse.password_file', file_check,
                                     'clickhouse.password_file', ch['password_file']))
    if ch.get('secure') and os.environ.get('SSL_CERT_FILE'):
        files.append(protected_check('clickhouse.ca_file', file_check,
                                     'clickhouse.ca_file', os.environ['SSL_CERT_FILE']))
    ready = config['dwd_ready']
    if ready['enabled'] and ready['redis']['mode'] == 'sentinel':
        for name in ('password_file', 'root_key_file', 'work_key_file'):
            files.append(protected_check('redis.' + name, file_check, 'redis.' + name, ready['redis'][name]))
    return files


def module_check(definition):
    config = definition.config
    required = set()
    if config['clickhouse'].get('password_file'):
        required.add('dsf')
    ready = config['dwd_ready']
    if ready['enabled']:
        required.add('redis')
        if ready['redis']['mode'] == 'sentinel':
            required.update(('dsf', 'psf', 'Crypto'))
    missing = sorted(name for name in required if importlib.util.find_spec(name) is None)
    return check('runtime.modules', 'FAIL' if missing else 'PASS', '解释器与所需模块可发现性',
                 dict(python=sys.executable, version=sys.version.split()[0], required=sorted(required), missing=missing),
                 '通过 start_data_converge.sh 加载平台 python.env 和 PYTHONPATH；模块可发现不代表解密已验证。')


def log_check():
    path = log_file_path()
    if path is None:
        return check('logs', 'WARN', '未配置业务日志文件路径', {},
                     '在服务启动环境中设置 LOG_PATH 或 SERVICE_LOG_PATH。')
    ancestor = path.parent
    while not ancestor.exists() and ancestor != ancestor.parent:
        ancestor = ancestor.parent
    writable = ((path.is_file() and os.access(path, os.W_OK)) if path.exists()
                else ancestor.is_dir() and os.access(ancestor, os.W_OK | os.X_OK))
    return check('logs', 'PASS' if writable else 'FAIL', '业务日志位置与写入权限（不创建文件）',
                 dict(path=str(path), exists=path.is_file(), writable=writable),
                 '' if writable else '检查日志卷挂载及服务账号写权限。')
