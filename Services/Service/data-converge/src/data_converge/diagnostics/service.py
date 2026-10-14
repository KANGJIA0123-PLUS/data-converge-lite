"""Compose diagnostic use cases without entering service startup or execution."""
from contextlib import closing
from datetime import datetime

from data_converge.config.application import binding, load_app_config, state_directory
from data_converge.dwd_ready.config import load_ready_config
from .checks import (credential_files, definition_check, failure, heartbeat_check,
                     log_check, logging_settings_check, module_check, protected_check,
                     state_check, task_check)
from .ledger import DiagnosticLedger
from .report import check, evidence_action, report, task_record
from .windows import list_windows, parse_window


def _read_context(path):
    raw = load_app_config(path)
    return raw, state_directory(raw, path)


def diagnose(path, *, online=False):
    checks = []
    try:
        raw, directory = _read_context(path)
    except Exception as error:
        checks.append(failure('config', error, '应用配置不可用',
                              '检查 --config 指定的文件是否存在、可读且为有效 YAML。', path=str(path)))
        return report('diagnose', checks)
    checks.append(check('config', 'PASS', '应用配置可读取', {'path': str(path)}))
    definition, result = definition_check(path, raw)
    checks.append(result)
    if definition is not None:
        checks.append(protected_check('runtime.modules', module_check, definition))
        checks.extend(credential_files(definition))
    checks.extend([protected_check('logging.settings', logging_settings_check, raw),
                   protected_check('logs', log_check), heartbeat_check(directory)])
    checks.extend(_ledger_checks(directory, definition))
    if online and definition is not None:
        from .connections import check_connections
        checks.extend(check_connections(definition))
    else:
        checks.append(check('connections', 'SKIP', '未检查在线连接', {},
                            '配置有效后运行 --diagnose --online，执行只读连接检查。'))
    return report('diagnose', checks)


def _ledger_checks(directory, definition):
    results = []
    try:
        with closing(DiagnosticLedger(directory / 'state.sqlite3')) as store:
            results.append(state_check(store, definition))
            results.append(task_check(store.status_counts()))
            if definition is not None and definition.config['dwd_ready']['enabled']:
                windows = list_windows(store, definition.config['dwd_ready'])
                late = [row for row in windows if row['warning_due']]
                results.append(check('dwd', 'WARN' if late else 'PASS', '最近 20 个完整窗口就绪检查',
                    {'checked_windows': len(windows), 'late_windows': len(late), 'latest': windows[:1]},
                    '运行 --dwd-status 查看缺失类型；旧窗口可用 --window 精确查询。'))
    except Exception as error:
        results.append(failure('state', error, '状态库不可读取或结构无效',
            '检查状态卷、文件权限和原状态库；本命令不会初始化、恢复或删除状态。',
            path=str(directory / 'state.sqlite3')))
    return results


def failures(path, *, task=None, limit=20, offset=0):
    try:
        _, directory = _read_context(path)
        with closing(DiagnosticLedger(directory / 'state.sqlite3')) as store:
            counts, rows = store.problems(task=task, limit=limit, offset=offset)
        total = sum(counts.values())
        data = dict(total=total, limit=limit, offset=offset, task=task,
                    has_more=offset + len(rows) < total, instances=[task_record(row) for row in rows])
        result = task_check(counts)
        result['evidence'].update(total=total, shown=len(rows), offset=offset, has_more=data['has_more'])
        return report('failures', [result], data)
    except Exception as error:
        return report('failures', [failure('state', error, '异常任务查询失败', '先运行 --diagnose。')])


def dwd_status(path, *, limit=20, window=None):
    try:
        raw, directory = _read_context(path)
        ready = load_ready_config(raw, path.parent)
        if not ready['enabled']:
            return report('dwd-status', [check('dwd', 'SKIP', '当前未启用 DWD 就绪门控')])
        with closing(DiagnosticLedger(directory / 'state.sqlite3')) as store:
            rows = list_windows(store, ready, limit=limit, selected=parse_window(window) if window else None)
        late = sum(row['warning_due'] for row in rows)
        result = check('dwd', 'WARN' if late else 'PASS', '窗口信号状态（北京时间）',
                       dict(windows=len(rows), late_windows=late, wait_warning_seconds=ready['wait_warning_seconds']),
                       'WAITING 时等待上游补齐；BEFORE_COVERAGE/WINDOW_OPEN 不属于当前完整覆盖窗口。')
        return report('dwd-status', [result], {'windows': rows})
    except Exception as error:
        return report('dwd-status', [failure('dwd', error, '就绪状态查询失败', '先运行 --diagnose。')])


def inspect_instance(path, instance_id):
    try:
        raw, directory = _read_context(path)
        with closing(DiagnosticLedger(directory / 'state.sqlite3')) as store:
            row = store.get_instance(instance_id)
            if row is None:
                return report('inspect-instance', [check('instance', 'FAIL', '未找到任务实例',
                              {'instance_id': instance_id}, '从 --failures 或 --status 复制完整 instance_id。')])
            data = dict(instance=task_record(row), diagnostic=None)
            checks = [task_check({row['status']: 1})]
            try:
                data['diagnostic'] = store.get_execution_diagnostic(instance_id)
            except Exception as error:
                checks.append(failure('execution-evidence', error, '附加错误证据不可读，主账本仍可查看',
                                      '按实例 query_id 核对服务端记录，并检查状态库完整性。'))
            data['action'] = evidence_action(data['diagnostic'], data['instance']['action'])
            definition, definition_result = definition_check(path, raw)
            if definition is None:
                checks.append(check('dependencies', 'SKIP', '配置无效，无法重建依赖',
                                    definition_result['evidence'], '修复配置后重新检查；实例账本仍可查看。'))
            else:
                try:
                    checks.extend(_inspect_dependencies(store, definition, row, data))
                except Exception as error:
                    checks.append(failure('dependencies', error, '无法推断依赖，主账本与错误证据仍可查看',
                                          '核对历史实例的时间字段、配置版本和状态库完整性。'))
        return report('inspect-instance', checks, data)
    except Exception as error:
        return report('inspect-instance', [failure('instance', error, '实例查询失败', '先运行 --diagnose。')])


def _inspect_dependencies(store, definition, row, data):
    if binding(definition.config, definition.dag) != store.get_meta('binding'):
        return [check('dependencies', 'SKIP', '当前配置与账本绑定不一致，未推断历史依赖', {},
                      '核对部署版本；实例原始状态与已保存错误证据仍可查看。')]
    expected = definition.builder.create_instance(definition.dag, row['taskname'],
                                                  datetime.fromisoformat(row['scheduled_time']))
    same = expected.instance_id == row['instance_id'] and all(
        getattr(expected, field) == datetime.fromisoformat(row[field]) for field in ('data_start', 'data_end'))
    if not same:
        return [check('dependencies', 'SKIP', '当前规则与历史实例窗口不一致', {}, '核对配置版本和状态库绑定。')]
    blocked = []
    for name in expected.upstream_instance_ids:
        status = store.get_status(name)
        if status is None or status.value != 'SUCCESS':
            blocked.append(dict(instance_id=name, status=status.value if status else 'NOT_FOUND'))
    data['dependencies'] = dict(total=len(expected.upstream_instance_ids), blocked_count=len(blocked),
                                blocked=blocked[:100], truncated=len(blocked) > 100)
    ready = definition.config['dwd_ready']
    if ready['enabled'] and expected.task.aggregation_interval == 'PT5M':
        data['windows'] = list_windows(store, ready, selected=expected.data_start)
    data['sql_file'] = str(definition.sql_dir / f'{expected.task.sql_id}.sql')
    return [check('dependencies', 'WARN' if blocked else 'PASS', '当前依赖状态',
                  {'total': len(expected.upstream_instance_ids), 'blocked': len(blocked)},
                  '按 blocked 中的 instance_id 继续 --inspect-instance；历史阻断原因可能已解除。' if blocked else '')]
