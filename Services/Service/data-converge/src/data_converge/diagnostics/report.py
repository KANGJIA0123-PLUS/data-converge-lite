"""One result model for operators and JSON automation; no raw exception bodies."""
from collections import Counter
from datetime import datetime
import json
import re

from data_converge.errors import safe_diagnostic_details
from data_converge.time_policy import system_timezone, system_timezone_name

LAUNCHER = 'bash /opt/its/data-converge/bin/start_data_converge.sh'


def check(identifier, status, summary, evidence=None, action=''):
    return dict(id=identifier, status=status, summary=summary,
                evidence=evidence or {}, action=action)


def report(command, checks, data=None):
    counts = Counter(item['status'] for item in checks)
    status = 'FAIL' if counts['FAIL'] else 'WARN' if counts['WARN'] else 'PASS'
    if counts and set(counts) == {'SKIP'}:
        status = 'SKIP'
    return dict(schema_version=1, command=command, status=status,
                checked_at=datetime.now(system_timezone()).isoformat(),
                timezone=system_timezone_name(), summary=dict(counts),
                checks=checks, data=data or {})


def public_error(value):
    if not value:
        return None
    code = str(value).split(':', 1)[0]
    if re.fullmatch(r'[A-Z][A-Z0-9_]{1,100}', code):
        return code
    if str(value).startswith('Upstream task instances are not successful:'):
        return 'UPSTREAM_NOT_READY'
    return 'ERROR_DETAIL_NOT_RECORDED'


def task_action(status, error=None):
    if status == 'UNKNOWN':
        return '先用 query_id 核实 ClickHouse 执行结果；确认后再人工 resolve，禁止直接重跑。'
    if status == 'FAILED':
        return '检查错误证据、attempts 和 next_retry_at；修复原因后按原重试策略处理。'
    if status == 'BLOCKED':
        return '用 --inspect-instance 查看当前缺失信号与未成功依赖。'
    if status == 'RUNNING':
        return '核对 started_at、query_id 和服务心跳；运行中不等于卡死。'
    return ''


def task_record(row):
    fields = ('instance_id', 'taskname', 'data_start', 'data_end', 'status', 'attempts',
              'retryable', 'next_retry_at', 'started_at', 'finished_at', 'query_id')
    value = {key: row.get(key) for key in fields}
    value['error'] = public_error(row.get('error'))
    value['action'] = task_action(row['status'], value['error'])
    return value


def attempt_record(row):
    fields = ('query_id', 'instance_id', 'taskname', 'scheduled_time', 'data_start', 'data_end',
              'attempt_number', 'status', 'started_at', 'finished_at', 'recovered_at',
              'retryable', 'next_retry_at')
    value = {key: row.get(key) for key in fields}
    value['error'] = public_error(row.get('error'))
    value['diagnostic'] = safe_diagnostic_details(row.get('diagnostic'))
    resolution = row.get('resolution')
    value['resolution'] = None if resolution is None else {
        key: resolution.get(key) for key in ('resolution', 'resolved_at', 'note_recorded')}
    return value


def legacy_resolution_record(row):
    return {key: row.get(key) for key in (
        'query_id', 'instance_id', 'taskname', 'resolution', 'resolved_at', 'note_recorded')}


def render_text(value):
    from .aggregation import render_aggregation

    lines = [f"{value['command']}  {value['status']}  {value['checked_at']}  ({value['timezone']})"]
    for item in value['checks']:
        lines.append(f"[{item['status']}] {item['id']}: {item['summary']}")
        if item['evidence']:
            lines.append('  证据: ' + json.dumps(item['evidence'], ensure_ascii=False, default=str))
        if item['action']:
            lines.append('  建议: ' + item['action'])
    data = value['data']
    for row in data.get('instances', []):
        lines.append(f"{row['status']}  {row['instance_id']}  error={row['error']}  query_id={row['query_id']}")
        if row['action']:
            lines.append('  建议: ' + row['action'])
    for row in data.get('windows', []):
        missing = ','.join(row['missing']) or '无'
        lines.append(f"{row['status']}  {row['window_start']}  缺少={missing}  等待={row['wait_seconds']}秒")
    if 'instance' in data:
        lines.extend(_render_instance(data))
    if value['command'] == 'history':
        lines.extend(_render_history(data))
    elif 'history' in data:
        lines.extend(_render_history(data['history']))
    if value['command'] == 'recovery-plan':
        lines.extend(_render_recovery(data))
    if value['command'] == 'events':
        lines.extend(_render_events(data))
    lines.extend(render_aggregation(data))
    return '\n'.join(lines)


def _render_recovery(data):
    lines = []
    for row in data.get('candidates', []):
        lines.append(f"{row['action']}  {row['instance_id']}  phase={row['phase'] or 'LEGACY'}"
                     f"  query_id={row['query_id'] or '-'}  attempts={row['attempts']}")
        lines.append(f"  判断依据: {row['reason']}")
    if data.get('has_more'):
        lines.append(f"下一页: --recovery-plan --limit {data['limit']} --offset {data['offset'] + data['limit']}")
    return lines


def _render_events(data):
    lines = [f"{row['event_id']}  {row['occurred_at']}  {row['event_type']}  {row['instance_id']}"
             f"  query_id={row['query_id'] or '-'}  attempt={row['attempt_number']}"
             for row in data.get('events', [])]
    if data.get('has_more'):
        lines.append(f"下一页: --events --after-event {data['next_after_event']} --limit {data['limit']}")
    return lines


def _render_history(data):
    if not data.get('available'):
        return ['执行历史: 未记录，无法追补旧版执行过程。'] if data.get('available') is False else []
    lines = [f"执行历史: 本页 {len(data['attempts'])} 条，偏移 {data['offset']}，每页 {data['limit']} 条"]
    for row in data['attempts']:
        lines.extend([f"  {row['status']}  {row['instance_id']}  第 {row['attempt_number']} 次尝试",
                      f"    窗口: [{row['data_start']}, {row['data_end']})",
                      f"    query_id: {row['query_id']}  错误码: {row['error'] or '无'}",
                      f"    started_at: {row['started_at']}  finished_at: {row['finished_at'] or '-'}",
                      f"    retryable: {row['retryable']}  next_retry_at: {row['next_retry_at'] or '-'}"])
        if row['recovered_at']:
            lines.append(f"    恢复为 UNKNOWN 的时间: {row['recovered_at']}")
        if row['diagnostic']:
            lines.append('    错误证据: ' + '  '.join(f'{key}={value}' for key, value in row['diagnostic'].items()))
        if resolution := row['resolution']:
            note = '已记录（不展示正文）' if resolution['note_recorded'] else '未记录'
            lines.append(f"    人工处理: {resolution['resolution']}  时间: {resolution['resolved_at']}  说明: {note}")
    if data['has_more']:
        lines.append(f"  还有历史记录；下一页使用 --limit {data['limit']} --offset {data['offset'] + data['limit']}。")
    lines.extend(_render_legacy_resolutions(data))
    return lines


def _render_legacy_resolutions(data):
    rows = data.get('legacy_resolutions', [])
    lines = [f"旧版执行的人工处置: 本页 {len(rows)} 条（原始执行过程未记录）"] if rows else []
    for row in rows:
        note = '已记录（不展示正文）' if row['note_recorded'] else '未记录'
        lines.extend([f"  {row['resolution']}  {row['instance_id']}  时间: {row['resolved_at']}",
                      f"    query_id: {row['query_id'] or '未记录'}  说明: {note}"])
    if data.get('legacy_resolutions_has_more'):
        lines.append('  旧版人工处置独立分页；下一页使用 '
                     f"--limit {data['limit']} --offset {data['offset'] + data['limit']}。")
    return lines


def _render_instance(data):
    row = data['instance']
    lines = [f"实例: {row['instance_id']}",
             f"  窗口: [{row['data_start']}, {row['data_end']})",
             f"  状态: {row['status']}  尝试次数: {row['attempts']}",
             f"  query_id: {row['query_id'] or '未记录'}",
             f"  错误码: {row['error'] or '无'}",
             f"  started_at: {row['started_at'] or '-'}  finished_at: {row['finished_at'] or '-'}",
             f"  retryable: {row['retryable']}  next_retry_at: {row['next_retry_at'] or '-'}"]
    diagnostic = data.get('diagnostic')
    if diagnostic:
        lines.append('  错误证据: ' + '  '.join(f'{key}={value}' for key, value in diagnostic.items()))
    else:
        lines.append('  错误证据: 未记录（历史错误无法追补，成功或已解决实例不展示旧错误）')
    if data.get('sql_file'):
        lines.append('  SQL 文件: ' + data['sql_file'])
    if data.get('action'):
        lines.append('  建议: ' + data['action'])
    if cross_period := data.get('lineage', {}).get('cross_period_tasks'):
        lines.append('  跨周期数据来源（按配置限时等待）: ' + ', '.join(cross_period))
    if dependencies := data.get('dependencies'):
        lines.append(f"  同周期执行前置实例: {dependencies['total']}，未成功: {dependencies['blocked_count']}")
        lines.extend(f"    {item['status']}  {item['instance_id']}" for item in dependencies['blocked'])
        if dependencies['truncated']:
            lines.append('    仅展示前 100 个未成功依赖。')
    return lines


def evidence_action(diagnostic, fallback):
    details = diagnostic or {}
    if 'tls_verify_code' in details:
        return '核对 SSL_CERT_FILE 指向的 CA 证书、证书有效期与服务域名；保持证书校验开启。' + fallback
    if details.get('http_status') == 401:
        return '核对配置账号、密码文件及解密后选中的账号。' + fallback
    if details.get('clickhouse_exception_code') == 16:
        return '核对 INSERT 目标列与现场 ClickHouse 表结构是否一致。' + fallback
    return fallback
