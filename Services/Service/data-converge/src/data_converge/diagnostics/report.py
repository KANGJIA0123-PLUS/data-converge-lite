"""One result model for operators and JSON automation; no raw exception bodies."""
from collections import Counter
from datetime import datetime
import json
import re
from zoneinfo import ZoneInfo

TZ = ZoneInfo('Asia/Shanghai')
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
                checked_at=datetime.now(TZ).isoformat(), summary=dict(counts),
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


def render_text(value):
    lines = [f"{value['command']}  {value['status']}  {value['checked_at']}"]
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
    return '\n'.join(lines)


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
    if dependencies := data.get('dependencies'):
        lines.append(f"  上游依赖: {dependencies['total']}，未成功: {dependencies['blocked_count']}")
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
