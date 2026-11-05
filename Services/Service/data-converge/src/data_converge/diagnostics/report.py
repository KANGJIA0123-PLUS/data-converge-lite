"""One result model for operators and JSON automation; no raw exception bodies."""
from collections import Counter
from datetime import datetime
import re

from data_converge.errors import safe_diagnostic_details
from data_converge.time_policy import system_timezone, system_timezone_name

LAUNCHER = 'dconv'


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


def render_text(value, *, verbose=False):
    from .render import render_report

    return render_report(value, verbose=verbose)


def evidence_action(diagnostic, fallback):
    details = diagnostic or {}
    if 'tls_verify_code' in details:
        return '核对 SSL_CERT_FILE 指向的 CA 证书、证书有效期与服务域名；保持证书校验开启。' + fallback
    if details.get('http_status') == 401:
        return '核对配置账号、密码文件及解密后选中的账号。' + fallback
    if details.get('clickhouse_exception_code') == 16:
        return '核对 INSERT 目标列与现场 ClickHouse 表结构是否一致。' + fallback
    return fallback
