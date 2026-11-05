"""Human-readable text for diagnostic reports: summary first, tables, then next steps.

JSON/JSONL output is unchanged; this module only shapes the default text view.
Evidence is hidden for passing checks unless ``verbose`` is requested.
"""
from .terminal import (badge, display_width, duration, seconds_text, evidence_lines, heading, local_time, pad, paint,
                       table, window)

COMMAND_TITLES = {
    'diagnose': '运行诊断', 'failures': '当前异常实例', 'history': '执行历史',
    'recovery-plan': '恢复预览（只读）', 'events': '执行事件', 'inspect-instance': '实例详情',
    'dwd-status': 'DWD 就绪信号', 'plan-config': '配置迁移预览（只读）',
}
SEVERITY = {'FAIL': 0, 'WARN': 1, 'PASS': 2, 'SKIP': 3}
CHECK_NAMES = {
    'config': '应用配置', 'definition': '任务与 SQL', 'runtime.modules': '运行模块',
    'logging.settings': '日志配置', 'state.archives': '状态备份', 'logs': '日志路径',
    'heartbeat': '进程心跳', 'state': '状态库', 'tasks': '任务状态', 'leadership': '选主配置',
    'leadership.redis': 'Redis 租约', 'dwd': 'DWD 就绪', 'connections': '在线连接',
    'execution-history': '执行历史', 'execution-events': '执行事件', 'dependencies': '上游依赖',
    'recovery-plan': '恢复分类', 'preaggregate': '提前汇聚', 'cross-period-wait': '跨周期等待',
    'historical-instance': '历史实例', 'instance': '实例', 'execution-evidence': '错误证据',
    'configuration-plan': '迁移计划', 'clickhouse': 'ClickHouse', 'redis': 'Redis',
}
RECOVERY_REASONS = {
    'PREPARED_NOT_DISPATCHED': 'SQL 尚未发出，重启后自动续跑',
    'OUTCOME_UNCONFIRMED': '结果未确认，需按 query_id 核对',
    'DISPATCH_COMMITTED': 'SQL 已发出，需按 query_id 核对',
    'NO_PRE_SUBMISSION_PROOF': '无提交前凭证，需按 query_id 核对',
}


def render_report(value, *, verbose=False):
    from .aggregation import render_aggregation

    data = value['data']
    lines = _header(value)
    lines.extend(_checks(value['checks'], verbose, value['command']))
    body = BODIES.get(value['command'])
    if body is not None:
        lines.extend(body(data))
    elif data.get('windows'):
        lines.extend(_windows(data['windows']))
    extra = render_aggregation(data)
    if extra:
        lines.extend(['', heading('提前汇聚 / 跨周期等待')] + ['  ' + line for line in extra])
    lines.extend(_next_steps(value))
    return '\n'.join(lines)


def _header(value):
    title = COMMAND_TITLES.get(value['command'], value['command'])
    counts = value.get('summary') or {}
    tally = '  '.join(f"{name} {counts[name]}" for name in ('FAIL', 'WARN', 'PASS', 'SKIP') if counts.get(name))
    stamp = f"{local_time(value['checked_at'])} ({value['timezone']})"
    return [paint(f"{title}  ", 'bold') + badge(value['status'], 4).rstrip()
            + (f"   {tally}" if tally else ''), paint(stamp, 'grey')]


def _checks(checks, verbose, command):
    if not checks:
        return []
    ordered = sorted(checks, key=lambda item: SEVERITY.get(item['status'], 9))
    names = [CHECK_NAMES.get(item['id'], item['id']) for item in ordered]
    width = max(display_width(name) for name in names)
    lines = ['']
    for item, name in zip(ordered, names):
        lines.append(f"  {badge(item['status'], 4)} {pad(name, width)}  {item['summary']}")
        if not (verbose or item['status'] in ('FAIL', 'WARN')):
            continue
        lines.extend(evidence_lines(item['evidence']))
        # Hints that point at the command already running add nothing.
        if item['action'] and item['action'].strip() != f"dconv --{command}":
            lines.append(paint('      → ', 'cyan') + item['action'])
    return lines


def _instances(data):
    rows = data.get('instances', [])
    if not rows:
        return ['', '  没有异常实例。'] if 'instances' in data else []
    lines = ['', heading(f"异常实例 {data.get('total', len(rows))} 个")]
    lines.extend(table(['状态', '任务', '窗口', '错误码', '尝试'], [
        [badge(row['status']), row['taskname'], window(row['data_start'], row['data_end']),
         row['error'], row['attempts']] for row in rows]))
    lines.extend(['', paint('  逐条处理：', 'grey')])
    for row in rows:
        lines.append(f"  {badge(row['status']).rstrip()}  {row['instance_id']}")
        lines.append(paint(f"      query_id {row['query_id'] or '-'}", 'grey'))
        if row['action']:
            lines.append(paint('      → ', 'cyan') + row['action'])
    return lines + _more(data, '--failures')


def _windows(rows):
    if not rows:
        return []
    lines = ['', heading('窗口信号')]
    lines.extend(table(['状态', '窗口', '已等待', '缺少（省略 _REALTIME；! 表示超过告警阈值）'], [
        [badge(row['status']), window(row['window_start'], row['window_end']),
         seconds_text(row['wait_seconds']) + (' !' if row.get('warning_due') else ''),
         ' '.join(name.removesuffix('_REALTIME') for name in row['missing']) or '无']
        for row in rows]))
    return lines


def _history(data):
    if not data.get('available'):
        return ['', '  执行历史: 未记录，无法追补旧版执行过程。'] if data.get('available') is False else []
    rows = data['attempts']
    lines = ['', heading(f"执行历史 本页 {len(rows)} 条（偏移 {data['offset']}）")]
    if rows:
        lines.extend(table(['状态', '任务', '窗口', '次', '耗时', '错误码', '开始'], [
            [badge(row['status']), row['taskname'], window(row['data_start'], row['data_end']),
             row['attempt_number'], duration(row['started_at'], row['finished_at']),
             row['error'], local_time(row['started_at'], date=False)] for row in rows]))
    lines.extend(_history_details(rows))
    if data['has_more']:
        lines.append(paint(f"  还有更多：--limit {data['limit']} --offset {data['offset'] + data['limit']}", 'grey'))
    return lines + _legacy_resolutions(data)


def _history_details(rows):
    """Only finished problems, evidence or operator actions get a detail block."""
    lines = []
    for row in rows:
        resolution, evidence = row['resolution'], row['diagnostic']
        if row['status'] in ('SUCCESS', 'RUNNING', 'PENDING') and not resolution and not evidence:
            continue
        lines.extend(['', f"  {badge(row['status']).rstrip()}  {row['instance_id']}  第 {row['attempt_number']} 次尝试",
                      f"      query_id: {row['query_id']}  错误码: {row['error'] or '无'}"])
        if row['next_retry_at']:
            lines.append(f"      下次重试: {local_time(row['next_retry_at'])}")
        if row['recovered_at']:
            lines.append(f"      恢复为 UNKNOWN 的时间: {local_time(row['recovered_at'])}")
        if evidence:
            lines.append('      错误证据: ' + '  '.join(f'{key}={value}' for key, value in evidence.items()))
        if resolution:
            note = '已记录（不展示正文）' if resolution['note_recorded'] else '未记录'
            lines.append(f"      人工处理: {resolution['resolution']}  "
                         f"时间: {local_time(resolution['resolved_at'])}  说明: {note}")
    return lines


def _legacy_resolutions(data):
    rows = data.get('legacy_resolutions', [])
    if not rows:
        return []
    lines = ['', heading(f"旧版执行的人工处置 {len(rows)} 条（原始执行过程未记录）")]
    for row in rows:
        note = '已记录（不展示正文）' if row['note_recorded'] else '未记录'
        lines.extend([f"  {row['resolution']}  {row['instance_id']}  时间: {local_time(row['resolved_at'])}",
                      f"      query_id: {row['query_id'] or '未记录'}  说明: {note}"])
    if data.get('legacy_resolutions_has_more'):
        lines.append(paint(f"  下一页: --limit {data['limit']} --offset {data['offset'] + data['limit']}", 'grey'))
    return lines


def _instance(data):
    if 'instance' not in data:
        return []
    row = data['instance']
    lines = ['', heading(f"实例 {row['instance_id']}"),
             f"  状态:     {badge(row['status']).rstrip()}   尝试 {row['attempts']} 次",
             f"  窗口:     {window(row['data_start'], row['data_end'])}",
             f"  query_id: {row['query_id'] or '未记录'}",
             f"  错误码:   {row['error'] or '无'}",
             f"  开始:     {local_time(row['started_at'])}",
             f"  结束:     {local_time(row['finished_at'])}  （耗时 {duration(row['started_at'], row['finished_at'])}）"]
    if row['next_retry_at']:
        lines.append(f"  下次重试: {local_time(row['next_retry_at'])}")
    diagnostic = data.get('diagnostic')
    lines.append('  错误证据: ' + ('  '.join(f'{key}={value}' for key, value in diagnostic.items()) if diagnostic
                                  else '未记录（历史错误无法追补，成功或已解决实例不展示旧错误）'))
    if data.get('sql_file'):
        lines.append(f"  SQL 文件: {data['sql_file']}")
    lines.extend(_instance_dependencies(data))
    if data.get('windows'):
        lines.extend(_windows(data['windows']))
    if data.get('action'):
        lines.extend(['', paint('  → ', 'cyan') + data['action']])
    if 'history' in data:
        lines.extend(_history(data['history']))
    return lines


def _instance_dependencies(data):
    lines = []
    if cross_period := data.get('lineage', {}).get('cross_period_tasks'):
        lines.append('  跨周期数据来源（按配置限时等待）: ' + ', '.join(cross_period))
    if dependencies := data.get('dependencies'):
        lines.append(f"  同周期执行前置实例: {dependencies['total']}，未成功: {dependencies['blocked_count']}")
        lines.extend(f"      {badge(item['status']).rstrip()}  {item['instance_id']}" for item in dependencies['blocked'])
        if dependencies['truncated']:
            lines.append('      仅展示前 100 个未成功依赖。')
    return lines


def _recovery(data):
    rows = data.get('candidates', [])
    if not rows:
        return ['', '  没有需要恢复或核对的实例。']
    lines = ['', heading(f"恢复候选 {data.get('total', len(rows))} 个")]
    lines.extend(table(['处理方式', '实例', '阶段', '尝试', '核对依据'], [
        [badge(row['action'], 11), row['instance_id'], row['phase'] or 'LEGACY', row['attempts'],
         f"query_id={row['query_id'] or '-'}"] for row in rows]))
    reasons = sorted({row['reason'] for row in rows})
    lines.extend(['', paint('  判断依据：', 'grey')])
    lines.extend(table(['依据', '含义'], [[reason, RECOVERY_REASONS.get(reason, '')] for reason in reasons], '    ')[1:])
    return lines + _more(data, '--recovery-plan')


def _events(data):
    rows = data.get('events', [])
    if not rows:
        return []
    lines = ['', heading(f"事件 {len(rows)} 条")]
    lines.extend(table(['序号', '时间', '事件', '实例', '次'], [
        [row['event_id'], local_time(row['occurred_at']), badge(row['event_type'], 15),
         row['instance_id'], row['attempt_number']] for row in rows]))
    if data.get('has_more'):
        lines.append(paint(f"  下一页: --events --after-event {data['next_after_event']} --limit {data['limit']}", 'grey'))
    return lines


def _more(data, command):
    if not data.get('has_more'):
        return []
    return [paint(f"  下一页: {command} --limit {data['limit']} --offset {data['offset'] + data['limit']}", 'grey')]


def _next_steps(value):
    """A short, copyable hint at the end; only when there is something to do."""
    hints = NEXT_STEPS.get(value['command'])
    if not hints or value['status'] in ('PASS', 'SKIP'):
        return []
    return ['', paint('下一步', 'bold')] + [f"  dconv {hint}" for hint in hints]


NEXT_STEPS = {
    'diagnose': ['--failures                    # 看具体哪些实例异常', '--diagnose --online           # 再查远端连通性'],
    'failures': ["--inspect-instance '实例ID'    # 查看单个实例证据和依赖"],
    'recovery-plan': ["--inspect-instance '实例ID'    # 按 query_id 核对后再 --resolve-instance"],
}
BODIES = {
    'failures': _instances, 'history': _history, 'inspect-instance': _instance,
    'recovery-plan': _recovery, 'events': _events, 'dwd-status': lambda data: _windows(data.get('windows', [])),
}
