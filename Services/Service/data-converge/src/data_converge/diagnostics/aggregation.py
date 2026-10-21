"""Read-only views of preliminary writes and persisted dependency waits."""
from datetime import datetime

from data_converge.errors import safe_diagnostic_details
from data_converge.scheduler.cross_period_wait import missing_summary
from data_converge.state import preaggregate
from data_converge.time_policy import system_timezone
from .report import check, public_error, report


def _public_attempt(row):
    fields = ('item_id', 'instance_id', 'taskname', 'data_start', 'data_end',
              'formal_scheduled_time', 'query_id', 'status', 'effective_status',
              'created_at', 'started_at', 'finished_at', 'covered_at')
    value = {name: row.get(name) for name in fields}
    value['execution_kind'] = 'PREAGGREGATE'
    value['error'] = public_error(row.get('error'))
    value['diagnostic'] = safe_diagnostic_details(row.get('diagnostic'))
    value['resolution'] = None if not row.get('resolution') else dict(
        resolution=row['resolution'], resolved_at=row.get('resolved_at'),
        note_recorded=bool(row.get('resolution_note')))
    return value


def preaggregate_page(store, *, instance_id=None, task=None, limit=20, offset=0,
                      only_unresolved=False, problems_only=False):
    filters = dict(taskname=task, only_unresolved=only_unresolved, problems_only=problems_only)
    rows = store.list_preaggregates(instance_id, limit=limit, offset=offset, **filters)
    more = len(rows) == limit and bool(store.list_preaggregates(
        instance_id, limit=1, offset=offset + limit, **filters))
    return dict(available=preaggregate.available(store._connection()),
                attempts=[_public_attempt(row) for row in rows], limit=limit,
                offset=offset, has_more=more, task=task, instance_id=instance_id)


def preaggregate_checks(page, mode):
    rows = page['attempts']
    if not rows:
        return []
    if mode == 'history':
        status, summary = 'PASS', '提前执行历史查询完成；提前成功不代表正式完成'
    elif mode == 'recovery':
        status, summary = 'WARN', '存在运行中或尚未核查的提前写入；未提交重跑'
    elif mode == 'failures':
        status, summary = 'FAIL', '存在尚未正式覆盖的提前执行异常'
    else:
        failed = rows[0]['effective_status'] in ('FAILED', 'UNKNOWN', 'BLOCKED')
        status, summary = ('FAIL' if failed else 'PASS'), '提前执行记录；正式完成需单独确认'
    return [check('preaggregate', status, summary,
                  dict(shown=len(rows), has_more=page['has_more']),
                  '提前 UNKNOWN 先核对清理和汇聚均已结束，再用 --resolve-preaggregate ITEM_ID '
                  '--resolution success|retry --note 核查依据；不会标记正式成功。')]


def inspect_preaggregate(store, instance_id, *, limit, offset):
    page = preaggregate_page(store, instance_id=instance_id, limit=limit, offset=offset)
    exists = page['attempts'] or store.list_preaggregates(instance_id, limit=1)
    if not exists:
        return report('inspect-instance', [check('instance', 'FAIL', '未找到任务实例',
                      {'instance_id': instance_id}, '从 --failures 或 --status 复制完整 instance_id。')])
    checks = [check('instance', 'PASS', '正式实例尚未入账；以下仅为提前执行记录')]
    checks.extend(preaggregate_checks(page, 'inspect'))
    return report('inspect-instance', checks, dict(
        instance_id=instance_id, formal_instance_registered=False, preaggregate=page))


def attach_instance_extras(store, row, data, checks, *, limit, offset):
    page = preaggregate_page(store, instance_id=row['instance_id'], limit=limit, offset=offset)
    data['preaggregate'] = page
    data['formal_instance_registered'] = True
    record = store.get_cross_period_wait(row['instance_id'])
    if record is None:
        return
    wait = dict(record)
    deadline = datetime.fromisoformat(wait['deadline_at'])
    wait['remaining_seconds'] = max(0, int((deadline - datetime.now(system_timezone())).total_seconds()))
    data['cross_period_wait'] = wait
    state = wait['status']
    checks.append(check('cross-period-wait', 'WARN' if state in ('WAITING', 'TIMED_OUT') else 'PASS',
                        '跨周期等待记录；超时执行不代表源数据完整',
                        {name: wait.get(name) for name in ('status', 'scheduled_due_at', 'deadline_at',
                         'wait_seconds', 'missing_count', 'decision_reason', 'remaining_seconds')}))


def attach_current_wait(store, definition, instance, data):
    if wait := data.get('cross_period_wait'):
        wait['enabled_now'] = definition.options.cross_period_wait_enabled
        wait['current_missing'] = missing_summary(definition.dag, store, instance)


def render_aggregation(data):
    lines = []
    if wait := data.get('cross_period_wait'):
        lines.extend([f"跨周期等待记录: {wait['status']}  截止: {wait['deadline_at']}",
                      f"  额外等待: {wait['wait_seconds']}秒  决策时未成功上游: {wait.get('missing_count', 0)}"])
        if 'enabled_now' in wait:
            lines.append(f"  当前配置开关: {wait['enabled_now']}  当前未成功上游: "
                         f"{wait['current_missing']['missing_count']}")
        lines.extend(f"  {row['status']}  {row['instance_id']}" for row in wait.get('missing_samples', []))
        if wait.get('missing_truncated'):
            lines.append('  未成功上游仅展示前100条。')
    page = data.get('preaggregate', {})
    for row in page.get('attempts', []):
        lines.extend([f"PREAGGREGATE  {row['status']}  {row['instance_id']}  item_id={row['item_id']}",
                      f"  query_id={row['query_id'] or '-'}  error={row['error'] or '-'}",
                      f"  正式计划时间: {row['formal_scheduled_time']}  正式覆盖时间: {row['covered_at'] or '未记录'}"])
        if resolution := row['resolution']:
            lines.append(f"  人工核查: {resolution['resolution']}  原始状态保留；不代表正式完成")
    if page.get('has_more'):
        lines.append(f"提前记录下一页: --limit {page['limit']} --offset {page['offset'] + page['limit']}")
    return lines
