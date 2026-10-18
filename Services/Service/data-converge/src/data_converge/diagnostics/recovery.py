"""Read-only projections of recovery decisions and transactional audit events."""
from contextlib import closing

from data_converge.config.application import load_app_config, state_directory

from .checks import failure
from .ledger import DiagnosticLedger
from .report import check, report


def _open_ledger(path):
    raw = load_app_config(path)
    return DiagnosticLedger(state_directory(raw, path) / 'state.sqlite3')


def recovery_plan(path, *, limit=20, offset=0):
    try:
        with closing(_open_ledger(path)) as ledger:
            counts = ledger.recovery_counts()
            rows = ledger.list_recovery_candidates(limit=limit, offset=offset)
        total = sum(counts.values())
        data = dict(counts=counts, total=total, limit=limit, offset=offset,
                    has_more=offset + len(rows) < total, candidates=rows)
        result = check('recovery-plan', 'WARN' if counts['RECONCILE'] else 'PASS',
                       '只读恢复预览；未修改状态，未提交 SQL', counts,
                       'AUTO_RESUME 由下次合法写进程启动时恢复；RECONCILE 需先按 query_id 核对。'
                       '运行中的任务也可能在此快照后完成，不应据此直接重跑。')
        return report('recovery-plan', [result], data)
    except Exception as error:
        return report('recovery-plan', [failure('state', error, '恢复计划查询失败',
                                               '先运行 --diagnose；本命令不会创建或修复账本。')])


def execution_events(path, *, after_event=0, limit=20):
    try:
        with closing(_open_ledger(path)) as ledger:
            available = ledger.events_available()
            rows = ledger.list_execution_events(after_event=after_event, limit=limit)
            cursor = rows[-1]['event_id'] if rows else after_event
            more = bool(ledger.list_execution_events(after_event=cursor, limit=1))
        data = dict(available=available, after_event=after_event, next_after_event=cursor,
                    limit=limit, has_more=more, events=rows)
        result = check('execution-events', 'PASS' if available else 'SKIP',
                       '事务事件查询完成' if available else '旧版未记录事务事件；不追补历史',
                       dict(shown=len(rows), next_after_event=cursor, has_more=more),
                       '事件序号仅在同一账本内有效；使用 --after-event 续读，JSONL 可重定向到文件。')
        return report('events', [result], data)
    except Exception as error:
        return report('events', [failure('state', error, '执行事件查询失败', '先运行 --diagnose。')])
