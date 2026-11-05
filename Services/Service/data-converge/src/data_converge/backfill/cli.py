"""Explicit background backfill submission and read-only progress commands."""
from datetime import datetime
from contextlib import closing
import json
import re

from data_converge.application import Application
from data_converge.errors import safe_error_details
from .queue import BackfillQueueError, Queue, TOKEN_PATTERN, validate_request_id


def add_arguments(parser, modes):
    modes.add_argument('--backfill', action='store_true', help='提交指定时间范围补算，由正常服务后台执行')
    modes.add_argument('--backfill-status', metavar='JOB_ID', help='只读查询补算进度，支持 --limit / --offset')
    parser.add_argument('--all-tasks', action='store_true', help='配合 --backfill，选择当前配置的全部任务')
    parser.add_argument('--from', dest='from_time', metavar='TIME', help='补算数据范围起点（含），带时区')
    parser.add_argument('--to', dest='to_time', metavar='TIME', help='补算数据范围终点（不含），带时区')
    parser.add_argument('--preaggregate', action='store_true', help='配合 --backfill，提前测试已开始的未结束窗口；正式到期仍覆盖')
    parser.add_argument('--recompute', action='store_true', help='配合 --backfill，明确重新汇聚已成功窗口')
    parser.add_argument('--dry-run', action='store_true', help='配合 --backfill，只预览完整窗口，不创建或提交请求')
    parser.add_argument('--request-id', metavar='ID', help='补算提交幂等标识；重复提交相同计划返回同一任务号')


def is_command(args):
    return args.backfill or args.backfill_status is not None


def validate_arguments(parser, args):
    for name in ('all_tasks', 'from_time', 'to_time', 'recompute', 'preaggregate', 'dry_run', 'request_id'):
        value = getattr(args, name)
        if value is not None and value is not False and not args.backfill:
            option = {'from_time': 'from', 'to_time': 'to'}.get(name, name.replace('_', '-'))
            parser.error('--' + option + ' requires --backfill')
    if args.backfill:
        if bool(args.task) == bool(args.all_tasks):
            parser.error('--backfill requires exactly one of --task or --all-tasks')
        if args.from_time is None or args.to_time is None:
            parser.error('--backfill requires --from and --to')
        for value in (args.from_time, args.to_time):
            try:
                instant = datetime.fromisoformat(value)
                if instant.tzinfo is None or instant.utcoffset() is None:
                    raise ValueError
            except (TypeError, ValueError):
                parser.error('--from and --to require ISO8601 timestamps with timezone')
        try:
            validate_request_id(args.request_id)
        except BackfillQueueError:
            parser.error('--request-id requires a safe token of 1 to 128 characters')
    if args.backfill_status is not None and not re.fullmatch(TOKEN_PATTERN, args.backfill_status):
        parser.error('--backfill-status requires a valid job ID')


def run(args):
    command = 'backfill-status' if args.backfill_status is not None else 'backfill'
    try:
        application = Application(args.config)
        value = _status(args, application) if args.backfill_status is not None else _submit(args, application)
    except Exception as error:
        evidence = safe_error_details(error)
        if re.fullmatch(r'BACKFILL_[A-Z0-9_]{1,100}', str(error)):
            evidence['error_code'] = str(error)
        value = {'schema_version': 1, 'command': command, 'status': 'FAIL', 'error': evidence}
    print(json.dumps(value, ensure_ascii=False) if args.format == 'json' else _render(value))
    return int(value['status'] in {'FAIL', 'COMPLETED_WITH_ERRORS'})


def _submit(args, application):
    plan = _build_plan(args, application)
    if args.dry_run:
        return {'schema_version': 1, 'command': 'backfill', 'status': 'PREVIEW', 'data': plan}
    if not (application.directory / 'state.sqlite3').is_file():
        raise BackfillQueueError('BACKFILL_STATE_MISSING')
    with Queue(application.directory / 'backfill.sqlite3', create=True) as queue:
        job_id = queue.submit(plan, request_id=args.request_id)
        result = queue.status(job_id)
    return {'schema_version': 1, 'command': 'backfill', 'status': result['status'], 'data': result}


def _build_plan(args, application):
    from data_converge.state import StateStore
    from .plan import build_plan

    options = dict(task=args.task, all_tasks=args.all_tasks, recompute=args.recompute)
    if not args.preaggregate:
        return build_plan(application.definition, args.from_time, args.to_time, **options)
    path = application.directory / 'state.sqlite3'
    if not path.is_file():
        raise BackfillQueueError('BACKFILL_STATE_MISSING')
    with closing(StateStore(path)) as store:
        store._connection().execute('BEGIN')
        return build_plan(application.definition, args.from_time, args.to_time,
                          preaggregate=True, store=store, **options)


def _status(args, application):
    path = application.directory / 'backfill.sqlite3'
    if not path.is_file():
        raise BackfillQueueError('BACKFILL_QUEUE_MISSING')
    with Queue(path, read_only=True) as queue:
        result = queue.status(args.backfill_status, limit=args.limit, offset=args.offset)
    return {'schema_version': 1, 'command': 'backfill-status', 'status': result['status'], 'data': result}


def _render(value):
    from data_converge.diagnostics.terminal import badge, heading, paint

    if value['status'] == 'FAIL':
        error = value['error']
        return (badge('FAIL', 4) + ' 补算命令未完成：' + error.get('error_code', error.get('error_type', 'UNKNOWN'))
                + '\n  → 请核对配置、起止时间及任务名称；查看 dconv --help 获取用法。')
    data = value['data']
    lines = _preview_lines(data) if value['status'] == 'PREVIEW' else _progress_lines(data)
    metadata = data if value['status'] == 'PREVIEW' else data.get('metadata', {})
    if metadata.get('preaggregate'):
        lines.append(paint('  ! PREAGGREGATE 为提前测试，成功不代表正式完成；到期正式汇聚仍将覆盖。', 'yellow'))
        lines.append(f"  省略尚未开始窗口：{metadata.get('omitted_future_count', 0)} 个；不创建写入条目。")
    if data.get('items'):
        lines.extend(['', heading('窗口明细')] + _item_table(data['items']))
    if data.get('has_more'):
        lines.append(paint(f"  下一页：dconv --backfill-status {data['job_id']} --limit {data['limit']}"
                           f" --offset {data['offset'] + data['limit']}", 'grey'))
    return '\n'.join(lines)


def _preview_lines(data):
    from data_converge.diagnostics.terminal import badge, paint, window

    lines = [paint('补算预览  ', 'bold') + badge('PREVIEW', 4).rstrip()
             + f"   共 {data['total']} 个完整窗口；未提交、未执行 SQL。",
             f"  范围：{window(data['start'], data['end'])}  [{data['start']}, {data['end']})",
             f"  重新汇聚成功窗口：{'是（--recompute）' if data['recompute'] else '否，已成功窗口会跳过'}",
             '  任务：' + ', '.join(data['selected_tasks'])]
    lines.extend(f"    {name}：{data['counts'][name]} 个窗口" for name in data['selected_tasks'])
    if data['omitted_tasks']:
        lines.append('  未纳入（范围内无完整窗口）：' + ', '.join(data['omitted_tasks']))
    lines.append(paint('  确认无误后去掉 --dry-run 重新执行即可提交。', 'cyan'))
    return lines


def _progress_lines(data):
    from data_converge.diagnostics.terminal import badge, paint

    counts = '  '.join(f"{name} {count}" for name, count in sorted(data['counts'].items()))
    return [paint(f"补算 {data['job_id']}  ", 'bold') + badge(data['status'], 4).rstrip()
            + f"   已完成 {data['completed']}/{data['total']}",
            f"  执行结果：{counts or '-'}",
            '  由正常服务后台串行处理；QUEUED 包含尚未落盘的当前执行项，提交成功不代表汇聚完成。',
            paint(f"  查询进度：dconv --backfill-status {data['job_id']}", 'cyan')]


def _item_table(items):
    from data_converge.diagnostics.terminal import badge, table, window

    return table(['状态', '类型', '任务', '窗口', 'query_id', '错误码'], [
        [badge(item.get('status', 'PLANNED')), item.get('execution_kind', 'BACKFILL'), item['taskname'],
         window(item['data_start'], item['data_end']) if item.get('data_start') else item['instance_id'],
         item.get('query_id'), item.get('error')] for item in items])
