"""Diagnostic CLI options cannot fall through into service startup."""
import json
import sys

from .report import render_text
from .windows import parse_window


def add_arguments(parser, modes):
    modes.add_argument('--diagnose', action='store_true', help='只读检查配置、状态库、心跳及异常')
    modes.add_argument('--failures', action='store_true', help='查询 UNKNOWN / FAILED / BLOCKED 实例')
    modes.add_argument('--history', action='store_true', help='只读查询各次执行结果与人工处理记录')
    modes.add_argument('--recovery-plan', action='store_true', help='只读预览可恢复和需核对的实例，不重跑')
    modes.add_argument('--events', action='store_true', help='只读导出事务内执行事件，支持 JSONL')
    modes.add_argument('--inspect-instance', metavar='ID', help='查看一个实例的错误证据、上游依赖和执行历史')
    modes.add_argument('--dwd-status', action='store_true', help='查询五分钟窗口的缺失 DWD 信号（默认最近20个窗口）')
    parser.add_argument('--online', action='store_true', help='仅与 --diagnose 使用，显式检查远端连接')
    parser.add_argument('--task', metavar='任务名', help='查询 --failures / --history 时筛选任务；--backfill 时指定补算任务')
    parser.add_argument('--limit', type=int, metavar='N', help='查询条数或实例历史条数，默认20，最大100')
    parser.add_argument('--offset', type=int, metavar='N', help='--failures / --history / --inspect-instance 分页偏移，默认0')
    parser.add_argument('--after-event', type=int, metavar='N', help='仅与 --events 使用，读取该事件序号之后的记录')
    parser.add_argument('--window', metavar='TIME', help='仅与 --dwd-status 使用，含时区的五分钟窗口开始时间')
    parser.add_argument('--format', choices=('text', 'json', 'jsonl'), metavar='text|json|jsonl',
                        help='默认 text 表格；json 供脚本/回传；jsonl 仅供 --events')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='文本输出时展开通过项的全部证据；默认只展开 FAIL / WARN')


def is_diagnostic(args):
    return bool(args.diagnose or args.failures or args.history or args.recovery_plan
                or args.events or args.inspect_instance is not None or args.dwd_status or args.plan_config)


def validate_arguments(parser, args):
    for value, option in ((args.inspect_instance, '--inspect-instance'), (args.task, '--task')):
        if value is not None and (not value.strip() or any(ord(char) < 32 for char in value)):
            parser.error(option + ' requires a nonempty value without control characters')
    for supplied, allowed, message in _option_rules(args):
        if supplied and not allowed:
            parser.error(message)
    if args.limit is not None and not 1 <= args.limit <= 100:
        parser.error('--limit must be between 1 and 100')
    if args.offset is not None and args.offset < 0:
        parser.error('--offset must be nonnegative')
    if args.after_event is not None and args.after_event < 0:
        parser.error('--after-event must be nonnegative')
    if args.window is not None:
        try:
            parse_window(args.window)
        except (ValueError, TypeError):
            parser.error('--window requires an aligned five-minute ISO8601 time with timezone')
    args.limit = 20 if args.limit is None else args.limit
    args.offset = 0 if args.offset is None else args.offset
    args.after_event = 0 if args.after_event is None else args.after_event
    args.format = args.format or 'text'


def _option_rules(args):
    backfill = getattr(args, 'backfill', False)
    backfill_status = getattr(args, 'backfill_status', None) is not None
    paginated = (args.failures or args.history or args.recovery_plan
                 or args.inspect_instance is not None or backfill_status)
    return [
        (args.online, args.diagnose, '--online requires --diagnose'),
        (args.task is not None, args.failures or args.history or backfill,
         '--task requires --failures, --history or --backfill'),
        (args.limit is not None, paginated or args.dwd_status or args.events,
         '--limit requires a paginated diagnostic command'),
        (args.offset is not None, paginated, '--offset requires --failures, --history or --inspect-instance'),
        (args.window is not None, args.dwd_status, '--window requires --dwd-status'),
        (args.format is not None, is_diagnostic(args) or backfill or backfill_status,
         '--format requires a diagnostic or backfill command'),
        (args.format == 'jsonl', args.events, '--format jsonl requires --events'),
        (args.after_event is not None, args.events, '--after-event requires --events'),
        (args.verbose, is_diagnostic(args), '--verbose requires a diagnostic command'),
    ]


def run(args):
    from . import service
    from .recovery import execution_events, recovery_plan

    if args.plan_config:
        from data_converge.configuration_migration.cli import preview
        value = preview(args)
    elif args.diagnose:
        value = service.diagnose(args.config, online=args.online)
    elif args.failures:
        value = service.failures(args.config, task=args.task, limit=args.limit, offset=args.offset)
    elif args.history:
        value = service.history(args.config, task=args.task, limit=args.limit, offset=args.offset)
    elif args.recovery_plan:
        value = recovery_plan(args.config, limit=args.limit, offset=args.offset)
    elif args.events:
        value = execution_events(args.config, after_event=args.after_event, limit=args.limit)
    elif args.inspect_instance is not None:
        value = service.inspect_instance(args.config, args.inspect_instance, limit=args.limit, offset=args.offset)
    else:
        value = service.dwd_status(args.config, limit=args.limit, window=args.window)
    _print_result(value, args.format, verbose=args.verbose)
    return 1 if value['status'] == 'FAIL' else 0


def _print_result(value, output_format, *, verbose=False):
    if output_format == 'jsonl':
        if value['status'] == 'FAIL':
            print(json.dumps(value, ensure_ascii=False), file=sys.stderr)
        else:
            for row in value['data']['events']:
                print(json.dumps(row, ensure_ascii=False))
        return
    print(json.dumps(value, ensure_ascii=False, default=str) if output_format == 'json' else render_text(value, verbose=verbose))
