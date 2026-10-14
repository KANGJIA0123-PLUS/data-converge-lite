"""Diagnostic CLI options cannot fall through into service startup."""
import json

from .report import render_text
from .windows import parse_window


def add_arguments(parser, modes):
    modes.add_argument('--diagnose', action='store_true', help='只读检查配置、状态库、心跳及异常')
    modes.add_argument('--failures', action='store_true', help='查询 UNKNOWN / FAILED / BLOCKED 实例')
    modes.add_argument('--history', action='store_true', help='只读查询各次执行结果与人工处理记录')
    modes.add_argument('--inspect-instance', metavar='ID', help='查看一个实例的错误证据和当前依赖')
    modes.add_argument('--dwd-status', action='store_true', help='查询五分钟窗口的缺失信号')
    parser.add_argument('--online', action='store_true', help='仅与 --diagnose 使用，显式检查远端连接')
    parser.add_argument('--task', help='与 --failures / --history 使用，按完整任务名筛选')
    parser.add_argument('--limit', type=int, help='查询条数或实例历史条数，默认20，最大100')
    parser.add_argument('--offset', type=int, help='--failures / --history / --inspect-instance 分页偏移，默认0')
    parser.add_argument('--window', help='仅与 --dwd-status 使用，含时区的五分钟窗口开始时间')
    parser.add_argument('--format', choices=('text', 'json'), help='新诊断命令的输出格式，默认text')


def is_diagnostic(args):
    return bool(args.diagnose or args.failures or args.history or args.inspect_instance is not None or args.dwd_status)


def validate_arguments(parser, args):
    for value, option in ((args.inspect_instance, '--inspect-instance'), (args.task, '--task')):
        if value is not None and (not value.strip() or any(ord(char) < 32 for char in value)):
            parser.error(option + ' requires a nonempty value without control characters')
    paginated = args.failures or args.history or args.inspect_instance is not None
    rules = [
        (args.online, args.diagnose, '--online requires --diagnose'),
        (args.task is not None, args.failures or args.history, '--task requires --failures or --history'),
        (args.limit is not None, paginated or args.dwd_status,
         '--limit requires --failures, --history, --inspect-instance or --dwd-status'),
        (args.offset is not None, paginated, '--offset requires --failures, --history or --inspect-instance'),
        (args.window is not None, args.dwd_status, '--window requires --dwd-status'),
        (args.format is not None, is_diagnostic(args), '--format requires a diagnostic command'),
    ]
    for supplied, allowed, message in rules:
        if supplied and not allowed:
            parser.error(message)
    if args.limit is not None and not 1 <= args.limit <= 100:
        parser.error('--limit must be between 1 and 100')
    if args.offset is not None and args.offset < 0:
        parser.error('--offset must be nonnegative')
    if args.window is not None:
        try:
            parse_window(args.window)
        except (ValueError, TypeError):
            parser.error('--window requires an aligned five-minute ISO8601 time with timezone')
    args.limit = 20 if args.limit is None else args.limit
    args.offset = 0 if args.offset is None else args.offset
    args.format = args.format or 'text'


def run(args):
    from . import service

    if args.diagnose:
        value = service.diagnose(args.config, online=args.online)
    elif args.failures:
        value = service.failures(args.config, task=args.task, limit=args.limit, offset=args.offset)
    elif args.history:
        value = service.history(args.config, task=args.task, limit=args.limit, offset=args.offset)
    elif args.inspect_instance is not None:
        value = service.inspect_instance(args.config, args.inspect_instance, limit=args.limit, offset=args.offset)
    else:
        value = service.dwd_status(args.config, limit=args.limit, window=args.window)
    print(json.dumps(value, ensure_ascii=False, default=str) if args.format == 'json' else render_text(value))
    return 1 if value['status'] == 'FAIL' else 0
