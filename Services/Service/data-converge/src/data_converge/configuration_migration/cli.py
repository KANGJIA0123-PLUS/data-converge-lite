"""Explicit migration arguments and safe read-only plan presentation."""

from pathlib import Path

from data_converge.application import Application
from data_converge.diagnostics.checks import failure
from data_converge.diagnostics.report import check, report


def add_arguments(parser, modes):
    modes.add_argument('--plan-config', action='store_true', help='只读预览配置迁移、任务生效起点及确认项')
    modes.add_argument('--apply-config', metavar='PLAN_ID', help='按已预览计划独占迁移配置，备份并保留执行账本')
    parser.add_argument('--previous-config', type=Path, metavar='PATH',
                        help='迁移旧账本时提供可信旧配置及其原 SQL 资源路径')
    parser.add_argument('--effective-from', metavar='TIME', help='新增或变更任务的生效起点；带时区并对齐五分钟')
    parser.add_argument('--allow-backfill', action='store_true', help='显式允许生效起点位于过去，计划和应用均须提供')
    parser.add_argument('--confirm-same-data', action='store_true', help='应用迁移时确认连接仍对应同一业务数据')
    parser.add_argument('--confirm-readiness-change', action='store_true', help='应用迁移时确认 DWD 来源变化仍属于原数据')
    parser.add_argument('--accept-legacy-baseline', action='store_true', help='应用旧账本迁移时接受操作者提供的旧 SQL 基线')
    parser.add_argument('--backup-dir', type=Path, metavar='PATH', help='应用迁移前的一致性备份目录')


def validate_arguments(parser, args):
    migration = args.plan_config or args.apply_config is not None
    common = ('previous_config', 'effective_from', 'allow_backfill')
    applying = ('confirm_same_data', 'confirm_readiness_change', 'accept_legacy_baseline', 'backup_dir')
    for name in common + applying:
        supplied = getattr(args, name)
        allowed = migration if name in common else args.apply_config is not None
        if supplied is not None and supplied is not False and not allowed:
            required = '--plan-config or --apply-config' if name in common else '--apply-config'
            parser.error('--' + name.replace('_', '-') + ' requires ' + required)
    if args.apply_config is not None:
        value = args.apply_config
        if not value.strip() or any(ord(char) < 32 for char in value):
            parser.error('--apply-config requires a nonempty plan ID without control characters')


def _common_options(args):
    return dict(previous_config=args.previous_config, effective_from=args.effective_from,
                allow_backfill=args.allow_backfill)


def preview(args):
    from .service import preview_config

    try:
        value = preview_config(Application(args.config), **_common_options(args))
        status = 'FAIL' if value['blockers'] else 'WARN' if value['required_confirmations'] else 'PASS'
        evidence = {name: value[name] for name in (
            'plan_id', 'legacy', 'changes', 'required_confirmations', 'blockers',
            'task_starts', 'effective_from', 'allow_backfill', 'constraints') if name in value}
        result = check('configuration-plan', status, '只读配置迁移预览；未备份、未修改账本、未提交 SQL',
                       evidence, '核对任务起点和确认项；清除阻断后使用 --apply-config PLAN_ID 显式应用。')
        return report('plan-config', [result], value)
    except Exception as error:
        return report('plan-config', [failure('configuration-plan', error, '配置迁移预览未完成',
            '核对原账本和可信旧配置；旧账本需 --previous-config，任务变化需 --effective-from。')])


def apply(args, application):
    from .service import apply_config

    return apply_config(application, args.apply_config, **_common_options(args),
                        confirm_same_data=args.confirm_same_data,
                        confirm_readiness_change=args.confirm_readiness_change,
                        accept_legacy_baseline=args.accept_legacy_baseline, backup_dir=args.backup_dir)
