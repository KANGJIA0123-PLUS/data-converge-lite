"""Operator-facing command help, independent of configuration and service startup."""
import argparse
import sys
from pathlib import Path

from data_converge.config.application import DEFAULT_APP_CONFIG_PATH


DESCRIPTION = '''Data Converge Lite 运维命令 dconv

排查问题，按顺序走：
  1. dconv --diagnose               总体体检：配置、心跳、状态库、任务计数
  2. dconv --failures               当前 UNKNOWN / FAILED / BLOCKED 的实例
  3. dconv --inspect-instance ID    单个实例：错误证据、上游依赖、执行历史
  4. dconv --dwd-status             上游 DWD 信号是否到齐（区分"我们"还是"上游"）

其他常用查询：
  dconv --diagnose --online         再测 ClickHouse / Redis 连通性与选主租约
  dconv --history --task 任务名      历史执行（含成功），查间歇性问题
  dconv --recovery-plan             主备切换或重启后：哪些自动续跑、哪些要人工核对
  dconv --backfill-status 任务号     补算进度（跳批次自动补算的任务号见日志 BATCH_SKIP_RESUME_QUEUED）

所有查询都只读：不跑 SQL、不改状态、不消费 Redis 消息。
输出：默认按表格显示，FAIL/WARN 自动展开证据；-v 展开全部；--format json 给脚本用。
退出码：0=无 FAIL，1=有 FAIL，2=参数错误。
'''

EXAMPLES = '''示例（时间按现场替换；ID 可从 --failures 输出直接复制）：
  dconv --failures --task task_gpu_5min_to_hour_dws
  dconv --inspect-instance 'task_gpu_realtime_to_5min_dws@2026-11-05T14:40:00+08:00'
  dconv --dwd-status --window '2026-11-05T14:35:00+08:00'
  dconv --history --task task_gpu_5min_to_hour_dws --limit 50
  dconv --diagnose --online --format json > diagnose.json     # 回传给开发

  注意两个时间口径：实例 ID 里是窗口【结束】时间；--window 是窗口【开始】时间。
  分页：--limit（默认20，最大100）与 --offset；输出末尾会提示下一页命令。

修复操作（会改状态，先核实再执行）：
  UNKNOWN 已在 ClickHouse 按 query_id 确认成功：
    dconv --resolve-instance ID --resolution success --note '依据'
  FAILED/UNKNOWN 允许重新执行（只置为待重试，由服务按计划执行）：
    dconv --resolve-instance ID --resolution retry --note '依据'
  上游迟到数据已补齐，重新汇聚某段时间（先 --dry-run 预览，去掉后提交）：
    dconv --backfill --task 任务名 --from '2026-11-05T14:00:00+08:00' --to '2026-11-05T15:00:00+08:00' --recompute --dry-run
    （--all-tasks 代替 --task 选全部任务；区间 [from,to) 须带时区并对齐窗口；默认跳过已成功窗口）
  镜像冲突（--diagnose 中 mirror 冲突）核对后处置：
    dconv --resolve-mirror-conflict ID --resolution resolved|ignored --note '依据' [--actor 姓名]
  启用 Redis 选主且服务正在运行时，上述写命令可能返回 LEADERSHIP_BUSY；补算不受影响。
  人工写操作需要独占状态锁；服务运行时不要删除锁文件或状态库。

配置变更（非排查用途）：
  dconv --plan-config --effective-from '2099-01-01T00:00:00+08:00'      只读预览，输出 PLAN_ID
  dconv --apply-config PLAN_ID --effective-from '...' [确认参数]          备份后事务迁移，不执行 SQL
    确认参数按预览提示添加：--confirm-same-data / --confirm-readiness-change /
    --accept-legacy-baseline / --allow-backfill；旧账本首次迁移需 --previous-config
  应用成功后先 --diagnose 再启动服务。

调度配置速查（app_config.yaml 的 scheduler 段）：
  task_timeout_seconds         单任务超时，按周期 5min/hour/day/month，默认 120/600/1800/1800 秒
  task_parallelism             同一任务同时执行的批次数，1-3，默认 1
  cross_period_wait_*          跨周期等上游，默认最多再等 300 秒；超时按已有数据执行并告警
  skip_batches / skip_mode     停机过久时跳到当前批次（默认关闭）：
    current_only               只跑当前批次，日志 BATCH_SKIPPED 给出对应补算命令
    current_then_resume        跑完当前批次后自动后台补跑，最多回溯 skip_max_resume_days 天
  skip_threshold_batches       落后多少批次触发，默认 5min=12 hour=3 day=2 month=2

其他与调试（一般不用于排查）：
  --status                原始 JSON 快照（兼容旧脚本；排查请用 --diagnose / --failures）
  --events                执行事件导出，供程序对账：--events --after-event 0 --format jsonl
  --validate-only         仅校验配置与 SQL（--diagnose 已包含）
  --healthcheck MODE      K8s 探针专用，只看退出码
  --init-state            首次部署初始化覆盖起点
  --run-once / --run-task 立即执行 SQL 并写账本，仅调试环境使用；重跑窗口请用 --backfill
  --preaggregate          补算时提前测试未结束窗口；正式到期会重新覆盖，提前成功不代表正式完成
  --resolve-preaggregate  核查提前汇聚结果：dconv --resolve-preaggregate ITEM_ID --resolution success|retry --note '依据'

环境：在 data-converge 容器内直接运行 dconv；容器外用 kubectl exec <Pod名> -- dconv --failures。
找不到命令时：bash /opt/its/data-converge/bin/dconv --failures，或 source /opt/its/data-converge/bin/command_env.sh。
默认配置 /opt/its/data-converge/lib/data-converge/resources/config/app_config.yaml，可用 --config 覆盖。
dconv 不带参数只显示本帮助；正式启动服务用 bash /opt/its/data-converge/bin/start.sh。
本帮助不读配置、不解密、不连库，配置损坏时也能查看。详见 docs/diagnostics.md。
'''


# Option sections for --help only; parsing, mutual exclusion and validation are unchanged.
OPTION_SECTIONS = (
    ('排查查询（只读）', ('--diagnose', '--failures', '--inspect-instance', '--dwd-status', '--history',
                     '--recovery-plan', '--backfill-status')),
    ('查询选项', ('--online', '--task', '--window', '--limit', '--offset', '--format', '--verbose')),
    ('修复操作（会改状态）', ('--resolve-instance', '--resolve-mirror-conflict', '--resolution', '--note',
                       '--actor', '--backfill', '--all-tasks', '--from', '--to', '--recompute',
                       '--dry-run', '--request-id')),
    ('配置变更', ('--plan-config', '--apply-config', '--previous-config', '--effective-from',
              '--allow-backfill', '--confirm-same-data', '--confirm-readiness-change',
              '--accept-legacy-baseline', '--backup-dir')),
    ('其他与调试', ('--status', '--events', '--after-event', '--validate-only', '--healthcheck',
               '--init-state', '--start-time', '--run-once', '--now', '--run-task',
               '--scheduled-time', '--preaggregate', '--resolve-preaggregate')),
    ('通用', ('--config', '--help')),
)


class SectionedHelpFormatter(argparse.RawDescriptionHelpFormatter):
    """Print options grouped by operator purpose instead of registration order."""

    def __init__(self, prog):
        super().__init__(prog, max_help_position=30, width=110)

    def add_arguments(self, actions):
        actions = list(actions)
        if self._current_section.heading != 'options':
            return super().add_arguments(actions)
        by_flag = {flag: action for action in actions for flag in action.option_strings}
        placed = set()
        for title, flags in OPTION_SECTIONS:
            group = [by_flag[flag] for flag in flags if flag in by_flag and by_flag[flag] not in placed]
            if not group:
                continue
            placed.update(group)
            self.end_section()
            self.start_section(title)
            super().add_arguments(group)
        rest = [action for action in actions if action not in placed]
        if rest:
            self.end_section()
            self.start_section('其他')
            super().add_arguments(rest)
        return None


class OperatorArgumentParser(argparse.ArgumentParser):
    """Keep argparse's English error (stable for scripts), then add a short Chinese hint."""

    def error(self, message):
        self.print_usage(sys.stderr)
        hint = next((text for key, text in ERROR_HINTS if key in message), '')
        self.exit(2, f"dconv: error: {message}\n" + (f"  提示：{hint}\n" if hint else '')
                  + "  完整用法：dconv --help\n")


ERROR_HINTS = (
    ('not allowed with argument', '每次只能使用一个主命令，例如 --failures 和 --history 不能同时出现。'),
    ('--window', '--window 填窗口开始时间，须带时区并对齐五分钟，例如 2026-11-05T14:35:00+08:00。'),
    ('--online requires', '--online 只能配合 --diagnose：dconv --diagnose --online。'),
    ('--limit', '--limit 取 1-100，且只能配合 --failures/--history/--recovery-plan/--events 等查询。'),
    ('--offset', '--offset 为非负整数，配合 --failures/--history/--inspect-instance/--recovery-plan。'),
    ('--task requires', '--task 只能配合 --failures、--history 或 --backfill。'),
    ('resolution', '实例用 --resolution success|retry，镜像冲突用 resolved|ignored，并且必须带 --note。'),
    ('--backfill requires exactly one', '补算需要 --task 任务名 或 --all-tasks 二选一。'),
    ('--from and --to', '时间须带时区，例如 2026-11-05T14:00:00+08:00。'),
    ('requires --backfill', '这些参数只能配合 --backfill 使用。'),
    ('--run-task requires', '--run-task 需要 --scheduled-time 带时区的时间。'),
    ('invalid choice', '取值不在允许范围内，见上方 choose from。'),
)


def create_parser():
    parser = OperatorArgumentParser(
        prog='dconv', usage='dconv [命令] [选项]    （不带参数显示本帮助）', add_help=False,
        description=DESCRIPTION, epilog=EXAMPLES, formatter_class=SectionedHelpFormatter)
    parser.add_argument('-h', '--help', action='help', help='显示中文命令指南并退出')
    parser.add_argument('--config', type=Path, default=DEFAULT_APP_CONFIG_PATH,
                        metavar='PATH', help='应用配置文件；默认使用随服务发布的 resources/config/app_config.yaml')
    return parser


def add_operation_arguments(parser, modes):
    modes.add_argument('--validate-only', action='store_true', help='仅校验配置、SQL 文件与依赖，不执行任务')
    modes.add_argument('--init-state', action='store_true', help='初始化状态账本（写操作，不会重置已有覆盖起点）')
    modes.add_argument('--status', action='store_true', help='查看原 JSON 状态快照：累计计数、最近20条实例和信号窗口')
    modes.add_argument('--healthcheck', choices=['liveness', 'readiness'], metavar='liveness|readiness',
                       help='以退出码检查本地心跳/启动就绪，不代表实时远端连通性')
    modes.add_argument('--run-once', action='store_true', help='运行一次调度，会消费信号并执行符合条件的 SQL')
    modes.add_argument('--run-task', metavar='TASK', help='执行指定任务，必须同时指定 --scheduled-time')
    modes.add_argument('--resolve-instance', metavar='ID', help='人工修改实例状态，必须指定 --resolution 和 --note')
    modes.add_argument('--resolve-mirror-conflict', metavar='ID',
                       help='处置镜像冲突，--resolution 使用 resolved 或 ignored，并指定 --note')
    modes.add_argument('--resolve-preaggregate', metavar='ITEM_ID',
                       help='核查提前执行的未知结果，不改变正式成功状态；需 --resolution 和 --note')
    parser.add_argument('--start-time', metavar='TIME', help='首次覆盖起点，仅配合 --init-state；带时区并对齐五分钟')
    parser.add_argument('--scheduled-time', metavar='TIME', help='--run-task 的调度时间，必须为带时区的 ISO8601 时间')
    parser.add_argument('--now', metavar='TIME', help='仅配合 --run-once，指定评估时间，必须带时区')
    parser.add_argument('--resolution', choices=['success', 'retry', 'resolved', 'ignored'], metavar='VALUE',
                        help='实例: success|retry；镜像冲突: resolved|ignored；先核实远端结果')
    parser.add_argument('--note', metavar='TEXT', help='配合人工核查命令记录处理依据，不能为空')
    parser.add_argument('--actor', metavar='NAME', help='镜像冲突处置人；仅配合 --resolve-mirror-conflict')
