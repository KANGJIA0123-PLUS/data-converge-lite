"""Operator-facing command help, independent of configuration and service startup."""
import argparse
from pathlib import Path

from data_converge.config.application import DEFAULT_APP_CONFIG_PATH
from data_converge.diagnostics.report import LAUNCHER


DESCRIPTION = '''Data Converge Lite · 运行与故障定位

先按问题选择命令：
  不知道哪里出问题 / 服务启动不了  --diagnose
  怀疑 ClickHouse 或 Redis 连不上  --diagnose --online
  想找失败、结果未确认或阻断任务   --failures
  想查每次执行和人工处理的历史     --history
  想追查某个任务为什么异常         --inspect-instance '实例ID'
  想查哪些 DWD 信号没到            --dwd-status
  想看原始状态快照                 --status

以下查询在 data-converge 容器内执行，启动辅助脚本会加载 Python 环境。
查询命令不重跑任务、不修改任务状态，也不消费 Redis 消息。
'''

EXAMPLES = f'''可直接复制的查询示例：
  # 先做本地检查；需要验证解密、认证与连通性时再加 --online
  {LAUNCHER} --diagnose
  {LAUNCHER} --diagnose --online

  # 异常任务：按任务名筛选，或分页查看（offset 从 0 开始）
  {LAUNCHER} --failures
  {LAUNCHER} --failures --task task_resource_5min_ads
  {LAUNCHER} --failures --limit 20 --offset 20

  # 执行历史包含成功与失败；支持筛选、分页和 JSON 导出
  {LAUNCHER} --history
  {LAUNCHER} --history --task task_resource_5min_ads --limit 20 --offset 20
  {LAUNCHER} --history --format json

  # 实例ID从 --failures 或 --status 复制；下面的时间按现场情况替换
  {LAUNCHER} --inspect-instance 'task_resource_5min_ads@2026-10-14T16:30:00+08:00'
  {LAUNCHER} --inspect-instance 'task_resource_5min_ads@2026-10-14T16:30:00+08:00' --limit 20 --offset 20
  {LAUNCHER} --dwd-status --window '2026-10-14T16:15:00+08:00'

  # 导出便于回传的结构化结果
  {LAUNCHER} --diagnose --online --format json

结果怎么看：
  PASS=该项通过；WARN=需要关注；FAIL=异常或检查未完成；SKIP=未检查。
  新诊断命令退出码：0=无 FAIL（可能有 WARN/SKIP），1=有 FAIL，2=参数错误。
  --status 保持原 JSON；--format 仅用于诊断查询命令，默认 text。
  --history 的 PASS 表示查询成功，历史失败不当作当前告警；当前异常请看 --failures。
  --inspect-instance 默认附带最近20次执行；--limit / --offset 只控制历史分页。
  旧版未记录的执行历史显示 SKIP，不生成或追补；人工说明只展示是否记录，不展示正文。
  UNKNOWN=执行结果未确认：先用 query_id 核实 ClickHouse，不能直接重跑。
  --window 是窗口开始时间，必须带时区并对齐五分钟；输出使用北京时间。
  实例ID中的时间通常是窗口结束时间，与 --window 的开始时间不同。

会改变运行状态的操作：
  不带命令会启动服务；--run-once / --run-task 会真正执行任务与 SQL、写账本。
  --run-once 可能补跑多个任务，不是预览；所有辅助参数应随对应主命令使用。
  --init-state 会初始化状态；--resolve-instance 会人工修改实例状态。
  --resolution success 仅用于 UNKNOWN；retry 用于 UNKNOWN/FAILED，置为等待重试，非立即执行。
  人工写操作需要独占状态锁；服务运行时不要删除锁文件或状态库。
  正常服务启动仍用：bash /opt/its/data-converge/bin/start.sh

帮助不读取业务配置、不解密、不连接数据库；即使配置损坏也可以查看。
更完整的定位说明见仓库 docs/diagnostics.md。
'''


def create_parser():
    parser = argparse.ArgumentParser(
        prog='data-converge', usage='%(prog)s [命令] [选项]', add_help=False,
        description=DESCRIPTION, epilog=EXAMPLES, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-h', '--help', action='help', help='显示中文命令指南并退出')
    parser.add_argument('--config', type=Path, default=DEFAULT_APP_CONFIG_PATH,
                        metavar='PATH', help='应用配置文件；默认使用随服务发布的 resources/config/app_config.yaml')
    return parser


def add_operation_arguments(parser, modes):
    modes.add_argument('--validate-only', action='store_true', help='仅校验配置、SQL 文件与依赖，不执行任务')
    modes.add_argument('--init-state', action='store_true', help='初始化状态账本（写操作，不会重置已有覆盖起点）')
    modes.add_argument('--status', action='store_true', help='查看原 JSON 状态快照：累计计数、最近20条实例和信号窗口')
    modes.add_argument('--healthcheck', choices=['liveness', 'readiness'],
                       help='以退出码检查本地心跳/启动就绪，不代表实时远端连通性')
    modes.add_argument('--run-once', action='store_true', help='运行一次调度，会消费信号并执行符合条件的 SQL')
    modes.add_argument('--run-task', metavar='TASK', help='执行指定任务，必须同时指定 --scheduled-time')
    modes.add_argument('--resolve-instance', metavar='ID', help='人工修改实例状态，必须指定 --resolution 和 --note')
    parser.add_argument('--start-time', metavar='TIME', help='首次覆盖起点，仅配合 --init-state；带时区并对齐五分钟')
    parser.add_argument('--scheduled-time', metavar='TIME', help='--run-task 的调度时间，必须为带时区的 ISO8601 时间')
    parser.add_argument('--now', metavar='TIME', help='仅配合 --run-once，指定评估时间，必须带时区')
    parser.add_argument('--resolution', choices=['success', 'retry'],
                        help='配合 --resolve-instance：确认成功，或将实例置为等待重试；先核实远端结果')
    parser.add_argument('--note', metavar='TEXT', help='配合 --resolve-instance，记录人工处理依据，不能为空')
