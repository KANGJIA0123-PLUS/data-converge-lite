# Data Converge Lite

基于 ClickHouse 的汇聚服务：接收 DWD 就绪信号，按任务周期和同周期依赖执行现有 60 个汇聚任务，并在本地账本记录执行结果。沿用现有业务 SQL、命令入口和公司字节码构建方式。

小时、日、月等跨周期任务默认开启限时等待：到 Cron 计划时间后，上游未齐最多额外等 5 分钟，提前齐全即可执行，超时按已有数据汇聚并记录缺失信息。`scheduler.cross_period_wait_enabled` 可关闭等待，时长可统一配置或按任务覆盖。同周期依赖仍须成功，五分钟 DWD 就绪条件继续生效。迟到数据不自动重算已成功窗口，可用 `--backfill --recompute` 修正；测试当前未结束的小时、整天或当月可用 `--backfill --preaggregate`，写入相同目标表，正常到期任务再清理并覆盖。当月支持跨覆盖起点和恢复切点，正式覆盖安排持久保存，重启后继续处理。具体边界及命令见 [在线补算说明](docs/backfill.md)。

2026-10-20 指标扩展包含四个新增模型服务任务，部署前需同步数据库结构和权限，见[指标扩展发布说明](docs/metrics-diff-release.md)。

恢复以持久化派发边界为依据：准备完成但未派发的实例可自动继续；可能已提交的实例保留 UNKNOWN，先核对再处理。执行事件与业务状态同事务保存，可只读预览恢复计划、按序号导出 JSONL。设计、验证和业务价值说明见 [架构](docs/architecture.md)、[故障演练证据](docs/evidence/recovery-verification-2026-10-18.md) 和 [参赛说明](docs/competition.md)。

## 目录与职责

```text
Services/Service/data-converge/
  src/data_converge/
    main.py                 # CLI、输出和退出码边界
    application.py          # 校验、初始化、查询、执行等应用用例
    runtime.py              # 工厂装配与运行资源生命周期
    config/                 # 配置校验、任务配置和 Cron 编译
    dag/                    # 任务模型、依赖图、执行和重试判断
    scheduler/service.py    # 时间窗口与调度
    dwd_ready/              # 就绪门控与 Redis 队列消费
    coordination/           # 写入准入、租约接口与执行守卫
    leader_election/        # 原子选主与续租生命周期
    infrastructure/redis/   # 独占连接池、平台认证、凭据与 PSM
    sql_execution/
      config.py             # ClickHouse 配置与凭据解析入口
      transport.py          # HTTP/TLS、连接与请求截止时间
      client.py             # SQL 请求与执行结果分类
      runner.py             # 将任务实例适配为 SQL 执行
    state/
      store.py              # 业务状态转换与事务
      database.py           # SQLite 连接、文件锁与首次启动
      history.py            # 与业务事务一起提交的执行历史
      checkpoints.py        # 准备、派发及恢复分类
      events.py             # 事务内事件与有序导出
    observability/
      settings.py           # 不可变日志配置
      retention.py          # 轮转与保留策略
      handlers.py           # 文件写入、进程锁与归档
      logging.py            # 日志路径和 LoggingSession 生命周期
  resources/                # 业务配置与 SQL，位置不变
  test/                     # 保留原测试位置，由构建脚本排除出镜像
```

`Application` 作为用例门面；运行时工厂负责装配，`ExitStack` 管理资源释放；SQL/Redis 适配器连接外部系统；`StateStore` 管理业务事务，组合 `LedgerDatabase` 管理连接与文件锁；日志策略与文件操作分别维护。旧的 `logging_config`、`scheduler.scheduler`、DWD 认证模块和 ClickHouse `client` 配置函数保留兼容导出。

任务执行、执行状态和就绪消息源通过 `Protocol` 定义契约；运行依赖可通过 `RuntimeDependencies` 显式注入。Redis 租约协调主备，本地文件锁控制同一物理账本的写入；历史连续性依赖完整持久状态卷及其存储保障。选主配置、逐次执行历史及能力边界见 [六项可靠性问题核对](docs/reliability.md)；扩展约定见 [业务架构](docs/architecture.md)。

## 启动与状态

公司启动入口保持 `bash /opt/its/data-converge/bin/start.sh`。业务首次启动时，如果状态目录没有账本、初始化标记或其他残留，会自动初始化 SQLite，日志记录 `STATE_INITIALIZED: coverage_start=...`。覆盖起点为当前时间向上取整到五分钟边界；之前窗口不在新账本的处理范围。已有账本会校验并复用，保留覆盖起点和任务结果。

服务启动调度前，在配置的 ClickHouse 业务库上执行只读 `SELECT 1`。名称解析失败、连接拒绝/中断/超时、TLS 意外 EOF、业务库尚不存在（ClickHouse Code 81）及临时 HTTP 错误会在进程内退避重试。默认等待最多 600 秒，间隔为 2、4、8、16、30 秒，之后保持 30 秒；每次查询最多 10 秒并受剩余总预算限制。可通过应用配置调整：

```yaml
startup:
  timeout_seconds: 600
  initial_retry_delay_seconds: 2
  max_retry_delay_seconds: 30
  probe_timeout_seconds: 10
```

上述值须为正有限数值，初始间隔不能大于最大间隔；未配置时使用默认值。等待期持续更新存活心跳，应用 readiness 为 false，不启动调度和归档；成功记录 `CLICKHOUSE_STARTUP_READY`，开始汇聚。每次重试记录 `CLICKHOUSE_STARTUP_WAIT` 及安全错误字段；超过预算记录 `CLICKHOUSE_STARTUP_TIMEOUT` 并失败退出。认证、权限、证书校验、其他明确 SQL 错误及配置错误立即失败。SIGTERM/SIGINT 或失去写入租约会停止等待并释放资源；退避期间每 0.2 秒检查一次，在途请求受查询超时约束，系统阻塞的 DNS 解析仍受平台 `getaddrinfo` 限制。

该策略只作用于正常服务启动的只读检查，不修改业务写入的重试/UNKNOWN 语义，也不自动创建数据库或表；安装流程仍需完成建库建表。它不会等待 Redis、凭据解密或其他初始化步骤。当前仓库的 `liveness_check.sh`、`readiness_check.sh` 仍保留原有直接 `exit 0`，因此上述 readiness 指应用心跳和 CLI 健康检查结果，部署脚本本身未在本次更改。

应用按运行进程的系统时区解释本地日期、Cron 日程、汇聚时间桶、诊断展示和每日状态备份。容器以自身的 `TZ` 或系统时区文件为准，不会自动继承宿主机时区；部署时可设置 `TZ=Asia/Shanghai`、`TZ=UTC` 或其他 IANA 时区名称，并确保镜像包含对应时区数据。有效时区在进程首次读取后固定，修改 `TZ` 或系统时区后需重启服务。`scheduler.timezone` 不再作为 YAML 配置项；内部 `SchedulerSettings.timezone` 只读返回有效系统时区。

若时区配置缺失、无效或读取失败，应用回退到 `Asia/Shanghai`，并记录一次 `SYSTEM_TIMEZONE_FALLBACK` 告警；显式配置的 UTC 仍使用 UTC。回退结果同样固定到进程结束，诊断中的 `timezone` 显示最终采用的时区。

带偏移的命令时间会保留同一个实际时刻，再转换到系统时区。五分钟窗口和等待时长按实际经过时间计算；小时窗口取相邻实际存在的本地整点，半小时夏令时切换时允许出现 90 分钟窗口；日、月边界按系统本地日历计算，缺失午夜顺延到当天首个实际时刻。SQL 使用调度器传入的绝对起止时间过滤，聚合结果的时间桶直接使用窗口起点，避免数据库重新取整产生不同边界。DWD 就绪消息的既有编码及时间语义保持不变；日志归档与状态备份文件名仍可采用 UTC。当前处于调测阶段，本轮不迁移旧账本或重写旧时间记录。

SQL 的 `start_time`、`end_time` 参数是 Unix 秒级时间戳。窗口筛选统一使用 `toDateTime({start_time:UInt32}, {system_timezone:String})` 和对应的 `end_time` 表达式，与输出时间桶的时区写法一致，范围仍为左闭右开。这里的时区参数来自应用进程；对同一个数字时间戳，指定 UTC 或应用时区表示的实际时刻相同，不会给窗口加减时差。显式传入时区可避免依赖 ClickHouse 的默认时区。速率公式中的 `dateDiff('second', ...)` 保留 UTC 写法，用于计算窗口实际经过的秒数。

初始化标记保存在状态目录的父目录，默认与 `lite-state/` 同级。2026-10-20 按调测要求，手动删除账本或状态子目录后，正常启动取得本地锁后可重新初始化，保留旧标记也不再以 `STATE_DATABASE_MISSING` 阻断。新账本默认从当前时刻向上对齐的五分钟边界开始，原成功记录、UNKNOWN、游标和积压不会恢复，也不会自动补算；ClickHouse 中已有数据仍然保留。已有数据库损坏、结构不完整或必要元数据缺失时仍拒绝执行，不会自动重建。需要指定首次覆盖起点时，仍可在首次服务启动前使用 `--init-state --start-time`，它不能重置已有覆盖起点。临时策略及后续处理见 [DC-STATE-002](docs/known-issues.md#dc-state-002)。

标记与账本都在本地存储中。整个 hostPath 丢失、Pod 调度到另一个空节点目录，或旧版本在写入标记前已经丢失账本时，无法仅凭空目录区别首次部署与状态丢失。正式迁移需保证账本存储连续；自动初始化不提供跨节点状态恢复。

按现场要求，已临时移除本地 `leadership_cluster` / `leadership_ledger_id` 与 Redis 永久 `:ledger` 注册项的身份阻断；旧元数据与注册键保留，新版本不再读写这些身份项。部署绑定、当前配置及 SQL 与已有有效快照不一致时也临时告警放行，原绑定、快照及执行历史保留。临时选主租约、续租、失租停止、本地锁，以及覆盖起点、快照和历史隔离结构的完整性检查继续保留。放行不会恢复丢失历史，也不保证独立状态卷接管不重复写入。遗留问题、后续方案和验收条件见 [DC-STATE-001](docs/known-issues.md#dc-state-001) 和 [DC-STATE-002](docs/known-issues.md#dc-state-002)。

## 快速定位

短命令使用 `dconv`，避免与系统计算器 `dc` 重名。同步更新镜像和 Chart 并重建 Pod 后，Chart 将服务 `bin` 纳入容器级 `PATH`，新终端可直接查询，无需手动加载环境脚本：

```bash
dconv --history
dconv --diagnose --online
dconv --failures
dconv --dwd-status
```

从容器外也可直接执行 `kubectl exec <Pod名> -- dconv --history`。Chart 设置完整的 `PATH` 值 `/opt/its/data-converge/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin`，会覆盖镜像中的原值；如部署还依赖其他平台命令目录，需显式补入。命令入口仍自动加载平台 `python.env`。

`start.sh` 保留为当前业务账号注册 Bash 命令环境的兼容措施，在 Chart 未生效或 Bash 重置路径时补充服务目录；原启动文件内容和路径顺序保留，重复加载不会重复追加。注册失败会在 `start.log` 记录 `COMMAND_ENV_REGISTRATION_FAILED`，服务仍继续启动；只要容器级 `PATH` 已生效，仍可直接使用 `dconv`。需要手动补充当前 Bash 环境时，可执行 `source /opt/its/data-converge/bin/command_env.sh`。

`dconv` 默认使用 `/opt/its/data-converge/lib/data-converge/resources/config/app_config.yaml`，支持 `--config` 覆盖；单独执行 `dconv` 显示帮助。无需加载命令环境时，可直接执行 `bash /opt/its/data-converge/bin/dconv --history`。完整入口 `bash /opt/its/data-converge/bin/start_data_converge.sh` 继续可用。

提供 `--diagnose`、`--diagnose --online`、`--failures`、`--history`、`--inspect-instance`、`--dwd-status`，默认中文文本，支持 `--format json`；原 `--status` 保持兼容。完整容器命令、错误证据及只读边界见 [运行定位指南](docs/diagnostics.md)。

新增 `--recovery-plan` 只读查看恢复分类；`--events --after-event 0 --limit 100 --format jsonl` 导出首批事务事件，使用最后一条 `event_id` 继续分页。服务运行日志继续沿用原轮转策略；事件原始记录在账本中，不依赖日志文件保存成功。

同一业务数据的配置升级需要明确版本与生效起点时，可使用 `--plan-config` 预览、`--apply-config PLAN_ID` 显式应用。任务定义和 SQL 正文均纳入迁移版本检查；普通迁移中的新增或变更任务须指定生效起点。迁移先备份，再事务保存配置与审计，保留已有执行事实。首次迁移旧账本需要可信的旧配置、规则和 SQL；具体命令及本轮临时放行边界见 [配置迁移指南](docs/configuration-migration.md)。

## 日志

业务日志优先写入 `${LOG_PATH}/${SERVICE_NAME}.log`，同时保留 stderr 输出。没有 `LOG_PATH` 时兼容部署注入的完整文件名 `SERVICE_LOG_PATH`，再按 `PRODUCT_NAME` 拼接平台路径。本地没有平台日志变量时只输出到 stderr。公司启动日志 `start.log` 继续由平台管理。

在 `resources/config/app_config.yaml` 中配置，修改后重启服务：

```yaml
logging:
  max_bytes: 10485760
  backup_count: 50
  max_total_bytes: 524288000
```

默认单文件 10 MiB、最多 50 个历史压缩包、总大小不超过 500 MiB。达到大小上限时轮转并 gzip；仅生成新压缩包时触发老化清理。归档名称包含 UTC 时间和唯一标识，以 `.archive` 结尾。

启动、轮转后及写入时每隔 60 秒检查老化；无日志写入期间不运行后台清理线程，下一次启动或写入会继续清理。数量和总大小上限可能使历史提前淘汰。只清理本服务生成的归档，不删除其他服务、平台轮转文件或符号链接。

固定的 `.lock` 文件用于 POSIX 进程锁，协调服务与手工 CLI 的追加、轮转和清理，运行中不要删除。兼容平台重命名日志后的重新打开；应用归档与平台归档分别由各自策略管理。

## 构建与验证

公司 `Services/build_framework/scripts/build.sh` 保留编译业务代码为 `.pyc`、删除业务 `.py`、复制到 `Services/Package/images/lib/data-converge/` 的流程。测试保留在服务的 `test/` 目录，跳过编译和源码删除；复制后清理镜像中的 `test/`、`tests/`、`__pycache__/` 和 `.pytest_cache/`，包括此前构建的残留。`src/`、`resources/`、`python3 -m data_converge.main` 和 XMS 解密子进程入口不变，无需为核心代码制作 wheel，也没有新增第三方依赖。

在已装好项目依赖的 Python 3.12 环境中，从仓库根执行：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD/Services/Service/data-converge/src" python3 -m pytest -c /dev/null -p no:cacheprovider Services/Service/data-converge/test -q
```

CI 在 Ubuntu runner、Python 3.12、`TZ=Asia/Shanghai` 下使用固定测试依赖（pytest 9.2.0、PyYAML 6.0.2、redis 5.2.1、APScheduler 3.11.0、Flask 3.1.3、fakeredis 2.40.0、lupa 2.8），保留当前已知的 50 个失败/错误节点，新增失败会阻断流水线。基线文件位于 `tools/test-baseline.txt`，门禁脚本会强制使用该时区，本地可直接运行：

```bash
python3 tools/test_baseline_gate.py
```

基线项修复后，门禁会将其列为 `RESOLVED_BASELINE_FAILURES`；更新基线前应确认失败节点已稳定修复或经过明确评审。

打包测试会在临时副本运行自定义构建脚本，检查字节码入口、全部 SQL、资源内容、测试源码保留和镜像测试排除，并在仓库外运行校验与解密子进程。完整镜像组装仍需要公司的外部 BuildTool 和基础镜像。
