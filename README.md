# Data Converge Lite

基于 ClickHouse 的汇聚服务：接收 DWD 就绪信号，按任务依赖执行现有 56 个汇聚任务，并在本地账本记录执行结果。业务流程、命令入口和公司字节码构建方式保持兼容。

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

任务执行、执行状态和就绪消息源通过 `Protocol` 定义契约；运行依赖可通过 `RuntimeDependencies` 显式注入。Redis 租约协调主备，本地文件锁和同一持久账本保证单写与状态连续。选主配置、逐次执行历史及能力边界见 [六项可靠性问题核对](docs/reliability.md)；扩展约定见 [业务架构](docs/architecture.md)。

## 启动与状态

公司启动入口保持 `bash /opt/its/data-converge/bin/start.sh`。业务首次启动时，如果状态目录没有账本、初始化标记或其他残留，会自动初始化 SQLite，日志记录 `STATE_INITIALIZED: coverage_start=...`。覆盖起点为当前时间向上取整到五分钟边界；之前窗口不在新账本的处理范围。已有账本会校验并复用，保留覆盖起点和任务结果。

初始化标记保存在状态目录的父目录，默认与 `lite-state/` 同级。删除账本或状态子目录但保留标记时，启动会报 `STATE_DATABASE_MISSING`；数据库损坏、不完整或部署绑定不匹配时也不会自动重建。不要通过删除标记绕过恢复检查。需要指定首次覆盖起点时，仍可在首次服务启动前使用 `--init-state --start-time`，它不能重置已有覆盖起点。

标记与账本都在本地存储中。整个 hostPath 丢失、Pod 调度到另一个空节点目录，或旧版本在写入标记前已经丢失账本时，无法仅凭空目录区别首次部署与状态丢失。正式迁移需保证账本存储连续；自动初始化不提供跨节点状态恢复。

## 快速定位

短命令使用 `dconv`，避免与系统计算器 `dc` 重名。更新镜像后，每次通过 `kubectl exec` 打开新的 Bash 终端，先在当前终端加载命令环境，再查询：

```bash
source /opt/its/data-converge/bin/command_env.sh
dconv --history
dconv --diagnose --online
dconv --failures
dconv --dwd-status
```

环境脚本只把自身所在的 `bin` 目录追加到原 `PATH` 末尾，重复加载不会重复追加，也不会启动服务。正式 `start.sh` 在平台 `function.sh` 之后加载它，但该启动进程导出的环境不会传给新开的 `kubectl exec` 终端。Chart 不设置固定 `PATH`；此前部署过固定 `PATH` 的环境，需要发布移除该项的 Chart 并重建 Pod，才能恢复容器原有路径。

`dconv` 默认使用 `/opt/its/data-converge/lib/data-converge/resources/config/app_config.yaml`，支持 `--config` 覆盖；单独执行 `dconv` 显示帮助。无需加载命令环境时，可直接执行 `bash /opt/its/data-converge/bin/dconv --history`。完整入口 `bash /opt/its/data-converge/bin/start_data_converge.sh` 继续可用。

提供 `--diagnose`、`--diagnose --online`、`--failures`、`--history`、`--inspect-instance`、`--dwd-status`，默认中文文本，支持 `--format json`；原 `--status` 保持兼容。完整容器命令、错误证据及只读边界见 [运行定位指南](docs/diagnostics.md)。

新增 `--recovery-plan` 只读查看恢复分类；`--events --after-event 0 --limit 100 --format jsonl` 导出首批事务事件，使用最后一条 `event_id` 继续分页。服务运行日志继续沿用原轮转策略；事件原始记录在账本中，不依赖日志文件保存成功。

同一业务数据的配置升级使用 `--plan-config` 预览、`--apply-config PLAN_ID` 显式应用。任务定义和 SQL 正文均纳入版本检查；新增或变更任务须指定生效起点。迁移先备份，再事务保存配置与审计，保留已有执行事实。首次迁移旧账本需要可信的旧配置、规则和 SQL；具体命令及边界见 [配置迁移指南](docs/configuration-migration.md)。

## 日志

业务日志优先写入 `${LOG_PATH}/${SERVICE_NAME}.log`，同时保留 stderr 输出。没有 `LOG_PATH` 时兼容部署注入的完整文件名 `SERVICE_LOG_PATH`，再按 `PRODUCT_NAME` 拼接平台路径。本地没有平台日志变量时只输出到 stderr。公司启动日志 `start.log` 继续由平台管理。

在 `resources/config/app_config.yaml` 中配置，修改后重启服务：

```yaml
logging:
  max_bytes: 20971520
  backup_count: 10
  retention_days: 7
```

默认单文件 20 MiB、最多 10 个历史文件、历史保留 7 天。达到大小上限前或跨 UTC 日期后的首次写入触发归档；单条日志超过上限时仍完整保留，下一条写入前轮转。归档名称包含 UTC 时间和唯一标识，以 `.archive` 结尾。

启动、轮转后及写入时每隔 60 秒检查老化，按归档最后写入时间清理；无日志写入期间不运行后台清理线程，下一次启动或写入会继续清理。数量上限可能使繁忙时期不足 7 天的历史提前淘汰，7 天是保留上限。只清理本服务生成的归档，不删除其他服务、平台轮转文件或符号链接。

固定的 `.lock` 文件用于 POSIX 进程锁，协调服务与手工 CLI 的追加、轮转和清理，运行中不要删除。兼容平台重命名日志后的重新打开；应用归档与平台归档分别由各自策略管理。

## 构建与验证

公司 `Services/build_framework/scripts/build.sh` 保留编译业务代码为 `.pyc`、删除业务 `.py`、复制到 `Services/Package/images/lib/data-converge/` 的流程。测试保留在服务的 `test/` 目录，跳过编译和源码删除；复制后清理镜像中的 `test/`、`tests/`、`__pycache__/` 和 `.pytest_cache/`，包括此前构建的残留。`src/`、`resources/`、`python3 -m data_converge.main` 和 XMS 解密子进程入口不变，无需为核心代码制作 wheel，也没有新增第三方依赖。

在已装好项目依赖的 Python 3.12 环境中，从仓库根执行：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD/Services/Service/data-converge/src" python3 -m pytest -c /dev/null -p no:cacheprovider Services/Service/data-converge/test -q
```

打包测试会在临时副本运行自定义构建脚本，检查字节码入口、全部 SQL、资源内容、测试源码保留和镜像测试排除，并在仓库外运行校验与解密子进程。完整镜像组装仍需要公司的外部 BuildTool 和基础镜像。
