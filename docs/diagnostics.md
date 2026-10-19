# 运行定位与诊断命令

诊断入口沿用现有启动辅助脚本，在 **data-converge 容器内**执行。脚本负责加载平台 `python.env`、业务代码、依赖目录及动态库路径，不需要上传脚本或单独拼接 `PYTHONPATH`。公司正式服务入口仍为 `bash /opt/its/data-converge/bin/start.sh`。

更新到包含诊断模块的镜像后使用以下命令。现有 `--status` 输出协议不变；新增命令默认中文文本，追加 `--format json` 输出同一份结构化结果。

## 常用命令

镜像提供 `dconv` 命令，避免与系统计算器 `dc` 重名。同步更新镜像和 Chart 并重建 Pod 后，容器级 `PATH` 包含服务 `bin`，新终端可直接使用，无需设置 alias、加载环境脚本或每次填写配置路径：

```bash
dconv --history             # 最近20条执行历史
dconv --history --limit 50   # 最近50条
dconv --diagnose            # 本地健康检查
dconv --diagnose --online   # 包含远端连接检查
dconv --failures            # 当前异常任务
dconv --dwd-status          # 上游窗口就绪状态
dconv                       # 显示帮助，不启动服务
```

标准部署默认配置为 `/opt/its/data-converge/lib/data-converge/resources/config/app_config.yaml`。
显式 `--config` 优先于环境变量 `APP_CONF_FILE`，未设置时使用 `${SERVICE_HOME}/lib/data-converge/resources/config/app_config.yaml`；`SERVICE_HOME` 默认 `/opt/its/data-converge`。
例如 `dconv --config /其他路径/app_config.yaml --history` 可以查询其他配置。
除无参数时显示帮助外，`dconv` 原样转发参数并返回原命令退出码，原有写操作仍是写操作。

Chart 的 `env_setting_data_converge.conf` 设置完整的容器级路径：

```text
PATH;value;/opt/its/data-converge/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin;;
```

该配置覆盖镜像原有的 `PATH`，不是动态追加；如部署依赖其他平台命令目录，需显式补入此值。仅更新镜像不会更新旧 Pod 的环境变量，需同时发布 Chart 并重建 Pod。`dconv` 的启动辅助脚本仍自动加载平台 `python.env`，无需在此固定值中写入具体 Python 版本路径。

容器外查询不再依赖登录 Bash，可直接执行：

```bash
kubectl exec <Pod名> -- dconv --history
```

`start.sh` 保留 Bash 环境注册作为兼容措施，用于 Chart 未生效或 Bash 启动文件重置路径的场景。在平台环境与命令权限准备完成后，它调用 `register_command.sh`，向当前 `$HOME/.bashrc` 及 Bash 首个现有可读登录文件（依次为 `.bash_profile`、`.bash_login`、`.profile`）追加仅供 Bash 使用的加载项；没有登录文件时创建 `.profile`。原文件内容保留，重复启动不会重复注册。

`command_env.sh` 只将自身所在的 `bin` 目录追加到原 `PATH` 末尾，保留已有目录及其查找顺序；重复加载不会重复追加。加载它不会启动服务、写入启动文件，也不会改变当前目录或 Shell 选项。需要手动补充当前 Bash 环境时，执行 `source /opt/its/data-converge/bin/command_env.sh`；单独运行 `bash command_env.sh` 不会修改当前终端环境。

如果 `$HOME` 缺失或不可写，或待更新的启动文件不可读、不可写、不是当前账号拥有的普通文件（包括符号链接），注册会拒绝写入，并在 `start.log` 记录 `COMMAND_ENV_REGISTRATION_FAILED`，服务仍继续启动。只要容器级 `PATH` 已生效，注册失败不影响直接使用 `dconv`；否则可手动加载环境脚本，或使用完整路径。

可用 `command -v dconv` 确认输出为 `/opt/its/data-converge/bin/dconv`。`dconv` 随镜像安装为可执行文件，正式 `start.sh` 启动时将其权限设为服务账号只读、可执行；调试部署跳过正式启动入口时需确认镜像中该文件已有执行权限。

也可以不加载命令环境，直接使用完整路径：

```bash
bash /opt/its/data-converge/bin/dconv --history
```

旧镜像或需要使用原始入口时，下面的完整入口命令继续可用。

不熟悉参数时，直接查看内置中文帮助，包含按问题选择的入口、可复制的命令、时间格式和退出码说明：

```bash
bash /opt/its/data-converge/bin/start_data_converge.sh --help
```

`-h` 与 `--help` 等效。帮助在加载业务配置、日志和远端连接前退出；配置文件缺失时也能查看，但辅助脚本仍需可用的平台 Python 环境。

先看哪里异常，本地检查不连接 Redis 或 ClickHouse：

```bash
bash /opt/its/data-converge/bin/start_data_converge.sh --diagnose
```

需要验证解密、认证和远端连通性时显式启用在线检查：

```bash
bash /opt/its/data-converge/bin/start_data_converge.sh --diagnose --online
```

只列异常任务，优先显示 UNKNOWN，其次 FAILED、BLOCKED；查询全历史异常，不会被最近的成功任务挤出：

```bash
bash /opt/its/data-converge/bin/start_data_converge.sh --failures
```

筛选资源汇聚任务，或翻到下一页：

```bash
bash /opt/its/data-converge/bin/start_data_converge.sh --failures --task task_resource_5min_ads --limit 20
bash /opt/its/data-converge/bin/start_data_converge.sh --failures --limit 20 --offset 20
```

查看一个实例。下面使用现场曾出现的实例 ID；查询其他窗口时从 `--failures` 复制完整 ID：

```bash
bash /opt/its/data-converge/bin/start_data_converge.sh --inspect-instance 'task_resource_5min_ads@2026-10-14T16:30:00+08:00'
```

查看逐次执行历史。重试成功后，之前的失败证据仍可查询；无需在日志文件中搜索：

```bash
bash /opt/its/data-converge/bin/start_data_converge.sh --history --limit 20
bash /opt/its/data-converge/bin/start_data_converge.sh --history --task task_resource_5min_ads --limit 20 --offset 20
```

查看最近完整窗口的信号，或精确定位 16:15–16:20 窗口：

```bash
bash /opt/its/data-converge/bin/start_data_converge.sh --dwd-status
bash /opt/its/data-converge/bin/start_data_converge.sh --dwd-status --window '2026-10-14T16:15:00+08:00'
```

导出可回传的诊断结果：

```bash
bash /opt/its/data-converge/bin/start_data_converge.sh --diagnose --online --format json
```

`--limit` 范围为 1–100，默认 20；`--offset` 从 0 开始，供 `--failures`、`--history` 及 `--inspect-instance` 的历史分页使用。`--window` 必须为带时区、对齐五分钟的窗口**开始时间**，也接受 UTC 输入，展示时转换为北京时间。`--online` 仅用于 `--diagnose`；错误组合及空实例 ID 会拒绝执行，不会启动服务。

## 每个入口能回答什么

| 入口 | 证据和范围 |
|---|---|
| `--diagnose` | 配置可读性、DAG/SQL文件与调度规则、Python解释器与SDK可发现性、凭据/CA文件权限、日志老化配置及日志路径、本实例心跳、状态库及选主绑定、历史异常计数、最近20个完整DWD窗口 |
| `--diagnose --online` | 追加ClickHouse凭据解析与固定`SELECT 1`；Redis凭据/PSM客户端创建与`PING`、两个队列的`TYPE`/`LLEN`；启用选主时只读观察租约和永久账本注册项 |
| `--failures` | 全历史异常总数、分页结果、任务/窗口、尝试次数、query_id、固定错误码及处理建议 |
| `--history` | 按 query_id 保存的历次执行、真实结果、安全错误证据及人工处置；可筛选任务和分页 |
| `--inspect-instance` | 单个实例、当前及历次尝试的安全错误证据、SQL文件路径、当前未成功依赖、对应五分钟DWD状态；依赖最多显示100条并标明截断 |
| `--dwd-status` | 每个窗口的已收到/缺失类型、等待时长、最后接收时间、收齐延迟；包括一条信号也没收到的完整窗口 |

`--inspect-instance` 比较当前配置与状态库绑定，再按已记录的调度时间重建依赖。配置不一致时不猜测历史依赖；配置、依赖或附加错误证据损坏，也保留能够读取的实例基本信息。

任务的 `retryable` 只是原执行策略中的标记，仍需结合最大尝试次数及 `next_retry_at` 判断，不代表命令会发起重试。UNKNOWN 表示结果未确认，必须先凭 `query_id` 核实 ClickHouse 结果，再按原人工解决流程处理，不能直接重跑。

DWD `READY` 表示本消费者已记录五类信号；`WAITING` 表示缺信号；`BEFORE_COVERAGE` 和 `WINDOW_OPEN` 分别表示早于账本覆盖起点、窗口尚未结束。等待时长从窗口结束算起，收齐后停止累计，另用 `ready_delay_seconds` 保留收齐延迟。默认只看最近20个完整窗口，旧窗口用 `--window` 精确查询；更早的已登记阻断任务可用 `--failures` 查到。

## 启动时的状态库绑定错误

`STATE_DEPLOYMENT_BINDING_CHANGED_OR_UNINITIALIZED` 表示启动未通过账本校验：当前配置指纹与账本中记录的不一致，或缺少绑定/覆盖起点信息。它发生在本次任务恢复和 SQL 执行之前。用同一配置运行 `--diagnose --format json`，`state` 项会区分“当前配置与状态库绑定不一致”和“状态库未完成初始化”。

新建或已迁移账本保存完整配置快照及 SQL 摘要，变化时报告 `CONFIG_MIGRATION_REQUIRED`。`CONFIGURATION_REJECTED` 日志给出前后版本摘要和新增、变化、移除的任务名，不输出连接身份原值。使用 `--plan-config` 查看差异，按 [配置迁移指南](configuration-migration.md) 备份并显式应用；不要删库或直接改指纹。`CONFIG_STATE_INVALID` 则表示配置快照、任务起点或初始化信息不完整，不能作为正常升级直接放行。

### 名词与职责

| 名词 | 含义 |
|---|---|
| ClickHouse | 保存原始指标和汇聚结果的业务数据库；例如某台服务器五分钟的统计结果。 |
| 状态库 / 账本 | 本地 SQLite 文件 `state.sqlite3`，保存服务的执行进度：任务实例、尝试记录、调度游标、就绪信号、配置绑定、覆盖起点等。它不是业务汇聚结果的存放位置，也不是可随意删除的缓存。 |
| 任务定义 | “如何汇聚”的规则，包含来源表、目标表、时间间隔、调度规则和上游依赖。例如每五分钟执行一次资源汇聚。 |
| 任务实例 / 时间窗口 | 一个任务在某个具体时间段的执行。例如“资源汇聚任务处理 09:25–09:30”。56 个任务定义会随时间生成很多实例，因此成功实例数可以远大于 56；实例数不是业务数据行数。 |
| `binding` / 配置指纹 | 对选定的部署配置计算 SHA-256 摘要，形成 64 位十六进制标识，用来检查当前程序配置是否仍与账本对应；它不是密码，也不保存完整旧配置。 |
| `coverage_start` / 覆盖起点 | 本部署允许处理的最早数据窗口起点；不是状态库创建时间、最后成功时间或最近启动时间。 |
| 主节点 / 租约 | 多副本运行时，通过 Redis 取得有期限的执行资格，并持续续租；拿到租约后仍需验证本地账本、绑定账本身份及完成应用启动。 |
| 心跳 | 进程定期更新的存活、就绪信息。应用退出后可能没有心跳；心跳缺失本身不能说明是哪一步失败。 |
| DWD 就绪信号 | 上游发出的明细数据就绪通知；本服务记录同一五分钟窗口所需的五类信号，再决定是否允许相关任务执行。缺信号不等于已经证明 ClickHouse 中没有数据。 |

账本的 `SUCCESS` 表示实例已记录成功；`UNKNOWN` 表示无法确认远端执行结果，例如 SQL 可能已经写入，但连接中断导致结果未返回；`BLOCKED` 表示暂不满足执行条件，例如上游未成功或 DWD 信号未收齐。不能把 UNKNOWN 无条件重跑，也不能仅凭总数认定所有 BLOCKED 都由 UNKNOWN 导致。

### 如何解读启动顺序

1. 公司启动脚本拉起后台进程后输出 `Start service success`，此时应用检查可能尚未完成，不能据此判定服务就绪。
2. `LEADERSHIP_ACQUIRED` 仅表示已获得 Redis 租约，不证明 ClickHouse 连接正常或调度已开始。
3. 应用读取状态库，比较部署指纹并检查覆盖起点；校验失败就退出，不进入本次任务恢复和调度。
4. 校验通过后，继续写入初始化标记、关联选主与账本身份、恢复任务状态，随后建立运行资源并启动调度。

因此，`ledger_bound=false` 表示账本中没有 `leadership_cluster` 记录，与“已拿到 Redis 租约”可以同时出现。`connections=SKIP` 表示本次诊断没有进行在线连接检查。

### 新增绑定日志

应用在校验拒绝时输出 `STATE_BINDING_REJECTED diagnostic={...}`，包含配置路径、状态库路径、当前任务数量、旧/新指纹以及以下原因列表：

| `reasons` 内容 | 含义 |
|---|---|
| `BINDING_MISMATCH` | 账本中的非空绑定与当前配置指纹不同。 |
| `MISSING_BINDING` | 账本中没有绑定信息，或内容为空。 |
| `MISSING_COVERAGE_START` | 没有覆盖起点，或内容为空；可与其他原因同时出现。 |

`stored_binding_present` 和 `coverage_start_present` 表示相应元数据是否非空。`stored_binding` / `current_binding` 仅输出格式合法的 64 位十六进制摘要；非法或缺失时输出 `null`，不输出原始异常内容。路径按 JSON 转义。日志不输出完整配置、连接账号、密码、SQL 或状态库中任意文本；也不为记录日志扫描全部任务。

`STATE_BINDING_VALIDATED` 在指纹匹配且覆盖起点解析通过后输出 INFO，表示这一项校验通过，不代表整个服务已经就绪。失败仍保留原 `SERVICE_FAILED` 和异常码，便于原有监控继续识别；只读诊断命令不会因此新增业务日志写入。

指纹纳入 ClickHouse 地址、数据库、账号、任务定义（描述除外）、DWD 开关，以及启用时的 DWD 队列/部分 Redis 配置。增加任务也会改变指纹。例如，旧账本绑定 52 个任务，升级后配置增加为 56 个任务，会发生不匹配；仅知道当前为 56 个任务，不能证明旧账本一定来自 52 个任务。SQL 文件正文、日志配置不直接纳入该指纹。

旧账本只保存整体摘要，因此日志能确定失败分支，无法还原具体变化字段。应比较升级前后的配置并核对状态卷归属：若配错部署，恢复正确配置；若是正常业务升级，需要受控迁移。不要通过删库、重置覆盖起点或直接覆盖指纹绕过检查，已有成功记录和不确定结果仍需保留。

最近 20 个完整 DWD 窗口约覆盖 100 分钟。全部超时表示本地账本在这些窗口没有记录完整就绪信号，不是“服务停了 100 分钟”的证明。`latest_finished_at` 是所有任务实例的最大结束时间，没有限定 SUCCESS，也不能当作停机时刻。

## 输出与退出码

每项检查固定包含 `id`、`status`、`summary`、`evidence`、`action`。JSON顶层包含 `schema_version=1`、命令、检查时间、汇总状态和数据。

- `PASS`：该项检查通过，仅代表其声明的检查范围。
- `WARN`：存在需要关注的阻断/信号等待/配置提示。
- `FAIL`：已知错误、结果未确认的任务，或检查无法完成。
- `SKIP`：未启用、未显式请求，或前置条件不足，不能解释为正常。

退出码 `0` 表示没有 FAIL（可能包含 WARN/SKIP），`1` 表示存在 FAIL，`2` 表示参数错误。自动化可同时读取JSON中的各项状态。正常等待阈值内的窗口也可能是 WAITING，不能把整个命令 PASS 解读为全部窗口READY。

## 诊断边界与兼容

诊断业务代码不创建或修复状态库、不获取写锁、不创建心跳、不写业务日志、不改任务状态，也不消费或确认 Redis 消息。账本通过只读连接和单次读事务查看；Redis的两个队列读数为各自采样，不保证原子快照。辅助脚本仍沿用公司既有的环境准备流程，依赖尚未展开时可能先准备依赖文件。

在线检查复用原解密及认证实现，查询阶段沿用10秒ClickHouse超时和Redis配置的socket超时；解密使用原KMS超时。系统DNS阻塞仍受操作系统解析行为影响，不承诺整个命令固定10秒结束。Redis检查的 `credential_user` 表示选中的PMS账号，不等于服务端ACL用户名。

错误证据仅保存固定错误码、异常类型、query_id、HTTP状态、数字ClickHouse异常码、TLS校验码、OS错误号，不打印密码、SDK异常正文、服务端SQL或Redis消息内容。`execution_diagnostics` 保留与当前实例 query_id 匹配的当前失败证据；`execution_attempts` 在派发、finish 和恢复事务中保存逐次尝试，旧 claim 接口仍兼容，重试不覆盖旧记录。人工处置单独入账，不改变原尝试的真实结果，输出不包含备注正文。旧账本中没有尝试行的人工处理在 `legacy_resolutions` 单独展示，各自使用同一组分页参数；无法补造旧执行过程。诊断只读不建表，无历史表时显示“旧版本未记录”。

## 恢复预览与事务事件

```bash
bash /opt/its/data-converge/bin/start_data_converge.sh --recovery-plan --format json
bash /opt/its/data-converge/bin/start_data_converge.sh --recovery-plan --limit 20 --offset 20
bash /opt/its/data-converge/bin/start_data_converge.sh --events --after-event 0 --limit 100 --format json
bash /opt/its/data-converge/bin/start_data_converge.sh --events --after-event 0 --limit 100 --format jsonl > execution-page.jsonl
```

`AUTO_RESUME` 表示当前 RUNNING 实例具有匹配 query_id 的 PREPARED 证明。下次合法写进程启动时会重新排队；本次查询不会恢复任务。`RECONCILE` 表示已派发、旧版无证明或已有 UNKNOWN，先核对远端执行结果和窗口数据，确认原查询结束后才考虑人工处置。服务仍运行时，快照里的任务可能随后完成，不能将预览结果当作卡死判断。

事件每页最多 100 条，JSON 包装包含 `next_after_event` 和 `has_more`；继续读取时传入该游标。JSONL 每行是一条事件，不带分页包装；用最后一行的 `event_id` 续读，直到返回空页。JSONL 查询失败时 stdout 为空，错误报告写 stderr，退出码为 1，避免污染采集文件。旧账本尚无事件表时 JSON 显示 SKIP，JSONL 返回空页，不追补历史。

事件字段固定为 `event_id`、`schema_version`、`occurred_at`（UTC）、`event_type`、`instance_id`、`taskname`、`query_id`、`attempt_number`、`status`。事件序号只在同一账本内有效；跨部署采集应同时保存部署/账本来源。导出是可重复生成的投影，消费者按来源和事件序号去重。记录不含 SQL、异常原文或人工备注。

状态和事件同事务提交；事件落盘失败会使对应状态转换回滚。磁盘不可用时宁可停止派发，不把未保存的动作伪装成已完成。文件日志依旧由原日志组件轮转，事务事件和尝试历史暂不自动清理，应监控状态卷容量。

本地心跳通过只代表进程心跳与启动就绪，不能证明调度线程持续推进；诊断同时展示最近完成时间、最新窗口与最早活动窗口供判断。当前不新增调度线程探针、不改公司现有探针脚本。成功执行或`SELECT 1`通过不证明汇聚结果行数正确，也不验证所有现场表结构。原有资源汇聚SQL保持不变。

## 设计依据与实现分工

参考成熟项目的可操作设计：

- [Django CheckMessage 源码](https://github.com/django/django/blob/stable/5.2.x/django/core/checks/messages.py)：稳定检查标识、严重度、问题说明与修复提示。
- [Django check 命令源码](https://github.com/django/django/blob/stable/5.2.x/django/core/management/commands/check.py)：显式选择数据库检查及失败级别。
- [Kubernetes Pod 调试指南](https://kubernetes.io/docs/tasks/debug/debug-application/debug-running-pod/)：将配置、状态和故障原因放在一起查看。
- [Airflow 健康检查](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/check-health.html)：分别判断元数据库、进程心跳及组件状态。

本实现按现有服务规模采用命令分派、独立检查函数、统一结果与文本/JSON呈现，复用现有连接适配器；没有引入Django、Airflow或新的依赖。

```mermaid
flowchart LR
  CLI[诊断命令] --> S[只读诊断用例]
  S --> L[本地配置 / 文件 / 心跳]
  S --> DB[SQLite 只读快照]
  S -->|显式 online| C[既有 ClickHouse / Redis 连接]
  L --> R[统一检查结果]
  DB --> R
  C --> R
  R --> T[中文文本]
  R --> J[JSON]
```

`diagnostics/cli.py`负责参数与分派；`service.py`组合检查；`checks.py`执行独立本地检查；`ledger.py`限制只读账本查询；`windows.py`解释就绪窗口；`connections.py`复用既有连接；`report.py`提供统一呈现。错误证据在`errors.py`白名单过滤，经执行器传入状态库，与任务完成一同提交。
