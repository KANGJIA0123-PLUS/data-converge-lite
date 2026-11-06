# 应用配置参考

本文逐项说明 `Services/Service/data-converge/resources/config/app_config.yaml` 中实际读取的配置键，包括含义、代码默认值、类型、必填性、取值范围和校验边界。默认值以校验代码为准，仓库随附的 `app_config.yaml` 若显式设置了不同值，会在“说明”中注明；两者冲突时按代码行为执行。

配置入口为 `dconv --config <路径>`，默认使用随服务发布的 `resources/config/app_config.yaml`。任务定义（`convergence_rules.yaml`）和 SQL 文件的字段不在本文范围内。

## 校验入口

| 配置段 | 主要校验代码 |
|---|---|
| 顶层 `clickhouse` | [sql_execution/config.py](../Services/Service/data-converge/src/data_converge/sql_execution/config.py)、[credentials.py](../Services/Service/data-converge/src/data_converge/sql_execution/credentials.py) |
| `logging` | [observability/settings.py](../Services/Service/data-converge/src/data_converge/observability/settings.py) |
| `clickhouse_pool` | [sql_execution/pool.py](../Services/Service/data-converge/src/data_converge/sql_execution/pool.py) |
| `startup` | [config/startup.py](../Services/Service/data-converge/src/data_converge/config/startup.py) |
| `state.directory`、`state.dataset_id` | [config/application.py](../Services/Service/data-converge/src/data_converge/config/application.py)、[config/evolution.py](../Services/Service/data-converge/src/data_converge/config/evolution.py) |
| `state.archives` | [archives/settings.py](../Services/Service/data-converge/src/data_converge/archives/settings.py) |
| `leadership` | [config/leadership.py](../Services/Service/data-converge/src/data_converge/config/leadership.py) |
| `dwd_ready` | [dwd_ready/config.py](../Services/Service/data-converge/src/data_converge/dwd_ready/config.py) |
| `redis_pools` | [infrastructure/redis/pools.py](../Services/Service/data-converge/src/data_converge/infrastructure/redis/pools.py) |
| `redis_state` | [state/redis_mirror.py](../Services/Service/data-converge/src/data_converge/state/redis_mirror.py) |
| `scheduler` | [config/application.py](../Services/Service/data-converge/src/data_converge/config/application.py) |
| `config` | [config/application.py](../Services/Service/data-converge/src/data_converge/config/application.py) |

## 通用规则

- **必填性**：顶层只有 `clickhouse` 必填；其余段均可省略并按代码默认值运行。各段内部仍可能有必填键（见下文）。
- **段结构**：各配置段若出现，必须是 YAML 映射（字典）；标量或列表会拒绝启动。`clickhouse` 还必须包含下述必填字段。
- **YAML 类型**：要求布尔值的键只接受 `true` / `false`，不接受字符串或 `0` / `1`；要求整数的键使用严格整数判断，布尔值不会被当作整数。
- **数值**：标为“数值”的键接受整数或浮点数，但必须是有限值（拒绝 `NaN` / 无穷）。`0` 是否合法按各键说明。
- **环境变量**：支持 `${NAME}` 形式展开；未定义或展开后仍含 `$` 会拒绝启动。`clickhouse.host` / `port` 与 DWD Redis 的连接字段支持该写法。
- **未知键**：不同配置段严格程度不同，见下表。被“忽略”的未知键不会生效，也不会报错。

| 配置段 | 未知键行为 |
|---|---|
| `logging`、`clickhouse_pool`、`startup`、`state.archives`、`leadership`、`dwd_ready`、`dwd_ready.redis`、`redis_pools`、`redis_state` | 拒绝启动，报告对应配置无效 |
| `clickhouse` | 未做白名单校验，额外键被忽略；但直接写 `password` 会拒绝，必须使用密码文件或环境变量 |
| `state`、`scheduler`、`config`、顶层其他键 | 未做白名单校验，额外键被忽略 |

## logging

日志轮转与保留策略。省略整段时使用代码默认值；日志路径由 `LOG_PATH`、`SERVICE_LOG_PATH`、`PRODUCT_NAME` 环境变量决定，不属于本节。

| 键 | 必填/默认值 | 类型与范围 | 说明 |
|---|---|---|---|
| `debug_enabled` | 可选；默认 `false` | 布尔值 | `false` 时失败 SQL、参数和原因按 ERROR 记录；`true` 时额外开启调用栈和阶段调试日志，并把 `data_converge` 日志级别设为 DEBUG。 |
| `max_bytes` | 可选；默认 `10485760`（10 MiB） | 整数，必须 `> 0`，无上限 | 当前文件非空且写入下一条记录会超过该值时轮转；单条记录本身可以超过上限并完整保留，因此实际文件可能略大于该值。 |
| `backup_count` | 可选；默认 `50` | 整数，必须 `> 0`，无上限 | 最多保留的归档数量（按修改时间从新到旧计数）。旧版本遗留的 `.archive` 与新的 `.gz` 一并计数。 |
| `max_total_bytes` | 可选；默认 `524288000`（500 MiB） | 整数，必须 `> 0`，无上限 | 全部归档文件的总大小上限。清理按修改时间从新到旧检查，数量或总量超限时删除对应归档；单个超大归档也可能被删除。 |

- 整段或单个键省略时使用默认值；出现未知键报告 `LOGGING_SETTING_INVALID`。
- 老化只在成功生成新压缩包时执行；无新压缩包时不做后台清理。详见 [README 日志一节](../README.md#日志)。

## clickhouse

ClickHouse HTTP 连接和凭据来源。`clickhouse` 段必填且必须是映射；`host`、`port`、`database`、`username` 必填。

| 键 | 必填/默认值 | 类型与范围 | 说明 |
|---|---|---|---|
| `host` | 必填 | 字符串 | 支持 `${NAME}` 展开。可写主机名/IP，也可带 `http://` 或 `https://` scheme；若带 scheme，必须与最终 `secure` 一致。禁止空白、控制字符、`?#\%`、URL 用户信息、路径、查询串和片段；主机名只允许 `[A-Za-z0-9_.:-]`。 |
| `port` | 必填 | 整数或纯数字字符串，`1`–`65535` | 支持 `${NAME}` 展开。若 `host` 中已带端口，必须与这里一致。 |
| `database` | 必填 | 非空字符串，不含控制字符 | 业务库名。 |
| `username` | 必填 | 非空字符串，不含控制字符和 `:` | ClickHouse 用户名。 |
| `password_file` | 二选一；示例使用该项 | 非空字符串路径 | XMS 密文密码文件。相对路径按配置文件所在目录解析；不存在时启动失败。与 `password_env` 必须且只能出现一个。 |
| `password_env` | 二选一 | 合法环境变量名 | 直接从这个环境变量读取明文密码。与 `password_file` 必须且只能出现一个。 |
| `paas_name` | 可选；默认 `clickhouse-pms-password` | 非空字符串 | PMS 中 ClickHouse 服务的名称，用于选择密码条目。 |
| `kms_timeout_seconds` | 可选；默认 `30` | 数值，`0 < x <= 300` | XMS 解密子进程的超时时间。 |
| `secure` | 可选；默认按 `host` 判断 | 布尔值 | `host` 带 scheme 时默认取该 scheme 是否为 `https`，否则默认 `false`；仓库示例显式设为 `true`。若带 scheme，必须与之一致。 |
| `verify` | 可选；默认 `true` | 布尔值 | 是否校验 ClickHouse TLS 证书。 |

- 凭据必须且只能使用 `password_file` 或 `password_env` 之一；直接配置 `password` 会报告 `CLICKHOUSE_PROVIDER_INVALID`。
- 环境变量缺失、URL 主机非法、端口越界、scheme 与 `secure` 不一致等都会在校验阶段拒绝，错误信息不包含密码内容。

## clickhouse_pool

汇聚任务专用 ClickHouse 连接池（导出查询使用自己的连接，不受本节影响）。整段可省略。

| 键 | 必填/默认值 | 类型与范围 | 说明 |
|---|---|---|---|
| `max_connections` | 可选；默认 `20` | 整数，`1`–`200` | 同时执行的汇聚查询上限（信号量）。实际并发还受 `scheduler.max_concurrent_tasks` 限制，取两者较小值。 |
| `connect_timeout_seconds` | 可选；默认 `10.0` | 数值，`0 < x <= 120` | 建立连接的截止时间。 |
| `settings` | 可选；默认见下 | 映射 | 合并到默认值后作为 ClickHouse URL 查询参数；出现未知设置项报告 `CLICKHOUSE_POOL_CONFIG_INVALID`。 |
| `settings.max_threads` | 可选；默认 `8` | 整数，`>= 0` | 单条汇聚 SQL 的内部并行线程数。 |
| `settings.readonly` | 可选；默认 `0` | 整数，`>= 0` | `0` 允许写入；应用只校验非负整数，具体含义由 ClickHouse 解释。 |
| `settings.load_balancing` | 可选；默认 `random` | 字母、数字、下划线组成的字符串 | 分发给 ClickHouse 集群的读请求均衡策略；应用不枚举具体值。 |
| `settings.max_memory_usage` | 可选；默认 `10000000000` | 整数，`>= 0` | 单条查询最大内存（字节），示例为 10 GB。 |

## startup

启动时对 ClickHouse 业务库执行只读 `SELECT 1` 的等待策略。整段可省略。

| 键 | 必填/默认值 | 类型与范围 | 说明 |
|---|---|---|---|
| `timeout_seconds` | 可选；默认 `600.0` | 有限数值，`> 0` | 启动探测总预算。超时后记录 `CLICKHOUSE_STARTUP_TIMEOUT` 并以失败退出。 |
| `initial_retry_delay_seconds` | 可选；默认 `2.0` | 有限数值，`> 0` | 第一次重试前的等待时间。必须不大于 `max_retry_delay_seconds`。 |
| `max_retry_delay_seconds` | 可选；默认 `30.0` | 有限数值，`> 0` | 退避等待上限；达到后保持该间隔重试。 |
| `probe_timeout_seconds` | 可选；默认 `10.0` | 有限数值，`> 0` | 单次 `SELECT 1` 的超时时间，同时受本段剩余总预算约束。 |

- 出现未知键报告 `STARTUP_SETTING_INVALID`；任一数值非有限或非正同样拒绝。
- 该策略只作用于正常服务启动，不改变业务写入的重试和 UNKNOWN 语义，也不修改 ClickHouse 数据库或表。

## state

本地状态目录、数据集标识及每日归档。`state` 段可省略；`directory` 省略时使用代码默认路径。

| 键 | 必填/默认值 | 类型与范围 | 说明 |
|---|---|---|---|
| `directory` | 可选；默认 `/srv/its/MindOpsBOSE/data-converge/data` | 非空字符串路径 | 支持环境变量和 `~` 展开；相对路径按配置文件所在目录解析。仓库示例显式设为 `/srv/its/MindOpsBOSE/data-converge/data/lite-state`，实际以显式值为准。 |
| `dataset_id` | 可选；默认 `null` | `null` 或匹配 `[A-Za-z0-9_.-]{1,128}` 的字符串 | 配置清单中的数据集标识；非法值报告 `DATASET_ID_INVALID`。 |
| `archives` | 可选；默认启用 | 映射，仅允许下列两个键 | 每日状态库一致性压缩备份；不影响活动账本中的记录。 |
| `archives.enabled` | 可选；默认 `true` | 布尔值 | 是否在正常服务后台执行每日归档。 |
| `archives.retention_days` | 可选；默认 `30` | 整数，`>= 1`，无上限 | 保留天数，按原快照生成时间计算；压缩或重启不重置期限。 |

- `state` 本身未做键白名单，额外键被忽略；`state.archives` 是严格映射，出现未知键报告 `ARCHIVE_SETTINGS_INVALID`。
- 归档只在正常服务启动检查完成后运行，不删除活动账本记录；细节见 [状态库每日压缩备份与老化](state-archives.md)。

## leadership

Redis 临时租约选主。省略整段时默认关闭；仓库示例显式启用。

| 键 | 必填/默认值 | 类型与范围 | 说明 |
|---|---|---|---|
| `enabled` | 可选；默认 `false` | 布尔值 | 是否启用选主。仓库示例为 `true`。 |
| `cluster_name` | 可选；默认 `data-converge-modelmate-metric` | 匹配 `[A-Za-z0-9_.:-]{1,100}` 的字符串 | 同一业务部署的所有副本必须一致；临时租约键为 `data_converge:leader:{cluster_name}`。 |
| `ttl_seconds` | 可选；默认 `30` | 整数，`1`–`300` | 租约有效期。Sentinel 的切换耗时必须小于该值，见[部署要求](reliability.md#部署要求)。 |
| `renew_interval_seconds` | 可选；默认 `5` | 整数，`1`–`300` | 后台续租间隔。必须满足 `renew_interval_seconds * 3 < ttl_seconds`。 |
| `standby_poll_seconds` | 可选；默认 `2` | 整数，`1`–`300` | 备机轮询/重新竞选前的等待时间。 |

- 出现未知键或类型不符报告 `LEADERSHIP_CONFIG_INVALID`。
- `enabled: true` 时必须同时启用 `dwd_ready`，且其 Redis `mode` 为 `standalone` 或 `sentinel`；原生 Redis Cluster 不受支持，错误码为 `LEADERSHIP_REQUIRES_STANDALONE_OR_SENTINEL`。
- 选主复用 `dwd_ready.redis` 的地址和认证，但使用独立连接池；配置关系见 [六项可靠性问题核对](reliability.md)。

## dwd_ready

DWD 就绪门控和 Redis 消息消费。省略整段时默认关闭。

| 键 | 必填/默认值 | 类型与范围 | 说明 |
|---|---|---|---|
| `enabled` | 可选；默认 `false` | 布尔值 | 是否启用 DWD 就绪门控。仓库示例为 `true`。启用时所有无依赖任务必须为 5 分钟周期，否则报告 `DWD_READY_REQUIRES_FIVE_MINUTE_ROOT_TASKS`。 |
| `queue_key` | 可选；默认 `modelverse:dwd:ready` | 非空字符串，不含空白/控制字符，长度 `<= 1024` | 就绪消息队列。若使用 `{...}` hash tag，队列与内部处理键必须落在同一 Redis slot，否则报告 `DWD_READY_QUEUE_SLOT_INVALID`。 |
| `block_seconds` | 可选；默认 `1` | 数值，`0 < x <= 2` | 阻塞读取的等待时间；Redis `socket_timeout` 必须严格大于该值。 |
| `batch_size` | 可选；默认 `100` | 整数，`1`–`1000` | 单次批量读取的消息数。 |
| `wait_warning_seconds` | 可选；默认 `60` | 有限数值，`> 0` | 等待上游未齐时输出告警的阈值。仓库示例显式设为 `120`。 |
| `redis` | `enabled: true` 时必填 | 映射 | Redis 连接和认证配置；`enabled: false` 时不读取也不校验。 |

Redis 公共字段（`mode` 为任意合法值时）：

| 键 | 必填/默认值 | 类型与范围 | 说明 |
|---|---|---|---|
| `mode` | 必填 | 枚举：`standalone`、`cluster`、`sentinel` | 连接模式。 |
| `db` | 可选；默认 `0` | 整数，`>= 0` | 数据库编号；`cluster` 模式必须为 `0`，否则报告 `DWD_READY_CLUSTER_REQUIRES_DB_ZERO`。 |
| `username` | 可选；`sentinel` 默认 `admin`，直连模式默认 `null` | 非空字符串（提供时） | Sentinel 模式下是 PMS 账号名，不作为 Redis ACL AUTH 用户名；直连模式下作为可选 ACL 用户名。 |
| `ssl` | 可选；默认 `true` | 布尔值 | 是否使用 TLS。 |
| `ssl_verify` | 可选；默认 `true` | 布尔值 | 是否校验证书。 |
| `socket_timeout` | 可选；默认 `3` | 数值，`0 < x <= 5` | 读写超时，必须严格大于 `block_seconds`，否则报告 `DWD_READY_SOCKET_TIMEOUT_TOO_SHORT`。 |
| `socket_connect_timeout` | 可选；默认 `2` | 数值，`0 < x <= 5` | 连接超时。 |

`sentinel` 模式追加字段：

| 键 | 必填/默认值 | 类型与范围 | 说明 |
|---|---|---|---|
| `service_name` | 必填 | 非空字符串 | Sentinel 主节点组名（不是原生 Redis Cluster 名）。 |
| `password_file` | 必填 | 非空路径字符串 | Redis 密文密码文件。相对路径按配置文件所在目录解析；支持 `${NAME}` 展开。 |
| `root_key_file` | 必填 | 非空路径字符串 | 根密钥文件；同样支持环境变量展开和相对路径解析。 |
| `work_key_file` | 必填 | 非空路径字符串 | 工作密钥文件；同样支持环境变量展开和相对路径解析。 |
| `paas_name` | 可选；默认 `null` | `null` 或非空字符串 | PMS 中 Redis 服务的准确名称；为 `null` 时要求账号唯一匹配。 |
| `kms_timeout_seconds` | 可选；默认 `30` | 数值，`0 < x <= 300` | XMS 解密超时时间。 |

`standalone` / `cluster` 模式追加字段：

| 键 | 必填/默认值 | 类型与范围 | 说明 |
|---|---|---|---|
| `host` | 必填 | 非空字符串 | 主机名或 IP；含 `:` 时按 IPv6 解析（可带方括号），否则只允许 `[A-Za-z0-9_.-]`。支持 `${NAME}` 展开。 |
| `port` | 必填 | 整数或纯数字字符串，`1`–`65535` | 支持 `${NAME}` 展开。 |
| `password_env` | 必填 | 合法环境变量名 | 从这个环境变量读取明文 Redis 密码。 |

- 字段集合按 `mode` 严格区分：Sentinel 不允许 `host`、`port`、`password_env`；直连模式不允许 `password_file`、`root_key_file`、`work_key_file` 等 Sentinel 字段。出现不适用于该模式的键会报告 `DWD_READY_CONFIG_INVALID`。
- `dwd_ready.enabled: false` 时只校验 `dwd_ready` 段的键名，不读取或校验 `redis` 内容。

## redis_pools

选主、状态读写、状态回写使用的三个独立 Redis 连接池，复用 `dwd_ready.redis` 的地址和认证，仅覆盖连接池参数。整段可省略，三个池都可单独省略。

| 池 | `max_connections` 默认/范围 | `socket_timeout` 默认/范围 | `socket_connect_timeout` 默认/范围 | 用途 |
|---|---|---|---|---|
| `leader` | `3`；`1`–`100` | `3.0`；`0 < x <= 60` | `2.0`；`0 < x <= 60` | 选主租约的获取、续租和释放。 |
| `status` | `15`；`1`–`100` | `5.0`；`0 < x <= 60` | `3.0`；`0 < x <= 60` | 共享状态读取和接管导入。 |
| `log` | `5`；`1`–`100` | `3.0`；`0 < x <= 60` | `3.0`；`0 < x <= 60` | 高频、轻量的共享状态回写。 |

- 出现未知池名、未知键或类型/范围不符报告 `REDIS_POOL_CONFIG_INVALID`。
- 注释建议 `status.max_connections` 至少为 `scheduler.max_concurrent_tasks + 2`，代码不强制校验该关系。
- 三个池各自拥有连接，避免慢读阻塞续租或状态写入；选主和共享状态均要求 DWD Redis 可用。

## redis_state

主备共享的任务状态、调度游标和 DWD 就绪事实。整段可省略；默认启用，但只有同时启用 `leadership` 和 `dwd_ready` 时才真正连接 Redis。

| 键 | 必填/默认值 | 类型与范围 | 说明 |
|---|---|---|---|
| `enabled` | 可选；默认 `true` | 布尔值 | 是否镜像共享状态。 |
| `ttl` | 可选；默认见下 | 映射，仅允许 `5min`、`hour`、`day`、`month` | 各类窗口的共享状态保留时长；只写部分周期时，未写周期保留默认值。 |
| `ttl.5min` | 可选；默认 `1` | 整数，`1`–`7` | 5 分钟窗口保留天数。 |
| `ttl.hour` | 可选；默认 `1` | 整数，`1`–`7` | 小时窗口保留天数。 |
| `ttl.day` | 可选；默认 `3` | 整数，`1`–`10` | 天窗口保留天数。 |
| `ttl.month` | 可选；默认 `3` | 整数，`1`–`12` | 月窗口保留月数（按 31 天/月计算）。 |

- 出现未知键或越界值报告 `REDIS_STATE_CONFIG_INVALID`。
- 保留期从窗口结束时间起算；游标和运行中集合不设 TTL。对已过保留期的迟到写入，至少再保留 6 小时，便于接管和依赖检查。
- 写入通过比较选主 token 的 Lua 脚本执行；失租的旧主写共享状态会被拒绝（`REDIS_STATE_FENCED`），这是共享状态层的 fencing，不是 ClickHouse 端幂等保证。

## scheduler

调度、超时、跨周期等待和跳批次策略。整段可省略。

| 键 | 必填/默认值 | 类型与范围 | 说明 |
|---|---|---|---|
| `max_concurrent_tasks` | 可选；默认 `4` | 整数，`> 0`，无上限 | 全局同时执行的任务实例上限。 |
| `task_parallelism` | 可选；默认 `1` | 整数，`1`–`3` | 同一任务同时执行的不同批次数量。 |
| `poll_interval_seconds` | 可选；默认 `5` | 整数，`> 0` | 调度轮询间隔。 |
| `catchup_limit` | 可选；默认 `100` | 整数，`> 0` | 每轮最多新派发/执行的实例数。 |
| `max_attempts` | 可选；默认 `3` | 整数，`> 0` | 全局重试的总尝试次数上限。 |
| `retry_delay_seconds` | 可选；默认 `30` | 整数，`> 0` | 全局重试间隔。配置校验不接受 `0`（即使执行器内部允许 `0`）。 |
| `task_timeout_seconds` | 可选；默认见下 | 映射，键为周期，值为整数秒 | 单条任务（清理 + 汇聚）的超时；未列出的周期保留默认值。未知周期键报告 `SCHEDULER_SETTING_INVALID`。 |
| `task_timeout_seconds.5min` | 可选；默认 `120` | 整数，`10`–`300` | 5 分钟任务超时；越界报告 `TASK_TIMEOUT_OUT_OF_RANGE`。 |
| `task_timeout_seconds.hour` | 可选；默认 `600` | 整数，`10`–`1800` | 小时任务超时。 |
| `task_timeout_seconds.day` | 可选；默认 `1800` | 整数，`10`–`3600` | 天任务超时；仓库示例为 `1800`。 |
| `task_timeout_seconds.month` | 可选；默认 `1800` | 整数，`10`–`3600` | 月任务超时；仓库示例为 `1800`。 |
| `default_timeout_seconds` | 可选；默认无 | 整数，`> 0` | 兼容旧配置：仅在未提供 `task_timeout_seconds` 时生效，统一应用到所有周期；由于还要满足各周期范围，实际只能取 `10`–`300`。 |
| `cross_period_wait_enabled` | 可选；默认 `true` | 布尔值 | 是否启用跨周期上游软等待。 |
| `cross_period_wait_seconds` | 可选；默认见下 | 非负整数，或 `hour`/`day`/`month` 到非负整数的映射 | 到 Cron 时间后额外等待的秒数；`0` 表示不等待。映射中出现其他周期键报告 `CROSS_PERIOD_WAIT_PERIOD_INVALID`。 |
| `cross_period_wait_seconds.hour` | 可选；默认 `300` | 整数，`>= 0` | 小时任务等待秒数。 |
| `cross_period_wait_seconds.day` | 可选；默认 `300` | 整数，`>= 0` | 天任务等待秒数。 |
| `cross_period_wait_seconds.month` | 可选；默认 `300` | 整数，`>= 0` | 月任务等待秒数。 |
| `cross_period_wait_overrides` | 可选；默认 `{}` | 任务名到非负整数的映射 | 按任务覆盖等待秒数；任务名必须是非空字符串（否则 `SCHEDULER_SETTING_INVALID`）且存在于当前 DAG（否则 `CROSS_PERIOD_WAIT_TASK_UNKNOWN`）。 |
| `skip_batches` | 可选；默认 `false` | 布尔值 | 是否启用长时间停机后的跳批次。 |
| `skip_mode` | 可选；默认 `current_only` | 枚举：`current_only`、`current_then_resume` | 跳过历史窗口后的处理方式；即使 `skip_batches` 为 `false` 也会校验该值。 |
| `skip_threshold_batches` | 可选；默认见下 | 周期到整数的映射，值必须 `>= 1` | 落后批次数超过阈值时触发跳批次。 |
| `skip_threshold_batches.5min` | 可选；默认 `12` | 整数，`>= 1` | 5 分钟周期阈值。 |
| `skip_threshold_batches.hour` | 可选；默认 `3` | 整数，`>= 1` | 小时周期阈值。 |
| `skip_threshold_batches.day` | 可选；默认 `2` | 整数，`>= 1` | 天周期阈值。 |
| `skip_threshold_batches.month` | 可选；默认 `2` | 整数，`>= 1` | 月周期阈值。 |
| `skip_max_resume_days` | 可选；默认 `31` | 整数，`1`–`31` | `current_then_resume` 最多回补的天数；更早历史只记录不补跑。 |

- 周期映射的键通常写 `5min`、`hour`、`day`、`month`；校验层也接受等价的 ISO 形式 `PT5M`、`PT1H`、`P1D`、`P1M`。`redis_state.ttl` 只接受短名称。
- `scheduler.timezone` 不是有效配置项：时区由容器 `TZ` 或系统时区决定，进程启动后固定。该键即使写入也会被忽略，不会生效。
- `scheduler` 下未列出的键会被忽略（不报错），这与 `leadership`、`startup` 等严格白名单段不同。
- 当前仓库示例的日/月超时为 `1800` 秒，部署终止宽限期必须不小于最长任务超时，见[部署要求](reliability.md#部署要求)。

## config

任务定义和 SQL 文件的路径。整段可省略。

| 键 | 必填/默认值 | 类型与范围 | 说明 |
|---|---|---|---|
| `convergence_rules_path` | 可选；默认随包 `resources/config/convergence_rules.yaml` | 非空字符串路径 | 以 `resources/` 开头时按随包资源解析；否则支持环境变量、`~` 展开，相对路径按配置文件所在目录解析。 |
| `sql_directory` | 可选；默认随包 `resources/sql` | 非空字符串路径 | 解析规则同上；SQL 文件必须存在且能读取。 |

- 两个路径均可省略；但解析后的规则文件和 SQL 文件必须存在，任务 DAG 才能构建成功。
- 规则文件中的任务字段由 [config/loader.py](../Services/Service/data-converge/src/data_converge/config/loader.py) 校验，Cron 表达式由 [config/cron.py](../Services/Service/data-converge/src/data_converge/config/cron.py) 编译；任务字段和 SQL 结构不在本文范围。
- `config` 段未做键白名单，额外键被忽略；路径为空、`$` 未展开或文件不可读会报告 `CONFIG_PATH_INVALID` / `CONFIG_PATH_ENV_MISSING` 等启动错误。

## 代码与注释/示例的差异

以下差异以校验代码为准；随附 `app_config.yaml` 的显式值会覆盖代码默认值。

| 项目 | 注释/示例 | 代码实际行为 |
|---|---|---|
| `dwd_ready.wait_warning_seconds` | 示例值 `120` | 省略时默认 `60`；示例覆盖为 `120`。 |
| `clickhouse.secure` | 示例显式 `true` | 省略时若 `host` 带 scheme 按 scheme 推导，否则默认 `false`；带 scheme 时仍须与 `secure` 一致。 |
| `state.directory` | 示例 `/srv/its/MindOpsBOSE/data-converge/data/lite-state` | 省略时默认 `/srv/its/MindOpsBOSE/data-converge/data`（不含 `lite-state`）；显式值优先。 |
| `leadership.enabled`、`dwd_ready.enabled` | 示例均为 `true` | 省略时均默认 `false`；示例显式启用。 |
| `scheduler.retry_delay_seconds` | 注释指向执行器重试间隔 | 配置校验要求严格正整数，`0` 会被拒绝；执行器内部参数本身允许 `0`。 |
| `scheduler.timezone` | 文档说明不再作为 YAML 配置项 | 代码不读取该字段；即使写入也不会报错，而是被忽略。 |
| `dwd_ready.redis.username` | 注释说省略时默认 `admin` | 仅 `sentinel` 模式默认 `admin`；`standalone` / `cluster` 模式默认不设置。 |
| `state.archives.retention_days` | 注释示例 `30` | 只要求整数 `>= 1`，没有上限；示例值不是最大值。 |

## 相关文档

- [部署要求](reliability.md#部署要求)
- [六项可靠性问题核对与实现](reliability.md)
- [已知问题与后续处理](known-issues.md)
- [状态库每日压缩备份与老化](state-archives.md)
