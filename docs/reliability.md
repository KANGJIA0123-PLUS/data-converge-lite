# 六项可靠性问题核对与实现

本次范围为同一份持久账本下的安全接管，沿用 SQLite、现有 Redis 认证与 ClickHouse HTTPS，不新增依赖，不修改公司构建、部署、探针脚本或业务 SQL。当前有 60 个汇聚任务。

## 现状与本次修复

| 原问题 | 当前代码事实 | 本次处理 |
|---|---|---|
| D1 选主未集成 | 旧选主模块未接入，续租与释放存在检查后修改的竞态 | 接入原子 Redis 租约、唯一会话 token、后台续租、失租停派发；保留本地文件锁；账本身份校验现已临时移除，见 DC-STATE-001 |
| D2 执行日志只有内存 | 生产已用 SQLite；旧 ExecutionLogger 不是生产数据源。但同一实例的旧尝试会被覆盖 | 逐 query_id 持久化尝试和人工处置；增加 --history，实例检查可分页查看历史 |
| D3 重试重复写 | 已成功实例不再执行，已发送但结果未知的操作不自动重试；没有数据库端 exactly-once | 保留保守防重；发送前 SQL 文件错误明确为 FAILED，避免误报 UNKNOWN。没有未经验证启用 ClickHouse 去重参数 |
| D4 无断点续传 | 实例、游标、DWD 信号已持久化，重启会补偿未完成窗口 | 准备/派发检查点区分可自动恢复和需核对任务；准备不消耗次数；保留成功不重放和未知不盲重试 |
| D5 kubectl/Pod 硬编码 | 已经使用配置中的 HTTPS 服务端点 | 保持现有直连实现 |
| D6 密码进入命令行 | 已经通过请求头发送凭据，XMS 密文经子进程标准输入传递 | 保持现有解密路径与错误证据白名单；不记录密码、原始 SQL 或服务端异常正文 |

## 写入资格与接管顺序

1. 应用配置和任务定义通过校验。启用选主的服务先建立本实例心跳，ready=false。
2. 使用独立 Redis 连接争取租约。备机只等待，不恢复账本、不消费消息、不构造 SQL 执行器。
3. 获取租约后获取同一账本的 POSIX 文件锁。原主若正在收尾，继续等它释放锁，同时续租。
4. 校验原部署指纹、配置版本和覆盖起点；不再核对本地选主身份与 Redis 永久账本注册。
5. 将匹配当前 query_id 的 PREPARED 实例重新排队；其他遗留 RUNNING 记为 UNKNOWN。创建运行资源，进行连接探测，随后启动调度。
6. SQL 提交前、Redis receive/ack 前检查租约。续租失败、超时或返回过晚均永久失租，本会话不会重新抢主继续执行。
7. 失租后停止新派发；已提交的 SQL 等待结果并按原规则记账，最后释放文件锁、租约及连接。强制杀死后，仅有尚未派发的持久化证明才自动重排，其余保持 UNKNOWN。

`--run-task`、`--run-once` 和 `--resolve-instance` 同样需要租约和文件锁；主服务已持租时立即报 `LEADERSHIP_BUSY`，不会抢占。维护操作需要先停止写服务。`--init-state` 保持离线，只持本地文件锁初始化，不发 SQL、不消费消息。所有只读查询不抢租。

租约和持久化承担不同职责：Redis 协调主备，文件锁控制同一物理账本写入，状态连续依赖完整账本和持久存储保障。Redis Sentinel 的异步复制无法单独提供严格互斥；本实现没有 ClickHouse 端 fencing。参见 [Redis 官方分布式锁说明](https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/)。

## 配置

随仓库交付的 `resources/config/app_config.yaml` 默认启用：

```yaml
leadership:
  enabled: true
  cluster_name: data-converge-modelmate-metric
  ttl_seconds: 30
  renew_interval_seconds: 5
  standby_poll_seconds: 2
```

省略 leadership 的旧配置保持单写模式。所有副本必须使用相同 cluster_name、Redis 服务、DB、业务配置；要连续恢复原执行历史，还必须使用同一份完整持久账本。选主开关或名称变更不再受旧选主身份元数据阻断；本轮调测对已有有效配置快照的差异也告警放行，原快照保留，详见 [DC-STATE-002](known-issues.md#dc-state-002)。关闭选主只适用于单写部署，更换命名空间前应停止原命名空间的所有写实例。配置放行或缺库重新初始化都不证明历史连续或不会重复写入。

连接复用 `dwd_ready.redis` 的 Sentinel/PSM/XMS 或 standalone 认证配置。共享连接工厂位于 `infrastructure/redis`，每个调用方拥有独立连接池；选主直接组合连接资源，不再构造 DWD 消费者，也不与阻塞消息读取共用连接。选主当前要求启用 DWD Redis，支持 standalone/sentinel，不支持原生 Redis Cluster。Redis 账号需有 GET、SET NX PX、PEXPIRE、DEL、EVAL 等相应权限；租约操作采用比较 token 的 Lua 脚本。

Redis 临时租约键为 `data_converge:leader:{cluster_name}`。旧版永久注册键加 `:ledger`；新版不再读取、写入或删除该键，也不读取或改写本地 `leadership_cluster` / `leadership_ledger_id` 作为准入依据。现存身份项留作现场证据。正常服务启用选主时打印 `LEADERSHIP_LEDGER_CHECK_DISABLED: issue=DC-STATE-001`，这是临时解除校验的遗留提示，不是启动失败；取得有效租约并通过其余状态检查后可以继续调度。该措施不能恢复丢失账本，详见 [DC-STATE-001](known-issues.md#dc-state-001)。

心跳默认按 `DATA_CONVERGE_INSTANCE_ID`、`POD_NAME`、`HOSTNAME`、系统主机名的顺序选取实例身份，文件存放在账本目录旁的 `.<账本目录名>.health/<身份哈希>/heartbeat.json`。不同 Pod 的默认 hostname 不同；同机启动多个进程时需分别设置稳定且不同的 `DATA_CONVERGE_INSTANCE_ID`，启动与探针命令使用相同值。备机的 Python liveness 可通过，readiness 保持失败；失租后下一次心跳写入会撤销 ready。仓库内的 `liveness_check.sh` / `readiness_check.sh` 已改为调用 `--healthcheck`；但主服务 Deployment 的探针声明不在本仓库 Chart 中，仍需现场核对，不能仅凭脚本存在宣称 Kubernetes 已启用真实探针。

逐键说明、默认值和校验边界见[应用配置参考](configuration-reference.md)。

## 部署要求

以下三项是主备接管的部署前提，不能只靠应用代码满足。

### 终止宽限期

`terminationGracePeriodSeconds` 必须不小于最长任务超时。随仓库配置的 `scheduler.task_timeout_seconds` 为 5min 120 秒、hour 600 秒、day 1800 秒、month 1800 秒，因此当前至少设置为 **1800 秒**，并建议留出连接收尾和记账余量；若把 day/month 调到校验上限 3600 秒，终止宽限期也要同步不低于新的最大值。该字段属于 Pod spec，需在主服务 Pod 模板中设置。

原因：SIGTERM/SIGINT 只停止新派发，服务会在调度循环退出时 `drain()`，等待已提交的在途 SQL 完成并记账，之后才释放 Redis 租约和本地文件锁（见 `scheduler/service.py`、`leader_election/lease.py`、`coordination/admission.py` 的注释与实现）。Kubernetes 默认的 30 秒远短于日/月任务，提前 SIGKILL 会让旧 INSERT 在 ClickHouse 侧继续执行；接管方必须等旧 query 确认停止后才重跑，恢复被推迟，也可能留下待处理窗口和人工核查（演练 BUG-4）。若改为主动 KILL 自己的在途 query 来缩短交接，必须把窗口明确标记为待重跑，不能直接释放租约让接管方盲目并发执行。

### 重启策略与存活探针

主服务 Pod 的 `restartPolicy` 必须是 `Always`。该字段属于 Pod spec，不是单个 container；Deployment 管理的 Pod 默认是 `Always`，但仓库 Chart 中的安装/删除 Job 使用 `OnFailure`，主服务不能沿用 Job 模板。仓库 Chart 目前只包含这些 Job，主服务 Deployment 及其探针不在本仓库，需在实际上线清单中核对。

同时配置存活与就绪探针。镜像已带 `liveness_check.sh` / `readiness_check.sh`，它们加载平台 Python 环境后执行 `--healthcheck`，不安装依赖：

```yaml
livenessProbe:
  exec:
    command: ["bash", "/opt/its/data-converge/bin/liveness_check.sh"]
  initialDelaySeconds: 30
  periodSeconds: 30
  timeoutSeconds: 10
  failureThreshold: 3
readinessProbe:
  exec:
    command: ["bash", "/opt/its/data-converge/bin/readiness_check.sh"]
  periodSeconds: 10
  timeoutSeconds: 10
  failureThreshold: 1
```

两个探针只读本实例心跳文件，不访问 Redis、ClickHouse，不消费消息，单次约 0.1 秒。判定规则：

| 状态 | liveness | readiness |
|---|---|---|
| 进程未启动 / 已退出 / 心跳超过 75 秒未刷新 | 失败 | 失败 |
| 备机等待竞选（含 Redis 闪断重试、失租后回到备机） | 通过 | 失败 |
| 主节点启动中等待 ClickHouse 就绪 | 通过 | 失败 |
| 主节点正常调度 | 通过 | 通过 |
| 主节点调度循环卡住超过预算 | **失败** | 失败 |

调度循环每完成一轮会刷新心跳中的 `progress_at`；超过预算未完成一轮即判定挂起，liveness 失败，由 kubelet 重启。预算 = 最长任务超时 + `poll_interval_seconds` + 120 秒（仓库配置为 1800 + 5 + 120 = 1925 秒，约 32 分钟），因为一轮内补算会同步执行一条 SQL，最长可达任务超时。备机等待、启动等待和关闭收尾阶段不检查调度进度。

`readiness` 只表示"本实例是当前写入者"：备机永远 NotReady 是预期行为。本服务不对外提供端口，readiness 不用于流量分发，但会影响 `kubectl rollout status` 和滚动更新判断——同时只会有一个 Pod Ready，滚动更新策略应为 `maxUnavailable: 1`、`maxSurge: 0`，或改用 `Recreate`，否则会一直等待第二个 Pod 就绪。

在 `initialDelaySeconds` 之前心跳文件可能尚未创建：服务启动先加载配置、取得租约前就会写入心跳，但首次部署解压依赖可能更久，可按现场启动耗时调大。

原因：Redis 故障、失租或被 fencing 时，服务会释放租约、在途 SQL 收尾并关闭本地文件锁，然后在进程内回到备机等待重新竞选（日志 `LEADERSHIP_TERM_ENDED`、`LEADERSHIP_REDIS_RETRY`），不再以退出码 1 退出；该行为已在 S2、S3、S4、S9 真实环境复测通过。但进程崩溃、死锁、心跳停止或状态存储不可写时仍需要这两项，不能假定进程永远自行恢复：`restartPolicy: Always` 让 kubelet 重启容器，存活探针用于发现进程仍在但已无响应的挂起状态。

### Sentinel 切换参数

`leadership.ttl_seconds`（仓库配置 30 秒，续租间隔 5 秒）必须大于 Sentinel 完成主从切换的总耗时，不能只比较 `down-after-milliseconds`。总耗时至少包含 `down-after-milliseconds`、Sentinel 选举与提升、客户端发现新主，以及 PSM 挑战认证和重连时间。演练中 `down-after-milliseconds 3000` 的实际切换约 3.6–4.1 秒；现场常见配置为 5–30 秒，总切换耗时还会叠加选举、认证和重连，可能更长。

建议 `down-after-milliseconds` 明显小于 `ttl_seconds`（例如 30 秒 TTL 时不超过 10 秒），并留出实测余量；若无法保证总切换时间小于 TTL，应增大 `ttl_seconds`（上限 300 秒）或降低 `down-after-milliseconds`。TTL 过短会在新主就绪前失租，导致不必要的主备切换和中断窗口重跑；TTL 过长会延长旧主被 fencing 前的风险窗口。修改后应在目标环境复测切换。`down-after-milliseconds` 是 Sentinel 侧配置，不在 `app_config.yaml` 中，应用不会校验它。

以上依据与复现见[双实例真实环境演练报告](evidence/dual-instance-lab-2026-11-06.md)的 BUG-2、BUG-4；配置键的完整范围见[应用配置参考](configuration-reference.md)。

## 运维查询

以下命令全部在 data-converge 容器内执行，辅助入口负责 Python 环境与 PYTHONPATH：

```bash
bash /opt/its/data-converge/bin/start_data_converge.sh --help
bash /opt/its/data-converge/bin/start_data_converge.sh --diagnose
bash /opt/its/data-converge/bin/start_data_converge.sh --diagnose --online
bash /opt/its/data-converge/bin/start_data_converge.sh --history --limit 20
bash /opt/its/data-converge/bin/start_data_converge.sh --history --task task_resource_5min_ads --limit 20 --offset 20
bash /opt/its/data-converge/bin/start_data_converge.sh --inspect-instance 'task_resource_5min_ads@2026-10-14T16:30:00+08:00' --limit 20
```

诊断命令均可加 `--format json`。`--history` 展示历次真实执行结果；`--failures` 仍只显示当前待处理异常。人工将 UNKNOWN 判定为成功，不会改写原尝试的 UNKNOWN，处置信息单独记录。历史没有自动按 7 天删除；日志文件老化不影响数据库中的审计记录。已被旧版本覆盖的尝试无法补造，无历史表时明确显示旧版未记录。

新增 `--recovery-plan` 和 `--events`。前者与真实恢复共享分类逻辑，但只展示快照，不会对正在运行的任务采取动作；后者导出与状态同事务保存的事件，支持 `--after-event` 游标及 JSONL。准备阶段不写尝试历史，只有派发意图提交后才计入尝试；事件中的 PREPARED/RECOVERED_UNSUBMITTED 补充说明未发送过程。详情及实测见 [恢复验证报告](evidence/recovery-verification-2026-10-18.md)。

`--diagnose` 检查当前选主配置和本实例心跳，不再核对旧选主身份。`--diagnose --online` 额外以 GET/TTL 观察 Redis 临时租约，不访问永久注册项，不写键、不续租、不消费消息。证据中的 `ledger_identity_enforced=false` 明示临时解除；检查不能证明账号有 EVAL 权限，也不把瞬时观察当作当前进程持主资格或历史连续性的证明。

## 不能越过的边界

- 连续恢复要求使用同一份完整持久账本、锁文件及初始化标记。两个节点的独立 hostPath 不具备共享历史，复制一个 SQLite 文件形成两份并行账本也不具备；临时解除身份阻断后，这些情况不会因永久注册不一致被拒绝，重复执行风险仍存在。不要简单把 SQLite 放到 NFS 就认为获得可靠共享存储；文件锁、同步和故障语义需要存储保障。[SQLite 官方网络存储说明](https://sqlite.org/useovernet.html)
- 本次代码与 Chart 没有增加跨节点独立磁盘自动恢复，也没有修改 Kubernetes 副本数、卷、探针或终止宽限期；实际部署仍需按上文[部署要求](#部署要求)配置并演练。
- SQLite 成功记账与 ClickHouse INSERT 没有共同事务。UNKNOWN 可能包含部分或全部已写数据；query_id 只是追踪标识。人工 retry 前必须确认原查询已结束并确认目标窗口可安全重跑。
- ClickHouse 的表引擎、去重窗口、版本、INSERT SELECT 的块边界及物化视图会影响数据库去重，不能只加一个 token 就承诺幂等。有关约束见 [ClickHouse 官方重试去重说明](https://clickhouse.com/docs/concepts/features/operations/insert/deduplicating-inserts-on-retries)。
- 本次没有改业务 SQL、目标表结构或任务依赖定义；此前新增节点汇聚的依赖与 SQL 来源差异仍需单独处理。
