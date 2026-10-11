# DWD 就绪信号消费

Python 汇聚服务消费生产端的 DWD 就绪信号，等待同一 5 分钟窗口的全部五类 DWD 完成后，才允许该窗口的 5 分钟汇聚执行。收到信号不代表汇聚 SQL 已成功；成功、失败、重试和 `UNKNOWN` 仍由原有 SQLite 执行账本记录。

本次仅接入现有 52 个任务的调度链，不迁入 Java enrichment、aggregation、quantile 代码，不新增 ClickHouse 统计表、自动重算或对账框架。生产端消息格式保持不变。**当前未连接公司 Redis 或 ClickHouse，生产信号和部署配置仍需公司环境联调。**

## 消息契约

生产端在某类型 DWD 的窗口写入成功后，向 Redis List `modelverse:dwd:ready` 执行 `LPUSH`，值为普通字符串：

```text
2025-08-01T10:05:GPU_REALTIME
```

它表示窗口 `[2025-08-01 10:05, 2025-08-01 10:10)`，不是 10:05 结束的窗口。消费端使用 `rsplit(":", 1)` 分离时间与类型，避免拆错时间中的冒号。消息不是 JSON。

全部类型为：

- `GPU_REALTIME`
- `NODE_REALTIME`
- `REQUEST_REALTIME`
- `MODELINSTANCE_REALTIME`
- `MODELMETRIC_REALTIME`

顺序不影响就绪判断；同一类型重复收到仍只算一种。时间格式严格为 `YYYY-MM-DDTHH:MM`，按 `Asia/Shanghai` 解释，分钟必须对齐 5 分钟，不接受秒、微秒或时区后缀。窗口未结束、早于已初始化的 `coverage_start`、未知类型或格式错误的消息不会授权汇聚执行。

## 调度与等待

消费发生在调度主线程的有界轮询中，不另建 Redis 消费后台线程。默认一轮最多处理 100 条消息；队列为空时阻塞等待最多 1 秒。SQL 执行期间不会并行消费，消息继续保留在 Redis，下一轮再处理。

同一窗口的五种信号齐全后，调度器可提前登记该窗口的全部 **13 个 `PT5M` 任务**，不必等它们原来的 cron 时间。登记采用现有窗口 ID 去重，不推进 cron 计划游标，也不将任何任务提前标为成功。

Cron 继续从持久游标登记到期窗口。事件登记、cron 登记和手工执行均经过同一执行器检查：启用就绪消费后，缺少任一种信号的 `PT5M` 窗口继续等待。小时、日、月任务仍要求覆盖其时间范围的全部上游窗口成功。

从**窗口结束时间起 60 秒**仍未齐全时记录 `DWD_WAIT_TIMEOUT` 告警，包含 `window` 和 `missing` 类型；即使一条信号也没收到，也应发现并告警。超时不会触发 partial 汇聚，不会把缺失类型视为成功。之后继续等待，信号补齐后再执行。这里的 60 秒不是从首条消息到达开始计算；检查随调度轮次进行，长 SQL 或积压可能使日志晚于阈值出现。重复告警按 `wait_warning_seconds` 节流，任务保留为 `BLOCKED`。

运行过程中 Redis 轮询失败不会放行未就绪窗口。SQLite 中已经持久化为五类齐全的窗口，仍可继续执行汇聚；Redis 恢复后继续接收其余信号。首次创建客户端是另一条路径：Cluster 客户端需要发现拓扑，初次无法连接或认证失败可能直接使服务启动失败，应修复连接后重启。进程健康探针和业务就绪信号是两件事：心跳正常不代表每个数据窗口已齐全，应同时检查缺失信号告警和任务状态。

## 唯一消费者与可靠确认

`modelverse:dwd:ready` 由这一个 data-converge 调度实例独占消费。**不得同时启动 Java 消费者、另一套 Python 消费者或人工 `BRPOP` 同一 List。** List 会把消息分给不同消费者，导致没有任何一方收到完整的五类信号。

消费与确认按以下顺序进行：

1. 使用 `BRPOPLPUSH` 将原 List 最旧的消息原子转移到 processing List；有积压时使用非阻塞转移继续处理。
2. 在主线程把合法消息对应的 `(window_start, dwd_type)` 就绪事实提交到 SQLite。
3. SQLite 提交成功后，才使用 `LREM` 从 processing List 确认移除该条消息。
4. 每轮先恢复 processing List 中未确认的消息，再接收新消息。

“转移到 processing，再确认移除”沿用 Redis 官方的可靠队列模式。`BRPOPLPUSH` 自 Redis 6.2 起被标记为 deprecated，官方建议新代码使用 `BLMOVE RIGHT LEFT`；本次保留旧命令以兼容既有 Redis 版本，部署时仍需确认命令可用。这不是端到端 exactly-once 保证。[Redis BRPOPLPUSH 文档](https://redis.io/docs/latest/commands/brpoplpush/)

格式错误、早于覆盖起点和未结束窗口的消息会记录相应诊断后确认移除，不重新入队，避免坏消息堵住 processing。它们不会写入就绪事实；提前发送的合法类型消息被丢弃后，需要生产端在窗口真实完成后重新发送。

这提供可恢复的重复投递，不承诺 Redis 与 SQLite 的分布式事务。重复处理通过 SQLite 唯一键去重；重复信号也不会重放已经 `SUCCESS` 的 SQL。SQLite 提交异常会停止服务且不 ACK；ACK 失败不会撤销已提交的就绪事实，已经收齐的窗口仍可执行。

默认两个键为：

```text
source:      modelverse:dwd:ready
processing:  {modelverse:dwd:ready}:data-converge:processing
```

默认 processing 的 hash tag 使用 source 的完整键名，使两个键落到同一 Redis Cluster slot，不需要修改生产端原有键名。processing key **只能由程序派生，不能在 YAML 显式配置**。自定义 source 已包含有效 hash tag 时，派生键保留该 tag，并追加完整 source 的 SHA-256 摘要，使共享一个 slot 的不同 source 不会共用 processing List。

唯一消费者是部署约束，不是 Redis 分布式锁。继续使用单实例、持久状态目录、Recreate，以及 hostPath 对应的固定节点。本机 `flock` 只保护同一状态目录，不能防止另一节点用另一个目录运行消费者。迁移前必须停止旧实例并迁移完整 SQLite 状态，不能在新节点初始化空账本后宣称高可用。

## 配置与部署输入

以下是配置形状示例。`mode`、`db` 和连接信息必须按实际发布端环境填写，示例不代表公司 Redis 的拓扑或数据库选择。

```yaml
dwd_ready:
  enabled: true
  queue_key: modelverse:dwd:ready
  # processing key 自动派生；不支持 processing_key 配置项。
  block_seconds: 1
  batch_size: 100
  wait_warning_seconds: 60
  redis:
    mode: standalone       # 按实际环境选择 standalone 或 cluster。
    host: "${DWD_REDIS_HOST}"
    port: "${DWD_REDIS_PORT}"
    db: 0                  # standalone 必须与发布端一致；cluster 必须为 0。
    password_env: DWD_REDIS_PASSWORD
    ssl: true
    ssl_verify: true
    socket_timeout: 3
    socket_connect_timeout: 2
```

只支持 Redis standalone 和 cluster；不把 Sentinel 配置或公司 SDK 的逻辑服务名自动解释为可直连地址。Redis Cluster 只支持数据库 0，不能使用 `SELECT` 切换数据库。[Redis SELECT 文档](https://redis.io/docs/latest/commands/select/)

Cluster 发现后的节点地址也必须从 Pod 可达。默认启用 TLS 并校验证书；公司 CA 应安装到 Python/Redis 客户端使用的信任链中。建议使用已验证的 redis-py **6.4.0**；启用 Cluster TLS 校验时，如果客户端版本不能保留主机名校验选项，程序会拒绝启动，不会静默降低校验强度。

部署侧需要提供：

| 输入 | 用途 |
|---|---|
| `DWD_REDIS_HOST`、`DWD_REDIS_PORT` | 实际 Redis 地址与服务端口，由环境变量注入 |
| `DWD_REDIS_PASSWORD` | 由部署 Secret 注入；YAML 只保存变量名，不保存密码 |
| 可选 Redis ACL 用户名 | 使用 ACL 时配置 `redis.username`，与发布端队列权限对应 |
| CA 证书及信任配置 | 校验 Redis TLS 证书；实际挂载路径由公司部署提供 |
| 实际 `mode`、`db`、队列名 | 与生产端使用的实例、数据库和 List 一致 |

Redis 密码环境变量应是 Redis 客户端能直接使用的认证值；本消费配置不会自动套用 ClickHouse 的 XMS/PMS 解密规则。不得把密码放入命令行、提交到 Git 或输出到日志。所需 Redis 权限至少涵盖就绪队列的转移、processing 恢复与确认；具体 ACL 和 Cluster 拓扑读取权限在公司联调时确认。

### 本机 modelverse 配置核对结果

只读检查了提供的 `辅助运营源代码/services/modelverse-exporter` 恢复工程：

- `src/main/resources/application.yml` 的 `its.security.redis` 启用了公司 Redis SDK，包含 `name`、`user`、`master`、连接池参数和 `database: 5`。
- `pom.xml` 引入 `security-sdk-client-redis` 与 `spring-data-redis`。配置没有提供足以确认部署拓扑的显式 host、port、TLS 或节点清单。
- 因此不能仅凭逻辑名称或 `master` 字段断言它使用 standalone、Redis Cluster 或 Sentinel，也不能从本机工程推断实际 Secret/ConfigMap 名称和挂载路径。
- 附件将连接描述为“cluster、database 5”，这与 Redis Cluster 只使用数据库 0 的要求不一致。必须由公司确认 SDK 的实际连接方式及信号所在数据库；不能直接照抄，也不能只把 Python 的 `db` 改为 0 就认为两端已经一致。
- 本机恢复工程的 `EnrichmentScheduler` 仍直接调用下游聚合，未找到附件所述 `RedisDwdReadyNotifier` 实现。附件说明发布端尚未提交/部署；本次不能据附件认定生产信号已经可用。

## 故障恢复

| 故障位置 | 恢复行为 |
|---|---|
| 消息已转到 processing，SQLite 尚未提交时进程退出 | 消息仍在 processing，重启后重新处理 |
| SQLite 已提交，ACK 前进程退出或 ACK 失败 | 重读消息；就绪事实幂等写入，再确认；不重复执行成功 SQL |
| 已启动服务的 Redis 轮询失败 | 记录连接故障并稍后重试；未就绪窗口等待，已就绪窗口可继续 |
| 初次创建 Redis 客户端失败 | 启动失败；尤其是 Cluster 拓扑发现阶段，修复连接或认证后重启 |
| 五类信号缺失，包括全部缺失 | 窗口结束 60 秒后告警并持续等待，不执行 partial |
| SQL 已发出但结果不明 | 沿用 `UNKNOWN` 人工核验流程；不能靠重复 ready 信号重发 SQL |
| Redis 数据丢失，且信号尚未写入 SQLite | 无法凭消费者还原信号；核验 DWD 完整性和生产端记录后再由受控流程补发 |
| SQLite 状态丢失 | 停止服务并恢复账本；不可删除 processing 或重新初始化来绕过核验 |

不要通过清空 processing List 解决消费故障，也不要把收到 ready 当作下游 SQL 完整写入的证据。生产端通知为 best-effort：DWD 写入成功但通知失败时，本实现选择等待和告警，需要生产端恢复或人工核验后补发信号。

## 与 Java 参考规格的差异

| 项目 | 附件中的 Java 参考方案 | 本次 Python 方案 |
|---|---|---|
| 运行位置 | DWD 主进程内的消费守护线程 | 独立 data-converge 主线程，有界同步轮询 |
| 消息获取 | `BRPOP` 后放入内存 | 原子转 processing，SQLite 提交后 ACK |
| 聚合状态 | 内存按窗口保存五类集合 | SQLite 持久就绪事实，重启可恢复 |
| 缺失超时 | 首条消息后 60 秒可触发 partial | 窗口结束后 60 秒告警，继续等待全部五类 |
| Cron | 可在信号缺失或 Redis 故障时直接触发 | 继续登记窗口，执行器仍检查 ready，不能绕过 |
| 幂等依据 | Redis processed guard，TTL 15 分钟 | 现有持久窗口状态；ready 本身不标记 SQL 成功 |
| 下游执行 | enrichment、aggregation、quantile Java 链 | 原 52 个 Python SQL 任务及完整窗口依赖 |
| 部署互斥 | Java 主节点与 Redis 锁体系 | 独占队列、单实例、同一持久状态目录；不承诺跨节点选主 |

这些差异来自本次确认的消费位置和“缺失继续等待、禁止 partial”要求。生产端 LPUSH 格式和五个类型保持兼容；不同时部署附件中的 Java 消费者。

## 公司环境联调

联调需确认实际 Redis 模式、端点、数据库、TLS/ACL 和生产端发布开关，再以完整五类消息验证正确窗口触发。还需覆盖重复和乱序信号、零信号告警、转移后重启、提交后 ACK 失败、Redis 恢复以及 SQL `UNKNOWN` 不重发。联调过程中确认旧 Java 下游调度和消费者的切换安排，避免两套流程同时写同一目标窗口。

本地测试能够验证解析、状态恢复与调度限制。当前机器未安装 `redis-server`，真实 Redis 回环集成测试明确跳过；fake client 和 redis-py 参数检查不等于真实服务验收。公司 SDK 配置映射、真实 Redis Cluster 路由、生产端通知可靠性和目标 SQL 的业务验收仍需在公司环境进行。
