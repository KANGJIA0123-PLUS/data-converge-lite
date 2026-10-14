# 六项可靠性问题核对与实现

本次范围为同一份持久账本下的安全接管，沿用 SQLite、现有 Redis 认证与 ClickHouse HTTPS，不新增依赖，不修改公司构建、部署、探针脚本或业务 SQL。当前有 52 个汇聚任务。

## 现状与本次修复

| 原问题 | 当前代码事实 | 本次处理 |
|---|---|---|
| D1 选主未集成 | 旧选主模块未接入，续租与释放存在检查后修改的竞态 | 接入原子 Redis 租约、唯一会话 token、后台续租、失租停派发、账本身份绑定；保留本地文件锁 |
| D2 执行日志只有内存 | 生产已用 SQLite；旧 ExecutionLogger 不是生产数据源。但同一实例的旧尝试会被覆盖 | 逐 query_id 持久化尝试和人工处置；增加 --history，实例检查可分页查看历史 |
| D3 重试重复写 | 已成功实例不再执行，已发送但结果未知的操作不自动重试；没有数据库端 exactly-once | 保留保守防重；发送前 SQL 文件错误明确为 FAILED，避免误报 UNKNOWN。没有未经验证启用 ClickHouse 去重参数 |
| D4 无断点续传 | 实例、游标、DWD 信号已持久化，重启会补偿未完成窗口 | 验证接管后复用账本与覆盖起点，成功任务不重放；在途 RUNNING 恢复 UNKNOWN，核实后处理 |
| D5 kubectl/Pod 硬编码 | 已经使用配置中的 HTTPS 服务端点 | 保持现有直连实现 |
| D6 密码进入命令行 | 已经通过请求头发送凭据，XMS 密文经子进程标准输入传递 | 保持现有解密路径与错误证据白名单；不记录密码、原始 SQL 或服务端异常正文 |

## 写入资格与接管顺序

1. 应用配置和任务定义通过校验。启用选主的服务先建立本实例心跳，ready=false。
2. 使用独立 Redis 连接争取租约。备机只等待，不恢复账本、不消费消息、不构造 SQL 执行器。
3. 获取租约后获取同一账本的 POSIX 文件锁。原主若正在收尾，继续等它释放锁，同时续租。
4. 校验原部署指纹、覆盖起点，以及 Redis 中的永久账本身份；不同账本拒绝接管。
5. 将遗留 RUNNING 记为 UNKNOWN，创建运行资源，进行连接探测，随后启动调度。
6. SQL 提交前、Redis receive/ack 前检查租约。续租失败、超时或返回过晚均永久失租，本会话不会重新抢主继续执行。
7. 失租后停止新派发；已提交的 SQL 等待结果并按原规则记账，最后释放文件锁、租约及连接。强制杀死时依靠 RUNNING→UNKNOWN 恢复保护。

`--run-task`、`--run-once` 和 `--resolve-instance` 同样需要租约和文件锁；主服务已持租时立即报 `LEADERSHIP_BUSY`，不会抢占。维护操作需要先停止写服务。`--init-state` 保持离线，只持本地文件锁初始化，不发 SQL、不消费消息；首次运行再进行远端账本绑定。所有只读查询不抢租。

租约和持久化承担不同职责：Redis 协调主备，文件锁与同一 SQLite 账本保证状态连续。Redis Sentinel 的异步复制无法单独提供严格互斥；本实现没有 ClickHouse 端 fencing。参见 [Redis 官方分布式锁说明](https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/)。

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

省略 leadership 的旧配置保持单写模式；已绑定的账本不能通过设置 enabled=false 绕过选主。所有副本必须使用相同 cluster_name、Redis 服务、DB、业务配置和同一份物理账本。独立部署使用不同名称，绑定后不要直接改名。

连接复用 `dwd_ready.redis` 的 Sentinel/PSM/XMS 或 standalone 认证配置，但连接池独立，不与阻塞消息读取共用。选主当前要求启用 DWD Redis，支持 standalone/sentinel，不支持原生 Redis Cluster。Redis 账号需有 GET、SET NX PX、PEXPIRE、DEL、EVAL 等相应权限；租约操作采用比较 token 的 Lua 脚本。

Redis 租约键为 `data_converge:leader:{cluster_name}`，永久账本注册键加 `:ledger`。永久注册项不随租约过期。原账本已记录绑定而注册项丢失时，拒绝静默重新创建；不要把清空这些键作为修复方式。

心跳默认按 `DATA_CONVERGE_INSTANCE_ID`、`POD_NAME`、`HOSTNAME`、系统主机名的顺序选取实例身份，文件存放在账本目录旁的 `.<账本目录名>.health/<身份哈希>/heartbeat.json`。不同 Pod 的默认 hostname 不同；同机启动多个进程时需分别设置稳定且不同的 `DATA_CONVERGE_INSTANCE_ID`，启动与探针命令使用相同值。备机的 Python liveness 可通过，readiness 保持失败；失租后下一次心跳写入会撤销 ready。公司探针脚本原有提前 `exit 0` 行为尚未修改，不能据此宣称 Kubernetes 已启用真实探针。

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

`--diagnose` 检查本地选主配置、账本绑定和本实例心跳。`--diagnose --online` 额外以 GET/TTL/EXISTS 观察 Redis 租约，不写键、不续租、不消费消息，不能证明账号有 EVAL 权限，也不把瞬时观察当作当前进程持主资格。

## 不能越过的边界

- 接管要求使用同一份完整持久账本、锁文件及初始化标记。两个节点的独立 hostPath 不符合要求，复制一个 SQLite 文件形成两份并行账本也不符合要求。不要简单把 SQLite 放到 NFS 就认为获得可靠共享存储；文件锁、同步和故障语义需要存储保障。[SQLite 官方网络存储说明](https://sqlite.org/useovernet.html)
- 本次没有增加跨节点独立磁盘自动恢复，也没有修改 Kubernetes 副本数、卷、探针或终止宽限期。
- SQLite 成功记账与 ClickHouse INSERT 没有共同事务。UNKNOWN 可能包含部分或全部已写数据；query_id 只是追踪标识。人工 retry 前必须确认原查询已结束并确认目标窗口可安全重跑。
- ClickHouse 的表引擎、去重窗口、版本、INSERT SELECT 的块边界及物化视图会影响数据库去重，不能只加一个 token 就承诺幂等。有关约束见 [ClickHouse 官方重试去重说明](https://clickhouse.com/docs/concepts/features/operations/insert/deduplicating-inserts-on-retries)。
- 本次没有改业务 SQL、目标表结构或任务依赖定义；此前新增节点汇聚的依赖与 SQL 来源差异仍需单独处理。
