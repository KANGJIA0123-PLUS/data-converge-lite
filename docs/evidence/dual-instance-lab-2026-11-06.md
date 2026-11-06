# 双实例真实环境演练报告（2026-11-06）

## 环境

| 组件 | 实际使用 | 说明 |
|---|---|---|
| ClickHouse | 官方 26.11.1 server（本机进程） | `modelmate_metric` 库；目标表为 `Distributed` 套 `MergeTree _local`，与现场一致 |
| SQL | 仓库生产 SQL：`task_gpu_realtime_to_5min_dws`、`task_gpu_5min_to_hour_dws` | 原样执行，含清理 `ALTER ... DELETE` 与 `INSERT ... SELECT` |
| Redis | 8.10 主 + 从 + 3 个 Sentinel（quorum 2） | 生产 `mode: sentinel` 代码路径、Lua fencing、连接池 |
| 网络故障 | toxiproxy：每个实例独立的 Sentinel/主/从/ClickHouse 链路 | 可单独切断某个实例 |
| 服务 | 两个独立进程 A、B，各自独立状态目录（模拟两个节点 hostPath） | 生产 `app_config.yaml`，仅调小 TTL=10s/续租 2s、5 分钟超时 |

唯一替换：公司 PSM 挑战认证（依赖不公开的 `psf/dsf` SDK）换成标准 Redis 密码 AUTH，见 `~/dconv-lab/shim/sitecustomize.py`；
另外把 Sentinel 返回的主节点地址映射到该实例自己的代理端口，以便单独断网。调度、租约、fencing、共享状态、DWD、执行器全部是生产代码。

慢查询通过源表视图 `sleepEachRow` 控制，单窗口 INSERT 可稳定持续 10–45 秒，用来把故障精确打在 SQL 执行中。

脚本：`~/dconv-lab/scripts/`（`s1_*`…`s8_*`），每个场景最后用 ClickHouse 实际行数对比源数据期望值判定。

## 结论总览

| # | 场景 | 结果 |
|---|---|---|
| S1 | kill -9 主节点（SQL 执行中），备机接管 | 通过：9 秒接管，中断窗口 KILL 后重跑，数据正确 |
| S1c | 同上，但接管瞬间备机到 ClickHouse 短暂不通 | 修复前窗口数据翻倍；**修复后通过** — BUG-1 |
| S2 | 主节点冻结（SIGSTOP）超过 TTL 后恢复 | 通过：旧主被 fencing，数据正确（修复后旧主回到备机等待，不再退出） |
| S2b | 冻结 + 接管时 KILL 失败 | 修复前窗口数据翻倍；**修复后通过** — BUG-1 |
| S3 | 主节点与 Redis 断网（拒绝连接 / 静默丢包） | 通过：旧主停止派发，备机接管，数据正确（修复后旧主回到备机等待） |
| S4 | Redis 主节点宕机，Sentinel 切换 | 修复前失败（两个实例同时退出）；**修复后通过** — BUG-2 |
| S5 | 滚动重启（SIGTERM 主节点） | 数据正确；**交接 25 秒，且补算请求跟着节点丢失** — BUG-4、BUG-5 |
| S6 | 汇聚超过任务超时 | 修复前记为 UNKNOWN；**修复后按可重试失败处理** — BUG-6 |
| S7 | 主节点在补算 `--recompute` 中途宕机 | 修复前窗口被清空仍显示成功；**修复后通过** — BUG-3 |
| S8 | 冻结期间到达的 DWD 信号 | 通过：无信号丢失，旧主恢复后被拒 |
| S9 | 备机等待期间 Redis 闪断 4–6 秒 | 修复前备机退出；**修复后通过** — BUG-2 |

## BUG-1（严重，已修复）：接管时 KILL 只尝试一次，失败后与旧查询并发执行，数据重复

**复现**：`s1_orphan.py nokill`、`s2b_zombie.py`。

1. A 正在执行窗口 W 的 INSERT（30+ 秒）。
2. A 被 kill -9（或冻结超过 TTL）。ClickHouse 不会因客户端断开取消 `wait_end_of_query=1` 的 INSERT，查询继续执行。
3. B 接管，`state/shared.py:_kill_previous` 发一次 `KILL QUERY ... ASYNC`。此时 B 到 ClickHouse 不通（网络抖动、ClickHouse 重启、连接池满都会如此），日志 `REDIS_STATE_KILL_PREVIOUS_FAILED`，**然后照常重跑 W**。
4. B 先执行清理 `DELETE`（此时旧 INSERT 尚未写入，删不到），随后旧 INSERT 和新 INSERT 都写入。

**证据**（S1c）：
```
orphan INSERT from dead leader stayed alive 33.2s after kill -9
victim query_log: QueryFinish 0 36279          ← 旧查询完整写入
2026-11-06 03:20:00 (80, 40) expected (40, 40) BAD
```
S2b（冻结旧主 + 接管时 KILL 失败）中同一窗口最终同样为 `(80, 40)`：旧主 INSERT 执行 44 秒后 `QueryFinish`，新主的重跑也成功。

**另一个现象**：`KILL ... ASYNC` 成功返回时（S1 中返回 `NOTHING_TO_KILL` code 785）同样被记为失败告警；而 ASYNC 返回成功也不代表旧查询已停止。

**建议**：
- 接管时在重跑前**确认旧 query 已结束**：轮询 `system.processes WHERE query_id IN (...)`，直到为空或达到上限；ClickHouse 不可达就不派发这些窗口（保持 PENDING 并带阻断原因），而不是直接重跑。
- 改用 `KILL QUERY ... SYNC`，并把 785 视为"已不存在"的成功。
- 根本解决：ClickHouse 侧没有 fencing。可在 INSERT 中带 `insert_deduplication_token`（Replicated 表）或让重跑使用新清理后再校验行数；至少在文档里说明该残余风险。

## BUG-2（严重，已修复）：Redis 短暂不可用会让主、备两个实例都退出，无自动恢复

**复现**：`s4_failover.py clean|lag`、S9。

- Sentinel 正常切换耗时约 3.6–4.1 秒（`down-after-milliseconds 3000`，现场常见配置 5–30 秒更长）。
- 主节点 A：续租重试到本地期限后，`_publish` 写共享状态失败，抛出 `StateSyncError: REDIS_STATE_WRITE_FAILED`，进程退出。
- 备节点 B：`admit_writer` 的 `while not lease.acquire()` 中，`elect_as_leader` 一次连接失败即抛 `LEADERSHIP_REDIS_FAILED`，**等待中的备机直接退出**（S9：已在等待的备机遇到 4 秒闪断也退出）。
- 结果：切换完成后两个实例都已退出，窗口全部停在 PENDING：
```
A alive False  SERVICE_FAILED: StateSyncError ... REDIS_STATE_WRITE_FAILED
B alive False  SERVICE_FAILED: LeadershipError ... LEADERSHIP_REDIS_FAILED
2026-11-06 03:40:00 None expected (40, 40) BAD   （此后所有窗口）
```
- 是否自动恢复完全取决于外层是否重启进程。仓库 `start.sh` 在 Python 退出后直接 `exit`，chart 中只有安装/删除 Job（`restartPolicy: OnFailure`），未看到主服务的重启策略或探针定义；需要现场确认 Pod 的 `restartPolicy` 和 liveness 设置。
- 手动重启两个实例后，系统正确恢复：B 导入共享状态、重跑中断窗口，数据全部正确（中断窗口多跑一次）。

**建议**：
- 备机等待阶段（`admit_writer(wait=True)`）把 `LeadershipError` 当作暂时错误，退避后继续等待，而不是退出。
- 主节点失去 Redis 时进入"降级为备机"流程（停止派发、等待在途 SQL、释放文件锁后重新排队竞选），而不是让整个进程以错误退出。
- 若保留"退出由 k8s 重启"的设计，在部署文档中明确要求 `restartPolicy: Always` 和存活探针，并做一次现场演练。

## BUG-3（严重，已修复）：补算执行时不写共享状态，主节点中途宕机会清空窗口且无人察觉

**复现**：`s7b.py`。

- 正常调度在 `mark_dispatched` 后调用 `_publish`，Redis 中出现 `RUNNING` 并进入 `running` 集合；接管时据此 KILL 并重跑。
- 补算路径 `state/backfill.py:_dispatch` 只更新本地 SQLite，**不发布到 Redis**。补算 `--recompute` 已成功窗口时，共享状态始终是 `SUCCESS`。
- A 执行补算：清理 `DELETE` 已删除该窗口原数据，INSERT 进行中 A 宕机（或 INSERT 被取消）。B 接管时看到共享状态 SUCCESS，不重跑、不 KILL。
```
shared state while backfill SQL runs: b'SUCCESS' running set: set()
orphan backfill INSERT outcome: ExceptionWhileProcessing 394
window 0 final: None expected (60, 60)          ← 窗口数据为空
B view of window 0: SUCCESS                        ← 账本仍显示成功
```
- 补算队列 `backfill.sqlite3` 在 A 的本地目录，B 也无法继续该请求（见 BUG-5）。
- 若旧 INSERT 恰好跑完（S7），数据正确，但这是巧合。

**建议**：`begin_backfill` 派发前与正常执行一样 `_publish` RUNNING；`finish` 已有发布逻辑。接管时对 RUNNING 的补算窗口按中断处理（KILL 确认后重跑）。

## BUG-4（中）：滚动重启时交接时间 = 在途 SQL 剩余时长

**复现**：`s5_graceful.py`。SIGTERM 后 A 等待在途 INSERT 完成（`drain`）才释放租约：
```
A exited 0 after 23.5 s
B acquired 24.7 s after SIGTERM
```
期间续租正常，没有数据问题。但小时/天任务超时是 600/1800 秒，若 SQL 更长，会超过 k8s `terminationGracePeriodSeconds`（默认 30 秒），被 SIGKILL 后落入 BUG-1 的路径。

**建议**：明确设置 `terminationGracePeriodSeconds` ≥ 最长任务超时；或 SIGTERM 时主动 KILL 自己的在途 query 并标记为待重跑，快速释放租约。

## BUG-5（中）：补算请求只写本节点队列，主备切换后丢失或被阻断

**复现**：`s5_graceful.py`。

- 在主节点 A 上提交 `--backfill`：队列在 A 的 `state/backfill.sqlite3`；A 停止后 B 接管，**该请求永远不会执行**（A 队列一直 `QUEUED 1`）。
- 在备节点 B 上提交：CLI 返回成功 `QUEUED`，B 接管后执行时报 `BLOCKED: BACKFILL_INSTANCE_RUNNING`（B 本地账本中该窗口为导入时的 RUNNING 旧值），请求结束，不会重试。
- 两种情况 CLI 都不提示"本节点不是主节点"。跳批次 `current_then_resume` 的自动补算也走同一队列，有同样问题。

**建议**：补算请求放进 Redis（与共享状态同一命名空间，带 fencing），或 CLI 提交时检查本节点是否持有租约并给出明确提示；`BACKFILL_INSTANCE_RUNNING` 应保持排队而不是终止。

## BUG-6（低，已修复）：服务端明确的超时取消被记为 UNKNOWN，需要人工处理

**复现**：`s6_timeout.py`（5 分钟超时设为 10 秒）。ClickHouse 返回 HTTP 500 + `TIMEOUT_EXCEEDED`（code 159），query_log 为 `ExceptionWhileProcessing`，结果确定是未写入。但 `client.py:_http_failure` 只按 401 区分，其余 HTTP 错误一律 `outcome_unknown=True`：
```
status=UNKNOWN error=CLICKHOUSE_OUTCOME_UNKNOWN   error_type=TimeoutError
```
UNKNOWN 不会自动重试，必须人工核对后 `--resolve-instance`。小时/天任务偶发超时会持续产生人工工单。

**建议**：对 `X-ClickHouse-Exception-Code` 为 159/394 等明确"未完成"的错误，在确认 `system.processes` 中已不存在后归为可重试 FAILED（清理是幂等的）。

## 修复与复测（BUG-1、BUG-3）

- 中断窗口在接管（导入共享 RUNNING）或本机重启恢复（RECONCILE）时记入本地 `orphan_queries` 表。
  调度每轮先确认这些旧 query 已不在 `system.processes`（必要时 `KILL QUERY ... SYNC`），确认前窗口保持
  `BLOCKED: PREVIOUS_QUERY_STILL_RUNNING:<query_id>`，补算同样阻断；ClickHouse 不可达时只等待、不重跑。
  代码：`state/orphans.py`、`scheduler/orphans.py`、`sql_execution/client.py:stop_queries`。
- 补算派发后立即把 RUNNING 发布到共享状态；本机重启把中断补算改为 UNKNOWN 后也会发布。
- 复测结果：S1c、S2b（KILL 失败）数据由 `(80, 40)` 变为正确的 `(40, 40)`；S7b 窗口由"清空且显示成功"变为
  接管后重跑、数据正确 `(60, 60)`；S1、S1b、S2、S5 回归通过。全量测试与基线一致（50 个既有失败，无新增）。

## 修复与复测（BUG-2、BUG-6）

- BUG-2：主节点遇到 Redis 不可用、失租或被 fencing 时结束当前任期，在途 SQL 收尾后释放租约与文件锁，进程内回到备机重新竞选
  （`application.py:serve` / `coordination/terms.py`）；备机等待时把 Redis 连接失败当作暂时错误重试（`coordination/admission.py`）。
  复测：S4 clean、S4 lag 两个进程均存活、备机自动接管、无双写、数据全部正确；S9 备机经历 6 秒闪断后仍存活；
  S2、S3 旧主被 fencing / 断网后回到备机等待，不再退出。
- BUG-6：响应头异常码 159/241/394 归为可重试的 `CLICKHOUSE_QUERY_ABORTED`；复测发现客户端与服务端同一时刻超时，仍记为 UNKNOWN，
  因此客户端等待时长改为服务端 `max_execution_time` + 5 秒。复测：S6 超时窗口按重试策略重跑后为 FAILED，不再需要人工核对。
- 全量测试与基线一致（50 个既有失败，无新增）。

## 已验证正常的部分

- 租约互斥：所有场景中没有出现 A、B 同时派发 SQL（S4 lag 场景同样未出现双主）。
- fencing：被冻结/断网的旧主恢复后，写共享状态被 Lua 拒绝（`REDIS_STATE_FENCED`）或因租约失效停止派发，并退出。
- 正常接管：成功窗口不重跑，中断窗口 KILL 后重跑一次；DWD 信号无丢失（处理中队列为空，B 记录全部 20 个信号）。
- Sentinel 切换后重启实例，可从半复制状态正确恢复（共享状态中残留的 RUNNING 被重新排队重跑）。

## 运维 CLI 观察（与本轮 `dconv` 改造相关）

- 在备机上 `--failures`、`--status`、`--recovery-plan` 读本地账本，显示的是接管前的旧数据（例如 `counts: {}`），没有任何"本节点不是主节点 / 数据可能过期"的提示。
- `--diagnose --online` 的 `leadership.redis` 只能看到"有主 + TTL"，无法确认是哪台。
- 主节点上执行 `--resolve-instance` 返回 `LEADERSHIP_BUSY`（预期行为），但错误输出是 `SERVICE_FAILED` 日志行，运维容易误以为服务故障。

## 复现方法

```bash
# 基础设施（已在本机运行）：ClickHouse :18123、Redis :16379/:16380、Sentinel :26379-26381、toxiproxy :8474
cd ~/dconv-lab
~/.venvs/dconv/bin/python scripts/s1_orphan.py nokill    # BUG-1
~/.venvs/dconv/bin/python scripts/s4_failover.py clean   # BUG-2
~/.venvs/dconv/bin/python scripts/s7b.py                 # BUG-3
~/.venvs/dconv/bin/python scripts/s5_graceful.py         # BUG-4/5
~/.venvs/dconv/bin/python scripts/s6_timeout.py          # BUG-6
```
