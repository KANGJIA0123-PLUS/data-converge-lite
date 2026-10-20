# 按时间段在线补算

部署包含此功能的镜像并重启服务一次后，在**当前主实例 Pod** 中使用 `dconv`。
提交命令只登记请求，正常服务继续运行并后台处理；不需要停服务或删除状态库。
默认使用服务自带的 app_config.yaml，自定义配置在每条命令加 `--config PATH`。

## 指定任务补算

例如处理 2026-10-19 10:00 至 12:00 的小时数据，先预览：

```bash
dconv --backfill --task task_model_instance_hour_dws \
  --from '2026-10-19T10:00:00+08:00' \
  --to '2026-10-19T12:00:00+08:00' \
  --recompute --dry-run
```

确认范围后，去掉 `--dry-run` 提交：

```bash
dconv --backfill --task task_model_instance_hour_dws \
  --from '2026-10-19T10:00:00+08:00' \
  --to '2026-10-19T12:00:00+08:00' \
  --recompute --request-id model-instance-hour-20261019-10-12
```

此例生成两个窗口 `[10:00,11:00)`、`[11:00,12:00)`。
`--recompute` 会重新清理并汇聚已经成功的窗口，适用于迟到数据补入后的修正；
不带它则跳过已有 SUCCESS，只尝试未完成窗口。
请求只选择指定任务，不自动扩展到上下游；单独补小时任务使用当前已有五分钟数据，
不会要求历史 12 个五分钟实例全部成功。

`--request-id` 可选。网络断开或不确定是否提交成功时，用同一标识和相同参数重提，
返回原批次；若已修复问题、需要发起一次新的执行，应使用新的标识。
同一标识不能对应不同计划。省略标识则每次创建新批次，带 `--recompute` 重复提交会再次执行。

## 全部任务补算

```bash
dconv --backfill --all-tasks \
  --from '2026-10-19T00:00:00+08:00' \
  --to '2026-10-20T00:00:00+08:00' \
  --recompute --dry-run --format json
```

去掉 `--dry-run` 提交。`--task` 与 `--all-tasks` 二选一。
全部任务按依赖拓扑顺序执行，每个任务内部按窗口时间升序执行。
上述整天范围包含五分钟、小时、天窗口；没有完整月窗口，所以月任务列入 `omitted_tasks`。
不会为了运行月任务而扩大用户指定范围。短范围没有完整天窗口时，天任务同理省略。

时间必须含时区；应用按部署系统时区解释周期边界（系统时区获取失败时沿用上海回退）。
单任务的两端必须对齐其周期；全部任务只选择完整包含的窗口。
不接受未来结束时间、空范围或超过 **10000 个任务窗口**的请求。超过限制请拆分日期。
例如 60 个任务各一个窗口计 60 项，而非一项。

## 查看进度和错误

提交结果返回 `bf_...` 批次号，将下面的 `JOB_ID` 替换为实际批次号：

```bash
dconv --backfill-status JOB_ID
dconv --backfill-status JOB_ID --limit 100 --offset 0 --format json
dconv --history --task task_model_instance_hour_dws --limit 50
```

| 批次状态 | 含义 |
| --- | --- |
| QUEUED | 请求已登记，尚无窗口结果完成落盘 |
| IN_PROGRESS | 已有部分窗口完成，其余仍排队 |
| COMPLETED | 全部窗口成功或按已有 SUCCESS 跳过 |
| COMPLETED_WITH_ERRORS | 全部处理完，但包含 FAILED、UNKNOWN 或 BLOCKED |

进度中的 `completed` 表示已处理条目数，包括失败和阻断，不等于成功条目数。
当前正在执行的条目在结果落盘前仍计入 QUEUED；查询显示的总数、计数和分页来自同一快照。
每个条目保留窗口、任务名、状态、错误码和执行后的 query_id。

| 条目状态/错误 | 处理方式 |
| --- | --- |
| SKIPPED | 已成功且未选重算；需要补入迟到数据时用 `--recompute` 发起新批次 |
| FAILED | 已知执行失败；修复原因后发起新批次，不自动进入普通重试 |
| UNKNOWN | 请求可能已经送达 ClickHouse，先核实 query_id；不能直接重跑 |
| BACKFILL_INSTANCE_UNKNOWN / RUNNING | 当前实例结果不确定或执行中；保留原状态，先定位该实例 |
| BACKFILL_UPSTREAM_NOT_SUCCESSFUL | 同周期依赖没有成功；先处理依赖。跨周期不受此限制 |
| BACKFILL_CONFIGURATION_CHANGED | 提交配置/SQL/时区与运行服务快照不同；确认部署一致后提交新批次 |
| BACKFILL_RETIRED_INSTANCE | 窗口属于旧部署退休历史；此命令不强制覆盖 |
| BACKFILL_WINDOW_CHANGED | 当前窗口定义与保存记录不一致，核对任务/时区配置 |

失败后的新请求不会自动追补原请求；原批次保留当时执行事实。
UNKNOWN 的人工处理沿用 `--inspect-instance`、`--resolve-instance` 流程。

服务日志使用 `BACKFILL_WINDOW_STARTED`、`BACKFILL_WINDOW_FINISHED` 和
`BACKFILL_QUEUE_UNAVAILABLE` 事件，关联批次号、实例和 query_id。
开启 `logging.debug_enabled: true` 并重启后，仅打印失败的清理/汇聚 SQL 和详细报错；
成功 SQL 不打印，任务与阶段信息仍保留。

## 调度和数据边界

- 每轮先处理正常调度，再最多处理一个补算窗口。无第二个 SQL writer，但单个慢补算
  会推迟下一轮；沿用默认超时，清理和汇聚各最多 300 秒。
- 显式补算直接使用已有源数据，不等待历史 DWD 信号。同周期依赖保留；
  批次内优先看本次依赖结果，防止拿旧 SUCCESS 掩盖本次补算失败。
- 可以显式处理覆盖起点之前的完整窗口，不修改正常游标、覆盖起点或退休记录。
  源数据若已老化，SQL 成功也可能写入零行；SUCCESS 表示 SQL 执行成功，不保证业务行数。
- 重算沿用现有“清理目标窗口、再 INSERT”的两阶段方式，二者不是数据库事务。
  中途失败可能暂时留下空窗口；中断或结果不确定时保留 UNKNOWN，待人工确认。
- 现有清理作用于 `<target_table>_local`，没有跨分片 `ON CLUSTER`。
  使用 Distributed 多分片部署时，应先由数据库维护方确认此清理范围覆盖实际数据；
  `mutations_sync=2` 不能单独证明跨分片重复执行安全。本功能不创建或调整表结构。

## 状态卷与恢复

请求和分页结果位于状态目录的 `backfill.sqlite3`，执行事实仍写入 `state.sqlite3`，
旧执行历史不会删除。服务持有主账本锁，命令只通过独立请求队列提交，因此可以在线使用。
SQL 成功已记入主账本但队列结果更新失败时，再次消费会同步原结果，不再执行 SQL。
已登记派发但进程中断的补算窗口恢复为 UNKNOWN，不自动重跑。

两个库都应放在同一持久卷，在同一个部署内保留。提交到未共享卷的备用 Pod，
请求不会自动转发到主 Pod；服务未运行时请求会一直排队。
现有每日归档只备份主状态库，**不包含请求库**；迁移、备份或恢复整个运行目录时，
应同时保留请求库及 SQLite WAL 文件，或停服务后制作一致副本。丢卷无法保证请求恢复。
