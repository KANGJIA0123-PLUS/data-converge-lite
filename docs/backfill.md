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
默认不接受未来结束时间；需要测试当前未结束窗口时显式使用下节的 `--preaggregate`。
不接受空范围或超过 **10000 个任务窗口**的请求。超过限制请拆分日期。
例如 60 个任务各一个窗口计 60 项，而非一项。

## 提前汇聚当前小时或当天

例如在 2026-10-21 白天，用当前已经存在的小时数据测试当天的日汇聚：

```bash
bash /opt/its/data-converge/bin/start_data_converge.sh \
  --backfill --task task_model_instance_day_dws \
  --from '2026-10-21T00:00:00+08:00' \
  --to '2026-10-22T00:00:00+08:00' \
  --preaggregate --dry-run
```

去掉 `--dry-run` 提交。提前测试当前 11:00–12:00 小时，换成小时任务和范围：

```bash
bash /opt/its/data-converge/bin/start_data_converge.sh \
  --backfill --task task_model_instance_hour_dws \
  --from '2026-10-21T11:00:00+08:00' \
  --to '2026-10-21T12:00:00+08:00' \
  --preaggregate
```

五分钟窗口同理，起止仍必须对齐周期。该模式写入**同一目标表、同一窗口**，继续使用
原业务 SQL 和完整窗口参数；它不预测未来数据，也不改变速率、均值等原有口径。
计划中已结束窗口仍按普通补算处理，未开始窗口列入 `omitted_future`；未结束窗口标记
`execution_kind=PREAGGREGATE`。提前日汇聚不自动先执行小时任务，要用现有小时数据，
或先提交需要的小时汇聚。

提前成功不会给正式实例记 SUCCESS，也不会推进正常调度游标。正式到期后仍执行原有
“清理该窗口再汇聚”，覆盖测试结果；多次提前测试也按相同方式覆盖。排队直到窗口已结束
才轮到的提前请求会 SKIPPED，不允许晚到的测试覆盖正式结果。

为保证到期可覆盖，提前窗口必须完整落在账本覆盖起点和任务生效起点之后，当前配置和
游标还必须能正常生成对应正式实例。因此本模式连预览也要求已有状态库；服务当天中途
新建状态库时，可能拒绝当天整日提前汇聚。不得靠删除状态库绕过。提前写入后，影响其
正式覆盖的配置修改会被拒绝，待正式覆盖后再修改。

提前 UNKNOWN 或重启恢复的未确认提前写入会阻止同窗口正式执行。先凭 query_id 核实
远端清理和汇聚都已结束、结果如何，然后在获得状态写锁的维护窗口执行：

```bash
dconv --resolve-preaggregate 'bf_实际批次号:序号' \
  --resolution success --note '已核实清理及汇聚结束，目标窗口结果符合预期'
```

`success` 只确认提前结果，不代表正式完成。确认原操作已结束且允许重新执行时用
`--resolution retry`；它只解除阻断，不自动重放原请求。需要再次提前测试时提交新批次。
历史保留原 UNKNOWN、人工处理和实际正式覆盖时间，可用 `--history`、
`--inspect-instance`、`--failures` 查看。

如果后续**正式执行**也返回 UNKNOWN，人工 `--resolve-instance ... --resolution success`
只记录核查结论，不填写 `covered_at`。此时先保留原配置，再显式 `--backfill --recompute`
该窗口；真实汇聚成功后解除配置覆盖保护。原提前请求不会因此自动重放。
已有提前写入的窗口尚未结束时，旧 `--run-task` 入口会拒绝提前标记正式完成，
继续提前测试应使用 `--preaggregate`。

## 正常小时、天任务最多额外等待 5 分钟

`app_config.yaml` 中配置：

```yaml
scheduler:
  cross_period_wait_enabled: true
  cross_period_wait_seconds: 300
  cross_period_wait_overrides: {}
  # 按需覆盖某个任务，例如等待 10 分钟：
  # cross_period_wait_overrides:
  #   task_model_instance_day_dws: 600
```

省略这些字段也默认开启、等待 300 秒；小时、天、月等跨周期任务统一适用。
例如原定 00:10 执行，上游未齐就等到最晚 00:15，00:13 齐全则下一轮即可执行；
到 00:15 仍未齐会告警并使用已有源数据。这里的“齐全”只认可正式上游 SUCCESS。
同周期依赖、DWD 和 UNKNOWN 等其他门槛仍然有效，超时不保证立刻执行。

开关为 false 或等待秒数为 0 时直接放行跨周期检查。配置随服务启动读取；重启后的
新时长只用于新入账的正常窗口，已有窗口保留原截止时间；关闭开关会放行已有等待，
重新开启也不续时。旧账本没有可信 Cron 锚点的实例维持原直接执行行为并记录日志。
显式补算和提前测试不受此软等待限制。超时汇聚成功后，上游迟到不会自动再汇聚，
请用 `--backfill --recompute` 修正。

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
| COMPLETED | 全部窗口成功或跳过；跳过可能是已有 SUCCESS，也可能是提前请求已过期 |
| COMPLETED_WITH_ERRORS | 全部处理完，但包含 FAILED、UNKNOWN 或 BLOCKED |

进度中的 `completed` 表示已处理条目数，包括失败和阻断，不等于成功条目数。
当前正在执行的条目在结果落盘前仍计入 QUEUED；查询显示的总数、计数和分页来自同一快照。
每个条目保留窗口、任务名、状态、错误码和执行后的 query_id。

| 条目状态/错误 | 处理方式 |
| --- | --- |
| SKIPPED | 已成功且未选重算，或提前请求过期；结合 error 判断，不能一概当作本次 SQL 成功 |
| FAILED | 已知执行失败；修复原因后发起新批次，不自动进入普通重试 |
| UNKNOWN | 请求可能已经送达 ClickHouse，先核实 query_id；不能直接重跑 |
| BACKFILL_INSTANCE_UNKNOWN / RUNNING | 当前实例结果不确定或执行中；保留原状态，先定位该实例 |
| BACKFILL_UPSTREAM_NOT_SUCCESSFUL | 同周期依赖没有成功；先处理依赖。跨周期不受此限制 |
| BACKFILL_CONFIGURATION_CHANGED | 提交配置/SQL/时区与运行服务快照不同；确认部署一致后提交新批次 |
| BACKFILL_RETIRED_INSTANCE | 窗口属于旧部署退休历史；此命令不强制覆盖 |
| BACKFILL_WINDOW_CHANGED | 当前窗口定义与保存记录不一致，核对任务/时区配置 |

失败后的新请求不会自动追补原请求；原批次保留当时执行事实。
普通 UNKNOWN 的人工处理沿用 `--inspect-instance`、`--resolve-instance` 流程；
PREAGGREGATE 使用独立 `--resolve-preaggregate`。

服务日志使用 `BACKFILL_WINDOW_STARTED`、`BACKFILL_WINDOW_FINISHED` 和
`BACKFILL_QUEUE_UNAVAILABLE` 事件，关联批次号、实例和 query_id。
默认以 ERROR 打印失败的清理/汇聚 SQL、参数、详细报错和失败阶段，无需开启 DEBUG；
成功 SQL 不打印。`logging.debug_enabled: true` 仅额外提供调用栈和阶段调试记录。

## 调度和数据边界

- 每轮先处理正常调度，再最多处理一个补算窗口。无第二个 SQL writer，但单个慢补算
  会推迟下一轮；沿用默认超时，清理和汇聚各最多 300 秒。
- 显式补算直接使用已有源数据，不等待历史 DWD 信号。同周期依赖保留；
  批次内优先看本次依赖结果，防止拿旧 SUCCESS 掩盖本次补算失败。
- 可以显式处理覆盖起点之前的完整窗口，不修改正常游标、覆盖起点或退休记录。
  源数据若已老化，SQL 成功也可能写入零行；SUCCESS 表示 SQL 执行成功，不保证业务行数。
- 重算沿用现有“清理目标窗口、再 INSERT”的两阶段方式，二者不是数据库事务。
  中途失败可能暂时留下空窗口；中断或结果不确定时保留 UNKNOWN，待人工确认。
- 清理表从实际 SQL 开头的 `INSERT INTO [TABLE] [库名.]表名` 推导：保留库名，只给表名加 `_local`，不依赖规则里的 `target_table`。支持前导注释及带引号的标识符；表函数、动态表名或其他无法识别的写入形式会在清理前报 `SQL_PREFLIGHT_FAILED`，不会提交 DELETE。该解析只检查写入目标，不验证整条 SQL。
- 现有清理仍没有跨分片 `ON CLUSTER`，`_local` 表的命名约定和清理列配置仍须与实际表结构一致。
  使用 Distributed 多分片部署时，应先由数据库维护方确认此清理范围覆盖实际数据；
  `mutations_sync=2` 不能单独证明跨分片重复执行安全。本功能不创建或调整表结构。

## 状态卷与恢复

请求和分页结果位于状态目录的 `backfill.sqlite3`，正式和提前执行事实写入 `state.sqlite3` 的独立表，
旧执行历史不会删除。服务持有主账本锁，命令只通过独立请求队列提交，因此可以在线使用。
SQL 成功已记入主账本但队列结果更新失败时，再次消费会同步原结果，不再执行 SQL。
已登记派发但进程中断的补算窗口恢复为 UNKNOWN，不自动重跑。

两个库都应放在同一持久卷，在同一个部署内保留。提交到未共享卷的备用 Pod，
请求不会自动转发到主 Pod；服务未运行时请求会一直排队。
现有每日归档只备份主状态库，**不包含请求库**；迁移、备份或恢复整个运行目录时，
应同时保留请求库及 SQLite WAL 文件，或停服务后制作一致副本。丢卷无法保证请求恢复。
