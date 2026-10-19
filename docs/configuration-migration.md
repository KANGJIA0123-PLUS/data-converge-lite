# 保留执行账本的配置升级

有可信旧配置时，可使用普通配置迁移保留历史连续执行，例如在原有 52 个任务上增加 4 个任务，或修改 SQL 后让新口径从明确时间开始生效。旧发布资料无法找回时，正常服务启动也可恢复新窗口：先备份并保留隔离所有旧实例，再从新的切点运行。两条路径都保留原账本，不替操作者判定历史 `UNKNOWN` 的真实结果。

若切换到另一份业务数据，应使用独立状态库和独立选主命名空间。不要删库、覆盖指纹、重置覆盖起点或清空 Redis 注册项来绕过保护。

## 术语与保留范围

| 名称 | 含义 |
|---|---|
| 业务数据 | ClickHouse 中的原始数据和汇聚结果。连接成功或表名相同不能证明仍是同一份业务数据。 |
| 账本 | `state.sqlite3`，记录任务实例、成功与不确定结果、尝试历史、调度游标、DWD 信号、覆盖起点和选主身份。 |
| 旧绑定 `binding` | 旧版本使用的整体配置摘要。包括任务定义等配置，但不包含 SQL 正文，不能据此还原或证明旧 SQL。 |
| 配置快照 / 版本 | 明确字段组成的配置记录及其摘要，包括 SQL 摘要和任务版本。任务定义或 SQL 改变，会使依赖它的下游任务版本一起变化。 |
| `coverage_start` | 整份账本允许处理的最早数据窗口起点，迁移不会将它重置。 |
| 每任务起点 | 当前任务版本允许处理的最早 `data_start`。未变化的任务保留原起点，新增或变化任务采用本次指定起点。 |
| `PLAN_ID` | 对原账本绑定、版本、目标配置、起点、任务约束和回填意图形成的预览摘要。用于防止应用已经过时的计划。 |

普通迁移不会重写原实例 ID，不删除成功、`UNKNOWN`、尝试记录或 DWD 信号，保留未变化及已移除任务的游标，也不更换原选主账本身份。新增或变化任务的游标会在普通迁移事务内重置，让调度从新起点按新 Cron 规则规划；原游标保存在迁移前备份中。普通迁移新增下游任务可以使用当前执行范围内上游已成功的窗口，不会为了新增下游而自动重算上游。

配置快照不保存明文密码、密钥或 SQL 正文。预览和日志只展示安全变更分类、任务名及摘要，不输出连接地址、账号等快照原文。SQLite 快照及备份仍包含运行配置和执行历史，应按账本管理其访问权限。

## 没有旧版本资料：优先恢复新窗口

接受“旧窗口和 UNKNOWN 保留待核查，暂不自动补算”时，不需要先找到旧镜像、旧 YAML 或旧 SQL。继续使用原状态卷、原选主配置和原 Redis 永久注册，安装当前版本后按原部署方式正常启动服务即可：

```bash
bash /opt/its/data-converge/bin/start.sh
```

自动恢复只发生在正常启动服务时。`--run-once`、`--run-task`、`--resolve-instance` 和 `--init-state` 都不会隐式建立新的执行范围。第一次正常启动前，`--diagnose` 仍可能显示旧绑定不一致；普通 `--plan-config` 也可能返回 `CONFIG_PREVIOUS_CONFIG_REQUIRED`，这些结果不代表必须先找回旧资料才能恢复新窗口。

此路径适用于没有新配置快照、没有 `configuration_required` 标记、旧绑定是有效摘要但与当前配置不一致、原覆盖起点有效的旧账本。已有新快照的配置变更继续使用普通迁移；元数据损坏、历史隔离结构缺失、备份失败、写锁冲突或选主身份冲突仍会阻止写入。

正常启动先取得原租约和物理账本文件锁，核对原 cluster、账本身份及 Redis 永久注册；身份冲突在备份和事务之前拒绝。备份成功后重新取当前时间，选择以下时间中最晚的一个，再向上对齐五分钟，作为恢复切点 `cutover`：

- 备份完成后的当前时间和原 `coverage_start`。
- 所有旧实例的最大 `data_end`。
- 所有旧 DWD 信号的 `window_start + 5 分钟`。

因此，备份耗时跨过窗口边界或账本中已有未来信号时，切点也会相应后移；预览不提供可当作已应用结果的估计切点。事务记录最终切点、备份路径和恢复摘要，将全部现存实例标为历史隔离范围，建立当前配置快照，将全部任务起点设置为切点并清除原调度游标。原游标保存在一致性备份中，原 `coverage_start`、实例、尝试、检查点、人工记录、事件和 DWD 原始信号都保留。

恢复后的全部任务只处理 `data_start >= cutover` 的完整窗口。五分钟任务从切点之后的新窗口推进；日、月任务会等待下一个完全位于切点之后的窗口，不补算跨越切点的半天或半月。旧未来信号也不会被用于切点之前的新任务窗口。历史成功记录不能作为当前任务已完成或依赖已满足的凭证；旧 `PENDING`、`BLOCKED`、`FAILED`、`UNKNOWN`、`RUNNING` 以及 `PREPARED` 记录都不会自动恢复或补算。

这一处理不证明新旧业务数据相同。恢复审计明确保存 `identity_verified=false` 和 `previous_definition_available=false`，不会伪造旧配置或旧 SQL 基线。历史不确定结果继续保留待核查，当前工具拒绝对已隔离实例执行重试、人工 resolve 或状态推进；先按原 `query_id` 与业务数据收集证据，再单独制定后续处理方案。

恢复会写入旧二进制无法匹配的账本绑定，使不理解历史隔离的旧程序拒绝启动。共享该账本的服务副本必须使用支持历史隔离的新版本；不要删除隔离标记或手工修改绑定让旧程序绕过保护。恢复事务完成后，如果正常选主绑定阶段短暂网络失败，已建立的恢复基线会保留，此次不会发送 SQL，下次正常启动继续使用该基线，不会再次隔离旧记录或重复备份。

启动后用 `--diagnose`、`--status`、`--inspect-instance` 和 `--recovery-plan` 核对：

- 当前任务结果与历史计数分开展示。历史问题为 `WARN`，注明“当前不会自动处理”；历史 `RUNNING` 只是原记录，不代表当前进程仍在执行。
- `--failures` 只列当前执行范围的异常，并附历史计数。`--inspect-instance` 仍能查看旧实例、原状态和执行证据，但跳过当前依赖解释，不建议对历史实例重试或 resolve。
- `--recovery-plan` 的 `AUTO_RESUME` 仅包含当前实例，另列历史计数；历史 `PREPARED` 不会成为自动恢复候选。
- `--diagnose` 与默认 `--dwd-status` 只检查切点之后的完整窗口。`--dwd-status --window` 仍可精确查看切点前的原始信号，标为 `historical=true`，缺信号不会被当成当前就绪告警。
- `--status` 保留原始全量字段，并附恢复摘要、当前计数和历史计数。以成功日志 `LEGACY_NEW_WINDOWS_RECOVERED` 或账本审计中的实际切点和备份路径为准。

后续正常重启仅处理当前执行范围。历史映射、隔离标记或关联审计丢失时会拒绝执行，不能降级为普通旧账本继续运行。恢复不提供跨 ClickHouse、SQLite 和 Redis 的统一回滚；产生新业务写入后，仍不能单独回滚 SQLite 副本。

## 普通迁移首次使用旧账本需要哪些材料

若要保留旧窗口的连续执行范围，通过 `--plan-config` / `--apply-config` 迁移没有配置快照的旧账本，必须提供可信的 `--previous-config`。工具会按旧算法重新计算该配置的绑定，并要求它与账本中的旧绑定完全匹配。

必须从实际部署过的旧镜像、发布包或可信归档取得以下材料：

1. 旧应用配置，包括旧连接身份和 DWD 设置。
2. 旧任务规则文件。
3. 该旧版本实际使用的全部 SQL 文件。

只有旧 YAML、任务名称列表或“之前大约有 52 个任务”都不足以建立可信基线。不要用当前 SQL 冒充旧 SQL，也不要按旧摘要反向拼凑配置。没有可信旧材料时，普通迁移保留此阻断；若优先恢复新窗口，可使用上面的正常启动路径，保留隔离旧窗口。

建议把整套旧资源放在独立只读目录，并在用于预览的旧配置中明确引用旧规则和旧 SQL 的绝对路径，例如：

```yaml
config:
  convergence_rules_path: /srv/data-converge/releases/old/resources/config/convergence_rules.yaml
  sql_directory: /srv/data-converge/releases/old/resources/sql
```

工具会为旧配置单独解析资源：完整旧发布目录中的 `resources/config/app_config.yaml` 使用默认路径或 `resources/...` 写法时，会定位到该旧发布的 `resources` 目录。其他目录布局如果仍省略路径或使用这类写法，会报 `CONFIG_PREVIOUS_RESOURCES_REQUIRED`，不会退回当前安装包。仍建议使用明确的旧资源绝对路径；仅复制旧应用 YAML，不能替代完整旧规则与 SQL。

首次旧账本应用还需要 `--accept-legacy-baseline`。它表示操作者确认这些旧资产可信；工具仍不会声称旧绑定证明了 SQL 正文。已有正式配置快照的账本直接使用已保存的快照，不需要人为重建旧 SQL 基线。

## 普通迁移的预览与应用流程

以下命令在服务容器中执行。目标配置的 `state.directory` 必须继续指向原状态卷及原账本。示例路径需要替换为现场真实路径。

### 1. 保存旧资产并预览

假设已从可信旧发布确认原有 52 个任务，新发布共 56 个任务，且 4 个新增任务从维护窗口之后开始处理。选择一个带时区、位于未来、分钟能被 5 整除且秒和微秒均为零的起点：

```bash
bash /opt/its/data-converge/bin/start_data_converge.sh \
  --config /srv/data-converge/releases/new/app_config.yaml \
  --plan-config \
  --previous-config /srv/data-converge/releases/old/app_config.yaml \
  --effective-from '2026-10-20T10:00:00+08:00' \
  --format json
```

日期仅为例子，执行时应重新选择足够晚的未来时间。新增或改变任务时必须指定 `--effective-from`；仅连接身份变更且任务版本未变时，无需人为改变任务起点。

`--plan-config` 是只读操作：不创建或升级状态表，不获取写租约，不创建业务日志、心跳或备份，不发 SQL，不消费或确认 DWD 消息。它读取目标配置、SQL 文件及账本；预览不能替代对业务数据归属的核实。

检查输出中的任务新增、变化、移除列表，所需确认项、阻断原因、任务起点和受影响任务的未完成数量。SQL 改动会显示为任务版本变化，并传播至下游，即使任务数量仍为 56。

### 2. 停止写服务，处理阻断项

应用迁移需要独占写资格。停止原写服务及可能接管的副本，确认它们已正常退出并释放租约和物理账本文件锁，再以相同参数重新预览，使用这次输出的 `PLAN_ID`。

当前执行范围的残留 `RUNNING` 会阻止普通迁移。迁移不会调用正常启动的恢复流程，也不会把 `RUNNING` 自动改为 `UNKNOWN` 或重新排队。已通过新窗口恢复隔离的历史实例不属于当前执行范围，不会永久阻止后续配置迁移。

| 阻断情况 | 处理原则 |
|---|---|
| 旧配置绑定不匹配或缺少可信旧资产 | 普通迁移需要补齐证据；资料无法找回且优先恢复新窗口时，使用正常启动的历史隔离恢复路径。 |
| 改变或移除的任务存在非 `SUCCESS` 实例 | 先处理其未完成执行事实；包括 `PENDING`、`BLOCKED`、`FAILED`、`UNKNOWN` 等状态。 |
| 新起点早于全局覆盖起点 | 重新选择合法起点，不能通过迁移扩大账本覆盖范围。 |
| 变化或重新加入的任务会覆盖已有窗口 | 起点不得早于该任务已记录窗口的最大 `data_end`，避免复用旧实例身份。 |
| 已有非空 `dataset_id` 被改变或删除 | 使用另一份账本与独立选主命名空间。 |
| 已绑定选主被关闭或改名 | 恢复原选主身份；迁移不提供绕过开关。 |
| Redis 中的原账本永久注册保护不满足 | 核查原选主配置与注册信息，不清空注册项。 |

未受配置变化影响的旧任务可以继续保留 `UNKNOWN`。例如只新增四个任务，无需把所有旧任务的未知结果“清零”；但依赖这些未知窗口的下游仍可能被阻塞。

### 3. 应用同一计划

旧账本首次迁移示例：

```bash
bash /opt/its/data-converge/bin/start_data_converge.sh \
  --config /srv/data-converge/releases/new/app_config.yaml \
  --apply-config PLAN_ID \
  --previous-config /srv/data-converge/releases/old/app_config.yaml \
  --effective-from '2026-10-20T10:00:00+08:00' \
  --accept-legacy-baseline \
  --backup-dir /srv/data-converge/migration-backups
```

将 `PLAN_ID` 替换为最后一次预览给出的值。应用命令直接输出 JSON，不接受诊断专用的 `--format` 参数。已有配置快照时，按实际预览省略旧配置和旧基线确认参数。仅添加预览要求且已经核实的确认项：

| 参数 | 操作者确认的事实 |
|---|---|
| `--accept-legacy-baseline` | 首次迁移使用的旧规则和旧 SQL 来自真实、可信的旧版本。 |
| `--confirm-same-data` | 连接身份变化、DWD 变化或首次建立 `dataset_id` 标签后，仍对应同一份业务数据。 |
| `--confirm-readiness-change` | DWD 定位或通知配置变化后，通知仍属于同一份业务数据；必须同时确认 `--confirm-same-data`。 |

这些参数不是通用放行开关。连接成功、`SELECT 1` 成功、数据库名或表名相同，都不能替代业务归属确认。不同业务数据不得继续使用旧成功记录来判定已经处理。

应用会取得要求的租约和原账本文件锁，重新计算并核对计划，拒绝残留 `RUNNING`，先生成一致性备份，再用一次事务提交新配置、任务起点、相关游标重置和迁移审计。期间不会开始业务调度。

如果计划已过时，重新预览，不要手工修改计划 ID。如果没有允许回填且应用时生效时间已经过去，应重新选择未来起点再预览。重复应用同一个已成功计划会返回已应用结果，不会重复覆盖迁移历史或重新执行业务 SQL。

## 未来生效与显式回填的区别

未来起点表示：新增任务只处理从指定时间开始的完整窗口。原有未变任务仍按原游标和原起点推进，历史成功记录保留。

例如新增五分钟任务起点为 `10:00+08:00`，首个可处理窗口为 `10:00–10:05`，不是 `09:55–10:00`。小时、天、月任务也要求整个窗口的 `data_start` 不早于起点；中午生效的日任务不会补算当天零点开始的部分历史窗口。

若确实需要新增任务处理过去窗口，应在预览和应用时都显式加 `--allow-backfill`，并选择合法的过去起点：

```bash
bash /opt/its/data-converge/bin/start_data_converge.sh \
  --config /srv/data-converge/releases/new/app_config.yaml \
  --plan-config \
  --previous-config /srv/data-converge/releases/old/app_config.yaml \
  --effective-from '2026-10-19T00:00:00+08:00' \
  --allow-backfill \
  --format json

bash /opt/its/data-converge/bin/start_data_converge.sh \
  --config /srv/data-converge/releases/new/app_config.yaml \
  --apply-config PLAN_ID \
  --previous-config /srv/data-converge/releases/old/app_config.yaml \
  --effective-from '2026-10-19T00:00:00+08:00' \
  --allow-backfill \
  --accept-legacy-baseline \
  --backup-dir /srv/data-converge/migration-backups
```

`--allow-backfill` 只允许过去的任务起点，不能越过 `coverage_start`、覆盖原记录窗口、绕过未确认执行结果或业务身份保护。已经恢复新窗口的账本，后续普通迁移也不能把任何任务起点移到恢复 `cutover` 之前，即使使用 `--allow-backfill`；计划中的 `recovery_cutover` 明示这一额外下限，违反时报告 `CONFIG_EFFECTIVE_BEFORE_COVERAGE`。它也不会让迁移命令立即执行 SQL；合法写服务随后启动时才按调度、依赖和 DWD 就绪条件处理窗口。回填新增下游所需的原上游窗口必须在当前执行范围内已有成功事实；迁移不会自动重算原上游。

Cron、DWD 事件入队、手动执行和待执行任务恢复均遵守任务起点。迟到的 DWD 通知不会使新增任务回到起点之前；旧账本没有每任务起点时，继续保持原有行为。

## 迁移后的诊断和执行记录

迁移成功后先做只读诊断，再按原部署方式启动服务：

```bash
bash /opt/its/data-converge/bin/start_data_converge.sh \
  --config /srv/data-converge/releases/new/app_config.yaml \
  --diagnose --format json

bash /opt/its/data-converge/bin/start_data_converge.sh \
  --config /srv/data-converge/releases/new/app_config.yaml \
  --history --limit 20 --format json

bash /opt/its/data-converge/bin/start_data_converge.sh \
  --config /srv/data-converge/releases/new/app_config.yaml \
  --recovery-plan --format json
```

`--history` 展示执行尝试，`--recovery-plan` 展示需要恢复或人工核对的执行实例；它们不是配置迁移命令。配置迁移记录及各历史快照保存在账本中。保留迁移输出里的计划 ID、配置版本和备份路径，便于与该账本关联核对。

正常启动会检查当前配置与批准快照是否一致，SQL 正文变化也会要求显式迁移。日志等级、并发等运行参数和密码内容轮换不属于任务语义变化。原旧账本仍可按原绑定兼容运行，但旧绑定不能提供 SQL 正文已经批准的证明。

运行期间 SQL 文件若被替换为未批准内容，执行会以 `SQL_CONFIGURATION_CHANGED` 拒绝发送，并记录为确定未提交的失败。不要靠自动重试反复执行。先恢复批准的 SQL，并按既有规则处理该失败实例；若要采用新 SQL，仍须先结清原任务的未完成实例，再生成新的配置迁移计划。

`APPLIED`、启动成功、获得租约或本地心跳正常，都不表示历史 `UNKNOWN` 已解决，也不证明汇聚结果正确。继续根据 query_id、远端查询结果与目标时间窗口核对异常，详见 [诊断说明](diagnostics.md) 和 [可靠性边界](reliability.md)。

## 备份与恢复限制

应用前备份使用 SQLite 一致性备份接口，拒绝覆盖已有文件，权限设为 `0600`，完成后运行 `quick_check`。备份失败时不提交配置事务；事务失败时配置、绑定、任务起点、游标重置和审计一起回滚。备份不能作为与原账本同时运行的第二份账本。

正常服务默认每天额外保存一份压缩快照，归档保留 30 天。默认 `configuration-backups` 中由当前账本审计引用的迁移备份满 24 小时后压缩，超过保留期限后清理；自定义外部 `--backup-dir` 不自动处理。原审计中的 `backup_path` 不改写，`--diagnose` 的 `state.archives` 检查附实际压缩路径或已到期缺失状态。活动账本的历史事实和审计不随归档文件到期删除。配置、清理边界与恢复限制见[状态库归档指南](state-archives.md)。

备份只包含 SQLite 账本，不包含 ClickHouse 业务数据、发布资产、初始化标记或 Redis 永久账本注册项。`quick_check` 检查 SQLite 一致性，不验证业务数据正确性。

如需恢复，先停止所有写入者并核查是否已经有新 ClickHouse 写入。只有在确认业务侧和执行事实仍与备份一致，并配套恢复匹配的代码、规则、SQL 和配置时，才可评估恢复账本副本。已有新 ClickHouse 写入后，不能单独回滚 SQLite：旧副本可能把已经执行的窗口重新视为未执行，造成重复汇聚。

这套迁移不提供跨 ClickHouse、SQLite 和 Redis 的统一回滚事务。不要复制备份启动另一套同业务写服务，也不要通过更换状态卷或选主命名空间来规避原账本中的未知结果。
