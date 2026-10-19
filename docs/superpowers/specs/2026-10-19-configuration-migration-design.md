# 配置演进与保留账本的迁移

> 历史设计保留。2026-10-19 后续按用户请求临时移除选主账本身份及 Redis 永久注册阻断；本文相关身份准入约定已被取代（superseded），配置快照和业务身份迁移要求仍保留。当前行为和遗留问题见 [DC-STATE-001](../../known-issues.md#dc-state-001)。

用户已同意实现讨论中的方案：保留业务身份保护，允许正常配置升级，保留执行事实，并明确新增/变更任务的生效窗口。不是自动清库或无条件覆盖旧指纹。本设计作为实施契约，不要求用户重复确认已授权的实现。

## 不变量

- 不修改业务 SQL、公司部署/打包/启动脚本，不增加产品依赖。Python 函数和测试函数不超过 50 行。
- 旧 binding 算法保持原样，继续用于旧账本证明与兼容；新配置快照采用显式字段和 schema_version=1，不对 dataclass 内部字段自动做身份推导。
- 原实例 ID、成功记录、UNKNOWN、尝试记录、未变化任务的游标、coverage_start、选主账本身份不因迁移被重置。新增/变化任务的旧游标保存在迁移前备份中，迁移事务清除其当前游标，使新版本从指定起点按新 Cron 规划。迁移不发业务 SQL、不恢复 RUNNING、不消费 DWD。
- 只读预览不创建表、日志、心跳或备份。写迁移必须取得原有租约和物理账本文件锁，拒绝残留 RUNNING，重新计算预览 ID，备份后用事务提交配置、任务起点、审计。
- 连接身份变更必须由操作者确认仍属于同一业务数据；工具不会声称 SELECT 1 或相同表名能证明数据归属。不同业务数据/已有 dataset_id 改变必须使用独立账本与独立选主命名空间。

## 配置快照

`config/evolution.py` 提供 `build_manifest(definition) -> dict`、`manifest_digest(manifest) -> str`、`compare_manifests(previous,current) -> dict`。

Manifest 固定字段：schema_version=1，dataset_id（可选 state.dataset_id；1..128 ASCII 字母数字 _ . -，省略为 null），connection（endpoint/database/username），readiness（enabled 与启用时 queue_key、Redis定位字段；不存密码、密钥、SDK对象），coordination（enabled/cluster_name），tasks（按名称映射）。任务项包含 definition（taskname/source_tables/target_table/aggregation_interval/cron/depends_on/sql_id）、sql_sha256（load_sql 返回的 stripped UTF-8 SQL 摘要）、revision（自身定义/SQL摘要及排序的上游revision摘要）。变更向下游传播。description、运行参数、明文密码不进入快照。

Diff 固定字段：added/changed/removed（排序的任务名列表），connection_changed/readiness_changed/coordination_changed/dataset_changed（bool）。SQL修改必须可检测。日志和预览只输出安全变更分类、任务名和摘要，不输出快照原文。

## 账本持久化

`state/configuration.py` 独立负责可选配置表/元数据。旧库只读查询时不存在即返回 None，不做DDL。

StateStore 新接口：
- `configuration_snapshot() -> dict | None`，返回 manifest/revision/task_starts。
- `configuration_history(limit=20) -> list[dict]`，有限审计记录。
- `get_task_effective_starts() -> dict[str, datetime]`，旧库返回空映射。
- `configuration_constraints(tasknames) -> dict`，返回 running 总数和 tasks[name]={unresolved,latest_end}；unresolved 是状态非 SUCCESS 的实例计数，latest_end 以时间值比较而非字符串字典序。
- `backup_configuration(destination: Path) -> Path`，持写锁、拒绝覆盖，SQLite backup 一致性副本，0600，验证 quick_check，失败只清理本次新文件。
- `apply_configuration(*, expected_binding, expected_revision, manifest, revision, task_starts, binding, migration_id, applied_at, backup_path=None, reset_task_cursors=()) -> None`，持写锁，在事务内CAS检查当前binding/revision，原子保存快照、必需快照标记和迁移历史，仅清除显式列出的新增/变更任务游标；未变化任务游标不变。

快照元数据仅保存脱敏快照。迁移历史包含 migration_id、前后revision、applied_at、backup_path、manifest、task_starts。重复migration_id不能覆盖历史。新增任务起点和任务版本在快照历史中持久保留，旧实例ID无需改写。

## 迁移策略和接口

新 CLI：`--plan-config`（只读诊断）、`--apply-config PLAN_ID`（独占写操作）。二者支持 `--previous-config PATH`（旧账本所需的可信旧配置，包含旧SQL资源路径）、`--effective-from ISO`（有新增/变更任务时必填，带时区的五分钟边界）、`--allow-backfill`（显式允许过去起点）。apply 另接受 `--confirm-same-data`、`--confirm-readiness-change`、`--accept-legacy-baseline`、`--backup-dir PATH`。不提供 force 绕过。

旧账本无快照：必须提供 previous-config 且用现有 binding 算法计算结果等于原binding；不能通过当前任务实例名称猜旧配置。旧binding不包含SQL，因此apply需要accept-legacy-baseline，明确由操作者提供可信旧镜像/SQL基线。没有旧快照时不声称能证明旧SQL。旧binding匹配的原服务仍可按旧模式运行，不自动采纳新SQL快照。

新初始化账本同时保存新快照和各任务coverage_start，之后启动比较新快照以检测SQL变化；配置变化需显式预览/应用。配置不变的重启和日志/并发/密码轮换继续兼容。

预览展示差异、所需确认、阻断原因、任务起点、影响任务未完成计数。PLAN_ID覆盖旧binding/revision、目标manifest摘要、coverage、起点、差异、受影响任务约束及backfill意图，不依赖当前秒数。apply在锁内重新计算并比对ID；非backfill且生效时间已经过去时拒绝，要求重新选择未来起点。未变任务保留原起点；新增/变更任务取effective_from，不得早于全局coverage。变更/移除的旧任务若存在非SUCCESS实例则阻断，任何变更/重新加入的任务不得覆盖其已记录窗口（effective_from >= latest_end）。新增下游可依赖原任务旧成功窗口，但不自动重算原任务。

同库连接变化需要confirm-same-data；DWD变化需要confirm-same-data及confirm-readiness-change，确认仍是同一业务数据的通知（否则独立账本）。相同来源的已有信号继续作为历史事实，不清空。已有非空dataset_id改变/删除、已绑定选主的关闭/改名均拒绝；从无dataset_id建立标签需要same-data确认。更换Redis时仍必须满足原Redis账本注册保护，不能用迁移清除注册项。

## 所有执行入口遵守任务起点

每任务起点由task_starts保存。cron没有游标时从max(coverage,task_start)规划；DWD事件按每任务起点过滤；手动执行在创建运行资源前拒绝更早窗口；run_pending遇到非法旧窗口拒绝执行。旧库无task_starts时维持原行为。日/月等只处理data_start位于起点之后的完整窗口。

复审修正：不能让变更任务沿用旧 Cron 的未来游标。例如旧小时任务的下一游标为 12:03，新五分钟 Cron 在 11:00 生效；沿用旧游标会漏排 11 点整之后的窗口。计划显式列出 `restart_cursors`，迁移原子清除这些游标，首次调度按生效起点重新计算；原值仍可在一致性备份中核对。

SQLTaskRunner 可接收启动时的预期 SQL 摘要映射（保持旧构造调用兼容），执行时在发送前核对同一份已读取SQL文本的摘要。文件运行中被替换时返回确定未提交的固定错误，禁止执行未批准SQL。

## 恢复与验收

首次迁移必须有可信旧配置；生产没有旧配置时保守阻断并明确缺少的证据。无需清零不受影响任务的UNKNOWN。迁移成功后先只读诊断再启动；启动不代表旧UNKNOWN已经解决。已产生新ClickHouse写入后不能单独回滚SQLite副本。

验收：52→56保留旧事实；SQL变化和下游版本传播；同库连接确认；跨dataset拒绝；legacy缺证明拒绝；只读无DDL；CAS过期预览拒绝；备份/事务故障不改旧状态；未确认旧实例阻止任务语义变更；cron/DWD/手动/持久入队的起点一致；运行中SQL替换不发送；重复应用幂等；诊断及CLI兼容；全套与基线失败集合对比。
