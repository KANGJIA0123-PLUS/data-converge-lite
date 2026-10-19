# 旧资料缺失时恢复新窗口

用户明确要求“找不到旧版本也不能失败”，并确认“接受，优先恢复新窗口；旧窗口和 UNKNOWN 保留待核查，暂不自动补算”。因此本次提供正常服务启动的自动恢复路径，不再要求旧发布资料。该授权取代前次设计中“所有旧账本升级都必须有旧配置”的绝对要求；保留连续历史执行的普通 plan/apply 流程仍要求可信基线。

## 行为及边界

- 仅 `Application.serve()` 的正常启动（bootstrap=True）自动恢复；run-once/run-task/resolve/init-state 不隐式改变账本版本。
- 触发条件：旧 snapshot 不存在、configuration_required 不存在、旧 binding 为有效 64 位十六进制摘要且不等于当前 binding、coverage 为有效带时区五分钟边界。已有新快照的配置变化仍走普通迁移。损坏元数据、丢失历史隔离结构、写锁/选主/备份失败继续拒绝写入。
- 用户选择的是新窗口可用性，不是证明新旧业务数据相同。恢复审计明确 `identity_verified=false`、`previous_definition_available=false`，不得伪造旧配置或 SQL 基线。
- 先取得原写租约与文件锁；核对已绑定 cluster/ledger id 及 Redis 永久注册，不清空或替换原身份。绑定完成前不调用会写 metadata 的 bind_writer；正常启动之后沿用既有绑定。
- Redis `check_ledger` 通过只含 GET 的原子脚本同时校验租约和永久注册；无本地身份时只接受远端尚未注册，绝不领养其它账本身份。预检失败不写本地账本。恢复提交后正常 bind_writer 若遇网络失败，不发送业务 SQL，保留基线供下次启动继续。
- 在备份成功后取当前时间，cutover=向上对齐五分钟的 max(now, coverage, 所有旧实例 data_end, 所有旧 DWD 信号 window_start+5min)。旧未来信号不用于新窗口；已有原始 DWD 记录不删除。
- 一次事务记录全部现存实例的历史隔离映射、新配置快照、全任务 cutover、全游标清除及恢复审计。保留原 coverage、所有实例/attempt/checkpoint/resolution/event 事实和选主身份；旧游标在一致性备份中保留。
- 恢复后正常启动及后续重启仅处理当前执行范围。所有任务的完整窗口 data_start 必须>=cutover；天/月不会回算跨越切点的半窗口。
- 恢复后的 binding 使用 SHA-256("legacy-retired-v1:" + 原配置摘要) 形成版本栅栏，旧程序不能以原摘要接管账本。初始化、启动、后续普通迁移、幂等检查和诊断均按同一派生规则比较，未恢复账本保持原算法。
- 后续普通迁移仍保留恢复 cutover 为任务起点下限，即使 --allow-backfill 也不能回到已隔离历史。预览附 recovery_cutover 并报告 CONFIG_EFFECTIVE_BEFORE_COVERAGE；状态事务及启动完整性校验再次检查下限。

## 状态层接口

在 `state/retirement.py` 内实现独立历史隔离模块，StateStore 暴露：
- `legacy_window_summary() -> dict`：`counts` 全实例状态计数，`latest_end` 所有旧实例最大 data_end 或 None，`latest_signal_end` 最大 DWD window_start+5min 或 None；时间按datetime比较，拒绝无时区/坏时间。
- `legacy_recovery() -> dict|None`：metadata `legacy_recovery` 的恢复审计摘要。
- `get_retirement(instance_id) -> dict|None`：该历史实例的 migration_id/retired_at。
- `retired_status_counts() -> dict`、`active_status_counts() -> dict`；原 `status_counts()` 和原始实例历史查询保持全量。
- `validate_retirement() -> None`：核对持久标记、映射表、对应configuration_history、snapshot.retirement_id、映射数量/ID摘要及原始实例存在性，缺失或不一致报 `LEGACY_RETIREMENT_INVALID`。
- `apply_configuration(..., retire_existing=False, recovery=None)`：仅 `retire_existing=True` 使用此路径且 expected_revision必须None；同一事务创建 retired_instances(instance_id PK,migration_id,retired_at)，映射全部现存实例、写入 retirement_required 和 legacy_recovery、snapshot.retirement_id；后续普通apply保留retirement_id。recovery为下述固定安全字段。默认false保持前次API行为。

`recovery` 固定字段：schema_version=1、migration_id、cutover（ISO）、previous_binding、current_binding、revision、backup_path、recovered_at（ISO）、identity_verified=false、previous_definition_available=false、historical_counts（dict）。状态层追加 retired_count 和 retired_id_digest。ID摘要按排序ID逐项带分隔计算。原实例事实不变。

所有执行入口隔离：enqueue_and_advance、claim、prepare_attempt、mark_dispatched、finish、mark_blocked、resolve 拒绝 retired ID，固定码 `HISTORICAL_INSTANCE_RETIRED`。get_status 对历史返回None（不能作为当前依赖成功证据）；get_instance原样保留历史读；list_pending active-only。recover_running、checkpoints恢复分类、history.record_recovery、configuration.constraints全部排除retired，防止恢复时改写历史及后续配置迁移被历史问题永久阻断。旧库无隔离元数据时与原行为一致，只读不DDL。

## 应用与诊断

新增 configuration_migration/legacy_recovery.py，root负责识别合法候选、检查租约注册、备份、重读目标SQL/定义一致性、计算cutover、事务调用及安全日志。Application只在bootstrap且非created时尝试恢复，随后走正常快照/binding校验；所有启动校验调用validate_retirement。

成功日志 `LEGACY_NEW_WINDOWS_RECOVERED` 包含切点、备份路径（JSON转义）、前后摘要和历史计数，不打印连接身份/SQL/凭据。每次后续启动输出历史隔离提示。

diagnose区分active任务结果与historical保留结果；历史问题显示WARN并明确“当前不会自动处理”，不冒充当前仍在RUNNING。inspect-instance标历史身份并跳过当前依赖解释；不给历史实例推荐resolve/retry。recovery-plan的AUTO_RESUME只包含active，独立展示历史计数。CLI status保留原字段，隔离启用时附恢复摘要/active_counts/historical_counts。

普通 --plan-config 在旧资料缺失时不再只报告无路可走：保留需要旧基线的错误证据，同时提示正常启动可按此模式恢复新窗口；不得把估计切点冒充已应用结果。

## 验证

真实旧SQLite包含SUCCESS/PENDING/BLOCKED/FAILED/UNKNOWN/RUNNING及PREPARED/DISPATCHED：全部原始事实逐字段不变，历史不再调度或满足依赖；新完整窗口执行成功；重复启动不重复备份/退役；备份、注册、事务失败源数据不变；备份跨边界切点重新计算；DWD未来信号被切点隔离；历史映射丢失拒绝执行；普通迁移不受历史UNKNOWN/RUNNING阻断；诊断历史提示准确。函数<=50行，不改业务SQL/公司脚本，不增依赖；与930passed/44skipped/7既有packaging失败基线比较。
