# 提前测试汇聚与跨周期等待 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** 在原目标表支持提前测试汇聚和正式覆盖，正常跨周期默认最多额外等待 300 秒。

**Architecture:** 提前执行使用独立主库审计，不改变正式成功状态。正常调度将原 Cron 触发与等待截止持久化，硬依赖与跨周期限时等待分开。既有单写者、目标清理、配置身份与恢复边界继续有效。

**Tech Stack:** Python 3.12、标准库 sqlite3、既有 pytest 与 ClickHouse SQL runner，无新增依赖。

**Spec:** ../specs/2026-10-21-preaggregate-and-cross-period-wait-design.md

## Global Constraints

- 用户已授权直接开发；默认 cross_period_wait_enabled=true、cross_period_wait_seconds=300，旧配置同默认。
- cross_period_wait_overrides 按任务覆盖；同周期、DWD、租约和 UNKNOWN 保护不受超时豁免。
- 业务 SQL、业务表结构、打包和部署文件不修改；原 SQL 参数采用完整逻辑窗口。
- Python 函数、测试、fixture 不超过 50 行。所有新增行为先写失败用例，再实现。
- 保留已有失败事实，不用放宽无关断言隐藏失败；按文件划分并行写权限，不并发修改同文件。
- 改动验证后按用户此前授权提交 GitHub main，不创建替代的 PR 发布流程。

## Review Focus

- 提前请求跨窗口结束排队：不得反向覆盖已正式完成结果（任务 2、3）。
- 清理成功而 INSERT 结果未知：正式写入须被保护，人工核查不能冒充正式成功（任务 2）。
- 手工 data_end 与正常 Cron scheduled_time 混用：等待不能错位或被重启延长（任务 1）。
- 直接改配置而未走迁移：提前结果的正式覆盖义务不能失去（任务 2、4）。
- 同周期提前 DWS/ADS 链：允许明确的提前依赖，正式链只认正式 SUCCESS（任务 3、4）。

## Task 1: 跨周期等待配置、调度与审计

**Files:** config/application.py、dag/builder.py、dag/executor.py、scheduler/service.py、runtime.py；新增 state/cross_period_waits.py 和 scheduler/cross_period_wait.py；resources/config/app_config.yaml；新增相关测试。

**Interfaces:** 设置 enabled: bool=true、seconds: int=300、overrides: dict[str,int]。保留 TaskDefinition 与业务 manifest 身份。
新增 required_cross_period_ids(dag, task, data_start, data_end)；同周期 upstream_instance_ids 语义不变。
正常 enqueue_and_advance 的可选 cross_period_waits 列表和正式主库登记同事务；state/cross_period_waits.register(conn, records) 负责建表及 ON CONFLICT(instance_id) DO NOTHING（不吞其他约束错误）。
等待服务读取独立表，使用原 due/deadline；只认可正式 SUCCESS。StateStore 的共享修改由任务 2 实现或交给 root 合并。

- [x] 写默认开关/300 秒、任务覆盖、非法配置、00:10→00:15、提前齐全、关闭、重启和旧实例无锚点的失败测试并观察 RED。
- [x] 实现配置、独立跨周期枚举、正常入队快照和非阻塞判定；无需等待的历史手工执行保持原语义。
- [x] 保留成功后的超时审计；缺失计数与样本限制 100，真正放行只警告一次。
- [x] 运行新增测试及 cross_period_scheduling/scheduler/configuration 相关回归，修复新回归。
- [x] 自审并报告涉及文件、RED/GREEN 证据和接口，供 root 集成审阅。

## Task 2: 提前执行独立账本、覆盖保护和人工核查

**Files:** 新增 state/preaggregate.py；修改 state/store.py、state/backfill.py、application.py、state/configuration.py 及必要恢复入口；新增 test_preaggregate_state.py 等。

**Interfaces:** StateStore.get_preaggregate(item_id)、begin_preaggregate(item, instance, now=...)、finish_preaggregate(item_id, status, error=None, diagnostic=None, now=None)、latest_preaggregate(instance_id)、list_preaggregates(instance_id=None, limit=20, offset=0)、resolve_preaggregate(item_id,resolution,note)。参数 item 携带 revision 和该任务所需配置证据。
对外提供 preaggregate_blocker(instance_id) 返回 str|None，正式派发/补算/核查调用；旧表不存在时读接口无副作用。
支持任务 1 的 enqueue_and_advance(..., cross_period_waits=()) 可选登记；仅本任务修改 state/store.py。

- [x] 写独立成功不影响正式状态、重复 item 去重、队列回执恢复、UNKNOWN、核查屏障和配置覆盖义务的失败测试并观察 RED。
- [x] 实现独立持久化、恢复 RUNNING→UNKNOWN、正式派发与手工重算屏障、人工核查 success/retry；原始历史不覆盖。
- [x] 保存相关定义/SQL/目标证据并在正常启动和迁移写入口校验尚未正式覆盖记录；不以全局等待参数改变数据身份。
- [x] 运行 preaggregate 与 backfill_state/checkpoints/configuration 相关回归。
- [x] 提交接口说明和自审结果；共享代码改动通知 root 与任务 1/3。

## Task 3: 提前汇聚计划、CLI 与后台消费

**Files:** backfill/plan.py、queue.py、cli.py、worker.py；新增 backfill/preaggregate.py；新增 test_preaggregate_plan.py、test_preaggregate_worker.py 等。

**Interfaces:** build_plan(..., preaggregate=False)；新计划明确版本与每条 execution_kind；旧计划仍可用。
未结束且已开始的条目使用任务 2 的独立账本；已结束条目仍走正式 backfill；未开始条目明确省略。
worker 原 SQL runner 不变；通过任务 2 API 查询/登记/结束/保护提前执行。
提前资格函数验证 coverage、effective_start、retirement、可达到的正常 Cron/cursor 和正式执行状态。

- [x] 写未来参数仍默认拒绝、显式提前、混合窗口分类、截止前后、覆盖资格、依赖和回执恢复的失败测试并观察 RED。
- [x] 实现 --preaggregate、只读预览、可验证计划版本、队列兼容和独立 worker 分支。
- [x] 同批上游结果优先；跨批只认同 revision 最近的提前成功，较新失败/UNKNOWN 和到期 SKIPPED 不得穿透。
- [x] 执行前再次校验；过期条目 SKIPPED 不写 SQL；日志和文本明确执行类型。
- [x] 运行新的计划/worker 测试和既有 backfill 全组；自审并报告接口。

## Task 4: 命令集成、诊断、端到端和发布

**Files:** main.py、cli_help.py、diagnostics/service.py/report.py/recovery.py 及必要新模块；README.md、docs/backfill.md、docs/architecture.md、docs/diagnostics.md；集成测试。

**Interfaces:** --resolve-preaggregate ITEM_ID --resolution success|retry --note TEXT；主库只读诊断可显示提前历史和跨周期等待证据；正式历史保持兼容。

- [x] root 完成各任务接口集成及新命令参数互斥检查；先写用户命令、只读诊断和正式覆盖的失败测试。
- [x] 验证原 SQL/同目标清理后写入；提前 DWS/ADS 后正式 DWS/ADS 均真实执行。
- [x] 用隔离 ClickHouse 或等价真实 runner 集成验证同窗口覆盖与不累加，不接触现场。
- [x] 更新中文帮助、配置示例、默认 300 秒和开关说明；清除旧的“所有跨周期不等待”文案。
- [x] 运行全部相关回归、函数行数检查、git diff --check；独立审阅整条变更并修正必要问题。
- [ ] 合入本地 main、推送 origin/main 并核对远端 SHA；清理本次 worktree 与测试容器。

## 执行记录

- 基线相关 132 项测试通过，HEAD 160078a；独立 worktree 为 /private/tmp/data-converge-preaggregate-wait。
- 用户“开始开发”明确授权执行；本计划用于落实已批准设计，不重复申请开发许可。

## 验证与审查结果

- 相关回归：390 passed，2 个真实 ClickHouse 用例在默认离线运行中跳过。
- 显式启用真实 ClickHouse 后：小时、日 DWS→ADS 两组覆盖用例及函数行数检查 3 passed。提前值 7，补源后正式值 18，目标始终一行；查询日志确认每次 ALTER DELETE→INSERT，正式覆盖后才填写 covered_at。
- 全量：1760 passed、216 skipped、42 failed、11 errors；42 项失败和 11 项错误与完整基线 160078a 逐项一致，无新增失败。
- 独立审查复核 158 passed、2 skipped，无剩余 P1/P2 阻断。已修复旧未来 run-task / run-once 可能提前标记正式完成的入口；保护仅针对存在待覆盖提前写入的窗口。
- 人工确认正式 UNKNOWN 不填写 covered_at 属保守设计；独立验证用原配置显式 --recompute 后实际成功可解除保护，不自动重放。已在操作文档明确。
- git diff --check 通过；本次专用 ClickHouse 容器均已删除，未操作现场数据库或已有容器。
