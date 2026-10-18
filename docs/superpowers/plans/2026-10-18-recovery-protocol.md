# 汇聚恢复协议 Implementation Plan

> **For agentic workers:** 使用测试先行、并行独立模块和最终独立检视；本次用户已授权重新实现与重构。

**Goal:** 用同一账本内的派发检查点、安全恢复分类和事务内审计替代附件中不安全的清理重跑方案。

**Architecture:** StateStore 持有事务，检查点区分尚未派发与结果未知；DAGExecutor 在提交工作线程前提交派发意图。诊断命令只读导出恢复计划和事件，不参与恢复决策写入。

**Tech Stack:** Python 3.12、标准库 sqlite3/typing/concurrent.futures、现有 pytest；不新增依赖。

**Spec:** ../specs/2026-10-18-recovery-protocol-design.md

## Global Constraints

- 现有 56 个业务任务、SQL、部署及构建保持兼容，不新增第三方依赖。
- 所有 Python 函数、方法、测试和 fixture 不超过 50 行；测试保留在原 test 目录。
- 不修改 TaskDefinition 序列化和部署绑定指纹；旧账本无需重建。
- 旧 claim() 保持“调用者可能已提交”的语义，恢复仍 UNKNOWN。

## Review Focus

- 旧账本缺少新表时只读操作不得迁移或假造历史。
- 过期 query_id、已有 SUCCESS 和历史 UNKNOWN 不得被准备记录覆盖。
- 派发标记或历史提交失败时运行器调用数必须为零。
- 已消费重试预算保持有效，准备中断本身不额外计次。
- 原始异常、SQL 和人工备注不得进入结构化事件。

## Task 1：事务内事件

文件：`state/events.py`、`test/test_execution_events.py`。

接口：`append_event(conn, event_type, instance_id, occurred_at)`、`available(conn)`、`list_events(conn, after_event=0, limit=20)`。序号递增，固定白名单字段，不单独提交调用者事务；单页 1–100 条。

- [x] 先验证事件随主事务回滚、只读旧库不建表、分页顺序与字段白名单。
- [x] 实现窄事件存储接口并运行测试。

## Task 2：检查点与恢复

文件：`state/checkpoints.py`、`state/store.py`、`test/test_dispatch_checkpoints.py`。

接口：`StateStore.prepare_attempt(instance_id, *, now=None, max_attempts=3)`、`mark_dispatched(instance_id, query_id, *, now=None)`；`checkpoints.recovery_rows(conn, limit, offset)` 返回与恢复共用的分类。保持 `recover_running()` 返回处理数量和旧 claim 契约。

- [x] 先验证准备不计次、派发恰好计一次、过期标识拒绝、同事务历史、两类恢复与旧库行为。
- [x] 实现事务和查询，调用 Task 1 的事件接口；不修改任务定义或配置指纹。
- [x] 验证恢复不会改变历史 UNKNOWN/SUCCESS，重试预算不重置。

## Task 3：执行器与只读诊断集成

文件：`dag/ports.py`、`dag/executor.py`、`diagnostics/recovery.py`、`diagnostics/cli.py`、`diagnostics/report.py` 及对应测试。

接口：可选 `CheckpointRepository` 同时提供 prepare_attempt 和 mark_dispatched；旧仓储仍调用 claim。新增 `--recovery-plan` 与 `--events --after-event N`，后者支持 `--format jsonl`。

- [x] 先验证数据库提交失败阻止运行器、正常只执行一次、停止时不再派发及旧仓储兼容。
- [x] 接入检查点和诊断，保持原 CLI 输出协议和只读快照。
- [x] 验证参数组合、缺库/旧库、分页、敏感字段和查询前后文件差异。

## Task 4：故障演练与交付材料

文件：原 test 目录内的子进程故障测试/演练入口、`docs/architecture.md`、`docs/reliability.md`、`docs/competition.md`、`docs/evidence/`。

- [x] 真实子进程在各持久化边界退出，重启核对 SQL 接收次数、状态、事件、尝试号。
- [x] 记录实测结果及环境，说明离线替身与生产验收的区别。
- [x] 更新架构图、状态机、操作指南、对比表、业务价值指标及答辩提纲。
- [x] 运行完整现有测试和新增测试、函数长度规则、git diff --check，再做独立检视。

## 完成记录

已实现四个任务，经过独立检视及两项边界修正。最终完整测试 732 passed、44 skipped、7 failed；失败集合与基线一致。四个子进程故障演练全部符合预期。原始证据和限制见 ../../evidence/recovery-verification-2026-10-18.md。
