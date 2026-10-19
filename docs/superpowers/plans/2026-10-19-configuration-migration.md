# Configuration Migration Implementation Plan

> **For agentic workers:** Use the approved design and test-driven implementation; independent modules can proceed in parallel with disjoint file ownership. Root integrates and commissions independent review.

**Goal:** 支持保留执行账本的受控配置升级，明确任务生效起点。
**Architecture:** 显式配置快照和任务版本；账本事务迁移；调度统一起点；CLI预览/应用边界。
**Tech Stack:** Python 3.12、SQLite、pytest、仓库已有依赖。
**Spec:** `docs/superpowers/specs/2026-10-19-configuration-migration-design.md`

## Global Constraints

- 不修改业务SQL、部署/启动/打包脚本，不新增产品依赖。
- 函数/fixture <=50行；测试位于原test目录。
- 旧binding算法、历史实例ID和已有执行事实保持兼容。
- 只读不写库，应用需要租约/文件锁、备份、CAS事务和审计。
- 身份无法证明或旧任务未结清时明确阻断，不提供force。

## Review Focus

1. SQL正文变化与下游任务口径变化：Task 1/4测试。
2. 首次legacy迁移没有可信旧配置、旧SQL证据：Task 4测试。
3. DWD迟到事件绕过新任务游标：Task 3测试。
4. 备份或事务中断、预览后状态变化：Task 2/4测试。
5. 相同taskname旧成功/UNKNOWN被新口径复用：Task 3/4测试。

## Task 1 — 显式manifest与diff

Files: config/evolution.py；test/test_configuration_manifest.py。
Interfaces: build_manifest、manifest_digest、compare_manifests（见spec）。
- [x] 先写SQL变化、四任务新增、无关参数稳定、脱敏、依赖传播测试，确认失败。
- [x] 实现固定schema的快照和比较。
- [x] 跑focused测试并给出结果；不修改其他模块。

## Task 2 — 账本配置事务和备份

Files: state/configuration.py、state/store.py；test/test_configuration_state.py。
Interfaces: spec列出的StateStore接口。
- [x] 先写只读旧库、快照历史、CAS、回滚、备份、未完成统计、入队起点测试，确认失败。
- [x] 实现事务API和备份；入队时对已配置task_starts拒绝早于起点的实例。
- [x] focused测试通过；接口实现不依赖manifest生成模块。

## Task 3 — 调度统一起点与SQL快照检查

Files: scheduler/service.py、sql_execution/runner.py；对应新测试文件。
- [x] 以真实SQLite测试cron、DWD事件、手动、run_pending，先红后绿。
- [x] 使用StateStore.get_task_effective_starts()；旧库无policy保持原行为。
- [x] SQLTaskRunner增加可选 expected_sql_hashes: dict[str,str]，key为taskname；发送前校验load_sql的同一份文本摘要。
- [x] focused测试通过；root负责Application/runtime的接口接入。

## Task 4 — 迁移用例、CLI和启动接入（root）

Files: configuration_migration包（planner/service/cli按职责分拆）、application.py、main.py、cli_help.py、diagnostics/checks.py、diagnostics/cli.py、runtime.py、errors.py；新迁移/CLI集成测试。
- [x] 按spec测试legacy证明、plan/apply、确认项、过期计划、已有未结清任务、ID不变、幂等、只读输出。
- [x] 接入新初始化快照、原legacy启动兼容和manifest启动校验。
- [x] 实现预览和独占维护应用；不经过recover_running。
- [x] 接入SQL摘要、手动窗口拒绝、诊断说明与安全错误码。
- [x] 更新文档、运行全套、独立复审、修复、核对基线后合并推送main。

## Progress

- 基线 b268682；隔离分支 codex/configuration-migration。
- 原生工作区工具无法解析当前应用上下文的main引用，因此使用目标仓库的Git worktree隔离。

## Verification and review result

- Fresh baseline: 739 passed, 44 skipped, 7 failed; final: 930 passed, 44 skipped, 7 failed. Failure identities are unchanged.
- The seven baseline failures are platform packaging harness checks: startup argument forwarding; precheck/init-Python exit propagation (2); startup permissions; liveness/readiness exit propagation (2); probe dependency installation. No product packaging or startup script was changed.
- New coverage includes the real 52→56 task definitions with SUCCESS/UNKNOWN preserved, SQLite backup and transactional rollback, explicit legacy proof, SQL changes, stale plans, readiness/identity confirmations, and all scheduler entrances.
- Independent review corrected cached migration input, cutover time crossing during backup, snapshot-required adoption, stale future Cron cursors, invalid task-start diagnosis, and historical dependency interpretation. Changed/new tasks restart their cursors transactionally; unchanged cursors and all execution facts remain.
- Application startup, migration and diagnostics share configuration snapshot validation. SQL runtime checks the exact text it sends.
- Function length check and git diff --check passed. Production migration has not been executed; real ClickHouse/Redis deployment validation remains an operator step.
