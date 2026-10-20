# Background Backfill Implementation Plan

**Goal:** 在线提交有界时间段补算，保留执行事实并可查询批次结果。

**Architecture:** 独立 SQLite 请求队列供命令并发提交；服务主线程顺序消费，
主账本以唯一条目 ID 原子登记执行及完成结果，跨库同步可重复而 SQL 不重复。

**Tech Stack:** Python、标准库 sqlite3、现有 DAG 与 SQL runner。

**Spec:** ../specs/2026-10-20-background-backfill-design.md

## Global Constraints

- 函数及测试不超过 50 行，不新增第三方依赖。
- 不修改业务 SQL、部署或打包；修复与重算有关的目标表配置错误。
- UNKNOWN 不自动重跑，执行结果落盘失败必须停止继续派发。

## Review Focus

- 崩溃发生在主账本提交与队列结果同步之间，不重复 SQL。
- 用户一次选择全部任务时，按周期截取完整窗口，保留依赖顺序。
- 配置变化、旧退休记录、缺失历史及 SQL 错误不会静默成功。
- 新请求与运行服务并存，不获取第二个 writer。
- 默认跳过成功窗口；显式重算成功后查询历史可看到两个不同 query_id。

## Tasks

- [x] 计划：`backfill/plan.py` + `test_backfill_plan.py`；验证边界、DST、限制及任务排序。
- [x] 请求与命令：`backfill/queue.py`、`backfill/cli.py`，接入 main/帮助/参数验证；
  `test_backfill_queue.py`、`test_backfill_cli.py` 验证只提交、不启动服务、并发与幂等。
- [x] 执行：`state/backfill.py`、`backfill/worker.py`；StateStore 完成/恢复接点、
  runtime/scheduler 顺序消费；测试重算审计、故障恢复、依赖及调度交错。
- [x] 目标配置：核对全部实际 SQL INSERT 与 target_table，修正已发现不一致并回归。
- [x] 文档：增加完整命令、范围与状态说明，说明持久卷和分片清理限制。
- [x] 验证：相关测试、完整测试与当前 main 基线比较、独立审查及 diff 检查。

每项先验证缺失行为的失败测试，再实现、运行对应测试。各模块并行开发，根代理集成，
最后独立审查整体安全边界；不连接或操作生产数据。

## 验证记录

完整套件：1542 passed、43 failed、84 skipped、11 errors。
与基线 67d2a47 的失败节点逐项比较，43 个失败和 11 个错误均已存在，没有新增失败节点。
其中新增 183 个通过案例；包括本地 HTTP 请求链路、单 writer 在线提交、
两库结果同步与失败回滚、DST、窗口选择、损坏队列隔离及 60 个实际任务的目标表一致性。
