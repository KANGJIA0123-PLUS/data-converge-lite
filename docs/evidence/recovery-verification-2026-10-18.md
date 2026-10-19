# 恢复协议验证记录

> 本文保留 2026-10-18 的原始验证事实，不代表后续版本的整体验收。2026-10-19 已临时移除选主账本身份校验；此前准入约束已被取代（superseded），本报告的同账本恢复结果不能用于证明独立状态卷接管安全。现状见 [DC-STATE-001](../known-issues.md#dc-state-001)。

## 环境与方法

- 基线提交：`a4f6138e2f5fbcb77c20c2304a09b5dc77234512`。
- 环境：macOS 26.5.1 arm64，Python 3.12.14，SQLite 3.53.1。
- 原始演练输出：[recovery-drill-2026-10-18.json](recovery-drill-2026-10-18.json)。生成时间和完整环境写在 JSON 中。
- 故障方式：真实子进程在已提交边界调用 `os._exit(91)`，跳过 Python 清理；父进程重新打开同一账本。
- 外部替身：独立 SQLite 接收记录，不设置去重约束，重复调用必然增加接收数。没有执行真实 ClickHouse SQL。
- 每个场景执行恢复、调度、再次恢复和再次调度；所有临时文件自动清理。

## 实测结果

| 场景 | 崩溃前状态/尝试数 | 恢复后状态/尝试数 | 两次回放后的接收数 | 结果 |
| --- | --- | --- | ---: | --- |
| prepared_before_dispatch | RUNNING / 0 | PENDING / 0 | 1 | 通过，最终 SUCCESS / 1 |
| dispatch_before_receiver | RUNNING / 1 | UNKNOWN / 1 | 0 | 通过，未自动提交 |
| accepted_before_finish | RUNNING / 1 | UNKNOWN / 1 | 1 | 通过，未重复提交 |
| success_before_replay | SUCCESS / 1 | SUCCESS / 1 | 1 | 通过，未重复提交 |

## 自动化测试

基线完整测试：**648 passed、44 skipped、7 failed**。

本轮集成完整测试：**732 passed、44 skipped、7 failed**。新增 84 项通过，没有新增失败；跳过项不计为通过。完整命令：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD/Services/Service/data-converge/src" python3 -m pytest -c /dev/null -p no:cacheprovider Services/Service/data-converge/test -q
```

两次测试失败集合相同，均在 `test_packaging.py`：

- `test_start_forwards_config_and_arguments_and_waits_for_child`
- `test_platform_start_failure_prevents_application_and_start_status[PRECHECK_EXIT_CODE-expected_hooks0]`
- `test_platform_start_failure_prevents_application_and_start_status[INIT_PYTHON_EXIT_CODE-expected_hooks1]`
- `test_platform_start_restores_company_file_permissions`
- `test_probe_uses_same_interpreter_and_returns_application_exit[liveness_check.sh-liveness]`
- `test_probe_uses_same_interpreter_and_returns_application_exit[readiness_check.sh-readiness]`
- `test_probe_does_not_install_dependencies`

前四项触发启动权限操作的测试目录边界保护，后三项与现有探针提前退出有关。已在独立导出的原提交上复现，未修改平台脚本或放宽断言。这些失败依然是正式交付前应处理的问题，不能将本报告描述为全套测试全绿。

## 覆盖的失效边界

- 派发提交失败时运行器调用数为零。
- 恢复只匹配当前 query_id，旧检查点不能覆盖新尝试或成功结果。
- 恢复事务中任何事件写入失败，实例、历史和检查点一起回滚。
- 准备中断不消耗预算，已有失败次数不被重置。
- 旧 claim 和内存仓储保留原契约，旧账本只读不建表。
- JSONL 分页有序，错误写 stderr，不包含 SQL、原始异常或人工备注。
- 停止请求阻止后续派发，并允许已提交任务收尾。
- Python 函数/测试行数规则继续通过，没有新增依赖。

## 独立检视

检视发现并修复了两项边界问题：降低重试上限后，耗尽预算的已恢复实例可能占用每轮派发名额；恢复统计会把全部 UNKNOWN 记录读入 Python。已分别增加多轮有限预算回归和 1000 条历史记录的读取量回归，随后独立复核无待修问题。

## 复现演练

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD/Services/Service/data-converge/src" python3 Services/Service/data-converge/test/recovery_drill.py > recovery-drill.json
```

输出 `passed: true` 表示四个固定场景的状态、接收数、历史和事件全部符合预期。JSON 中 `clickhouse_cluster_validated: false` 必须保留；它不是目标数据库的端到端验收。

## 发布前剩余验证

处理既有平台脚本测试失败；在目标容器和持久卷上验证同一账本连续性、磁盘满及重启；在实际 ClickHouse 配置下核对未知结果的处置流程；采样新增持久化提交的延迟与账本增长；最后补充生产故障频率和人工耗时，计算业务收益。
