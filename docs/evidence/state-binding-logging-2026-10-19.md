# 状态库绑定日志验证（2026-10-19）

基线：`33ebd84b65c46ec98f4db8417c73d23dc986e847`。本次只增加绑定校验日志和运行说明；指纹算法、原异常码、任务规则、SQL、启动脚本及账本恢复策略不变。

## 验证结果

- 新增 7 个场景：配置变化拒绝启动并保留 RUNNING 记录；分别缺少绑定、覆盖起点或两者；异常元数据与控制字符不注入日志；正常重启不重复执行 SQL；覆盖起点无效时不误报校验通过。
- 修改前，6 个新增日志场景因缺少对应事件失败，1 个原错误语义场景通过。修改后，7 个场景与函数长度检查全部通过。
- 完整回归：**739 passed、44 skipped、7 failed**，耗时 19.14 秒；跳过项未验证。
- 在基线归档中重新执行 `test_packaging.py`：**6 passed、7 failed**。失败集合与本轮完整回归一致，无新增失败。
- `git diff --check` 通过；独立代码复审没有发现需要阻止提交的问题。

现有失败用例均在 `Services/Service/data-converge/test/test_packaging.py`：

1. `test_start_forwards_config_and_arguments_and_waits_for_child`
2. `test_platform_start_failure_prevents_application_and_start_status[PRECHECK_EXIT_CODE-expected_hooks0]`
3. `test_platform_start_failure_prevents_application_and_start_status[INIT_PYTHON_EXIT_CODE-expected_hooks1]`
4. `test_platform_start_restores_company_file_permissions`
5. `test_probe_uses_same_interpreter_and_returns_application_exit[liveness_check.sh-liveness]`
6. `test_probe_uses_same_interpreter_and_returns_application_exit[readiness_check.sh-readiness]`
7. `test_probe_does_not_install_dependencies`

测试使用独立 Python 3.12.14 环境及仓库已有依赖，不增加产品依赖。完整命令（在仓库根目录）：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD/Services/Service/data-converge/src" \
  /private/tmp/data-converge-binding-logs-venv/bin/python -m pytest \
  -c /dev/null -p no:cacheprovider Services/Service/data-converge/test -q
```

## 边界

使用本地 SQLite 与模拟 HTTP 接收端验证，没有连接生产 ClickHouse、Redis 或修改现场状态库。日志只能报告缺失信息/整体指纹不符，无法恢复旧配置或识别具体变化字段；本次未实现状态迁移，也不声称现场启动问题已消除。
