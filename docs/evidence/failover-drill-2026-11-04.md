# 本地双实例主备演练证据（2026-11-04）

## 运行环境

- 分支：`main`（演练加强提交 `d9392a8`）
- Python：3.12.14（uv 虚拟环境）
- Redis：演练脚本使用 `fakeredis[lua]` TCP 服务（本机未安装 `redis-server`）
- ClickHouse：`tools/failover-drill/mock_clickhouse.py`

运行命令：

```bash
/tmp/data-converge-drill-venv-20261104/bin/python tools/failover-drill/drill.py
```

## 结果

`tools/failover-drill/out/report.json` 返回 `passed: true`，9 项检查全部通过：

| 检查 | 结果 |
| --- | --- |
| 备实例接管 | PASS |
| 杀主前已存在成功窗口（非空验证） | PASS |
| 共享状态中的全部窗口为 SUCCESS | PASS |
| 已成功窗口未重跑 | PASS |
| 被中断窗口恰好重跑一次 | PASS |
| 无残留 RUNNING | PASS |
| 旧 token 被共享状态 Lua 拒绝（合成探针） | PASS |
| ClickHouse settings 已发送 | PASS |
| 5 分钟任务超时不超过 120 秒 | PASS |

连续 3 次运行均通过：

| 次数 | INSERT | 覆盖窗口 | 中断后重跑窗口 | 杀主前 SUCCESS 窗口 |
| --- | --- | --- | --- | --- |
| 1 | 14 | 12 | 2 | 3 |
| 2 | 14 | 12 | 2 | 3 |
| 3 | 13 | 12 | 1 | 2 |

`success_before_kill` 是 SIGKILL 前一刻从共享 Redis 读到的真实状态。以第 1 次为例，杀主前已有 `m5_a` 的 12:45、12:50 和 `m5_b` 的 12:45 三个窗口成功；接管后这三个窗口都没有再次执行，说明 `no_success_window_rerun` 不再是对空集合求值。

第 1、2 次同时有两个 RUNNING（`m5_a`、`m5_b` 是不同任务，各自并行度 1），接管后各自重跑一次，共 14 次 INSERT；第 3 次只有一个中断窗口，13 次 INSERT。三次都满足“每个中断窗口恰好重跑一次”。

## 本次加强

旧版 drill 在第一个 `ALTER`（pre-cleanup）出现后约 1 秒就 SIGKILL；mock 的 INSERT 延迟 4 秒，杀主时没有任何窗口成功，`success_before_kill` 为空，`no_success_window_rerun` 对空集合恒为 `True`。

新版流程：

1. 先等至少 2 个窗口进入 `SUCCESS`；
2. 再等一个窗口进入 `RUNNING`，延迟 1 秒让 INSERT 真正开始；
3. 紧贴 SIGKILL 前抓取 `done_before`；
4. `verify()` 增加 `success_windows_before_kill` 检查，杀主前成功窗口少于 2 个时 `passed=false`。

fencing 检查仍是合成探针：演练进程用旧 token 调用同一个 Lua，证明 token 校验生效。旧主 A 已被 SIGKILL，不能代表“旧主进程迟到写入”；真实 store 层 fencing 由 `test_redis_state.py::test_deposed_leader_cannot_overwrite_shared_state` 覆盖，端到端的“旧主存活但失租”场景待补。

DWD 信号在两个实例启动前预置，目的是让 6 个历史窗口在启动时立即可执行，把演练聚焦在接管路径；这并不用于证明“晚到信号无法重开窗口”。

## 回归测试

- `test/test_redis_state.py`：8 passed（需要 `fakeredis[lua]`）。
- 全量测试（Python 3.12）：当前分支 `2052 passed, 42 failed, 11 errors, 224 skipped`；基线 `6c56461` 为 `2014 passed, 42 failed, 11 errors, 224 skipped`。失败/错误共 53 个，测试 ID 与基线完全一致，无新增失败。
- `git diff --check` 通过。
