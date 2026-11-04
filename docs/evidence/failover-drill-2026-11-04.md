# 本地双实例主备演练证据（2026-11-04）

## 运行环境

- 分支：`main`（主备演练加强 `d9392a8`，fencing 场景与 Redis 就绪等待 `400ccfd`）
- Python：3.12.14（uv 虚拟环境）
- Redis：演练脚本使用 `fakeredis[lua]` TCP 服务（本机未安装 `redis-server`）
- ClickHouse：`tools/failover-drill/mock_clickhouse.py`

运行命令：

```bash
/tmp/data-converge-drill-venv-20261104/bin/python tools/failover-drill/drill.py
/tmp/data-converge-drill-venv-20261104/bin/python tools/failover-drill/fencing_drill.py
```

## 主备演练结果

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
| 3 | 14 | 12 | 2 | 3 |

`success_before_kill` 是 SIGKILL 前一刻从共享 Redis 读到的真实状态。以第 1 次为例，杀主前已有 `m5_a` 的 13:25、13:30 和 `m5_b` 的 13:25 三个窗口成功；接管后这三个窗口都没有再次执行，说明 `no_success_window_rerun` 不再是对空集合求值。

三次都同时有两个 RUNNING（`m5_a`、`m5_b` 是不同任务，各自并行度 1），接管后各自重跑一次，共 14 次 INSERT；三次都满足“每个中断窗口恰好重跑一次”。

## 本次加强

旧版 drill 在第一个 `ALTER`（pre-cleanup）出现后约 1 秒就 SIGKILL；mock 的 INSERT 延迟 4 秒，杀主时没有任何窗口成功，`success_before_kill` 为空，`no_success_window_rerun` 对空集合恒为 `True`。

新版流程：

1. 先等至少 2 个窗口进入 `SUCCESS`；
2. 再等一个窗口进入 `RUNNING`，延迟 1 秒让 INSERT 真正开始；
3. 紧贴 SIGKILL 前抓取 `done_before`；
4. `verify()` 增加 `success_windows_before_kill` 检查，杀主前成功窗口少于 2 个时 `passed=false`。

`local_redis.py` 现在在 `CONFIG SET requirepass` 完成后写 `--ready-file`，drill 等到该文件出现再启动实例。修复前 fakeredis 在设置密码的瞬间会关闭早期连接，表现为启动即 `REDIS_STATE_WRITE_FAILED`；修复后主备演练和 fencing 演练各连续 3 次通过。

## 旧主存活 fencing 演练

`fencing_drill.py` 验证“旧主进程没有被杀、只是失租”的场景：A 的 SQL 处于 `RUNNING` 时 `SIGSTOP` A；B 在租约 TTL 到期后接管，导入 RUNNING、KILL 旧 query、重跑；B 完成全部窗口后再 `SIGCONT` A，让 A 的迟到结果走完本地 `finish` 并尝试发布。

连续 3 次运行均 `passed: true`：

| 次数 | 接管耗时（秒） | 中断窗口 | 旧主本地状态 | 旧主退出码 |
| --- | --- | --- | --- | --- |
| 1 | 6.21 | `m5_a@13:20` | SUCCESS | 1 |
| 2 | 6.20 | `m5_a@13:20` | SUCCESS | 1 |
| 3 | 6.18 | `m5_a@13:20` | SUCCESS | 1 |

7 项检查全部通过：

| 检查 | 结果 |
| --- | --- |
| 备实例接管 | PASS |
| 中断窗口最终 SUCCESS | PASS |
| 旧主 `finish` 已本地提交 | PASS |
| 旧主迟到写入被拒 | PASS |
| 共享状态未被覆盖 | PASS |
| 无残留 RUNNING | PASS |
| 新主存活 | PASS |

A 恢复后日志出现 `SERVICE_FAILED: FencedOut diagnostic={"error_code": "REDIS_STATE_FENCED"}`，进程以 1 退出；A 的本地账本中中断窗口已是 `SUCCESS`，说明 `finish` 已经提交，随后才会尝试镜像。SIGCONT 前后逐字段比较共享实例 hash，内容完全一致；B 的 token 和状态没有被旧主覆盖。

该演练证明的是 Redis 共享状态的写入 fencing，不等于 ClickHouse 端 exactly-once：旧 query 已提交的物理写入仍可能发生，现场仍需依赖 KILL QUERY、下游幂等清理和人工核对 `UNKNOWN`。

## 回归测试

- `test/test_redis_state.py`：8 passed（需要 `fakeredis[lua]`）。
- 全量测试（Python 3.12）：当前分支 `2052 passed, 42 failed, 11 errors, 224 skipped`；基线 `6c56461` 为 `2014 passed, 42 failed, 11 errors, 224 skipped`。失败/错误共 53 个，测试 ID 与基线完全一致，无新增失败。
- `git diff --check` 通过。
