# 本地双实例主备演练证据（2026-11-04）

## 运行环境

- 分支：`main`（主备演练加强 `d9392a8`，fencing/Redis 就绪等待 `400ccfd`，镜像写恢复 `29bfb51`）
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

`success_before_kill` 是 SIGKILL 前一刻从共享 Redis 读到的真实状态。以第 2 次为例，杀主前已有 `m5_a` 的 13:45、13:50 和 `m5_b` 的 13:45 三个窗口成功；接管后这三个窗口都没有再次执行，说明 `no_success_window_rerun` 不再是对空集合求值。

三次都同时有两个 RUNNING（`m5_a`、`m5_b` 是不同任务，各自并行度 1），接管后各自重跑一次，共 14 次 INSERT；三次都满足“每个中断窗口恰好重跑一次”。

## 本次加强

旧版 drill 在第一个 `ALTER`（pre-cleanup）出现后约 1 秒就 SIGKILL；mock 的 INSERT 延迟 4 秒，杀主时没有任何窗口成功，`success_before_kill` 为空，`no_success_window_rerun` 对空集合恒为 `True`。

新版流程：

1. 先等至少 2 个窗口进入 `SUCCESS`；
2. 再等一个窗口进入 `RUNNING`，延迟 1 秒让 INSERT 真正开始；
3. 紧贴 SIGKILL 前抓取 `done_before`；
4. `verify()` 增加 `success_windows_before_kill` 检查，杀主前成功窗口少于 2 个时 `passed=false`。

`local_redis.py` 现在在 `CONFIG SET requirepass` 完成后写 `--ready-file`，drill 等到该文件出现再启动实例。修复前 fakeredis 在设置密码的瞬间会关闭早期连接，表现为启动即 `REDIS_STATE_WRITE_FAILED`；修复后主备演练和 fencing 演练各连续 3 次通过。

主备 drill 内的 fencing 检查仍是合成探针（旧主被 SIGKILL，不能代表旧主存活）；旧主存活场景见下一节。

## 旧主存活 fencing 演练

`fencing_drill.py` 验证“旧主进程没有被杀、只是失租”的场景：A 的 SQL 处于 `RUNNING` 时 `SIGSTOP` A；B 在租约 TTL 到期后接管，导入 RUNNING、KILL 旧 query、重跑；B 完成全部窗口后再 `SIGCONT` A，让 A 的迟到结果走完本地 `finish` 并尝试发布。

P0-3 之后连续 3 次运行均 `passed: true`：

| 次数 | 接管耗时（秒） | 中断窗口 | 旧主本地状态 | 旧主退出码 |
| --- | --- | --- | --- | --- |
| 1 | 6.27 | `m5_a@13:45` | SUCCESS | 1 |
| 2 | 6.13 | `m5_a@14:00` | SUCCESS | 1 |
| 3 | 6.12 | `m5_a@14:00` | SUCCESS | 1 |

7 项检查全部通过：备机接管、中断窗口最终 SUCCESS、旧主 `finish` 已本地提交、旧主迟到写入被拒、共享状态未被覆盖、无残留 RUNNING、新主存活。

A 恢复后日志出现 `SERVICE_FAILED: FencedOut diagnostic={"error_code": "REDIS_STATE_FENCED"}`，进程以 1 退出；A 的本地账本中中断窗口已是 `SUCCESS`，说明 `finish` 已经提交，随后才会尝试镜像。SIGCONT 前后逐字段比较共享实例 hash，内容完全一致；B 的 token 和状态没有被旧主覆盖。

该演练证明的是 Redis 共享状态的写入 fencing，不等于 ClickHouse 端 exactly-once：旧 query 已提交的物理写入仍可能发生，现场仍需依赖 KILL QUERY、下游幂等清理和人工核对 `UNKNOWN`。

## 镜像写失败恢复（P0-3）

问题：本地 `finish` 已提交 SUCCESS、Redis 写失败时，进程退出；旧 Redis 仍是 RUNNING，备机可能重跑已成功窗口。原来的 `enqueue_and_advance` 先写实例后写 cursor，已经避免了“cursor 领先于 PENDING”的丢失路径，但终态镜像失败仍会留下陈旧 RUNNING。

实现：

1. 新增本地 `mirror_pending` 表。
2. `_publish` 对 `StateSyncError` 做 3 次尝试（2 次 0.2 秒退避）；`FencedOut` 不重试。
3. `finish` 在本地终态事务中同时写入 pending 标记（write-ahead），共享 Redis 写成功后再清除。进程在提交后、镜像前崩溃也能留下可重放事实。
4. 新主接管时，在导入共享状态、KILL 旧 query 之后调用 `flush_pending()` 补发；共享状态已是终态且与本地不同则跳过并打 `REDIS_STATE_PENDING_CONFLICT`，不覆盖新主结果。
5. `enqueue_and_advance` 发布失败时记录整批实例，且 cursor 只在实例全部发布成功后写，避免 cursor 领先。

新增 `test_mirror_recovery.py` 8 个测试：瞬时失败重试、finish 写前标记、批量入队回放、终态冲突保护、FencedOut 不入队、旧账本 schema 升级、接管时 flush、发布失败时 cursor 不前进。

限制：pending 集合只在**本机 SQLite 账本**。如果 A 没有重启、B 先接管，B 仍可能看到旧 RUNNING 并重跑；该机制能保证 A 重启后补发，但不能替代 ClickHouse 端幂等，也不承诺 exactly-once。

## 回归测试

- `test/test_mirror_recovery.py`：8 passed。
- `test/test_redis_state.py`：8 passed（需要 `fakeredis[lua]`）。
- 全量测试（Python 3.12）：当前分支 `2060 passed, 42 failed, 11 errors, 224 skipped`；基线 `6c56461` 为 `2014 passed, 42 failed, 11 errors, 224 skipped`。失败/错误共 53 个，测试 ID 与基线完全一致，无新增失败；新增的 8 个通过项来自 `test_mirror_recovery.py`。
- `git diff --check` 通过。
