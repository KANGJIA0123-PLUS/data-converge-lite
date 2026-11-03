# 本地双实例主备演练证据（2026-11-04）

## 运行环境

- 分支：`feature/design-alignment`
- Python：3.12.14（uv 虚拟环境）
- Redis：演练脚本使用 `fakeredis[lua]` TCP 服务（本机未安装 `redis-server`）
- ClickHouse：`tools/failover-drill/mock_clickhouse.py`

运行命令：

```bash
/tmp/data-converge-drill-venv-20261104/bin/python tools/failover-drill/drill.py
```

## 结果

`tools/failover-drill/out/report.json` 返回 `passed: true`，8 项检查全部通过：

| 检查 | 结果 |
| --- | --- |
| 备实例接管 | PASS |
| 共享状态中的全部窗口为 SUCCESS | PASS |
| 已成功窗口未重跑 | PASS |
| 被中断窗口恰好重跑一次 | PASS |
| 无残留 RUNNING | PASS |
| 旧主写入被 fencing 拒绝 | PASS |
| ClickHouse settings 已发送 | PASS |
| 5 分钟任务超时不超过 120 秒 | PASS |

本次演练选出的主实例为 `A`，在 SQL 执行中被 `SIGKILL`；`B` 完成接管。共记录 13 次 INSERT，覆盖 12 个任务窗口，其中被中断的 `db.t_a` 窗口重跑一次。

## 修复内容

演练最初暴露出排队中的 `PENDING` 实例只保存在本地主库、没有及时镜像到 Redis。主实例被杀后，备实例只能恢复当前 `RUNNING` 实例，历史排队窗口会随游标推进而丢失。现在 `StateStore.enqueue_and_advance` 在本地事务提交后同步发布新插入的实例；演练也在两个实例启动前预置 DWD 就绪信号，避免已到期窗口先被标记为 `BLOCKED`。

回归测试：

```text
test_redis_state.py: 8 passed
```

全量测试与基线对比（同一 Python 3.12 环境）：当前分支 `2052 passed, 42 failed, 11 errors, 224 skipped`；基线 `6c56461` 为 `2014 passed, 42 failed, 11 errors, 224 skipped`。失败和错误总数均为 53，没有新增失败；差异中的通过项来自本分支已有的新功能测试。

