# 本机主备与 fencing 演练证据（2026-11-05）

## 运行环境

- 分支：`main`，已包含接管 KILL、重试时间归一化、首次及非空接管 DWD 补发、SCAN 竞态修复和冲突处置。
- Python：3.12.14（uv 临时环境）。
- Redis：`tools/failover-drill/local_redis.py` 提供的 fakeredis[lua] TCP 服务。
- ClickHouse：`tools/failover-drill/mock_clickhouse.py`。
- 时区：`Asia/Shanghai`。

## 主备演练

命令：

```bash
PYTHONDONTWRITEBYTECODE=1 TZ=Asia/Shanghai \
PYTHONPATH=Services/Service/data-converge/src \
uv run --no-project --python python3.12 \
  --with PyYAML==6.0.2 --with redis==5.2.1 \
  --with APScheduler==3.11.0 --with Flask==3.1.3 \
  --with fakeredis --with lupa \
  python tools/failover-drill/drill.py
```

结果为 `passed: true`，9 项检查全部通过：备机接管、杀主前已有成功窗口、全部窗口成功、成功窗口未重跑、中断窗口只重跑一次、无残留 RUNNING、旧主写入被 fencing 拒绝、ClickHouse settings 和 5 分钟超时均生效。

本次覆盖 12 个窗口，INSERT 14 次；杀主前已有 3 个成功窗口，中断后重跑 2 个窗口。演练只证明 fakeredis 和 mock ClickHouse 下的协议行为，不代表真实 Redis、ClickHouse 容量或 exactly-once。

## 旧主存活 fencing

命令同上，将脚本替换为 `python tools/failover-drill/fencing_drill.py`。

结果为 `passed: true`，7 项检查全部通过：备机接管、中断窗口最终成功、旧主已提交本地完成状态、迟到写入被拒、共享状态未被覆盖、无残留 RUNNING、新主仍存活。接管耗时 6.32 秒；旧主退出码为 1，日志包含 `REDIS_STATE_FENCED`。

## 回归验证

- 接管、重试、DWD seed、Redis 镜像和冲突处置定向测试：`41 passed`。
- 全量测试：`2087 passed, 39 failed, 11 errors, 224 skipped`；50 个失败/错误 nodeid 与 `tools/test-baseline.txt` 完全一致，没有新增失败。
- CI 门禁固定 Ubuntu、Python 3.12、`TZ=Asia/Shanghai` 和测试依赖版本；归档校验同时覆盖 Linux 删除重建复用 inode 的文件替换场景。
- `git diff --check`：通过。
