# 本次交付验证记录

日期：2026-10-12。环境：macOS ARM64、Python 3.12.14、redis-py 6.4.0。测试使用临时目录、合成凭据、本机 HTTP(S) 服务和 Redis 测试替身，没有连接公司服务或执行生产 SQL。

## 最终结果

| 验证 | 结果 |
|---|---|
| `.venv/bin/python -m pytest -q` | **289 passed, 1 skipped**，5.02 秒 |
| `ruff check Services/Service/data-converge/src tools` | 通过 |
| Python AST 解析 | 55 个 Python 文件通过 |
| `bash -n` | 20 个 Shell 脚本通过 |
| `python -m build --wheel --no-isolation` | 成功，无构建警告；包含 52 个 SQL 和 DWD 消费模块 |
| Wheel 独立安装校验 | 安装到临时目录，在源码目录外运行 `--validate-only`，返回 `VALID`、52 个任务 |
| `python tools/build_package.py --output build/data-converge-lite.tar.gz` | 成功；包含 52 个 SQL、DWD 模块和可执行启动脚本，不混入缓存或 egg-info |
| 独立代码复查 | 初审问题修复后复查，未发现仍存的已复现 P1/P2 阻塞项 |

跳过的唯一测试是 `test_dwd_ready_transport.py` 中的真实 Redis 回环集成：本机未安装 `redis-server`。Redis 协议/故障/确认顺序由测试替身验证，客户端构造与参数使用真实 redis-py；这些不等于已经验证公司 Redis 或真实 Redis Cluster。

导入的公司 SQL 保留原格式，其中存在历史行尾空白；未为格式检查批量改写 52 个文件。排除该 SQL 目录后的暂存变更通过 `git diff --cached --check`。提交候选文件中没有 Java 源码、真实密钥、运行账本、SDK 二进制或本机虚拟环境。

## 已覆盖的关键行为

- XMS helper 使用同一解释器、受控管道与超时，屏蔽 SDK 标准输出/错误噪音；严格解析 Base64 PMS 并唯一选择账号。测试凭据为合成内容，未使用真实密码。
- 真正的回环 HTTP(S) 请求验证 POST、认证、窗口参数、query_id、错误分类、断线和截断。HTTP/HTTPS 响应头、chunk header 和响应体缓慢发送的六种场景均验证绝对截止时间；计时线程得到回收。
- SQLite 成功去重、RUNNING 恢复为 UNKNOWN、仅明确未发送错误重试、人工核验恢复、部署绑定和独占写锁；上游依赖要求完整覆盖下游窗口。
- 入口离线校验不读取密码、不访问网络、不创建状态；显式初始化不自动回填。真实子进程验证前台启动、心跳和 SIGTERM；信号打断持锁代码不会重入死锁。
- DWD 五种类型严格解析、去重、持久化与 ACK 顺序；SQLite 失败不确认，ACK 失败可重读；不足五种、零消息和 Redis 故障不会放行未就绪窗口。
- 事件提前登记全部 13 个五分钟任务，cron/手工/事件使用同一门控与账本；重复消息或进程重启不重放成功 SQL。
- 等待消息跨过五分钟边界时，用单调时钟推进到达时间，避免把刚结束的窗口误判为未来并丢弃；极端未来时间不导致服务崩溃。
- 双架构依赖目录选择、只读依赖重启、探针返回码、前台 PID 和无破坏打包；外部平台初始化函数使用替身，没有执行公司宿主机初始化。
- 两处 SQL 执行缺陷修复：固定日期改窗口参数，以及重复表达式导致的 INSERT/SELECT 列数不匹配。其余统计口径保留并列入已知问题。

新增故障边界先用失败回归复现，再修实现并确认通过。代码复查覆盖入口/调度/状态及连接/解密两个方向，DWD 接入后再次复查。

## 尚需公司环境验收

1. GDE Python 与实际 ARM/x86 原生 `dsf` SDK 的兼容性、XMS 网络与挂载。
2. ClickHouse 实际 HTTP(S) 端口、CA、账号权限、表引擎和字段合同，以及 52 个 SQL 的业务结果。
3. Redis 的实际模式、端点、数据库、ACL/TLS 与生产者发布开关；当前参考 SDK 的 database 5 不能直接推导为 Redis Cluster。
4. 确认 data-converge 独占消息 List，停用会重复写入同一目标的旧链路，核验缺失信号的补发流程。
5. UID/目录权限、持久存储、固定节点、Recreate、探针、SIGTERM 宽限和停机迁移账本。

本次未部署公司集群，未声称完成生产切换、真实 Redis 联调或全部统计口径修正。详见 [DWD 消费](dwd-ready.md) 与 [已知问题](known-issues.md)。
