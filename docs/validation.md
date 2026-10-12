# 本次交付验证记录

日期：2026-10-12。环境：macOS ARM64、Python 3.12.14、redis-py 6.4.0；连接兼容性另以 5.0.8 验证。测试使用临时目录、合成凭据、本机 HTTP(S)/RESP 服务和测试 SDK，没有连接公司服务或执行生产 SQL。

本记录已按用户要求更新：核心代码采用上传的原 `build_framework` 编译并复制 `.pyc`，不构建 whl。此前独立打包方式已删除。

公司原有构建、依赖及测试配置恢复上传版本；新增的 `requirements-dev.txt`、`ruff.toml` 已删除。本次测试通过命令行指定业务路径，不修改公司配置。执行命令如下（关闭缓存以避免 pytest 向 `/dev` 写入缓存）：

```bash
PYTHONPATH="$PWD/Services/Service/data-converge/src" .venv/bin/python -m pytest -c /dev/null -p no:cacheprovider Services/Service/data-converge/test -q -ra --tb=short
```

## 最终结果

| 验证 | 结果 |
|---|---|
| 上述业务测试命令（redis-py 6.4.0） | **429 passed, 1 skipped**，7.66 秒 |
| redis-py 5.0.8 连接/认证/传输兼容回归 | **155 passed, 1 skipped**，0.64 秒 |
| 业务目录 Python AST 解析 | 52 个 Python 文件通过 |
| Shell | 原交付 20 个 Shell 脚本语法通过，本次未改动 |
| 原始 `build.sh` 一致性 | 与上传附件脚本内容一致，SHA-256 见导入记录 |
| 公司配置一致性 | 根 `pyproject.toml`、`requirements.txt`、`pytest.ini` 及平台 `bin/pyproject.toml` 与上传原文一致（去除导出分隔空行） |
| 原框架构建校验 | 在临时源码副本执行原脚本；生成 `.pyc`、删除副本 `.py`、复制到 `images/lib/data-converge`；52 个 SQL 和配置内容一致 |
| 字节码主入口与启动器 | 在源码目录外运行 `.pyc` 主入口与真实 `start_data_converge.sh`，均返回 `VALID`、52 个任务 |
| 字节码 XMS helper | 从 `.pyc` 启动隔离子进程，使用合成 SDK 完成解密协议校验 |
| 独立代码复查 | Sentinel/PSM 适配复查未发现可复现 P1/P2 问题 |
| 本次改动范围 | 相对 `ae08967`，构建、依赖、启动脚本、Deployment 和 52 个 SQL 无差异 |

跳过的唯一测试是 `test_dwd_ready_transport.py` 中的真实 Redis 服务集成：本机未安装 `redis-server`。新增 Sentinel 测试使用真实 redis-py 5.0.8/6.4.0 与本机 RESP 服务端，验证发现、AUTH、SELECT、List 命令及 READONLY/断连恢复；模拟两个 master 共享 List 状态，不代表验证真实 Redis 复制或公司故障切换。

PSM 的 46 项测试使用真实 PyCryptodome AES-GCM 与按参考接口构造的测试 PSF；密钥材料、根 XOR 和头格式均为合成数据。未安装真实 PSF，不能把这些测试当作生产密码格式兼容性证明。本地验证只在虚拟环境安装 Crypto，未修改依赖清单、打包或构建配置。

导入的公司 SQL 保留原格式，其中存在历史行尾空白；未为格式检查批量改写 52 个文件。排除该 SQL 目录后的暂存变更通过 `git diff --cached --check`。提交候选文件中没有 Java 源码、真实密钥、运行账本、SDK 二进制或本机虚拟环境。

## 已覆盖的关键行为

- XMS helper 使用同一解释器、受控管道与超时，屏蔽 SDK 标准输出/错误噪音；严格解析 Base64 PMS 并唯一选择账号。测试凭据为合成内容，未使用真实密码。
- Redis PMS 按唯一服务/账号选择，拒绝重复 JSON 键、歧义服务、重复账号、无效或过多 Sentinel 地址；IPv6 需方括号，密码保持原值且不进入对象 repr。密码轮换和地址顺序不影响账本身份，切换 Sentinel 地址集合则拒绝复用旧身份。
- PSM 双次 AUTH 只剥离一次 `>hex` / `ERR >hex` 前缀，校验回传随机数；重连重新生成 nonce；Sentinel 不 SELECT，数据节点才 SELECT；失败不退回明文认证，AUTH 前不发送 PING。隔离进程禁止导入 redis/PSF/Crypto 时仍可导入主入口并离线加载配置。
- 回环客户端在旧 master 拒绝 ACK（READONLY）或断开 socket 后不盲目重发；下一轮重新发现新 master、重新认证并从 processing 恢复同一消息；关闭数据和 Sentinel 的客户端及连接池。
- 真正的回环 HTTP(S) 请求验证 POST、认证、窗口参数、query_id、错误分类、断线和截断。HTTP/HTTPS 响应头、chunk header 和响应体缓慢发送的六种场景均验证绝对截止时间；计时线程得到回收。
- SQLite 成功去重、RUNNING 恢复为 UNKNOWN、仅明确未发送错误重试、人工核验恢复、部署绑定和独占写锁；上游依赖要求完整覆盖下游窗口。
- 入口离线校验不读取密码、不访问网络、不创建状态；显式初始化不自动回填。真实子进程验证前台启动、心跳和 SIGTERM；信号打断持锁代码不会重入死锁。
- DWD 五种类型严格解析、去重、持久化与 ACK 顺序；SQLite 失败不确认，ACK 失败可重读；不足五种、零消息和 Redis 故障不会放行未就绪窗口。
- 事件提前登记全部 13 个五分钟任务，cron/手工/事件使用同一门控与账本；重复消息或进程重启不重放成功 SQL。
- 等待消息跨过五分钟边界时，用单调时钟推进到达时间，避免把刚结束的窗口误判为未来并丢弃；极端未来时间不导致服务崩溃。
- 双架构依赖目录选择、只读依赖重启、探针返回码、前台 PID，以及原框架字节码构建。只在临时副本执行会删除 `.py` 的原脚本，实际仓库源码保持完整。外部 BuildTool/function.sh 和平台初始化边界使用替身，没有执行公司宿主机初始化或完整镜像发布。
- 两处 SQL 执行缺陷修复：固定日期改窗口参数，以及重复表达式导致的 INSERT/SELECT 列数不匹配。其余统计口径保留并列入已知问题。

新增故障边界先用失败回归复现，再修实现并确认通过。代码复查覆盖入口/调度/状态及连接/解密两个方向，DWD 接入后再次复查。

## 尚需公司环境验收

1. GDE Python 与实际 ARM/x86 `dsf`、`psf`、`Crypto` 的兼容性、XMS 网络及真实 Redis PMS/PSM 格式；确认公司 SDK 自身日志不输出密钥。
2. ClickHouse 实际 HTTP(S) 端口、CA、账号权限、表引擎和字段合同，以及 52 个 SQL 的业务结果。
3. 确认 `gde-adapter-certfiles` 中实际 Redis 密文键名，替换 `password_file` 占位；核验参考 Sentinel 主节点组 `redis-cluster`、DB 5、TLS、命令权限与生产者发布开关。两条根/工作密钥路径已经 Deployment 确认。
4. 确认 data-converge 独占消息 List，停用会重复写入同一目标的旧链路，核验缺失信号的补发流程。
5. 公司完整 BuildTool/Maven/镜像构建链，构建与运行 Python 小版本一致；UID/目录权限、持久存储、固定节点、Recreate、探针、SIGTERM 宽限和停机迁移账本。

本次未部署公司集群，未声称完成生产切换、真实 Redis 联调或全部统计口径修正。详见 [DWD 消费](dwd-ready.md) 与 [已知问题](known-issues.md)。
