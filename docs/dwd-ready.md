# DWD 就绪信号消费

Python 汇聚服务消费生产端的 DWD 就绪信号，等待同一 5 分钟窗口的全部五类 DWD 完成后，才允许该窗口的 5 分钟汇聚执行。收到信号不代表汇聚 SQL 已成功；成功、失败、重试和 `UNKNOWN` 仍由原有 SQLite 执行账本记录。

本次仅接入现有 52 个任务的调度链，不迁入 Java enrichment、aggregation、quantile 代码，不新增 ClickHouse 统计表、自动重算或对账框架。生产端消息格式保持不变。**当前未连接公司 Redis 或 ClickHouse，生产信号和部署配置仍需公司环境联调。**

## 消息契约

生产端在某类型 DWD 的窗口写入成功后，向 Redis List `modelverse:dwd:ready` 执行 `LPUSH`，值为普通字符串：

```text
2025-08-01T10:05:GPU_REALTIME
```

它表示窗口 `[2025-08-01 10:05, 2025-08-01 10:10)`，不是 10:05 结束的窗口。消费端使用 `rsplit(":", 1)` 分离时间与类型，避免拆错时间中的冒号。消息不是 JSON。

全部类型为：

- `GPU_REALTIME`
- `NODE_REALTIME`
- `REQUEST_REALTIME`
- `MODELINSTANCE_REALTIME`
- `MODELMETRIC_REALTIME`

顺序不影响就绪判断；同一类型重复收到仍只算一种。时间格式严格为 `YYYY-MM-DDTHH:MM`，按 `Asia/Shanghai` 解释，分钟必须对齐 5 分钟，不接受秒、微秒或时区后缀。窗口未结束、早于已初始化的 `coverage_start`、未知类型或格式错误的消息不会授权汇聚执行。

## 调度与等待

消费发生在调度主线程的有界轮询中，不另建 Redis 消费后台线程。默认一轮最多处理 100 条消息；队列为空时阻塞等待最多 1 秒。SQL 执行期间不会并行消费，消息继续保留在 Redis，下一轮再处理。

同一窗口的五种信号齐全后，调度器可提前登记该窗口的全部 **13 个 `PT5M` 任务**，不必等它们原来的 cron 时间。登记采用现有窗口 ID 去重，不推进 cron 计划游标，也不将任何任务提前标为成功。

Cron 继续从持久游标登记到期窗口。事件登记、cron 登记和手工执行均经过同一执行器检查：启用就绪消费后，缺少任一种信号的 `PT5M` 窗口继续等待。小时、日、月任务仍要求覆盖其时间范围的全部上游窗口成功。

从**窗口结束时间起 60 秒**仍未齐全时记录 `DWD_WAIT_TIMEOUT` 告警，包含 `window` 和 `missing` 类型；即使一条信号也没收到，也应发现并告警。超时不会触发 partial 汇聚，不会把缺失类型视为成功。之后继续等待，信号补齐后再执行。这里的 60 秒不是从首条消息到达开始计算；检查随调度轮次进行，长 SQL 或积压可能使日志晚于阈值出现。重复告警按 `wait_warning_seconds` 节流，任务保留为 `BLOCKED`。

运行过程中 Redis 轮询失败不会放行未就绪窗口。SQLite 中已经持久化为五类齐全的窗口，仍可继续执行汇聚；Redis 恢复后继续接收其余信号。首次创建客户端是另一条路径：Cluster 客户端需要发现拓扑，初次无法连接或认证失败可能直接使服务启动失败，应修复连接后重启。进程健康探针和业务就绪信号是两件事：心跳正常不代表每个数据窗口已齐全，应同时检查缺失信号告警和任务状态。

## 唯一消费者与可靠确认

`modelverse:dwd:ready` 由这一个 data-converge 调度实例独占消费。**不得同时启动 Java 消费者、另一套 Python 消费者或人工 `BRPOP` 同一 List。** List 会把消息分给不同消费者，导致没有任何一方收到完整的五类信号。

消费与确认按以下顺序进行：

1. 使用 `BRPOPLPUSH` 将原 List 最旧的消息原子转移到 processing List；有积压时使用非阻塞转移继续处理。
2. 在主线程把合法消息对应的 `(window_start, dwd_type)` 就绪事实提交到 SQLite。
3. SQLite 提交成功后，才使用 `LREM` 从 processing List 确认移除该条消息。
4. 每轮先恢复 processing List 中未确认的消息，再接收新消息。

“转移到 processing，再确认移除”沿用 Redis 官方的可靠队列模式。`BRPOPLPUSH` 自 Redis 6.2 起被标记为 deprecated，官方建议新代码使用 `BLMOVE RIGHT LEFT`；本次保留旧命令以兼容既有 Redis 版本，部署时仍需确认命令可用。这不是端到端 exactly-once 保证。[Redis BRPOPLPUSH 文档](https://redis.io/docs/latest/commands/brpoplpush/)

格式错误、早于覆盖起点和未结束窗口的消息会记录相应诊断后确认移除，不重新入队，避免坏消息堵住 processing。它们不会写入就绪事实；提前发送的合法类型消息被丢弃后，需要生产端在窗口真实完成后重新发送。

这提供可恢复的重复投递，不承诺 Redis 与 SQLite 的分布式事务。重复处理通过 SQLite 唯一键去重；重复信号也不会重放已经 `SUCCESS` 的 SQL。SQLite 提交异常会停止服务且不 ACK；ACK 失败不会撤销已提交的就绪事实，已经收齐的窗口仍可执行。

默认两个键为：

```text
source:      modelverse:dwd:ready
processing:  {modelverse:dwd:ready}:data-converge:processing
```

默认 processing 的 hash tag 使用 source 的完整键名，使两个键落到同一 Redis Cluster slot，不需要修改生产端原有键名。processing key **只能由程序派生，不能在 YAML 显式配置**。自定义 source 已包含有效 hash tag 时，派生键保留该 tag，并追加完整 source 的 SHA-256 摘要，使共享一个 slot 的不同 source 不会共用 processing List。

唯一消费者是部署约束，不是 Redis 分布式锁。继续使用单实例、持久状态目录、Recreate，以及 hostPath 对应的固定节点。本机 `flock` 只保护同一状态目录，不能防止另一节点用另一个目录运行消费者。迁移前必须停止旧实例并迁移完整 SQLite 状态，不能在新节点初始化空账本后宣称高可用。

## 公司 Sentinel 连接与认证

公司 Python 参考使用 `Sentinel(...).master_for("redis-cluster", db=5)`，用户已在容器中验证这条连接路径。本实现继续使用原有 List 消费流程。`redis-cluster` 在这里是 Sentinel 主节点组名，不能据此改用 `RedisCluster`，也不固定某次发现的主节点 IP。

业务配置如下；不修改公司构建、依赖、启动脚本或 Deployment：

```yaml
dwd_ready:
  enabled: true
  queue_key: modelverse:dwd:ready
  block_seconds: 1
  batch_size: 100
  wait_warning_seconds: 60
  redis:
    mode: sentinel
    service_name: redis-cluster
    db: 5
    username: admin  # 可配置；Sentinel 模式省略时默认 admin。
    password_file: /opt/its/data-converge/secret/redis/password
    paas_name: null
    kms_timeout_seconds: 30
    root_key_file: /opt/its/secret/its-common-secret/redis_root.key
    work_key_file: /opt/its/secret/its-common-secret/redis_shared.key
    ssl: false
    ssl_verify: true
    socket_timeout: 3
    socket_connect_timeout: 2
```

### 挂载依据

用户上传的 `apiVersion.txt`、原平台模板及现场命令回执确认了以下映射：

| 输入 | 已确认来源 |
|---|---|
| `redis_root.key` | Secret `redis-cipher-secret` 的同名键；挂载 `/opt/its/secret/its-common-secret/redis_root.key` |
| `redis_shared.key` | Secret `redis-cipher-secret` 的同名键；挂载 `/opt/its/secret/its-common-secret/redis_shared.key` |
| 服务 Secret 目录 | `gde-adapter-certfiles` 整体挂载 `/opt/its/data-converge/secret`，现场列出的键均为证书及其配套材料，不含 Redis 密码 |
| Redis 密文来源 | `kube-system/its-redis-pwd-secret` 的 `password` 键，已通过 XMS 解密并完成连接验证 |
| Redis 密文文件 | 业务配置约定 `/opt/its/data-converge/secret/redis/password`；原 Deployment 尚未挂载，需发布时补齐，或将 `password_file` 改为实际挂载文件 |
| XMS | 沿用原 Deployment 的 `XMS_SERVICE_URL`、SDK 配置及密钥挂载 |

`CIPHER_ROOT=/opt/its/data-converge/secret/redis-cipher-secret` 只是已有环境值；实际单独挂载的根/工作密钥在公共 Secret 目录，所以本实现使用已确认的实际文件路径。没有照搬参考服务的 `/app/cert/...`。

密码文件内容是 Secret `password` 键投影出的原始密文，不是诊断用的 Secret JSON，也不是明文密码。若挂载到另一目录，只调整 `password_file`。此次只修改业务代码与配置，未代改 Deployment；诊断临时文件不作为正式密码来源。`--validate-only`、`--init-state` 不读取秘密文件，因此离线 VALID 不证明挂载、SDK 或连接已就绪。

### 凭据与认证流程

1. 启动时有界读取 Redis 密文文件，复用现有隔离 XMS helper 调用 `dsf.crypto.xms_crypto.xms_decrypt`，继续 Base64 解码 PMS。现场已经验证实际 Redis 密文可由该 XMS 方案解密。
2. PMS `IPS` 解析为 Sentinel 地址列表，支持 `host:port` 和 `[IPv6]:port`；最多 8 个节点，拒绝重复、空值和不合法端口。按 `username` 唯一选择账号，默认 `admin`，可显式改为 `ossdbuser` 等已有账号；空值、缺失账号或重复匹配均拒绝，不自动回退账号。保留密码原值；指定 `paas_name` 时按准确服务名选取，未指定时要求恰好一个含 `IPS` 且匹配该账号的服务。不会直接取 `paasconfig[0]`。
3. `psf` 初始化原根/工作密钥，复用平台 PBKDF2、根物料 XOR 和 `CipherStructureV1`。本适配只处理 Redis 的 1024 字节密钥结构、AES-GCM 和末尾零填充；不把 ClickHouse 密码当作 Redis 密码。
4. 每次建立 Sentinel 或数据连接时，用原 PSF `gen_random_hex_string(32)` 生成挑战随机数，发送 `AUTH <随机数`，解密服务端挑战并核对回传随机数，再发送 `AUTH >加密应答`。认证失败不会退回明文 AUTH。
5. Sentinel 连接不执行 SELECT；发现数据主节点后，数据连接才执行 `SELECT 5`。`username` 用于 PMS 账号选择，不作为公司 AUTH 的额外参数。
6. 数据客户端、Sentinel 客户端及各自连接池在退出时关闭；断连或旧主节点 READONLY 后，由下一轮重新发现主节点并重新认证。隐式命令重试为 0，不自动重发结果不明的队列命令。

现场 `admin` 凭据已通过 Sentinel 和数据节点认证，但数据节点 `ACL WHOAMI` 返回 `default`。`username` 是 PMS 凭据选择，不等于服务端 ACL 身份；不因为 WHOAMI 的返回值改写配置或推断所有管理权限。正常消费无需执行 WHOAMI，不新增这条命令的权限要求。

参考代码处理 `ERR >` 后又切掉首字符，可能损坏密文。当前实现根据 redis-py 的解析行为，接受 `>hex` 或 `ERR >hex`，只去掉一次完整前缀，并检查随机数和应答结构；AUTH 前不发送健康检查 PING。这里使用每连接回调，没有全局替换 redis-py 方法、导入即连接、全局单例或后台清理进程。

PMS 明文只在内存及受控 helper 管道中传递；应用异常使用固定错误码。PSF/Crypto 只在 Sentinel 运行路径导入，模块导入、离线校验不加载它们。公司 SDK 自身日志策略由平台控制，现场需确认不会输出密钥材料。

参考的 Sentinel 连接没有启用 TLS，本业务配置与之保持一致。PSM 保护认证应答，不替代传输 TLS；现场端点若启用 TLS，设置 `ssl: true`，`ssl_verify: true` 会校验证书和主机名，公司 CA 使用系统信任或 `SSL_CERT_FILE`。同一配置用于 Sentinel 和数据节点；混合 TLS 模式不在本次支持范围。

### 账本绑定与旧模式

Sentinel 模式将配置身份以及解密得到的 Sentinel 地址集合、组名、数据库和账号绑定到账本。密码轮换和地址排序变化不改变身份；正常主节点切换也不改变身份。PMS 变成另一套 Sentinel 地址时启动拒绝继续，避免拿旧的 ready 事实消费另一条数据源。已初始化的直连账本不能直接通过切换 `mode` 复用；需停机核验后做明确的状态迁移，不能删除账本绕过。

默认 `admin` 面向新部署；已经用 `ossdbuser` 初始化账本的部署，升级时继续显式保留 `username: ossdbuser` 及原密码路径。更改账号或密码文件路径会触发绑定检查，不能通过重新初始化或删除账本绕过。配置账号后重启生效，不支持运行中切换身份。

原有 `standalone` / 原生 `cluster` 直连仍保留，使用 `host`、`port`、`password_env` 和可选 ACL `username`，不混用 Sentinel 文件参数。直连密码环境变量须为可直接认证的值。原生 Cluster 仍只允许数据库 0；Sentinel 的数据库 5 不受这一限制。[Redis SELECT 文档](https://redis.io/docs/latest/commands/select/)

连接实现支持 redis-py 5/6，实际回环测试覆盖 5.0.8 和 6.4.0。参考接口依据：[redis-py 6.4 Sentinel 文档](https://redis.readthedocs.io/en/v6.4.0/connections.html#sentinel-client)。公司环境需已有 `dsf`、`psf`、`Crypto`，本次没有修改任何依赖清单。旧 `deploy/deployment.patch.yaml` 的 `DWD_REDIS_HOST/PORT/PASSWORD` 段仅适用直连模式，不可整份套到本次 Sentinel 配置。

### 发布端边界

本机恢复的 modelverse 工程仍使用公司 Java Redis SDK；其 `EnrichmentScheduler` 直接调用下游聚合，未找到附件所述 `RedisDwdReadyNotifier` 实现。这是本机源码状态，不能据此断言当前集群未部署通知。用户现场已验证 Python 认证连接；检查时 DB 5 的 `modelverse:dwd:ready` 不存在，仅表示当时没有该键。生产端是否实际发布、是否使用同一 Sentinel 主节点组/DB/独占 List，以及五类齐全后的 SQL 执行，仍需主流程验收。

### 容器运行条件

当前容器已具备 Redis 认证连接条件：Python 3.12.10、redis-py 5.2.1、架构依赖目录中的 `dsf` / `psf` / `Crypto`、XMS 环境、两条 PSM 密钥和 Sentinel 网络均已现场成功。正式交付仍需：

- 将 `its-redis-pwd-secret/password` 持久挂载，并部署包含 `data_converge.dwd_ready` 的新版业务包。先前镜像未发现该模块。
- 由原公司依赖交付方式保留 redis-py 5/6，以及 PyYAML、APScheduler 3.11.x；用户目录临时安装成功不证明镜像重建后仍存在。本次不修改构建或依赖清单。
- 确认 SQLite、Asia/Shanghai 时区、可写持久状态目录和唯一消费者；首次按约定边界初始化状态，已有状态继续保留。
- 通过真实就绪消息验证 `LINDEX`、`BRPOPLPUSH`、`RPOPLPUSH`、`LREM` 权限和业务流程。此前 PING/TYPE/LLEN 成功不能代替消费验收。

以下命令全部在 **data-converge 容器内**执行，不需要上传文件；只检查运行条件，不读取密码内容、不消费消息。平台环境必须先加载，`PYTHONPATH` 才能找到 SDK 和业务模块：

```bash
cd /opt/gde/python/python-3.12.10
source ./python.env </dev/null

DC_SERVICE_HOME=/opt/its/data-converge
case "$(uname -m)" in
  aarch64|arm64) DC_DEP="$DC_SERVICE_HOME/scripts/dependency_arm" ;;
  x86_64|amd64) DC_DEP="$DC_SERVICE_HOME/scripts/dependency_x86" ;;
esac
export PYTHONPATH="$DC_SERVICE_HOME/lib/data-converge/src:$DC_SERVICE_HOME/bin/py/site-packages:$DC_DEP:$DC_DEP/gaussdb_client${PYTHONPATH:+:$PYTHONPATH}"
export LD_LIBRARY_PATH="$DC_DEP/gaussdb_client${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export PYTHONDONTWRITEBYTECODE=1
cd "$DC_SERVICE_HOME"

command -v python3
python3 --version
python3 -c '
import os, sys
from importlib.util import find_spec
from importlib.metadata import version, PackageNotFoundError
from pathlib import Path
print("解释器 =", sys.executable)
for name in ("redis", "dsf", "psf", "Crypto", "yaml", "apscheduler", "data_converge.dwd_ready"):
    try:
        spec = find_spec(name)
        print("模块", name, "=", spec.origin if spec else "缺失")
    except ModuleNotFoundError:
        print("模块", name, "= 缺失")
for package in ("redis", "PyYAML", "APScheduler"):
    try:
        print("版本", package, "=", version(package))
    except PackageNotFoundError:
        print("版本", package, "= 未找到安装信息")
for value in (
    "/opt/its/data-converge/secret/redis/password",
    "/opt/its/secret/its-common-secret/redis_root.key",
    "/opt/its/secret/its-common-secret/redis_shared.key",
):
    print("文件", value, "存在 =", Path(value).is_file(), "可读 =", os.access(value, os.R_OK))
import sqlite3
from zoneinfo import ZoneInfo
with sqlite3.connect(":memory:") as db:
    print("SQLite =", db.execute("select sqlite_version()").fetchone()[0])
print("时区 =", ZoneInfo("Asia/Shanghai"))
'

# 新业务包部署后执行；只校验配置，不连接 Redis/ClickHouse，不创建账本。
python3 -m data_converge.main --config "$DC_SERVICE_HOME/lib/data-converge/resources/config/app_config.yaml" --validate-only
```

若修改了 `password_file`，文件检查命令中的密码路径也要相应替换。平台 `CLICKHOUSE_URL`、`CLICKHOUSE_PORT` 沿用 Deployment 注入。`VALID` 仅说明业务配置可加载；正式运行仍需上述持久挂载、状态准备和消息联调。

## 故障恢复

| 故障位置 | 恢复行为 |
|---|---|
| 消息已转到 processing，SQLite 尚未提交时进程退出 | 消息仍在 processing，重启后重新处理 |
| SQLite 已提交，ACK 前进程退出或 ACK 失败 | 重读消息；就绪事实幂等写入，再确认；不重复执行成功 SQL |
| 已启动服务的 Redis 轮询失败 | 记录连接故障并稍后重试；未就绪窗口等待，已就绪窗口可继续 |
| 初次加载 PMS/PSM 失败 | 启动失败，检查密文路径、XMS/PSF SDK 和密钥挂载；错误信息不包含凭据 |
| Sentinel 发现、认证失败或旧主节点降级 | 已有 ready 事实仍可使用；其余窗口等待，后续轮询重新发现并挑战认证 |
| 原生 Cluster 首次客户端创建失败 | 可能在拓扑发现时使启动失败；该分支不使用公司 PSM |
| 五类信号缺失，包括全部缺失 | 窗口结束 60 秒后告警并持续等待，不执行 partial |
| SQL 已发出但结果不明 | 沿用 `UNKNOWN` 人工核验流程；不能靠重复 ready 信号重发 SQL |
| Redis 数据丢失，且信号尚未写入 SQLite | 无法凭消费者还原信号；核验 DWD 完整性和生产端记录后再由受控流程补发 |
| SQLite 状态丢失 | 停止服务并恢复账本；不可删除 processing 或重新初始化来绕过核验 |

不要通过清空 processing List 解决消费故障，也不要把收到 ready 当作下游 SQL 完整写入的证据。生产端通知为 best-effort：DWD 写入成功但通知失败时，本实现选择等待和告警，需要生产端恢复或人工核验后补发信号。

## 与 Java 参考规格的差异

| 项目 | 附件中的 Java 参考方案 | 本次 Python 方案 |
|---|---|---|
| 运行位置 | DWD 主进程内的消费守护线程 | 独立 data-converge 主线程，有界同步轮询 |
| 消息获取 | `BRPOP` 后放入内存 | 原子转 processing，SQLite 提交后 ACK |
| 聚合状态 | 内存按窗口保存五类集合 | SQLite 持久就绪事实，重启可恢复 |
| 缺失超时 | 首条消息后 60 秒可触发 partial | 窗口结束后 60 秒告警，继续等待全部五类 |
| Cron | 可在信号缺失或 Redis 故障时直接触发 | 继续登记窗口，执行器仍检查 ready，不能绕过 |
| 幂等依据 | Redis processed guard，TTL 15 分钟 | 现有持久窗口状态；ready 本身不标记 SQL 成功 |
| 下游执行 | enrichment、aggregation、quantile Java 链 | 原 52 个 Python SQL 任务及完整窗口依赖 |
| 部署互斥 | Java 主节点与 Redis 锁体系 | 独占队列、单实例、同一持久状态目录；不承诺跨节点选主 |

这些差异来自本次确认的消费位置和“缺失继续等待、禁止 partial”要求。生产端 LPUSH 格式和五个类型保持兼容；不同时部署附件中的 Java 消费者。

## 公司环境联调

联调需确认实际 Redis 模式、端点、数据库、TLS/ACL 和生产端发布开关，再以完整五类消息验证正确窗口触发。还需覆盖重复和乱序信号、零信号告警、转移后重启、提交后 ACK 失败、Redis 恢复以及 SQL `UNKNOWN` 不重发。联调过程中确认旧 Java 下游调度和消费者的切换安排，避免两套流程同时写同一目标窗口。

本地测试覆盖解析、状态恢复与调度限制。真实 redis-py 5.0.8/6.4.0 已与本机 RESP 协议服务端验证 Sentinel 发现、两次 AUTH、SELECT 和主节点断连/READONLY 后的恢复。当前机器未安装 `redis-server`，该真实服务测试明确跳过；PSM 使用真实 AES-GCM 与合成 PSF 接口测试，没有真实平台 SDK/生产格式向量。这些不等于公司服务验收。实际密文键名、SDK 兼容性、TLS、Sentinel 故障切换、发布端信号和目标 SQL 仍需公司环境联调。
