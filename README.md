# data-converge-lite

以公司现有源码为基础的第一阶段主流程迁移。保留 `Services/Service/data-converge`、`Services/Package` 和原有 52 个汇聚任务。Python 调度依赖 PyYAML、APScheduler（Cron 解析）、redis；ClickHouse 与 Redis PMS 密文使用平台已有 `dsf` XMS 解密。公司 Redis 连接采用 Sentinel + `psf`/`Crypto` 挑战认证，用于消费 DWD 就绪信号。

本阶段交付可启动、可测试、可记录执行状态的汇聚主流程。Redis 解密、认证和连接已经用户在公司容器中验证；**尚未完成就绪消息到汇聚 SQL 的主流程联调，也未宣称原有 52 个 SQL 的业务统计口径全部正确**；延期的问题见 [已知问题](docs/known-issues.md)。

## 已迁移的主流程

```text
start.sh → GDE Python / 平台依赖 → 配置与 XMS 解密
         → 同一状态目录加锁 → 恢复遗留 RUNNING 为 UNKNOWN
         → Redis 就绪消息 → SQLite 落盘后确认 → 五类齐全
         → 持久 cron 游标 / 就绪事件 → 完整上游依赖 → HTTP 执行 SQL
         → 保存窗口结果 / query_id → 下一轮
```

- ClickHouse 的地址、HTTP(S) 端口来自部署环境；SQL 通过 HTTP POST 直接执行。
- 密文由同一解释器调用 `dsf.crypto.xms_crypto.xms_decrypt`，继续 Base64 解码 PMS，并按配置的服务名和用户名选取密码。
- SQLite 只保存计划游标、窗口状态、尝试次数和查询 ID；成功窗口不会自动再次执行。
- 明确未发送的连接拒绝、DNS 故障可以限次重试；超时、断连、响应异常、进程遗留执行均记为 `UNKNOWN`，不盲目重发 INSERT。
- 上级汇总要求其时间范围内全部上游窗口成功；晚触发从持久游标补齐。
- 同窗五类 DWD 信号收齐才执行五分钟任务；缺失 60 秒告警并继续等待，cron 和手工执行也不能绕过。消息先转入处理中队列，SQLite 落盘后确认，支持中断恢复。
- 前台进程接收 SIGTERM；停止新任务，保存当前结果后退出。探针检查本机心跳，不解密或访问数据库。

本阶段不加入原 GitHub 完整版的统计私表、迟到重算、分区发布、自动对账、报表、跨节点选主。公司原有 `leader_election` 和平台生命周期辅助文件保留便于对照，实际消费由单实例和同一状态目录写锁约束。

## 目录

```text
Services/Service/data-converge/
  src/data_converge/       # config / dag / dwd_ready / scheduler / sql_execution / state
  resources/config/       # app_config.yaml、52条 convergence_rules.yaml
  resources/sql/          # 原52个SQL，第一阶段只修两项执行缺陷
  test/                   # 离线、回环HTTP、恢复与打包测试
Services/Package/
  images/bin/             # 启动、停止、探针和平台脚本
  charts/templates/       # 原平台模板
Services/build_framework/scripts/build.sh  # 上传的原始构建脚本，保持不变
deploy/deployment.patch.yaml
```

## 业务代码离线校验

公司构建、依赖及测试配置保持上传版本，Python 版本按原平台配置使用。业务需要 PyYAML、APScheduler 3.11.x 和 redis-py 5/6（Sentinel 回环验证使用 5.0.8、6.4.0），公司认证使用已有 `dsf`、`psf` 和 `Crypto`。需确认已有公司镜像或依赖包提供这些模块及兼容版本；本次不修改任何依赖清单、构建框架、打包、启动脚本或 Deployment。测试使用现有环境与命令行参数。

```bash
export PYTHONPATH="$PWD/Services/Service/data-converge/src${PYTHONPATH:+:$PYTHONPATH}"
# 显式指定本次业务路径；不修改公司原测试配置。
python3 -m pytest -c /dev/null -p no:cacheprovider Services/Service/data-converge/test -q

export CLICKHOUSE_URL=clickhouse.example.internal
export CLICKHOUSE_PORT=8443
python3 -m data_converge.main --validate-only
```

离线校验只验证配置、52任务、SQL文件、Cron及窗口依赖，不读取密码、不导入 SDK、不访问 ClickHouse/Redis、不创建状态。上面的域名只用于离线示例，不代表实际服务器。

默认配置 [app_config.yaml](Services/Service/data-converge/resources/config/app_config.yaml) 对应公司 Deployment：

| 配置 | 来源/说明 |
|---|---|
| `clickhouse.host` | `${CLICKHOUSE_URL}`，裸主机或 HTTP(S) 根地址 |
| `clickhouse.port` | `${CLICKHOUSE_PORT}`，来自 ConfigMap 的 `httpPort` |
| `secure` | 默认保留公司 TLS 设置 `true`；核对实际 HTTP/HTTPS，不能仅凭端口名称判断 |
| `verify` | 默认 `true`；平台自签 CA 可由 `SSL_CERT_FILE` 提供信任链 |
| `username` | 默认 `nethouse`，按 PMS `info[].user` 选择对应密码 |
| `password_file` | `/opt/its/data-converge/secret/clickhouse/password` |
| `state.directory` | `/srv/its/MindOpsBOSE/data-converge/data/lite-state` |

SDK 由公司提供，匹配 CPU 架构及 GDE Python 版本。已有 `XMS_SERVICE_URL`、`XMS_SDK_SECRET_PATH`、`XMS_WORKSPACE_DIR` 和相应挂载需保留。程序只在启动时解密一次；密码或 SDK 配置变更后重启。密码不应写入 YAML、命令行或 Git。

[本地配置示例](Services/Service/data-converge/resources/config/app_config.local.example.yaml) 使用 `password_env`，它与 `password_file` 互斥；真实执行前由外部环境提供该变量。

平台配置默认开启 DWD 就绪消费，使用 **Sentinel、主节点组 `redis-cluster`、数据库 5**。启动时从 Redis 密文文件经 XMS 解密、Base64/PMS 解析，获取 `IPS` 中的 Sentinel 地址和指定账号的密码；每次连接和重连均执行公司 PSM 双向挑战认证。这个主节点组名不表示原生 Redis Cluster。

`dwd_ready.redis.username` 可配置，Sentinel 模式省略时默认 `admin`，也可显式填写 `ossdbuser` 等 PMS 中存在的账号。选定账号不存在或匹配不唯一时失败，不自动换用其他账号。它只用于选择凭据，不作为自定义 AUTH 的用户名参数。现场使用 `admin` 凭据认证成功后，`ACL WHOAMI` 返回 `default`；这不需要把配置改成 `default`，也不能据此认定具有全部管理员权限。直连模式的可选 ACL 用户名保持原语义。

上传 Deployment 已确认根/工作密钥路径为 `/opt/its/secret/its-common-secret/redis_root.key` 和 `redis_shared.key`。现场确认 Redis 密文来自 **`kube-system/its-redis-pwd-secret` 的 `password` 键**；`gde-adapter-certfiles` 只有证书相关内容，不含 Redis 密码。业务配置约定 `password_file: /opt/its/data-converge/secret/redis/password`，但原 Deployment 尚无这一挂载，发布时需将该键挂到此文件，或调整配置指向实际挂载文件。文件应是 Secret 键对应的密文内容，不是 `kubectl get secret -o json` 的 JSON 文档。

当前容器的 Python 3.12.10、redis-py 5.2.1、`dsf`、`psf`、`Crypto` 和密钥已完成连接验证。正式运行仍需部署新版业务包、持久挂载密码，并保证镜像重建后依赖仍在；用户目录临时安装的 redis 不代表已固化到镜像。业务主流程还需 PyYAML、APScheduler 和 SQLite，容器核验命令及详细边界见 [DWD 就绪消费](docs/dwd-ready.md#容器运行条件)。本地示例默认关闭该功能；已有账本不能通过改开关绕过门控。

**保留的公司 SQL 使用 `modelmate_metric` 全限定表名。只修改连接的 `database` 不会改写 SQL 的目标库。** 本阶段不创建或迁移业务表；所有原始表、目标表和资源维表须先按公司合同准备好。禁止把连接参数当作隔离验收库重定向开关。

## 首次启用与正常运行

先停用旧调度器并选定切换边界，确保只有一个写入者。初始化仅创建本地账本，不访问 ClickHouse、不创建业务表。

```bash
# 仅首次执行；默认起点为当前时刻向上取整到下一个5分钟边界。
python3 -m data_converge.main --config /path/app_config.yaml --init-state

# 若已明确切换边界，可以显式指定带时区、对齐5分钟的时间。
# python3 -m data_converge.main --config /path/app_config.yaml \
#   --init-state --start-time '<切换时间，ISO8601带+08:00>'

python3 -m data_converge.main --config /path/app_config.yaml
```

首次只处理 `data_start >= coverage_start` 的完整窗口：中途启用不会自动补写过去半小时、当天或当月。小时、日、月分别等待第一个完整周期结束。已有库重复初始化不会回退起点、清空记录或重新执行成功窗口。

不要删除账本来解决故障，也不要将完整版的状态库直接复制给轻量版。账本丢失后启动会拒绝运行，需先恢复备份并核对已写入的业务窗口。改变端点、账号、任务窗口/目标定义或 DWD 门控开关/队列绑定会触发检查；普通并发、超时调整不影响绑定。Sentinel 模式还在启动时绑定 PMS 中的 Sentinel 地址集合、组名、数据库和账号；密码轮换及地址顺序变化不影响该绑定，主节点正常切换也不影响。此前按直连模式初始化的账本不能直接切换模式后继续使用，需停机核验并制定状态迁移方案。

```bash
# 只读状态，无需解密或数据库网络，显示计数、最近20个任务及DWD接收窗口。
python3 -m data_converge.main --config /path/app_config.yaml --status

# 执行截至当前时刻到期的窗口一轮，然后退出。
python3 -m data_converge.main --config /path/app_config.yaml --run-once

# 手工执行指定窗口，仍检查覆盖起点、成功去重及上游依赖。
python3 -m data_converge.main --config /path/app_config.yaml \
  --run-task task_gpu_realtime_to_5min_dws --scheduled-time '<ISO8601带时区>'
```

`--run-once` 存在非成功结果时返回非零；状态查询是定位失败/未知窗口的依据。默认每轮每任务最多登记100个错过的计划时间，每轮最多执行100个窗口；积压在后续轮次继续处理。

## 失败与人工核验

自动重试最多3次（包含第一次），仅适用于能确定未发送的连接错误。写入之前，`query_id` 和 RUNNING 状态已落盘。

- `SUCCESS`：不自动重放。
- `FAILED`：明确失败；连接类故障可自动重试，次数耗尽或修复配置后可人工重试。
- `UNKNOWN`：可能已经写入或部分写入；包括超时、断线、进程中断。先在执行节点的查询日志及目标窗口核验。
- `BLOCKED`：上游窗口未全部成功，之后每轮重新检查。

先停止服务，取得同一状态目录的写锁后再处理。以下命令要求真实核验备注，不对数据库做删除或补偿操作：

```bash
# 仅确认完整写入成功的UNKNOWN窗口可标为success。
python3 -m data_converge.main --config /path/app_config.yaml \
  --resolve-instance '<状态中的instance_id>' --resolution success --note '<核验依据>'

# UNKNOWN只有确认未写入、或已人工清理部分写入后才可retry；
# FAILED在修复连接/权限后也可人工retry，SUCCESS不接受此操作。
python3 -m data_converge.main --config /path/app_config.yaml \
  --resolve-instance '<状态中的instance_id>' --resolution retry --note '<核验及处理依据>'
```

轻量版不承诺跨数据库与SQLite的原子提交，也不自动判定未知查询结果。人工重试会生成新的 query_id。

## 打包与平台启动

核心代码不构建或安装 whl，直接沿用上传的 `build_framework`。`Services/build_framework/scripts/build.sh` 已恢复原始内容，不增加参数或替换构建步骤。公司流水线按 `build.conf` 调用该脚本：

1. `python3 -m compileall -b -f` 生成与源码相邻的 `.pyc`。
2. 删除流水线应用目录中的 `.py`。
3. 将整个 `Services/Service/data-converge` 复制到 `Services/Package/images/lib/data-converge`，保留 `src`、`resources/config` 和 52 个 SQL。

后续镜像及服务包继续由原平台 BuildTool/Maven 流程处理。第三方依赖仍使用平台的 `bin/py/python_pkg.tar.gz` 与公司 SDK 包；平台 `Services/Package/images/bin/pyproject.toml` 和原有根配置已恢复上传原版。原根配置随公司源码保留，核心交付仍只走上述 `build_framework`，不调用本项目的 whl 构建或安装流程，也不新增依赖清单和工具配置。

流水线需提供 `REPOSITORY_PATH`、`WORKSPACE` 及 `${WORKSPACE}/BuildTool/Services/build_framework/scripts/function.sh`。原脚本会删除构建副本中的源码，且目录变量未加引号，因此应在平台的一次性、无空格工作目录执行；本机测试也只在临时副本运行。构建与运行使用同一 Python 小版本，以保证 `.pyc` 兼容。外部 BuildTool 文件不在上传附件内，本仓库不替换它。

启动脚本默认安装路径 `/opt/its/data-converge`，可用 `SERVICE_HOME`、`PYTHON_ENV_FILE`、`APP_CONF_FILE` 覆盖。当存在多个 GDE Python 版本时，必须明确 `PYTHON_ENV_FILE`。依赖可预安装，或由平台提供 `bin/py/python_pkg.tar.gz`（根目录 `site-packages`）以及 `scripts/dependency_x86.tar.gz` / `dependency_arm.tar.gz`（同名根目录）。不从公网下载公司 SDK。

部署与探针最终都使用同一解释器和 PYTHONPATH。应用搜索路径仍为 `lib/data-converge/src`，`python3 -m data_converge.main` 可直接加载原构建生成的 `main.pyc`，XMS helper 也从 `.pyc` 启动。`start.sh` 前台 exec 主程序；`liveness_check.sh` 和 `readiness_check.sh` 读取进程心跳，缺失、过期或进程退出均失败。25642是原平台端口声明，本服务没有在该端口实现 HTTP API。

## 部署边界

[Deployment patch 示例](deploy/deployment.patch.yaml) 是此前直连 Redis 的部署建议，**不会自动应用**。其中 `DWD_REDIS_HOST/PORT/PASSWORD` 的占位引用只用于直连模式；本次 Sentinel 配置从已有挂载读取，不能整份照用旧补丁。公司原 Deployment、模板与构建文件均未修改；原有 ClickHouse/XMS 环境变量、Secret、UID 19101 和 run-data 挂载保留。运行互斥与状态目录仍须按以下要求核验。

- 一套目标表只运行一个调度实例。使用 Recreate，并确保旧实例停止、不会因节点失联仍在运行；本机锁不是分布式选主。
- 沿用 hostPath 时固定状态节点；迁移节点需在停机后迁移整个状态目录并核对结果。不要在新节点自动初始化空账本。
- 数据目录必须对运行用户可写；代码和依赖可预安装为只读。日志输出到容器标准输出/错误，由平台采集。
- SIGTERM等待当前执行波次退出；示例宽限360秒，单SQL默认300秒。服务器超时/中断仍按UNKNOWN处理，不能据容器退出推断数据库没有写入。
- 原始公司部署的跨节点 RollingUpdate + hostPath 不能直接提供这些保证。

## 验证与后续

测试覆盖配置、SDK隔离、真实本机HTTP(S)、截断/断线、窗口完整依赖、重启、重复窗口、重试、锁、退出信号、DWD信号/确认/门控、双架构启动和交付包。实际测试记录见 [验证记录](docs/validation.md)。公司 XMS、ARM 原生SDK、真实 Redis/ClickHouse 和 Kubernetes 切换需在公司环境单独联调。

第二阶段建议逐项修复 [已知统计问题](docs/known-issues.md)，每次使用小样例确认口径后再迁移。源码还原指纹与方法见 [导入记录](docs/source-import.md)。
