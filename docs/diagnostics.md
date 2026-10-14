# 运行定位与诊断命令

诊断入口沿用现有启动辅助脚本，在 **data-converge 容器内**执行。脚本负责加载平台 `python.env`、业务代码、依赖目录及动态库路径，不需要上传脚本或单独拼接 `PYTHONPATH`。公司正式服务入口仍为 `bash /opt/its/data-converge/bin/start.sh`。

更新到包含诊断模块的镜像后使用以下命令。现有 `--status` 输出协议不变；新增命令默认中文文本，追加 `--format json` 输出同一份结构化结果。

## 常用命令

先看哪里异常，本地检查不连接 Redis 或 ClickHouse：

```bash
bash /opt/its/data-converge/bin/start_data_converge.sh --diagnose
```

需要验证解密、认证和远端连通性时显式启用在线检查：

```bash
bash /opt/its/data-converge/bin/start_data_converge.sh --diagnose --online
```

只列异常任务，优先显示 UNKNOWN，其次 FAILED、BLOCKED；查询全历史异常，不会被最近的成功任务挤出：

```bash
bash /opt/its/data-converge/bin/start_data_converge.sh --failures
```

筛选资源汇聚任务，或翻到下一页：

```bash
bash /opt/its/data-converge/bin/start_data_converge.sh --failures --task task_resource_5min_ads --limit 20
bash /opt/its/data-converge/bin/start_data_converge.sh --failures --limit 20 --offset 20
```

查看一个实例。下面使用现场曾出现的实例 ID；查询其他窗口时从 `--failures` 复制完整 ID：

```bash
bash /opt/its/data-converge/bin/start_data_converge.sh --inspect-instance 'task_resource_5min_ads@2026-10-14T16:30:00+08:00'
```

查看最近完整窗口的信号，或精确定位 16:15–16:20 窗口：

```bash
bash /opt/its/data-converge/bin/start_data_converge.sh --dwd-status
bash /opt/its/data-converge/bin/start_data_converge.sh --dwd-status --window '2026-10-14T16:15:00+08:00'
```

导出可回传的诊断结果：

```bash
bash /opt/its/data-converge/bin/start_data_converge.sh --diagnose --online --format json
```

`--limit` 范围为 1–100，默认 20；`--offset` 从 0 开始，仅供 `--failures` 使用。`--window` 必须为带时区、对齐五分钟的窗口**开始时间**，也接受 UTC 输入，展示时转换为北京时间。`--online` 仅用于 `--diagnose`；错误组合及空实例 ID 会拒绝执行，不会启动服务。

## 每个入口能回答什么

| 入口 | 证据和范围 |
|---|---|
| `--diagnose` | 配置可读性、DAG/SQL文件与调度规则、Python解释器与SDK可发现性、凭据/CA文件权限、日志老化配置及日志路径、心跳、状态库绑定、历史异常计数、最近20个完整DWD窗口 |
| `--diagnose --online` | 追加ClickHouse凭据解析与固定`SELECT 1`；Redis凭据/PSM客户端创建与`PING`、源队列和处理中队列的`TYPE`/`LLEN` |
| `--failures` | 全历史异常总数、分页结果、任务/窗口、尝试次数、query_id、固定错误码及处理建议 |
| `--inspect-instance` | 单个实例、当前尝试的安全错误证据、SQL文件路径、当前未成功依赖、对应五分钟DWD状态；依赖最多显示100条并标明截断 |
| `--dwd-status` | 每个窗口的已收到/缺失类型、等待时长、最后接收时间、收齐延迟；包括一条信号也没收到的完整窗口 |

`--inspect-instance` 比较当前配置与状态库绑定，再按已记录的调度时间重建依赖。配置不一致时不猜测历史依赖；配置、依赖或附加错误证据损坏，也保留能够读取的实例基本信息。

任务的 `retryable` 只是原执行策略中的标记，仍需结合最大尝试次数及 `next_retry_at` 判断，不代表命令会发起重试。UNKNOWN 表示结果未确认，必须先凭 `query_id` 核实 ClickHouse 结果，再按原人工解决流程处理，不能直接重跑。

DWD `READY` 表示本消费者已记录五类信号；`WAITING` 表示缺信号；`BEFORE_COVERAGE` 和 `WINDOW_OPEN` 分别表示早于账本覆盖起点、窗口尚未结束。等待时长从窗口结束算起，收齐后停止累计，另用 `ready_delay_seconds` 保留收齐延迟。默认只看最近20个完整窗口，旧窗口用 `--window` 精确查询；更早的已登记阻断任务可用 `--failures` 查到。

## 输出与退出码

每项检查固定包含 `id`、`status`、`summary`、`evidence`、`action`。JSON顶层包含 `schema_version=1`、命令、检查时间、汇总状态和数据。

- `PASS`：该项检查通过，仅代表其声明的检查范围。
- `WARN`：存在需要关注的阻断/信号等待/配置提示。
- `FAIL`：已知错误、结果未确认的任务，或检查无法完成。
- `SKIP`：未启用、未显式请求，或前置条件不足，不能解释为正常。

退出码 `0` 表示没有 FAIL（可能包含 WARN/SKIP），`1` 表示存在 FAIL，`2` 表示参数错误。自动化可同时读取JSON中的各项状态。正常等待阈值内的窗口也可能是 WAITING，不能把整个命令 PASS 解读为全部窗口READY。

## 诊断边界与兼容

诊断业务代码不创建或修复状态库、不获取写锁、不创建心跳、不写业务日志、不改任务状态，也不消费或确认 Redis 消息。账本通过只读连接和单次读事务查看；Redis的两个队列读数为各自采样，不保证原子快照。辅助脚本仍沿用公司既有的环境准备流程，依赖尚未展开时可能先准备依赖文件。

在线检查复用原解密及认证实现，查询阶段沿用10秒ClickHouse超时和Redis配置的socket超时；解密使用原KMS超时。系统DNS阻塞仍受操作系统解析行为影响，不承诺整个命令固定10秒结束。Redis检查的 `credential_user` 表示选中的PMS账号，不等于服务端ACL用户名。

错误证据仅保存固定错误码、异常类型、query_id、HTTP状态、数字ClickHouse异常码、TLS校验码、OS错误号，不打印密码、SDK异常正文、服务端SQL或Redis消息内容。升级后的失败会在原任务完成事务中写入独立 `execution_diagnostics` 表；诊断旧库不创建该表，历史错误显示“未记录”。证据与当前实例的query_id匹配，已解决/成功或新尝试不展示旧证据。这里保留当前失败尝试，不建立完整尝试历史。

本地心跳通过只代表进程心跳与启动就绪，不能证明调度线程持续推进；诊断同时展示最近完成时间、最新窗口与最早活动窗口供判断。当前不新增调度线程探针、不改公司现有探针脚本。成功执行或`SELECT 1`通过不证明汇聚结果行数正确，也不验证所有现场表结构。原有资源汇聚SQL保持不变。

## 设计依据与实现分工

参考成熟项目的可操作设计：

- [Django CheckMessage 源码](https://github.com/django/django/blob/stable/5.2.x/django/core/checks/messages.py)：稳定检查标识、严重度、问题说明与修复提示。
- [Django check 命令源码](https://github.com/django/django/blob/stable/5.2.x/django/core/management/commands/check.py)：显式选择数据库检查及失败级别。
- [Kubernetes Pod 调试指南](https://kubernetes.io/docs/tasks/debug/debug-application/debug-running-pod/)：将配置、状态和故障原因放在一起查看。
- [Airflow 健康检查](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/check-health.html)：分别判断元数据库、进程心跳及组件状态。

本实现按现有服务规模采用命令分派、独立检查函数、统一结果与文本/JSON呈现，复用现有连接适配器；没有引入Django、Airflow或新的依赖。

```mermaid
flowchart LR
  CLI[诊断命令] --> S[只读诊断用例]
  S --> L[本地配置 / 文件 / 心跳]
  S --> DB[SQLite 只读快照]
  S -->|显式 online| C[既有 ClickHouse / Redis 连接]
  L --> R[统一检查结果]
  DB --> R
  C --> R
  R --> T[中文文本]
  R --> J[JSON]
```

`diagnostics/cli.py`负责参数与分派；`service.py`组合检查；`checks.py`执行独立本地检查；`ledger.py`限制只读账本查询；`windows.py`解释就绪窗口；`connections.py`复用既有连接；`report.py`提供统一呈现。错误证据在`errors.py`白名单过滤，经执行器传入状态库，与任务完成一同提交。
