# Data Converge Lite

基于 ClickHouse 的汇聚服务：接收 DWD 就绪信号，按任务依赖执行现有 48 个汇聚任务，并在本地账本记录执行结果。业务流程、命令入口和公司字节码构建方式保持兼容。

## 目录与职责

```text
Services/Service/data-converge/
  src/data_converge/
    main.py                 # CLI、输出和退出码边界
    application.py          # 校验、初始化、查询、执行等应用用例
    runtime.py              # 工厂装配与运行资源生命周期
    config/                 # 配置校验、任务配置和 Cron 编译
    dag/                    # 任务模型、依赖图、执行和重试判断
    scheduler/service.py    # 时间窗口与调度
    dwd_ready/              # 就绪门控、Redis 消费与平台认证适配
    sql_execution/
      config.py             # ClickHouse 配置与凭据解析入口
      transport.py          # HTTP/TLS、连接与请求截止时间
      client.py             # SQL 请求与执行结果分类
      runner.py             # 将任务实例适配为 SQL 执行
    state/                  # SQLite 执行账本
    observability/
      settings.py           # 不可变日志配置
      retention.py          # 轮转与保留策略
      handlers.py           # 文件写入、进程锁与归档
      logging.py            # 日志路径和 LoggingSession 生命周期
  resources/                # 业务配置与 SQL，位置不变
tests/                      # 仓库测试，不进入镜像
```

`Application` 作为用例门面；运行时工厂负责装配，`ExitStack` 管理资源释放；SQL/Redis 适配器连接外部系统；`StateStore` 管理持久化；日志策略与文件操作分别维护。旧的 `logging_config`、`scheduler.scheduler` 和 ClickHouse `client` 配置函数保留兼容导出。

## 日志

业务日志优先写入 `${LOG_PATH}/${SERVICE_NAME}.log`，同时保留 stderr 输出。没有 `LOG_PATH` 时兼容部署注入的完整文件名 `SERVICE_LOG_PATH`，再按 `PRODUCT_NAME` 拼接平台路径。本地没有平台日志变量时只输出到 stderr。公司启动日志 `start.log` 继续由平台管理。

在 `resources/config/app_config.yaml` 中配置，修改后重启服务：

```yaml
logging:
  max_bytes: 20971520
  backup_count: 10
  retention_days: 7
```

默认单文件 20 MiB、最多 10 个历史文件、历史保留 7 天。达到大小上限前或跨 UTC 日期后的首次写入触发归档；单条日志超过上限时仍完整保留，下一条写入前轮转。归档名称包含 UTC 时间和唯一标识，以 `.archive` 结尾。

启动、轮转后及写入时每隔 60 秒检查老化，按归档最后写入时间清理；无日志写入期间不运行后台清理线程，下一次启动或写入会继续清理。数量上限可能使繁忙时期不足 7 天的历史提前淘汰，7 天是保留上限。只清理本服务生成的归档，不删除其他服务、平台轮转文件或符号链接。

固定的 `.lock` 文件用于 POSIX 进程锁，协调服务与手工 CLI 的追加、轮转和清理，运行中不要删除。兼容平台重命名日志后的重新打开；应用归档与平台归档分别由各自策略管理。

## 构建与验证

公司 `Services/build_framework/scripts/build.sh` 保持原样：编译服务目录为 `.pyc`，再复制到 `Services/Package/images/lib/data-converge/`。`src/`、`resources/`、`python3 -m data_converge.main` 和 XMS 解密子进程入口不变。测试移出服务目录，从源头排除在镜像复制范围之外，无需为核心代码制作 wheel，也没有新增第三方依赖。

在已装好项目依赖的 Python 3.12 环境中，从仓库根执行：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD/Services/Service/data-converge/src" python3 -m pytest -c /dev/null -p no:cacheprovider tests -q
```

打包测试会在临时副本运行原自定义构建脚本，检查字节码入口、全部 SQL、资源内容和测试排除，并在仓库外运行校验与解密子进程。完整镜像组装仍需要公司的外部 BuildTool 和基础镜像。
