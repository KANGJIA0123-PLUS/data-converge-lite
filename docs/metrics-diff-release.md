# 指标扩展发布说明（2026-10-20）

本次合入附件中的 16 个业务 SQL 和命名空间权限调整，并补齐任务配置及计算问题。当前共有 60 个任务。代码提交不等于数据库结构和集群权限已经完成升级。

## 已补齐的代码改动

- 注册 `task_model_server_{5min,hour,day,month}_ads` 四个模型服务资源汇聚任务，分别等待同周期实例 DWS 和 GPU DWS。
- 实例指标 ADS 新增卡数关联后，增加同周期模型实例 ADS 的任务依赖与源表声明，避免在卡数产出前执行。
- 新 SQL 使用与现有调度一致的绝对时间参数和系统时区；日、月窗口不再重新按数据库默认时区分桶。
- 卡数按节点 IP、卡号、芯片号共同去重。日、月忙闲指标使用 GPU DWS 已经划分好的忙闲字段，避免用日、月窗口起点的小时数重新分类。
- 节点指标限定源字段，避免 ClickHouse 将同名聚合别名展开为嵌套聚合；零分母按现有指标约定输出 0，避免 NULL 转非空浮点类型失败。PD 场景的每请求 token 分母与成功请求统计保持一致。
- `unit_token_computepower_consumption` 按“卡时／百万 token”的注释口径计算：`卡数 × 窗口秒数 × 1,000,000 ÷ (3,600 × 总 token 数)`。输入、输出 token 均计入总量。这里的卡数是窗口内去重卡数，不代表精确按绑定时长积分；窗口内动态扩缩容时属于估算口径。

## 数据库配套

附件和本仓库均未提供生产数据库 DDL。部署前须由数据库维护方按实际表引擎、分区、排序键、分布式表与本地表关系补齐结构；不要只发布镜像。以下 `<周期>` 为 `5min`、`hour`、`day`、`month`。

| 目标表 | 必需配套 |
| --- | --- |
| `t_mops_modelinstance_<周期>_ads` | 四周期增加 `allocated_npu_num`；日、月另外增加 6 个忙闲利用率字段及 `work_time_ai_core_util_status`。 |
| `t_mops__modelmetric_instance__<周期>_ads` | 四周期增加 `input_output_token_rate`、`request_received_total_qps`、`unit_token_computepower_consumption`。 |
| `t_mops__modelmetric_node__<周期>_ads` | 四周期各增加 17 列：成功请求增量、6 个时延 sum/count 增量及 10 个速率、比率和平均值字段。 |
| `t_mops__modelinstance_server__<周期>_ads` | 新增四张模型服务资源表，5 分钟／小时 SQL 写入 85 列，日／月写入 92 列。完整列名与顺序见对应 SQL 的 INSERT 列清单。 |

日、月新增忙时状态值沿用附件：`超负荷使用`（>80）、`常规使用`（15–80）、`低效使用`（0–<15），缺失值为 NULL。数据库枚举、字段可空性和报表展示应与之匹配。`allocated_npu_num` 按现有 GPU DWS 绑定键计数，新增 SQL 未按设备厂商进一步筛选。

在与服务相同的 ClickHouse 入口，用查询工具执行 [metrics-schema-check.sql](metrics-schema-check.sql)。这是只读 SQL，列出缺失的表字段；新表不存在时会列出其全部必需字段。返回空集只表示列名齐全，仍需核对列类型、可空性、枚举、表引擎及各分片结构。检查包含 452 项“表、列”组合，既覆盖现有表新增字段，也覆盖四张新表的全部写入字段。

另需核对既有的 `t_mops_modelinstance_<周期>_dws.dev_id`：实例 ADS 已经读取该列，但仓库上游 DWS 的 INSERT 列清单没有写入它，是否存在及如何取值由实际 DDL 决定。本次未修改这一既有逻辑，新增依赖会要求实例 ADS 成功后才能执行实例指标 ADS。

旧窗口数据不会自动拥有新的统计口径。升级中应避免用缺少新字段数据的历史 5 分钟结果验证小时、日、月的新指标；需要历史数据时，另行安排受控回算并先核对去重方式。

## 发布核对

1. 完成数据库结构变更，并在服务实际连接的节点执行上述只读检查。
2. 按 [RBAC 升级核对](rbac-upgrade.md) 检查完整发布包的安装／卸载脚本权限、命名空间，以及旧 ClusterRoleBinding 的残留。仓库不含外部 BuildTool 脚本，不能据此声称完整 Helm 生命周期已经验收。
3. 构建新镜像并保留原状态卷。若需为本次任务和 SQL 变化明确版本生效时间，可按[配置迁移说明](configuration-migration.md)安排维护窗口：保留可信旧配置与旧 SQL，停止旧写入实例，用 `--plan-config` 检查变化和生效时间，再按预览执行 `--apply-config`，完成后启动服务。2026-10-20 调测期间有效配置差异已临时告警放行，原快照及执行历史保留，见 [DC-STATE-002](known-issues.md#dc-state-002)；这不代表迁移已完成。清空账本后重新初始化不会恢复历史或自动补算，也不能假设所有旧账本都会进入新窗口恢复。
4. 首次调测部署按正常初始化流程启动；执行 `dconv --validate-only` 应加载 60 个任务，再执行 `dconv --diagnose --online --format json`。
5. 对新产生的完整窗口按任务查询，例如 `dconv --history --task task_model_server_5min_ads --limit 20 --format json`；将任务名替换为四个新增模型服务任务及对应实例指标任务逐项核对。全局最近 60 次尝试不保证覆盖 60 个任务，小时、日、月记录可能被较新的 5 分钟任务挤出。按周期等待相应窗口结束，不能以等待 5 分钟作为小时、日、月任务的验收条件。
6. 用源表样本核对目标表同一窗口的行数、卡数、token、请求增量和比率。异常实例先用 `dconv --inspect-instance '<实例ID>' --format json` 查询；`UNKNOWN` 不能直接当作未写入重跑。

本地验证覆盖真实 ClickHouse 25.8 的样例计算和 SQL 执行，但样例表不是生产 DDL；现场数据库结构与外部发布脚本仍需按上述步骤验证。
