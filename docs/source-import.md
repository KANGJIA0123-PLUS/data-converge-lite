# 公司源码导入记录

来源：用户提供的 `合并文件 (2).txt`，2026-10-12 还原。

- 附件 SHA-256：`6f9d30cbf7ec9d2f779b3429410a9226bc58715a8223ff8beb095f67d96c62bf`。
- 原始文件共 **124 个**，其中 SQL **52 个**；保留 `Services/Service/data-converge/{src,resources,test}`、`Services/Package`、`Services/build_framework` 和根构建配置的原始路径。
- 23 个分片的还原顺序（按附件“文件 N/23”编号）：`16 → 17 → 14 → 11 → 18 → 22 → 2 → 5 → 8 → 6 → 4 → 12 → 21 → 13 → 9 → 10 → 23 → 1 → 3 → 19 → 15 → 7 → 20`。其中分片 20 为 736 行，其余每片 1000 行。

还原依据为文件遍历顺序与分片首尾的 Python、XML、SQL 连续语句。先移除附件分片包装，再按上述顺序拼接，最后依照“文件路径 / 完整路径”头拆出文件；删除导出器添加的文件头和分隔空行，不将其写入源码。目标目录在导入前不存在，未覆盖其他工作。

导入基线的静态验证：36 个 Python 文件通过 `ast.parse`；52 个 SQL 均由 SQLGlot 的 ClickHouse 方言解析为单条 `INSERT`；20 个 Shell 通过 `bash -n`；2 个 TOML 通过 `tomllib`；1 个 XML 通过 ElementTree；2 个普通 YAML 通过安全解析。4 个 Helm YAML 模板保留原始模板语法，仅在内存中替换标量占位符，并枚举所有 `if/else` 组合验证 YAML 结构；这不等于真实平台的 Helm 渲染验收。

未发现缺失文件头、片间重复或被截断的文件。原源码中的 Redis 密码为空；Secret、证书及密钥文件仅为路径引用。合并附件及本机临时还原脚本未纳入仓库。没有运行公司包脚本、连接公司服务或验证生产 SQL 的执行结果。

本记录描述导入时的基线；后续轻量化修改由 Git 变更记录追踪。附件不包含完整平台工具链或外部部署配置，因此还原完整性不代表可独立执行原平台发布流程。

2026-10-12 按用户要求恢复原 `Services/build_framework/scripts/build.sh`，保留全部脚本内容，仅去除导出器分隔空行、保留末尾换行。SHA-256：`a476480f2a4a346434f815395d524611a0cff2a5d4d9797144aa1339652254dd`。此后业务修改不调整该构建框架。

同日按用户要求恢复 `Services/Package/images/bin/pyproject.toml` 的原始 Python/依赖版本及源配置，不再添加 APScheduler、redis 的构建声明。原有根 `pyproject.toml`、`requirements.txt`、`pytest.ini` 同样恢复上传原版；只删除新增的 `requirements-dev.txt` 和 `ruff.toml`。业务运行所需的兼容依赖由已有公司环境提供。
