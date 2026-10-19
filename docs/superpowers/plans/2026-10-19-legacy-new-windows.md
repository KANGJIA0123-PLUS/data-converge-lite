# Legacy New Windows Implementation Plan

**Goal:** 旧资料缺失时正常启动自动恢复新窗口，完整保留并隔离旧执行事实。
**Architecture:** 同账本历史实例映射隔离；备份后事务建立新基线；执行/恢复/诊断统一区分历史与当前。
**Tech Stack:** 原Python、SQLite和pytest，无新依赖。
**Spec:** docs/superpowers/specs/2026-10-19-legacy-new-windows-design.md

用户已明确授权功能及历史积压取舍。原生worktree创建因当前应用项目引用不匹配失败，使用目标仓库Git worktree。按已有授权直接实施、测试、独立复审后推送main。

## Global constraints

- Python函数、测试和fixture不超过50行。不修改业务SQL、公司部署/启动/打包脚本，不新增依赖。
- 保留原任务事实、UNKNOWN、coverage和选主身份；执行入口不得把历史事实当新配置的完成凭证。
- 自动入口仅正常serve；损坏/备份/锁错误不放行。使用共享同一物理账本与原注册。

## Review focus

1. history.record_recovery独立UPDATE修改历史RUNNING：状态任务测试。
2. 历史SUCCESS为新依赖放行/直接prepare绕过scheduler：状态任务测试。
3. DWD未来信号与长备份跨切点：root恢复测试。
4. 映射表丢失或普通apply丢失隔离标记：状态和root启动测试。
5. 旧未知结果仍被诊断推荐重试：诊断测试。

## Task 1: 历史事实隔离（状态代理）

Files: state/retirement.py, state/store.py, state/configuration.py, state/history.py, state/checkpoints.py; test/test_legacy_retirement.py。
- [x] 按spec API先写真实SQLite红测试，再实现同事务隔离/持久完整性验证。
- [x] 所有状态写入口、get_status、list_pending、恢复与constraints排除历史；原读接口不丢数据。
- [x] 测试完整rollback、重复/丢失标记、后续普通迁移保留隔离、全部旧事实不变；跑状态回归和行数检查。

## Task 2: 自动新窗口恢复（root）

Files: configuration_migration/legacy_recovery.py, application.py; test/test_legacy_startup_recovery.py。
- [x] 先写正常serve旧mismatch完整恢复、新窗口执行、非serve不自动恢复的红测试。
- [x] 持租约/文件锁，备份、校验目标文件、计算不重叠cutover、state.apply_configuration事务及安全日志。
- [x] 检查注册/备份/文件漂移失败不改账本、慢备份时间和DWD边界、幂等重启。

## Task 3: 诊断与运维说明（诊断代理）

Files: diagnostics/checks.py, diagnostics/service.py, diagnostics/recovery.py, diagnostics/ledger.py, errors.py, configuration_migration/cli.py, docs/configuration-migration.md; test/test_legacy_recovery_diagnostics.py。
- [x] 先测试active/historical统计、旧实例不按新配置解释、历史PREPARED不建议自动恢复/重试。
- [x] 接入spec接口，提供切点/备份/历史提示，旧plan缺基线时明确新窗口启动路径。
- [x] 文档明确无需旧包即可正常启动、历史仍待核查、日月完整窗口限制、故障边界。

## Task 4: 集成与发布

- [x] 独立复审，修复可复现问题，完整测试并核对既有失败集合。
- [ ] git diff --check、主分支同步、提交推送main，核对远端SHA；不声称已在生产执行。

## 验证记录

- 改动前全量：930 passed、44 skipped、7 failed；最终全量：1031 passed、44 skipped、7 failed。
- 失败为同一组既有 test_packaging：启动参数转发、两项平台前置失败、公司权限恢复、两项探针返回码、探针不安装依赖；没有放宽断言或修改打包脚本。
- 新增真实SQLite历史保存/隔离、事务回滚、慢备份切点、未来DWD信号、正常启动及幂等重启、本地HTTP新窗口执行、注册预检失败、版本栅栏和后续迁移下限验证。
- 注册预检覆盖Redis调用边界与返回语义，未连接真实Redis执行Lua；本地验证不代表已在生产恢复。
- 独立复审完成，补强较新账本丢失快照的误识别、后续迁移和当前快照的cutover下限、DWD诊断历史范围。
