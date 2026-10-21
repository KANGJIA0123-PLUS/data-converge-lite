# 当前月提前汇聚验证记录（2026-10-21）

基线：`3ba7cb8`（保留主分支最新失败SQL日志改动）。本次只扩展当前月提前汇聚、校正节点月任务的日来源依赖，未修改业务SQL或部署脚本。

## 验证结果

- 新增96项月周期用例全部通过：覆盖15个实际任务、月中首次初始化和恢复、派发前持久化正式覆盖义务、重启/游标越过到期窗口、原始300秒截止、UNKNOWN和失败处理、配置保护、缺失午夜/DST/闰月/跨年。
- 相关回归及真实ClickHouse合计316项通过，无跳过。真实ClickHouse包含小时、天、月的6项演练：先提前写入，再追加源数据，关闭并重开SQLite，正式清理并覆盖，重复调度不增加行数；查询日志确认清理和INSERT阶段。
- 全量：2010 passed / 181 skipped / 42 failed / 11 errors。基线为1914 passed / 179 skipped / 42 failed / 11 errors；新增96项单元测试及2项需显式启用的月ClickHouse测试。53个失败/错误名称逐项相同，没有新增回归；本次未修复这些既存问题。
- 独立审查发现的月初缺失午夜边界已通过新增回归并修复，最终审查无剩余可操作问题。
- `git diff --check`、函数不超过50行检查通过。
- 文档Bash命令经`bash -n`及隔离Linux容器验证：GNU date正确生成10月1日至11月1日，使用替代启动函数验证全部15个调用，不连接现场、不写业务数据。

## 重现命令

在仓库根目录，使用具备项目依赖的Python环境：

```bash
PYTHONPATH=Services/Service/data-converge/src python -m pytest Services/Service/data-converge/test -q

RUN_CLICKHOUSE_INTEGRATION=1 PYTHONPATH=Services/Service/data-converge/src \
python -m pytest Services/Service/data-converge/test/test_preaggregate_*.py \
  Services/Service/data-converge/test/test_cross_period_*.py \
  Services/Service/data-converge/test/test_month_preaggregate*.py \
  Services/Service/data-converge/test/test_code_rules.py -q
```

真实ClickHouse测试需要Docker，使用独立容器、独立数据库，结束后清理。

## 基线已有失败和错误

下面条目在基线和本次全量运行中均失败；主要涉及既存清理SQL断言、恢复/checkpoint、打包脚本环境、恢复演练和备份断言。

- `failure` — `Services.Service.data-converge.test.test_configuration_application::test_existing_snapshot_warns_and_keeps_original_configuration[run_once-connection]`
- `failure` — `Services.Service.data-converge.test.test_configuration_application::test_existing_snapshot_warns_and_keeps_original_configuration[run_once-sql]`
- `failure` — `Services.Service.data-converge.test.test_configuration_application::test_legacy_binding_difference_does_not_block_execution`
- `failure` — `Services.Service.data-converge.test.test_configuration_application::test_legacy_ledger_does_not_automatically_adopt_a_snapshot`
- `failure` — `Services.Service.data-converge.test.test_configuration_migration::test_additive_migration_preserves_success_and_uses_explicit_cutover`
- `failure` — `Services.Service.data-converge.test.test_configuration_sql::test_approved_digest_uses_the_stripped_sql_sent_to_clickhouse`
- `failure` — `Services.Service.data-converge.test.test_configuration_sql::test_expected_hashes_are_keyed_by_task_name_not_shared_sql_resource`
- `failure` — `Services.Service.data-converge.test.test_configuration_sql::test_runner_copies_expected_hashes_and_rejects_missing_task`
- `failure` — `Services.Service.data-converge.test.test_configuration_sql::test_runner_executes_the_same_text_it_verified`
- `failure` — `Services.Service.data-converge.test.test_dag_sql_integration::test_dag_executor_runs_sql_tasks_in_dependency_order`
- `failure` — `Services.Service.data-converge.test.test_dag_sql_integration::test_unknown_sql_outcome_blocks_downstream_task`
- `failure` — `Services.Service.data-converge.test.test_dispatch_checkpoints::test_dispatched_and_legacy_claim_recovery_remain_unknown`
- `failure` — `Services.Service.data-converge.test.test_dispatch_checkpoints::test_failed_mixed_recovery_rolls_back_safe_requeue_and_prior_events`
- `failure` — `Services.Service.data-converge.test.test_dispatch_checkpoints::test_legacy_claim_after_prepared_recovery_cannot_reuse_its_proof`
- `failure` — `Services.Service.data-converge.test.test_dispatch_checkpoints::test_old_prepared_checkpoint_cannot_reset_a_different_current_attempt`
- `failure` — `Services.Service.data-converge.test.test_dispatch_checkpoints::test_prepared_recovery_requeues_without_changing_consumed_attempts`
- `failure` — `Services.Service.data-converge.test.test_execution_history::test_failed_recovery_history_write_preserves_both_running_states`
- `failure` — `Services.Service.data-converge.test.test_execution_history::test_legacy_running_attempt_is_not_backfilled_by_recovery`
- `failure` — `Services.Service.data-converge.test.test_execution_history::test_recovery_keeps_query_id_without_inventing_finish_time`
- `failure` — `Services.Service.data-converge.test.test_legacy_retirement::test_recovery_never_rewrites_retired_running_attempts_or_checkpoints`
- `failure` — `Services.Service.data-converge.test.test_legacy_startup_recovery::test_normal_start_retires_old_facts_executes_new_windows_and_restarts_once`
- `failure` — `Services.Service.data-converge.test.test_main::test_cli_http_window_runs_once_across_restart`
- `failure` — `Services.Service.data-converge.test.test_main::test_normal_start_recreates_missing_state_and_executes_new_window`
- `failure` — `Services.Service.data-converge.test.test_main::test_ready_cli_event_cron_and_manual_share_gate_and_success_ledger`
- `failure` — `Services.Service.data-converge.test.test_main::test_run_uses_one_configuration_snapshot`
- `failure` — `Services.Service.data-converge.test.test_packaging::test_platform_start_failure_prevents_application_and_start_status[INIT_PYTHON_EXIT_CODE-expected_hooks1]`
- `failure` — `Services.Service.data-converge.test.test_packaging::test_platform_start_failure_prevents_application_and_start_status[PRECHECK_EXIT_CODE-expected_hooks0]`
- `failure` — `Services.Service.data-converge.test.test_packaging::test_platform_start_restores_company_file_permissions`
- `failure` — `Services.Service.data-converge.test.test_packaging::test_probe_does_not_install_dependencies`
- `failure` — `Services.Service.data-converge.test.test_packaging::test_probe_uses_same_interpreter_and_returns_application_exit[liveness_check.sh-liveness]`
- `failure` — `Services.Service.data-converge.test.test_packaging::test_probe_uses_same_interpreter_and_returns_application_exit[readiness_check.sh-readiness]`
- `failure` — `Services.Service.data-converge.test.test_packaging::test_start_forwards_config_and_arguments_and_waits_for_child`
- `error` — `Services.Service.data-converge.test.test_recovery_drill::test_crash_classification_matches_persisted_boundaries[accepted_before_finish-1-UNKNOWN-1-1]`
- `error` — `Services.Service.data-converge.test.test_recovery_drill::test_crash_classification_matches_persisted_boundaries[dispatch_before_receiver-1-UNKNOWN-1-0]`
- `error` — `Services.Service.data-converge.test.test_recovery_drill::test_crash_classification_matches_persisted_boundaries[prepared_before_dispatch-1-PENDING-0-0]`
- `error` — `Services.Service.data-converge.test.test_recovery_drill::test_crash_classification_matches_persisted_boundaries[success_before_replay-0-SUCCESS-1-1]`
- `failure` — `Services.Service.data-converge.test.test_recovery_drill::test_drill_can_run_again_without_reusing_prior_state`
- `error` — `Services.Service.data-converge.test.test_recovery_drill::test_drill_compares_audited_events_with_fixed_expectations[accepted_before_finish-event_types2]`
- `error` — `Services.Service.data-converge.test.test_recovery_drill::test_drill_compares_audited_events_with_fixed_expectations[dispatch_before_receiver-event_types1]`
- `error` — `Services.Service.data-converge.test.test_recovery_drill::test_drill_compares_audited_events_with_fixed_expectations[prepared_before_dispatch-event_types0]`
- `error` — `Services.Service.data-converge.test.test_recovery_drill::test_drill_compares_audited_events_with_fixed_expectations[success_before_replay-event_types3]`
- `error` — `Services.Service.data-converge.test.test_recovery_drill::test_drill_evidence_is_ordered_and_excludes_production_claims`
- `error` — `Services.Service.data-converge.test.test_recovery_drill::test_safe_replay_uses_new_query_and_dispatches_only_once`
- `error` — `Services.Service.data-converge.test.test_recovery_drill::test_unknown_and_success_replay_preserve_external_acceptance`
- `failure` — `Services.Service.data-converge.test.test_sql_execution::test_runner_passes_persisted_query_id_to_clickhouse`
- `failure` — `Services.Service.data-converge.test.test_sql_execution::test_runner_resolves_password_once_and_preserves_window`
- `failure` — `Services.Service.data-converge.test.test_sql_preflight::test_execution_errors_stay_outside_preflight_classification[error0]`
- `failure` — `Services.Service.data-converge.test.test_sql_preflight::test_execution_errors_stay_outside_preflight_classification[error1]`
- `failure` — `Services.Service.data-converge.test.test_sql_preflight::test_execution_errors_stay_outside_preflight_classification[error2]`
- `failure` — `Services.Service.data-converge.test.test_state::test_abandoned_running_is_unknown_and_requires_explicit_resolution`
- `failure` — `Services.Service.data-converge.test.test_state_binding_logging::test_changed_binding_logs_evidence_and_preserves_running_work`
- `failure` — `Services.Service.data-converge.test.test_state_binding_logging::test_valid_binding_logs_success_and_restart_does_not_repeat_sql`
- `failure` — `Services.Service.data-converge.test.test_system_timezone::test_manual_run_accepts_later_window_in_repeated_hour`
