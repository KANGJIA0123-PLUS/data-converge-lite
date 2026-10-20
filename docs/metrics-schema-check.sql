-- Read-only deployment check for the 2026-10-20 metrics changes.
-- Execute on the same ClickHouse endpoint/database used by data-converge.
-- No rows means all required column names exist; types/engines still need review.
WITH expected AS (
    SELECT table_name, arrayJoin([
                'allocated_npu_num'
           ]) AS column_name
    FROM (SELECT arrayJoin(['t_mops_modelinstance_5min_ads', 't_mops_modelinstance_hour_ads']) AS table_name)
    UNION ALL
    SELECT table_name, arrayJoin([
                'allocated_npu_num',
                'work_time_ai_core_util_avg',
                'work_time_ai_core_util_max',
                'work_time_ai_core_util_min',
                'idle_time_ai_core_util_avg',
                'idle_time_ai_core_util_max',
                'idle_time_ai_core_util_min',
                'work_time_ai_core_util_status'
           ]) AS column_name
    FROM (SELECT arrayJoin(['t_mops_modelinstance_day_ads', 't_mops_modelinstance_month_ads']) AS table_name)
    UNION ALL
    SELECT table_name, arrayJoin([
                'input_output_token_rate',
                'request_received_total_qps',
                'unit_token_computepower_consumption'
           ]) AS column_name
    FROM (SELECT arrayJoin(['t_mops__modelmetric_instance__5min_ads', 't_mops__modelmetric_instance__hour_ads', 't_mops__modelmetric_instance__day_ads', 't_mops__modelmetric_instance__month_ads']) AS table_name)
    UNION ALL
    SELECT table_name, arrayJoin([
                'request_success_total_delta',
                'e2e_request_latency_seconds_sum_delta',
                'e2e_request_latency_seconds_count_delta',
                'time_to_first_token_seconds_sum_delta',
                'time_to_first_token_seconds_count_delta',
                'time_per_output_token_seconds_sum_delta',
                'time_per_output_token_seconds_count_delta',
                'generation_tokens_per_s_avg',
                'generation_tokens_per_request_avg',
                'e2e_latency_avg',
                'prompt_tokens_per_s_avg',
                'request_failed_rate',
                'request_success_rate',
                'request_received_total_qpm_avg',
                'time_to_first_token_seconds_avg',
                'time_per_output_token_seconds_avg',
                'total_tokens_per_s_avg'
           ]) AS column_name
    FROM (SELECT arrayJoin(['t_mops__modelmetric_node__5min_ads', 't_mops__modelmetric_node__hour_ads', 't_mops__modelmetric_node__day_ads', 't_mops__modelmetric_node__month_ads']) AS table_name)
    UNION ALL
    SELECT table_name, arrayJoin([
                'window_time',
                'server_name',
                'modelinstance_name',
                'framework_type',
                'framework_version',
                'deploy_info',
                'model_name',
                'model_id',
                'container_cpu_util_avg',
                'container_cpu_util_max',
                'container_cpu_util_min',
                'container_cpu_util_argMax',
                'container_mem_util_avg',
                'container_mem_util_max',
                'container_mem_util_min',
                'container_mem_util_argMax',
                'container_mem_usage_mb_avg',
                'container_mem_usage_mb_max',
                'container_mem_usage_mb_min',
                'container_mem_usage_mb_argMax',
                'container_disk_usage_mb_avg',
                'container_disk_usage_mb_max',
                'container_disk_usage_mb_min',
                'container_disk_usage_mb_argMax',
                'container_npu_util_avg',
                'container_npu_util_max',
                'container_npu_util_min',
                'container_npu_util_argMax',
                'container_hbm_util_avg',
                'container_hbm_util_max',
                'container_hbm_util_min',
                'container_hbm_util_argMax',
                'container_recv_bytes_rate_avg',
                'container_recv_bytes_rate_max',
                'container_recv_bytes_rate_min',
                'container_recv_bytes_rate_argMax',
                'container_send_bytes_rate_avg',
                'container_send_bytes_rate_max',
                'container_send_bytes_rate_min',
                'container_send_bytes_rate_argMax',
                'container_tx_pack_drop_rate_avg',
                'container_tx_pack_drop_rate_max',
                'container_tx_pack_drop_rate_min',
                'container_tx_pack_drop_rate_argMax',
                'container_rx_pack_drop_rate_avg',
                'container_rx_pack_drop_rate_max',
                'container_rx_pack_drop_rate_min',
                'container_rx_pack_drop_rate_argMax',
                'allocated_npu_num',
                'hbm_util_avg',
                'hbm_util_max',
                'hbm_util_min',
                'ai_core_util_avg',
                'ai_core_util_max',
                'ai_core_util_min',
                'ai_core_util_argMax',
                'hbm_bandwidth_util_avg',
                'hbm_bandwidth_util_max',
                'hbm_bandwidth_util_min',
                'ai_card_temp_avg',
                'ai_card_temp_max',
                'ai_card_temp_min',
                'ai_card_power_avg',
                'ai_card_power_max',
                'ai_card_power_min',
                'bandwidth_tx_avg',
                'bandwidth_tx_max',
                'bandwidth_tx_min',
                'bandwidth_rx_avg',
                'bandwidth_rx_max',
                'bandwidth_rx_min',
                'ai_card_health_argMax',
                'ai_card_normal_state_counter',
                'ai_card_abnormal_state_counter',
                'ai_card_health_ratio_avg',
                'ai_card_offline_ratio_avg',
                'ai_card_used_argMax',
                'ai_card_used_counter',
                'ai_card_unused_counter',
                'ai_card_used_active_counter',
                'ai_card_used_ratio_avg',
                'ai_card_computing_power_argMax',
                'ai_card_used_computing_power_counter',
                'ai_card_unused_computing_power_counter',
                'sync_flag'
           ]) AS column_name
    FROM (SELECT arrayJoin(['t_mops__modelinstance_server__5min_ads', 't_mops__modelinstance_server__hour_ads', 't_mops__modelinstance_server__day_ads', 't_mops__modelinstance_server__month_ads']) AS table_name)
    UNION ALL
    SELECT table_name, arrayJoin([
                'work_time_ai_core_util_avg',
                'work_time_ai_core_util_max',
                'work_time_ai_core_util_min',
                'idle_time_ai_core_util_avg',
                'idle_time_ai_core_util_max',
                'idle_time_ai_core_util_min',
                'work_time_ai_core_util_status'
           ]) AS column_name
    FROM (SELECT arrayJoin(['t_mops__modelinstance_server__day_ads', 't_mops__modelinstance_server__month_ads']) AS table_name)
)
SELECT expected.table_name, expected.column_name AS missing_column
FROM expected
LEFT ANTI JOIN system.columns AS actual
    ON actual.database = 'modelmate_metric'
   AND actual.table = expected.table_name
   AND actual.name = expected.column_name
ORDER BY expected.table_name, expected.column_name;
