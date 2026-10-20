-- 6.3.3 由t_mops__modelmetric_server__day_ads和resource_config_local ->t_mops__resource_server__day_ads
INSERT INTO modelmate_metric.t_mops__resource_server__day_ads
(
    window_time,
    org_id_path,
    org_id,
    server_name,
    org_name,
    framework_type,
    framework_version,
    deploy_info,
    model_name,
    model_id,
    coord_request_received_total_delta,
    coord_request_received_total_qpm_avg,
    coord_request_failed_total_delta,
    coord_request_success_total_delta,
    coord_request_success_rate_avg,
    coord_request_failed_rate_avg,
    coord_num_requests_waiting_avg,
    coord_e2e_request_latency_seconds_avg,
    coord_e2e_request_latency_seconds_sum_delta,
    coord_e2e_request_latency_seconds_count_delta,
    coord_e2e_request_latency_seconds_bucket_delta,
    engine_request_received_total_delta,
    engine_request_failed_total_delta,
    engine_request_success_total_delta,
    engine_request_success_rate_avg,
    engine_request_failed_rate_avg,
    engine_inference_concurrent_requests_avg,
    engine_num_requests_running_avg,
    engine_num_requests_waiting_avg,
    engine_num_requests_swapped_avg,
    engine_num_preemptions_total_delta,
    engine_prompt_tokens_total_delta,
    engine_generation_tokens_total_delta,
    engine_total_throughput_rate,
    engine_prompt_tokens_total_delta_rate_avg,
    engine_generation_tokens_total_delta_rate_avg,
    engine_avg_prompt_throughput_toks_per_s_avg,
    engine_avg_prompt_throughput_toks_per_s_delta,
    engine_avg_prompt_throughput_toks_per_s_max,
    engine_avg_generation_throughput_toks_per_s_avg,
    engine_avg_generation_throughput_toks_per_s_delta,
    engine_avg_generation_throughput_toks_per_s_max,
    engine_time_to_first_token_seconds_avg,
    engine_time_to_first_token_seconds_sum_delta,
    engine_time_to_first_token_seconds_count_delta,
    engine_time_to_first_token_seconds_bucket_delta,
    engine_time_per_output_token_seconds_avg,
    engine_time_per_output_token_seconds_sum_delta,
    engine_time_per_output_token_seconds_count_delta,
    engine_time_per_output_token_seconds_bucket_delta,
    engine_e2e_request_latency_seconds_avg,
    engine_e2e_request_latency_seconds_sum_delta,
    engine_e2e_request_latency_seconds_count_delta,
    engine_e2e_request_latency_seconds_bucket_delta,
    engine_npu_cache_usage_perc_avg,
    engine_npu_cache_usage_perc_max,
    engine_cpu_cache_usage_perc_avg,
    engine_cpu_cache_usage_perc_max,
    engine_npu_prefix_cache_hit_rate_avg,
    engine_request_prompt_tokens_avg,
    engine_request_prompt_tokens_max,
    engine_request_prompt_tokens_sum_delta,
    engine_request_prompt_tokens_count_delta,
    engine_request_prompt_tokens_bucket_delta,
    engine_request_generation_tokens_avg,
    engine_request_generation_tokens_max,
    engine_request_generation_tokens_sum_delta,
    engine_request_generation_tokens_count_delta,
    engine_request_generation_tokens_bucket_delta,
    sync_flag
)
SELECT
    toDateTime({start_time:UInt32}, {system_timezone:String}) AS window_time,
    r.org_id_path,
    r.org_id,
    s.server_name,

    -- 属性信息
    any(r.org_name) AS org_name,
    any(s.framework_type) AS framework_type,
    any(s.framework_version) AS framework_version,
    any(s.deploy_info) AS deploy_info,
    any(s.model_name) AS model_name,
    any(s.model_id) AS model_id,

    -- 网关层指标
    sum(s.coord_request_received_total_delta) AS coord_request_received_total_delta,
    avg(s.coord_request_received_total_qpm_avg) AS coord_request_received_total_qpm_avg,
    sum(s.coord_request_failed_total_delta) AS coord_request_failed_total_delta,
    sum(s.coord_request_success_total_delta) AS coord_request_success_total_delta,
    if((sum(s.coord_request_success_total_delta) + sum(s.coord_request_failed_total_delta)) > 0,
        sum(s.coord_request_success_total_delta) / (sum(s.coord_request_success_total_delta) + sum(s.coord_request_failed_total_delta)), NULL) AS coord_request_success_rate_avg,
    if((sum(s.coord_request_success_total_delta) + sum(s.coord_request_failed_total_delta)) > 0,
        sum(s.coord_request_failed_total_delta) / (sum(s.coord_request_success_total_delta) + sum(s.coord_request_failed_total_delta)), NULL) AS coord_request_failed_rate_avg,
    avg(s.coord_num_requests_waiting_avg) AS coord_num_requests_waiting_avg,
    if(sum(s.coord_e2e_request_latency_seconds_count_delta) > 0,
        sum(s.coord_e2e_request_latency_seconds_sum_delta) / sum(s.coord_e2e_request_latency_seconds_count_delta), NULL) AS coord_e2e_request_latency_seconds_avg,
    sum(s.coord_e2e_request_latency_seconds_sum_delta) AS coord_e2e_request_latency_seconds_sum_delta,
    sum(s.coord_e2e_request_latency_seconds_count_delta) AS coord_e2e_request_latency_seconds_count_delta,
    mapFromArrays(
        sumMap(
            mapKeys(s.coord_e2e_request_latency_seconds_bucket_delta),
            mapValues(s.coord_e2e_request_latency_seconds_bucket_delta)
        ).1,
        sumMap(
            mapKeys(s.coord_e2e_request_latency_seconds_bucket_delta),
            mapValues(s.coord_e2e_request_latency_seconds_bucket_delta)
        ).2
    ) AS coord_e2e_request_latency_seconds_bucket_delta,
    -- 引擎层指标与夏令时兼容的吞吐速率计算
    sum(s.engine_request_received_total_delta) AS engine_request_received_total_delta,
    sum(s.engine_request_failed_total_delta) AS engine_request_failed_total_delta,
    sum(s.engine_request_success_total_delta) AS engine_request_success_total_delta,
    if((sum(s.engine_request_success_total_delta) + sum(s.engine_request_failed_total_delta)) > 0,
        sum(s.engine_request_success_total_delta) / (sum(s.engine_request_success_total_delta) + sum(s.engine_request_failed_total_delta)), NULL) AS engine_request_success_rate_avg,
    if((sum(s.engine_request_success_total_delta) + sum(s.engine_request_failed_total_delta)) > 0,
        sum(s.engine_request_failed_total_delta) / (sum(s.engine_request_success_total_delta) + sum(s.engine_request_failed_total_delta)), NULL) AS engine_request_failed_rate_avg,

    avg(s.engine_inference_concurrent_requests_avg) AS engine_inference_concurrent_requests_avg,
    avg(s.engine_num_requests_running_avg) AS engine_num_requests_running_avg,
    avg(s.engine_num_requests_waiting_avg) AS engine_num_requests_waiting_avg,
    avg(s.engine_num_requests_swapped_avg) AS engine_num_requests_swapped_avg,
    sum(s.engine_num_preemptions_total_delta) AS engine_num_preemptions_total_delta,

    sum(s.engine_prompt_tokens_total_delta) AS engine_prompt_tokens_total_delta,
    sum(s.engine_generation_tokens_total_delta) AS engine_generation_tokens_total_delta,

    -- 使用 dateDiff 处理夏令时天数长短差异
    cast((sum(s.engine_prompt_tokens_total_delta) + sum(s.engine_generation_tokens_total_delta)) / toFloat64(nullIf(dateDiff('second', toDateTime({start_time:UInt32}, 'UTC'), toDateTime({end_time:UInt32}, 'UTC')), 0)) AS Nullable(Float32)) AS engine_total_throughput_rate,
    cast(sum(s.engine_prompt_tokens_total_delta) / toFloat64(nullIf(dateDiff('second', toDateTime({start_time:UInt32}, 'UTC'), toDateTime({end_time:UInt32}, 'UTC')), 0)) AS Nullable(Float32)) AS engine_prompt_tokens_total_delta_rate_avg,
    cast(sum(s.engine_generation_tokens_total_delta) / toFloat64(nullIf(dateDiff('second', toDateTime({start_time:UInt32}, 'UTC'), toDateTime({end_time:UInt32}, 'UTC')), 0)) AS Nullable(Float32)) AS engine_generation_tokens_total_delta_rate_avg,
    avg(s.engine_avg_prompt_throughput_toks_per_s_avg) AS engine_avg_prompt_throughput_toks_per_s_avg,
    sum(s.engine_avg_prompt_throughput_toks_per_s_delta) AS engine_avg_prompt_throughput_toks_per_s_delta,
    max(s.engine_avg_prompt_throughput_toks_per_s_max) AS engine_avg_prompt_throughput_toks_per_s_max,
    avg(s.engine_avg_generation_throughput_toks_per_s_avg) AS engine_avg_generation_throughput_toks_per_s_avg,
    sum(s.engine_avg_generation_throughput_toks_per_s_delta) AS engine_avg_generation_throughput_toks_per_s_delta,
    max(s.engine_avg_generation_throughput_toks_per_s_max) AS engine_avg_generation_throughput_toks_per_s_max,

    -- 耗时与延时
    if(sum(s.engine_time_to_first_token_seconds_count_delta) > 0, sum(s.engine_time_to_first_token_seconds_sum_delta) / sum(s.engine_time_to_first_token_seconds_count_delta), NULL) AS engine_time_to_first_token_seconds_avg,
    sum(s.engine_time_to_first_token_seconds_sum_delta) AS engine_time_to_first_token_seconds_sum_delta,
    sum(s.engine_time_to_first_token_seconds_count_delta) AS engine_time_to_first_token_seconds_count_delta,
    mapFromArrays(
        sumMap(
            mapKeys(s.engine_time_to_first_token_seconds_bucket_delta),
            mapValues(s.engine_time_to_first_token_seconds_bucket_delta)
        ).1,
        sumMap(
            mapKeys(s.engine_time_to_first_token_seconds_bucket_delta),
            mapValues(s.engine_time_to_first_token_seconds_bucket_delta)
        ).2
    ) AS engine_time_to_first_token_seconds_bucket_delta,

    if(sum(s.engine_time_per_output_token_seconds_count_delta) > 0, sum(s.engine_time_per_output_token_seconds_sum_delta) / sum(s.engine_time_per_output_token_seconds_count_delta), NULL) AS engine_time_per_output_token_seconds_avg,
    sum(s.engine_time_per_output_token_seconds_sum_delta) AS engine_time_per_output_token_seconds_sum_delta,
    sum(s.engine_time_per_output_token_seconds_count_delta) AS engine_time_per_output_token_seconds_count_delta,
    mapFromArrays(
        sumMap(
            mapKeys(s.engine_time_per_output_token_seconds_bucket_delta),
            mapValues(s.engine_time_per_output_token_seconds_bucket_delta)
        ).1,
        sumMap(
            mapKeys(s.engine_time_per_output_token_seconds_bucket_delta),
            mapValues(s.engine_time_per_output_token_seconds_bucket_delta)
        ).2
    ) AS engine_time_per_output_token_seconds_bucket_delta,

    if(sum(s.engine_e2e_request_latency_seconds_count_delta) > 0, sum(s.engine_e2e_request_latency_seconds_sum_delta) / sum(s.engine_e2e_request_latency_seconds_count_delta), NULL) AS engine_e2e_request_latency_seconds_avg,
    sum(s.engine_e2e_request_latency_seconds_sum_delta) AS engine_e2e_request_latency_seconds_sum_delta,
    sum(s.engine_e2e_request_latency_seconds_count_delta) AS engine_e2e_request_latency_seconds_count_delta,
    mapFromArrays(
        sumMap(
            mapKeys(s.engine_e2e_request_latency_seconds_bucket_delta),
            mapValues(s.engine_e2e_request_latency_seconds_bucket_delta)
        ).1,
        sumMap(
            mapKeys(s.engine_e2e_request_latency_seconds_bucket_delta),
            mapValues(s.engine_e2e_request_latency_seconds_bucket_delta)
        ).2
    ) AS engine_e2e_request_latency_seconds_bucket_delta,

    -- 显存与 Cache
    avg(s.engine_npu_cache_usage_perc_avg) AS engine_npu_cache_usage_perc_avg,
    max(s.engine_npu_cache_usage_perc_max) AS engine_npu_cache_usage_perc_max,
    avg(s.engine_cpu_cache_usage_perc_avg) AS engine_cpu_cache_usage_perc_avg,
    max(s.engine_cpu_cache_usage_perc_max) AS engine_cpu_cache_usage_perc_max,
    avg(s.engine_npu_prefix_cache_hit_rate_avg) AS engine_npu_prefix_cache_hit_rate_avg,

    -- Token 尺寸分布
    avg(s.engine_request_prompt_tokens_avg) AS engine_request_prompt_tokens_avg,
    max(s.engine_request_prompt_tokens_max) AS engine_request_prompt_tokens_max,
    sum(s.engine_request_prompt_tokens_sum_delta) AS engine_request_prompt_tokens_sum_delta,
    sum(s.engine_request_prompt_tokens_count_delta) AS engine_request_prompt_tokens_count_delta,
    mapFromArrays(
        sumMap(
            mapKeys(s.engine_request_prompt_tokens_bucket_delta),
            mapValues(s.engine_request_prompt_tokens_bucket_delta)
        ).1,
        sumMap(
            mapKeys(s.engine_request_prompt_tokens_bucket_delta),
            mapValues(s.engine_request_prompt_tokens_bucket_delta)
        ).2
    ) AS engine_request_prompt_tokens_bucket_delta,

    avg(s.engine_request_generation_tokens_avg) AS engine_request_generation_tokens_avg,
    max(s.engine_request_generation_tokens_max) AS engine_request_generation_tokens_max,
    sum(s.engine_request_generation_tokens_sum_delta) AS engine_request_generation_tokens_sum_delta,
    sum(s.engine_request_generation_tokens_count_delta) AS engine_request_generation_tokens_count_delta,
    mapFromArrays(
        sumMap(
            mapKeys(s.engine_request_generation_tokens_bucket_delta),
            mapValues(s.engine_request_generation_tokens_bucket_delta)
        ).1,
        sumMap(
            mapKeys(s.engine_request_generation_tokens_bucket_delta),
            mapValues(s.engine_request_generation_tokens_bucket_delta)
        ).2
    ) AS engine_request_generation_tokens_bucket_delta,

    1 AS sync_flag

FROM
    (
        SELECT
            *,
            arrayJoin(arrayDistinct(node_ips)) AS join_node_ip
        FROM modelmate_metric.t_mops__modelmetric_server__day_ads AS source
        WHERE source.window_time >= toDateTime({start_time:UInt32}, {system_timezone:String})
          AND source.window_time <  toDateTime({end_time:UInt32}, {system_timezone:String})
    ) AS s
    INNER JOIN
    (
        SELECT *
        FROM modelmate_metric.t_mops_org_resource_flat_local
        WHERE is_deleted = 0
    ) AS r
        ON s.join_node_ip = r.node_ip
GROUP BY
    toDateTime({start_time:UInt32}, {system_timezone:String}), -- 修正 GROUP BY 别名问题
    r.org_id_path,
    r.org_id,
    s.server_name;
