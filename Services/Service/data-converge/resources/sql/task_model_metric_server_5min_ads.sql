-- 6.2.1 -- 由 t_mops__modelmetric_instance__5min_ads -> t_mops__modelmetric_server__5min_ads
INSERT INTO modelmate_metric.t_mops__modelmetric_server__5min_ads
(
    window_time,
    server_name,
    framework_type,
    framework_version,
    deploy_info,
    model_name,
    model_id,
    node_ids,
    node_ips,
    node_name,
    node_type,
    dev_mfr,
    dev_model,
    dev_status_code,
    dev_status_name,
    coord_request_received_total_argMax,
    coord_request_received_total_delta,
    coord_request_received_total_qpm_avg,
    coord_request_failed_total_argMax,
    coord_request_failed_total_delta,
    coord_request_success_total_argMax,
    coord_request_success_total_delta,
    coord_request_success_rate_avg,
    coord_request_failed_rate_avg,
    coord_num_requests_waiting_avg,
    coord_e2e_request_latency_seconds_avg,
    coord_e2e_request_latency_seconds_count_argMax,
    coord_e2e_request_latency_seconds_count_delta,
    coord_e2e_request_latency_seconds_sum_argMax,
    coord_e2e_request_latency_seconds_sum_delta,
    coord_e2e_request_latency_seconds_bucket_delta,
    engine_request_received_total_argMax,
    engine_request_received_total_delta,
    engine_request_failed_total_argMax,
    engine_request_failed_total_delta,
    engine_request_success_total_argMax,
    engine_request_success_total_delta,
    engine_request_success_rate_avg,
    engine_request_failed_rate_avg,
    engine_inference_concurrent_requests_avg,
    engine_num_requests_running_avg,
    engine_num_requests_waiting_avg,
    engine_num_requests_swapped_avg,
    engine_num_preemptions_total_argMax,
    engine_num_preemptions_total_delta,
    engine_prompt_tokens_total_argMax,
    engine_prompt_tokens_total_delta,
    engine_generation_tokens_total_argMax,
    engine_generation_tokens_total_delta,
    engine_prompt_tokens_total_delta_rate_avg,
    engine_generation_tokens_total_delta_rate_avg,
    engine_total_throughput_rate,
    engine_avg_prompt_throughput_toks_per_s_avg,
    engine_avg_prompt_throughput_toks_per_s_delta,
    engine_avg_prompt_throughput_toks_per_s_max,
    engine_avg_generation_throughput_toks_per_s_avg,
    engine_avg_generation_throughput_toks_per_s_delta,
    engine_avg_generation_throughput_toks_per_s_max,
    engine_time_to_first_token_seconds_avg,
    engine_time_to_first_token_seconds_count_argMax,
    engine_time_to_first_token_seconds_count_delta,
    engine_time_to_first_token_seconds_sum_argMax,
    engine_time_to_first_token_seconds_sum_delta,
    engine_time_to_first_token_seconds_bucket_delta,
    engine_time_per_output_token_seconds_avg,
    engine_time_per_output_token_seconds_count_argMax,
    engine_time_per_output_token_seconds_count_delta,
    engine_time_per_output_token_seconds_sum_argMax,
    engine_time_per_output_token_seconds_sum_delta,
    engine_time_per_output_token_seconds_bucket_delta,
    engine_e2e_request_latency_seconds_avg,
    engine_e2e_request_latency_seconds_count_argMax,
    engine_e2e_request_latency_seconds_count_delta,
    engine_e2e_request_latency_seconds_sum_argMax,
    engine_e2e_request_latency_seconds_sum_delta,
    engine_e2e_request_latency_seconds_bucket_delta,
    engine_npu_cache_usage_perc_avg,
    engine_npu_cache_usage_perc_max,
    engine_cpu_cache_usage_perc_avg,
    engine_cpu_cache_usage_perc_max,
    engine_npu_prefix_cache_hit_rate_avg,
    engine_request_prompt_tokens_avg,
    engine_request_prompt_tokens_max,
    engine_request_prompt_tokens_count_argMax,
    engine_request_prompt_tokens_count_delta,
    engine_request_prompt_tokens_sum_argMax,
    engine_request_prompt_tokens_sum_delta,
    engine_request_prompt_tokens_bucket_delta,
    engine_request_generation_tokens_avg,
    engine_request_generation_tokens_max,
    engine_request_generation_tokens_count_argMax,
    engine_request_generation_tokens_count_delta,
    engine_request_generation_tokens_sum_argMax,
    engine_request_generation_tokens_sum_delta,
    engine_request_generation_tokens_bucket_delta,
     -- 新增补充指标
    input_output_token_rate,
    request_received_total_qps,
    unit_token_computepower_consumption,
    sync_flag
)
SELECT
    t.window_time,
    t.server_name,
    t.framework_type,
    t.framework_version,
    t.deploy_info,
    t.model_name,
    t.model_id,
    t.node_ids,
    t.node_ips,
    t.node_name,
    t.node_type,
    t.dev_mfr,
    t.dev_model,
    t.dev_status_code,
    t.dev_status_name,
    t.coord_request_received_total_argMax,
    t.coord_request_received_total_delta,
    t.coord_request_received_total_qpm_avg,
    t.coord_request_failed_total_argMax,
    t.coord_request_failed_total_delta,
    t.coord_request_success_total_argMax,
    t.coord_request_success_total_delta,
    t.coord_request_success_rate_avg,
    t.coord_request_failed_rate_avg,
    t.coord_num_requests_waiting_avg,
    t.coord_e2e_request_latency_seconds_avg,
    t.coord_e2e_request_latency_seconds_count_argMax,
    t.coord_e2e_request_latency_seconds_count_delta,
    t.coord_e2e_request_latency_seconds_sum_argMax,
    t.coord_e2e_request_latency_seconds_sum_delta,
    t.coord_e2e_request_latency_seconds_bucket_delta,
    t.engine_request_received_total_argMax,
    t.engine_request_received_total_delta,
    t.engine_request_failed_total_argMax,
    t.engine_request_failed_total_delta,
    t.engine_request_success_total_argMax,
    t.engine_request_success_total_delta,
    t.engine_request_success_rate_avg,
    t.engine_request_failed_rate_avg,
    t.engine_inference_concurrent_requests_avg,
    t.engine_num_requests_running_avg,
    t.engine_num_requests_waiting_avg,
    t.engine_num_requests_swapped_avg,
    t.engine_num_preemptions_total_argMax,
    t.engine_num_preemptions_total_delta,
    t.engine_prompt_tokens_total_argMax,
    t.engine_prompt_tokens_total_delta,
    t.engine_generation_tokens_total_argMax,
    t.engine_generation_tokens_total_delta,
    t.engine_prompt_tokens_total_delta_rate_avg,
    t.engine_generation_tokens_total_delta_rate_avg,
    t.engine_total_throughput_rate,
    t.engine_avg_prompt_throughput_toks_per_s_avg,
    t.engine_avg_prompt_throughput_toks_per_s_delta,
    t.engine_avg_prompt_throughput_toks_per_s_max,
    t.engine_avg_generation_throughput_toks_per_s_avg,
    t.engine_avg_generation_throughput_toks_per_s_delta,
    t.engine_avg_generation_throughput_toks_per_s_max,
    t.engine_time_to_first_token_seconds_avg,
    t.engine_time_to_first_token_seconds_count_argMax,
    t.engine_time_to_first_token_seconds_count_delta,
    t.engine_time_to_first_token_seconds_sum_argMax,
    t.engine_time_to_first_token_seconds_sum_delta,
    t.engine_time_to_first_token_seconds_bucket_delta,
    t.engine_time_per_output_token_seconds_avg,
    t.engine_time_per_output_token_seconds_count_argMax,
    t.engine_time_per_output_token_seconds_count_delta,
    t.engine_time_per_output_token_seconds_sum_argMax,
    t.engine_time_per_output_token_seconds_sum_delta,
    t.engine_time_per_output_token_seconds_bucket_delta,
    t.engine_e2e_request_latency_seconds_avg,
    t.engine_e2e_request_latency_seconds_count_argMax,
    t.engine_e2e_request_latency_seconds_count_delta,
    t.engine_e2e_request_latency_seconds_sum_argMax,
    t.engine_e2e_request_latency_seconds_sum_delta,
    t.engine_e2e_request_latency_seconds_bucket_delta,
    t.engine_npu_cache_usage_perc_avg,
    t.engine_npu_cache_usage_perc_max,
    t.engine_cpu_cache_usage_perc_avg,
    t.engine_cpu_cache_usage_perc_max,
    t.engine_npu_prefix_cache_hit_rate_avg,
    t.engine_request_prompt_tokens_avg,
    t.engine_request_prompt_tokens_max,
    t.engine_request_prompt_tokens_count_argMax,
    t.engine_request_prompt_tokens_count_delta,
    t.engine_request_prompt_tokens_sum_argMax,
    t.engine_request_prompt_tokens_sum_delta,
    t.engine_request_prompt_tokens_bucket_delta,
    t.engine_request_generation_tokens_avg,
    t.engine_request_generation_tokens_max,
    t.engine_request_generation_tokens_count_argMax,
    t.engine_request_generation_tokens_count_delta,
    t.engine_request_generation_tokens_sum_argMax,
    t.engine_request_generation_tokens_sum_delta,
    t.engine_request_generation_tokens_bucket_delta,

    -- ============================================================================
    -- 新增补充指标计算 (服务级别)
    -- ============================================================================
    -- (1) 输入/输出比率
    CAST(coalesce(
        t.engine_prompt_tokens_total_delta / nullIf(t.engine_generation_tokens_total_delta, 0.0),
        0.0
    ) AS Nullable(Float32)) AS input_output_token_rate,

    -- (2) 总请求 QPS
    CAST(coalesce(
        t.engine_request_received_total_delta / nullIf(toFloat64(dateDiff('second', {start_time:DateTime}, {end_time:DateTime})), 0.0),
        0.0
    ) AS Nullable(Float32)) AS request_received_total_qps,

    -- (3) 产生百万 token 所用的卡时: ((prompt_delta + gen_delta) * 3600 * 服务总NPU卡数) / (1000000 * 区间秒数)
    CAST(coalesce(
        ((coalesce(t.engine_prompt_tokens_total_delta, 0) + coalesce(t.engine_generation_tokens_total_delta, 0)) * 3600.0 * coalesce(s.total_allocated_npu, 0))
        / nullIf(1000000.0 * toFloat64(dateDiff('second', {start_time:DateTime}, {end_time:DateTime})), 0.0),
        0.0
    ) AS Nullable(Float32)) AS unit_token_computepower_consumption,

    0 AS sync_flag
FROM
(
    SELECT
    -- ============================================================================
    -- 1. 时间与 ADS 核心维度 (抹除 process_role、group_id、container_id、modelinstance_id)
    -- ============================================================================
    ins.window_time,
    ins.server_name,

    -- ============================================================================
    -- 2. 服务关联属性 (锁住窗口内最新快照)
    -- ============================================================================
    any(ins.framework_type)     AS framework_type,
    any(ins.framework_version)  AS framework_version,
    any(ins.deploy_info)        AS deploy_info,
    any(ins.model_name)         AS model_name,
    any(ins.model_id)           AS model_id,
    -- 将子查询中每个实例节点数组合并为唯一的全实例节点数组
    arrayDistinct(arrayFlatten(groupArray(ins.node_ids))) AS node_ids,
    arrayDistinct(arrayFlatten(groupArray(ins.node_ips))) AS node_ips,
    any(ins.node_name)          AS node_name, -- 注意这里不能直接使用node_name查东西了，要使用id和ip
    any(ins.node_type)          AS node_type, -- 注意这里不能直接使用node_type查东西了，要使用id和ip
    any(ins.dev_mfr)            AS dev_mfr,
    any(ins.dev_model)          AS dev_model,
    any(ins.dev_status_code)    AS dev_status_code,
    any(ins.dev_status_name)    AS dev_status_name,

    -- ============================================================================
    -- 3. 服务粒度 Coordinator (网关/协调节点) 视角指标 (包含 coordinator 与 single)
    -- ============================================================================
    sum(ins.coord_request_received_total_argMax)                            AS coord_request_received_total_argMax,
    sum(ins.coord_request_received_total_delta)                             AS coord_request_received_total_delta,

    -- 【优化】：动态计算 QPM，通过具体时间范围总分钟数作为分母
    CAST(coalesce(
        sum(ins.coord_request_received_total_delta)
        / nullIf(dateDiff('second', toDateTime({start_time:UInt32}, 'UTC'), toDateTime({end_time:UInt32}, 'UTC')) / 60.0, 0.0),
        0.0
    ) AS Float32) AS coord_request_received_total_qpm_avg,

    sum(ins.coord_request_failed_total_argMax)                              AS coord_request_failed_total_argMax,
    sum(ins.coord_request_failed_total_delta)                               AS coord_request_failed_total_delta,

    sum(ins.coord_request_success_total_argMax)                             AS coord_request_success_total_argMax,
    sum(ins.coord_request_success_total_delta)                              AS coord_request_success_total_delta,

    CAST(coalesce(
        sum(ins.coord_request_success_total_delta) * 100.0
        / nullIf(sum(ins.coord_request_received_total_delta), 0),
        0.0
    ) AS Float32) AS coord_request_success_rate_avg,
    CAST(coalesce(
        sum(ins.coord_request_failed_total_delta) * 100.0
        / nullIf(sum(ins.coord_request_received_total_delta), 0),
        0.0
    ) AS Float32) AS coord_request_failed_rate_avg,

    avg(ins.coord_num_requests_waiting_avg)                           AS coord_num_requests_waiting_avg,

    -- Coordinator E2E Latency
    CAST(coalesce(
        sum(ins.coord_e2e_request_latency_seconds_sum_delta)
        / nullIf(sum(ins.coord_e2e_request_latency_seconds_count_delta), 0),
        0.0
    ) AS Float32) AS coord_e2e_request_latency_seconds_avg,
    sum(ins.coord_e2e_request_latency_seconds_count_argMax)                                                           AS coord_e2e_request_latency_seconds_count_argMax,
    sum(ins.coord_e2e_request_latency_seconds_count_delta)                                                            AS coord_e2e_request_latency_seconds_count_delta,
    sum(ins.coord_e2e_request_latency_seconds_sum_argMax)                                                             AS coord_e2e_request_latency_seconds_sum_argMax,
    sum(ins.coord_e2e_request_latency_seconds_sum_delta)                                                            AS coord_e2e_request_latency_seconds_sum_delta,
    mapFromArrays(
        sumMap(
            mapKeys(ins.coord_e2e_request_latency_seconds_bucket_delta),
            mapValues(ins.coord_e2e_request_latency_seconds_bucket_delta)
        ).1,
        sumMap(
            mapKeys(ins.coord_e2e_request_latency_seconds_bucket_delta),
            mapValues(ins.coord_e2e_request_latency_seconds_bucket_delta)
        ).2
    ) AS coord_e2e_request_latency_seconds_bucket_delta,

    -- ============================================================================
    -- 4. Engine (物理引擎节点) 视角 - Request & Concurrency Metrics
    -- ============================================================================
    sum(ins.engine_request_received_total_argMax)                           AS engine_request_received_total_argMax,
    sum(ins.engine_request_received_total_delta)                            AS engine_request_received_total_delta,

    sum(ins.engine_request_failed_total_argMax)                             AS engine_request_failed_total_argMax,
    sum(ins.engine_request_failed_total_delta)                              AS engine_request_failed_total_delta,

    sum(ins.engine_request_success_total_argMax)                            AS engine_request_success_total_argMax,
    sum(ins.engine_request_success_total_delta)                             AS engine_request_success_total_delta,

    CAST(coalesce(
        sum(ins.engine_request_success_total_delta) * 100.0
        / nullIf(sum(ins.engine_request_received_total_delta), 0),
        0.0
    ) AS Float32) AS engine_request_success_rate_avg,
    CAST(coalesce(
        sum(ins.engine_request_failed_total_delta) * 100.0
        / nullIf(sum(ins.engine_request_received_total_delta), 0),
        0.0
    ) AS Float32) AS engine_request_failed_rate_avg,

    sum(ins.engine_inference_concurrent_requests_avg)  AS engine_inference_concurrent_requests_avg,
    sum(ins.engine_num_requests_running_avg) AS engine_num_requests_running_avg,
    sum(ins.engine_num_requests_waiting_avg) AS engine_num_requests_waiting_avg,
    sum(ins.engine_num_requests_swapped_avg) AS engine_num_requests_swapped_avg,

    sum(ins.engine_num_preemptions_total_argMax) AS engine_num_preemptions_total_argMax,
    sum(ins.engine_num_preemptions_total_delta)  AS engine_num_preemptions_total_delta,

    -- ============================================================================
    -- 5. Engine (物理引擎节点) 视角 - Token Totals & Throughput Rates
    -- ============================================================================
    sum(ins.engine_prompt_tokens_total_argMax)  AS engine_prompt_tokens_total_argMax,
    sum(ins.engine_prompt_tokens_total_delta)   AS engine_prompt_tokens_total_delta,

    sum(ins.engine_generation_tokens_total_argMax)  AS engine_generation_tokens_total_argMax,
    sum(ins.engine_generation_tokens_total_delta)   AS engine_generation_tokens_total_delta,

    -- 【优化】：动态计算吞吐率分母（基于具体时间范围总秒数）
    CAST(coalesce(
        sum(ins.engine_prompt_tokens_total_delta)
        / nullIf(toFloat64(dateDiff('second', toDateTime({start_time:UInt32}, 'UTC'), toDateTime({end_time:UInt32}, 'UTC'))), 0.0),
        0.0
    ) AS Float32) AS engine_prompt_tokens_total_delta_rate_avg,
    CAST(coalesce(
        sum(ins.engine_generation_tokens_total_delta)
        / nullIf(toFloat64(dateDiff('second', toDateTime({start_time:UInt32}, 'UTC'), toDateTime({end_time:UInt32}, 'UTC'))), 0.0),
        0.0
    ) AS Float32) AS engine_generation_tokens_total_delta_rate_avg,
    CAST(coalesce(
        (sum(ins.engine_prompt_tokens_total_delta) + sum(ins.engine_generation_tokens_total_delta))
        / nullIf(toFloat64(dateDiff('second', toDateTime({start_time:UInt32}, 'UTC'), toDateTime({end_time:UInt32}, 'UTC'))), 0.0),
        0.0
    ) AS Float32) AS engine_total_throughput_rate,

    avg(ins.engine_avg_prompt_throughput_toks_per_s_avg)   AS engine_avg_prompt_throughput_toks_per_s_avg,
    sum(ins.engine_avg_prompt_throughput_toks_per_s_delta) AS engine_avg_prompt_throughput_toks_per_s_delta,
    max(ins.engine_avg_prompt_throughput_toks_per_s_max)   AS engine_avg_prompt_throughput_toks_per_s_max,

    avg(ins.engine_avg_generation_throughput_toks_per_s_avg) AS engine_avg_generation_throughput_toks_per_s_avg,
    sum(ins.engine_avg_generation_throughput_toks_per_s_delta) AS engine_avg_generation_throughput_toks_per_s_delta,
    max(ins.engine_avg_generation_throughput_toks_per_s_max) AS engine_avg_generation_throughput_toks_per_s_max,

    -- ============================================================================
    -- 6. Engine (物理引擎节点) 视角 - Key Latencies (TTFT / TPOT / Engine E2E)
    -- ============================================================================
    -- TTFT (归属于 prefill, engine, single)
    CAST(coalesce(
        sum(ins.engine_time_to_first_token_seconds_sum_delta)
        / nullIf(sum(ins.engine_time_to_first_token_seconds_count_delta), 0),
        0.0
    ) AS Float32) AS engine_time_to_first_token_seconds_avg,
    sum(ins.engine_time_to_first_token_seconds_count_argMax) AS engine_time_to_first_token_seconds_count_argMax,
    sum(ins.engine_time_to_first_token_seconds_count_delta) AS engine_time_to_first_token_seconds_count_delta,
    sum(ins.engine_time_to_first_token_seconds_sum_argMax) AS engine_time_to_first_token_seconds_sum_argMax,
    sum(ins.engine_time_to_first_token_seconds_sum_delta) AS engine_time_to_first_token_seconds_sum_delta,
    mapFromArrays(
        sumMap(
            mapKeys(ins.engine_time_to_first_token_seconds_bucket_delta),
            mapValues(ins.engine_time_to_first_token_seconds_bucket_delta)
        ).1,
        sumMap(
            mapKeys(ins.engine_time_to_first_token_seconds_bucket_delta),
            mapValues(ins.engine_time_to_first_token_seconds_bucket_delta)
        ).2
    ) AS engine_time_to_first_token_seconds_bucket_delta,

    -- TPOT (归属于 decode, engine, single)
    CAST(coalesce(
        sum(ins.engine_time_per_output_token_seconds_sum_delta)
        / nullIf(sum(ins.engine_time_per_output_token_seconds_count_delta), 0),
        0.0
    ) AS Float32) AS engine_time_per_output_token_seconds_avg,
    sum(ins.engine_time_per_output_token_seconds_count_argMax) AS engine_time_per_output_token_seconds_count_argMax,
    sum(ins.engine_time_per_output_token_seconds_count_delta) AS engine_time_per_output_token_seconds_count_delta,
    sum(ins.engine_time_per_output_token_seconds_sum_argMax) AS engine_time_per_output_token_seconds_sum_argMax,
    sum(ins.engine_time_per_output_token_seconds_sum_delta) AS engine_time_per_output_token_seconds_sum_delta,
    mapFromArrays(
        sumMap(
            mapKeys(ins.engine_time_per_output_token_seconds_bucket_delta),
            mapValues(ins.engine_time_per_output_token_seconds_bucket_delta)
        ).1,
        sumMap(
            mapKeys(ins.engine_time_per_output_token_seconds_bucket_delta),
            mapValues(ins.engine_time_per_output_token_seconds_bucket_delta)
        ).2
    ) AS engine_time_per_output_token_seconds_bucket_delta,

    -- Engine E2E (纯物理引擎端到端: engine, prefill, decode, single)
    CAST(coalesce(
        sum(ins.engine_e2e_request_latency_seconds_sum_delta)
        / nullIf(sum(ins.engine_e2e_request_latency_seconds_count_delta), 0),
        0.0
    ) AS Float32) AS engine_e2e_request_latency_seconds_avg,
    sum(ins.engine_e2e_request_latency_seconds_count_argMax) AS engine_e2e_request_latency_seconds_count_argMax,
    sum(ins.engine_e2e_request_latency_seconds_count_delta) AS engine_e2e_request_latency_seconds_count_delta,
    sum(ins.engine_e2e_request_latency_seconds_sum_argMax) AS engine_e2e_request_latency_seconds_sum_argMax,
    sum(ins.engine_e2e_request_latency_seconds_sum_delta) AS engine_e2e_request_latency_seconds_sum_delta,
    mapFromArrays(
        sumMap(
            mapKeys(ins.engine_e2e_request_latency_seconds_bucket_delta),
            mapValues(ins.engine_e2e_request_latency_seconds_bucket_delta)
        ).1,
        sumMap(
            mapKeys(ins.engine_e2e_request_latency_seconds_bucket_delta),
            mapValues(ins.engine_e2e_request_latency_seconds_bucket_delta)
        ).2
    ) AS engine_e2e_request_latency_seconds_bucket_delta,

    -- ============================================================================
    -- 7. Engine (物理引擎节点) 视角 - Cache & Memory Metrics
    -- ============================================================================
    avg(ins.engine_npu_cache_usage_perc_avg) AS engine_npu_cache_usage_perc_avg,
    max(ins.engine_npu_cache_usage_perc_max) AS engine_npu_cache_usage_perc_max,
    avg(ins.engine_cpu_cache_usage_perc_avg) AS engine_cpu_cache_usage_perc_avg,
    max(ins.engine_cpu_cache_usage_perc_max) AS engine_cpu_cache_usage_perc_max,
    avg(ins.engine_npu_prefix_cache_hit_rate_avg) AS engine_npu_prefix_cache_hit_rate_avg,

    -- ============================================================================
    -- 8. Engine (物理引擎节点) 视角 - Prompt & Generation Tokens 分布
    -- ============================================================================
    CAST(coalesce(
        sum(ins.engine_request_prompt_tokens_sum_delta)
        / nullIf(sum(ins.engine_request_prompt_tokens_count_delta), 0),
        0.0
    ) AS Float32) AS engine_request_prompt_tokens_avg,
    max(ins.engine_request_prompt_tokens_max) AS engine_request_prompt_tokens_max,
    sum(ins.engine_request_prompt_tokens_count_argMax) AS engine_request_prompt_tokens_count_argMax,
    sum(ins.engine_request_prompt_tokens_count_delta) AS engine_request_prompt_tokens_count_delta,
    sum(ins.engine_request_prompt_tokens_sum_argMax) AS engine_request_prompt_tokens_sum_argMax,
    sum(ins.engine_request_prompt_tokens_sum_delta) AS engine_request_prompt_tokens_sum_delta,
    mapFromArrays(
        sumMap(
            mapKeys(ins.engine_request_prompt_tokens_bucket_delta),
            mapValues(ins.engine_request_prompt_tokens_bucket_delta)
        ).1,
        sumMap(
            mapKeys(ins.engine_request_prompt_tokens_bucket_delta),
            mapValues(ins.engine_request_prompt_tokens_bucket_delta)
        ).2
    ) AS engine_request_prompt_tokens_bucket_delta,

    CAST(coalesce(
        sum(ins.engine_request_generation_tokens_sum_delta)
        / nullIf(sum(ins.engine_request_generation_tokens_count_delta), 0),
        0.0
    ) AS Float32) AS engine_request_generation_tokens_avg,
    max(ins.engine_request_generation_tokens_max) AS engine_request_generation_tokens_max,
    sum(ins.engine_request_generation_tokens_count_argMax) AS engine_request_generation_tokens_count_argMax,
    sum(ins.engine_request_generation_tokens_count_delta) AS engine_request_generation_tokens_count_delta,
    sum(ins.engine_request_generation_tokens_sum_argMax) AS engine_request_generation_tokens_sum_argMax,
    sum(ins.engine_request_generation_tokens_sum_delta) AS engine_request_generation_tokens_sum_delta,
    mapFromArrays(
        sumMap(
            mapKeys(ins.engine_request_generation_tokens_bucket_delta),
            mapValues(ins.engine_request_generation_tokens_bucket_delta)
        ).1,
        sumMap(
            mapKeys(ins.engine_request_generation_tokens_bucket_delta),
            mapValues(ins.engine_request_generation_tokens_bucket_delta)
        ).2
    ) AS engine_request_generation_tokens_bucket_delta,

    -- ============================================================================
    -- 9. 标识控制
    -- ============================================================================
    0 AS sync_flag

  FROM modelmate_metric.t_mops__modelmetric_instance__5min_ads AS ins
  WHERE ins.window_time >= toDateTime({start_time:UInt32}, 'UTC')
    AND ins.window_time <  toDateTime({end_time:UInt32}, 'UTC')
    AND ins.sync_flag = 0
  GROUP BY
      ins.window_time,
      ins.server_name
) AS t
LEFT JOIN
(
    -- 关联实例级 ADS 表，汇总计算服务级别总 NPU 卡数
    SELECT
        window_time,
        server_name,
        sum(allocated_npu_num) AS total_allocated_npu
    FROM modelmate_metric.t_mops__modelmetric_instance__5min_ads
    WHERE window_time >= {start_time:DateTime}
      AND window_time <  {end_time:DateTime}
    GROUP BY window_time, server_name
) AS s
ON t.window_time = s.window_time
   AND t.server_name = s.server_name;
