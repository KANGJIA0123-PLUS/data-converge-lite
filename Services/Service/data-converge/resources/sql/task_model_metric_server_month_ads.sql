-- 6.2.4 由 t_mops__modelmetric_instance__month_ads -> t_mops__modelmetric_server__month_ads

INSERT INTO modelmate_metric.t_mops__modelmetric_server__month_ads
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
    -- 外层计算：新增补充指标
    -- ============================================================================
    -- (1) 输入/输出比率
    CAST(coalesce(
        t.engine_prompt_tokens_total_delta / nullIf(t.engine_generation_tokens_total_delta, 0.0),
        0.0
    ) AS Nullable(Float32)) AS input_output_token_rate,

    -- (2) 总请求 QPS
    CAST(coalesce(
        t.engine_request_received_total_delta / nullIf(toFloat64(dateDiff('second', toDateTime({start_time:UInt32}, 'UTC'), toDateTime({end_time:UInt32}, 'UTC'))), 0.0),
        0.0
    ) AS Nullable(Float32)) AS request_received_total_qps,

    -- (3) 百万 token 所用卡时 = 服务总卡数 × 窗口秒数 × 1,000,000 / (3,600 × 总 token 数)
    CAST(coalesce(
        coalesce(s.total_allocated_npu, 0) * toFloat64(dateDiff('second', toDateTime({start_time:UInt32}, 'UTC'), toDateTime({end_time:UInt32}, 'UTC'))) * 1000000.0
        / nullIf(3600.0 * (coalesce(t.engine_prompt_tokens_total_delta, 0) + coalesce(t.engine_generation_tokens_total_delta, 0)), 0.0),
        0.0
    ) AS Nullable(Float32)) AS unit_token_computepower_consumption,

    0 AS sync_flag
FROM
(
   SELECT
    -- ============================================================================
    -- 1. 时间粒度与核心维度（抹除 process_role，向上汇聚到服务级）
    -- ============================================================================
    toDateTime({start_time:UInt32}, {system_timezone:String})                          AS window_time,
    ins.server_name,

    -- ============================================================================
    -- 2. 服务关联属性快照
    -- ============================================================================
    argMax(ins.framework_type, ins.window_time)                   AS framework_type,
    argMax(ins.framework_version, ins.window_time)                AS framework_version,
    argMax(ins.deploy_info, ins.window_time)                      AS deploy_info,
    argMax(ins.model_name, ins.window_time)                       AS model_name,
    argMax(ins.model_id, ins.window_time)                         AS model_id,
    arrayDistinct(arrayFlatten(groupArray(ins.node_ids)))   AS node_ids,
    arrayDistinct(arrayFlatten(groupArray(ins.node_ips)))   AS node_ips,
    argMax(ins.node_name, ins.window_time)                        AS node_name,
    argMax(ins.node_type, ins.window_time)                        AS node_type,
    argMax(ins.dev_mfr, ins.window_time)                          AS dev_mfr,
    argMax(ins.dev_model, ins.window_time)                        AS dev_model,
    argMax(ins.dev_status_code, ins.window_time)                  AS dev_status_code,
    argMax(ins.dev_status_name, ins.window_time)                  AS dev_status_name,

    -- ============================================================================
    -- 3. Coordinator 视角指标抽取（包含 coordinator 与 single）
    -- ============================================================================
    max(ins.coord_request_received_total_argMax)      AS coord_request_received_total_argMax,
    sum(ins.coord_request_received_total_delta)       AS coord_request_received_total_delta,

    -- 【动态化优化】使用具体时间范围总分钟数作为 QPM 分母
    CAST(coalesce(
        sum(ins.coord_request_received_total_delta)
        / nullIf(dateDiff('second', toDateTime({start_time:UInt32}, 'UTC'), toDateTime({end_time:UInt32}, 'UTC')) / 60.0, 0.0),
        0.0
    ) AS Float32) AS coord_request_received_total_qpm_avg,

    max(ins.coord_request_failed_total_argMax)        AS coord_request_failed_total_argMax,
    sum(ins.coord_request_failed_total_delta)         AS coord_request_failed_total_delta,

    max(ins.coord_request_success_total_argMax)       AS coord_request_success_total_argMax,
    sum(ins.coord_request_success_total_delta)        AS coord_request_success_total_delta,

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

    avg(ins.coord_num_requests_waiting_avg)           AS coord_num_requests_waiting_avg,

    CAST(coalesce(
        sum(ins.coord_e2e_request_latency_seconds_sum_delta)
        / nullIf(sum(ins.coord_e2e_request_latency_seconds_count_delta), 0),
        0.0
    ) AS Float32) AS coord_e2e_request_latency_seconds_avg,
    max(ins.coord_e2e_request_latency_seconds_count_argMax) AS coord_e2e_request_latency_seconds_count_argMax,
    sum(ins.coord_e2e_request_latency_seconds_count_delta)  AS coord_e2e_request_latency_seconds_count_delta,
    max(ins.coord_e2e_request_latency_seconds_sum_argMax)   AS coord_e2e_request_latency_seconds_sum_argMax,
    sum(ins.coord_e2e_request_latency_seconds_sum_delta)    AS coord_e2e_request_latency_seconds_sum_delta,
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
    -- 4. Engine 视角指标抽取（包含 engine, prefill, decode, single）
    -- ============================================================================
    -- 4.1 引擎请求与并发 (跨 P/D 物理叠加)
    max(ins.engine_request_received_total_argMax)  AS engine_request_received_total_argMax,
    sum(ins.engine_request_received_total_delta)   AS engine_request_received_total_delta,

    max(ins.engine_request_failed_total_argMax)    AS engine_request_failed_total_argMax,
    sum(ins.engine_request_failed_total_delta)     AS engine_request_failed_total_delta,

    max(ins.engine_request_success_total_argMax)   AS engine_request_success_total_argMax,
    sum(ins.engine_request_success_total_delta)    AS engine_request_success_total_delta,

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

    sum(ins.engine_inference_concurrent_requests_avg) AS engine_inference_concurrent_requests_avg,
    sum(ins.engine_num_requests_running_avg)           AS engine_num_requests_running_avg,
    sum(ins.engine_num_requests_waiting_avg)           AS engine_num_requests_waiting_avg,
    sum(ins.engine_num_requests_swapped_avg)           AS engine_num_requests_swapped_avg,

    max(ins.engine_num_preemptions_total_argMax)       AS engine_num_preemptions_total_argMax,
    sum(ins.engine_num_preemptions_total_delta)        AS engine_num_preemptions_total_delta,

    -- 4.2 Token 吞吐
    max(ins.engine_prompt_tokens_total_argMax)        AS engine_prompt_tokens_total_argMax,
    sum(ins.engine_prompt_tokens_total_delta)         AS engine_prompt_tokens_total_delta,

    max(ins.engine_generation_tokens_total_argMax)    AS engine_generation_tokens_total_argMax,
    sum(ins.engine_generation_tokens_total_delta)     AS engine_generation_tokens_total_delta,

    -- 【动态化优化】使用具体时间范围总秒数作为速率分母
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

    sum(ins.engine_avg_prompt_throughput_toks_per_s_avg)              AS engine_avg_prompt_throughput_toks_per_s_avg,
    sum(ins.engine_avg_prompt_throughput_toks_per_s_delta)            AS engine_avg_prompt_throughput_toks_per_s_delta,
    max(ins.engine_avg_prompt_throughput_toks_per_s_max)              AS engine_avg_prompt_throughput_toks_per_s_max,

    sum(ins.engine_avg_generation_throughput_toks_per_s_avg)          AS engine_avg_generation_throughput_toks_per_s_avg,
    sum(ins.engine_avg_generation_throughput_toks_per_s_delta)        AS engine_avg_generation_throughput_toks_per_s_delta,
    max(ins.engine_avg_generation_throughput_toks_per_s_max)          AS engine_avg_generation_throughput_toks_per_s_max,

    -- 4.3 引擎关键耗时 (TTFT: prefill/engine/single, TPOT: decode/engine/single, Engine E2E: 全量)
    CAST(coalesce(
        sum(ins.engine_time_to_first_token_seconds_sum_delta)
        / nullIf(sum(ins.engine_time_to_first_token_seconds_count_delta), 0),
        0.0
    ) AS Float32) AS engine_time_to_first_token_seconds_avg,
    max(ins.engine_time_to_first_token_seconds_count_argMax)          AS engine_time_to_first_token_seconds_count_argMax,
    sum(ins.engine_time_to_first_token_seconds_count_delta)           AS engine_time_to_first_token_seconds_count_delta,
    max(ins.engine_time_to_first_token_seconds_sum_argMax)            AS engine_time_to_first_token_seconds_sum_argMax,
    sum(ins.engine_time_to_first_token_seconds_sum_delta)              AS engine_time_to_first_token_seconds_sum_delta,
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

    CAST(coalesce(
        sum(ins.engine_time_per_output_token_seconds_sum_delta)
        / nullIf(sum(ins.engine_time_per_output_token_seconds_count_delta), 0),
        0.0
    ) AS Float32) AS engine_time_per_output_token_seconds_avg,
    max(ins.engine_time_per_output_token_seconds_count_argMax)       AS engine_time_per_output_token_seconds_count_argMax,
    sum(ins.engine_time_per_output_token_seconds_count_delta)        AS engine_time_per_output_token_seconds_count_delta,
    max(ins.engine_time_per_output_token_seconds_sum_argMax)          AS engine_time_per_output_token_seconds_sum_argMax,
    sum(ins.engine_time_per_output_token_seconds_sum_delta)           AS engine_time_per_output_token_seconds_sum_delta,
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

    CAST(coalesce(
        sum(ins.engine_e2e_request_latency_seconds_sum_delta)
        / nullIf(sum(ins.engine_e2e_request_latency_seconds_count_delta), 0),
        0.0
    ) AS Float32) AS engine_e2e_request_latency_seconds_avg,
    max(ins.engine_e2e_request_latency_seconds_count_argMax) AS engine_e2e_request_latency_seconds_count_argMax,
    sum(ins.engine_e2e_request_latency_seconds_count_delta)  AS engine_e2e_request_latency_seconds_count_delta,
    max(ins.engine_e2e_request_latency_seconds_sum_argMax)    AS engine_e2e_request_latency_seconds_sum_argMax,
    sum(ins.engine_e2e_request_latency_seconds_sum_delta)      AS engine_e2e_request_latency_seconds_sum_delta,
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

    -- 4.4 显存与 Cache 水位
    avg(ins.engine_npu_cache_usage_perc_avg)            AS engine_npu_cache_usage_perc_avg,
    max(ins.engine_npu_cache_usage_perc_max)            AS engine_npu_cache_usage_perc_max,
    avg(ins.engine_cpu_cache_usage_perc_avg)            AS engine_cpu_cache_usage_perc_avg,
    max(ins.engine_cpu_cache_usage_perc_max)            AS engine_cpu_cache_usage_perc_max,
    avg(ins.engine_npu_prefix_cache_hit_rate_avg)       AS engine_npu_prefix_cache_hit_rate_avg,

    -- 4.5 Prompt & Generation Token 尺寸与分布
    CAST(coalesce(
        sum(ins.engine_request_prompt_tokens_sum_delta)
        / nullIf(sum(ins.engine_request_prompt_tokens_count_delta), 0),
        0.0
    ) AS Float32) AS engine_request_prompt_tokens_avg,
    max(ins.engine_request_prompt_tokens_max)                          AS engine_request_prompt_tokens_max,
    max(ins.engine_request_prompt_tokens_count_argMax)                 AS engine_request_prompt_tokens_count_argMax,
    sum(ins.engine_request_prompt_tokens_count_delta)                  AS engine_request_prompt_tokens_count_delta,
    max(ins.engine_request_prompt_tokens_sum_argMax)                   AS engine_request_prompt_tokens_sum_argMax,
    sum(ins.engine_request_prompt_tokens_sum_delta)                     AS engine_request_prompt_tokens_sum_delta,
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
    max(ins.engine_request_generation_tokens_max)                      AS engine_request_generation_tokens_max,
    max(ins.engine_request_generation_tokens_count_argMax)             AS engine_request_generation_tokens_count_argMax,
    sum(ins.engine_request_generation_tokens_count_delta)              AS engine_request_generation_tokens_count_delta,
    max(ins.engine_request_generation_tokens_sum_argMax)               AS engine_request_generation_tokens_sum_argMax,
    sum(ins.engine_request_generation_tokens_sum_delta)                  AS engine_request_generation_tokens_sum_delta,
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

    -- 标识控制
    0 AS sync_flag

FROM modelmate_metric.t_mops__modelmetric_instance__month_ads AS ins
WHERE ins.window_time >= toDateTime({start_time:UInt32}, 'UTC')
  AND ins.window_time <  toDateTime({end_time:UInt32}, 'UTC')
  AND ins.sync_flag = 0
GROUP BY
    toDateTime({start_time:UInt32}, {system_timezone:String}),
    ins.server_name
) AS t
LEFT JOIN
(
    -- 资源实例 ADS 提供卡数；先按实例去重，再汇总服务卡数，避免重复行放大。
    SELECT
        allocation.window_time,
        allocation.server_name,
        sum(allocation.allocated_npu_num) AS total_allocated_npu
    FROM
    (
        SELECT
            toDateTime({start_time:UInt32}, {system_timezone:String}) AS window_time,
            resource.server_name,
            resource.modelinstance_id,
            anyLast(resource.allocated_npu_num) AS allocated_npu_num
        FROM modelmate_metric.t_mops_modelinstance_month_ads AS resource
        WHERE resource.window_time >= toDateTime({start_time:UInt32}, 'UTC')
          AND resource.window_time <  toDateTime({end_time:UInt32}, 'UTC')
        GROUP BY toDateTime({start_time:UInt32}, {system_timezone:String}), resource.server_name, resource.modelinstance_id
    ) AS allocation
    GROUP BY allocation.window_time, allocation.server_name
) AS s
ON  t.window_time = s.window_time
AND t.server_name = s.server_name;
