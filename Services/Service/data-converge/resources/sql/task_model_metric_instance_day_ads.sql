-- 6.1.3 由 t_mops__modelmetric_instance__day_dws -> t_mops__modelmetric_instance__day_ads

INSERT INTO modelmate_metric.t_mops__modelmetric_instance__day_ads
(
    window_time,
    server_name,
    modelinstance_id,
    modelinstance_name,
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
    coord_e2e_request_latency_seconds_bucket_argMax,
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
    engine_time_to_first_token_seconds_bucket_argMax,
    engine_time_to_first_token_seconds_bucket_delta,
    engine_time_per_output_token_seconds_avg,
    engine_time_per_output_token_seconds_count_argMax,
    engine_time_per_output_token_seconds_count_delta,
    engine_time_per_output_token_seconds_sum_argMax,
    engine_time_per_output_token_seconds_sum_delta,
    engine_time_per_output_token_seconds_bucket_argMax,
    engine_time_per_output_token_seconds_bucket_delta,
    engine_e2e_request_latency_seconds_avg,
    engine_e2e_request_latency_seconds_count_argMax,
    engine_e2e_request_latency_seconds_count_delta,
    engine_e2e_request_latency_seconds_sum_argMax,
    engine_e2e_request_latency_seconds_sum_delta,
    engine_e2e_request_latency_seconds_bucket_argMax,
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
    engine_request_prompt_tokens_bucket_argMax,
    engine_request_prompt_tokens_bucket_delta,
    engine_request_generation_tokens_avg,
    engine_request_generation_tokens_max,
    engine_request_generation_tokens_count_argMax,
    engine_request_generation_tokens_count_delta,
    engine_request_generation_tokens_sum_argMax,
    engine_request_generation_tokens_sum_delta,
    engine_request_generation_tokens_bucket_argMax,
    engine_request_generation_tokens_bucket_delta,
    -- 新增补充指标
    input_output_token_rate,
    request_received_total_qps,
    unit_token_computepower_consumption,

    sync_flag
)
SELECT
    -- ============================================================================
    -- 1. 时间与 ADS 核心维度
    -- ============================================================================
    t.window_time,
    t.server_name,
    t.modelinstance_id,

    -- ============================================================================
    -- 2. 实例关联属性
    -- ============================================================================
    t.modelinstance_name,
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

    -- ============================================================================
    -- 3. Coordinator 视角指标
    -- ============================================================================
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
    t.coord_e2e_request_latency_seconds_bucket_argMax,
    t.coord_e2e_request_latency_seconds_bucket_delta,

    -- ============================================================================
    -- 4. Engine 视角指标
    -- ============================================================================
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
    t.engine_time_to_first_token_seconds_bucket_argMax,
    t.engine_time_to_first_token_seconds_bucket_delta,
    t.engine_time_per_output_token_seconds_avg,
    t.engine_time_per_output_token_seconds_count_argMax,
    t.engine_time_per_output_token_seconds_count_delta,
    t.engine_time_per_output_token_seconds_sum_argMax,
    t.engine_time_per_output_token_seconds_sum_delta,
    t.engine_time_per_output_token_seconds_bucket_argMax,
    t.engine_time_per_output_token_seconds_bucket_delta,
    t.engine_e2e_request_latency_seconds_avg,
    t.engine_e2e_request_latency_seconds_count_argMax,
    t.engine_e2e_request_latency_seconds_count_delta,
    t.engine_e2e_request_latency_seconds_sum_argMax,
    t.engine_e2e_request_latency_seconds_sum_delta,
    t.engine_e2e_request_latency_seconds_bucket_argMax,
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
    t.engine_request_prompt_tokens_bucket_argMax,
    t.engine_request_prompt_tokens_bucket_delta,
    t.engine_request_generation_tokens_avg,
    t.engine_request_generation_tokens_max,
    t.engine_request_generation_tokens_count_argMax,
    t.engine_request_generation_tokens_count_delta,
    t.engine_request_generation_tokens_sum_argMax,
    t.engine_request_generation_tokens_sum_delta,
    t.engine_request_generation_tokens_bucket_argMax,
    t.engine_request_generation_tokens_bucket_delta,

    -- ============================================================================
    -- 5. 新增补充指标计算（天级时间跨度会自动适配 {start_time} 到 {end_time} 的秒数）
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

    -- (3) 产生百万 token 所用的卡时 = 卡数 × 区间秒数 × 1000000 / (3600 × token 增量)
    CAST(coalesce(
        (coalesce(m.allocated_npu_num, 0) * toFloat64(dateDiff('second', toDateTime({start_time:UInt32}, 'UTC'), toDateTime({end_time:UInt32}, 'UTC'))) * 1000000.0)
        / nullIf(3600.0 * (coalesce(t.engine_prompt_tokens_total_delta, 0) + coalesce(t.engine_generation_tokens_total_delta, 0)), 0.0),
        0.0
    ) AS Nullable(Float32)) AS unit_token_computepower_consumption,

    -- ============================================================================
    -- 6. 标识控制
    -- ============================================================================
    t.sync_flag

FROM
(
    SELECT
    -- ============================================================================
    -- 1. 时间粒度与核心维度（抹除 process_role，向上汇聚到实例级）
    -- ============================================================================
    dws.window_time,
    dws.server_name,
    dws.modelinstance_id,

    -- ============================================================================
    -- 2. 实例关联属性快照
    -- ============================================================================
    argMax(dws.modelinstance_name, dws.window_time)               AS modelinstance_name,
    argMax(dws.framework_type, dws.window_time)                   AS framework_type,
    argMax(dws.framework_version, dws.window_time)                AS framework_version,
    argMax(dws.deploy_info, dws.window_time)                      AS deploy_info,
    argMax(dws.model_name, dws.window_time)                       AS model_name,
    argMax(dws.model_id, dws.window_time)                         AS model_id,
    arrayDistinct(arrayFlatten(groupArray(dws.node_ids)))   AS node_ids,
    arrayDistinct(arrayFlatten(groupArray(dws.node_ips)))   AS node_ips,
    argMax(dws.node_name, dws.window_time)                        AS node_name,
    argMax(dws.node_type, dws.window_time)                        AS node_type,
    argMax(dws.dev_mfr, dws.window_time)                          AS dev_mfr,
    argMax(dws.dev_model, dws.window_time)                        AS dev_model,
    argMax(dws.dev_status_code, dws.window_time)                  AS dev_status_code,
    argMax(dws.dev_status_name, dws.window_time)                  AS dev_status_name,

    -- ============================================================================
    -- 3. Coordinator 视角指标抽取（包含 coordinator 与 single）
    -- ============================================================================
    maxIf(dws.request_received_total_argMax, dws.process_role IN ('coordinator', 'single'))      AS coord_request_received_total_argMax,
    sumIf(dws.request_received_total_delta, dws.process_role IN ('coordinator', 'single'))       AS coord_request_received_total_delta,
    -- 【动态化】：天表 QPM 分母由固定 1440.0 改为根据传入的动态天区间总分钟数计算 (秒数 / 60.0)
    CAST(coalesce(
        sumIf(dws.request_received_total_delta, dws.process_role IN ('coordinator', 'single'))
        / nullIf(dateDiff('second', toDateTime({start_time:UInt32}, 'UTC'), toDateTime({end_time:UInt32}, 'UTC')) / 60.0, 0.0),
        0.0
    ) AS Nullable(Float32)) AS coord_request_received_total_qpm_avg,

    maxIf(dws.request_failed_total_argMax, dws.process_role IN ('coordinator', 'single'))        AS coord_request_failed_total_argMax,
    sumIf(dws.request_failed_total_delta, dws.process_role IN ('coordinator', 'single'))         AS coord_request_failed_total_delta,

    maxIf(dws.request_success_total_argMax, dws.process_role IN ('coordinator', 'single'))       AS coord_request_success_total_argMax,
    sumIf(dws.request_success_total_delta, dws.process_role IN ('coordinator', 'single'))        AS coord_request_success_total_delta,

    CAST(coalesce(
        sumIf(dws.request_success_total_delta, dws.process_role IN ('coordinator', 'single')) * 100.0
        / nullIf(sumIf(dws.request_received_total_delta, dws.process_role IN ('coordinator', 'single')), 0),
        0.0
    ) AS Nullable(Float32)) AS coord_request_success_rate_avg,
    CAST(coalesce(
        sumIf(dws.request_failed_total_delta, dws.process_role IN ('coordinator', 'single')) * 100.0
        / nullIf(sumIf(dws.request_received_total_delta, dws.process_role IN ('coordinator', 'single')), 0),
        0.0
    ) AS Nullable(Float32)) AS coord_request_failed_rate_avg,



    avgIf(dws.num_requests_waiting_avg, dws.process_role IN ('coordinator', 'single'))           AS coord_num_requests_waiting_avg,

    CAST(coalesce(
        sumIf(dws.e2e_request_latency_seconds_sum_delta, dws.process_role IN ('coordinator', 'single'))
        / nullIf(sumIf(dws.e2e_request_latency_seconds_count_delta, dws.process_role IN ('coordinator', 'single')), 0),
        0.0
    ) AS Nullable(Float32)) AS coord_e2e_request_latency_seconds_avg,
    maxIf(dws.e2e_request_latency_seconds_count_argMax, dws.process_role IN ('coordinator', 'single')) AS coord_e2e_request_latency_seconds_count_argMax,
    sumIf(dws.e2e_request_latency_seconds_count_delta, dws.process_role IN ('coordinator', 'single'))  AS coord_e2e_request_latency_seconds_count_delta,
    maxIf(dws.e2e_request_latency_seconds_sum_argMax, dws.process_role IN ('coordinator', 'single'))   AS coord_e2e_request_latency_seconds_sum_argMax,
    sumIf(dws.e2e_request_latency_seconds_sum_delta, dws.process_role IN ('coordinator', 'single'))    AS coord_e2e_request_latency_seconds_sum_delta,
    argMaxIf(dws.e2e_request_latency_seconds_bucket_argMax, dws.window_time, dws.process_role IN ('coordinator', 'single')) AS coord_e2e_request_latency_seconds_bucket_argMax,
    mapFromArrays(
        sumMappedArraysIf(
            mapKeys(dws.e2e_request_latency_seconds_bucket_delta),
            mapValues(dws.e2e_request_latency_seconds_bucket_delta),
            dws.process_role IN ('coordinator', 'single')
        ).1,
        sumMappedArraysIf(
            mapKeys(dws.e2e_request_latency_seconds_bucket_delta),
            mapValues(dws.e2e_request_latency_seconds_bucket_delta),
            dws.process_role IN ('coordinator', 'single')
        ).2
    ) AS coord_e2e_request_latency_seconds_bucket_delta,

    -- ============================================================================
    -- 4. Engine 视角指标抽取（包含 engine, prefill, decode, single）
    -- ============================================================================
    -- 4.1 引擎请求与并发 (跨 P/D 物理叠加)
    maxIf(dws.request_received_total_argMax, dws.process_role IN ('engine', 'prefill', 'decode', 'single'))  AS engine_request_received_total_argMax,
    sumIf(dws.request_received_total_delta, dws.process_role IN ('engine', 'prefill', 'decode', 'single'))   AS engine_request_received_total_delta,

    maxIf(dws.request_failed_total_argMax, dws.process_role IN ('engine', 'prefill', 'decode', 'single'))    AS engine_request_failed_total_argMax,
    sumIf(dws.request_failed_total_delta, dws.process_role IN ('engine', 'prefill', 'decode', 'single'))     AS engine_request_failed_total_delta,

    maxIf(dws.request_success_total_argMax, dws.process_role IN ('engine', 'prefill', 'decode', 'single'))   AS engine_request_success_total_argMax,
    sumIf(dws.request_success_total_delta, dws.process_role IN ('engine', 'prefill', 'decode', 'single'))    AS engine_request_success_total_delta,

    CAST(coalesce(
        sumIf(dws.request_success_total_delta, dws.process_role IN ('engine', 'prefill', 'decode', 'single')) * 100.0
        / nullIf(sumIf(dws.request_received_total_delta, dws.process_role IN ('engine', 'prefill', 'decode', 'single')), 0),
        0.0
    ) AS Nullable(Float32)) AS engine_request_success_rate_avg,
    CAST(coalesce(
        sumIf(dws.request_failed_total_delta, dws.process_role IN ('engine', 'prefill', 'decode', 'single')) * 100.0
        / nullIf(sumIf(dws.request_received_total_delta, dws.process_role IN ('engine', 'prefill', 'decode', 'single')), 0),
        0.0
    ) AS Nullable(Float32)) AS engine_request_failed_rate_avg,

    sumIf(dws.inference_concurrent_requests_avg, dws.process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_inference_concurrent_requests_avg,
    sumIf(dws.num_requests_running_avg, dws.process_role IN ('engine', 'prefill', 'decode', 'single'))           AS engine_num_requests_running_avg,
    sumIf(dws.num_requests_waiting_avg, dws.process_role IN ('engine', 'prefill', 'decode', 'single'))           AS engine_num_requests_waiting_avg,
    sumIf(dws.num_requests_swapped_avg, dws.process_role IN ('engine', 'prefill', 'decode', 'single'))           AS engine_num_requests_swapped_avg,

    maxIf(dws.num_preemptions_total_argMax, dws.process_role IN ('engine', 'prefill', 'decode', 'single'))       AS engine_num_preemptions_total_argMax,
    sumIf(dws.num_preemptions_total_delta, dws.process_role IN ('engine', 'prefill', 'decode', 'single'))        AS engine_num_preemptions_total_delta,

    -- 4.2 Token 吞吐
    maxIf(dws.prompt_tokens_total_argMax, dws.process_role IN ('engine', 'prefill', 'decode', 'single'))        AS engine_prompt_tokens_total_argMax,
    sumIf(dws.prompt_tokens_total_delta, dws.process_role IN ('engine', 'prefill', 'decode', 'single'))         AS engine_prompt_tokens_total_delta,

    maxIf(dws.generation_tokens_total_argMax, dws.process_role IN ('engine', 'prefill', 'decode', 'single'))    AS engine_generation_tokens_total_argMax,
    sumIf(dws.generation_tokens_total_delta, dws.process_role IN ('engine', 'prefill', 'decode', 'single'))     AS engine_generation_tokens_total_delta,

    -- 【动态化】：Token 速率分母由硬编码改为了动态时间差计算 (dateDiff)
    CAST(coalesce(
        sumIf(dws.prompt_tokens_total_delta, dws.process_role IN ('engine', 'prefill', 'decode', 'single'))
        / nullIf(toFloat64(dateDiff('second', toDateTime({start_time:UInt32}, 'UTC'), toDateTime({end_time:UInt32}, 'UTC'))), 0.0),
        0.0
    ) AS Nullable(Float32)) AS engine_prompt_tokens_total_delta_rate_avg,
    CAST(coalesce(
        sumIf(dws.generation_tokens_total_delta, dws.process_role IN ('engine', 'prefill', 'decode', 'single'))
        / nullIf(toFloat64(dateDiff('second', toDateTime({start_time:UInt32}, 'UTC'), toDateTime({end_time:UInt32}, 'UTC'))), 0.0),
        0.0
    ) AS Nullable(Float32)) AS engine_generation_tokens_total_delta_rate_avg,
    CAST(coalesce(
        (sumIf(dws.prompt_tokens_total_delta, dws.process_role IN ('engine', 'prefill', 'decode', 'single')) + sumIf(dws.generation_tokens_total_delta, dws.process_role IN ('engine', 'prefill', 'decode', 'single')))
        / nullIf(toFloat64(dateDiff('second', toDateTime({start_time:UInt32}, 'UTC'), toDateTime({end_time:UInt32}, 'UTC'))), 0.0),
        0.0
    ) AS Nullable(Float32)) AS engine_total_throughput_rate,

    sumIf(dws.avg_prompt_throughput_toks_per_s_avg, dws.process_role IN ('prefill', 'engine', 'single'))              AS engine_avg_prompt_throughput_toks_per_s_avg,
    sumIf(dws.avg_prompt_throughput_toks_per_s_delta, dws.process_role IN ('prefill', 'engine', 'single'))            AS engine_avg_prompt_throughput_toks_per_s_delta,
    maxIf(dws.avg_prompt_throughput_toks_per_s_max, dws.process_role IN ('prefill', 'engine', 'single'))              AS engine_avg_prompt_throughput_toks_per_s_max,

    sumIf(dws.avg_generation_throughput_toks_per_s_avg, dws.process_role IN ('decode', 'engine', 'single'))          AS engine_avg_generation_throughput_toks_per_s_avg,
    sumIf(dws.avg_generation_throughput_toks_per_s_delta, dws.process_role IN ('decode', 'engine', 'single'))        AS engine_avg_generation_throughput_toks_per_s_delta,
    maxIf(dws.avg_generation_throughput_toks_per_s_max, dws.process_role IN ('decode', 'engine', 'single'))          AS engine_avg_generation_throughput_toks_per_s_max,

    -- 4.3 引擎关键耗时 (TTFT: prefill/engine/single, TPOT: decode/engine/single, Engine E2E: 全量)
    CAST(coalesce(
        sumIf(dws.time_to_first_token_seconds_sum_delta, dws.process_role IN ('prefill', 'engine', 'single'))
        / nullIf(sumIf(dws.time_to_first_token_seconds_count_delta, dws.process_role IN ('prefill', 'engine', 'single')), 0),
        0.0
    ) AS Nullable(Float32)) AS engine_time_to_first_token_seconds_avg,
    maxIf(dws.time_to_first_token_seconds_count_argMax, dws.process_role IN ('prefill', 'engine', 'single'))          AS engine_time_to_first_token_seconds_count_argMax,
    sumIf(dws.time_to_first_token_seconds_count_delta, dws.process_role IN ('prefill', 'engine', 'single'))           AS engine_time_to_first_token_seconds_count_delta,
    maxIf(dws.time_to_first_token_seconds_sum_argMax, dws.process_role IN ('prefill', 'engine', 'single'))            AS engine_time_to_first_token_seconds_sum_argMax,
    sumIf(dws.time_to_first_token_seconds_sum_delta, dws.process_role IN ('prefill', 'engine', 'single'))              AS engine_time_to_first_token_seconds_sum_delta,
    argMaxIf(dws.time_to_first_token_seconds_bucket_argMax, dws.window_time, dws.process_role IN ('prefill', 'engine', 'single')) AS engine_time_to_first_token_seconds_bucket_argMax,
    mapFromArrays(
        sumMappedArraysIf(
            mapKeys(dws.time_to_first_token_seconds_bucket_delta),
            mapValues(dws.time_to_first_token_seconds_bucket_delta),
            dws.process_role IN ('prefill', 'engine', 'single')
        ).1,
        sumMappedArraysIf(
            mapKeys(dws.time_to_first_token_seconds_bucket_delta),
            mapValues(dws.time_to_first_token_seconds_bucket_delta),
            dws.process_role IN ('prefill', 'engine', 'single')
        ).2
    ) AS engine_time_to_first_token_seconds_bucket_delta,

    CAST(coalesce(
        sumIf(dws.time_per_output_token_seconds_sum_delta, dws.process_role IN ('decode', 'engine', 'single'))
        / nullIf(sumIf(dws.time_per_output_token_seconds_count_delta, dws.process_role IN ('decode', 'engine', 'single')), 0),
        0.0
    ) AS Nullable(Float32)) AS engine_time_per_output_token_seconds_avg,
    maxIf(dws.time_per_output_token_seconds_count_argMax, dws.process_role IN ('decode', 'engine', 'single'))       AS engine_time_per_output_token_seconds_count_argMax,
    sumIf(dws.time_per_output_token_seconds_count_delta, dws.process_role IN ('decode', 'engine', 'single'))        AS engine_time_per_output_token_seconds_count_delta,
    maxIf(dws.time_per_output_token_seconds_sum_argMax, dws.process_role IN ('decode', 'engine', 'single'))          AS engine_time_per_output_token_seconds_sum_argMax,
    sumIf(dws.time_per_output_token_seconds_sum_delta, dws.process_role IN ('decode', 'engine', 'single'))           AS engine_time_per_output_token_seconds_sum_delta,
    argMaxIf(dws.time_per_output_token_seconds_bucket_argMax, dws.window_time, dws.process_role IN ('decode', 'engine', 'single')) AS engine_time_per_output_token_seconds_bucket_argMax,
    mapFromArrays(
        sumMappedArraysIf(
            mapKeys(dws.time_per_output_token_seconds_bucket_delta),
            mapValues(dws.time_per_output_token_seconds_bucket_delta),
            dws.process_role IN ('decode', 'engine', 'single')
        ).1,
        sumMappedArraysIf(
            mapKeys(dws.time_per_output_token_seconds_bucket_delta),
            mapValues(dws.time_per_output_token_seconds_bucket_delta),
            dws.process_role IN ('decode', 'engine', 'single')
        ).2
    ) AS engine_time_per_output_token_seconds_bucket_delta,

    CAST(coalesce(
        sumIf(dws.e2e_request_latency_seconds_sum_delta, dws.process_role IN ('engine', 'prefill', 'decode', 'single'))
        / nullIf(sumIf(dws.e2e_request_latency_seconds_count_delta, dws.process_role IN ('engine', 'prefill', 'decode', 'single')), 0),
        0.0
    ) AS Nullable(Float32)) AS engine_e2e_request_latency_seconds_avg,
    maxIf(dws.e2e_request_latency_seconds_count_argMax, dws.process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_e2e_request_latency_seconds_count_argMax,
    sumIf(dws.e2e_request_latency_seconds_count_delta, dws.process_role IN ('engine', 'prefill', 'decode', 'single'))  AS engine_e2e_request_latency_seconds_count_delta,
    maxIf(dws.e2e_request_latency_seconds_sum_argMax, dws.process_role IN ('engine', 'prefill', 'decode', 'single'))    AS engine_e2e_request_latency_seconds_sum_argMax,
    sumIf(dws.e2e_request_latency_seconds_sum_delta, dws.process_role IN ('engine', 'prefill', 'decode', 'single'))      AS engine_e2e_request_latency_seconds_sum_delta,
    argMaxIf(dws.e2e_request_latency_seconds_bucket_argMax, dws.window_time, dws.process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_e2e_request_latency_seconds_bucket_argMax,
    mapFromArrays(
        sumMappedArraysIf(
            mapKeys(dws.e2e_request_latency_seconds_bucket_delta),
            mapValues(dws.e2e_request_latency_seconds_bucket_delta),
            dws.process_role IN ('engine', 'prefill', 'decode', 'single')
        ).1,
        sumMappedArraysIf(
            mapKeys(dws.e2e_request_latency_seconds_bucket_delta),
            mapValues(dws.e2e_request_latency_seconds_bucket_delta),
            dws.process_role IN ('engine', 'prefill', 'decode', 'single')
        ).2
    ) AS engine_e2e_request_latency_seconds_bucket_delta,

    -- 4.4 显存与 Cache 水位
    avgIf(dws.npu_cache_usage_perc_avg, dws.process_role IN ('engine', 'prefill', 'decode', 'single'))            AS engine_npu_cache_usage_perc_avg,
    maxIf(dws.npu_cache_usage_perc_max, dws.process_role IN ('engine', 'prefill', 'decode', 'single'))            AS engine_npu_cache_usage_perc_max,
    avgIf(dws.cpu_cache_usage_perc_avg, dws.process_role IN ('engine', 'prefill', 'decode', 'single'))            AS engine_cpu_cache_usage_perc_avg,
    maxIf(dws.cpu_cache_usage_perc_max, dws.process_role IN ('engine', 'prefill', 'decode', 'single'))            AS engine_cpu_cache_usage_perc_max,
    avgIf(dws.npu_prefix_cache_hit_rate_avg, dws.process_role IN ('engine', 'prefill', 'decode', 'single'))       AS engine_npu_prefix_cache_hit_rate_avg,

    -- 4.5 Prompt & Generation Token 尺寸与分布
    CAST(coalesce(
        sumIf(dws.request_prompt_tokens_sum_delta, dws.process_role IN ('prefill', 'engine', 'single'))
        / nullIf(sumIf(dws.request_prompt_tokens_count_delta, dws.process_role IN ('prefill', 'engine', 'single')), 0),
        0.0
    ) AS Nullable(Float32)) AS engine_request_prompt_tokens_avg,
    maxIf(dws.request_prompt_tokens_max, dws.process_role IN ('prefill', 'engine', 'single'))                          AS engine_request_prompt_tokens_max,
    maxIf(dws.request_prompt_tokens_count_argMax, dws.process_role IN ('prefill', 'engine', 'single'))                 AS engine_request_prompt_tokens_count_argMax,
    sumIf(dws.request_prompt_tokens_count_delta, dws.process_role IN ('prefill', 'engine', 'single'))                  AS engine_request_prompt_tokens_count_delta,
    maxIf(dws.request_prompt_tokens_sum_argMax, dws.process_role IN ('prefill', 'engine', 'single'))                   AS engine_request_prompt_tokens_sum_argMax,
    sumIf(dws.request_prompt_tokens_sum_delta, dws.process_role IN ('prefill', 'engine', 'single'))                     AS engine_request_prompt_tokens_sum_delta,
    argMaxIf(dws.request_prompt_tokens_bucket_argMax, dws.window_time, dws.process_role IN ('prefill', 'engine', 'single')) AS engine_request_prompt_tokens_bucket_argMax,
    mapFromArrays(
        sumMappedArraysIf(
            mapKeys(dws.request_prompt_tokens_bucket_delta),
            mapValues(dws.request_prompt_tokens_bucket_delta),
            dws.process_role IN ('prefill', 'engine', 'single')
        ).1,
        sumMappedArraysIf(
            mapKeys(dws.request_prompt_tokens_bucket_delta),
            mapValues(dws.request_prompt_tokens_bucket_delta),
            dws.process_role IN ('prefill', 'engine', 'single')
        ).2
    ) AS engine_request_prompt_tokens_bucket_delta,

    CAST(coalesce(
        sumIf(dws.request_generation_tokens_sum_delta, dws.process_role IN ('decode', 'engine', 'single'))
        / nullIf(sumIf(dws.request_generation_tokens_count_delta, dws.process_role IN ('decode', 'engine', 'single')), 0),
        0.0
    ) AS Nullable(Float32)) AS engine_request_generation_tokens_avg,
    maxIf(dws.request_generation_tokens_max, dws.process_role IN ('decode', 'engine', 'single'))                      AS engine_request_generation_tokens_max,
    maxIf(dws.request_generation_tokens_count_argMax, dws.process_role IN ('decode', 'engine', 'single'))             AS engine_request_generation_tokens_count_argMax,
    sumIf(dws.request_generation_tokens_count_delta, dws.process_role IN ('decode', 'engine', 'single'))              AS engine_request_generation_tokens_count_delta,
    maxIf(dws.request_generation_tokens_sum_argMax, dws.process_role IN ('decode', 'engine', 'single'))               AS engine_request_generation_tokens_sum_argMax,
    sumIf(dws.request_generation_tokens_sum_delta, dws.process_role IN ('decode', 'engine', 'single'))                  AS engine_request_generation_tokens_sum_delta,
    argMaxIf(dws.request_generation_tokens_bucket_argMax, dws.window_time, dws.process_role IN ('decode', 'engine', 'single')) AS engine_request_generation_tokens_bucket_argMax,
    mapFromArrays(
        sumMappedArraysIf(
            mapKeys(dws.request_generation_tokens_bucket_delta),
            mapValues(dws.request_generation_tokens_bucket_delta),
            dws.process_role IN ('decode', 'engine', 'single')
        ).1,
        sumMappedArraysIf(
            mapKeys(dws.request_generation_tokens_bucket_delta),
            mapValues(dws.request_generation_tokens_bucket_delta),
            dws.process_role IN ('decode', 'engine', 'single')
        ).2
    ) AS engine_request_generation_tokens_bucket_delta,

    -- 标识控制
    any(dws.sync_flag)                                                                                              AS sync_flag

FROM modelmate_metric.t_mops__modelmetric_instance__day_dws AS dws
WHERE dws.window_time >= toDateTime({start_time:UInt32}, {system_timezone:String})
  AND dws.window_time <  toDateTime({end_time:UInt32}, {system_timezone:String})
  AND dws.process_role != 'worker' -- 【关键过滤】：彻底丢弃 worker 数据
GROUP BY
    dws.window_time,
    dws.server_name,
    dws.modelinstance_id
) AS t
LEFT JOIN
(
    -- 关联本地表获取 allocated_npu_num
    SELECT
        resource.window_time,
        resource.server_name,
        resource.modelinstance_id,
        anyLast(resource.allocated_npu_num) AS allocated_npu_num
    FROM modelmate_metric.t_mops_modelinstance_day_ads AS resource
    WHERE resource.window_time >= toDateTime({start_time:UInt32}, {system_timezone:String})
      AND resource.window_time <  toDateTime({end_time:UInt32}, {system_timezone:String})
    GROUP BY resource.window_time, resource.server_name, resource.modelinstance_id
) AS m
ON t.window_time = m.window_time
   AND t.server_name = m.server_name
   AND t.modelinstance_id = m.modelinstance_id;
