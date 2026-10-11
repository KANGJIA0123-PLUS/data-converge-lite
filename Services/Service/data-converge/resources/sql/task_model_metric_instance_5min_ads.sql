-- 6.1.1 由 t_mops__modelmetric_instance__5min_dws -> t_mops__modelmetric_instance__5min_ads
 -- [最外层：实例 ADS 层面] ── GROUP BY window_time, server_name, modelinstance_id
 --    ▲
 --    │ (把 Prefill 和 Decode 角色物理叠加，计算实例整体 QPM、总 Token 吞吐、整体 KV Cache)
 --    │
 -- [中间层：角色级 Role 层面] ── GROUP BY window_time, server_name, modelinstance_id, process_role
 --    ▲
 --    │ (隔离 TTFT/TPOT：Prefill 角色的时延计算为 TTFT，Decode 角色的时延计算为 TPOT)
 --    │
 -- [最内层：逻辑组级 Group 层面] ── GROUP BY window_time, server_name, modelinstance_id, process_role, group_id
 --    ▲
 --    │ (抹除 TP/PP 8卡重复上报：组内采用 MAX/argMax 锁住 Rank 0 主容器上报的数值)
 --    │
 -- (底层数据源：t_mops__modelmetric_instance__5min_dws)
 -- 1.问题：DWS 进 ADS 时，worker 节点的 Counter 类指标虽然被 max() 选出，但它的性能/缓存指标（如 CPU/NPU Cache Usage）会在内层被 avg() 算进去
 -- 在内层 WHERE 条件中直接加 AND process_role != 'worker'。worker 卡在底层的任何指标都不应该参与 ADS 汇总。
 -- 2.外层 Coordinator 视角：精确对齐 coordinator + single
 -- WHERE process_role IN ('coordinator', 'single'),单机混跑（single）既是 Gateway 也是 Engine，它的 QPM 和 E2E 耗时也必须计入 Coordinator 指标。
 -- 3.外层 Engine 视角：精确对齐 engine + prefill + decode + single
 -- 外层 Engine 视角：精确对齐 engine + prefill + decode + single, 用显式白名单替代 != 模糊匹配，防止上游上报脏数据或 worker 漏网。
 -- 4. 4. TTFT / TPOT 时延判定：精细化匹配
 -- TTFT（首包）：WHERE process_role IN ('prefill', 'engine', 'single')
 -- TPOT（逐字）：WHERE process_role IN ('decode', 'engine', 'single')

INSERT INTO modelmate_metric.t_mops__modelmetric_instance__5min_ads
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
    sync_flag
)
SELECT
    -- ============================================================================
    -- 1. 时间与 ADS 核心维度 (抹除 process_role、group_id、container_id)
    -- ============================================================================
    window_time,
    server_name,
    modelinstance_id,

    -- ============================================================================
    -- 2. 实例关联属性 (锁住窗口内最新快照)
    -- ============================================================================
    any(modelinstance_name) AS modelinstance_name,
    any(framework_type)     AS framework_type,
    any(framework_version)  AS framework_version,
    any(deploy_info)        AS deploy_info,
    any(model_name)         AS model_name,
    any(model_id)           AS model_id,
    -- 将子查询中每个 group 的节点数组合并为唯一的全实例节点数组
    arrayDistinct(arrayFlatten(groupArray(group_node_ids))) AS node_ids,
    arrayDistinct(arrayFlatten(groupArray(group_node_ips))) AS node_ips,
    any(node_name)          AS node_name, -- 注意这里不能直接使用node_name查东西了，要使用id和ip
    any(node_type)          AS node_type, -- 注意这里不能直接使用node_type查东西了，要使用id和ip
    any(dev_mfr)            AS dev_mfr,
    any(dev_model)          AS dev_model,
    any(dev_status_code)    AS dev_status_code,
    any(dev_status_name)    AS dev_status_name,

    -- ============================================================================
    -- 3. Coordinator (网关/协调节点) 视角指标 (包含 coordinator 与 single)
    -- ============================================================================
    sumIf(group_request_received_argMax, process_role IN ('coordinator', 'single')) AS coord_request_received_total_argMax,
    sumIf(group_request_received_delta,  process_role IN ('coordinator', 'single')) AS coord_request_received_total_delta,
    -- 【动态化】：QPM 改为通过动态分钟数计算 (区间秒数 / 60.0)
    CAST(coalesce(
        sumIf(group_request_received_delta, process_role IN ('coordinator', 'single'))
        / nullIf(dateDiff('second', {start_time:DateTime}, {end_time:DateTime}) / 60.0, 0.0),
        0.0
    ) AS Float32) AS coord_request_received_total_qpm_avg,

    sumIf(group_request_failed_argMax,   process_role IN ('coordinator', 'single')) AS coord_request_failed_total_argMax,
    sumIf(group_request_failed_delta,    process_role IN ('coordinator', 'single')) AS coord_request_failed_total_delta,

    sumIf(group_request_success_argMax,  process_role IN ('coordinator', 'single')) AS coord_request_success_total_argMax,
    sumIf(group_request_success_delta,   process_role IN ('coordinator', 'single')) AS coord_request_success_total_delta,

    CAST(coalesce(
        sumIf(group_request_success_delta, process_role IN ('coordinator', 'single')) * 100.0
        / nullIf(sumIf(group_request_received_delta, process_role IN ('coordinator', 'single')), 0),
        0.0
    ) AS Float32) AS coord_request_success_rate_avg,
    CAST(coalesce(
        sumIf(group_request_failed_delta, process_role IN ('coordinator', 'single')) * 100.0
        / nullIf(sumIf(group_request_received_delta, process_role IN ('coordinator', 'single')), 0),
        0.0
    ) AS Float32) AS coord_request_failed_rate_avg,

    avgIf(group_num_requests_waiting_avg, process_role IN ('coordinator', 'single')) AS coord_num_requests_waiting_avg,

    -- Coordinator E2E Latency
    CAST(coalesce(
        sumIf(group_e2e_sum_delta, process_role IN ('coordinator', 'single'))
        / nullIf(sumIf(group_e2e_count_delta, process_role IN ('coordinator', 'single')), 0),
        0.0
    ) AS Float32) AS coord_e2e_request_latency_seconds_avg,
    sumIf(group_e2e_count_argMax,        process_role IN ('coordinator', 'single')) AS coord_e2e_request_latency_seconds_count_argMax,
    sumIf(group_e2e_count_delta,         process_role IN ('coordinator', 'single')) AS coord_e2e_request_latency_seconds_count_delta,
    sumIf(group_e2e_sum_argMax,          process_role IN ('coordinator', 'single')) AS coord_e2e_request_latency_seconds_sum_argMax,
    sumIf(group_e2e_sum_delta,           process_role IN ('coordinator', 'single')) AS coord_e2e_request_latency_seconds_sum_delta,
    anyIf(group_e2e_bucket_argMax,       process_role IN ('coordinator', 'single')) AS coord_e2e_request_latency_seconds_bucket_argMax,
    mapFromArrays(
        sumMappedArraysIf(
            mapKeys(group_e2e_bucket_delta),
            mapValues(group_e2e_bucket_delta),
            process_role IN ('coordinator', 'single')
        ).1,
        sumMappedArraysIf(
            mapKeys(group_e2e_bucket_delta),
            mapValues(group_e2e_bucket_delta),
            process_role IN ('coordinator', 'single')
        ).2
    ) AS coord_e2e_request_latency_seconds_bucket_delta,

    -- ============================================================================
    -- 4. Engine (物理引擎节点) 视角 - Request & Concurrency Metrics
    -- ============================================================================
    sumIf(group_request_received_argMax, process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_request_received_total_argMax,
    sumIf(group_request_received_delta,  process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_request_received_total_delta,

    sumIf(group_request_failed_argMax,   process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_request_failed_total_argMax,
    sumIf(group_request_failed_delta,    process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_request_failed_total_delta,

    sumIf(group_request_success_argMax,  process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_request_success_total_argMax,
    sumIf(group_request_success_delta,   process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_request_success_total_delta,

    CAST(coalesce(
        sumIf(group_request_success_delta, process_role IN ('engine', 'prefill', 'decode', 'single')) * 100.0
        / nullIf(sumIf(group_request_received_delta, process_role IN ('engine', 'prefill', 'decode', 'single')), 0),
        0.0
    ) AS Float32) AS engine_request_success_rate_avg,

    CAST(coalesce(
        sumIf(group_request_failed_delta, process_role IN ('engine', 'prefill', 'decode', 'single')) * 100.0
        / nullIf(sumIf(group_request_received_delta, process_role IN ('engine', 'prefill', 'decode', 'single')), 0),
        0.0
    ) AS Float32) AS engine_request_failed_rate_avg,
    sumIf(group_concurrent_requests_avg,  process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_inference_concurrent_requests_avg,
    sumIf(group_num_requests_running_avg, process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_num_requests_running_avg,
    sumIf(group_num_requests_waiting_avg, process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_num_requests_waiting_avg,
    sumIf(group_num_requests_swapped_avg, process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_num_requests_swapped_avg,

    sumIf(group_num_preemptions_argMax,  process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_num_preemptions_total_argMax,
    sumIf(group_num_preemptions_delta,   process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_num_preemptions_total_delta,

    -- ============================================================================
    -- 5. Engine (物理引擎节点) 视角 - Token Totals & Throughput Rates
    -- ============================================================================
    sumIf(group_prompt_tokens_argMax,     process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_prompt_tokens_total_argMax,
    sumIf(group_prompt_tokens_delta,      process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_prompt_tokens_total_delta,

    sumIf(group_generation_tokens_argMax, process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_generation_tokens_total_argMax,
    sumIf(group_generation_tokens_delta,  process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_generation_tokens_total_delta,

    -- 【动态化】：Token 速率分母改为动态秒数 (dateDiff)
    CAST(coalesce(
        sumIf(group_prompt_tokens_delta, process_role IN ('engine', 'prefill', 'decode', 'single'))
        / nullIf(toFloat64(dateDiff('second', {start_time:DateTime}, {end_time:DateTime})), 0.0),
        0.0
    ) AS Float32) AS engine_prompt_tokens_total_delta_rate_avg,

    CAST(coalesce(
        sumIf(group_generation_tokens_delta, process_role IN ('engine', 'prefill', 'decode', 'single'))
        / nullIf(toFloat64(dateDiff('second', {start_time:DateTime}, {end_time:DateTime})), 0.0),
        0.0
    ) AS Float32) AS engine_generation_tokens_total_delta_rate_avg,

    CAST(coalesce(
        (sumIf(group_prompt_tokens_delta, process_role IN ('engine', 'prefill', 'decode', 'single'))
            + sumIf(group_generation_tokens_delta, process_role IN ('engine', 'prefill', 'decode', 'single')))
        / nullIf(toFloat64(dateDiff('second', {start_time:DateTime}, {end_time:DateTime})), 0.0), 0.0
    ) AS Float32) AS engine_total_throughput_rate,
    avgIf(group_prompt_throughput_avg,   process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_avg_prompt_throughput_toks_per_s_avg,
    sumIf(group_prompt_throughput_delta, process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_avg_prompt_throughput_toks_per_s_delta,
    maxIf(group_prompt_throughput_max,   process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_avg_prompt_throughput_toks_per_s_max,

    avgIf(group_generation_throughput_avg,   process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_avg_generation_throughput_toks_per_s_avg,
    sumIf(group_generation_throughput_delta, process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_avg_generation_throughput_toks_per_s_delta,
    maxIf(group_generation_throughput_max,   process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_avg_generation_throughput_toks_per_s_max,

    -- ============================================================================
    -- 6. Engine (物理引擎节点) 视角 - Key Latencies (TTFT / TPOT / Engine E2E)
    -- ============================================================================
    -- TTFT (归属于 prefill, engine, single)
    CAST(coalesce(
        sumIf(group_ttft_sum_delta, process_role IN ('prefill', 'engine', 'single'))
        / nullIf(sumIf(group_ttft_count_delta, process_role IN ('prefill', 'engine', 'single')), 0),
        0.0
    ) AS Float32) AS engine_time_to_first_token_seconds_avg,
    sumIf(group_ttft_count_argMax, process_role IN ('prefill', 'engine', 'single')) AS engine_time_to_first_token_seconds_count_argMax,
    sumIf(group_ttft_count_delta,  process_role IN ('prefill', 'engine', 'single')) AS engine_time_to_first_token_seconds_count_delta,
    sumIf(group_ttft_sum_argMax,   process_role IN ('prefill', 'engine', 'single')) AS engine_time_to_first_token_seconds_sum_argMax,
    sumIf(group_ttft_sum_delta,    process_role IN ('prefill', 'engine', 'single')) AS engine_time_to_first_token_seconds_sum_delta,
    anyIf(group_ttft_bucket_argMax, process_role IN ('prefill', 'engine', 'single')) AS engine_time_to_first_token_seconds_bucket_argMax,
    mapFromArrays(
        sumMappedArraysIf(
            mapKeys(group_ttft_bucket_delta),
            mapValues(group_ttft_bucket_delta),
            process_role IN ('prefill', 'engine', 'single')
        ).1,
        sumMappedArraysIf(
            mapKeys(group_ttft_bucket_delta),
            mapValues(group_ttft_bucket_delta),
            process_role IN ('prefill', 'engine', 'single')
        ).2
    ) AS engine_time_to_first_token_seconds_bucket_delta,

    -- TPOT (归属于 decode, engine, single)
    CAST(coalesce(
        sumIf(group_tpot_sum_delta, process_role IN ('decode', 'engine', 'single'))
        / nullIf(sumIf(group_tpot_count_delta, process_role IN ('decode', 'engine', 'single')), 0),
        0.0
    ) AS Float32) AS engine_time_per_output_token_seconds_avg,
    sumIf(group_tpot_count_argMax, process_role IN ('decode', 'engine', 'single')) AS engine_time_per_output_token_seconds_count_argMax,
    sumIf(group_tpot_count_delta,  process_role IN ('decode', 'engine', 'single')) AS engine_time_per_output_token_seconds_count_delta,
    sumIf(group_tpot_sum_argMax,   process_role IN ('decode', 'engine', 'single')) AS engine_time_per_output_token_seconds_sum_argMax,
    sumIf(group_tpot_sum_delta,    process_role IN ('decode', 'engine', 'single')) AS engine_time_per_output_token_seconds_sum_delta,
    anyIf(group_tpot_bucket_argMax, process_role IN ('decode', 'engine', 'single')) AS engine_time_per_output_token_seconds_bucket_argMax,
    mapFromArrays(
        sumMappedArraysIf(
            mapKeys(group_tpot_bucket_delta),
            mapValues(group_tpot_bucket_delta),
            process_role IN ('decode', 'engine', 'single')
        ).1,
        sumMappedArraysIf(
            mapKeys(group_tpot_bucket_delta),
            mapValues(group_tpot_bucket_delta),
            process_role IN ('decode', 'engine', 'single')
        ).2
    ) AS engine_time_per_output_token_seconds_bucket_delta,

    -- Engine E2E (纯物理引擎端到端: engine, prefill, decode, single)
    CAST(coalesce(
        sumIf(group_e2e_sum_delta, process_role IN ('engine', 'prefill', 'decode', 'single'))
        / nullIf(sumIf(group_e2e_count_delta, process_role IN ('engine', 'prefill', 'decode', 'single')), 0),
        0.0
    ) AS Float32) AS engine_e2e_request_latency_seconds_avg,
    sumIf(group_e2e_count_argMax,  process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_e2e_request_latency_seconds_count_argMax,
    sumIf(group_e2e_count_delta,   process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_e2e_request_latency_seconds_count_delta,
    sumIf(group_e2e_sum_argMax,    process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_e2e_request_latency_seconds_sum_argMax,
    sumIf(group_e2e_sum_delta,     process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_e2e_request_latency_seconds_sum_delta,
    anyIf(group_e2e_bucket_argMax, process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_e2e_request_latency_seconds_bucket_argMax,
    mapFromArrays(
        sumMappedArraysIf(
            mapKeys(group_e2e_bucket_delta),
            mapValues(group_e2e_bucket_delta),
            process_role IN ('engine', 'prefill', 'decode', 'single')
        ).1,
        sumMappedArraysIf(
            mapKeys(group_e2e_bucket_delta),
            mapValues(group_e2e_bucket_delta),
            process_role IN ('engine', 'prefill', 'decode', 'single')
        ).2
    ) AS engine_e2e_request_latency_seconds_bucket_delta,
    -- ============================================================================
    -- 7. Engine (物理引擎节点) 视角 - Cache & Memory Metrics
    -- ============================================================================
    avgIf(group_npu_cache_usage_avg,      process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_npu_cache_usage_perc_avg,
    maxIf(group_npu_cache_usage_max,      process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_npu_cache_usage_perc_max,
    avgIf(group_cpu_cache_usage_avg,      process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_cpu_cache_usage_perc_avg,
    maxIf(group_cpu_cache_usage_max,      process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_cpu_cache_usage_perc_max,
    avgIf(group_prefix_cache_hit_rate_avg, process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_npu_prefix_cache_hit_rate_avg,

    -- ============================================================================
    -- 8. Engine (物理引擎节点) 视角 - Prompt & Generation Tokens 分布
    -- ============================================================================
    CAST(coalesce(
        sumIf(group_prompt_tok_sum_delta, process_role IN ('engine', 'prefill', 'decode', 'single'))
        / nullIf(sumIf(group_prompt_tok_cnt_delta, process_role IN ('engine', 'prefill', 'decode', 'single')), 0),
        0.0
    ) AS Float32) AS engine_request_prompt_tokens_avg,
    maxIf(group_prompt_tok_max,              process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_request_prompt_tokens_max,
    sumIf(group_prompt_tok_cnt_argMax,      process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_request_prompt_tokens_count_argMax,
    sumIf(group_prompt_tok_cnt_delta,        process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_request_prompt_tokens_count_delta,
    sumIf(group_prompt_tok_sum_argMax,       process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_request_prompt_tokens_sum_argMax,
    sumIf(group_prompt_tok_sum_delta,        process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_request_prompt_tokens_sum_delta,
    anyIf(group_prompt_tok_bucket_argMax,    process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_request_prompt_tokens_bucket_argMax,
    mapFromArrays(
        sumMappedArraysIf(
            mapKeys(group_prompt_tok_bucket_delta),
            mapValues(group_prompt_tok_bucket_delta),
            process_role IN ('engine', 'prefill', 'decode', 'single')
        ).1,
        sumMappedArraysIf(
            mapKeys(group_prompt_tok_bucket_delta),
            mapValues(group_prompt_tok_bucket_delta),
            process_role IN ('engine', 'prefill', 'decode', 'single')
        ).2
    ) AS engine_request_prompt_tokens_bucket_delta,

    CAST(coalesce(
        sumIf(group_gen_tok_sum_delta, process_role IN ('engine', 'prefill', 'decode', 'single'))
        / nullIf(sumIf(group_gen_tok_cnt_delta, process_role IN ('engine', 'prefill', 'decode', 'single')), 0),
        0.0
    ) AS Float32) AS engine_request_generation_tokens_avg,
    maxIf(group_gen_tok_max,              process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_request_generation_tokens_max,
    sumIf(group_gen_tok_cnt_argMax,      process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_request_generation_tokens_count_argMax,
    sumIf(group_gen_tok_cnt_delta,        process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_request_generation_tokens_count_delta,
    sumIf(group_gen_tok_sum_argMax,       process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_request_generation_tokens_sum_argMax,
    sumIf(group_gen_tok_sum_delta,        process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_request_generation_tokens_sum_delta,
    anyIf(group_gen_tok_bucket_argMax,    process_role IN ('engine', 'prefill', 'decode', 'single')) AS engine_request_generation_tokens_bucket_argMax,
    mapFromArrays(
        sumMappedArraysIf(
            mapKeys(group_gen_tok_bucket_delta),
            mapValues(group_gen_tok_bucket_delta),
            process_role IN ('engine', 'prefill', 'decode', 'single')
        ).1,
        sumMappedArraysIf(
            mapKeys(group_gen_tok_bucket_delta),
            mapValues(group_gen_tok_bucket_delta),
            process_role IN ('engine', 'prefill', 'decode', 'single')
        ).2
    ) AS engine_request_generation_tokens_bucket_delta,

    -- ============================================================================
    -- 9. 标识控制
    -- ============================================================================
    0 AS sync_flag

FROM
(
    -- ============================================================================
    -- 【核心子查询】：按 group_id + process_role 组内去重 (并在此处彻底过滤 worker 节点)
    -- ============================================================================
    SELECT
        dws.window_time,
        dws.server_name,
        dws.modelinstance_id,
        dws.process_role,
        dws.group_id,

        -- ------------------------------------------------------------------------
        -- A. 实例元数据透传
        -- ------------------------------------------------------------------------
        any(dws.modelinstance_name) AS modelinstance_name,
        any(dws.framework_type)     AS framework_type,
        any(dws.framework_version)  AS framework_version,
        any(dws.deploy_info)        AS deploy_info,
        any(dws.model_name)         AS model_name,
        any(dws.model_id)           AS model_id,
        -- 子查询内聚合：将当前 group_id 下的所有 node_id/ip 进行节点维度的去重聚合
        arrayDistinct(groupArray(dws.node_id)) AS group_node_ids,
        arrayDistinct(groupArray(dws.node_ip)) AS group_node_ips,
        any(dws.node_name)          AS node_name,
        any(dws.node_type)          AS node_type,
        any(dws.dev_mfr)            AS dev_mfr,
        any(dws.dev_model)          AS dev_model,
        any(dws.dev_status_code)    AS dev_status_code,
        any(dws.dev_status_name)    AS dev_status_name,

        -- ------------------------------------------------------------------------
        -- B. 物理增量与 Counter 快照组内去重
        -- ------------------------------------------------------------------------
        max(dws.request_received_total_argMax)   AS group_request_received_argMax,
        max(dws.request_received_total_delta)    AS group_request_received_delta,
        max(dws.request_failed_total_argMax)     AS group_request_failed_argMax,
        max(dws.request_failed_total_delta)      AS group_request_failed_delta,
        max(dws.request_success_total_argMax)    AS group_request_success_argMax,
        max(dws.request_success_total_delta)     AS group_request_success_delta,

        max(dws.num_preemptions_total_argMax)   AS group_num_preemptions_argMax,
        max(dws.num_preemptions_total_delta)    AS group_num_preemptions_delta,

        max(dws.prompt_tokens_total_argMax)     AS group_prompt_tokens_argMax,
        max(dws.prompt_tokens_total_delta)      AS group_prompt_tokens_delta,
        max(dws.generation_tokens_total_argMax) AS group_generation_tokens_argMax,
        max(dws.generation_tokens_total_delta)  AS group_generation_tokens_delta,

        -- ------------------------------------------------------------------------
        -- C. 时延及 Token 尺寸的 Count/Sum 组内去重
        -- ------------------------------------------------------------------------
        max(dws.time_to_first_token_seconds_count_argMax) AS group_ttft_count_argMax,
        max(dws.time_to_first_token_seconds_count_delta)  AS group_ttft_count_delta,
        max(dws.time_to_first_token_seconds_sum_argMax)   AS group_ttft_sum_argMax,
        max(dws.time_to_first_token_seconds_sum_delta)    AS group_ttft_sum_delta,

        max(dws.time_per_output_token_seconds_count_argMax) AS group_tpot_count_argMax,
        max(dws.time_per_output_token_seconds_count_delta)  AS group_tpot_count_delta,
        max(dws.time_per_output_token_seconds_sum_argMax)   AS group_tpot_sum_argMax,
        max(dws.time_per_output_token_seconds_sum_delta)    AS group_tpot_sum_delta,

        max(dws.e2e_request_latency_seconds_count_argMax) AS group_e2e_count_argMax,
        max(dws.e2e_request_latency_seconds_count_delta)  AS group_e2e_count_delta,
        max(dws.e2e_request_latency_seconds_sum_argMax)   AS group_e2e_sum_argMax,
        max(dws.e2e_request_latency_seconds_sum_delta)    AS group_e2e_sum_delta,

        max(dws.request_prompt_tokens_count_argMax) AS group_prompt_tok_cnt_argMax,
        max(dws.request_prompt_tokens_count_delta)  AS group_prompt_tok_cnt_delta,
        max(dws.request_prompt_tokens_sum_argMax)   AS group_prompt_tok_sum_argMax,
        max(dws.request_prompt_tokens_sum_delta)    AS group_prompt_tok_sum_delta,
        max(dws.request_prompt_tokens_max)          AS group_prompt_tok_max,

        max(dws.request_generation_tokens_count_argMax) AS group_gen_tok_cnt_argMax,
        max(dws.request_generation_tokens_count_delta)  AS group_gen_tok_cnt_delta,
        max(dws.request_generation_tokens_sum_argMax)   AS group_gen_tok_sum_argMax,
        max(dws.request_generation_tokens_sum_delta)    AS group_gen_tok_sum_delta,
        max(dws.request_generation_tokens_max)          AS group_gen_tok_max,

        -- ------------------------------------------------------------------------
        -- D. 直方图 Bucket 快照与增量透传
        -- ------------------------------------------------------------------------
        any(dws.time_to_first_token_seconds_bucket_argMax) AS group_ttft_bucket_argMax,
        argMax(dws.time_to_first_token_seconds_bucket_delta, dws.time_to_first_token_seconds_count_delta) AS group_ttft_bucket_delta,

        any(dws.time_per_output_token_seconds_bucket_argMax) AS group_tpot_bucket_argMax,
        argMax(dws.time_per_output_token_seconds_bucket_delta, dws.time_per_output_token_seconds_count_delta) AS group_tpot_bucket_delta,

        any(dws.e2e_request_latency_seconds_bucket_argMax) AS group_e2e_bucket_argMax,
        argMax(dws.e2e_request_latency_seconds_bucket_delta, dws.e2e_request_latency_seconds_count_delta) AS group_e2e_bucket_delta,

        any(dws.request_prompt_tokens_bucket_argMax) AS group_prompt_tok_bucket_argMax,
        argMax(dws.request_prompt_tokens_bucket_delta, dws.request_prompt_tokens_count_delta) AS group_prompt_tok_bucket_delta,

        any(dws.request_generation_tokens_bucket_argMax) AS group_gen_tok_bucket_argMax,
        argMax(dws.request_generation_tokens_bucket_delta, dws.request_generation_tokens_count_delta) AS group_gen_tok_bucket_delta,

        -- ------------------------------------------------------------------------
        -- E. 状态均值与吞吐峰值
        -- ------------------------------------------------------------------------
        avg(dws.inference_concurrent_requests_avg) AS group_concurrent_requests_avg,
        avg(dws.num_requests_running_avg)          AS group_num_requests_running_avg,
        avg(dws.num_requests_waiting_avg)          AS group_num_requests_waiting_avg,
        avg(dws.num_requests_swapped_avg)          AS group_num_requests_swapped_avg,

        avg(dws.avg_prompt_throughput_toks_per_s_avg)     AS group_prompt_throughput_avg,
        max(dws.avg_prompt_throughput_toks_per_s_delta)   AS group_prompt_throughput_delta,
        max(dws.avg_prompt_throughput_toks_per_s_max)     AS group_prompt_throughput_max,

        avg(dws.avg_generation_throughput_toks_per_s_avg) AS group_generation_throughput_avg,
        max(dws.avg_generation_throughput_toks_per_s_delta) AS group_generation_throughput_delta,
        max(dws.avg_generation_throughput_toks_per_s_max) AS group_generation_throughput_max,

        avg(dws.npu_cache_usage_perc_avg)      AS group_npu_cache_usage_avg,
        max(dws.npu_cache_usage_perc_max)      AS group_npu_cache_usage_max,
        avg(dws.cpu_cache_usage_perc_avg)      AS group_cpu_cache_usage_avg,
        max(dws.cpu_cache_usage_perc_max)      AS group_cpu_cache_usage_max,
        avg(dws.npu_prefix_cache_hit_rate_avg) AS group_prefix_cache_hit_rate_avg

    FROM modelmate_metric.t_mops__modelmetric_instance__5min_dws AS dws
    WHERE dws.window_time >= {start_time:DateTime}
      AND dws.window_time <  {end_time:DateTime}
      AND dws.sync_flag = 0
      AND dws.process_role != 'worker' -- 【关键新增】：彻底排除所有跨机/多卡的从属节点
    GROUP BY
        dws.window_time,
        dws.server_name,
        dws.modelinstance_id,
        dws.process_role,
        dws.group_id
)
GROUP BY
    window_time,
    server_name,
    modelinstance_id
