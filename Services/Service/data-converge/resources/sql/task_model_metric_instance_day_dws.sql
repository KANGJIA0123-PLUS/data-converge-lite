-- 5.2.3 模型实例汇聚 由t_mops__modelmetric_instance__hour_dws -> t_mops__modelmetric_instance__day_dws
-- 无需内层查询了
INSERT INTO modelmate_metric.t_mops__modelmetric_instance__day_dws (
    -- 1. 时间粒度与维度分组
    window_time,
    server_name,
    modelinstance_id,
    process_role,

    -- 2. 实例关联属性
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

    -- 3. Request Metrics
    request_received_total_argMax,
    request_received_total_delta,
    request_received_total_qpm_avg,
    request_failed_total_argMax,
    request_failed_total_delta,
    request_success_total_argMax,
    request_success_total_delta,
    request_success_rate_avg,
    request_failed_rate_avg,

    -- 4. Concurrency & Queue Metrics
    inference_concurrent_requests_avg,
    num_requests_running_avg,
    num_requests_waiting_avg,
    num_requests_swapped_avg,
    num_preemptions_total_argMax,
    num_preemptions_total_delta,

    -- 5. Token Totals & Throughput Rates
    prompt_tokens_total_argMax,
    prompt_tokens_total_delta,
    generation_tokens_total_argMax,
    generation_tokens_total_delta,
    prompt_tokens_total_delta_rate_avg,
    generation_tokens_total_delta_rate_avg,
    total_throughput_rate,
    avg_prompt_throughput_toks_per_s_avg,
    avg_prompt_throughput_toks_per_s_delta,
    avg_prompt_throughput_toks_per_s_max,
    avg_generation_throughput_toks_per_s_avg,
    avg_generation_throughput_toks_per_s_delta,
    avg_generation_throughput_toks_per_s_max,

    time_to_first_token_seconds_argMax,
    time_to_first_token_seconds_avg,
    time_to_first_token_seconds_count_argMax,
    time_to_first_token_seconds_count_delta,
    time_to_first_token_seconds_sum_argMax,
    time_to_first_token_seconds_sum_delta,
    time_to_first_token_seconds_bucket_argMax,
    time_to_first_token_seconds_bucket_delta,

    -- 6. TPOT & E2E
    time_per_output_token_seconds_argMax,
    time_per_output_token_seconds_avg,
    time_per_output_token_seconds_count_argMax,
    time_per_output_token_seconds_count_delta,
    time_per_output_token_seconds_sum_argMax,
    time_per_output_token_seconds_sum_delta,
    time_per_output_token_seconds_bucket_argMax,
    time_per_output_token_seconds_bucket_delta,
    e2e_request_latency_seconds_argMax,
    e2e_request_latency_seconds_avg,
    e2e_request_latency_seconds_count_argMax,
    e2e_request_latency_seconds_count_delta,
    e2e_request_latency_seconds_sum_argMax,
    e2e_request_latency_seconds_sum_delta,
    e2e_request_latency_seconds_bucket_argMax,
    e2e_request_latency_seconds_bucket_delta,

    -- 7. Cache & Memory
    npu_cache_usage_perc_avg,
    npu_cache_usage_perc_max,
    cpu_cache_usage_perc_avg,
    cpu_cache_usage_perc_max,
    npu_prefix_cache_hit_rate_avg,

    -- 8. Prompt & Generation Tokens 分布
    request_prompt_tokens_argMax,
    request_prompt_tokens_avg,
    request_prompt_tokens_max,
    request_prompt_tokens_count_argMax,
    request_prompt_tokens_count_delta,
    request_prompt_tokens_sum_argMax,
    request_prompt_tokens_sum_delta,
    request_prompt_tokens_bucket_argMax,
    request_prompt_tokens_bucket_delta,
    request_generation_tokens_argMax,
    request_generation_tokens_avg,
    request_generation_tokens_max,
    request_generation_tokens_count_argMax,
    request_generation_tokens_count_delta,
    request_generation_tokens_sum_argMax,
    request_generation_tokens_sum_delta,
    request_generation_tokens_bucket_argMax,
    request_generation_tokens_bucket_delta,

    -- 9. 标识控制
    sync_flag
)
SELECT
    -- ============================================================================
    -- 1. 时间粒度与维度分组 (按天截断，保持实例与角色隔离)
    -- ============================================================================
    toStartOfDay(h.window_time)                           AS window_time,
    h.server_name,
    h.modelinstance_id,
    h.process_role,

    -- ============================================================================
    -- 2. 实例关联属性（取全天最后一个小时窗口的最新快照）
    -- ============================================================================
    argMax(h.modelinstance_name, h.window_time)             AS modelinstance_name,
    argMax(h.framework_type, h.window_time)                  AS framework_type,
    argMax(h.framework_version, h.window_time)               AS framework_version,
    argMax(h.deploy_info, h.window_time)                    AS deploy_info,
    argMax(h.model_name, h.window_time)                     AS model_name,
    argMax(h.model_id, h.window_time)                       AS model_id,
    arrayDistinct(arrayFlatten(groupArray(h.node_ids)))   AS node_ids,
    arrayDistinct(arrayFlatten(groupArray(h.node_ips)))   AS node_ips,
    argMax(h.node_name, h.window_time)                      AS node_name,
    argMax(h.node_type, h.window_time)                      AS node_type,
    argMax(h.dev_mfr, h.window_time)                        AS dev_mfr,
    argMax(h.dev_model, h.window_time)                      AS dev_model,
    argMax(h.dev_status_code, h.window_time)                AS dev_status_code,
    argMax(h.dev_status_name, h.window_time)                AS dev_status_name,

    -- ============================================================================
    -- 3. Request Metrics: 物理请求量累加与 QPM (使用动态时间区间计算分钟数)
    -- ============================================================================
    argMax(h.request_received_total_argMax, h.window_time)  AS request_received_total_argMax,
    sum(h.request_received_total_delta)                   AS request_received_total_delta,
    -- 【动态化】：QPM 分母改为由动态时间差计算 (秒数 / 60.0)
    cast(sum(h.request_received_total_delta) / (dateDiff('second', {start_time:DateTime}, {end_time:DateTime}) / 60.0) AS Float32) AS request_received_total_qpm_avg,

    argMax(h.request_failed_total_argMax, h.window_time)    AS request_failed_total_argMax,
    sum(h.request_failed_total_delta)                     AS request_failed_total_delta,

    argMax(h.request_success_total_argMax, h.window_time)   AS request_success_total_argMax,
    sum(h.request_success_total_delta)                    AS request_success_total_delta,

    -- 全天请求成功率与失败率 (%)
    cast(sum(h.request_success_total_delta) / nullIf(sum(h.request_received_total_delta), 0) * 100.0 AS Float32)   AS request_success_rate_avg,
    cast(sum(h.request_failed_total_delta) / nullIf(sum(h.request_received_total_delta), 0) * 100.0 AS Float32)    AS request_failed_rate_avg,

    -- ============================================================================
    -- 4. Concurrency & Queue Metrics: 全天平均并发与排队
    -- ============================================================================
    avg(h.inference_concurrent_requests_avg)              AS inference_concurrent_requests_avg,
    avg(h.num_requests_running_avg)                       AS num_requests_running_avg,
    avg(h.num_requests_waiting_avg)                       AS num_requests_waiting_avg,
    avg(h.num_requests_swapped_avg)                       AS num_requests_swapped_avg,

    argMax(h.num_preemptions_total_argMax, h.window_time)   AS num_preemptions_total_argMax,
    sum(h.num_preemptions_total_delta)                    AS num_preemptions_total_delta,

    -- ============================================================================
    -- 5. Token Totals & Throughput Rates: Token 总量与物理吞吐率 (使用动态时间差计算秒数)
    -- ============================================================================
    argMax(h.prompt_tokens_total_argMax, h.window_time)     AS prompt_tokens_total_argMax,
    sum(h.prompt_tokens_total_delta)                      AS prompt_tokens_total_delta,

    argMax(h.generation_tokens_total_argMax, h.window_time) AS generation_tokens_total_argMax,
    sum(h.generation_tokens_total_delta)                  AS generation_tokens_total_delta,

    -- 【动态化】：Token 速率分母改为使用动态时间差 (dateDiff)
    cast(sum(h.prompt_tokens_total_delta) / toFloat32(dateDiff('second', {start_time:DateTime}, {end_time:DateTime})) AS Float32) AS prompt_tokens_total_delta_rate_avg,
    cast(sum(h.generation_tokens_total_delta) / toFloat32(dateDiff('second', {start_time:DateTime}, {end_time:DateTime})) AS Float32) AS generation_tokens_total_delta_rate_avg,
    cast((sum(h.prompt_tokens_total_delta) + sum(h.generation_tokens_total_delta)) / toFloat32(dateDiff('second', {start_time:DateTime}, {end_time:DateTime})) AS Float32) AS total_throughput_rate,

    avg(h.avg_prompt_throughput_toks_per_s_avg)           AS avg_prompt_throughput_toks_per_s_avg,
    sum(h.avg_prompt_throughput_toks_per_s_delta)         AS avg_prompt_throughput_toks_per_s_delta,
    max(h.avg_prompt_throughput_toks_per_s_max)           AS avg_prompt_throughput_toks_per_s_max,

    avg(h.avg_generation_throughput_toks_per_s_avg)       AS avg_generation_throughput_toks_per_s_avg,
    sum(h.avg_generation_throughput_toks_per_s_delta)     AS avg_generation_throughput_toks_per_s_delta,
    max(h.avg_generation_throughput_toks_per_s_max)       AS avg_generation_throughput_toks_per_s_max,
    
    -- ============================================================================
    -- TTFT
    -- ============================================================================
    argMax(h.time_to_first_token_seconds_argMax, h.window_time)       AS time_to_first_token_seconds_argMax,
    cast(
        coalesce(
            sum(h.time_to_first_token_seconds_sum_delta)
            / nullIf(sum(h.time_to_first_token_seconds_count_delta), 0),
            0.0
        )
        AS Float32
    ) AS time_to_first_token_seconds_avg,
    argMax(h.time_to_first_token_seconds_count_argMax, h.window_time) AS time_to_first_token_seconds_count_argMax,
    sum(h.time_to_first_token_seconds_count_delta)             AS time_to_first_token_seconds_count_delta,
    argMax(h.time_to_first_token_seconds_sum_argMax, h.window_time)   AS time_to_first_token_seconds_sum_argMax,
    sum(h.time_to_first_token_seconds_sum_delta)               AS time_to_first_token_seconds_sum_delta,
    argMax(h.time_to_first_token_seconds_bucket_argMax, h.window_time)AS time_to_first_token_seconds_bucket_argMax,
    mapFromArrays(
        sumMap(
            mapKeys(h.time_to_first_token_seconds_bucket_delta),
            mapValues(h.time_to_first_token_seconds_bucket_delta)
        ).1,
        sumMap(
            mapKeys(h.time_to_first_token_seconds_bucket_delta),
            mapValues(h.time_to_first_token_seconds_bucket_delta)
        ).2
    ) AS time_to_first_token_seconds_bucket_delta,

    -- ============================================================================
    -- 6. TPOT & E2E: 全天时延加权重算与分布地图叠加 (`sumMap`)
    -- ============================================================================
    argMax(h.time_per_output_token_seconds_argMax, h.window_time)    AS time_per_output_token_seconds_argMax,
    -- TPOT 全天加权重算
    cast(sum(h.time_per_output_token_seconds_sum_delta) / nullIf(sum(h.time_per_output_token_seconds_count_delta), 0) AS Float32) AS time_per_output_token_seconds_avg,
    argMax(h.time_per_output_token_seconds_count_argMax, h.window_time)     AS time_per_output_token_seconds_count_argMax,
    sum(h.time_per_output_token_seconds_count_delta)      AS time_per_output_token_seconds_count_delta,
    argMax(h.time_per_output_token_seconds_sum_argMax, h.window_time)       AS time_per_output_token_seconds_sum_argMax,
    sum(h.time_per_output_token_seconds_sum_delta)        AS time_per_output_token_seconds_sum_delta,
    argMax(h.time_per_output_token_seconds_bucket_argMax, h.window_time) AS time_per_output_token_seconds_bucket_argMax,
    mapFromArrays(
        sumMap(
            mapKeys(h.time_per_output_token_seconds_bucket_delta),
            mapValues(h.time_per_output_token_seconds_bucket_delta)
        ).1,
        sumMap(
            mapKeys(h.time_per_output_token_seconds_bucket_delta),
            mapValues(h.time_per_output_token_seconds_bucket_delta)
        ).2
    ) AS time_per_output_token_seconds_bucket_delta,

    argMax(h.e2e_request_latency_seconds_argMax, h.window_time)    AS e2e_request_latency_seconds_argMax,
    -- E2E 全天加权重算
    cast(sum(h.e2e_request_latency_seconds_sum_delta) / nullIf(sum(h.e2e_request_latency_seconds_count_delta), 0) AS Float32) AS e2e_request_latency_seconds_avg,
    argMax(h.e2e_request_latency_seconds_count_argMax, h.window_time)   AS e2e_request_latency_seconds_count_argMax,
    sum(h.e2e_request_latency_seconds_count_delta)        AS e2e_request_latency_seconds_count_delta,
    argMax(h.e2e_request_latency_seconds_sum_argMax, h.window_time)   AS e2e_request_latency_seconds_sum_argMax,
    sum(h.e2e_request_latency_seconds_sum_delta)          AS e2e_request_latency_seconds_sum_delta,
    argMax(h.e2e_request_latency_seconds_bucket_argMax, h.window_time) AS e2e_request_latency_seconds_bucket_argMax,
    mapFromArrays(
        sumMap(
            mapKeys(h.e2e_request_latency_seconds_bucket_delta),
            mapValues(h.e2e_request_latency_seconds_bucket_delta)
        ).1,
        sumMap(
            mapKeys(h.e2e_request_latency_seconds_bucket_delta),
            mapValues(h.e2e_request_latency_seconds_bucket_delta)
        ).2
    ) AS e2e_request_latency_seconds_bucket_delta,

    -- ============================================================================
    -- 7. Cache & Memory: 全天平均显存/Cache 水位与最高极值
    -- ============================================================================
    avg(h.npu_cache_usage_perc_avg)                       AS npu_cache_usage_perc_avg,
    max(h.npu_cache_usage_perc_max)                       AS npu_cache_usage_perc_max, -- 全天最高显存爆满峰值
    avg(h.cpu_cache_usage_perc_avg)                       AS cpu_cache_usage_perc_avg,
    max(h.cpu_cache_usage_perc_max)                       AS cpu_cache_usage_perc_max,
    avg(h.npu_prefix_cache_hit_rate_avg)                  AS npu_prefix_cache_hit_rate_avg,

    -- ============================================================================
    -- 8. Prompt & Generation Tokens 分布
    -- ============================================================================
    argMax(h.request_prompt_tokens_argMax, h.window_time)                   AS request_prompt_tokens_argMax,
    cast(sum(h.request_prompt_tokens_sum_delta) / nullIf(sum(h.request_prompt_tokens_count_delta), 0) AS Float32) AS request_prompt_tokens_avg,
    max(h.request_prompt_tokens_max)                      AS request_prompt_tokens_max,
    argMax(h.request_prompt_tokens_count_argMax, h.window_time)             AS request_prompt_tokens_count_argMax,
    sum(h.request_prompt_tokens_count_delta)              AS request_prompt_tokens_count_delta,
    argMax(h.request_prompt_tokens_sum_argMax, h.window_time)                AS request_prompt_tokens_sum_argMax,
    sum(h.request_prompt_tokens_sum_delta)                AS request_prompt_tokens_sum_delta,
    argMax(h.request_prompt_tokens_bucket_argMax, h.window_time) AS request_prompt_tokens_bucket_argMax,
    mapFromArrays(
        sumMap(
            mapKeys(h.request_prompt_tokens_bucket_delta),
            mapValues(h.request_prompt_tokens_bucket_delta)
        ).1,
        sumMap(
            mapKeys(h.request_prompt_tokens_bucket_delta),
            mapValues(h.request_prompt_tokens_bucket_delta)
        ).2
    ) AS request_prompt_tokens_bucket_delta,

    argMax(h.request_generation_tokens_argMax, h.window_time)               AS request_generation_tokens_argMax,
    cast(sum(h.request_generation_tokens_sum_delta) / nullIf(sum(h.request_generation_tokens_count_delta), 0) AS Float32) AS request_generation_tokens_avg,
    max(h.request_generation_tokens_max)                  AS request_generation_tokens_max,
    argMax(h.request_generation_tokens_count_argMax, h.window_time)         AS request_generation_tokens_count_argMax,
    sum(h.request_generation_tokens_count_delta)          AS request_generation_tokens_count_delta,
    argMax(h.request_generation_tokens_sum_argMax, h.window_time)           AS request_generation_tokens_sum_argMax,
    sum(h.request_generation_tokens_sum_delta)            AS request_generation_tokens_sum_delta,
    argMax(h.request_generation_tokens_bucket_argMax, h.window_time) AS request_generation_tokens_bucket_argMax,
    mapFromArrays(
        sumMap(
            mapKeys(h.request_generation_tokens_bucket_delta),
            mapValues(h.request_generation_tokens_bucket_delta)
        ).1,
        sumMap(
            mapKeys(h.request_generation_tokens_bucket_delta),
            mapValues(h.request_generation_tokens_bucket_delta)
        ).2
    ) AS request_generation_tokens_bucket_delta,

    -- ============================================================================
    -- 9. 标识控制
    -- ============================================================================
    any(h.sync_flag)                                      AS sync_flag

FROM modelmate_metric.t_mops__modelmetric_instance__hour_dws AS h
WHERE h.window_time >= {start_time:DateTime}
  AND h.window_time <  {end_time:DateTime}
  AND h.sync_flag = 0
GROUP BY
    toStartOfDay(h.window_time),
    h.server_name,
    h.modelinstance_id,
    h.process_role;
