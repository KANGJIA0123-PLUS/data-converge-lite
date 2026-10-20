-- 5.2.4 模型实例汇聚 由t_mops__modelmetric_instance__day_dws -> t_mops__modelmetric_instance__month_dws
-- 按调度器给出的月窗口过滤，分组时间直接使用窗口起点。
-- 速率使用窗口实际经过时间；快照仍按源记录时间取最新值。

INSERT INTO modelmate_metric.t_mops__modelmetric_instance__month_dws (
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

    -- 6. TTFT (Time To First Token)
    time_to_first_token_seconds_argMax,
    time_to_first_token_seconds_avg,
    time_to_first_token_seconds_count_argMax,
    time_to_first_token_seconds_count_delta,
    time_to_first_token_seconds_sum_argMax,
    time_to_first_token_seconds_sum_delta,
    time_to_first_token_seconds_bucket_argMax,
    time_to_first_token_seconds_bucket_delta,

    -- 7. TPOT & E2E
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

    -- 8. Cache & Memory
    npu_cache_usage_perc_avg,
    npu_cache_usage_perc_max,
    cpu_cache_usage_perc_avg,
    cpu_cache_usage_perc_max,
    npu_prefix_cache_hit_rate_avg,

    -- 9. Prompt & Generation Tokens 分布
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

    -- 10. 标识控制
    sync_flag
)
SELECT
    -- ============================================================================
    -- 1. 时间粒度与维度分组 (按月截断，保持实例与角色隔离)
    -- ============================================================================
    toDateTime({start_time:UInt32}, {system_timezone:String})                         AS window_time,
    d.server_name,
    d.modelinstance_id,
    d.process_role,

    -- ============================================================================
    -- 2. 实例关联属性（锁住当月最后一天的最新快照）
    -- ============================================================================
    argMax(d.modelinstance_name, d.window_time)             AS modelinstance_name,
    argMax(d.framework_type, d.window_time)                  AS framework_type,
    argMax(d.framework_version, d.window_time)               AS framework_version,
    argMax(d.deploy_info, d.window_time)                    AS deploy_info,
    argMax(d.model_name, d.window_time)                     AS model_name,
    argMax(d.model_id, d.window_time)                       AS model_id,
    arrayDistinct(arrayFlatten(groupArray(d.node_ids)))   AS node_ids,
    arrayDistinct(arrayFlatten(groupArray(d.node_ips)))   AS node_ips,
    argMax(d.node_name, d.window_time)                      AS node_name,
    argMax(d.node_type, d.window_time)                      AS node_type,
    argMax(d.dev_mfr, d.window_time)                        AS dev_mfr,
    argMax(d.dev_model, d.window_time)                      AS dev_model,
    argMax(d.dev_status_code, d.window_time)                AS dev_status_code,
    argMax(d.dev_status_name, d.window_time)                AS dev_status_name,

    -- ============================================================================
    -- 3. Request Metrics: 全月物理请求量累加与月均 QPM
    -- ============================================================================
    argMax(d.request_received_total_argMax, d.window_time) AS request_received_total_argMax,
    sum(d.request_received_total_delta)                   AS request_received_total_delta,
    -- 动态范围 QPM：使用外部传入的 {start_time} 与 {end_time} 总分钟数作为分母
    cast(sum(d.request_received_total_delta)
        / nullIf(dateDiff('minute', toDateTime({start_time:UInt32}, 'UTC'), toDateTime({end_time:UInt32}, 'UTC')), 0) AS Float32) AS request_received_total_qpm_avg,

    argMax(d.request_failed_total_argMax, d.window_time)   AS request_failed_total_argMax,
    sum(d.request_failed_total_delta)                     AS request_failed_total_delta,

    argMax(d.request_success_total_argMax, d.window_time)  AS request_success_total_argMax,
    sum(d.request_success_total_delta)                    AS request_success_total_delta,

    -- 全月请求成功率与失败率 (%)
    cast(sum(d.request_success_total_delta)
        / nullIf(sum(d.request_received_total_delta), 0) * 100.0 AS Float32) AS request_success_rate_avg,
    cast(sum(d.request_failed_total_delta)
        / nullIf(sum(d.request_received_total_delta), 0) * 100.0 AS Float32) AS request_failed_rate_avg,

    -- ============================================================================
    -- 4. Concurrency & Queue Metrics: 全月平均并发与排队
    -- ============================================================================
    avg(d.inference_concurrent_requests_avg)              AS inference_concurrent_requests_avg,
    avg(d.num_requests_running_avg)                       AS num_requests_running_avg,
    avg(d.num_requests_waiting_avg)                       AS num_requests_waiting_avg,
    avg(d.num_requests_swapped_avg)                       AS num_requests_swapped_avg,

    argMax(d.num_preemptions_total_argMax, d.window_time)  AS num_preemptions_total_argMax,
    sum(d.num_preemptions_total_delta)                    AS num_preemptions_total_delta,

    -- ============================================================================
    -- 5. Token Totals & Throughput Rates: 全月 Token 总量与物理吞吐率
    -- ============================================================================
    argMax(d.prompt_tokens_total_argMax, d.window_time)     AS prompt_tokens_total_argMax,
    sum(d.prompt_tokens_total_delta)                      AS prompt_tokens_total_delta,

    argMax(d.generation_tokens_total_argMax, d.window_time) AS generation_tokens_total_argMax,
    sum(d.generation_tokens_total_delta)                  AS generation_tokens_total_delta,

    -- 动态范围物理吞吐率：使用外部传入的 {start_time} 与 {end_time} 总秒数作为分母
    cast(sum(d.prompt_tokens_total_delta)
        / nullIf(dateDiff('second', toDateTime({start_time:UInt32}, 'UTC'), toDateTime({end_time:UInt32}, 'UTC')), 0) AS Float32) AS prompt_tokens_total_delta_rate_avg,
    cast(sum(d.generation_tokens_total_delta)
        / nullIf(dateDiff('second', toDateTime({start_time:UInt32}, 'UTC'), toDateTime({end_time:UInt32}, 'UTC')), 0) AS Float32) AS generation_tokens_total_delta_rate_avg,
    cast((sum(d.prompt_tokens_total_delta) + sum(d.generation_tokens_total_delta))
        / nullIf(dateDiff('second', toDateTime({start_time:UInt32}, 'UTC'), toDateTime({end_time:UInt32}, 'UTC')), 0) AS Float32) AS total_throughput_rate,

    avg(d.avg_prompt_throughput_toks_per_s_avg)           AS avg_prompt_throughput_toks_per_s_avg,
    sum(d.avg_prompt_throughput_toks_per_s_delta)         AS avg_prompt_throughput_toks_per_s_delta,
    max(d.avg_prompt_throughput_toks_per_s_max)           AS avg_prompt_throughput_toks_per_s_max,

    avg(d.avg_generation_throughput_toks_per_s_avg)       AS avg_generation_throughput_toks_per_s_avg,
    sum(d.avg_generation_throughput_toks_per_s_delta)     AS avg_generation_throughput_toks_per_s_delta,
    max(d.avg_generation_throughput_toks_per_s_max)       AS avg_generation_throughput_toks_per_s_max,

    -- ============================================================================
    -- 6. TTFT: Time To First Token 全月时延加权重算与分布地图叠加
    -- ============================================================================
    argMax(d.time_to_first_token_seconds_argMax, d.window_time)       AS time_to_first_token_seconds_argMax,
    -- TTFT 全月加权重算
    cast(sum(d.time_to_first_token_seconds_sum_delta) / nullIf(sum(d.time_to_first_token_seconds_count_delta), 0) AS Float32) AS time_to_first_token_seconds_avg,
    argMax(d.time_to_first_token_seconds_count_argMax, d.window_time) AS time_to_first_token_seconds_count_argMax,
    sum(d.time_to_first_token_seconds_count_delta)             AS time_to_first_token_seconds_count_delta,
    argMax(d.time_to_first_token_seconds_sum_argMax, d.window_time)   AS time_to_first_token_seconds_sum_argMax,
    sum(d.time_to_first_token_seconds_sum_delta)               AS time_to_first_token_seconds_sum_delta,
    argMax(d.time_to_first_token_seconds_bucket_argMax, d.window_time) AS time_to_first_token_seconds_bucket_argMax,
    mapFromArrays(
        sumMap(
            mapKeys(d.time_to_first_token_seconds_bucket_delta),
            mapValues(d.time_to_first_token_seconds_bucket_delta)
        ).1,
        sumMap(
            mapKeys(d.time_to_first_token_seconds_bucket_delta),
            mapValues(d.time_to_first_token_seconds_bucket_delta)
        ).2
    ) AS time_to_first_token_seconds_bucket_delta,

    -- ============================================================================
    -- 7. TPOT & E2E: 全月时延加权重算与分布地图叠加 (`sumMap`)
    -- ============================================================================
    argMax(d.time_per_output_token_seconds_argMax, d.window_time)   AS time_per_output_token_seconds_argMax,
    -- TPOT 全月加权重算
    cast(sum(d.time_per_output_token_seconds_sum_delta)
        / nullIf(sum(d.time_per_output_token_seconds_count_delta), 0) AS Float32) AS time_per_output_token_seconds_avg,
    argMax(d.time_per_output_token_seconds_count_argMax, d.window_time) AS time_per_output_token_seconds_count_argMax,
    sum(d.time_per_output_token_seconds_count_delta)              AS time_per_output_token_seconds_count_delta,
    argMax(d.time_per_output_token_seconds_sum_argMax, d.window_time)   AS time_per_output_token_seconds_sum_argMax,
    sum(d.time_per_output_token_seconds_sum_delta)                AS time_per_output_token_seconds_sum_delta,
    argMax(d.time_per_output_token_seconds_bucket_argMax, d.window_time) AS time_per_output_token_seconds_bucket_argMax,
    mapFromArrays(
        sumMap(
            mapKeys(d.time_per_output_token_seconds_bucket_delta),
            mapValues(d.time_per_output_token_seconds_bucket_delta)
        ).1,
        sumMap(
            mapKeys(d.time_per_output_token_seconds_bucket_delta),
            mapValues(d.time_per_output_token_seconds_bucket_delta)
        ).2
    ) AS time_per_output_token_seconds_bucket_delta,

    argMax(d.e2e_request_latency_seconds_argMax, d.window_time)     AS e2e_request_latency_seconds_argMax,
    -- E2E 全月加权重算
    cast(sum(d.e2e_request_latency_seconds_sum_delta)
        / nullIf(sum(d.e2e_request_latency_seconds_count_delta), 0) AS Float32) AS e2e_request_latency_seconds_avg,
    argMax(d.e2e_request_latency_seconds_count_argMax, d.window_time)   AS e2e_request_latency_seconds_count_argMax,
    sum(d.e2e_request_latency_seconds_count_delta)                  AS e2e_request_latency_seconds_count_delta,
    argMax(d.e2e_request_latency_seconds_sum_argMax, d.window_time)     AS e2e_request_latency_seconds_sum_argMax,
    sum(d.e2e_request_latency_seconds_sum_delta)                    AS e2e_request_latency_seconds_sum_delta,
    argMax(d.e2e_request_latency_seconds_bucket_argMax, d.window_time) AS e2e_request_latency_seconds_bucket_argMax,
    mapFromArrays(
        sumMap(
            mapKeys(d.e2e_request_latency_seconds_bucket_delta),
            mapValues(d.e2e_request_latency_seconds_bucket_delta)
        ).1,
        sumMap(
            mapKeys(d.e2e_request_latency_seconds_bucket_delta),
            mapValues(d.e2e_request_latency_seconds_bucket_delta)
        ).2
    ) AS e2e_request_latency_seconds_bucket_delta,

    -- ============================================================================
    -- 8. Cache & Memory: 全月平均显存/Cache 水位与最高极值
    -- ============================================================================
    avg(d.npu_cache_usage_perc_avg)                       AS npu_cache_usage_perc_avg,
    max(d.npu_cache_usage_perc_max)                       AS npu_cache_usage_perc_max, -- 全月最高显存爆满峰值
    avg(d.cpu_cache_usage_perc_avg)                       AS cpu_cache_usage_perc_avg,
    max(d.cpu_cache_usage_perc_max)                       AS cpu_cache_usage_perc_max,
    avg(d.npu_prefix_cache_hit_rate_avg)                  AS npu_prefix_cache_hit_rate_avg,

    -- ============================================================================
    -- 9. Prompt & Generation Tokens 分布
    -- ============================================================================
    argMax(d.request_prompt_tokens_argMax, d.window_time)           AS request_prompt_tokens_argMax,
    cast(sum(d.request_prompt_tokens_sum_delta)
        / nullIf(sum(d.request_prompt_tokens_count_delta), 0) AS Float32) AS request_prompt_tokens_avg,
    max(d.request_prompt_tokens_max)                              AS request_prompt_tokens_max,
    argMax(d.request_prompt_tokens_count_argMax, d.window_time)     AS request_prompt_tokens_count_argMax,
    sum(d.request_prompt_tokens_count_delta)                      AS request_prompt_tokens_count_delta,
    argMax(d.request_prompt_tokens_sum_argMax, d.window_time)       AS request_prompt_tokens_sum_argMax,
    sum(d.request_prompt_tokens_sum_delta)                        AS request_prompt_tokens_sum_delta,
    argMax(d.request_prompt_tokens_bucket_argMax, d.window_time)    AS request_prompt_tokens_bucket_argMax,
    mapFromArrays(
        sumMap(
            mapKeys(d.request_prompt_tokens_bucket_delta),
            mapValues(d.request_prompt_tokens_bucket_delta)
        ).1,
        sumMap(
            mapKeys(d.request_prompt_tokens_bucket_delta),
            mapValues(d.request_prompt_tokens_bucket_delta)
        ).2
    ) AS request_prompt_tokens_bucket_delta,

    argMax(d.request_generation_tokens_argMax, d.window_time)       AS request_generation_tokens_argMax,
    cast(sum(d.request_generation_tokens_sum_delta)
        / nullIf(sum(d.request_generation_tokens_count_delta), 0) AS Float32) AS request_generation_tokens_avg,
    max(d.request_generation_tokens_max)                          AS request_generation_tokens_max,
    argMax(d.request_generation_tokens_count_argMax, d.window_time) AS request_generation_tokens_count_argMax,
    sum(d.request_generation_tokens_count_delta)                  AS request_generation_tokens_count_delta,
    argMax(d.request_generation_tokens_sum_argMax, d.window_time)   AS request_generation_tokens_sum_argMax,
    sum(d.request_generation_tokens_sum_delta)                    AS request_generation_tokens_sum_delta,
    argMax(d.request_generation_tokens_bucket_argMax, d.window_time) AS request_generation_tokens_bucket_argMax,
    mapFromArrays(
        sumMap(
            mapKeys(d.request_generation_tokens_bucket_delta),
            mapValues(d.request_generation_tokens_bucket_delta)
        ).1,
        sumMap(
            mapKeys(d.request_generation_tokens_bucket_delta),
            mapValues(d.request_generation_tokens_bucket_delta)
        ).2
    ) AS request_generation_tokens_bucket_delta,

    -- ============================================================================
    -- 10. 标识控制
    -- ============================================================================
    any(d.sync_flag)                                      AS sync_flag

FROM modelmate_metric.t_mops__modelmetric_instance__day_dws AS d
WHERE d.window_time >= toDateTime({start_time:UInt32}, 'UTC')
  AND d.window_time <  toDateTime({end_time:UInt32}, 'UTC')
  AND d.sync_flag = 0
GROUP BY
    toDateTime({start_time:UInt32}, {system_timezone:String}),
    d.server_name,
    d.modelinstance_id,
    d.process_role;
