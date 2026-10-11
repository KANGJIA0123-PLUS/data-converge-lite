-- 5.2.1 模型实例汇聚 由t_mops_modelmetric_realtime_dwd -> t_mops__modelmetric_instance__5min_dws

INSERT INTO modelmate_metric.t_mops__modelmetric_instance__5min_dws
SELECT
    -- ============================================================================
    -- 1. 时间粒度与维度分组 (Group By Keys)
    -- ============================================================================
    toStartOfFiveMinutes(r.metric_datetime)              AS window_time,
    r.server_name,
    r.modelinstance_id,
    r.container_id,

    -- ============================================================================
    -- 2. 实例关联属性（取窗口内最新快照 argMax）
    -- ============================================================================
    -- process_role 与 group_id 采用 argMax 获取最新状态，防止中间状态变化导致 GROUP BY 拆行
    argMax(r.process_role, r.metric_datetime)              AS process_role,
    argMax(r.group_id, r.metric_datetime)                  AS group_id,
    argMax(r.modelinstance_name, r.metric_datetime)        AS modelinstance_name,
    argMax(r.framework_type, r.metric_datetime)             AS framework_type,
    argMax(r.framework_version, r.metric_datetime)          AS framework_version,
    argMax(r.deploy_info, r.metric_datetime)               AS deploy_info,
    argMax(r.model_name, r.metric_datetime)                AS model_name,
    argMax(r.model_id, r.metric_datetime)                  AS model_id,
    argMax(r.node_id, r.metric_datetime)                   AS node_id,
    argMax(r.node_ip, r.metric_datetime)                   AS node_ip,
    argMax(r.node_name, r.metric_datetime)                 AS node_name,
    argMax(r.node_type, r.metric_datetime)                 AS node_type,

    argMax(r.dev_mfr, r.metric_datetime)                   AS dev_mfr,
    argMax(r.dev_model, r.metric_datetime)                 AS dev_model,
    argMax(r.dev_status_code, r.metric_datetime)           AS dev_status_code,
    argMax(r.dev_status_name, r.metric_datetime)           AS dev_status_name,

    -- ============================================================================
    -- 3. Request Metrics: 请求数与 QPM 指标
    -- ============================================================================
    argMax(r.request_received_total, r.metric_datetime)     AS request_received_total_argMax,
    sum(r.request_received_total_delta)                  AS request_received_total_delta,
    -- 【动态化】：QPM 改为通过动态分钟数计算 (区间秒数 / 60.0)
    cast(sum(r.request_received_total_delta) / (dateDiff('second', {start_time:DateTime}, {end_time:DateTime}) / 60.0) AS Float32) AS request_received_total_qpm_avg,

    argMax(r.request_failed_total, r.metric_datetime)       AS request_failed_total_argMax,
    sum(r.request_failed_total_delta)                    AS request_failed_total_delta,

    argMax(r.request_received_total - r.request_failed_total, r.metric_datetime) AS request_success_total_argMax,
    sum(r.request_received_total_delta) - sum(r.request_failed_total_delta)   AS request_success_total_delta,

    -- 请求成功率与失败率 (%)
    cast(
        coalesce(
            (sum(r.request_received_total_delta) - sum(r.request_failed_total_delta))
            / nullIf(sum(r.request_received_total_delta), 0)
            * 100.0,
            0.0
        )
        AS Float32
    ) AS request_success_rate_avg,
    cast(
        coalesce(
            sum(r.request_failed_total_delta)
            / nullIf(sum(r.request_received_total_delta), 0)
            * 100.0,
            0.0
        )
        AS Float32
    ) AS request_failed_rate_avg,

    -- ============================================================================
    -- 4. Concurrency & Queue Metrics: 并发请求、状态与抢占指标
    -- ============================================================================
    avg(r.num_requests_running) + avg(r.num_requests_waiting) + avg(r.num_requests_swapped) AS inference_concurrent_requests_avg,
    avg(r.num_requests_running)                          AS num_requests_running_avg,
    avg(r.num_requests_waiting)                          AS num_requests_waiting_avg,
    avg(r.num_requests_swapped)                          AS num_requests_swapped_avg,

    argMax(r.num_preemptions_total, r.metric_datetime)     AS num_preemptions_total_argMax,
    sum(r.num_preemptions_total_delta)                   AS num_preemptions_total_delta,

    -- ============================================================================
    -- 5. Token Totals & Throughput Rates: Token 总量与吞吐率指标
    -- ============================================================================
    argMax(r.prompt_tokens_total, r.metric_datetime)       AS prompt_tokens_total_argMax,
    sum(r.prompt_tokens_total_delta)                     AS prompt_tokens_total_delta,

    argMax(r.generation_tokens_total, r.metric_datetime)   AS generation_tokens_total_argMax,
    sum(r.generation_tokens_total_delta)                 AS generation_tokens_total_delta,

    -- 【动态化】：Token 速率分母改为动态秒数 (dateDiff)
    cast(sum(r.prompt_tokens_total_delta) / toFloat32(dateDiff('second', {start_time:DateTime}, {end_time:DateTime})) AS Float32)     AS prompt_tokens_total_delta_rate_avg,
    cast(sum(r.generation_tokens_total_delta) / toFloat32(dateDiff('second', {start_time:DateTime}, {end_time:DateTime})) AS Float32) AS generation_tokens_total_delta_rate_avg,
    cast((sum(r.prompt_tokens_total_delta) + sum(r.generation_tokens_total_delta)) / toFloat32(dateDiff('second', {start_time:DateTime}, {end_time:DateTime})) AS Float32) AS total_throughput_rate,

    -- 引擎报出的参考吞吐率与增量
    avg(r.avg_prompt_throughput_toks_per_s)              AS avg_prompt_throughput_toks_per_s_avg,
    sum(r.avg_prompt_throughput_toks_per_s_delta)        AS avg_prompt_throughput_toks_per_s_delta,
    max(r.avg_prompt_throughput_toks_per_s)              AS avg_prompt_throughput_toks_per_s_max,

    avg(r.avg_generation_throughput_toks_per_s)          AS avg_generation_throughput_toks_per_s_avg,
    sum(r.avg_generation_throughput_toks_per_s_delta)    AS avg_generation_throughput_toks_per_s_delta,
    max(r.avg_generation_throughput_toks_per_s)          AS avg_generation_throughput_toks_per_s_max,

    -- ============================================================================
    -- 6. TTFT: Time To First Token (首 Token 延迟)
    -- ============================================================================
    argMax(r.time_to_first_token_seconds, r.metric_datetime)       AS time_to_first_token_seconds_argMax,
    cast(
        coalesce(
            sum(r.time_to_first_token_seconds_sum_delta)
            / nullIf(sum(r.time_to_first_token_seconds_count_delta), 0),
            0.0
        )
        AS Float32
    ) AS time_to_first_token_seconds_avg,
    argMax(r.time_to_first_token_seconds_count, r.metric_datetime) AS time_to_first_token_seconds_count_argMax,
    sum(r.time_to_first_token_seconds_count_delta)             AS time_to_first_token_seconds_count_delta,
    argMax(r.time_to_first_token_seconds_sum, r.metric_datetime)   AS time_to_first_token_seconds_sum_argMax,
    sum(r.time_to_first_token_seconds_sum_delta)               AS time_to_first_token_seconds_sum_delta,
    argMax(r.time_to_first_token_seconds_bucket, r.metric_datetime)AS time_to_first_token_seconds_bucket_argMax,
    mapFromArrays(
        sumMap(
            mapKeys(r.time_to_first_token_seconds_bucket_delta),
            mapValues(r.time_to_first_token_seconds_bucket_delta)
        ).1,
        sumMap(
            mapKeys(r.time_to_first_token_seconds_bucket_delta),
            mapValues(r.time_to_first_token_seconds_bucket_delta)
        ).2
    ) AS time_to_first_token_seconds_bucket_delta,

    -- ============================================================================
    -- 7. TPOT: Time Per Output Token (每输出 Token 延迟 / Decode 逐字时延)
    -- ============================================================================
    argMax(r.time_per_output_token_seconds, r.metric_datetime)       AS time_per_output_token_seconds_argMax,
    cast(
        coalesce(
            sum(r.time_per_output_token_seconds_sum_delta)
            / nullIf(sum(r.time_per_output_token_seconds_count_delta), 0),
            0.0
        )
        AS Float32
    ) AS time_per_output_token_seconds_avg,
    argMax(r.time_per_output_token_seconds_count, r.metric_datetime) AS time_per_output_token_seconds_count_argMax,
    sum(r.time_per_output_token_seconds_count_delta)             AS time_per_output_token_seconds_count_delta,
    argMax(r.time_per_output_token_seconds_sum, r.metric_datetime)   AS time_per_output_token_seconds_sum_argMax,
    sum(r.time_per_output_token_seconds_sum_delta)               AS time_per_output_token_seconds_sum_delta,
    argMax(r.time_per_output_token_seconds_bucket, r.metric_datetime)AS time_per_output_token_seconds_bucket_argMax,
    mapFromArrays(
        sumMap(
            mapKeys(r.time_per_output_token_seconds_bucket_delta),
            mapValues(r.time_per_output_token_seconds_bucket_delta)
        ).1,
        sumMap(
            mapKeys(r.time_per_output_token_seconds_bucket_delta),
            mapValues(r.time_per_output_token_seconds_bucket_delta)
        ).2
    ) AS time_per_output_token_seconds_bucket_delta,

    -- ============================================================================
    -- 8. E2E: End-to-End Request Latency (端到端请求延迟)
    -- ============================================================================
    argMax(r.e2e_request_latency_seconds, r.metric_datetime)       AS e2e_request_latency_seconds_argMax,
    cast(
        coalesce(
            sum(r.e2e_request_latency_seconds_sum_delta)
            / nullIf(sum(r.e2e_request_latency_seconds_count_delta), 0),
            0.0
        )
        AS Float32
    ) AS e2e_request_latency_seconds_avg,
    argMax(r.e2e_request_latency_seconds_count, r.metric_datetime) AS e2e_request_latency_seconds_count_argMax,
    sum(r.e2e_request_latency_seconds_count_delta)             AS e2e_request_latency_seconds_count_delta,
    argMax(r.e2e_request_latency_seconds_sum, r.metric_datetime)   AS e2e_request_latency_seconds_sum_argMax,
    sum(r.e2e_request_latency_seconds_sum_delta)               AS e2e_request_latency_seconds_sum_delta,
    argMax(r.e2e_request_latency_seconds_bucket, r.metric_datetime)AS e2e_request_latency_seconds_bucket_argMax,
    mapFromArrays(
        sumMap(
            mapKeys(r.e2e_request_latency_seconds_bucket_delta),
            mapValues(r.e2e_request_latency_seconds_bucket_delta)
        ).1,
        sumMap(
            mapKeys(r.e2e_request_latency_seconds_bucket_delta),
            mapValues(r.e2e_request_latency_seconds_bucket_delta)
        ).2
    ) AS e2e_request_latency_seconds_bucket_delta,

    -- ============================================================================
    -- 9. Cache & Memory Metrics: NPU/CPU Cache 使用率与前缀缓存命中率
    -- ============================================================================
    avg(r.npu_cache_usage_perc)                          AS npu_cache_usage_perc_avg,
    max(r.npu_cache_usage_perc)                          AS npu_cache_usage_perc_max,
    avg(r.cpu_cache_usage_perc)                          AS cpu_cache_usage_perc_avg,
    max(r.cpu_cache_usage_perc)                          AS cpu_cache_usage_perc_max,
    avg(r.npu_prefix_cache_hit_rate)                     AS npu_prefix_cache_hit_rate_avg,

    -- ============================================================================
    -- 10. Request Prompt Tokens: 输入提示词 Token 尺寸与分布指标
    -- ============================================================================
    argMax(r.request_prompt_tokens, r.metric_datetime)     AS request_prompt_tokens_argMax,
    cast(
        coalesce(
            sum(r.request_prompt_tokens_sum_delta)
            / nullIf(sum(r.request_prompt_tokens_count_delta), 0),
            0.0
        )
        AS Float32
    ) AS request_prompt_tokens_avg,
    max(r.request_prompt_tokens)                         AS request_prompt_tokens_max,
    argMax(r.request_prompt_tokens_count, r.metric_datetime) AS request_prompt_tokens_count_argMax,
    sum(r.request_prompt_tokens_count_delta)             AS request_prompt_tokens_count_delta,
    argMax(r.request_prompt_tokens_sum, r.metric_datetime) AS request_prompt_tokens_sum_argMax,
    sum(r.request_prompt_tokens_sum_delta)               AS request_prompt_tokens_sum_delta,
    argMax(r.request_prompt_tokens_bucket, r.metric_datetime) AS request_prompt_tokens_bucket_argMax,
    mapFromArrays(
        sumMap(
            mapKeys(r.request_prompt_tokens_bucket_delta),
            mapValues(r.request_prompt_tokens_bucket_delta)
        ).1,
        sumMap(
            mapKeys(r.request_prompt_tokens_bucket_delta),
            mapValues(r.request_prompt_tokens_bucket_delta)
        ).2
    ) AS request_prompt_tokens_bucket_delta,

    -- ============================================================================
    -- 11. Request Generation Tokens: 输出生成 Token 尺寸与分布指标
    -- ============================================================================
    argMax(r.request_generation_tokens, r.metric_datetime) AS request_generation_tokens_argMax,
    cast(
        coalesce(
            sum(r.request_generation_tokens_sum_delta)
            / nullIf(sum(r.request_generation_tokens_count_delta), 0),
            0.0
        )
        AS Float32
    ) AS request_generation_tokens_avg,
    max(r.request_generation_tokens)                     AS request_generation_tokens_max,
    argMax(r.request_generation_tokens_count, r.metric_datetime) AS request_generation_tokens_count_argMax,
    sum(r.request_generation_tokens_count_delta)         AS request_generation_tokens_count_delta,
    argMax(r.request_generation_tokens_sum, r.metric_datetime) AS request_generation_tokens_sum_argMax,
    sum(r.request_generation_tokens_sum_delta)           AS request_generation_tokens_sum_delta,
    argMax(r.request_generation_tokens_bucket, r.metric_datetime) AS request_generation_tokens_bucket_argMax,
    mapFromArrays(
        sumMap(
            mapKeys(r.request_generation_tokens_bucket_delta),
            mapValues(r.request_generation_tokens_bucket_delta)
        ).1,
        sumMap(
            mapKeys(r.request_generation_tokens_bucket_delta),
            mapValues(r.request_generation_tokens_bucket_delta)
        ).2
    ) AS request_generation_tokens_bucket_delta,

    -- ============================================================================
    -- 12. 标识控制
    -- ============================================================================
    any(r.sync_flag)                                         AS sync_flag

FROM modelmate_metric.t_mops_modelmetric_realtime_dwd AS r
-- 建议按具体调度批次传入时间过滤条件，例如汇聚上个5分钟窗口的数据：
WHERE r.metric_datetime >= {start_time:DateTime}
  AND r.metric_datetime <  {end_time:DateTime}
  AND r.sync_flag = 0 -- 只针对探针实时上报的数据做 5 分钟汇聚
GROUP BY
    window_time,
    r.server_name,
    r.modelinstance_id,
    r.container_id;
