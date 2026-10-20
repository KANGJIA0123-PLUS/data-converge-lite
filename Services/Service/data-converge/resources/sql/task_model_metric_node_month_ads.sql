-- 6.4 节点级别metric指标
-- 6.4.4 节点粒度 month Metric指标，由t_mops__modelmetric_hour__day_ads 根据节点ID独立汇聚即可
INSERT INTO modelmate_metric.t_mops__modelmetric_node__month_ads
(
    window_time,
    node_id,
    node_name,
    node_ip,
    node_type,
    dev_mfr,
    dev_model,
    dev_status_code,
    dev_status_name,
    request_received_total_delta,
    request_failed_total_delta,
    request_success_total_delta,
    prompt_tokens_total_delta,
    generation_tokens_total_delta,
    num_preemptions_total_delta,

    -- 2. 时延底层 Sum / Count 增量（用于更上一层周期精确加权）
    e2e_request_latency_seconds_sum_delta,
    e2e_request_latency_seconds_count_delta,
    time_to_first_token_seconds_sum_delta,
    time_to_first_token_seconds_count_delta,
    time_per_output_token_seconds_sum_delta,
    time_per_output_token_seconds_count_delta,

    -- 3. 复合、速率与加权平均指标
    generation_tokens_per_s_avg,
    generation_tokens_per_request_avg,
    e2e_latency_avg,
    prompt_tokens_per_s_avg,
    request_failed_rate,
    request_success_rate,
    request_received_total_qpm_avg,
    time_to_first_token_seconds_avg,
    time_per_output_token_seconds_avg,
    total_tokens_per_s_avg,

    sync_flag
)
SELECT
    toDateTime({start_time:UInt32}, {system_timezone:String}) AS window_time,
    source.node_id,

    argMax(source.node_name, source.window_time) AS node_name,
    source.node_ip,
    any(source.node_type) AS node_type,
    argMax(source.dev_mfr, source.window_time) AS dev_mfr,
    argMax(source.dev_model, source.window_time) AS dev_model,
    argMax(source.dev_status_code, source.window_time) AS dev_status_code,
    argMax(source.dev_status_name, source.window_time) AS dev_status_name,

    -- ============================================================================
    -- 1. 基础绝对增量：直接求和
    -- ============================================================================
    sum(source.request_received_total_delta)       AS request_received_total_delta,
    sum(source.request_failed_total_delta)         AS request_failed_total_delta,
    sum(source.request_success_total_delta)        AS request_success_total_delta,
    sum(source.prompt_tokens_total_delta)          AS prompt_tokens_total_delta,
    sum(source.generation_tokens_total_delta)      AS generation_tokens_total_delta,
    sum(source.num_preemptions_total_delta)        AS num_preemptions_total_delta,

    -- ============================================================================
    -- 2. 时延底层 Sum / Count 增量：直接求和
    -- ============================================================================
    sum(source.e2e_request_latency_seconds_sum_delta)   AS e2e_request_latency_seconds_sum_delta,
    sum(source.e2e_request_latency_seconds_count_delta) AS e2e_request_latency_seconds_count_delta,
    sum(source.time_to_first_token_seconds_sum_delta)   AS time_to_first_token_seconds_sum_delta,
    sum(source.time_to_first_token_seconds_count_delta) AS time_to_first_token_seconds_count_delta,
    sum(source.time_per_output_token_seconds_sum_delta)   AS time_per_output_token_seconds_sum_delta,
    sum(source.time_per_output_token_seconds_count_delta) AS time_per_output_token_seconds_count_delta,

    -- ============================================================================
    -- 3. 复合指标、速率与精确加权计算
    -- ============================================================================

    -- 节点每秒生成 tokens 平均速率（月内的天平均值再做平滑平均）
    cast(avg(source.generation_tokens_per_s_avg) AS UInt64) AS generation_tokens_per_s_avg,

    -- 每请求平均生成 tokens 数 = 月总生成 Token / 月总成功请求数
    cast(coalesce(sum(source.generation_tokens_total_delta)
         / nullIf(sum(source.request_success_total_delta), 0), 0.0) AS Float32) AS generation_tokens_per_request_avg,

    -- 精确加权端到端时延 = 月内时延总和增量 / 月内总请求计数增量
    cast(coalesce(sum(source.e2e_request_latency_seconds_sum_delta)
         / nullIf(sum(source.e2e_request_latency_seconds_count_delta), 0), 0.0) AS Float32) AS e2e_latency_avg,

    -- 节点每秒输入 tokens 平均速率
    cast(avg(source.prompt_tokens_per_s_avg) AS UInt64) AS prompt_tokens_per_s_avg,

    -- 精确请求失败率 (%) = 月总失败数 / 月总接收数 * 100
    cast(coalesce(sum(source.request_failed_total_delta)
         / nullIf(sum(source.request_received_total_delta), 0) * 100.0, 0.0) AS Float32) AS request_failed_rate,

    -- 精确请求成功率 (%) = 月总成功数 / 月总接收数 * 100
    cast(coalesce(sum(source.request_success_total_delta)
         / nullIf(sum(source.request_received_total_delta), 0) * 100.0, 0.0) AS Float32) AS request_success_rate,

    -- 每分钟请求数 (QPM) 平均值
    cast(avg(source.request_received_total_qpm_avg) AS Float32) AS request_received_total_qpm_avg,

    -- 精确 TTFT 加权平均 = 月内 TTFT 总和 / 月内 TTFT 计数
    cast(coalesce(sum(source.time_to_first_token_seconds_sum_delta)
         / nullIf(sum(source.time_to_first_token_seconds_count_delta), 0), 0.0) AS Float32) AS time_to_first_token_seconds_avg,

    -- 精确 TPOT 加权平均 = 月内 TPOT 总和 / 月内 TPOT 计数
    cast(coalesce(sum(source.time_per_output_token_seconds_sum_delta)
         / nullIf(sum(source.time_per_output_token_seconds_count_delta), 0), 0.0) AS Float32) AS time_per_output_token_seconds_avg,

    -- 平均每秒总 token 数
    cast(avg(source.total_tokens_per_s_avg) AS Float32) AS total_tokens_per_s_avg,

    any(source.sync_flag) AS sync_flag

FROM modelmate_metric.t_mops__modelmetric_node__day_ads AS source
WHERE source.window_time >= toDateTime({start_time:UInt32}, 'UTC')
  AND source.window_time < toDateTime({end_time:UInt32}, 'UTC')
GROUP BY
    toDateTime({start_time:UInt32}, {system_timezone:String}), -- 使用本次调度窗口的起点
    source.node_id,
    source.node_ip;
