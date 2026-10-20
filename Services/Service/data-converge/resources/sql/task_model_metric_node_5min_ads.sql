-- 6.4 节点级别metric指标
-- 6.4.1 节点粒度 5min Metric指标，由t_mops__modelmetric_instance__5min_dws 根据节点ID独立汇聚即可
INSERT INTO modelmate_metric.t_mops__modelmetric_node__5min_ads
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

    -- 时延底层 Sum / Count 增量（用于小时级精确加权）
    e2e_request_latency_seconds_sum_delta,
    e2e_request_latency_seconds_count_delta,
    time_to_first_token_seconds_sum_delta,
    time_to_first_token_seconds_count_delta,
    time_per_output_token_seconds_sum_delta,
    time_per_output_token_seconds_count_delta,
    -- 新增指标
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
    source.window_time,
    source.node_id,

    -- 节点属性快照
    argMax(source.node_name, source.window_time) AS node_name,
    source.node_ip,
    any(source.node_type) AS node_type,
    argMax(source.dev_mfr, source.window_time) AS dev_mfr,
    argMax(source.dev_model, source.window_time) AS dev_model,
    argMax(source.dev_status_code, source.window_time) AS dev_status_code,
    argMax(source.dev_status_name, source.window_time) AS dev_status_name,

    -- 核心推理指标（应用 PD 分离过滤与求和）
    -- 1. 接收请求数：PD分离场景下由 P 节点或无角色实例贡献，排除纯 D 节点
    sumIf(source.request_received_total_delta, source.process_role != 'decode') AS request_received_total_delta,

    -- 2. 失败请求数：全机或 P+D 共同承担，全量求和
    sum(source.request_failed_total_delta) AS request_failed_total_delta,

    -- 3. 成功请求数增量 = 接收-失败
    sumIf(source.request_received_total_delta, source.process_role != 'decode') - sum(source.request_failed_total_delta) AS request_success_total_delta,

    -- 4. Prompt Tokens：由 P 节点贡献，排除纯 D 节点
    sumIf(source.prompt_tokens_total_delta, source.process_role != 'decode') AS prompt_tokens_total_delta,

    -- 5. Generation Tokens：由 D 节点贡献，排除纯 P 节点
    sumIf(source.generation_tokens_total_delta, source.process_role != 'prefill') AS generation_tokens_total_delta,

    -- 6. 抢占总次数：全机汇总
    sum(source.num_preemptions_total_delta) AS num_preemptions_total_delta,


    -- ============================================================================
    -- 新增时延底层 Sum / Count 增量汇总
    -- ============================================================================
    sum(source.e2e_request_latency_seconds_sum_delta)   AS e2e_request_latency_seconds_sum_delta,
    sum(source.e2e_request_latency_seconds_count_delta) AS e2e_request_latency_seconds_count_delta,

    sum(source.time_to_first_token_seconds_sum_delta)   AS time_to_first_token_seconds_sum_delta,
    sum(source.time_to_first_token_seconds_count_delta) AS time_to_first_token_seconds_count_delta,

    sum(source.time_per_output_token_seconds_sum_delta)   AS time_per_output_token_seconds_sum_delta,
    sum(source.time_per_output_token_seconds_count_delta) AS time_per_output_token_seconds_count_delta,

    -- ============================================================================
    -- 新增指标的聚合逻辑
    -- ============================================================================

    -- 节点粒度每秒生成 tokens 数 (利用 DWS 层的速率求和)
    cast(sumIf(source.generation_tokens_total_delta_rate_avg, source.process_role != 'prefill') AS UInt64) AS generation_tokens_per_s_avg,

    -- 每请求平均生成 tokens 数 = 总生成 Token 增量 / 总成功请求数
    cast(coalesce(sumIf(source.generation_tokens_total_delta, source.process_role != 'prefill')
         / nullIf(sumIf(source.request_received_total_delta, source.process_role != 'decode') - sum(source.request_failed_total_delta), 0), 0.0) AS Float32) AS generation_tokens_per_request_avg,

    -- 节点端到端平均时延 (E2E Latency)：跨实例加权平均 (总时延和 / 总请求数)
    cast(coalesce(sum(source.e2e_request_latency_seconds_sum_delta)
         / nullIf(sum(source.e2e_request_latency_seconds_count_delta), 0), 0.0) AS Float32) AS e2e_latency_avg,

    -- 节点粒度每秒输入 tokens 数
    cast(sumIf(source.prompt_tokens_total_delta_rate_avg, source.process_role != 'decode') AS UInt64) AS prompt_tokens_per_s_avg,

    -- 请求失败率 (%) = 节点总失败请求数 / 节点总接收请求数 * 100
    cast(coalesce(sum(source.request_failed_total_delta)
         / nullIf(sumIf(source.request_received_total_delta, source.process_role != 'decode'), 0) * 100.0, 0.0) AS Float32) AS request_failed_rate,

    -- 请求成功率 (%) = 节点总成功请求数 / 节点总接收请求数 * 100
    cast(coalesce((sumIf(source.request_received_total_delta, source.process_role != 'decode') - sum(source.request_failed_total_delta))
         / nullIf(sumIf(source.request_received_total_delta, source.process_role != 'decode'), 0) * 100.0, 0.0) AS Float32) AS request_success_rate,

    -- 每分钟请求数 (QPM)：节点内各实例 QPM 求和
    sumIf(source.request_received_total_qpm_avg, source.process_role != 'decode') AS request_received_total_qpm_avg,

    -- TTFT 加权平均值 (秒) = 汇总子实例的时延加和 / 汇总子实例的计数
    cast(coalesce(sum(source.time_to_first_token_seconds_sum_delta)
         / nullIf(sum(source.time_to_first_token_seconds_count_delta), 0), 0.0) AS Float32) AS time_to_first_token_seconds_avg,

    -- TPOT 加权平均值 (秒) = 汇总子实例的 Decode 时延加和 / 汇总子实例的 Decode 计数
    cast(coalesce(sum(source.time_per_output_token_seconds_sum_delta)
         / nullIf(sum(source.time_per_output_token_seconds_count_delta), 0), 0.0) AS Float32) AS time_per_output_token_seconds_avg,

    -- 平均每秒总 token 数 (Prompt 速率 + Generation 速率)
    cast(sumIf(source.prompt_tokens_total_delta_rate_avg, source.process_role != 'decode') + sumIf(source.generation_tokens_total_delta_rate_avg, source.process_role != 'prefill') AS Float32) AS total_tokens_per_s_avg,

    any(source.sync_flag) AS sync_flag

FROM modelmate_metric.t_mops__modelmetric_instance__5min_dws AS source
WHERE source.window_time >= toDateTime({start_time:UInt32}, 'UTC')
  AND source.window_time < toDateTime({end_time:UInt32}, 'UTC')
GROUP BY
    source.window_time,
    source.node_id,
    source.node_ip;
