-- 7.2.3.1 t_mops_agent_day_ads 面向agent_id视角的汇聚表
  -- 由 t_mops_request_day_dws -> t_mops_agent_day_ads
  INSERT INTO modelmate_metric.t_mops_agent_day_ads
(
    window_time,
    agent_id,
    agent_name,
    prefill_time_avg,
    prefill_time_min,
    prefill_time_max,
    prefill_time_sum,
    decode_time_average_avg,
    decode_time_average_min,
    decode_time_average_max,
    decode_time_average_sum,
    input_tokens_avg,
    input_tokens_min,
    input_tokens_max,
    input_tokens_sum,
    output_tokens_avg,
    output_tokens_min,
    output_tokens_max,
    output_tokens_sum,
    total_tokens_sum,
    total_time_sum,
    request_total_sum,
    request_success_sum,
    -- 补充指标字段列表
    request_failed_rate,
    request_success_rate,
    infer_input_output_token_rate,
    infer_e2e_request_latency_seconds_avg,
    infer_input_token_num_length_avg,
    infer_output_token_num_length_avg,
    infer_request_received_qps,
    time_per_output_token_seconds_avg,
    time_to_first_token_seconds_avg,
    prompt_tokens_per_s_avg,
    generation_tokens_per_s_avg,
    infer_input_token_sum_per_s,
    infer_output_token_sum_per_s,
    sync_flag
)
SELECT
    -- 1. 时间维度：将小时级时间向下取整对齐到天起始时间
    toDateTime({start_time:UInt32}, {system_timezone:String}) AS window_time,

    -- 2. 维度与属性
    dws.agent_id,
    any(dws.agent_name) AS agent_name,

    -- 3. 基础平均与累加指标计算
    toFloat32(
        CASE WHEN coalesce(sum(dws.request_total_sum), 0) > 0
             THEN coalesce(sum(dws.prefill_time_sum), 0) / sum(dws.request_total_sum)
             ELSE 0.00
        END
    ) AS prefill_time_avg,
    min(dws.prefill_time_min) AS prefill_time_min,
    max(dws.prefill_time_max) AS prefill_time_max,
    sum(dws.prefill_time_sum) AS prefill_time_sum,

    toFloat32(
        CASE WHEN coalesce(sum(dws.request_total_sum), 0) > 0
             THEN coalesce(sum(dws.decode_time_average_sum), 0) / sum(dws.request_total_sum)
             ELSE 0.00
        END
    ) AS decode_time_average_avg,
    min(dws.decode_time_average_min) AS decode_time_average_min,
    max(dws.decode_time_average_max) AS decode_time_average_max,
    sum(dws.decode_time_average_sum) AS decode_time_average_sum,

    toFloat32(
        CASE WHEN coalesce(sum(dws.request_total_sum), 0) > 0
             THEN coalesce(sum(dws.input_tokens_sum), 0) / sum(dws.request_total_sum)
             ELSE 0.00
        END
    ) AS input_tokens_avg,
    min(dws.input_tokens_min) AS input_tokens_min,
    max(dws.input_tokens_max) AS input_tokens_max,
    toInt32(sum(dws.input_tokens_sum)) AS input_tokens_sum,

    toFloat32(
        CASE WHEN coalesce(sum(dws.request_total_sum), 0) > 0
             THEN coalesce(sum(dws.output_tokens_sum), 0) / sum(dws.request_total_sum)
             ELSE 0.00
        END
    ) AS output_tokens_avg,
    min(dws.output_tokens_min) AS output_tokens_min,
    max(dws.output_tokens_max) AS output_tokens_max,
    toInt32(sum(dws.output_tokens_sum)) AS output_tokens_sum,

    toInt32(sum(dws.total_tokens_sum)) AS total_tokens_sum,
    sum(dws.total_time_sum) AS total_time_sum,
    toUInt64(sum(dws.request_total_sum)) AS request_total_sum,
    toUInt64(coalesce(sum(dws.request_success_count), 0)) AS request_success_sum,

    -- 4. 补充指标计算
    -- 请求失败率 (%)
    CAST(coalesce(
        sum(dws.request_failed_count) * 100.0 / nullIf(sum(dws.request_total_sum), 0),
        0.0
    ) AS Nullable(Float32)) AS request_failed_rate,

    -- 请求成功率 (%)
    CAST(coalesce(
        sum(dws.request_success_count) * 100.0 / nullIf(sum(dws.request_total_sum), 0),
        0.0
    ) AS Nullable(Float32)) AS request_success_rate,

    -- 输入/输出 Token 比率
    CAST(coalesce(
        sum(dws.input_tokens_sum) / nullIf(toFloat64(sum(dws.output_tokens_sum)), 0.0),
        0.0
    ) AS Nullable(Float32)) AS infer_input_output_token_rate,

    -- 端到端请求延迟：毫秒总耗时按请求数平均，再转换为秒
    CAST(coalesce(
        sum(dws.total_time_sum) / nullIf(toFloat64(sum(dws.request_total_sum)), 0.0) / 1000.0,
        0.0
    ) AS Nullable(Float32)) AS infer_e2e_request_latency_seconds_avg,

    -- 平均输入 Token 长度
    CAST(coalesce(
        sum(dws.input_tokens_sum) / nullIf(toFloat64(sum(dws.request_total_sum)), 0.0),
        0.0
    ) AS Nullable(Float64)) AS infer_input_token_num_length_avg,

    -- 平均输出 Token 长度
    CAST(coalesce(
        sum(dws.output_tokens_sum) / nullIf(toFloat64(sum(dws.request_total_sum)), 0.0),
        0.0
    ) AS Nullable(Float64)) AS infer_output_token_num_length_avg,

    -- 平均每秒请求数 (QPS)
    CAST(coalesce(
        sum(dws.request_total_sum) / nullIf(toFloat64(dateDiff('second', toDateTime({start_time:UInt32}, 'UTC'), toDateTime({end_time:UInt32}, 'UTC'))), 0.0),
        0.0
    ) AS Nullable(Float64)) AS infer_request_received_qps,

    -- TPOT：源字段已是每请求的单 Token 平均耗时（毫秒），按请求数平均后转秒
    CAST(coalesce(
        sum(dws.decode_time_average_sum) / nullIf(toFloat64(sum(dws.request_total_sum)), 0.0) / 1000.0,
        0.0
    ) AS Nullable(Float32)) AS time_per_output_token_seconds_avg,

    -- TTFT：首 Token 耗时（毫秒）按请求数平均后转秒，不按输入 Token 数相除
    CAST(coalesce(
        sum(dws.prefill_time_sum) / nullIf(toFloat64(sum(dws.request_total_sum)), 0.0) / 1000.0,
        0.0
    ) AS Nullable(Float32)) AS time_to_first_token_seconds_avg,

    -- 平均每秒输入 Token 数 (tokens/s)
    CAST(coalesce(
        sum(dws.input_tokens_sum) / nullIf(toFloat64(dateDiff('second', toDateTime({start_time:UInt32}, 'UTC'), toDateTime({end_time:UInt32}, 'UTC'))), 0.0),
        0.0
    ) AS Nullable(Float64)) AS prompt_tokens_per_s_avg,

    -- 平均每秒输出 Token 数 (tokens/s)
    CAST(coalesce(
        sum(dws.output_tokens_sum) / nullIf(toFloat64(dateDiff('second', toDateTime({start_time:UInt32}, 'UTC'), toDateTime({end_time:UInt32}, 'UTC'))), 0.0),
        0.0
    ) AS Nullable(Float64)) AS generation_tokens_per_s_avg,

    -- 统计周期内，输入 token 吞吐趋势 (tokens/s) = 输入token数 / (解码耗时ms / 1000)
    CAST(coalesce(
        sum(dws.input_tokens_sum) / nullIf(toFloat64(sum(dws.total_time_sum) - sum(dws.prefill_time_sum)) / 1000.0, 0.0),
        0.0
    ) AS Nullable(Float64)) AS infer_input_token_sum_per_s,

    -- 统计周期内，输出 token 吞吐趋势 (tokens/s) = 输出token数 / (解码耗时ms / 1000)
    CAST(coalesce(
        sum(dws.output_tokens_sum) / nullIf(toFloat64(sum(dws.total_time_sum) - sum(dws.prefill_time_sum)) / 1000.0, 0.0),
        0.0
    ) AS Nullable(Float64)) AS infer_output_token_sum_per_s,

    -- 5. 标识控制
    any(dws.sync_flag) AS sync_flag

FROM modelmate_metric.t_mops_request_day_dws AS dws
WHERE dws.window_time >= toDateTime({start_time:UInt32}, {system_timezone:String})
  AND dws.window_time <  toDateTime({end_time:UInt32}, {system_timezone:String})
  AND dws.sync_flag = 0 -- 只针对探针实时上报的数据做汇聚
GROUP BY
    toDateTime({start_time:UInt32}, {system_timezone:String}),
    dws.agent_id;
