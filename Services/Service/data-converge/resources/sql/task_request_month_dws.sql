-- 7.1.4  由t_mops_request_day_dws -> t_mops_request_month_dws
INSERT INTO modelmate_metric.t_mops_request_month_dws (
    window_time,
    agent_id,
    server_name,
    model_name,
    model_id,
    agent_name,
    node_id_array,
    node_ip_array,
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
    -- 补充字段
    request_failed_count,
    request_success_count,
    sync_flag
)
SELECT
    -- 1. 时间维度：将天窗口起始时间向下取整对齐到月开始时间
    toDateTime({start_time:UInt32}, {system_timezone:String}) AS window_time,

    -- 2. 维度键
    d.agent_id,
    d.server_name,

    -- 3. 属性信息：取月窗口内的最新值
    argMax(d.model_name, d.window_time)                  AS model_name,
    argMax(d.model_id, d.window_time)                    AS model_id,
    argMax(d.agent_name, d.window_time)                  AS agent_name,

    -- 节点信息列表合并与去重：将一整月内所有天的数组合并展平并去重
    arrayDistinct(flatten(groupUniqArray(d.node_id_array))) AS node_id_array,
    arrayDistinct(flatten(groupUniqArray(d.node_ip_array))) AS node_ip_array,

    -- 4. 汇聚指标（通过加权平均与累加求和处理）
    -- Prefill 时间平均值：月总prefill时间 / 月总请求数
    toFloat32(
        CASE WHEN coalesce(sum(d.request_total_sum), 0) > 0
             THEN coalesce(sum(d.prefill_time_sum), 0) / sum(d.request_total_sum)
             ELSE 0.00
        END
    )                                                   AS prefill_time_avg,
    min(d.prefill_time_min)                             AS prefill_time_min,
    max(d.prefill_time_max)                             AS prefill_time_max,
    sum(d.prefill_time_sum)                             AS prefill_time_sum,

    -- Decode 平均时间平均值：月总decode平均时间总和 / 月总请求数
    toFloat32(
        CASE WHEN coalesce(sum(d.request_total_sum), 0) > 0
             THEN coalesce(sum(d.decode_time_average_sum), 0) / sum(d.request_total_sum)
             ELSE 0.00
        END
    )                                                   AS decode_time_average_avg,
    min(d.decode_time_average_min)                      AS decode_time_average_min,
    max(d.decode_time_average_max)                      AS decode_time_average_max,
    sum(d.decode_time_average_sum)                      AS decode_time_average_sum,

    -- 输入 Token 平均值：月输入Token总数 / 月总请求数
    toFloat32(
        CASE WHEN coalesce(sum(d.request_total_sum), 0) > 0
             THEN coalesce(sum(d.input_tokens_sum), 0) / sum(d.request_total_sum)
             ELSE 0.00
        END
    )                                                   AS input_tokens_avg,
    min(d.input_tokens_min)                             AS input_tokens_min,
    max(d.input_tokens_max)                             AS input_tokens_max,
    toInt32(sum(d.input_tokens_sum))                    AS input_tokens_sum,

    -- 输出 Token 平均值：月输出Token总数 / 月总请求数
    toFloat32(
        CASE WHEN coalesce(sum(d.request_total_sum), 0) > 0
             THEN coalesce(sum(d.output_tokens_sum), 0) / sum(d.request_total_sum)
             ELSE 0.00
        END
    )                                                   AS output_tokens_avg,
    min(d.output_tokens_min)                            AS output_tokens_min,
    max(d.output_tokens_max)                            AS output_tokens_max,
    toInt32(sum(d.output_tokens_sum))                   AS output_tokens_sum,

    -- 总 Token 与总耗时及请求总数
    toInt32(sum(d.total_tokens_sum))                    AS total_tokens_sum,
    sum(d.total_time_sum)                               AS total_time_sum,
    toUInt64(sum(d.request_total_sum))                  AS request_total_sum,
    -- 补充指标计算
    toUInt64(sum(d.request_failed_count))               AS request_failed_count,
    toUInt64(sum(d.request_success_count))               AS request_success_count,

    -- 5. 标志位
    any(d.sync_flag)                                    AS sync_flag

FROM modelmate_metric.t_mops_request_day_dws AS d
WHERE d.window_time >= toDateTime({start_time:UInt32}, {system_timezone:String})  -- 调度时传入的月份时间范围（例如月初 00:00:00，左闭右开）
  AND d.window_time <  toDateTime({end_time:UInt32}, {system_timezone:String})
  AND d.sync_flag = 0
GROUP BY
    toDateTime({start_time:UInt32}, {system_timezone:String}),
    d.agent_id,
    d.server_name;
