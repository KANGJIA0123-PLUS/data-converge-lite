-- 7.1.3  由t_mops_request_hour_dws -> t_mops_request_day_dws
INSERT INTO modelmate_metric.t_mops_request_day_dws (
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
    sync_flag
)
SELECT
    -- 1. 时间维度：将小时窗口起始时间向下取整对齐到天开始时间
    toStartOfDay(h.window_time) AS window_time,

    -- 2. 维度键
    h.agent_id,
    h.server_name,

    -- 3. 属性信息：取天窗口内的最新值
    argMax(h.model_name, h.window_time)                  AS model_name,
    argMax(h.model_id, h.window_time)                    AS model_id,
    argMax(h.agent_name, h.window_time)                  AS agent_name,

    -- 节点信息列表合并与去重：将一整天内所有小时的数组合并展平并去重
    arrayDistinct(flatten(groupUniqArray(h.node_id_array))) AS node_id_array,
    arrayDistinct(flatten(groupUniqArray(h.node_ip_array))) AS node_ip_array,

    -- 4. 汇聚指标（通过加权平均与累加求和处理）
    -- Prefill 时间平均值：天总prefill时间 / 天总请求数
    toFloat32(
        CASE WHEN coalesce(sum(h.request_total_sum), 0) > 0
             THEN coalesce(sum(h.prefill_time_sum), 0) / sum(h.request_total_sum)
             ELSE 0.00
        END
    )                                                   AS prefill_time_avg,
    min(h.prefill_time_min)                             AS prefill_time_min,
    max(h.prefill_time_max)                             AS prefill_time_max,
    sum(h.prefill_time_sum)                             AS prefill_time_sum,

    -- Decode 平均时间平均值：天总decode平均时间总和 / 天总请求数
    toFloat32(
        CASE WHEN coalesce(sum(h.request_total_sum), 0) > 0
             THEN coalesce(sum(h.decode_time_average_sum), 0) / sum(h.request_total_sum)
             ELSE 0.00
        END
    )                                                   AS decode_time_average_avg,
    min(h.decode_time_average_min)                      AS decode_time_average_min,
    max(h.decode_time_average_max)                      AS decode_time_average_max,
    sum(h.decode_time_average_sum)                      AS decode_time_average_sum,

    -- 输入 Token 平均值：天输入Token总数 / 天总请求数
    toFloat32(
        CASE WHEN coalesce(sum(h.request_total_sum), 0) > 0
             THEN coalesce(sum(h.input_tokens_sum), 0) / sum(h.request_total_sum)
             ELSE 0.00
        END
    )                                                   AS input_tokens_avg,
    min(h.input_tokens_min)                             AS input_tokens_min,
    max(h.input_tokens_max)                             AS input_tokens_max,
    toInt32(sum(h.input_tokens_sum))                    AS input_tokens_sum,

    -- 输出 Token 平均值：天输出Token总数 / 天总请求数
    toFloat32(
        CASE WHEN coalesce(sum(h.request_total_sum), 0) > 0
             THEN coalesce(sum(h.output_tokens_sum), 0) / sum(h.request_total_sum)
             ELSE 0.00
        END
    )                                                   AS output_tokens_avg,
    min(h.output_tokens_min)                            AS output_tokens_min,
    max(h.output_tokens_max)                            AS output_tokens_max,
    toInt32(sum(h.output_tokens_sum))                   AS output_tokens_sum,

    -- 总 Token 与总耗时及请求总数
    toInt32(sum(h.total_tokens_sum))                    AS total_tokens_sum,
    sum(h.total_time_sum)                               AS total_time_sum,
    toUInt64(sum(h.request_total_sum))                  AS request_total_sum,

    -- 5. 标志位
    any(h.sync_flag)                                    AS sync_flag

FROM modelmate_metric.t_mops_request_hour_dws AS h
WHERE h.window_time >= {start_time:DateTime}  -- 调度时传入的天级时间范围（如当天的 00:00:00，左闭右开）
  AND h.window_time <  {end_time:DateTime}
  AND h.sync_flag = 0
GROUP BY
    window_time,
    h.agent_id,
    h.server_name;
