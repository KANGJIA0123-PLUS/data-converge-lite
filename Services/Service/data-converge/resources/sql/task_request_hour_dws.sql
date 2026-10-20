-- 7.1.2  由t_mops_request_5min_dws -> t_mops_request_hour_dws
INSERT INTO modelmate_metric.t_mops_request_hour_dws (
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
    -- 1. 时间维度：将 5 分钟窗口起始时间向下取整对齐到小时
    toDateTime({start_time:UInt32}, {system_timezone:String}) AS window_time,

    -- 2. 维度键
    f.agent_id,
    f.server_name,

    -- 3. 属性信息：取小时窗口内的最新值
    argMax(f.model_name, f.window_time)                  AS model_name,
    argMax(f.model_id, f.window_time)                    AS model_id,
    argMax(f.agent_name, f.window_time)                  AS agent_name,

    -- 节点信息列表合并与去重：将多个 5 分钟的数组合并并通过 arrayDistinct 展平去重
    arrayDistinct(flatten(groupUniqArray(f.node_id_array))) AS node_id_array,
    arrayDistinct(flatten(groupUniqArray(f.node_ip_array))) AS node_ip_array,

    -- 4. 汇聚指标（正确处理平均值加权与总和累加）
    -- Prefill 时间：平均值 = 小时总prefill时间 / 小时总请求数
    toFloat32(
        CASE WHEN coalesce(sum(f.request_total_sum), 0) > 0
             THEN coalesce(sum(f.prefill_time_sum), 0) / sum(f.request_total_sum)
             ELSE 0.00
        END
    )                                                   AS prefill_time_avg,
    min(f.prefill_time_min)                             AS prefill_time_min,
    max(f.prefill_time_max)                             AS prefill_time_max,
    sum(f.prefill_time_sum)                             AS prefill_time_sum,

    -- Decode 平均时间：平均值 = 小时总decode时间平均值总和 / 小时总请求数
    toFloat32(
        CASE WHEN coalesce(sum(f.request_total_sum), 0) > 0
             THEN coalesce(sum(f.decode_time_average_sum), 0) / sum(f.request_total_sum)
             ELSE 0.00
        END
    )                                                   AS decode_time_average_avg,
    min(f.decode_time_average_min)                      AS decode_time_average_min,
    max(f.decode_time_average_max)                      AS decode_time_average_max,
    sum(f.decode_time_average_sum)                      AS decode_time_average_sum,

    -- 输入 Token：平均值 = 小时输入Token总数 / 小时总请求数
    toFloat32(
        CASE WHEN coalesce(sum(f.request_total_sum), 0) > 0
             THEN coalesce(sum(f.input_tokens_sum), 0) / sum(f.request_total_sum)
             ELSE 0.00
        END
    )                                                   AS input_tokens_avg,
    min(f.input_tokens_min)                             AS input_tokens_min,
    max(f.input_tokens_max)                             AS input_tokens_max,
    toInt32(sum(f.input_tokens_sum))                    AS input_tokens_sum,

    -- 输出 Token：平均值 = 小时输出Token总数 / 小时总请求数
    toFloat32(
        CASE WHEN coalesce(sum(f.request_total_sum), 0) > 0
             THEN coalesce(sum(f.output_tokens_sum), 0) / sum(f.request_total_sum)
             ELSE 0.00
        END
    )                                                   AS output_tokens_avg,
    min(f.output_tokens_min)                            AS output_tokens_min,
    max(f.output_tokens_max)                            AS output_tokens_max,
    toInt32(sum(f.output_tokens_sum))                   AS output_tokens_sum,

    -- 总 Token 与总耗时及请求总数
    toInt32(sum(f.total_tokens_sum))                    AS total_tokens_sum,
    sum(f.total_time_sum)                               AS total_time_sum,
    toUInt64(sum(f.request_total_sum))                  AS request_total_sum,

    -- 5. 标志位
    any(f.sync_flag)                                    AS sync_flag

FROM modelmate_metric.t_mops_request_5min_dws AS f
WHERE f.window_time >= toDateTime({start_time:UInt32}, 'UTC')  -- 调度时传入的小时/天级时间范围（左闭右开）
  AND f.window_time <  toDateTime({end_time:UInt32}, 'UTC')
  AND f.sync_flag = 0
GROUP BY
    toDateTime({start_time:UInt32}, {system_timezone:String}),
    f.agent_id,
    f.server_name;
