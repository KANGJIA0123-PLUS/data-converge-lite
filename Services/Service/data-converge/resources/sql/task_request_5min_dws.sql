-- 7.1.1  由t_mops_request_realtime_dwd -> t_mops_request_5min_dws
INSERT INTO modelmate_metric.t_mops_request_5min_dws (
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
    -- 1. 时间维度：将 1 分钟级 metric_datetime 向下取整对齐到 5 分钟窗口
    toDateTime({start_time:UInt32}, {system_timezone:String}) AS window_time,

    -- 2. 维度键
    r.agent_id,
    r.server_name,

    -- 3. 属性信息：取窗口内的最新值（用 argMax 保证数据确定性）
    argMax(r.model_name, r.metric_datetime)               AS model_name,
    argMax(r.model_id, r.metric_datetime)                 AS model_id,
    argMax(r.agent_name, r.metric_datetime)               AS agent_name,

    -- 节点信息列表聚合：使用 groupUniqArray 将窗口内的节点 ID 和 IP 收集去重
    groupUniqArray(r.node_id)                             AS node_id_array,
    groupUniqArray(r.node_ip)                             AS node_ip_array,

    -- 4. 汇聚指标（平均值字段现已适配为 Float32，无需强转整型，可直接输出小数精度）
    -- Prefill 时延指标
    toFloat32(avg(r.prefill_time))                        AS prefill_time_avg,
    min(r.prefill_time)                                   AS prefill_time_min,
    max(r.prefill_time)                                   AS prefill_time_max,
    sum(r.prefill_time)                                   AS prefill_time_sum,

    -- Decode 平均时间指标
    toFloat32(avg(r.decode_time_average))                 AS decode_time_average_avg,
    min(r.decode_time_average)                            AS decode_time_average_min,
    max(r.decode_time_average)                            AS decode_time_average_max,
    sum(r.decode_time_average)                            AS decode_time_average_sum,

    -- 输入 Token 指标
    toFloat32(avg(r.input_tokens))                        AS input_tokens_avg,
    min(r.input_tokens)                                   AS input_tokens_min,
    max(r.input_tokens)                                   AS input_tokens_max,
    toInt32(sum(r.input_tokens))                          AS input_tokens_sum,

    -- 输出 Token 指标
    toFloat32(avg(r.output_tokens))                       AS output_tokens_avg,
    min(r.output_tokens)                                  AS output_tokens_min,
    max(r.output_tokens)                                  AS output_tokens_max,
    toInt32(sum(r.output_tokens))                         AS output_tokens_sum,

    -- 总 Token 与总耗时及请求总数
    toInt32(sum(r.total_tokens))                          AS total_tokens_sum,
    sum(r.total_time)                                     AS total_time_sum,
    toUInt64(count(*))                                    AS request_total_sum,

    -- 5. 标志位
    any(r.sync_flag)                                      AS sync_flag

FROM modelmate_metric.t_mops_request_realtime_dwd AS r
WHERE r.metric_datetime >= toDateTime({start_time:UInt32}, {system_timezone:String})  -- 调度时传入的时间范围（左闭右开）
  AND r.metric_datetime <  toDateTime({end_time:UInt32}, {system_timezone:String})
  AND r.sync_flag = 0
GROUP BY
    toDateTime({start_time:UInt32}, {system_timezone:String}),
    r.agent_id,
    r.server_name;
