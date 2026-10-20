-- 7.2.3.2 t_mops_server_day_ads 面向推理服务视角的汇聚表
 -- 由 t_mops_request_day_dws -> t_mops_server_day_ads
 INSERT INTO modelmate_metric.t_mops_server_day_ads
(
    window_time,
    server_name,
    model_name,
    model_id,
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
    -- 1. 时间维度：对齐到天
    toDateTime({start_time:UInt32}, {system_timezone:String}) AS window_time,

    -- 2. 维度与属性
    dws.server_name,
    argMax(dws.model_name, dws.window_time) AS model_name,
    argMax(dws.model_id, dws.window_time)   AS model_id,

    -- 3. 汇聚指标（跨小时、跨 Agent 累加与加权平均计算）
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

    -- 4. 标识控制
    any(dws.sync_flag) AS sync_flag

FROM modelmate_metric.t_mops_request_day_dws AS dws
WHERE dws.window_time >= toDateTime({start_time:UInt32}, 'UTC')
  AND dws.window_time <  toDateTime({end_time:UInt32}, 'UTC')
  AND dws.sync_flag = 0 -- 只针对探针实时上报的数据做汇聚
GROUP BY
    toDateTime({start_time:UInt32}, {system_timezone:String}),
    dws.server_name;
