-- 1.3  t_mops_gpu_hour_dws -> t_mops_gpu_day_dws
INSERT INTO modelmate_metric.t_mops_gpu_day_dws (
    window_time,
    ai_card_type,
    ai_card_id,
    chip_id,

    node_id,
    node_ip,
    node_type,
    node_name,
    ai_card_index,
    dev_mfr,
    dev_model,
    dev_status_code,
    dev_status_name,

    hbm_util_avg,
    hbm_util_max,
    hbm_util_min,
    ai_core_util_avg,
    ai_core_util_max,
    ai_core_util_min,
    ai_core_util_argMax,
    hbm_bandwidth_util_avg,
    hbm_bandwidth_util_max,
    hbm_bandwidth_util_min,
    ai_card_temp_avg,
    ai_card_temp_max,
    ai_card_temp_min,
    ai_card_power_avg,
    ai_card_power_max,
    ai_card_power_min,
    bandwidth_tx_avg,
    bandwidth_tx_max,
    bandwidth_tx_min,
    bandwidth_rx_avg,
    bandwidth_rx_max,
    bandwidth_rx_min,
    ai_card_health_argMax,

    ai_card_normal_state_counter,
    ai_card_abnormal_state_counter,
    ai_card_health_ratio_avg,
    ai_card_offline_ratio_avg,

    ai_card_used_argMax,
    ai_card_used_counter,
    ai_card_unused_counter,
    ai_card_used_active_counter,
    ai_card_used_ratio_avg,
    
    ai_card_computing_power_argMax,
    ai_card_used_computing_power_counter,
    ai_card_unused_computing_power_counter,
    
    work_time_ai_core_util_avg, -- 天月表独有指标
    work_time_ai_core_util_max, -- 天月表独有指标
    work_time_ai_core_util_min, -- 天月表独有指标
    idle_time_ai_core_util_avg, -- 天月表独有指标
    idle_time_ai_core_util_max, -- 天月表独有指标
    idle_time_ai_core_util_min, -- 天月表独有指标
    work_time_ai_core_util_status, -- 天月表独有指标

    sync_flag
)
SELECT
    -- 1. 时间维度：对齐到天起点 (00:00:00)
    toStartOfDay(h.window_time)                                              AS window_time,

    -- 2. 核心维度键 (GROUP BY)
    h.ai_card_type,
    h.ai_card_id,
    h.chip_id,

    -- 3. 透传维度与节点属性：静态属性使用 any()，最新状态使用 anyLast()
    any(h.node_id)                                                           AS node_id,
    any(h.node_ip)                                                           AS node_ip,
    any(h.node_type)                                                         AS node_type,
    any(h.node_name)                                                         AS node_name,
    any(h.ai_card_index)                                                     AS ai_card_index,
    any(h.dev_mfr)                                                           AS dev_mfr,
    any(h.dev_model)                                                         AS dev_model,
    anyLast(h.dev_status_code)                                               AS dev_status_code,
    anyLast(h.dev_status_name)                                               AS dev_status_name,

    -- 4. 连续数值指标三次聚合 (AVG/MAX/MIN)
    avg(h.hbm_util_avg)                                                      AS hbm_util_avg,
    max(h.hbm_util_max)                                                      AS hbm_util_max,
    min(h.hbm_util_min)                                                      AS hbm_util_min,

    avg(h.ai_core_util_avg)                                                  AS ai_core_util_avg,
    max(h.ai_core_util_max)                                                  AS ai_core_util_max,
    min(h.ai_core_util_min)                                                  AS ai_core_util_min,
    argMax(h.ai_core_util_argMax, h.window_time)                             AS ai_core_util_argMax,

    avg(h.hbm_bandwidth_util_avg)                                            AS hbm_bandwidth_util_avg,
    max(h.hbm_bandwidth_util_max)                                            AS hbm_bandwidth_util_max,
    min(h.hbm_bandwidth_util_min)                                            AS hbm_bandwidth_util_min,

    avg(h.ai_card_temp_avg)                                                  AS ai_card_temp_avg,
    max(h.ai_card_temp_max)                                                  AS ai_card_temp_max,
    min(h.ai_card_temp_min)                                                  AS ai_card_temp_min,

    avg(h.ai_card_power_avg)                                                 AS ai_card_power_avg,
    max(h.ai_card_power_max)                                                 AS ai_card_power_max,
    min(h.ai_card_power_min)                                                 AS ai_card_power_min,

    avg(h.bandwidth_tx_avg)                                                  AS bandwidth_tx_avg,
    max(h.bandwidth_tx_max)                                                  AS bandwidth_tx_max,
    min(h.bandwidth_tx_min)                                                  AS bandwidth_tx_min,

    avg(h.bandwidth_rx_avg)                                                  AS bandwidth_rx_avg,
    max(h.bandwidth_rx_max)                                                  AS bandwidth_rx_max,
    min(h.bandwidth_rx_min)                                                  AS bandwidth_rx_min,

    argMax(h.ai_card_health_argMax, h.window_time)                           AS ai_card_health_argMax,

    -- 5. 健康/异常计数器累加与占比加权计算（全天）
    sum(h.ai_card_normal_state_counter)                                      AS ai_card_normal_state_counter,
    sum(h.ai_card_abnormal_state_counter)                                    AS ai_card_abnormal_state_counter,
    CAST(
        sum(h.ai_card_normal_state_counter) / nullIf(sum(h.ai_card_normal_state_counter) + sum(h.ai_card_abnormal_state_counter), 0)
        AS Nullable(Float32)
    )                                                                      AS ai_card_health_ratio_avg,
    CAST(
        sum(h.ai_card_abnormal_state_counter) / nullIf(sum(h.ai_card_normal_state_counter) + sum(h.ai_card_abnormal_state_counter), 0)
        AS Nullable(Float32)
    )                                                                      AS ai_card_offline_ratio_avg,

    -- 6. 使用率相关计数器累加与占比加权计算（全天）
    argMax(h.ai_card_used_argMax, h.window_time)                             AS ai_card_used_argMax,
    sum(h.ai_card_used_counter)                                              AS ai_card_used_counter,
    sum(h.ai_card_unused_counter)                                            AS ai_card_unused_counter,
    sum(h.ai_card_used_active_counter)                                       AS ai_card_used_active_counter,
    CAST(
        sum(h.ai_card_used_active_counter) / nullIf(sum(h.ai_card_used_counter), 0)
        AS Nullable(Float32)
    )                                                                      AS ai_card_used_ratio_avg,

    -- 7.算力容量/分配累加指标
    argMax(h.ai_card_computing_power_argMax, h.window_time)                  AS ai_card_computing_power_argMax,
    sum(h.ai_card_used_computing_power_counter)                              AS ai_card_used_computing_power_counter,
    sum(h.ai_card_unused_computing_power_counter)                            AS ai_card_unused_computing_power_counter,

    --  忙时指标 (09:00 <= hour < 18:00)
    avgIf(h.ai_core_util_avg, toHour(h.window_time) >= 9 AND toHour(h.window_time) < 18) AS work_time_ai_core_util_avg,
    maxIf(h.ai_core_util_max, toHour(h.window_time) >= 9 AND toHour(h.window_time) < 18) AS work_time_ai_core_util_max,
    minIf(h.ai_core_util_min, toHour(h.window_time) >= 9 AND toHour(h.window_time) < 18) AS work_time_ai_core_util_min,

    --  闲时指标 (hour < 9 OR hour >= 18)
    avgIf(h.ai_core_util_avg, toHour(h.window_time) < 9 OR toHour(h.window_time) >= 18) AS idle_time_ai_core_util_avg,
    maxIf(h.ai_core_util_max, toHour(h.window_time) < 9 OR toHour(h.window_time) >= 18) AS idle_time_ai_core_util_max,
    minIf(h.ai_core_util_min, toHour(h.window_time) < 9 OR toHour(h.window_time) >= 18) AS idle_time_ai_core_util_min,

        -- 3. 天忙时状态打标 (基于当天忙时整体平均利用率)
    multiIf(
        avgIf(h.ai_core_util_avg, toHour(h.window_time) >= 9 AND toHour(h.window_time) < 18) IS NULL, NULL,
        avgIf(h.ai_core_util_avg, toHour(h.window_time) >= 9 AND toHour(h.window_time) < 18) > 80,   'overuse',
        avgIf(h.ai_core_util_avg, toHour(h.window_time) >= 9 AND toHour(h.window_time) < 18) >= 15,  'normal_use',
        avgIf(h.ai_core_util_avg, toHour(h.window_time) >= 9 AND toHour(h.window_time) < 18) >= 0,   'inefficient_use',
        NULL
    ) AS work_time_ai_core_util_status,


    -- 8. 标记位透传
    any(h.sync_flag)                                                         AS sync_flag

FROM modelmate_metric.t_mops_gpu_hour_dws AS h
/* 调度时按天传入时间约束区间，例如每日凌晨运行前一天的数据计算 */
WHERE h.window_time >= {start_time:DateTime}
  AND h.window_time <  {end_time:DateTime}
  AND h.sync_flag = 0
GROUP BY
    toStartOfDay(h.window_time),
    h.ai_card_type,
    h.ai_card_id,
    h.chip_id;
    
