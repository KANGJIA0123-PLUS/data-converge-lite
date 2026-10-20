-- 1.4  t_mops_gpu_day_dws -> t_mops_gpu_month_dws
INSERT INTO modelmate_metric.t_mops_gpu_month_dws (
    -- 1. 时间与核心维度 (1~4)
    window_time,
    ai_card_type,
    ai_card_id,
    chip_id,

    -- 2. 透传维度与属性 (5~13)
    node_id,
    node_ip,
    node_type,
    node_name,
    ai_card_index,
    dev_mfr,
    dev_model,
    dev_status_code,
    dev_status_name,

    -- 3. 连续数值聚合指标多次聚合 (14~34)
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

    -- 4. 健康与离线状态计数器与加权占比 (35~38)
    ai_card_normal_state_counter,
    ai_card_abnormal_state_counter,
    ai_card_health_ratio_avg,
    ai_card_offline_ratio_avg,

    -- 5. 使用率相关计数器与加权占比 (39~42)
    ai_card_used_argMax,
    ai_card_used_counter,
    ai_card_unused_counter,
    ai_card_used_active_counter,
    ai_card_used_ratio_avg,
    
    -- 6. 算力容量/分配累加指标
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

    -- 7. 数据同步标记 (43)
    sync_flag
)
SELECT
    -- 1. 时间维度：取当月 1 号 00:00:00 作为月窗口起点
    toDateTime({start_time:UInt32}, {system_timezone:String})                                            AS window_time,

    -- 2. 核心维度键 (GROUP BY)
    d.ai_card_type,
    d.ai_card_id,
    d.chip_id,

    -- 3. 透传维度与节点属性：静态属性使用 any()，最新状态使用 anyLast()
    any(d.node_id)                                                           AS node_id,
    any(d.node_ip)                                                           AS node_ip,
    any(d.node_type)                                                         AS node_type,
    any(d.node_name)                                                         AS node_name,
    any(d.ai_card_index)                                                     AS ai_card_index,
    any(d.dev_mfr)                                                           AS dev_mfr,
    any(d.dev_model)                                                         AS dev_model,
    anyLast(d.dev_status_code)                                               AS dev_status_code,
    anyLast(d.dev_status_name)                                               AS dev_status_name,

    -- 4. 连续数值指标四次聚合 (AVG/MAX/MIN)
    avg(d.hbm_util_avg)                                                      AS hbm_util_avg,
    max(d.hbm_util_max)                                                      AS hbm_util_max,
    min(d.hbm_util_min)                                                      AS hbm_util_min,

    avg(d.ai_core_util_avg)                                                  AS ai_core_util_avg,
    max(d.ai_core_util_max)                                                  AS ai_core_util_max,
    min(d.ai_core_util_min)                                                  AS ai_core_util_min,
    argMax(d.ai_core_util_argMax, d.window_time)                               AS ai_core_util_argMax,

    avg(d.hbm_bandwidth_util_avg)                                            AS hbm_bandwidth_util_avg,
    max(d.hbm_bandwidth_util_max)                                            AS hbm_bandwidth_util_max,
    min(d.hbm_bandwidth_util_min)                                            AS hbm_bandwidth_util_min,

    avg(d.ai_card_temp_avg)                                                  AS ai_card_temp_avg,
    max(d.ai_card_temp_max)                                                  AS ai_card_temp_max,
    min(d.ai_card_temp_min)                                                  AS ai_card_temp_min,

    avg(d.ai_card_power_avg)                                                 AS ai_card_power_avg,
    max(d.ai_card_power_max)                                                 AS ai_card_power_max,
    min(d.ai_card_power_min)                                                 AS ai_card_power_min,

    avg(d.bandwidth_tx_avg)                                                  AS bandwidth_tx_avg,
    max(d.bandwidth_tx_max)                                                  AS bandwidth_tx_max,
    min(d.bandwidth_tx_min)                                                  AS bandwidth_tx_min,

    avg(d.bandwidth_rx_avg)                                                  AS bandwidth_rx_avg,
    max(d.bandwidth_rx_max)                                                  AS bandwidth_rx_max,
    min(d.bandwidth_rx_min)                                                  AS bandwidth_rx_min,
    
    argMax(d.ai_card_health_argMax, d.window_time)                             AS ai_card_health_argMax,

    -- 5. 健康/异常计数器全月累加与全月加权占比重新计算
    sum(d.ai_card_normal_state_counter)                                      AS ai_card_normal_state_counter,
    sum(d.ai_card_abnormal_state_counter)                                    AS ai_card_abnormal_state_counter,
    CAST(
        sum(d.ai_card_normal_state_counter) / nullIf(sum(d.ai_card_normal_state_counter) + sum(d.ai_card_abnormal_state_counter), 0)
        AS Nullable(Float32)
    )                                                                      AS ai_card_health_ratio_avg,
    CAST(
        sum(d.ai_card_abnormal_state_counter) / nullIf(sum(d.ai_card_normal_state_counter) + sum(d.ai_card_abnormal_state_counter), 0)
        AS Nullable(Float32)
    )                                                                      AS ai_card_offline_ratio_avg,

    -- 6. 使用率相关计数器全月累加与全月加权占比重新计算
    argMax(d.ai_card_used_argMax, d.window_time)                           AS ai_card_used_argMax,
    sum(d.ai_card_used_counter)                                              AS ai_card_used_counter,
    sum(d.ai_card_unused_counter)                                            AS ai_card_unused_counter,
    sum(d.ai_card_used_active_counter)                                       AS ai_card_used_active_counter,
    CAST(
        sum(d.ai_card_used_active_counter) / nullIf(sum(d.ai_card_used_counter), 0)
        AS Nullable(Float32)
    )                                                                      AS ai_card_used_ratio_avg,
    
    -- 7.算力容量/分配累加指标
    argMax(d.ai_card_computing_power_argMax, d.window_time)                      AS ai_card_computing_power_argMax,
    sum(d.ai_card_used_computing_power_counter)                                    AS ai_card_used_computing_power_counter,
    sum(d.ai_card_unused_computing_power_counter)                                  AS ai_card_unused_computing_power_counter,
    
    -- 1. 忙时月度汇聚
    avg(d.work_time_ai_core_util_avg) AS work_time_ai_core_util_avg, -- 月度忙时平均利用率
    max(d.work_time_ai_core_util_max) AS work_time_ai_core_util_max, -- 月度忙时最高峰值
    min(d.work_time_ai_core_util_min) AS work_time_ai_core_util_min, -- 月度忙时最低谷值

    -- 2. 闲时月度汇聚
    avg(d.idle_time_ai_core_util_avg) AS idle_time_ai_core_util_avg, -- 月度闲时平均利用率
    max(d.idle_time_ai_core_util_max) AS idle_time_ai_core_util_max, -- 月度闲时最高峰值
    min(d.idle_time_ai_core_util_min) AS idle_time_ai_core_util_min, -- 月度闲时最低谷值
    
    -- 3. 月度忙时状态打标 (基于当月忙时整体平均利用率)
    multiIf(
        avg(d.work_time_ai_core_util_avg) IS NULL, NULL,
        avg(d.work_time_ai_core_util_avg) > 80,   'overuse',
        avg(d.work_time_ai_core_util_avg) >= 15,  'normal_use',
        avg(d.work_time_ai_core_util_avg) >= 0,   'inefficient_use',
        NULL
    ) AS work_time_ai_core_util_status,

    -- 7. 标记位透传
    any(d.sync_flag)                                                         AS sync_flag

FROM modelmate_metric.t_mops_gpu_day_dws AS d
/* 按月调度：通常在每月 1 号凌晨运行，处理上一个自然月全天的数据 */
WHERE d.window_time >= toDateTime({start_time:UInt32}, {system_timezone:String})
  AND d.window_time <  toDateTime({end_time:UInt32}, {system_timezone:String})
  AND d.sync_flag = 0
GROUP BY
    toDateTime({start_time:UInt32}, {system_timezone:String}),
    d.ai_card_type,
    d.ai_card_id,
    d.chip_id;
    
    
