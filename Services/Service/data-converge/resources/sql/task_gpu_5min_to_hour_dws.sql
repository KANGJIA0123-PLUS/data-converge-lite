-- 1.2  t_mops_gpu_5min_dws -> t_mops_gpu_hour_dws
INSERT INTO modelmate_metric.t_mops_gpu_hour_dws (
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

    -- 3. 连续数值聚合指标二次聚合 (14~34)
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
    

    -- 7. 数据同步标记 (43)
    sync_flag
)
SELECT
    -- 1. 时间维度：对齐到 1 小时窗口起点
    toDateTime({start_time:UInt32}, {system_timezone:String})                                             AS window_time,

    -- 2. 核心维度键 (GROUP BY)
    ai_card_type,
    ai_card_id,
    chip_id,

    -- 3. 透传维度与节点属性：静态属性使用 any()，最新状态使用 anyLast()
    any(node_id)                                                           AS node_id,
    any(node_ip)                                                           AS node_ip,
    any(node_type)                                                         AS node_type,
    any(node_name)                                                         AS node_name,
    any(ai_card_index)                                                     AS ai_card_index,
    any(dev_mfr)                                                           AS dev_mfr,
    any(dev_model)                                                         AS dev_model,
    anyLast(dev_status_code)                                               AS dev_status_code,
    anyLast(dev_status_name)                                               AS dev_status_name,

    -- 4. 连续数值指标二次聚合 (AVG/MAX/MIN)
    avg(hbm_util_avg)                                                      AS hbm_util_avg,
    max(hbm_util_max)                                                      AS hbm_util_max,
    min(hbm_util_min)                                                      AS hbm_util_min,

    avg(ai_core_util_avg)                                                  AS ai_core_util_avg,
    max(ai_core_util_max)                                                  AS ai_core_util_max,
    min(ai_core_util_min)                                                  AS ai_core_util_min,
    argMax(ai_core_util_argMax, f.window_time)                               AS ai_core_util_argMax,

    avg(hbm_bandwidth_util_avg)                                            AS hbm_bandwidth_util_avg,
    max(hbm_bandwidth_util_max)                                            AS hbm_bandwidth_util_max,
    min(hbm_bandwidth_util_min)                                            AS hbm_bandwidth_util_min,

    avg(ai_card_temp_avg)                                                  AS ai_card_temp_avg,
    max(ai_card_temp_max)                                                  AS ai_card_temp_max,
    min(ai_card_temp_min)                                                  AS ai_card_temp_min,

    avg(ai_card_power_avg)                                                 AS ai_card_power_avg,
    max(ai_card_power_max)                                                 AS ai_card_power_max,
    min(ai_card_power_min)                                                 AS ai_card_power_min,

    avg(bandwidth_tx_avg)                                                  AS bandwidth_tx_avg,
    max(bandwidth_tx_max)                                                  AS bandwidth_tx_max,
    min(bandwidth_tx_min)                                                  AS bandwidth_tx_min,

    avg(bandwidth_rx_avg)                                                  AS bandwidth_rx_avg,
    max(bandwidth_rx_max)                                                  AS bandwidth_rx_max,
    min(bandwidth_rx_min)                                                  AS bandwidth_rx_min,
    
    argMax(ai_card_health_argMax, f.window_time)                             AS ai_card_health_argMax,

    -- 5. 健康/异常计数器累加与占比重新计算（加权平均）
    sum(ai_card_normal_state_counter)                                      AS ai_card_normal_state_counter,
    sum(ai_card_abnormal_state_counter)                                    AS ai_card_abnormal_state_counter,
    CAST(
        sum(f.ai_card_normal_state_counter) / nullIf(sum(f.ai_card_normal_state_counter) + sum(f.ai_card_abnormal_state_counter), 0)
        AS Nullable(Float32)
    )                                                                      AS ai_card_health_ratio_avg,
    CAST(
        sum(f.ai_card_abnormal_state_counter) / nullIf(sum(f.ai_card_normal_state_counter) + sum(f.ai_card_abnormal_state_counter), 0)
        AS Nullable(Float32)
    )                                                                      AS ai_card_offline_ratio_avg,

    -- 6. 使用率相关计数器累加与占比重新计算（加权平均）
    argMax(f.ai_card_used_argMax, f.window_time)                           AS ai_card_used_argMax,
    sum(f.ai_card_used_counter)                                              AS ai_card_used_counter,
    sum(f.ai_card_unused_counter)                                            AS ai_card_unused_counter,
    sum(f.ai_card_used_active_counter)                                       AS ai_card_used_active_counter,
    CAST(
        sum(f.ai_card_used_active_counter) / nullIf(sum(f.ai_card_used_counter), 0)
        AS Nullable(Float32)
    )                                                                      AS ai_card_used_ratio_avg,
    
    -- 7.算力容量/分配累加指标
    argMax(f.ai_card_computing_power_argMax, f.window_time)                      AS ai_card_computing_power_argMax,
    sum(f.ai_card_used_computing_power_counter)                                    AS ai_card_used_computing_power_counter,
    sum(f.ai_card_unused_computing_power_counter)                                  AS ai_card_unused_computing_power_counter,

    -- 8. 标记位透传
    any(f.sync_flag)                                                         AS sync_flag

FROM modelmate_metric.t_mops_gpu_5min_dws AS f
/* 调度时按小时传入时间约束区间，例如按小时增量调度 */
WHERE f.window_time >=  toDateTime({start_time:UInt32}, 'UTC')
  AND f.window_time <  toDateTime({end_time:UInt32}, 'UTC')
  AND f.sync_flag = 0 -- 仅针对探针汇聚数据进行二次汇聚
GROUP BY
    toDateTime({start_time:UInt32}, {system_timezone:String}),
    f.ai_card_type,
    f.ai_card_id,
    f.chip_id;

