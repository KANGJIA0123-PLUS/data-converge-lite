-- 1.1 t_mops_gpu_realtime_dwd -> t_mops_gpu_5min_dws
INSERT INTO modelmate_metric.t_mops_gpu_5min_dws(
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
    -- 1. 时间维度：对齐到 5 分钟窗口起点
    toDateTime({start_time:UInt32}, {system_timezone:String})                 AS window_time,

    -- 2. 核心维度键 (GROUP BY)
    ifNull(r.ai_card_type, '')                             AS ai_card_type,
    r.ai_card_id,
    r.chip_id,

    -- 3. 透传维度/属性字段：使用 any() 拿取静态属性，使用 anyLast() 锁死当前窗口最新状态
    any(r.node_id)                                         AS node_id,
    any(r.node_ip)                                         AS node_ip,
    any(r.node_type)                                       AS node_type,
    any(r.node_name)                                       AS node_name,
    any(r.ai_card_index)                                   AS ai_card_index,
    any(r.dev_mfr)                                         AS dev_mfr,
    any(r.dev_model)                                       AS dev_model,
    anyLast(r.dev_status_code)                             AS dev_status_code,
    anyLast(r.dev_status_name)                             AS dev_status_name,

    -- 4. 连续数值指标聚合 (AVG / MAX / MIN)
    avg(r.hbm_util)                                        AS hbm_util_avg,
    max(r.hbm_util)                                        AS hbm_util_max,
    min(r.hbm_util)                                        AS hbm_util_min,
    
    avg(r.ai_core_util)                                    AS ai_core_util_avg,
    max(r.ai_core_util)                                    AS ai_core_util_max,
    min(r.ai_core_util)                                    AS ai_core_util_min,
    argMax(r.ai_core_util, r.metric_datetime)                    AS ai_core_util_argMax,
    
    avg(r.hbm_bandwidth_util)                              AS hbm_bandwidth_util_avg,
    max(r.hbm_bandwidth_util)                              AS hbm_bandwidth_util_max,
    min(r.hbm_bandwidth_util)                              AS hbm_bandwidth_util_min,
    
    avg(r.ai_card_temp)                                    AS ai_card_temp_avg,
    max(r.ai_card_temp)                                    AS ai_card_temp_max,
    min(r.ai_card_temp)                                    AS ai_card_temp_min,
    
    avg(r.ai_card_power)                                   AS ai_card_power_avg,
    max(r.ai_card_power)                                   AS ai_card_power_max,
    min(r.ai_card_power)                                   AS ai_card_power_min,
    
    avg(r.bandwidth_tx_mbps)                               AS bandwidth_tx_avg,
    max(r.bandwidth_tx_mbps)                               AS bandwidth_tx_max,
    min(r.bandwidth_tx_mbps)                               AS bandwidth_tx_min,
    
    avg(r.bandwidth_rx_mbps)                               AS bandwidth_rx_avg,
    max(r.bandwidth_rx_mbps)                               AS bandwidth_rx_max,
    min(r.bandwidth_rx_mbps)                               AS bandwidth_rx_min,
    
    argMax(r.health_status, metric_datetime)                   AS ai_card_health_argMax,

    -- 5. 健康状态相关计数器与占比
    countIf(lower(r.health_status) = 'ok')                                  AS ai_card_normal_state_counter,
    countIf(lower(ifNull(r.health_status, '')) != 'ok')                     AS ai_card_abnormal_state_counter,
    avgIf(lower(r.health_status) = 'ok', r.health_status IS NOT NULL)         AS ai_card_health_ratio_avg,
    avgIf(lower(r.health_status) != 'ok', r.health_status IS NOT NULL)        AS ai_card_offline_ratio_avg,

    -- 6. 使用率与有效负载相关计数器与占比
    argMax(r.ai_card_used, r.metric_datetime)                                 AS ai_card_used_argMax,
    countIf(r.ai_card_used = 1)                                             AS ai_card_used_counter,
    countIf(r.ai_card_used != 1 OR r.ai_card_used IS NULL)                    AS ai_card_unused_counter,
    countIf(r.ai_card_used = 1 AND ai_core_util > 0)                        AS ai_card_used_active_counter,
    ifNull(countIf(r.ai_card_used = 1) / nullIf(count(*), 0), 0)            AS ai_card_used_ratio_avg,
    
    -- 7. 算力容量/分配累加指标
    argMax(r.ai_card_computing_power, r.metric_datetime)                      AS ai_card_computing_power_argMax,
    sum(r.ai_card_used_computing_power)                                     AS ai_card_used_computing_power_counter,
    sum(r.ai_card_unused_computing_power)                                   AS ai_card_unused_computing_power_counter,

    -- 8. 数据同步标记 (直接透传或按场景取 anyLast)
    any(r.sync_flag)                                                                     AS sync_flag

FROM modelmate_metric.t_mops_gpu_realtime_dwd AS r
/* 增量计算时可解除注释添加时间范围约束*/
WHERE r.metric_datetime >= toDateTime({start_time:UInt32}, {system_timezone:String})
  AND r.metric_datetime <  toDateTime({end_time:UInt32}, {system_timezone:String})
  AND r.sync_flag = 0 -- 只针对探针实时上报的数据做 5 分钟汇聚
GROUP BY
    toDateTime({start_time:UInt32}, {system_timezone:String}),
    r.ai_card_type,
    r.ai_card_id,
    r.chip_id;
    
