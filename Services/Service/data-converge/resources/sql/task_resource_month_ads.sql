-- 4.4 资源粒度ADS表汇聚逻辑，由 t_mops_node_month_ads和t_mops_org_resource_flat_local -> t_mops_resource_month_ads
INSERT INTO modelmate_metric.t_mops_resource_month_ads
(
    -- 1. 时间窗口与资源维度
    window_time,
    org_id_path,
    org_id,
    -- 2. 非维度汇聚信息
    org_name,
    sync_flag,

    -- 3. 算力卡指标汇总
    hbm_util_avg,
    hbm_util_max,
    hbm_util_min,
    ai_core_util_avg,
    ai_core_util_max,
    ai_core_util_min,
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

    -- 状态计数与比例指标
    ai_card_normal_state_counter,
    ai_card_abnormal_state_counter,
    ai_card_health_ratio_avg,
    ai_card_offline_ratio_avg,
    ai_card_used_counter,
    ai_card_unused_counter,
    ai_card_used_active_counter,
    ai_card_used_ratio_avg,

    -- 算力容量/分配指标
    ai_card_computing_power_argMax,
    ai_card_used_computing_power_counter,
    ai_card_unused_computing_power_counter,

    -- 4. 节点物理资源汇聚指标
    cpu_util_avg,
    cpu_util_max,
    cpu_util_min,
    mem_util_avg,
    mem_util_max,
    mem_util_min,
    disk_util_avg,
    disk_util_max,
    disk_util_min,
    cpu_usage_avg,
    cpu_usage_max,
    cpu_usage_min,
    mem_usage_avg,
    mem_usage_max,
    mem_usage_min,
    disk_total_avg,
    disk_total_max,
    disk_total_min,
    disk_total_argMax,
    disk_usage_avg,
    disk_usage_max,
    disk_usage_min,
    disk_io_ps_avg,
    disk_io_ps_max,
    disk_io_ps_min,
    disk_read_rate_avg,
    disk_read_rate_max,
    disk_read_rate_min,
    disk_write_rate_avg,
    disk_write_rate_max,
    disk_write_rate_min,
    network_in_mbps_avg,
    network_in_mbps_max,
    network_in_mbps_min,
    network_out_mbps_avg,
    network_out_mbps_max,
    network_out_mbps_min,

    -- 资源容量总量
    cpu_total_argMax,
    hbm_total_argMax,
    ai_card_total_argMax,
    mem_total_argMax,
    computing_power_total_argMax,

    -- 建设状态与卡分配汇总指标
    node_build_status_argMax,
    node_build_status_ok_counter,
    node_build_status_nok_counter,
    node_active_ratio_avg,
    node_active_ratio_last,
    allocated_ai_card_num,
    ai_card_health_ratio_last,
    ai_card_used_ratio_last,
    ai_card_offline_ratio_last,
    allocated_computing_power,
    allocated_hbm_gb,
    allocated_cpu_cores,
    allocated_mem_mb,
    allocated_disk_gb,
    use_computing_power_last,
    free_computing_power_last,
    ai_card_unused_num_last,
    ai_card_used_num_last,
    ai_core_util_status, 
    work_time_ai_core_util_avg,
    work_time_ai_core_util_max,
    work_time_ai_core_util_min,
    idle_time_ai_core_util_avg,
    idle_time_ai_core_util_max,
    idle_time_ai_core_util_min,
    work_time_ai_core_util_status
    

)
SELECT
    -- 1. 时间窗口与维度
    n.window_time,
    r.org_id_path,
    r.org_id,

    -- 非汇聚信息 取维表更新时间最新的 org_name
    argMax(r.org_name, r.updated_time) AS org_name,
    -- 非汇聚信息 取任意 sync_flag
    any(n.sync_flag)                        AS sync_flag,

    -- 3. 算力卡指标二次汇聚
    avg(n.hbm_util_avg)                  AS hbm_util_avg,
    max(n.hbm_util_max)                  AS hbm_util_max,
    min(n.hbm_util_min)                  AS hbm_util_min,
    avg(n.ai_core_util_avg)              AS ai_core_util_avg,
    max(n.ai_core_util_max)              AS ai_core_util_max,
    min(n.ai_core_util_min)              AS ai_core_util_min,
    avg(n.hbm_bandwidth_util_avg)        AS hbm_bandwidth_util_avg,
    max(n.hbm_bandwidth_util_max)        AS hbm_bandwidth_util_max,
    min(n.hbm_bandwidth_util_min)        AS hbm_bandwidth_util_min,
    avg(n.ai_card_temp_avg)              AS ai_card_temp_avg,
    max(n.ai_card_temp_max)              AS ai_card_temp_max,
    min(n.ai_card_temp_min)              AS ai_card_temp_min,
    avg(n.ai_card_power_avg)             AS ai_card_power_avg,
    max(n.ai_card_power_max)             AS ai_card_power_max,
    min(n.ai_card_power_min)             AS ai_card_power_min,
    avg(n.bandwidth_tx_avg)              AS bandwidth_tx_avg,
    max(n.bandwidth_tx_max)              AS bandwidth_tx_max,
    min(n.bandwidth_tx_min)              AS bandwidth_tx_min,
    avg(n.bandwidth_rx_avg)              AS bandwidth_rx_avg,
    max(n.bandwidth_rx_max)              AS bandwidth_rx_max,
    min(n.bandwidth_rx_min)              AS bandwidth_rx_min,

    -- 算力卡状态计数与比例重算
    sum(n.ai_card_normal_state_counter)   AS ai_card_normal_state_counter,
    sum(n.ai_card_abnormal_state_counter) AS ai_card_abnormal_state_counter,

    if((coalesce(sum(n.ai_card_normal_state_counter), 0) + coalesce(sum(n.ai_card_abnormal_state_counter), 0)) > 0,
       sum(n.ai_card_normal_state_counter) / (sum(n.ai_card_normal_state_counter) + sum(n.ai_card_abnormal_state_counter)),
       NULL) AS ai_card_health_ratio_avg,

    if((coalesce(sum(n.ai_card_normal_state_counter), 0) + coalesce(sum(n.ai_card_abnormal_state_counter), 0)) > 0,
       sum(n.ai_card_abnormal_state_counter) / (sum(n.ai_card_normal_state_counter) + sum(n.ai_card_abnormal_state_counter)),
       NULL) AS ai_card_offline_ratio_avg,

    sum(n.ai_card_used_counter)        AS ai_card_used_counter,
    sum(n.ai_card_unused_counter)      AS ai_card_unused_counter,
    sum(n.ai_card_used_active_counter) AS ai_card_used_active_counter,

    if(coalesce(sum(n.ai_card_used_counter), 0) > 0,
       sum(n.ai_card_used_active_counter) / sum(n.ai_card_used_counter),
       NULL) AS ai_card_used_ratio_avg,

    -- 算力容量与分配
    sum(n.ai_card_computing_power_argMax)        AS ai_card_computing_power_argMax,
    sum(n.ai_card_used_computing_power_counter)  AS ai_card_used_computing_power_counter,
    sum(n.ai_card_unused_computing_power_counter) AS ai_card_unused_computing_power_counter,

    -- 4. 节点物理资源汇总
    avg(n.cpu_util_avg)         AS cpu_util_avg,
    max(n.cpu_util_max)         AS cpu_util_max,
    min(n.cpu_util_min)         AS cpu_util_min,
    avg(n.mem_util_avg)         AS mem_util_avg,
    max(n.mem_util_max)         AS mem_util_max,
    min(n.mem_util_min)         AS mem_util_min,
    avg(n.disk_util_avg)        AS disk_util_avg,
    max(n.disk_util_max)        AS disk_util_max,
    min(n.disk_util_min)        AS disk_util_min,
    avg(n.cpu_usage_avg)        AS cpu_usage_avg,
    max(n.cpu_usage_max)        AS cpu_usage_max,
    min(n.cpu_usage_min)        AS cpu_usage_min,
    avg(n.mem_usage_avg)        AS mem_usage_avg,
    max(n.mem_usage_max)        AS mem_usage_max,
    min(n.mem_usage_min)        AS mem_usage_min,
    avg(n.disk_total_avg)       AS disk_total_avg,
    max(n.disk_total_max)       AS disk_total_max,
    min(n.disk_total_min)       AS disk_total_min,
    sum(n.disk_total_argMax)    AS disk_total_argMax,
    avg(n.disk_usage_avg)       AS disk_usage_avg,
    max(n.disk_usage_max)       AS disk_usage_max,
    min(n.disk_usage_min)       AS disk_usage_min,
    avg(n.disk_io_ps_avg)       AS disk_io_ps_avg,
    max(n.disk_io_ps_max)       AS disk_io_ps_max,
    min(n.disk_io_ps_min)       AS disk_io_ps_min,
    avg(n.disk_read_rate_avg)   AS disk_read_rate_avg,
    max(n.disk_read_rate_max)   AS disk_read_rate_max,
    min(n.disk_read_rate_min)   AS disk_read_rate_min,
    avg(n.disk_write_rate_avg)  AS disk_write_rate_avg,
    max(n.disk_write_rate_max)  AS disk_write_rate_max,
    min(n.disk_write_rate_min)  AS disk_write_rate_min,
    avg(n.network_in_mbps_avg)  AS network_in_mbps_avg,
    max(n.network_in_mbps_max)  AS network_in_mbps_max,
    min(n.network_in_mbps_min)  AS network_in_mbps_min,
    avg(n.network_out_mbps_avg) AS network_out_mbps_avg,
    max(n.network_out_mbps_max) AS network_out_mbps_max,
    min(n.network_out_mbps_min) AS network_out_mbps_min,

    -- 资源池总量指标
    sum(n.cpu_total_argMax)             AS cpu_total_argMax,
    sum(n.hbm_total_argMax)             AS hbm_total_argMax,
    sum(n.ai_card_total_argMax)          AS ai_card_total_argMax,
    sum(n.mem_total_argMax)             AS mem_total_argMax,
    sum(n.computing_power_total_argMax) AS computing_power_total_argMax,

    -- 建设状态与已分配卡数指标
    max(n.node_build_status_argMax)     AS node_build_status_argMax,
    sum(n.node_build_status_ok_counter)  AS node_build_status_ok_counter,
    sum(n.node_build_status_nok_counter) AS node_build_status_nok_counter,

    -- 资源池已分配算力占比重算
    if((coalesce(sum(n.node_build_status_ok_counter), 0) + coalesce(sum(n.node_build_status_nok_counter), 0)) > 0,
       sum(n.node_build_status_ok_counter) / (sum(n.node_build_status_ok_counter) + sum(n.node_build_status_nok_counter)),
       NULL) AS node_active_ratio_avg,

    -- node_active_ratio_last 重算
    if(count(n.node_build_status_argMax) > 0,
       countIf(n.node_build_status_argMax = 1) / count(n.node_build_status_argMax),
       NULL) AS node_active_ratio_last,

    sum(n.allocated_ai_card_num)        AS allocated_ai_card_num,

    if(coalesce(sum(n.ai_card_total_argMax), 0) > 0,
    sum(n.ai_card_health_count_last) / sum(n.ai_card_total_argMax),
    NULL) AS ai_card_health_ratio_last,

    if((coalesce(sum(n.ai_card_used_active_count_last), 0) + coalesce(sum(n.ai_card_used_num_last), 0)) > 0,
       sum(n.ai_card_used_active_count_last) / (sum(n.ai_card_used_active_count_last) + sum(n.ai_card_used_num_last)),
       NULL) AS ai_card_used_ratio_last,

    if((coalesce(sum(n.ai_card_used_num_last), 0)) > 0,
       sum(n.ai_card_unused_num_last) / sum(n.ai_card_used_num_last),
       NULL) AS ai_card_offline_ratio_last,

    sumIf(n.computing_power_total_argMax, n.node_build_status_argMax = 1) AS allocated_computing_power,
    sumIf(n.hbm_total_argMax, n.node_build_status_argMax = 1) AS allocated_hbm_gb,
    sumIf(n.cpu_total_argMax, n.node_build_status_argMax = 1) AS allocated_cpu_cores,
    sumIf(n.mem_total_argMax, n.node_build_status_argMax = 1) AS allocated_mem_mb,
    sumIf(n.disk_total_argMax, n.node_build_status_argMax = 1) AS allocated_disk_gb,
    sum(n.use_computing_power_last) AS use_computing_power_last,
    sum(n.free_computing_power_last) AS free_computing_power_last,
    sum(n.ai_card_unused_num_last)  AS ai_card_unused_num_last,
    sum(n.ai_card_used_num_last)    AS ai_card_used_num_last,

    multiIf(
        avg(n.ai_core_util_avg) IS NULL, 'unknow',
        avg(n.ai_core_util_avg) > 80,   'overuse',
        avg(n.ai_core_util_avg) >= 15,  'normal_use',
        avg(n.ai_core_util_avg) >= 0,   'inefficient_use',
        'unknow'
    ) AS ai_core_util_status,
    avg(n.work_time_ai_core_util_avg)        AS work_time_ai_core_util_avg,
    max(n.work_time_ai_core_util_max)        AS work_time_ai_core_util_max,
    min(n.work_time_ai_core_util_min)        AS work_time_ai_core_util_min,
    avg(n.idle_time_ai_core_util_avg)        AS idle_time_ai_core_util_avg,
    max(n.idle_time_ai_core_util_max)        AS idle_time_ai_core_util_max,
    min(n.idle_time_ai_core_util_min)        AS idle_time_ai_core_util_min,
    multiIf(
        avg(n.work_time_ai_core_util_avg) IS NULL, 'unknow',
        avg(n.work_time_ai_core_util_avg) > 80,   'overuse',
        avg(n.work_time_ai_core_util_avg) >= 15,  'normal_use',
        avg(n.work_time_ai_core_util_avg) >= 0,   'inefficient_use',
        'unknow'
    ) AS work_time_ai_core_util_status

FROM modelmate_metric.t_mops_node_month_ads n
INNER JOIN modelmate_metric.t_mops_org_resource_flat_local r
    ON n.node_ip = r.node_ip
WHERE n.window_time >= {start_time:DateTime}
  AND n.window_time <  {end_time:DateTime}
  AND r.is_deleted = 0
  AND n.sync_flag = 0
GROUP BY
    n.window_time,
    r.org_id_path,
    r.org_id
    
