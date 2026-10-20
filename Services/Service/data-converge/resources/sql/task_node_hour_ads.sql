-- 3.2 由t_mops_gpu_hour_dws与t_mops_node_hour_dws -> t_mops_node_hour_ads
INSERT INTO modelmate_metric.t_mops_node_hour_ads
(
    window_time,
    node_id,
    
    -- 2. 节点属性维度
    node_ip,
    node_type,
    node_name,
    dev_mfr,
    dev_model,
    dev_status_code,
    dev_status_name,
    
    -- 3. Node维度下的算力卡指标汇总 (由 aicard_fact 聚合)
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
    
    -- 算力容量/分配累加指标
    ai_card_computing_power_argMax,
    ai_card_used_computing_power_counter,
    ai_card_unused_computing_power_counter,
    
    -- 4. 宿主节点物理资源指标 (由 node_fact 直接透传)
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
    
    cpu_total_argMax,
    hbm_total_argMax,
    ai_card_total_argMax,
    mem_total_argMax,
    computing_power_total_argMax,
    node_build_status_argMax,
    node_build_status_ok_counter,
    node_build_status_nok_counter,
    node_active_ratio_avg,
    allocated_ai_card_num,
    
    -- 5. 跨表衍生指标 (算力卡按节点汇总的派生指标)
    ai_card_health_ratio_last,
    ai_card_health_count_last,
    ai_card_unhealth_count_last,
    ai_card_unused_num_last,
    ai_card_used_num_last,
    ai_card_used_active_count_last,
    ai_card_used_ratio_last,
    use_computing_power_last,
    free_computing_power_last, 
    ai_core_util_status, 
    
    -- 7. 调度控制
    sync_flag
)
WITH card_agg AS (
    SELECT
        dws.window_time,
        dws.node_id,
        
        -- 基础算力卡性能指标二次/加权汇聚
        avg(dws.hbm_util_avg)                  AS hbm_util_avg,
        max(dws.hbm_util_max)                  AS hbm_util_max,
        min(dws.hbm_util_min)                  AS hbm_util_min,
        avg(dws.ai_core_util_avg)              AS ai_core_util_avg,
        max(dws.ai_core_util_max)              AS ai_core_util_max,
        min(dws.ai_core_util_min)              AS ai_core_util_min,
        avg(dws.hbm_bandwidth_util_avg)        AS hbm_bandwidth_util_avg,
        max(dws.hbm_bandwidth_util_max)        AS hbm_bandwidth_util_max,
        min(dws.hbm_bandwidth_util_min)        AS hbm_bandwidth_util_min,
        avg(dws.ai_card_temp_avg)              AS ai_card_temp_avg,
        max(dws.ai_card_temp_max)              AS ai_card_temp_max,
        min(dws.ai_card_temp_min)              AS ai_card_temp_min,
        avg(dws.ai_card_power_avg)             AS ai_card_power_avg,
        max(dws.ai_card_power_max)             AS ai_card_power_max,
        min(dws.ai_card_power_min)             AS ai_card_power_min,
        avg(dws.bandwidth_tx_avg)              AS bandwidth_tx_avg,
        max(dws.bandwidth_tx_max)              AS bandwidth_tx_max,
        min(dws.bandwidth_tx_min)              AS bandwidth_tx_min,
        avg(dws.bandwidth_rx_avg)              AS bandwidth_rx_avg,
        max(dws.bandwidth_rx_max)              AS bandwidth_rx_max,
        min(dws.bandwidth_rx_min)              AS bandwidth_rx_min,

        -- 状态计数与比例汇总
        sum(dws.ai_card_normal_state_counter)   AS ai_card_normal_state_counter,
        sum(dws.ai_card_abnormal_state_counter) AS ai_card_abnormal_state_counter,
        
        -- 计算健康率与掉线率 (安全除法处理 0 场景)
        if((sum(dws.ai_card_normal_state_counter) + sum(dws.ai_card_abnormal_state_counter)) > 0, 
           sum(dws.ai_card_normal_state_counter) / (sum(dws.ai_card_normal_state_counter) + sum(dws.ai_card_abnormal_state_counter)), 
           NULL) AS ai_card_health_ratio_avg,
           
        if((sum(dws.ai_card_normal_state_counter) + sum(dws.ai_card_abnormal_state_counter)) > 0, 
           sum(dws.ai_card_abnormal_state_counter) / (sum(dws.ai_card_normal_state_counter) + sum(dws.ai_card_abnormal_state_counter)), 
           NULL) AS ai_card_offline_ratio_avg,

        sum(dws.ai_card_used_counter)          AS ai_card_used_counter,
        sum(dws.ai_card_unused_counter)        AS ai_card_unused_counter,
        sum(dws.ai_card_used_active_counter)   AS ai_card_used_active_counter,
        
        -- 被实际使用的卡时占比
        if(sum(dws.ai_card_used_counter) > 0, 
           sum(dws.ai_card_used_active_counter) / sum(dws.ai_card_used_counter), 
           NULL) AS ai_card_used_ratio_avg,

        -- 算力容量与使用计数
        sum(dws.ai_card_computing_power_argMax)        AS ai_card_computing_power_argMax,
        sum(dws.ai_card_used_computing_power_counter)  AS ai_card_used_computing_power_counter,
        sum(dws.ai_card_unused_computing_power_counter) AS ai_card_unused_computing_power_counter,

        -- 派生指标计算
        countIf(lower(dws.ai_card_health_argMax) = 'ok')  AS ai_card_health_count_last,
        countIf(lower(dws.ai_card_health_argMax) != 'ok') AS ai_card_unhealth_count_last,
        
        countIf(dws.ai_card_used_argMax != 1)             AS ai_card_unused_num_last,
        countIf(dws.ai_card_used_argMax = 1)              AS ai_card_used_num_last,
        countIf(dws.ai_card_used_argMax = 1 AND dws.ai_core_util_argMax > 0) AS ai_card_used_active_count_last,
        if(
          countIf(dws.ai_card_used_argMax = 1) > 0,
          countIf(dws.ai_card_used_argMax = 1 AND dws.ai_core_util_argMax > 0) / countIf(dws.ai_card_used_argMax = 1),
        NULL) AS ai_card_used_ratio_last
      
        
    FROM modelmate_metric.t_mops_gpu_hour_dws AS dws
    -- 使用动态调度时间窗口参数
    WHERE dws.window_time >= toDateTime({start_time:UInt32}, {system_timezone:String})
      AND dws.window_time <  toDateTime({end_time:UInt32}, {system_timezone:String})
      AND dws.sync_flag = 0
    GROUP BY dws.window_time, dws.node_id
)
SELECT
    n.window_time,
    n.node_id,

    -- 2. 节点维表/属性字段
    n.node_ip,
    n.node_type,
    n.node_name,
    n.dev_mfr,
    n.dev_model,
    n.dev_status_code,
    n.dev_status_name,

    -- 3. 算力卡汇总指标 (来自 CTE 预聚表，没有关联上则自动补 NULL)
    c.hbm_util_avg,
    c.hbm_util_max,
    c.hbm_util_min,
    c.ai_core_util_avg,
    c.ai_core_util_max,
    c.ai_core_util_min,
    c.hbm_bandwidth_util_avg,
    c.hbm_bandwidth_util_max,
    c.hbm_bandwidth_util_min,
    c.ai_card_temp_avg,
    c.ai_card_temp_max,
    c.ai_card_temp_min,
    c.ai_card_power_avg,
    c.ai_card_power_max,
    c.ai_card_power_min,
    c.bandwidth_tx_avg,
    c.bandwidth_tx_max,
    c.bandwidth_tx_min,
    c.bandwidth_rx_avg,
    c.bandwidth_rx_max,
    c.bandwidth_rx_min,

    c.ai_card_normal_state_counter,
    c.ai_card_abnormal_state_counter,
    c.ai_card_health_ratio_avg,
    c.ai_card_offline_ratio_avg,
    c.ai_card_used_counter,
    c.ai_card_unused_counter,
    c.ai_card_used_active_counter,
    c.ai_card_used_ratio_avg,

    c.ai_card_computing_power_argMax,
    c.ai_card_used_computing_power_counter,
    c.ai_card_unused_computing_power_counter,

    -- 4. 节点物理资源指标 (直接透传)
    n.cpu_util_avg,
    n.cpu_util_max,
    n.cpu_util_min,
    n.mem_util_avg,
    n.mem_util_max,
    n.mem_util_min,
    n.disk_util_avg,
    n.disk_util_max,
    n.disk_util_min,
    n.cpu_usage_avg,
    n.cpu_usage_max,
    n.cpu_usage_min,
    n.mem_usage_avg,
    n.mem_usage_max,
    n.mem_usage_min,
    n.disk_total_avg,
    n.disk_total_max,
    n.disk_total_min,
    n.disk_total_argMax,
    n.disk_usage_avg,
    n.disk_usage_max,
    n.disk_usage_min,
    n.disk_io_ps_avg,
    n.disk_io_ps_max,
    n.disk_io_ps_min,
    n.disk_read_rate_avg,
    n.disk_read_rate_max,
    n.disk_read_rate_min,
    n.disk_write_rate_avg,
    n.disk_write_rate_max,
    n.disk_write_rate_min,
    n.network_in_mbps_avg,
    n.network_in_mbps_max,
    n.network_in_mbps_min,
    n.network_out_mbps_avg,
    n.network_out_mbps_max,
    n.network_out_mbps_min,

    n.cpu_total_argMax,
    n.hbm_total_argMax,
    n.ai_card_total_argMax,
    n.mem_total_argMax,
    n.computing_power_total_argMax,
    n.node_build_status_argMax,
    n.node_build_status_ok_counter,
    n.node_build_status_nok_counter,
    n.node_active_ratio_avg,
    n.allocated_ai_card_num,

    -- 5. 衍生复合计算指标
    -- 使用 coalesce(c.ok_card_count, 0) 防止未关联上卡数据时表达式报错
    if(coalesce(n.ai_card_total_argMax, 0) > 0,
       coalesce(c.ai_card_health_count_last, 0) / n.ai_card_total_argMax,
       NULL) AS ai_card_health_ratio_last,

    c.ai_card_health_count_last,
    c.ai_card_unhealth_count_last,

    coalesce(c.ai_card_unused_num_last, 0) AS ai_card_unused_num_last,
    coalesce(c.ai_card_used_num_last, 0) AS ai_card_used_num_last,
    c.ai_card_used_active_count_last,
    c.ai_card_used_ratio_last,

    if(
        (coalesce(c.ai_card_used_num_last, 0) + coalesce(c.ai_card_unused_num_last, 0)) > 0,
        (coalesce(c.ai_card_used_num_last, 0) / (coalesce(c.ai_card_used_num_last, 0) + coalesce(c.ai_card_unused_num_last, 0))) * n.computing_power_total_argMax,
    NULL) AS use_computing_power_last,

    if(
        (coalesce(c.ai_card_used_num_last, 0) + coalesce(c.ai_card_unused_num_last, 0)) > 0,
        (coalesce(c.ai_card_unused_num_last, 0) / (coalesce(c.ai_card_used_num_last, 0) + coalesce(c.ai_card_unused_num_last, 0))) * n.computing_power_total_argMax,
    NULL) AS free_computing_power_last,

     multiIf(
        c.ai_core_util_avg IS NULL, NULL,
        c.ai_core_util_avg > 80,   'overuse',
        c.ai_core_util_avg >= 15,  'normal_use',
        c.ai_core_util_avg >= 0,   'inefficient_use',
        'unknow'
        ) AS ai_core_util_status,

    -- 7. 调度控制标志直接继承节点表
    n.sync_flag
FROM modelmate_metric.t_mops_node_hour_dws n
LEFT JOIN card_agg c
    ON n.window_time = c.window_time AND n.node_id = c.node_id
WHERE n.window_time >= toDateTime({start_time:UInt32}, {system_timezone:String})
  AND n.window_time <  toDateTime({end_time:UInt32}, {system_timezone:String})
  AND n.sync_flag = 0;
