-- 2.3 t_mops_node_hour_dws -> t_mops_node_day_dws
INSERT INTO modelmate_metric.t_mops_node_day_dws (
    window_time,
    node_type,
    node_id,
    node_ip,
    node_name,
    dev_mfr,
    dev_model,
    dev_status_code,
    dev_status_name,
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
    sync_flag
)
SELECT
    -- 1. 时间窗口切分 (按天截断，如 2026-08-04 00:00:00)
    toDateTime({start_time:UInt32}, {system_timezone:String})                          AS window_time,

    -- 2. 维度字段
    coalesce(h.node_type, 'UNKNOWN')                     AS node_type,
    h.node_id,
    h.node_ip,

    -- 3. 节点属性字段 (取一天中最新一个小时的状态)
    argMax(h.node_name, h.window_time)                   AS node_name,
    argMax(h.dev_mfr, h.window_time)                     AS dev_mfr,
    argMax(h.dev_model, h.window_time)                   AS dev_model,
    argMax(h.dev_status_code, h.window_time)             AS dev_status_code,
    argMax(h.dev_status_name, h.window_time)             AS dev_status_name,

    -- 4. 汇聚指标 (小时表 -> 天表二次计算)
    avg(h.cpu_util_avg)                                  AS cpu_util_avg,
    max(h.cpu_util_max)                                  AS cpu_util_max,
    min(h.cpu_util_min)                                  AS cpu_util_min,

    avg(h.mem_util_avg)                                  AS mem_util_avg,
    max(h.mem_util_max)                                  AS mem_util_max,
    min(h.mem_util_min)                                  AS mem_util_min,

    avg(h.disk_util_avg)                                 AS disk_util_avg,
    max(h.disk_util_max)                                 AS disk_util_max,
    min(h.disk_util_min)                                 AS disk_util_min,

    avg(h.cpu_usage_avg)                                 AS cpu_usage_avg,
    max(h.cpu_usage_max)                                 AS cpu_usage_max,
    min(h.cpu_usage_min)                                 AS cpu_usage_min,

    avg(h.mem_usage_avg)                                 AS mem_usage_avg,
    max(h.mem_usage_max)                                 AS mem_usage_max,
    min(h.mem_usage_min)                                 AS mem_usage_min,

    avg(h.disk_total_avg)                                AS disk_total_avg,
    max(h.disk_total_max)                                AS disk_total_max,
    min(h.disk_total_min)                                AS disk_total_min,
    argMax(h.disk_total_argMax, h.window_time)           AS disk_total_argMax,

    avg(h.disk_usage_avg)                                AS disk_usage_avg,
    max(h.disk_usage_max)                                AS disk_usage_max,
    min(h.disk_usage_min)                                AS disk_usage_min,

    avg(h.disk_io_ps_avg)                                AS disk_io_ps_avg,
    max(h.disk_io_ps_max)                                AS disk_io_ps_max,
    min(h.disk_io_ps_min)                                AS disk_io_ps_min,

    avg(h.disk_read_rate_avg)                            AS disk_read_rate_avg,
    max(h.disk_read_rate_max)                            AS disk_read_rate_max,
    min(h.disk_read_rate_min)                            AS disk_read_rate_min,

    avg(h.disk_write_rate_avg)                           AS disk_write_rate_avg,
    max(h.disk_write_rate_max)                           AS disk_write_rate_max,
    min(h.disk_write_rate_min)                           AS disk_write_rate_min,

    avg(h.network_in_mbps_avg)                           AS network_in_mbps_avg,
    max(h.network_in_mbps_max)                           AS network_in_mbps_max,
    min(h.network_in_mbps_min)                           AS network_in_mbps_min,

    avg(h.network_out_mbps_avg)                          AS network_out_mbps_avg,
    max(h.network_out_mbps_max)                          AS network_out_mbps_max,
    min(h.network_out_mbps_min)                          AS network_out_mbps_min,

    -- 5. 最新容量/硬件配置指标 (取一天中最新配置)
    argMax(h.cpu_total_argMax, h.window_time)            AS cpu_total_argMax,
    argMax(h.hbm_total_argMax, h.window_time)            AS hbm_total_argMax,
    argMax(h.ai_card_total_argMax, h.window_time)        AS ai_card_total_argMax,
    argMax(h.mem_total_argMax, h.window_time)            AS mem_total_argMax,
    argMax(h.computing_power_total_argMax, h.window_time) AS computing_power_total_argMax,
    argMax(h.node_build_status_argMax, h.window_time)    AS node_build_status_argMax,  -- 最新的节点建设状态 0是未建设，1是建设中
    sum(h.node_build_status_ok_counter)                   AS node_build_status_ok_counter, -- 最新的节点建设OK状态计数器
    sum(h.node_build_status_nok_counter)                  AS node_build_status_nok_counter, -- 最新的节点建设不OK状态计数器
    CAST(
        sum(h.node_build_status_ok_counter) / nullIf(sum(h.node_build_status_ok_counter) + sum(h.node_build_status_nok_counter), 0)
        AS Nullable(Float32)
    )                                                   AS node_active_ratio_avg,

    -- 节点已经分配的算力卡数：
    if(argMax(h.node_build_status_argMax, h.window_time) = 1,
       coalesce(argMax(h.ai_card_total_argMax, h.window_time), 0),
       0)                                                AS allocated_ai_card_num,

    -- 6. 数据同步标志
    any(h.sync_flag)                                     AS sync_flag

FROM modelmate_metric.t_mops_node_hour_dws AS h
-- 【动态化优化】使用外部传入的起止时间范围替代固定死的时间计算
WHERE h.window_time >= toDateTime({start_time:UInt32}, {system_timezone:String})
  AND h.window_time <  toDateTime({end_time:UInt32}, {system_timezone:String})
  AND h.sync_flag = 0
GROUP BY
    toDateTime({start_time:UInt32}, {system_timezone:String}),
    coalesce(h.node_type, 'UNKNOWN'),
    h.node_id,
    h.node_ip;
