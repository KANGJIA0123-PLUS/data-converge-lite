-- 2.2 t_mops_node_5min_dws -> t_mops_node_hour_dws
INSERT INTO modelmate_metric.t_mops_node_hour_dws (
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
    -- 1. 时间窗口切分 (取小时窗口起始时间，如 10:00:00)
    toStartOfHour(f.window_time)                           AS window_time,

    -- 2. 维度字段
    coalesce(f.node_type, 'UNKNOWN')                       AS node_type,
    f.node_id,
    f.node_ip,

    -- 3. 节点属性字段 (取小时内最新一个 5 分钟窗口的状态)
    argMax(f.node_name, f.window_time)                      AS node_name,
    argMax(f.dev_mfr, f.window_time)                        AS dev_mfr,
    argMax(f.dev_model, f.window_time)                      AS dev_model,
    argMax(f.dev_status_code, f.window_time)                AS dev_status_code,
    argMax(f.dev_status_name, f.window_time)                AS dev_status_name,

    -- 4. 汇聚指标二次聚合 (数学极值/平均值换算)
    -- 注意：极值取极值，平均值对平均值求 AVG
    avg(f.cpu_util_avg)                                   AS cpu_util_avg,
    max(f.cpu_util_max)                                   AS cpu_util_max,
    min(f.cpu_util_min)                                   AS cpu_util_min,

    avg(f.mem_util_avg)                                   AS mem_util_avg,
    max(f.mem_util_max)                                   AS mem_util_max,
    min(f.mem_util_min)                                   AS mem_util_min,

    avg(f.disk_util_avg)                                  AS disk_util_avg,
    max(f.disk_util_max)                                  AS disk_util_max,
    min(f.disk_util_min)                                  AS disk_util_min,

    avg(f.cpu_usage_avg)                                  AS cpu_usage_avg,
    max(f.cpu_usage_max)                                  AS cpu_usage_max,
    min(f.cpu_usage_min)                                  AS cpu_usage_min,

    avg(f.mem_usage_avg)                                  AS mem_usage_avg,
    max(f.mem_usage_max)                                  AS mem_usage_max,
    min(f.mem_usage_min)                                  AS mem_usage_min,

    avg(f.disk_total_avg)                                 AS disk_total_avg,
    max(f.disk_total_max)                                 AS disk_total_max,
    min(f.disk_total_min)                                 AS disk_total_min,
    argMax(f.disk_total_argMax, f.window_time)             AS disk_total_argMax,

    avg(f.disk_usage_avg)                                 AS disk_usage_avg,
    max(f.disk_usage_max)                                 AS disk_usage_max,
    min(f.disk_usage_min)                                 AS disk_usage_min,

    avg(f.disk_io_ps_avg)                                 AS disk_io_ps_avg,
    max(f.disk_io_ps_max)                                 AS disk_io_ps_max,
    min(f.disk_io_ps_min)                                 AS disk_io_ps_min,

    avg(f.disk_read_rate_avg)                             AS disk_read_rate_avg,
    max(f.disk_read_rate_max)                             AS disk_read_rate_max,
    min(f.disk_read_rate_min)                             AS disk_read_rate_min,

    avg(f.disk_write_rate_avg)                            AS disk_write_rate_avg,
    max(f.disk_write_rate_max)                            AS disk_write_rate_max,
    min(f.disk_write_rate_min)                            AS disk_write_rate_min,

    avg(f.network_in_mbps_avg)                            AS network_in_mbps_avg,
    max(f.network_in_mbps_max)                            AS network_in_mbps_max,
    min(f.network_in_mbps_min)                           AS network_in_mbps_min,

    avg(f.network_out_mbps_avg)                           AS network_out_mbps_avg,
    max(f.network_out_mbps_max)                           AS network_out_mbps_max,
    min(f.network_out_mbps_min)                           AS network_out_mbps_min,

    -- 5. 最新容量/硬件配置指标 (取小时内最新的配置快照)
    argMax(f.cpu_total_argMax, f.window_time)                 AS cpu_total_argMax,
    argMax(f.hbm_total_argMax, f.window_time)                 AS hbm_total_argMax,
    argMax(f.ai_card_total_argMax, f.window_time)             AS ai_card_total_argMax,
    argMax(f.mem_total_argMax, f.window_time)                 AS mem_total_argMax,
    argMax(f.computing_power_total_argMax, f.window_time) AS computing_power_total_argMax,

    argMax(f.node_build_status_argMax, f.window_time)        AS node_build_status_argMax,  -- 最新的节点建设状态 0是未建设，1是建设中
    sum(f.node_build_status_ok_counter)                   AS node_build_status_ok_counter, -- 最新的节点建设OK状态计数器
    sum(f.node_build_status_nok_counter)                  AS node_build_status_nok_counter, -- 最新的节点建设不OK状态计数器
    CAST(
        sum(f.node_build_status_ok_counter) / nullIf(sum(f.node_build_status_ok_counter) + sum(f.node_build_status_nok_counter), 0)
        AS Nullable(Float32)
    )                                                   AS node_active_ratio_avg,
    -- 节点已经分配的算力卡数：
    if(argMax(f.node_build_status_argMax, f.window_time) = 1,
       coalesce(argMax(f.ai_card_total_argMax, f.window_time), 0),
       0)                                                AS allocated_ai_card_num,

    -- 6. 数据同步标志
    any(f.sync_flag)                                       AS sync_flag

FROM modelmate_metric.t_mops_node_5min_dws AS f
-- 增量调度过滤条件示例：计算上一个已完成的整点小时（例如 10:00:00 ~ 11:00:00）
WHERE f.window_time >= {start_time:DateTime}
  AND f.window_time <  {end_time:DateTime}
  AND f.sync_flag = 0
GROUP BY
    toStartOfHour(f.window_time),
    coalesce(f.node_type, 'UNKNOWN'),
    f.node_id,
    f.node_ip;
