-- 2.1 t_mops_node_realtime_dwd -> t_mops_node_5min_dws
INSERT INTO modelmate_metric.t_mops_node_5min_dws (
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
    -- 1. 时间窗口切分 (取5分钟窗口起始时间)
    toDateTime({start_time:UInt32}, {system_timezone:String}) AS window_time,

    -- 2. 维度字段 (按分组维度聚合)
    -- 注意：如果 node_type 在 DWD 中有 NULL 值，可配合 coalesce 处理或直接回填
    coalesce(r.node_type, 'UNKNOWN')                     AS node_type,
    r.node_id,
    r.node_ip,

    -- 3. 节点属性字段 (不参与数值计算，通过 argMax 取窗口内最新一条数据记录的状态)
    argMax(r.node_name, r.metric_datetime)               AS node_name,
    argMax(r.dev_mfr, r.metric_datetime)                 AS dev_mfr,
    argMax(r.dev_model, r.metric_datetime)               AS dev_model,
    argMax(r.dev_status_code, r.metric_datetime)         AS dev_status_code,
    argMax(r.dev_status_name, r.metric_datetime)         AS dev_status_name,

    -- 4. 汇聚指标 (基础监控指标: AVG / MAX / MIN)
    avg(r.cpu_util)                                      AS cpu_util_avg,
    max(r.cpu_util)                                      AS cpu_util_max,
    min(r.cpu_util)                                      AS cpu_util_min,

    avg(r.mem_util)                                      AS mem_util_avg,
    max(r.mem_util)                                      AS mem_util_max,
    min(r.mem_util)                                      AS mem_util_min,

    avg(r.disk_util)                                     AS disk_util_avg,
    max(r.disk_util)                                     AS disk_util_max,
    min(r.disk_util)                                     AS disk_util_min,

    avg(r.cpu_usage)                                     AS cpu_usage_avg,
    max(r.cpu_usage)                                     AS cpu_usage_max,
    min(r.cpu_usage)                                     AS cpu_usage_min,

    avg(r.mem_usage)                                     AS mem_usage_avg,
    max(r.mem_usage)                                     AS mem_usage_max,
    min(r.mem_usage)                                     AS mem_usage_min,

    avg(r.disk_total)                                    AS disk_total_avg,
    max(r.disk_total)                                    AS disk_total_max,
    min(r.disk_total)                                    AS disk_total_min,
    argMax(r.disk_total, r.metric_datetime)              AS disk_total_argMax,

    avg(r.disk_usage)                                    AS disk_usage_avg,
    max(r.disk_usage)                                    AS disk_usage_max,
    min(r.disk_usage)                                    AS disk_usage_min,

    avg(r.disk_io_ps)                                    AS disk_io_ps_avg,
    max(r.disk_io_ps)                                    AS disk_io_ps_max,
    min(r.disk_io_ps)                                    AS disk_io_ps_min,

    avg(r.disk_read_rate)                                AS disk_read_rate_avg,
    max(r.disk_read_rate)                                AS disk_read_rate_max,
    min(r.disk_read_rate)                                AS disk_read_rate_min,

    avg(r.disk_write_rate)                               AS disk_write_rate_avg,
    max(r.disk_write_rate)                               AS disk_write_rate_max,
    min(r.disk_write_rate)                               AS disk_write_rate_min,

    avg(r.network_in_mbps)                               AS network_in_mbps_avg,
    max(r.network_in_mbps)                               AS network_in_mbps_max,
    min(r.network_in_mbps)                               AS network_in_mbps_min,

    avg(r.network_out_mbps)                              AS network_out_mbps_avg,
    max(r.network_out_mbps)                              AS network_out_mbps_max,
    min(r.network_out_mbps)                              AS network_out_mbps_min,

    -- 5. 最新容量/硬件配置指标 (静态/准静态配置，统一取窗口最新值)
    argMax(r.cpu_total, r.metric_datetime)               AS cpu_total_argMax,
    argMax(r.hbm_total, r.metric_datetime)               AS hbm_total_argMax,
    argMax(r.ai_card_total, r.metric_datetime)           AS ai_card_total_argMax,
    argMax(r.mem_total, r.metric_datetime)               AS mem_total_argMax,
    argMax(r.computing_power_total, r.metric_datetime)   AS computing_power_total_argMax,
    argMax(r.node_build_status, r.metric_datetime)       AS node_build_status_argMax,  -- 最新的节点建设状态 0是未建设，1是建设中

    countIf(r.node_build_status = 1)                     AS node_build_status_ok_counter, -- 最新的节点建设OK状态计数器
    countIf(r.node_build_status = 0)                     AS node_build_status_nok_counter, -- 最新的节点建设不OK状态计数器
    CAST(
        countIf(r.node_build_status = 1) / nullIf(countIf(r.node_build_status = 1) + countIf(r.node_build_status = 0), 0)
        AS Nullable(Float32)
    )                                                   AS node_active_ratio_avg,
    -- 节点已经分配的算力卡数：当节点 node_build_status = 1 的时候返回 ai_card_total，否则返回 0
    if(argMax(r.node_build_status, r.metric_datetime) = 1,
       coalesce(argMax(r.ai_card_total, r.metric_datetime), 0),
       0)                                               AS allocated_ai_card_num,

    -- 6. 数据同步标志 (按 sync_flag 分组汇聚或取最新)
    any(r.sync_flag)                                     AS sync_flag

FROM modelmate_metric.t_mops_node_realtime_dwd AS r
-- 【动态化优化】使用外部传入的起止时间范围替代写死的 now()
WHERE r.metric_datetime >= toDateTime({start_time:UInt32}, 'UTC')
  AND r.metric_datetime <  toDateTime({end_time:UInt32}, 'UTC')
  AND r.sync_flag = 0
GROUP BY
    toDateTime({start_time:UInt32}, {system_timezone:String}),
    coalesce(r.node_type, 'UNKNOWN'),
    r.node_id,
    r.node_ip;
