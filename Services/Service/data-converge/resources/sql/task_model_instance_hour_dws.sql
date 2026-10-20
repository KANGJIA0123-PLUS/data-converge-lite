-- 5.1.2 由t_mops_modelinstance_5min_dws 汇聚生成 t_mops_modelinstance_hour_dws
INSERT INTO modelmate_metric.t_mops_modelinstance_hour_dws(
    window_time,
    server_name,
    modelinstance_id,
    group_id,
    container_id,
    modelinstance_name,
    framework_type,
    framework_version,
    deploy_info,
    process_role,
    model_name,
    model_id,
    node_id,
    node_ip,
    node_name,
    node_type,
    ai_card_id,
    chip_id,
    dev_mfr,
    dev_model,
    dev_status_code,
    dev_status_name,

    container_cpu_util_avg,
    container_cpu_util_max,
    container_cpu_util_min,
    container_cpu_util_argMax,

    container_mem_util_avg,
    container_mem_util_max,
    container_mem_util_min,
    container_mem_util_argMax,

    container_mem_usage_mb_avg,
    container_mem_usage_mb_max,
    container_mem_usage_mb_min,
    container_mem_usage_mb_argMax,

    container_disk_usage_mb_avg,
    container_disk_usage_mb_max,
    container_disk_usage_mb_min,
    container_disk_usage_mb_argMax,

    container_npu_util_avg,
    container_npu_util_max,
    container_npu_util_min,
    container_npu_util_argMax,

    container_hbm_util_avg,
    container_hbm_util_max,
    container_hbm_util_min,
    container_hbm_util_argMax,

    container_recv_bytes_rate_avg,
    container_recv_bytes_rate_max,
    container_recv_bytes_rate_min,
    container_recv_bytes_rate_argMax,

    container_send_bytes_rate_avg,
    container_send_bytes_rate_max,
    container_send_bytes_rate_min,
    container_send_bytes_rate_argMax,

    container_tx_pack_drop_rate_avg,
    container_tx_pack_drop_rate_max,
    container_tx_pack_drop_rate_min,
    container_tx_pack_drop_rate_argMax,

    container_rx_pack_drop_rate_avg,
    container_rx_pack_drop_rate_max,
    container_rx_pack_drop_rate_min,
    container_rx_pack_drop_rate_argMax,

    sync_flag
)
SELECT
    -- 1. 时间维度：对齐到 小时窗口起点
    toDateTime({start_time:UInt32}, {system_timezone:String})                          AS window_time,

    -- 2. 核心维度键 (GROUP BY)
    f.server_name,
    f.modelinstance_id,
    ifNull(f.group_id, '')                                 AS group_id,
    f.container_id,

    -- 3. 透传维度/属性字段：使用 any() 拿取静态属性，使用 anyLast() 锁死当前窗口最新状态
    any(f.modelinstance_name)                             AS modelinstance_name,
    any(f.framework_type)                                  AS framework_type,
    any(f.framework_version)                               AS framework_version,
    any(f.deploy_info)                                     AS deploy_info,
    any(f.process_role)                                   AS process_role,
    any(f.model_name)                                     AS model_name,
    any(f.model_id)                                        AS model_id,
    any(f.node_id)                                        AS node_id,
    any(f.node_ip)                                        AS node_ip,
    any(f.node_name)                                      AS node_name,
    any(f.node_type)                                      AS node_type,
    any(f.ai_card_id)                                     AS ai_card_id,
    any(f.chip_id)                                        AS chip_id,
    any(f.dev_mfr)                                         AS dev_mfr,
    any(f.dev_model)                                       AS dev_model,
    anyLast(f.dev_status_code)                             AS dev_status_code,
    anyLast(f.dev_status_name)                             AS dev_status_name,

    -- 4. 连续数值指标聚合 (AVG / MAX / MIN / argMax )
    avg(f.container_cpu_util_avg)                          AS container_cpu_util_avg,
    max(f.container_cpu_util_max)                          AS container_cpu_util_max,
    min(f.container_cpu_util_min)                          AS container_cpu_util_min,
    argMax(f.container_cpu_util_argMax, f.window_time)       AS container_cpu_util_argMax,

    avg(f.container_mem_util_avg)                          AS container_mem_util_avg,
    max(f.container_mem_util_max)                          AS container_mem_util_max,
    min(f.container_mem_util_min)                          AS container_mem_util_min,
    argMax(f.container_mem_util_argMax, f.window_time)       AS container_mem_util_argMax,

    avg(f.container_mem_usage_mb_avg)                      AS container_mem_usage_mb_avg,
    max(f.container_mem_usage_mb_max)                      AS container_mem_usage_mb_max,
    min(f.container_mem_usage_mb_min)                      AS container_mem_usage_mb_min,
    argMax(f.container_mem_usage_mb_argMax, f.window_time)   AS container_mem_usage_mb_argMax,

    avg(f.container_disk_usage_mb_avg)                    AS container_disk_usage_mb_avg,
    max(f.container_disk_usage_mb_max)                     AS container_disk_usage_mb_max,
    min(f.container_disk_usage_mb_min)                     AS container_disk_usage_mb_min,
    argMax(f.container_disk_usage_mb_argMax, f.window_time)  AS container_disk_usage_mb_argMax,

    avg(f.container_npu_util_avg)                          AS container_npu_util_avg,
    max(f.container_npu_util_max)                          AS container_npu_util_max,
    min(f.container_npu_util_min)                          AS container_npu_util_min,
    argMax(f.container_npu_util_argMax, f.window_time)       AS container_npu_util_argMax,

    avg(f.container_hbm_util_avg)                          AS container_hbm_util_avg,
    max(f.container_hbm_util_max)                          AS container_hbm_util_max,
    min(f.container_hbm_util_min)                          AS container_hbm_util_min,
    argMax(f.container_hbm_util_argMax, f.window_time)       AS container_hbm_util_argMax,

    avg(f.container_recv_bytes_rate_avg)                   AS container_recv_bytes_rate_avg,
    max(f.container_recv_bytes_rate_max)                   AS container_recv_bytes_rate_max,
    min(f.container_recv_bytes_rate_min)                   AS container_recv_bytes_rate_min,
    argMax(f.container_recv_bytes_rate_argMax, f.window_time) AS container_recv_bytes_rate_argMax,

    avg(f.container_send_bytes_rate_avg)                    AS container_send_bytes_rate_avg,
    max(f.container_send_bytes_rate_max)                    AS container_send_bytes_rate_max,
    min(f.container_send_bytes_rate_min)                    AS container_send_bytes_rate_min,
    argMax(f.container_send_bytes_rate_argMax, f.window_time) AS container_send_bytes_rate_argMax,

    avg(f.container_tx_pack_drop_rate_avg)                   AS container_tx_pack_drop_rate_avg,
    max(f.container_tx_pack_drop_rate_max)                   AS container_tx_pack_drop_rate_max,
    min(f.container_tx_pack_drop_rate_min)                   AS container_tx_pack_drop_rate_min,
    argMax(f.container_tx_pack_drop_rate_argMax, f.window_time) AS container_tx_pack_drop_rate_argMax,

    avg(f.container_rx_pack_drop_rate_avg)                    AS container_rx_pack_drop_rate_avg,
    max(f.container_rx_pack_drop_rate_max)                    AS container_rx_pack_drop_rate_max,
    min(f.container_rx_pack_drop_rate_min)                    AS container_rx_pack_drop_rate_min,
    argMax(f.container_rx_pack_drop_rate_argMax, f.window_time) AS container_rx_pack_drop_rate_argMax,

    -- 8. 数据同步标记 (直接透传或按场景取 anyLast)
    any(f.sync_flag)                                                    AS sync_flag

FROM modelmate_metric.t_mops_modelinstance_5min_dws AS f
/* 增量计算时可解除注释添加时间范围约束*/
WHERE f.window_time >= toDateTime({start_time:UInt32}, 'UTC')
  AND f.window_time <  toDateTime({end_time:UInt32}, 'UTC')
  AND f.sync_flag = 0 -- 只针对探针实时上报的数据做小时汇聚
GROUP BY
    toDateTime({start_time:UInt32}, {system_timezone:String}),
    f.server_name,
    f.modelinstance_id,
    f.group_id,
    f.container_id;
