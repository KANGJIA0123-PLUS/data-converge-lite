-- 5.1.4 由t_mops_modelinstance_day_dws 汇聚生成 t_mops_modelinstance_month_dws
INSERT INTO modelmate_metric.t_mops_modelinstance_month_dws(
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
    -- 1. 时间窗口切分 (按月截断，如 2026-08-01 00:00:00)
    toDateTime({start_time:UInt32}, {system_timezone:String})                          AS window_time,

    -- 2. 核心维度键 (GROUP BY)
    d.server_name,
    d.modelinstance_id,
    ifNull(d.group_id, '')                                 AS group_id,
    d.container_id,

    -- 3. 透传维度/属性字段：使用 any() 拿取静态属性，使用 anyLast() 锁死当前窗口最新状态
    any(d.modelinstance_name)                             AS modelinstance_name,
    any(d.framework_type)                                 AS framework_type,
    any(d.framework_version)                              AS framework_version,
    any(d.deploy_info)                                    AS deploy_info,
    any(d.process_role)                                   AS process_role,
    any(d.model_name)                                     AS model_name,
    any(d.model_id)                                       AS model_id,
    any(d.node_id)                                        AS node_id,
    any(d.node_ip)                                        AS node_ip,
    any(d.node_name)                                      AS node_name,
    any(d.node_type)                                      AS node_type,
    any(d.ai_card_id)                                     AS ai_card_id,
    any(d.chip_id)                                        AS chip_id,
    any(d.dev_mfr)                                        AS dev_mfr,
    any(d.dev_model)                                      AS dev_model,
    anyLast(d.dev_status_code)                            AS dev_status_code,
    anyLast(d.dev_status_name)                            AS dev_status_name,

    -- 4. 连续数值指标聚合 (AVG / MAX / MIN / argMax )
    avg(d.container_cpu_util_avg)                          AS container_cpu_util_avg,
    max(d.container_cpu_util_max)                          AS container_cpu_util_max,
    min(d.container_cpu_util_min)                          AS container_cpu_util_min,
    argMax(d.container_cpu_util_argMax, d.window_time)       AS container_cpu_util_argMax,

    avg(d.container_mem_util_avg)                          AS container_mem_util_avg,
    max(d.container_mem_util_max)                          AS container_mem_util_max,
    min(d.container_mem_util_min)                          AS container_mem_util_min,
    argMax(d.container_mem_util_argMax, d.window_time)       AS container_mem_util_argMax,

    avg(d.container_mem_usage_mb_avg)                      AS container_mem_usage_mb_avg,
    max(d.container_mem_usage_mb_max)                      AS container_mem_usage_mb_max,
    min(d.container_mem_usage_mb_min)                      AS container_mem_usage_mb_min,
    argMax(d.container_mem_usage_mb_argMax, d.window_time)   AS container_mem_usage_mb_argMax,

    avg(d.container_disk_usage_mb_avg)                    AS container_disk_usage_mb_avg,
    max(d.container_disk_usage_mb_max)                     AS container_disk_usage_mb_max,
    min(d.container_disk_usage_mb_min)                     AS container_disk_usage_mb_min,
    argMax(d.container_disk_usage_mb_argMax, d.window_time)  AS container_disk_usage_mb_argMax,

    avg(d.container_npu_util_avg)                          AS container_npu_util_avg,
    max(d.container_npu_util_max)                          AS container_npu_util_max,
    min(d.container_npu_util_min)                          AS container_npu_util_min,
    argMax(d.container_npu_util_argMax, d.window_time)       AS container_npu_util_argMax,

    avg(d.container_hbm_util_avg)                          AS container_hbm_util_avg,
    max(d.container_hbm_util_max)                          AS container_hbm_util_max,
    min(d.container_hbm_util_min)                          AS container_hbm_util_min,
    argMax(d.container_hbm_util_argMax, d.window_time)       AS container_hbm_util_argMax,

    avg(d.container_recv_bytes_rate_avg)                   AS container_recv_bytes_rate_avg,
    max(d.container_recv_bytes_rate_max)                   AS container_recv_bytes_rate_max,
    min(d.container_recv_bytes_rate_min)                   AS container_recv_bytes_rate_min,
    argMax(d.container_recv_bytes_rate_argMax, d.window_time) AS container_recv_bytes_rate_argMax,

    avg(d.container_send_bytes_rate_avg)                    AS container_send_bytes_rate_avg,
    max(d.container_send_bytes_rate_max)                    AS container_send_bytes_rate_max,
    min(d.container_send_bytes_rate_min)                    AS container_send_bytes_rate_min,
    argMax(d.container_send_bytes_rate_argMax, d.window_time) AS container_send_bytes_rate_argMax,

    avg(d.container_tx_pack_drop_rate_avg)                   AS container_tx_pack_drop_rate_avg,
    max(d.container_tx_pack_drop_rate_max)                   AS container_tx_pack_drop_rate_max,
    min(d.container_tx_pack_drop_rate_min)                   AS container_tx_pack_drop_rate_min,
    argMax(d.container_tx_pack_drop_rate_argMax, d.window_time) AS container_tx_pack_drop_rate_argMax,

    avg(d.container_rx_pack_drop_rate_avg)                    AS container_rx_pack_drop_rate_avg,
    max(d.container_rx_pack_drop_rate_max)                    AS container_rx_pack_drop_rate_max,
    min(d.container_rx_pack_drop_rate_min)                    AS container_rx_pack_drop_rate_min,
    argMax(d.container_rx_pack_drop_rate_argMax, d.window_time) AS container_rx_pack_drop_rate_argMax,

    -- 8. 数据同步标记 (直接透传或按场景取 anyLast)
    any(d.sync_flag)                                                    AS sync_flag

FROM modelmate_metric.t_mops_modelinstance_day_dws AS d
/* 增量计算时可解除注释添加时间范围约束*/
WHERE d.window_time >= toDateTime({start_time:UInt32}, {system_timezone:String})
  AND d.window_time <  toDateTime({end_time:UInt32}, {system_timezone:String})
  AND d.sync_flag = 0 -- 只针对探针实时上报的数据做 5 分钟汇聚
GROUP BY
    toDateTime({start_time:UInt32}, {system_timezone:String}) ,
    d.server_name,
    d.modelinstance_id,
    d.group_id,
    d.container_id;
