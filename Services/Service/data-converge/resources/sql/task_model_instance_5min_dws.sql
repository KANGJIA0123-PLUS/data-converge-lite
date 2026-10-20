-- 5.1.1 由t_mops_modelinstance_realtime_dwd 汇聚生成 t_mops_modelinstance_5min_dws
INSERT INTO modelmate_metric.t_mops_modelinstance_5min_dws(
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
    -- 1. 时间维度：对齐到 5 分钟窗口起点
    toDateTime({start_time:UInt32}, {system_timezone:String})                 AS window_time,

    -- 2. 核心维度键 (GROUP BY)
    r.server_name,
    r.modelinstance_id,
    ifNull(r.group_id, '')                                 AS group_id,
    r.container_id,

    -- 3. 透传维度/属性字段：使用 any() 拿取静态属性，使用 anyLast() 锁死当前窗口最新状态
    any(r.modelinstance_name)                             AS modelinstance_name,
    any(r.framework_type)                                  AS framework_type,
    any(r.framework_version)                               AS framework_version,
    any(r.deploy_info)                                     AS deploy_info,
    any(r.process_role)                                   AS process_role,
    any(r.model_name)                                     AS model_name,
    any(r.model_id)                                        AS model_id,
    any(r.node_id)                                        AS node_id,
    any(r.node_ip)                                        AS node_ip,
    any(r.node_name)                                      AS node_name,
    any(r.node_type)                                      AS node_type,
    any(r.ai_card_id)                                     AS ai_card_id,
    any(r.chip_id)                                        AS chip_id,
    any(r.dev_mfr)                                         AS dev_mfr,
    any(r.dev_model)                                       AS dev_model,
    anyLast(r.dev_status_code)                             AS dev_status_code,
    anyLast(r.dev_status_name)                             AS dev_status_name,

    -- 4. 连续数值指标聚合 (AVG / MAX / MIN / argMax )
    avg(r.container_cpu_util)                          AS container_cpu_util_avg,
    max(r.container_cpu_util)                          AS container_cpu_util_max,
    min(r.container_cpu_util)                          AS container_cpu_util_min,
    argMax(r.container_cpu_util, r.metric_datetime)       AS container_cpu_util_argMax,

    avg(r.container_mem_util)                          AS container_mem_util_avg,
    max(r.container_mem_util)                          AS container_mem_util_max,
    min(r.container_mem_util)                          AS container_mem_util_min,
    argMax(r.container_mem_util, r.metric_datetime)       AS container_mem_util_argMax,

    avg(r.container_mem_usage_mb)                      AS container_mem_usage_mb_avg,
    max(r.container_mem_usage_mb)                      AS container_mem_usage_mb_max,
    min(r.container_mem_usage_mb)                      AS container_mem_usage_mb_min,
    argMax(r.container_mem_usage_mb, r.metric_datetime)   AS container_mem_usage_mb_argMax,

    avg(r.container_disk_usage_mb)                    AS container_disk_usage_mb_avg,
    max(r.container_disk_usage_mb)                     AS container_disk_usage_mb_max,
    min(r.container_disk_usage_mb)                     AS container_disk_usage_mb_min,
    argMax(r.container_disk_usage_mb, r.metric_datetime)  AS container_disk_usage_mb_argMax,

    avg(r.container_npu_util)                          AS container_npu_util_avg,
    max(r.container_npu_util)                          AS container_npu_util_max,
    min(r.container_npu_util)                          AS container_npu_util_min,
    argMax(r.container_npu_util, r.metric_datetime)       AS container_npu_util_argMax,

    avg(r.container_hbm_util)                          AS container_hbm_util_avg,
    max(r.container_hbm_util)                          AS container_hbm_util_max,
    min(r.container_hbm_util)                          AS container_hbm_util_min,
    argMax(r.container_hbm_util, r.metric_datetime)       AS container_hbm_util_argMax,

    avg(r.container_recv_bytes_rate)                   AS container_recv_bytes_rate_avg,
    max(r.container_recv_bytes_rate)                   AS container_recv_bytes_rate_max,
    min(r.container_recv_bytes_rate)                   AS container_recv_bytes_rate_min,
    argMax(r.container_recv_bytes_rate, r.metric_datetime) AS container_recv_bytes_rate_argMax,

    avg(r.container_send_bytes_rate)                    AS container_send_bytes_rate_avg,
    max(r.container_send_bytes_rate)                    AS container_send_bytes_rate_max,
    min(r.container_send_bytes_rate)                    AS container_send_bytes_rate_min,
    argMax(r.container_send_bytes_rate, r.metric_datetime) AS container_send_bytes_rate_argMax,

    avg(r.container_tx_pack_drop_rate)                   AS container_tx_pack_drop_rate_avg,
    max(r.container_tx_pack_drop_rate)                   AS container_tx_pack_drop_rate_max,
    min(r.container_tx_pack_drop_rate)                   AS container_tx_pack_drop_rate_min,
    argMax(r.container_tx_pack_drop_rate, r.metric_datetime) AS container_tx_pack_drop_rate_argMax,

    avg(r.container_rx_pack_drop_rate)                    AS container_rx_pack_drop_rate_avg,
    max(r.container_rx_pack_drop_rate)                    AS container_rx_pack_drop_rate_max,
    min(r.container_rx_pack_drop_rate)                    AS container_rx_pack_drop_rate_min,
    argMax(r.container_rx_pack_drop_rate, r.metric_datetime) AS container_rx_pack_drop_rate_argMax,

    -- 8. 数据同步标记 (直接透传或按场景取 anyLast)
    any(r.sync_flag)                                                    AS sync_flag

FROM modelmate_metric.t_mops_modelinstance_realtime_dwd AS r
/* 增量计算时可解除注释添加时间范围约束*/
WHERE r.metric_datetime >= toDateTime({start_time:UInt32}, {system_timezone:String})
  AND r.metric_datetime <  toDateTime({end_time:UInt32}, {system_timezone:String})
  AND r.sync_flag = 0 -- 只针对探针实时上报的数据做 5 分钟汇聚
GROUP BY
    toDateTime({start_time:UInt32}, {system_timezone:String}),
    r.server_name,
    r.modelinstance_id,
    r.group_id,
    r.container_id;
