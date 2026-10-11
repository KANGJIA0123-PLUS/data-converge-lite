-- 5.1.3 由t_mops_modelinstance_hour_dws 汇聚生成 t_mops_modelinstance_day_dws
INSERT INTO modelmate_metric.t_mops_modelinstance_day_dws(
    window_time,

    -- 核心维度
    server_name,
    modelinstance_id,
    group_id,
    container_id,

    -- 模型实例属性
    modelinstance_name,
    framework_type,
    framework_version,
    deploy_info,
    process_role,
    model_name,
    model_id,

    -- 节点属性
    node_id,
    node_ip,
    node_name,
    node_type,

    -- 设备属性
    ai_card_id,
    chip_id,
    dev_mfr,
    dev_model,
    dev_status_code,
    dev_status_name,

    -- 容器 CPU 利用率
    container_cpu_util_avg,
    container_cpu_util_max,
    container_cpu_util_min,
    container_cpu_util_argMax,

    -- 容器内存利用率
    container_mem_util_avg,
    container_mem_util_max,
    container_mem_util_min,
    container_mem_util_argMax,

    -- 容器内存使用量
    container_mem_usage_mb_avg,
    container_mem_usage_mb_max,
    container_mem_usage_mb_min,
    container_mem_usage_mb_argMax,

    -- 容器磁盘使用量
    container_disk_usage_mb_avg,
    container_disk_usage_mb_max,
    container_disk_usage_mb_min,
    container_disk_usage_mb_argMax,

    -- 容器 NPU 利用率
    container_npu_util_avg,
    container_npu_util_max,
    container_npu_util_min,
    container_npu_util_argMax,

    -- 容器 HBM 利用率
    container_hbm_util_avg,
    container_hbm_util_max,
    container_hbm_util_min,
    container_hbm_util_argMax,

    -- 容器网络接收速率
    container_recv_bytes_rate_avg,
    container_recv_bytes_rate_max,
    container_recv_bytes_rate_min,
    container_recv_bytes_rate_argMax,

    -- 容器网络发送速率
    container_send_bytes_rate_avg,
    container_send_bytes_rate_max,
    container_send_bytes_rate_min,
    container_send_bytes_rate_argMax,

    -- 容器发送丢包率
    container_tx_pack_drop_rate_avg,
    container_tx_pack_drop_rate_max,
    container_tx_pack_drop_rate_min,
    container_tx_pack_drop_rate_argMax,

    -- 容器接收丢包率
    container_rx_pack_drop_rate_avg,
    container_rx_pack_drop_rate_max,
    container_rx_pack_drop_rate_min,
    container_rx_pack_drop_rate_argMax,

    -- 数据同步标记
    sync_flag
)
SELECT
    -- 1. 时间维度：对齐到天起点 (00:00:00)
    toStartOfDay(h.window_time)                           AS window_time,

    -- 2. 核心维度键 (GROUP BY)
    h.server_name,
    h.modelinstance_id,
    ifNull(h.group_id, '')                                 AS group_id,
    h.container_id,

    -- 3. 透传维度/属性字段：使用 any() 拿取静态属性，使用 anyLast() 锁死当前窗口最新状态
    any(h.modelinstance_name)                             AS modelinstance_name,
    any(h.framework_type)                                 AS framework_type,
    any(h.framework_version)                              AS framework_version,
    any(h.deploy_info)                                    AS deploy_info,
    any(h.process_role)                                   AS process_role,
    any(h.model_name)                                     AS model_name,
    any(h.model_id)                                       AS model_id,
    any(h.node_id)                                        AS node_id,
    any(h.node_ip)                                        AS node_ip,
    any(h.node_name)                                      AS node_name,
    any(h.node_type)                                      AS node_type,
    any(h.ai_card_id)                                     AS ai_card_id,
    any(h.chip_id)                                        AS chip_id,
    any(h.dev_mfr)                                        AS dev_mfr,
    any(h.dev_model)                                      AS dev_model,
    anyLast(h.dev_status_code)                            AS dev_status_code,
    anyLast(h.dev_status_name)                            AS dev_status_name,

    -- 4. 连续数值指标聚合 (AVG / MAX / MIN / argMax )
    avg(h.container_cpu_util_avg)                          AS container_cpu_util_avg,
    max(h.container_cpu_util_max)                          AS container_cpu_util_max,
    min(h.container_cpu_util_min)                          AS container_cpu_util_min,
    argMax(h.container_cpu_util_argMax,h.window_time)       AS container_cpu_util_argMax,

    avg(h.container_mem_util_avg)                          AS container_mem_util_avg,
    max(h.container_mem_util_max)                          AS container_mem_util_max,
    min(h.container_mem_util_min)                          AS container_mem_util_min,
    argMax(h.container_mem_util_argMax,h.window_time)       AS container_mem_util_argMax,

    avg(h.container_mem_usage_mb_avg)                      AS container_mem_usage_mb_avg,
    max(h.container_mem_usage_mb_max)                      AS container_mem_usage_mb_max,
    min(h.container_mem_usage_mb_min)                      AS container_mem_usage_mb_min,
    argMax(h.container_mem_usage_mb_argMax,h.window_time)   AS container_mem_usage_mb_argMax,

    avg(h.container_disk_usage_mb_avg)                    AS container_disk_usage_mb_avg,
    max(h.container_disk_usage_mb_max)                     AS container_disk_usage_mb_max,
    min(h.container_disk_usage_mb_min)                     AS container_disk_usage_mb_min,
    argMax(h.container_disk_usage_mb_argMax,h.window_time)  AS container_disk_usage_mb_argMax,

    avg(h.container_npu_util_avg)                          AS container_npu_util_avg,
    max(h.container_npu_util_max)                          AS container_npu_util_max,
    min(h.container_npu_util_min)                          AS container_npu_util_min,
    argMax(h.container_npu_util_argMax,h.window_time)       AS container_npu_util_argMax,

    avg(h.container_hbm_util_avg)                          AS container_hbm_util_avg,
    max(h.container_hbm_util_max)                          AS container_hbm_util_max,
    min(h.container_hbm_util_min)                          AS container_hbm_util_min,
    argMax(h.container_hbm_util_argMax,h.window_time)       AS container_hbm_util_argMax,

    avg(h.container_recv_bytes_rate_avg)                   AS container_recv_bytes_rate_avg,
    max(h.container_recv_bytes_rate_max)                   AS container_recv_bytes_rate_max,
    min(h.container_recv_bytes_rate_min)                   AS container_recv_bytes_rate_min,
    argMax(h.container_recv_bytes_rate_argMax,h.window_time) AS container_recv_bytes_rate_argMax,

    avg(h.container_send_bytes_rate_avg)                    AS container_send_bytes_rate_avg,
    max(h.container_send_bytes_rate_max)                    AS container_send_bytes_rate_max,
    min(h.container_send_bytes_rate_min)                    AS container_send_bytes_rate_min,
    argMax(h.container_send_bytes_rate_argMax, h.window_time) AS container_send_bytes_rate_argMax,

    avg(h.container_tx_pack_drop_rate_avg)                   AS container_tx_pack_drop_rate_avg,
    max(h.container_tx_pack_drop_rate_max)                   AS container_tx_pack_drop_rate_max,
    min(h.container_tx_pack_drop_rate_min)                   AS container_tx_pack_drop_rate_min,
    argMax(h.container_tx_pack_drop_rate_argMax,h.window_time) AS container_tx_pack_drop_rate_argMax,

    avg(h.container_rx_pack_drop_rate_avg)                    AS container_rx_pack_drop_rate_avg,
    max(h.container_rx_pack_drop_rate_max)                    AS container_rx_pack_drop_rate_max,
    min(h.container_rx_pack_drop_rate_min)                    AS container_rx_pack_drop_rate_min,
    argMax(h.container_rx_pack_drop_rate_argMax, h.window_time) AS container_rx_pack_drop_rate_argMax,

    -- 8. 数据同步标记 (直接透传或按场景取 anyLast)
    any(h.sync_flag)                                                    AS sync_flag

FROM modelmate_metric.t_mops_modelinstance_hour_dws AS h
/* 增量计算时可解除注释添加时间范围约束*/
WHERE h.window_time >= {start_time:DateTime}
  AND h.window_time <  {end_time:DateTime}
  AND h.sync_flag = 0 -- 只针对探针实时上报的数据做天汇聚
GROUP BY
    toStartOfDay(h.window_time),
    h.server_name,
    h.modelinstance_id,
    h.group_id,
    h.container_id;
