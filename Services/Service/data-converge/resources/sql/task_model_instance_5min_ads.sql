-- 5.1.4 由t_mops_modelinstance_5min_dws与t_mops_gpu_5min_dws 生成t_mops_modelinstance_5min_ads
-- 考虑到一个K上可能起多个container，因此有了这个内层汇聚逻辑。 可以这样写
INSERT INTO modelmate_metric.t_mops_modelinstance_5min_ads
(
    window_time,
    server_name,
    modelinstance_id,
    modelinstance_name,
    framework_type,
    framework_version,
    deploy_info,
    model_name,
    model_id,
    node_id,
    node_ip,
    node_name,
    node_type,
    dev_mfr,
    dev_model,
    dev_id,
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
    ai_card_normal_state_counter,
    ai_card_abnormal_state_counter,
    ai_card_health_ratio_avg,
    ai_card_offline_ratio_avg,
    ai_card_used_argMax,
    ai_card_used_counter,
    ai_card_unused_counter,
    ai_card_used_active_counter,
    ai_card_used_ratio_avg,
    ai_card_computing_power_argMax,
    ai_card_used_computing_power_counter,
    ai_card_unused_computing_power_counter,
    sync_flag
)
WITH
    -- 1. 容器/实例指标汇总（彻底抹平 container_id / group_id / card_id，收拢至实例粒度）
    instance_base AS (
        SELECT
            dws.window_time,
            dws.server_name,
            dws.modelinstance_id,

            -- 维度快照
            argMax(dws.modelinstance_name, dws.window_time) AS modelinstance_name,
            argMax(dws.framework_type, dws.window_time)     AS framework_type,
            argMax(dws.framework_version, dws.window_time)  AS framework_version,
            argMax(dws.deploy_info, dws.window_time)        AS deploy_info,
            argMax(dws.model_name, dws.window_time)         AS model_name,
            argMax(dws.model_id, dws.window_time)           AS model_id,
            argMax(dws.node_id, dws.window_time)            AS node_id,
            argMax(dws.node_ip, dws.window_time)            AS node_ip,
            argMax(dws.node_name, dws.window_time)          AS node_name,
            argMax(dws.node_type, dws.window_time)          AS node_type,
            argMax(dws.dev_mfr, dws.window_time)            AS dev_mfr,
            argMax(dws.dev_model, dws.window_time)          AS dev_model,
            argMax(dws.dev_id, dws.window_time)             AS dev_id,
            argMax(dws.dev_status_code, dws.window_time)    AS dev_status_code,
            argMax(dws.dev_status_name, dws.window_time)    AS dev_status_name,

            -- 容器/进程指标汇总
            avg(dws.container_cpu_util_avg)             AS container_cpu_util_avg,
            max(dws.container_cpu_util_max)             AS container_cpu_util_max,
            min(dws.container_cpu_util_min)             AS container_cpu_util_min,
            argMax(dws.container_cpu_util_argMax, dws.window_time) AS container_cpu_util_argMax,

            avg(dws.container_mem_util_avg)             AS container_mem_util_avg,
            max(dws.container_mem_util_max)             AS container_mem_util_max,
            min(dws.container_mem_util_min)             AS container_mem_util_min,
            argMax(dws.container_mem_util_argMax, dws.window_time) AS container_mem_util_argMax,

            avg(dws.container_mem_usage_mb_avg)         AS container_mem_usage_mb_avg,
            max(dws.container_mem_usage_mb_max)         AS container_mem_usage_mb_max,
            min(dws.container_mem_usage_mb_min)         AS container_mem_usage_mb_min,
            argMax(dws.container_mem_usage_mb_argMax, dws.window_time) AS container_mem_usage_mb_argMax,

            avg(dws.container_disk_usage_mb_avg)        AS container_disk_usage_mb_avg,
            max(dws.container_disk_usage_mb_max)        AS container_disk_usage_mb_max,
            min(dws.container_disk_usage_mb_min)        AS container_disk_usage_mb_min,
            argMax(dws.container_disk_usage_mb_argMax, dws.window_time) AS container_disk_usage_mb_argMax,

            avg(dws.container_npu_util_avg)             AS container_npu_util_avg,
            max(dws.container_npu_util_max)             AS container_npu_util_max,
            min(dws.container_npu_util_min)             AS container_npu_util_min,
            argMax(dws.container_npu_util_argMax, dws.window_time) AS container_npu_util_argMax,

            avg(dws.container_hbm_util_avg)             AS container_hbm_util_avg,
            max(dws.container_hbm_util_max)             AS container_hbm_util_max,
            min(dws.container_hbm_util_min)             AS container_hbm_util_min,
            argMax(dws.container_hbm_util_argMax, dws.window_time) AS container_hbm_util_argMax,

            avg(dws.container_recv_bytes_rate_avg)      AS container_recv_bytes_rate_avg,
            max(dws.container_recv_bytes_rate_max)      AS container_recv_bytes_rate_max,
            min(dws.container_recv_bytes_rate_min)      AS container_recv_bytes_rate_min,
            argMax(dws.container_recv_bytes_rate_argMax, dws.window_time) AS container_recv_bytes_rate_argMax,

            avg(dws.container_send_bytes_rate_avg)      AS container_send_bytes_rate_avg,
            max(dws.container_send_bytes_rate_max)      AS container_send_bytes_rate_max,
            min(dws.container_send_bytes_rate_min)      AS container_send_bytes_rate_min,
            argMax(dws.container_send_bytes_rate_argMax, dws.window_time) AS container_send_bytes_rate_argMax,

            avg(dws.container_tx_pack_drop_rate_avg)    AS container_tx_pack_drop_rate_avg,
            max(dws.container_tx_pack_drop_rate_max)    AS container_tx_pack_drop_rate_max,
            min(dws.container_tx_pack_drop_rate_min)    AS container_tx_pack_drop_rate_min,
            argMax(dws.container_tx_pack_drop_rate_argMax, dws.window_time) AS container_tx_pack_drop_rate_argMax,

            avg(dws.container_rx_pack_drop_rate_avg)    AS container_rx_pack_drop_rate_avg,
            max(dws.container_rx_pack_drop_rate_max)    AS container_rx_pack_drop_rate_max,
            min(dws.container_rx_pack_drop_rate_min)    AS container_rx_pack_drop_rate_min,
            argMax(dws.container_rx_pack_drop_rate_argMax, dws.window_time) AS container_rx_pack_drop_rate_argMax,

            argMax(dws.sync_flag, dws.window_time)          AS sync_flag
        FROM modelmate_metric.t_mops_modelinstance_5min_dws AS dws
        WHERE dws.window_time >= {start_time:DateTime}
          AND dws.window_time <  {end_time:DateTime}
          AND dws.sync_flag = 0
        GROUP BY
            dws.window_time,
            dws.server_name,
            dws.modelinstance_id
    ),

    -- 2. 抽取"实例-物理卡"去重映射关系（不管一个卡上有多少个容器，在当前实例下只算作一个卡/芯片关联节点）
    instance_card_rel AS (
        SELECT DISTINCT
            dws.window_time,
            dws.server_name,
            dws.modelinstance_id,
            dws.node_ip,
            dws.ai_card_id,
            dws.chip_id
        FROM modelmate_metric.t_mops_modelinstance_5min_dws AS dws
        WHERE dws.window_time >= {start_time:DateTime}
          AND dws.window_time <  {end_time:DateTime}
          AND dws.sync_flag = 0
    ),

    -- 3. 关联物理卡表计算"卡层指标"（通过关联去重表，解决一对多 JOIN 导致的数据计算膨胀问题）
    card_metrics_agg AS (
        SELECT
            r.window_time,
            r.server_name,
            r.modelinstance_id,

            avg(c.hbm_util_avg)                                     AS hbm_util_avg,
            max(c.hbm_util_max)                                     AS hbm_util_max,
            min(c.hbm_util_min)                                     AS hbm_util_min,

            avg(c.ai_core_util_avg)                                 AS ai_core_util_avg,
            max(c.ai_core_util_max)                                 AS ai_core_util_max,
            min(c.ai_core_util_min)                                 AS ai_core_util_min,
            argMax(c.ai_core_util_argMax, c.window_time)            AS ai_core_util_argMax,

            avg(c.hbm_bandwidth_util_avg)                           AS hbm_bandwidth_util_avg,
            max(c.hbm_bandwidth_util_max)                           AS hbm_bandwidth_util_max,
            min(c.hbm_bandwidth_util_min)                           AS hbm_bandwidth_util_min,

            avg(c.ai_card_temp_avg)                                 AS ai_card_temp_avg,
            max(c.ai_card_temp_max)                                 AS ai_card_temp_max,
            min(c.ai_card_temp_min)                                 AS ai_card_temp_min,

            avg(c.ai_card_power_avg)                                AS ai_card_power_avg,
            max(c.ai_card_power_max)                                AS ai_card_power_max,
            min(c.ai_card_power_min)                                AS ai_card_power_min,

            avg(c.bandwidth_tx_avg)                                 AS bandwidth_tx_avg,
            max(c.bandwidth_tx_max)                                 AS bandwidth_tx_max,
            min(c.bandwidth_tx_min)                                 AS bandwidth_tx_min,

            avg(c.bandwidth_rx_avg)                                 AS bandwidth_rx_avg,
            max(c.bandwidth_rx_max)                                 AS bandwidth_rx_max,
            min(c.bandwidth_rx_min)                                 AS bandwidth_rx_min,

            argMax(c.ai_card_health_argMax, c.window_time)         AS ai_card_health_argMax,

            -- 绑卡状态计数与比例（此处 sum 保证每张卡只加一次）
            sum(c.ai_card_normal_state_counter)                     AS ai_card_normal_state_counter,
            sum(c.ai_card_abnormal_state_counter)                   AS ai_card_abnormal_state_counter,
            avg(c.ai_card_health_ratio_avg)                         AS ai_card_health_ratio_avg,
            avg(c.ai_card_offline_ratio_avg)                        AS ai_card_offline_ratio_avg,

            argMax(c.ai_card_used_argMax, c.window_time)            AS ai_card_used_argMax,
            sum(c.ai_card_used_counter)                             AS ai_card_used_counter,
            sum(c.ai_card_unused_counter)                           AS ai_card_unused_counter,
            sum(c.ai_card_used_active_counter)                      AS ai_card_used_active_counter,
            avg(c.ai_card_used_ratio_avg)                           AS ai_card_used_ratio_avg,

            -- 算力容量与分配累加指标
            argMax(c.ai_card_computing_power_argMax, c.window_time) AS ai_card_computing_power_argMax,
            sum(c.ai_card_used_computing_power_counter)           AS ai_card_used_computing_power_counter,
            sum(c.ai_card_unused_computing_power_counter)         AS ai_card_unused_computing_power_counter
        FROM instance_card_rel AS r
        INNER JOIN modelmate_metric.t_mops_gpu_5min_dws AS c
            ON  r.window_time = c.window_time
            AND r.node_ip     = c.node_ip
            AND r.ai_card_id = c.ai_card_index
            AND r.chip_id     = c.chip_id
        WHERE c.window_time >= {start_time:DateTime}
          AND c.window_time <  {end_time:DateTime}
          AND c.sync_flag = 0
        GROUP BY
            r.window_time,
            r.server_name,
            r.modelinstance_id
    )

-- 4. 主表与卡聚合表合并落库
SELECT
    i.window_time,
    i.server_name,
    i.modelinstance_id,

    -- 属性快照
    i.modelinstance_name,
    i.framework_type,
    i.framework_version,
    i.deploy_info,
    i.model_name,
    i.model_id,
    i.node_id,
    i.node_ip,
    i.node_name,
    i.node_type,
    i.dev_mfr,
    i.dev_model,
    i.dev_id,
    i.dev_status_code,
    i.dev_status_name,

    -- 容器指标
    i.container_cpu_util_avg,
    i.container_cpu_util_max,
    i.container_cpu_util_min,
    i.container_cpu_util_argMax,

    i.container_mem_util_avg,
    i.container_mem_util_max,
    i.container_mem_util_min,
    i.container_mem_util_argMax,

    i.container_mem_usage_mb_avg,
    i.container_mem_usage_mb_max,
    i.container_mem_usage_mb_min,
    i.container_mem_usage_mb_argMax,

    i.container_disk_usage_mb_avg,
    i.container_disk_usage_mb_max,
    i.container_disk_usage_mb_min,
    i.container_disk_usage_mb_argMax,

    i.container_npu_util_avg,
    i.container_npu_util_max,
    i.container_npu_util_min,
    i.container_npu_util_argMax,

    i.container_hbm_util_avg,
    i.container_hbm_util_max,
    i.container_hbm_util_min,
    i.container_hbm_util_argMax,

    i.container_recv_bytes_rate_avg,
    i.container_recv_bytes_rate_max,
    i.container_recv_bytes_rate_min,
    i.container_recv_bytes_rate_argMax,

    i.container_send_bytes_rate_avg,
    i.container_send_bytes_rate_max,
    i.container_send_bytes_rate_min,
    i.container_send_bytes_rate_argMax,

    i.container_tx_pack_drop_rate_avg,
    i.container_tx_pack_drop_rate_max,
    i.container_tx_pack_drop_rate_min,
    i.container_tx_pack_drop_rate_argMax,

    i.container_rx_pack_drop_rate_avg,
    i.container_rx_pack_drop_rate_max,
    i.container_rx_pack_drop_rate_min,
    i.container_rx_pack_drop_rate_argMax,

    -- 卡指标（可能存在无卡绑定的纯 CPU 实例，因此采用 LEFT JOIN）
    c.hbm_util_avg,
    c.hbm_util_max,
    c.hbm_util_min,

    c.ai_core_util_avg,
    c.ai_core_util_max,
    c.ai_core_util_min,
    c.ai_core_util_argMax,

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

    c.ai_card_health_argMax,

    c.ai_card_normal_state_counter,
    c.ai_card_abnormal_state_counter,
    c.ai_card_health_ratio_avg,
    c.ai_card_offline_ratio_avg,

    c.ai_card_used_argMax,
    c.ai_card_used_counter,
    c.ai_card_unused_counter,
    c.ai_card_used_active_counter,
    c.ai_card_used_ratio_avg,

    c.ai_card_computing_power_argMax,
    c.ai_card_used_computing_power_counter,
    c.ai_card_unused_computing_power_counter,

    i.sync_flag
FROM instance_base AS i
LEFT JOIN card_metrics_agg AS c
    ON  i.window_time       = c.window_time
    AND i.server_name       = c.server_name
    AND i.modelinstance_id  = c.modelinstance_id
