INSERT INTO modelmate_metric.t_mops__modelinstance_server__day_ads
(
    window_time,
    server_name,
    modelinstance_name,
    framework_type,
    framework_version,
    deploy_info,
    model_name,
    model_id,
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
    -- 模型服务使用的总 NPU 卡数
    allocated_npu_num,
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
    -- 忙闲时算力指标与状态打标
    work_time_ai_core_util_avg,
    work_time_ai_core_util_max,
    work_time_ai_core_util_min,
    idle_time_ai_core_util_avg,
    idle_time_ai_core_util_max,
    idle_time_ai_core_util_min,
    work_time_ai_core_util_status,
    sync_flag
)
WITH
    -- 1. 服务级基础维度与容器指标汇总（抹平 instance_id，收拢至天+服务粒度）
    server_base AS (
        SELECT
            toDateTime({start_time:UInt32}, {system_timezone:String}) AS window_time,
            dws.server_name,

            -- 维度快照（取天内最新值）
            argMax(dws.modelinstance_name, dws.window_time) AS modelinstance_name,
            argMax(dws.framework_type, dws.window_time)     AS framework_type,
            argMax(dws.framework_version, dws.window_time)  AS framework_version,
            argMax(dws.deploy_info, dws.window_time)        AS deploy_info,
            argMax(dws.model_name, dws.window_time)         AS model_name,
            argMax(dws.model_id, dws.window_time)           AS model_id,

            -- 容器级指标天级汇总
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
        FROM modelmate_metric.t_mops_modelinstance_day_dws AS dws
        WHERE dws.window_time >= toDateTime({start_time:UInt32}, {system_timezone:String})
          AND dws.window_time <  toDateTime({end_time:UInt32}, {system_timezone:String})
          AND dws.sync_flag = 0
        GROUP BY
            toDateTime({start_time:UInt32}, {system_timezone:String}),
            dws.server_name
    ),

    -- 2. 抽取天级"模型服务-物理卡"去重映射关系
    server_card_rel AS (
        SELECT DISTINCT
            toDateTime({start_time:UInt32}, {system_timezone:String}) AS window_time,
            dws.server_name,
            dws.node_ip,
            dws.ai_card_id,
            dws.chip_id
        FROM modelmate_metric.t_mops_modelinstance_day_dws AS dws
        WHERE dws.window_time >= toDateTime({start_time:UInt32}, {system_timezone:String})
          AND dws.window_time <  toDateTime({end_time:UInt32}, {system_timezone:String})
          AND dws.sync_flag = 0
    ),

    -- 3. 关联天级物理卡 DWS 表计算服务级卡层及忙闲时指标
    card_metrics_agg AS (
        SELECT
            r.window_time,
            r.server_name,

            -- 统计该模型服务去重后的绑定 NPU 卡数
            uniqExact(tuple(c.node_ip, c.ai_card_index, c.chip_id)) AS allocated_npu_num,

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

            -- 绑卡状态计数与算力（天级聚合累加）
            sum(c.ai_card_normal_state_counter)                     AS ai_card_normal_state_counter,
            sum(c.ai_card_abnormal_state_counter)                   AS ai_card_abnormal_state_counter,
            avg(c.ai_card_health_ratio_avg)                         AS ai_card_health_ratio_avg,
            avg(c.ai_card_offline_ratio_avg)                        AS ai_card_offline_ratio_avg,

            argMax(c.ai_card_used_argMax, c.window_time)            AS ai_card_used_argMax,
            sum(c.ai_card_used_counter)                             AS ai_card_used_counter,
            sum(c.ai_card_unused_counter)                           AS ai_card_unused_counter,
            sum(c.ai_card_used_active_counter)                      AS ai_card_used_active_counter,
            avg(c.ai_card_used_ratio_avg)                           AS ai_card_used_ratio_avg,

            argMax(c.ai_card_computing_power_argMax, c.window_time) AS ai_card_computing_power_argMax,
            sum(c.ai_card_used_computing_power_counter)           AS ai_card_used_computing_power_counter,
            sum(c.ai_card_unused_computing_power_counter)         AS ai_card_unused_computing_power_counter,

            -- 服务级别下的忙闲时 AI Core 利用率与状态打标
            avg(c.work_time_ai_core_util_avg) AS work_time_ai_core_util_avg,
            max(c.work_time_ai_core_util_max) AS work_time_ai_core_util_max,
            min(c.work_time_ai_core_util_min) AS work_time_ai_core_util_min,

            avg(c.idle_time_ai_core_util_avg) AS idle_time_ai_core_util_avg,
            max(c.idle_time_ai_core_util_max) AS idle_time_ai_core_util_max,
            min(c.idle_time_ai_core_util_min) AS idle_time_ai_core_util_min,

            multiIf(
                avg(c.work_time_ai_core_util_avg) IS NULL, NULL,
                avg(c.work_time_ai_core_util_avg) > 80,   '超负荷使用',
                avg(c.work_time_ai_core_util_avg) >= 15,  '常规使用',
                avg(c.work_time_ai_core_util_avg) >= 0,   '低效使用',
                NULL
            ) AS work_time_ai_core_util_status

        FROM server_card_rel AS r
        INNER JOIN modelmate_metric.t_mops_gpu_day_dws AS c
            ON  r.window_time = c.window_time
            AND r.node_ip     = c.node_ip
            AND r.ai_card_id  = c.ai_card_index
            AND r.chip_id     = c.chip_id
        WHERE c.window_time >= toDateTime({start_time:UInt32}, {system_timezone:String})
          AND c.window_time <  toDateTime({end_time:UInt32}, {system_timezone:String})
          AND c.sync_flag = 0
        GROUP BY
            r.window_time,
            r.server_name
    )

-- 4. 服务主表与物理卡表连接并写入服务级天表 ADS
SELECT
    s.window_time,
    s.server_name,

    -- 维度属性快照
    s.modelinstance_name,
    s.framework_type,
    s.framework_version,
    s.deploy_info,
    s.model_name,
    s.model_id,

    -- 容器指标
    s.container_cpu_util_avg,
    s.container_cpu_util_max,
    s.container_cpu_util_min,
    s.container_cpu_util_argMax,

    s.container_mem_util_avg,
    s.container_mem_util_max,
    s.container_mem_util_min,
    s.container_mem_util_argMax,

    s.container_mem_usage_mb_avg,
    s.container_mem_usage_mb_max,
    s.container_mem_usage_mb_min,
    s.container_mem_usage_mb_argMax,

    s.container_disk_usage_mb_avg,
    s.container_disk_usage_mb_max,
    s.container_disk_usage_mb_min,
    s.container_disk_usage_mb_argMax,

    s.container_npu_util_avg,
    s.container_npu_util_max,
    s.container_npu_util_min,
    s.container_npu_util_argMax,

    s.container_hbm_util_avg,
    s.container_hbm_util_max,
    s.container_hbm_util_min,
    s.container_hbm_util_argMax,

    s.container_recv_bytes_rate_avg,
    s.container_recv_bytes_rate_max,
    s.container_recv_bytes_rate_min,
    s.container_recv_bytes_rate_argMax,

    s.container_send_bytes_rate_avg,
    s.container_send_bytes_rate_max,
    s.container_send_bytes_rate_min,
    s.container_send_bytes_rate_argMax,

    s.container_tx_pack_drop_rate_avg,
    s.container_tx_pack_drop_rate_max,
    s.container_tx_pack_drop_rate_min,
    s.container_tx_pack_drop_rate_argMax,

    s.container_rx_pack_drop_rate_avg,
    s.container_rx_pack_drop_rate_max,
    s.container_rx_pack_drop_rate_min,
    s.container_rx_pack_drop_rate_argMax,

    -- 服务卡指标
    coalesce(c.allocated_npu_num, 0) AS allocated_npu_num,
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

    -- 忙闲时及状态打标指标映射
    c.work_time_ai_core_util_avg,
    c.work_time_ai_core_util_max,
    c.work_time_ai_core_util_min,
    c.idle_time_ai_core_util_avg,
    c.idle_time_ai_core_util_max,
    c.idle_time_ai_core_util_min,
    c.work_time_ai_core_util_status,

    s.sync_flag
FROM server_base AS s
LEFT JOIN card_metrics_agg AS c
    ON  s.window_time = c.window_time
    AND s.server_name = c.server_name;
