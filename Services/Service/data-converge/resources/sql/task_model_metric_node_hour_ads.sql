-- 6.4 节点级别metric指标
-- 6.4.2 节点粒度 hour Metric指标，由t_mops__modelmetric_node__5min_ads 根据节点ID独立汇聚即可
INSERT INTO modelmate_metric.t_mops__modelmetric_node__hour_ads
(
    window_time,
    node_id,
    node_name,
    node_ip,
    node_type,
    dev_mfr,
    dev_model,
    dev_status_code,
    dev_status_name,
    request_received_total_delta,
    request_failed_total_delta,
    prompt_tokens_total_delta,
    generation_tokens_total_delta,
    num_preemptions_total_delta,
    sync_flag
)
SELECT
    toDateTime({start_time:UInt32}, {system_timezone:String}) AS window_time,
    node_id,

    -- 节点属性取小时内的最新值
    argMax(node_name, source.window_time) AS node_name,
    node_ip,
    any(node_type) AS node_type,
    argMax(dev_mfr, source.window_time) AS dev_mfr,
    argMax(dev_model, source.window_time) AS dev_model,
    argMax(dev_status_code, source.window_time) AS dev_status_code,
    argMax(dev_status_name, source.window_time) AS dev_status_name,

    -- 指标直接求和
    sum(request_received_total_delta) AS request_received_total_delta,
    sum(request_failed_total_delta) AS request_failed_total_delta,
    sum(prompt_tokens_total_delta) AS prompt_tokens_total_delta,
    sum(generation_tokens_total_delta) AS generation_tokens_total_delta,
    sum(num_preemptions_total_delta) AS num_preemptions_total_delta,

    any(sync_flag) AS sync_flag

FROM modelmate_metric.t_mops__modelmetric_node__5min_ads AS source
WHERE source.window_time >= toDateTime({start_time:UInt32}, 'UTC')
  AND source.window_time < toDateTime({end_time:UInt32}, 'UTC')
GROUP BY
    toDateTime({start_time:UInt32}, {system_timezone:String}), -- 使用本次调度窗口的起点
    node_id,
    node_ip;