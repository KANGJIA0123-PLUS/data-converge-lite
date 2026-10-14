-- 6.4 节点级别metric指标
-- 6.4.3 节点粒度 day Metric指标，由t_mops__modelmetric_hour__hour_ads 根据节点ID独立汇聚即可，核心汇聚逻辑是对delta数值求SUM即可
INSERT INTO modelmate_metric.t_mops__modelmetric_node__day_ads
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
    toStartOfDay(window_time) AS window_time,
    node_id,

    argMax(node_name, window_time) AS node_name,
    node_ip,
    any(node_type) AS node_type,
    argMax(dev_mfr, window_time) AS dev_mfr,
    argMax(dev_model, window_time) AS dev_model,
    argMax(dev_status_code, window_time) AS dev_status_code,
    argMax(dev_status_name, window_time) AS dev_status_name,

    sum(request_received_total_delta) AS request_received_total_delta,
    sum(request_failed_total_delta) AS request_failed_total_delta,
    sum(prompt_tokens_total_delta) AS prompt_tokens_total_delta,
    sum(generation_tokens_total_delta) AS generation_tokens_total_delta,
    sum(num_preemptions_total_delta) AS num_preemptions_total_delta,

    any(sync_flag) AS sync_flag

FROM modelmate_metric.t_mops__modelmetric_node__hour_ads
WHERE window_time >= {start_time:DateTime}
  AND window_time < {end_time:DateTime}
GROUP BY
    window_time, -- 规整为天起始时间
    node_id,
    node_ip;