-- 6.4 节点级别metric指标
-- 6.4.1 节点粒度 5min Metric指标，由t_mops__modelmetric_instance__5min_dws 根据节点ID独立汇聚即可
INSERT INTO modelmate_metric.t_mops__modelmetric_node__5min_ads
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
    window_time,
    node_id,

    -- 节点属性快照
    argMax(node_name, window_time) AS node_name,
    node_ip,
    any(node_type) AS node_type,
    argMax(dev_mfr, window_time) AS dev_mfr,
    argMax(dev_model, window_time) AS dev_model,
    argMax(dev_status_code, window_time) AS dev_status_code,
    argMax(dev_status_name, window_time) AS dev_status_name,

    -- 核心推理指标（应用 PD 分离过滤与求和）
    -- 1. 接收请求数：PD分离场景下由 P 节点或无角色实例贡献，排除纯 D 节点
    sumIf(request_received_total_delta, process_role != 'decode') AS request_received_total_delta,

    -- 2. 失败请求数：全机或 P+D 共同承担，全量求和
    sum(request_failed_total_delta) AS request_failed_total_delta,

    -- 3. Prompt Tokens：由 P 节点贡献，排除纯 D 节点
    sumIf(prompt_tokens_total_delta, process_role != 'decode') AS prompt_tokens_total_delta,

    -- 4. Generation Tokens：由 D 节点贡献，排除纯 P 节点
    sumIf(generation_tokens_total_delta, process_role != 'prefill') AS generation_tokens_total_delta,

    -- 5. 抢占总次数：全机汇总
    sum(num_preemptions_total_delta) AS num_preemptions_total_delta,

    any(sync_flag) AS sync_flag

FROM modelmate_metric.t_mops__modelmetric_instance__5min_dws
WHERE window_time >= toDateTime({start_time:UInt32}, 'UTC')
  AND window_time < toDateTime({end_time:UInt32}, 'UTC')
GROUP BY
    window_time,
    node_id,
    node_ip;