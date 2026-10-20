-- 5.2.2 模型实例汇聚 由t_mops__modelmetric_instance__5min_dws -> t_mops__modelmetric_instance__hour_dws
-- 为了在汇聚到小时表时消除TP组内多容器重复计算，在ETL中先将5min数据按group_id提取主节点/平均值，再按小时汇总
-- 物理去重在底层（Group层），逻辑隔离在中层（Role层），大盘汇总在顶层（Instance/ADS层）
-- [ADS层：实例与服务级] ── GROUP BY toDateTime({start_time:UInt32}, {system_timezone:String}), server_name, modelinstance_id
--      ▲
--      │ (合并 P/D 角色：算实例总吞吐、总并发、实例级 KV Cache 平均)
--      │
-- [DWS最外层：角色级 (P/D)(DWS层做到这里)] ── GROUP BY toDateTime({start_time:UInt32}, {system_timezone:String}), server_name, modelinstance_id, process_role
--      ▲
--      │ (跨组累加：把 P1_Group + P2_Group 物理累加，隔离 TTFT/TPOT 时延)
--      │
-- [DWS最内层：逻辑组级 (Group)] ── GROUP BY toDateTime({start_time:UInt32}, {system_timezone:String}), server_name, modelinstance_id, process_role, group_id
--      ▲
--      │ (组内 MAX 去重：抹除 TP/PP 8卡重复上报，锁住 Rank 0)
--      │
-- (底层数据源：t_mops__modelmetric_instance__5min_dws)
-- 内存查询的逻辑与规则：
-- 1、什么情况下使用 MAX 或 ArgMax，针对：组内高度重复/冗余的指标.这类指标的核心特征是：在同一个 Tensor Parallelism (TP) 组内，多张卡/多个容器上报的数据在逻辑上是完全相同或同步的。如果直接用 SUM，会导致数据随着卡数（TP Size）线性翻倍。
-- 1.1、绝对物理累加值 / 累积计数器（Counters & Deltas）：如：request_received_total_delta（请求数增量）、prompt_tokens_total_delta（输入 Token 增量）、generation_tokens_total_delta（输出 Token 增量）、时延中的 sum_delta 与 count_delta等
-- 取 MAX 逻辑：在 TP 场景下，一个请求会被广播给组内的所有 Rank 处理。Rank 0 记录了 100 个 Token 的增量，Rank 1~7 也会记录 100 个 Token 的增量。为了得到该 Group 真实的物理吞吐，必须对组内多容器取 MAX（相当于提取了主节点 Rank 0 的真实物理值），消除了 8 倍的虚高重复计算。
-- 1.2、累计指标的最新快照：如：request_received_total_argMax、prompt_tokens_total_argMax等
-- 取 MAX / ArgMax 逻辑：快照代表的是某个时间点上的累计总计数。组内各个 Rank 上报的值微秒级同步，取 MAX 即可拿到该 Group 在该时间窗口内最新的绝对累计值。
-- 1.3、分布直方图：如：time_per_output_token_seconds_bucket_delta 等Map结构
-- 取 ArgMax(bucket, count) 逻辑：每个容器都会统计自己的分布 Bucket。由于所有 Rank 都在协同处理同一个 Batch 的请求，它们的请求分布完全一致。因此选择样本数最多（Count 最大）的那个容器的 Bucket Map 作为代表，比直接将地图 sumMap（会导致分布样本数翻倍）要准确得多
-- 2、 什么情况下使用avg，针对：组内各自独立/资源共享的物理状态。这类指标的核心特征是：容器/芯片虽然在协作，但它们消耗的物理资源或瞬时状态是独立的，或者引擎层报出的是一个“速率/比例”。
-- 2.1 硬件资源与缓存占用率:如：npu_cache_usage_perc（KV Cache 使用率）、cpu_cache_usage_perc、npu_prefix_cache_hit_rate（前缀缓存命中率）
-- 取 AVG 逻辑：在分布式推理中，虽然权重是切分的，但不同 Rank 上的 KV Cache 占用率可能存在微小的倾斜（例如 Rank 0 额外承载上下文调度）。取 AVG 代表该 Group 总体显存/缓存的平均消耗水平。
-- 2.2 并发与队列状态：如：num_requests_running（运行中请求）、num_requests_waiting（排队中请求）、num_requests_swapped（交换中请求）
-- 取 AVG 逻辑：运行与排队请求数属于瞬时采样（Gauges）。在 1 小时（12 个 5 分钟窗口）的跨时间维度上，取 AVG 能准确反映该 Group 在这 1 小时内的平均并发压力。
-- 2.3 引擎预估速率：如：avg_prompt_throughput_toks_per_s（引擎报出的参考吞吐率），取 AVG 逻辑：速率指标（Tokens/s）在数学上属于“非可加性指标”（Non-additive metric），不能直接 SUM。取 AVG 可以平滑掉瞬时抖动，得到更稳健的平均速率。
-- 3、针对五类场景，内存查询逻辑正确性分析
-- 3.1 场景 1：通用单节点单卡 / 单进程 (Single Card)
-- 部署特征：只有 1 个容器，container_id、group_id 与 modelinstance_id 是 1:1:1 的关系。
-- 在子查询中，MAX(group_..._delta) 作用于只有 1 个元素的集合，在数学上 MAX(X) = X
-- AVG(group_..._perc) 作用于 1 个元素，AVG(X) = X
-- 结论：逻辑退化为传统的单节点简单聚合，没有任何数据失真或额外计算开销。
-- 3.2 场景 2：单节点多卡 TP/PP 分布式 (Single-Node Multi-GPU)
-- 部署特征：1 个 Group 包含 N 个 Rank（如 TP=8）。8 个容器在同一台物理机上，上报几乎相同的请求和 Token 监控。
-- 如果按容器直接 SUM，100 个 Token 会被算成 800 个，吞吐量放大 8 倍。现在子查询 GROUP BY group_id + MAX(prompt_tokens_total_delta)，直接锁定了 Rank 0 的真实上报值。组内 8 个容器被成功收缩为 1 个逻辑组数据。
-- 在 TP=8 模式下，一个 Batch 的推理是 8 张卡严格同步执行的。原则上，Rank 0 到 Rank 7 统计到的请求延迟分布（Bucket）是完全相同的。使用 argMax(bucket_delta, count_delta) 取组内采样次数最多（通常就是 Rank 0）的那一个容器的 Bucket Map。这样既保留了真实的延迟区间分布，又保证了样本总数不被翻倍。
-- 在 TP=8 模式下，虽然 8 张卡在协同计算同一个模型，但在实际运行中，Rank 0 通常需要额外承担上下文调度、Master 节点通信等微小开销，导致 Rank 0 的显存/KV Cache 占用率可能比 Rank 1~7 略高（显存倾斜 Skew）。如：npu_cache_usage_perc取 AVG 代表了这个 TP=8 逻辑组整体显存/KV Cache 的平均水位，真实反映了该推理组的资源利用率。
-- 结论：彻底修正了分布式 TP 带来的指标膨胀问题，算出了真正的物理吞吐。
-- 3.3 场景 3：多节点跨机器分布式 (Multi-Node Distributed TP/PP)
-- 部署特征：一个大模型实例跨越多台机器（多 IP），但它们共享同一个 modelinstance_id 和 group_id。
-- 数据去重的边界是逻辑组（group_id），而不是物理机（server_name / node_ip）。
-- 即使容器散落在 Node-A 和 Node-B 上，只要它们同属于 Group-1，子查询就会在全集群范围内将该 Group 的数据做 MAX 去重，而物理机 IP 等信息通过 argMax(node_ip, window_time) 提炼为最新的快照标签。
-- AVG(npu_cache_usage_perc_avg) 会对分布在 Node-A 和 Node-B 上的所有芯片/容器求平均，准确反映了跨机大模型组总体的平均显存水位与负载
-- 结论：实现了物理拓扑与业务吞吐计算的解耦
-- 3.4 场景 4：PD 分离架构 (Prefill / Decode Separation)
-- [ 实例层: modelinstance_id ] (例如: "qwen-72b-pd-online")
--  │
--  ├── [ 角色层: process_role = 'Prefill' ]
--  │     ├── [ P1_Group ] ──► 包含 P1, P2, P3 容器 (组内可能走 TP=2)
--  │     └── [ P2_Group ] ──► 包含 P4, P5, P6 容器 (组内可能走 TP=2)
--  │
--  └── [ 角色层: process_role = 'Decode' ]
--        ├── [ D1_Group ] ──► 包含 D1, D2 容器
--        └── [ D2_Group ] ──► 包含 D3, D4 容器
-- 组内（Group 内部）：如 P1_Group 内部的 P1、P2、P3，它们之间是 TP/PP 分布式协同关系，原有逻辑会数据冗余/重复上报，但是现在没有了。
-- 内层查询：组内group by group_id 解决了组内重复，组内去重零膨胀（靠 group_id + MAX）
-- 外层查询：组外GROUP BY process_role 解决了组件物理累加，多组容量全累加（靠 process_role + SUM）；对于 Prefill 角色：SQL 会把 P1_Group 的真实吞吐和 P2_Group 的真实吞吐做 SUM 加和；对于 Decode 角色：SQL 会把 D1_Group 的真实吞吐和 D2_Group 的真实吞吐做 SUM 加和
-- 组间（Group 与 Group 之间）：P1_Group 和 P2_Group 之间是平行的负载均衡（Load Balancing）关系，它们各自处理不同的请求，数据是独立的、需要累加的。
-- 部署特征：实例被拆分为 Prefill 组（P 节点）和 Decode 组（D 节点）。P 节点负责 Prompt，产生 TTFT；D 节点负责 Generation，产生 TPOT。
-- 小时表的分组（GROUP BY）中显式保留了 process_role
-- P 节点的 Group（role='Prefill'）和 D 节点的 Group（role='Decode'）在小时表中是分两行独立存储的，没有混在一起。
-- P 节点的 TPOT 计数通常为 0，D 节点的 TTFT 计数通常为 0。
-- 结论：精确反映了 PD 分离架构下不同节点的专职性能指标。

-- 3.5 场景 5：动态扩缩容 / Pod 漂移
-- 部署特征：1 小时内由于 K8s Pod 抢占、重启或 HPA 扩缩容，container_id 频繁变动，容器数量从 4 个变成 8 个。
-- 小时表抹除了 container_id，只按逻辑组 group_id 汇聚。无论 1 小时内 Pod 重启了多少次，对于 Hour DWS 来说，它只关心该逻辑组在这 1 小时内累积完成了多少总 Token、平均并发是多少。


INSERT INTO modelmate_metric.t_mops__modelmetric_instance__hour_dws (
    window_time,
    server_name,
    modelinstance_id,
    process_role,
    modelinstance_name,
    framework_type,
    framework_version,
    deploy_info,
    model_name,
    model_id,
    node_ids,
    node_ips,
    node_name,
    node_type,
    dev_mfr,
    dev_model,
    dev_status_code,
    dev_status_name,
    request_received_total_argMax,
    request_received_total_delta,
    request_received_total_qpm_avg,
    request_failed_total_argMax,
    request_failed_total_delta,
    request_success_total_argMax,
    request_success_total_delta,
    request_success_rate_avg,
    request_failed_rate_avg,
    inference_concurrent_requests_avg,
    num_requests_running_avg,
    num_requests_waiting_avg,
    num_requests_swapped_avg,
    num_preemptions_total_argMax,
    num_preemptions_total_delta,
    prompt_tokens_total_argMax,
    prompt_tokens_total_delta,
    generation_tokens_total_argMax,
    generation_tokens_total_delta,
    prompt_tokens_total_delta_rate_avg,
    generation_tokens_total_delta_rate_avg,
    total_throughput_rate,
    avg_prompt_throughput_toks_per_s_avg,
    avg_prompt_throughput_toks_per_s_delta,
    avg_prompt_throughput_toks_per_s_max,
    avg_generation_throughput_toks_per_s_avg,
    avg_generation_throughput_toks_per_s_delta,
    avg_generation_throughput_toks_per_s_max,

    time_to_first_token_seconds_argMax,
    time_to_first_token_seconds_avg,
    time_to_first_token_seconds_count_argMax,
    time_to_first_token_seconds_count_delta,
    time_to_first_token_seconds_sum_argMax,
    time_to_first_token_seconds_sum_delta,
    time_to_first_token_seconds_bucket_argMax,
    time_to_first_token_seconds_bucket_delta,

    time_per_output_token_seconds_argMax,
    time_per_output_token_seconds_avg,
    time_per_output_token_seconds_count_argMax,
    time_per_output_token_seconds_count_delta,
    time_per_output_token_seconds_sum_argMax,
    time_per_output_token_seconds_sum_delta,
    time_per_output_token_seconds_bucket_argMax,
    time_per_output_token_seconds_bucket_delta,
    e2e_request_latency_seconds_argMax,
    e2e_request_latency_seconds_avg,
    e2e_request_latency_seconds_count_argMax,
    e2e_request_latency_seconds_count_delta,
    e2e_request_latency_seconds_sum_argMax,
    e2e_request_latency_seconds_sum_delta,
    e2e_request_latency_seconds_bucket_argMax,
    e2e_request_latency_seconds_bucket_delta,
    npu_cache_usage_perc_avg,
    npu_cache_usage_perc_max,
    cpu_cache_usage_perc_avg,
    cpu_cache_usage_perc_max,
    npu_prefix_cache_hit_rate_avg,
    request_prompt_tokens_argMax,
    request_prompt_tokens_avg,
    request_prompt_tokens_max,
    request_prompt_tokens_count_argMax,
    request_prompt_tokens_count_delta,
    request_prompt_tokens_sum_argMax,
    request_prompt_tokens_sum_delta,
    request_prompt_tokens_bucket_argMax,
    request_prompt_tokens_bucket_delta,
    request_generation_tokens_argMax,
    request_generation_tokens_avg,
    request_generation_tokens_max,
    request_generation_tokens_count_argMax,
    request_generation_tokens_count_delta,
    request_generation_tokens_sum_argMax,
    request_generation_tokens_sum_delta,
    request_generation_tokens_bucket_argMax,
    request_generation_tokens_bucket_delta,
    sync_flag
)
SELECT
    -- ============================================================================
    -- 1. 时间粒度与逻辑维度分组（彻底抹除 container_id 和 group_id）
    -- ============================================================================
    toDateTime({start_time:UInt32}, {system_timezone:String})                          AS window_time,
    e.server_name,                                        -- 推理服务名称
    e.modelinstance_id,                                   -- 实例 ID
    e.process_role,                                       -- 必须保留：区分 P/D/Coordinator

    -- ============================================================================
    -- 2. 实例关联属性（锁住整小时内最后一个 5 分钟窗口的最新快照）
    -- ============================================================================
    argMax(e.modelinstance_name, e.orig_window_time)             AS modelinstance_name,
    argMax(e.framework_type, e.orig_window_time)                  AS framework_type,
    argMax(e.framework_version, e.orig_window_time)               AS framework_version,
    argMax(e.deploy_info, e.orig_window_time)                    AS deploy_info,
    argMax(e.model_name, e.orig_window_time)                     AS model_name,
    argMax(e.model_id, e.orig_window_time)                       AS model_id,
    arrayDistinct(groupArray(e.node_id))                  AS node_ids,
    arrayDistinct(groupArray(e.node_ip))                  AS node_ips,
    argMax(e.node_name, e.orig_window_time)                      AS node_name,
    argMax(e.node_type, e.orig_window_time)                      AS node_type,
    argMax(e.dev_mfr, e.orig_window_time)                        AS dev_mfr,
    argMax(e.dev_model, e.orig_window_time)                      AS dev_model,
    argMax(e.dev_status_code, e.orig_window_time)                AS dev_status_code,
    argMax(e.dev_status_name, e.orig_window_time)                AS dev_status_name,

    -- ============================================================================
    -- 3. Request Metrics: 跨 Group 物理累加 (`P1_Group` + `P2_Group`)
    -- ============================================================================
    argMax(e.request_received_total_argMax, e.orig_window_time)          AS request_received_total_argMax,
    sum(e.group_request_received_delta)                           AS request_received_total_delta,
    cast(sum(e.group_request_received_delta) / (dateDiff('second', toDateTime({start_time:UInt32}, 'UTC'), toDateTime({end_time:UInt32}, 'UTC')) / 60.0) AS Nullable(Float32))   AS request_received_total_qpm_avg,

    argMax(e.request_failed_total_argMax, e.orig_window_time)            AS request_failed_total_argMax,
    sum(e.group_request_failed_delta)                             AS request_failed_total_delta,

    argMax(e.request_success_total_argMax, e.orig_window_time)           AS request_success_total_argMax,
    sum(e.group_request_received_delta - e.group_request_failed_delta) AS request_success_total_delta,

    cast((sum(e.group_request_received_delta - e.group_request_failed_delta))
        / nullIf(sum(e.group_request_received_delta), 0) * 100.0 AS Nullable(Float32)) AS request_success_rate_avg,
    cast(sum(e.group_request_failed_delta)
        / nullIf(sum(e.group_request_received_delta), 0) * 100.0 AS Nullable(Float32)) AS request_failed_rate_avg,

    -- ============================================================================
    -- 4. Concurrency & Queue Metrics: 跨 Group 物理累加（DP 多组并发相加）
    -- ============================================================================
    sum(e.group_concurrent_requests)                       AS inference_concurrent_requests_avg,
    sum(e.group_num_requests_running)                      AS num_requests_running_avg,
    sum(e.group_num_requests_waiting)                      AS num_requests_waiting_avg,
    sum(e.group_num_requests_swapped)                      AS num_requests_swapped_avg,

    argMax(e.num_preemptions_total_argMax, e.orig_window_time)           AS num_preemptions_total_argMax,
    sum(e.group_num_preemptions_delta)                            AS num_preemptions_total_delta,

    -- ============================================================================
    -- 5. Token Totals & Throughput Rates: 跨 Group 物理累加
    -- ============================================================================
    argMax(e.prompt_tokens_total_argMax, e.orig_window_time)              AS prompt_tokens_total_argMax,
    sum(e.group_prompt_tokens_delta)                              AS prompt_tokens_total_delta,

    argMax(e.generation_tokens_total_argMax, e.orig_window_time)          AS generation_tokens_total_argMax,
    sum(e.group_generation_tokens_delta)                          AS generation_tokens_total_delta,

    cast(sum(e.group_prompt_tokens_delta) / (dateDiff('second', toDateTime({start_time:UInt32}, 'UTC'), toDateTime({end_time:UInt32}, 'UTC'))) AS Nullable(Float32))    AS prompt_tokens_total_delta_rate_avg,
    cast(sum(e.group_generation_tokens_delta) / (dateDiff('second', toDateTime({start_time:UInt32}, 'UTC'), toDateTime({end_time:UInt32}, 'UTC'))) AS Nullable(Float32)) AS generation_tokens_total_delta_rate_avg,
    cast((sum(e.group_prompt_tokens_delta) + sum(e.group_generation_tokens_delta)) / (dateDiff('second', toDateTime({start_time:UInt32}, 'UTC'), toDateTime({end_time:UInt32}, 'UTC'))) AS Nullable(Float32)) AS total_throughput_rate,

    sum(e.avg_prompt_throughput_toks_per_s_avg)           AS avg_prompt_throughput_toks_per_s_avg,
    sum(e.group_avg_prompt_throughput_delta)              AS avg_prompt_throughput_toks_per_s_delta,
    max(e.avg_prompt_throughput_toks_per_s_max)           AS avg_prompt_throughput_toks_per_s_max,

    sum(e.avg_generation_throughput_toks_per_s_avg)       AS avg_generation_throughput_toks_per_s_avg,
    sum(e.group_avg_generation_throughput_delta)          AS avg_generation_throughput_toks_per_s_delta,
    max(e.avg_generation_throughput_toks_per_s_max)       AS avg_generation_throughput_toks_per_s_max,

    argMax(e.time_to_first_token_seconds_argMax, e.orig_window_time) AS time_to_first_token_seconds_argMax,
    cast(sum(e.group_ttft_sum_delta)
        / nullIf(sum(e.group_ttft_count_delta), 0) AS Nullable(Float32)) AS time_to_first_token_seconds_avg,
    argMax(e.time_to_first_token_seconds_count_argMax, e.orig_window_time) AS time_to_first_token_seconds_count_argMax,
    sum(e.group_ttft_count_delta) AS time_to_first_token_seconds_count_delta,
    argMax(e.time_to_first_token_seconds_sum_argMax, e.orig_window_time) AS time_to_first_token_seconds_sum_argMax,
    sum(e.group_ttft_sum_delta) AS time_to_first_token_seconds_sum_delta,
    argMax(e.time_to_first_token_seconds_bucket_argMax,e.orig_window_time) AS time_to_first_token_seconds_bucket_argMax,
    -- 不同 Group 处理不同请求，Bucket 样本需要物理累加
    mapFromArrays(
        sumMap(
            mapKeys(e.group_ttft_bucket_delta),
            mapValues(e.group_ttft_bucket_delta)
        ).1,
        sumMap(
            mapKeys(e.group_ttft_bucket_delta),
            mapValues(e.group_ttft_bucket_delta)
        ).2
    ) AS time_to_first_token_seconds_bucket_delta,


    -- ============================================================================
    -- 6. TPOT & E2E: 跨 Group 加权重算与分布地图叠加 (`sumMap`)
    -- ============================================================================
    argMax(e.time_per_output_token_seconds_argMax, e.orig_window_time)   AS time_per_output_token_seconds_argMax,
    cast(sum(e.group_tpot_sum_delta)
        / nullIf(sum(e.group_tpot_count_delta), 0) AS Nullable(Float32))    AS time_per_output_token_seconds_avg,
    argMax(e.time_per_output_token_seconds_count_argMax, e.orig_window_time) AS time_per_output_token_seconds_count_argMax,
    sum(e.group_tpot_count_delta)                                  AS time_per_output_token_seconds_count_delta,
    argMax(e.time_per_output_token_seconds_sum_argMax, e.orig_window_time)   AS time_per_output_token_seconds_sum_argMax,
    sum(e.group_tpot_sum_delta)                                   AS time_per_output_token_seconds_sum_delta,
    argMax(e.time_per_output_token_seconds_bucket_argMax, e.orig_window_time) AS time_per_output_token_seconds_bucket_argMax,
    -- 跨组的 Bucket Map 必须用 sumMap 叠加！
    mapFromArrays(
        sumMap(
            mapKeys(e.group_tpot_bucket_delta),
            mapValues(e.group_tpot_bucket_delta)
        ).1,
        sumMap(
            mapKeys(e.group_tpot_bucket_delta),
            mapValues(e.group_tpot_bucket_delta)
        ).2
    ) AS time_per_output_token_seconds_bucket_delta,

    argMax(e.e2e_request_latency_seconds_argMax, e.orig_window_time)     AS e2e_request_latency_seconds_argMax,
    -- 加权重算 E2E
    cast(sum(e.group_e2e_sum_delta)
        / nullIf(sum(e.group_e2e_count_delta), 0) AS Nullable(Float32))     AS e2e_request_latency_seconds_avg,
    argMax(e.e2e_request_latency_seconds_count_argMax, e.orig_window_time)  AS e2e_request_latency_seconds_count_argMax,
    sum(e.group_e2e_count_delta)                          AS e2e_request_latency_seconds_count_delta,
    argMax(e.e2e_request_latency_seconds_sum_argMax, e.orig_window_time)    AS e2e_request_latency_seconds_sum_argMax,
    sum(e.group_e2e_sum_delta)                            AS e2e_request_latency_seconds_sum_delta,
    argMax(e.e2e_request_latency_seconds_bucket_argMax, e.orig_window_time) AS e2e_request_latency_seconds_bucket_argMax,
    -- 跨组的 Bucket Map 必须用 sumMap 叠加！
    mapFromArrays(
        sumMap(
            mapKeys(e.group_e2e_bucket_delta),
            mapValues(e.group_e2e_bucket_delta)
        ).1,
        sumMap(
            mapKeys(e.group_e2e_bucket_delta),
            mapValues(e.group_e2e_bucket_delta)
        ).2
    ) AS e2e_request_latency_seconds_bucket_delta,

    -- ============================================================================
    -- 7. Cache & Memory: 跨 Group 取均值 (组间 KV Cache 占有率求平均)
    -- ============================================================================
    avg(e.group_npu_cache_avg)                            AS npu_cache_usage_perc_avg,
    max(e.npu_cache_usage_perc_max)                       AS npu_cache_usage_perc_max,
    avg(e.group_cpu_cache_avg)                            AS cpu_cache_usage_perc_avg,
    max(e.cpu_cache_usage_perc_max)                       AS cpu_cache_usage_perc_max,
    avg(e.group_prefix_hit_avg)                           AS npu_prefix_cache_hit_rate_avg,

    -- ============================================================================
    -- 8. Prompt & Generation Tokens 分布
    -- ============================================================================
    argMax(e.request_prompt_tokens_argMax, e.orig_window_time)           AS request_prompt_tokens_argMax,
    cast(sum(e.group_prompt_tok_sum_delta)
        / nullIf(sum(e.group_prompt_tok_cnt_delta), 0) AS Nullable(Float32)) AS request_prompt_tokens_avg,
    max(e.request_prompt_tokens_max)                              AS request_prompt_tokens_max,
    argMax(e.request_prompt_tokens_count_argMax, e.orig_window_time)     AS request_prompt_tokens_count_argMax,
    sum(e.group_prompt_tok_cnt_delta)                             AS request_prompt_tokens_count_delta,
    argMax(e.request_prompt_tokens_sum_argMax, e.orig_window_time)       AS request_prompt_tokens_sum_argMax,
    sum(e.group_prompt_tok_sum_delta)                             AS request_prompt_tokens_sum_delta,
    argMax(e.request_prompt_tokens_bucket_argMax, e.orig_window_time)    AS request_prompt_tokens_bucket_argMax,
    mapFromArrays(
        sumMap(
            mapKeys(e.group_prompt_tok_bucket_delta),
            mapValues(e.group_prompt_tok_bucket_delta)
        ).1,
        sumMap(
            mapKeys(e.group_prompt_tok_bucket_delta),
            mapValues(e.group_prompt_tok_bucket_delta)
        ).2
    ) AS request_prompt_tokens_bucket_delta,

    argMax(e.request_generation_tokens_argMax, e.orig_window_time)       AS request_generation_tokens_argMax,
    cast(sum(e.group_gen_tok_sum_delta)
        / nullIf(sum(e.group_gen_tok_cnt_delta), 0) AS Nullable(Float32))   AS request_generation_tokens_avg,
    max(e.request_generation_tokens_max)                          AS request_generation_tokens_max,
    argMax(e.request_generation_tokens_count_argMax, e.orig_window_time) AS request_generation_tokens_count_argMax,
    sum(e.group_gen_tok_cnt_delta)                                AS request_generation_tokens_count_delta,
    argMax(e.request_generation_tokens_sum_argMax, e.orig_window_time)   AS request_generation_tokens_sum_argMax,
    sum(e.group_gen_tok_sum_delta)                                AS request_generation_tokens_sum_delta,
    argMax(e.request_generation_tokens_bucket_argMax, e.orig_window_time) AS request_generation_tokens_bucket_argMax,
    mapFromArrays(
        sumMap(
            mapKeys(e.group_gen_tok_bucket_delta),
            mapValues(e.group_gen_tok_bucket_delta)
        ).1,
        sumMap(
            mapKeys(e.group_gen_tok_bucket_delta),
            mapValues(e.group_gen_tok_bucket_delta)
        ).2
    ) AS request_generation_tokens_bucket_delta,

    -- ============================================================================
    -- 9. 标识控制
    -- ============================================================================
    any(e.sync_flag)                                      AS sync_flag

FROM
(
    -- 【子查询】：按 5 分钟 + group_id 聚合（组内多容器去重，锁住 Rank 0）
    SELECT
        f.window_time AS orig_window_time,
        f.server_name,
        f.modelinstance_id,
        f.process_role,
        f.group_id,

        argMax(f.node_ip, f.window_time)                    AS node_ip,
        argMax(f.modelinstance_name, f.window_time)         AS modelinstance_name,
        argMax(f.framework_type, f.window_time)              AS framework_type,
        argMax(f.framework_version, f.window_time)           AS framework_version,
        argMax(f.deploy_info, f.window_time)                AS deploy_info,
        argMax(f.model_name, f.window_time)                 AS model_name,
        argMax(f.model_id, f.window_time)                   AS model_id,
        argMax(f.node_id, f.window_time)                    AS node_id,
        argMax(f.node_name, f.window_time)                  AS node_name,
        argMax(f.node_type, f.window_time)                  AS node_type,
        argMax(f.dev_mfr, f.window_time)                    AS dev_mfr,
        argMax(f.dev_model, f.window_time)                  AS dev_model,
        argMax(f.dev_status_code, f.window_time)            AS dev_status_code,
        argMax(f.dev_status_name, f.window_time)            AS dev_status_name,

        -- 1. 组内去重：物理增量取 MAX
        max(f.request_received_total_delta)               AS group_request_received_delta,
        max(f.request_failed_total_delta)                 AS group_request_failed_delta,
        max(f.num_preemptions_total_delta)                AS group_num_preemptions_delta,
        max(f.prompt_tokens_total_delta)                  AS group_prompt_tokens_delta,
        max(f.generation_tokens_total_delta)              AS group_generation_tokens_delta,
        max(f.avg_prompt_throughput_toks_per_s_delta)     AS group_avg_prompt_throughput_delta,
        max(f.avg_generation_throughput_toks_per_s_delta) AS group_avg_generation_throughput_delta,

        -- 2. 组内去重：时延与 Token 分布的 Sum/Count 取 MAX
        max(f.time_to_first_token_seconds_sum_delta)      AS group_ttft_sum_delta,
        max(f.time_to_first_token_seconds_count_delta)    AS group_ttft_count_delta,
        max(f.time_per_output_token_seconds_sum_delta)    AS group_tpot_sum_delta,
        max(f.time_per_output_token_seconds_count_delta)  AS group_tpot_count_delta,
        max(f.e2e_request_latency_seconds_sum_delta)      AS group_e2e_sum_delta,
        max(f.e2e_request_latency_seconds_count_delta)    AS group_e2e_count_delta,
        max(f.request_prompt_tokens_sum_delta)            AS group_prompt_tok_sum_delta,
        max(f.request_prompt_tokens_count_delta)          AS group_prompt_tok_cnt_delta,
        max(f.request_generation_tokens_sum_delta)        AS group_gen_tok_sum_delta,
        max(f.request_generation_tokens_count_delta)      AS group_gen_tok_cnt_delta,

        -- 快照极值取组内最大
        max(f.request_received_total_argMax)              AS request_received_total_argMax,
        max(f.request_failed_total_argMax)                AS request_failed_total_argMax,
        max(f.request_success_total_argMax)               AS request_success_total_argMax,
        max(f.num_preemptions_total_argMax)               AS num_preemptions_total_argMax,
        max(f.prompt_tokens_total_argMax)                 AS prompt_tokens_total_argMax,
        max(f.generation_tokens_total_argMax)             AS generation_tokens_total_argMax,
        max(f.avg_prompt_throughput_toks_per_s_max)       AS avg_prompt_throughput_toks_per_s_max,
        max(f.avg_generation_throughput_toks_per_s_max)   AS avg_generation_throughput_toks_per_s_max,
        max(f.time_to_first_token_seconds_argMax)         AS time_to_first_token_seconds_argMax,
        max(f.time_to_first_token_seconds_count_argMax)   AS time_to_first_token_seconds_count_argMax,
        max(f.time_to_first_token_seconds_sum_argMax)     AS time_to_first_token_seconds_sum_argMax,
        argMax(f.time_to_first_token_seconds_bucket_argMax, f.window_time) AS time_to_first_token_seconds_bucket_argMax,
        max(f.time_per_output_token_seconds_argMax)       AS time_per_output_token_seconds_argMax,
        max(f.time_per_output_token_seconds_count_argMax) AS time_per_output_token_seconds_count_argMax,
        max(f.time_per_output_token_seconds_sum_argMax)   AS time_per_output_token_seconds_sum_argMax,
        argMax(f.time_per_output_token_seconds_bucket_argMax, f.window_time) AS time_per_output_token_seconds_bucket_argMax,
        max(f.e2e_request_latency_seconds_argMax)         AS e2e_request_latency_seconds_argMax,
        max(f.e2e_request_latency_seconds_count_argMax)   AS e2e_request_latency_seconds_count_argMax,
        max(f.e2e_request_latency_seconds_sum_argMax)     AS e2e_request_latency_seconds_sum_argMax,
        argMax(f.e2e_request_latency_seconds_bucket_argMax, f.window_time) AS e2e_request_latency_seconds_bucket_argMax,
        max(f.npu_cache_usage_perc_max)                   AS npu_cache_usage_perc_max,
        max(f.cpu_cache_usage_perc_max)                   AS cpu_cache_usage_perc_max,
        max(f.request_prompt_tokens_argMax)               AS request_prompt_tokens_argMax,
        max(f.request_prompt_tokens_max)                  AS request_prompt_tokens_max,
        max(f.request_prompt_tokens_count_argMax)         AS request_prompt_tokens_count_argMax,
        max(f.request_prompt_tokens_sum_argMax)           AS request_prompt_tokens_sum_argMax,
        argMax(f.request_prompt_tokens_bucket_argMax, f.window_time) AS request_prompt_tokens_bucket_argMax,
        max(f.request_generation_tokens_argMax)           AS request_generation_tokens_argMax,
        max(f.request_generation_tokens_max)              AS request_generation_tokens_max,
        max(f.request_generation_tokens_count_argMax)     AS request_generation_tokens_count_argMax,
        max(f.request_generation_tokens_sum_argMax)       AS request_generation_tokens_sum_argMax,
        argMax(f.request_generation_tokens_bucket_argMax, f.window_time) AS request_generation_tokens_bucket_argMax,

        -- 状态均值：组内求 AVG
        avg(f.inference_concurrent_requests_avg)          AS group_concurrent_requests,
        avg(f.num_requests_running_avg)                   AS group_num_requests_running,
        avg(f.num_requests_waiting_avg)                   AS group_num_requests_waiting,
        avg(f.num_requests_swapped_avg)                   AS group_num_requests_swapped,
        avg(f.avg_prompt_throughput_toks_per_s_avg)       AS avg_prompt_throughput_toks_per_s_avg,
        avg(f.avg_generation_throughput_toks_per_s_avg)   AS avg_generation_throughput_toks_per_s_avg,
        avg(f.npu_cache_usage_perc_avg)                   AS group_npu_cache_avg,
        avg(f.cpu_cache_usage_perc_avg)                   AS group_cpu_cache_avg,
        avg(f.npu_prefix_cache_hit_rate_avg)              AS group_prefix_hit_avg,

        -- Bucket Map：组内提取 Rank 0 容器的 Map
        argMax(f.time_to_first_token_seconds_bucket_delta, f.time_to_first_token_seconds_count_delta)     AS group_ttft_bucket_delta,
        argMax(f.time_per_output_token_seconds_bucket_delta, f.time_per_output_token_seconds_count_delta) AS group_tpot_bucket_delta,
        argMax(f.e2e_request_latency_seconds_bucket_delta, f.e2e_request_latency_seconds_count_delta)     AS group_e2e_bucket_delta,
        argMax(f.request_prompt_tokens_bucket_delta, f.request_prompt_tokens_count_delta)                 AS group_prompt_tok_bucket_delta,
        argMax(f.request_generation_tokens_bucket_delta, f.request_generation_tokens_count_delta)         AS group_gen_tok_bucket_delta,

        any(f.sync_flag)                                  AS sync_flag

    FROM modelmate_metric.t_mops__modelmetric_instance__5min_dws AS f
    WHERE f.window_time >= toDateTime({start_time:UInt32}, {system_timezone:String})
      AND f.window_time <  toDateTime({end_time:UInt32}, {system_timezone:String})
      AND f.sync_flag = 0
    -- 子查询按 group_id 聚集（组内去重）
    GROUP BY
        f.window_time,
        f.server_name,
        f.modelinstance_id,
        f.process_role,
        f.group_id
) AS e
-- 【DWS 最外层】：按 1 小时 + process_role 向上聚合（抹除 group_id，跨组物理累加）
GROUP BY
    toDateTime({start_time:UInt32}, {system_timezone:String}),
    e.server_name,
    e.modelinstance_id,
    e.process_role;
