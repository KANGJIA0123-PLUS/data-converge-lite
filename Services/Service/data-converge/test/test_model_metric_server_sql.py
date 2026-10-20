"""Execute service metric SQL against independent metric and resource fixtures."""

import json
import re
import subprocess

import pytest

from .test_model_metric_sql import PERIODS, START, clickhouse_local, metric_sql, run_query


STRING_COLUMNS = {
    "server_name", "framework_type", "framework_version", "deploy_info", "model_name",
    "model_id", "node_name", "node_type", "dev_mfr", "dev_model", "dev_status_code",
    "dev_status_name",
}


def metric_column(name, row):
    if name == "window_time":
        return "DateTime('UTC')", str(START + row.get("offset", 0))
    if name in STRING_COLUMNS:
        return "String", repr(row.get(name, "service"))
    if name in ("node_ids", "node_ips"):
        return "Array(String)", "['node-1']"
    if name.endswith("_bucket_delta"):
        return "Map(Float64, Float64)", "map(1.0, 1.0)"
    return "Float64", str(row.get(name, 0))


def metric_source(period, rows):
    columns = sorted(set(re.findall(r"\bins\.(\w+)", metric_sql("server", period))))
    schema = ", ".join(f"{name} {metric_column(name, {})[0]}" for name in columns)
    records = ["(" + ", ".join(metric_column(name, row)[1] for name in columns) + ")"
               for row in rows]
    return "values('" + schema.replace("'", "\\'") + "', " + ", ".join(records) + ")"


def resource_source(rows):
    schema = ("window_time DateTime('UTC'), server_name String, "
              "modelinstance_id String, allocated_npu_num UInt64")
    records = [f"({START + offset}, '{server}', '{instance}', {cards})"
               for offset, server, instance, cards in rows]
    return "values('" + schema.replace("'", "\\'") + "', " + ", ".join(records) + ")"


def server_query(period, metrics, resources):
    sql = re.sub(r"--[^\n]*", "", metric_sql("server", period))
    sql = sql[sql.index("SELECT"):].rstrip().rstrip(";")
    sql = sql.replace(f"modelmate_metric.t_mops__modelmetric_instance__{period}_ads",
                      metric_source(period, metrics))
    sql = sql.replace(f"modelmate_metric.t_mops_modelinstance_{period}_ads",
                      resource_source(resources))
    return sql + " FORMAT JSONEachRow"


def service_metrics(tokens=1000000):
    return [
        {"engine_prompt_tokens_total_delta": tokens / 4,
         "engine_generation_tokens_total_delta": tokens / 4,
         "engine_request_received_total_delta": 30},
        {"engine_prompt_tokens_total_delta": tokens / 4,
         "engine_generation_tokens_total_delta": tokens / 4,
         "engine_request_received_total_delta": 60},
    ]


@pytest.mark.parametrize("period", PERIODS)
def test_server_counts_resource_instances_once_and_keeps_service_totals(clickhouse_local, period):
    resources = [(0, "service", "one", 2), (0, "service", "one", 2),
                 (0, "service", "two", 4), (0, "other", "one", 100)]
    rows = run_query(clickhouse_local, server_query(period, service_metrics(), resources))
    assert len(rows) == 1
    row = rows[0]
    assert row["server_name"] == "service"
    assert row["engine_prompt_tokens_total_delta"] == 500000
    assert row["engine_generation_tokens_total_delta"] == 500000
    assert row["request_received_total_qps"] == pytest.approx(0.3)
    # Six cards for five minutes produce 0.5 card-hours per million tokens.
    assert row["unit_token_computepower_consumption"] == pytest.approx(0.5)


def formula_query(period, tokens):
    sql = metric_sql("server", period)
    formula = re.search(
        r"CAST\(coalesce\([\s\S]*?\) AS Nullable\(Float32\)\) AS unit_token_computepower_consumption",
        sql[sql.index("-- (3)"):], re.I,
    ).group()
    source = (f"SELECT toFloat64({tokens / 2}) AS engine_prompt_tokens_total_delta, "
              f"toFloat64({tokens / 2}) AS engine_generation_tokens_total_delta")
    return (f"SELECT {formula} FROM ({source}) AS t "
            "CROSS JOIN (SELECT 6 AS total_allocated_npu) AS s FORMAT JSONEachRow")


@pytest.mark.parametrize("period", PERIODS)
@pytest.mark.parametrize("tokens,expected", [(1000000, 0.5), (2000000, 0.25), (0, 0)])
def test_server_card_hours_per_million_tokens(clickhouse_local, period, tokens, expected):
    row = run_query(clickhouse_local, formula_query(period, tokens))[0]
    assert row["unit_token_computepower_consumption"] == pytest.approx(expected)


@pytest.mark.parametrize("period,seconds,expected", [
    ("5min", 300, 0.5), ("hour", 3600, 6), ("day", 86400, 144),
    ("day", 82800, 138), ("month", 2678400, 4464),
])
def test_server_card_hours_follow_actual_window_duration(clickhouse_local, period, seconds, expected):
    result = subprocess.run(
        clickhouse_local + ["--query", formula_query(period, 1000000),
                            "--param_start_time", str(START),
                            "--param_end_time", str(START + seconds)],
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stderr
    row = json.loads(result.stdout)
    assert row["unit_token_computepower_consumption"] == pytest.approx(expected)


@pytest.mark.parametrize("period", PERIODS)
def test_server_filters_metric_and_resource_windows_with_exclusive_end(clickhouse_local, period):
    metrics = service_metrics()
    metrics += [dict(service_metrics(100000000)[0], offset=offset) for offset in (-1, 300)]
    resources = [(0, "service", "one", 6), (-1, "service", "before", 100),
                 (300, "service", "after", 100)]
    rows = run_query(clickhouse_local, server_query(period, metrics, resources))
    assert len(rows) == 1
    assert rows[0]["engine_prompt_tokens_total_delta"] == 500000
    assert rows[0]["unit_token_computepower_consumption"] == pytest.approx(0.5)


@pytest.mark.parametrize("period", PERIODS)
def test_server_missing_allocation_preserves_metrics_with_zero_card_hours(clickhouse_local, period):
    query = server_query(period, service_metrics(), [(0, "other", "one", 100)])
    rows = run_query(clickhouse_local, query)
    assert len(rows) == 1
    assert rows[0]["engine_prompt_tokens_total_delta"] == 500000
    assert rows[0]["unit_token_computepower_consumption"] == 0


@pytest.mark.parametrize("period", PERIODS)
def test_server_window_parameters_use_the_unsigned_epoch_contract(period):
    sql = re.sub(r"--[^\n]*", "", metric_sql("server", period))
    parameters = re.findall(r"\{(start_time|end_time):([^}]+)\}", sql)
    assert parameters and {kind for _, kind in parameters} == {"UInt32"}
