"""Metric SQL regressions; opt into real ClickHouse local with a JSON command list."""

import json
import os
import re
import subprocess

import pytest

from .paths import RESOURCES_ROOT

PERIODS = ("5min", "hour", "day", "month")
SQL_ROOT = RESOURCES_ROOT / "sql"
START = 1788915600
STRING_COLUMNS = (
    "node_id", "node_name", "node_ip", "node_type", "dev_mfr", "dev_model",
    "dev_status_code", "dev_status_name", "process_role",
)
NUMBER_COLUMNS = (
    "request_received_total_delta", "request_failed_total_delta",
    "request_success_total_delta", "prompt_tokens_total_delta",
    "generation_tokens_total_delta", "num_preemptions_total_delta",
    "e2e_request_latency_seconds_sum_delta", "e2e_request_latency_seconds_count_delta",
    "time_to_first_token_seconds_sum_delta", "time_to_first_token_seconds_count_delta",
    "time_per_output_token_seconds_sum_delta", "time_per_output_token_seconds_count_delta",
    "generation_tokens_total_delta_rate_avg", "prompt_tokens_total_delta_rate_avg",
    "generation_tokens_per_s_avg", "prompt_tokens_per_s_avg",
    "request_received_total_qpm_avg", "total_tokens_per_s_avg", "sync_flag",
)


@pytest.fixture
def clickhouse_local():
    command = os.environ.get("DATA_CONVERGE_CLICKHOUSE_LOCAL_COMMAND")
    if not command:
        pytest.skip("Set DATA_CONVERGE_CLICKHOUSE_LOCAL_COMMAND to a JSON command list")
    return json.loads(command)


def run_query(command, query):
    result = subprocess.run(
        command + ["--query", query, "--param_start_time", str(START),
                   "--param_end_time", str(START + 300),
                   "--param_system_timezone", "Asia/Kathmandu"],
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stderr
    return [json.loads(line) for line in result.stdout.splitlines() if line.strip()]


def metric_sql(kind, period):
    return (SQL_ROOT / f"task_model_metric_{kind}_{period}_ads.sql").read_text()


def source_values(rows):
    schema = ["window_time DateTime('UTC')"]
    schema += [f"{column} String" for column in STRING_COLUMNS]
    schema += [f"{column} Float64" for column in NUMBER_COLUMNS]
    records = []
    for row in rows:
        values = [str(START + row.get("offset", 0))]
        values += [repr(row.get(column, "node")) for column in STRING_COLUMNS]
        values += [str(row.get(column, 0)) for column in NUMBER_COLUMNS]
        records.append("(" + ", ".join(values) + ")")
    definition = ", ".join(schema).replace("'", "\\'")
    return "values('" + definition + "', " + ", ".join(records) + ")"


def node_query(period, rows):
    sql = re.sub(r"--[^\n]*", "", metric_sql("node", period))
    select = sql[sql.index("SELECT"):].rstrip().rstrip(";")
    return re.sub(r"modelmate_metric\.\w+", source_values(rows), select, count=1) + " FORMAT JSONEachRow"


@pytest.mark.parametrize("period", PERIODS)
def test_instance_window_parameters_keep_the_unsigned_epoch_contract(period):
    sql = re.sub(r"--[^\n]*", "", metric_sql("instance", period))
    parameters = re.findall(r"\{(start_time|end_time):([^}]+)\}", sql)
    assert parameters and {kind for _, kind in parameters} == {"UInt32"}


@pytest.mark.parametrize("period", PERIODS)
def test_node_idle_windows_produce_zero_ratios(clickhouse_local, period):
    row = run_query(clickhouse_local, node_query(period, [{}]))[0]
    for name in ("generation_tokens_per_request_avg", "e2e_latency_avg",
                 "request_failed_rate", "request_success_rate",
                 "time_to_first_token_seconds_avg", "time_per_output_token_seconds_avg"):
        assert row[name] == 0, name


def test_node_pd_success_denominator_includes_decode_failures(clickhouse_local):
    rows = [
        {"process_role": "prefill", "request_received_total_delta": 10,
         "request_failed_total_delta": 1, "prompt_tokens_total_delta": 300},
        {"process_role": "decode", "request_received_total_delta": 10,
         "request_failed_total_delta": 2, "generation_tokens_total_delta": 700},
    ]
    row = run_query(clickhouse_local, node_query("5min", rows))[0]
    assert row["request_received_total_delta"] == 10
    assert row["request_success_total_delta"] == 7
    assert row["generation_tokens_per_request_avg"] == pytest.approx(100)
    assert row["request_failed_rate"] == pytest.approx(30)
    assert row["request_success_rate"] == pytest.approx(70)


@pytest.mark.parametrize("period", ("hour", "day", "month"))
def test_node_rollups_weight_latency_by_request_count(clickhouse_local, period):
    rows = [
        {"request_received_total_delta": 10, "request_failed_total_delta": 1,
         "request_success_total_delta": 9, "generation_tokens_total_delta": 90,
         "e2e_request_latency_seconds_sum_delta": 20,
         "e2e_request_latency_seconds_count_delta": 2},
        {"request_received_total_delta": 20, "request_failed_total_delta": 2,
         "request_success_total_delta": 18, "generation_tokens_total_delta": 180,
         "e2e_request_latency_seconds_sum_delta": 60,
         "e2e_request_latency_seconds_count_delta": 3},
    ]
    row = run_query(clickhouse_local, node_query(period, rows))[0]
    assert row["request_success_total_delta"] == 27
    assert row["generation_tokens_per_request_avg"] == pytest.approx(10)
    assert row["e2e_latency_avg"] == pytest.approx(16)


def instance_formula_query(period, tokens):
    sql = metric_sql("instance", period)
    formula = re.search(
        r"CAST\(coalesce\([\s\S]*?\) AS Nullable\(Float32\)\) AS unit_token_computepower_consumption",
        sql[sql.index("-- (3)"):], re.I,
    ).group()
    source = (f"SELECT toFloat64({tokens}) AS engine_prompt_tokens_total_delta, "
              "toFloat64(0) AS engine_generation_tokens_total_delta")
    return f"SELECT {formula} FROM ({source}) AS t CROSS JOIN (SELECT 2 AS allocated_npu_num) AS m FORMAT JSONEachRow"


@pytest.mark.parametrize("period", PERIODS)
def test_instance_allocation_join_executes_with_real_clickhouse(clickhouse_local, period):
    sql = metric_sql("instance", period)
    query = sql.rsplit("LEFT JOIN\n(", 1)[1].split(") AS m", 1)[0]
    source = ("values('window_time DateTime, server_name String, modelinstance_id String, "
              f"allocated_npu_num UInt64', ({START}, 'service', 'instance', 2))")
    query = query.replace(f"modelmate_metric.t_mops_modelinstance_{period}_ads", source)
    rows = run_query(clickhouse_local, query + " FORMAT JSONEachRow")
    assert len(rows) == 1
    assert rows[0]["allocated_npu_num"] == 2
    assert rows[0]["server_name"] == "service"
    assert rows[0]["modelinstance_id"] == "instance"


@pytest.mark.parametrize("period", PERIODS)
@pytest.mark.parametrize("tokens,expected", [(1000000, 1 / 6), (2000000, 1 / 12), (0, 0)])
def test_instance_card_hours_per_million_tokens(clickhouse_local, period, tokens, expected):
    row = run_query(clickhouse_local, instance_formula_query(period, tokens))[0]
    assert row["unit_token_computepower_consumption"] == pytest.approx(expected)
