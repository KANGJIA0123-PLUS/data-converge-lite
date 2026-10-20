"""Execute request rollups and agent metrics against local ClickHouse."""

import json
import re
import subprocess

import pytest

from .test_model_metric_sql import SQL_ROOT, START, clickhouse_local


PERIODS = ("5min", "hour", "day", "month")
METRICS = (
    "request_failed_rate", "request_success_rate", "infer_input_output_token_rate",
    "infer_e2e_request_latency_seconds_avg", "infer_input_token_num_length_avg",
    "infer_output_token_num_length_avg", "infer_request_received_qps",
    "time_per_output_token_seconds_avg", "time_to_first_token_seconds_avg",
    "prompt_tokens_per_s_avg", "generation_tokens_per_s_avg",
)


def request_sql(period, layer):
    text = (SQL_ROOT / f"task_request_{period}_{layer}.sql").read_text()
    return re.sub(r"--[^\n]*", "", text).strip().rstrip(";")


def execute_request(command, query, *, seconds=300, start=START):
    result = subprocess.run(
        command + ["--query", query + " FORMAT JSONEachRow",
                   "--param_start_time", str(start), "--param_end_time", str(start + seconds),
                   "--param_system_timezone", "Asia/Kathmandu"],
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stderr
    return [json.loads(line) for line in result.stdout.splitlines() if line.strip()]


def values_source(columns, rows):
    schema = ", ".join(f"{name} {kind}" for name, kind, _ in columns).replace("'", "\\'")
    records = []
    for row in rows:
        values = [repr(row.get(name, default)) if row.get(name, default) is not None else "NULL"
                  for name, _, default in columns]
        records.append("(" + ", ".join(values) + ")")
    return f"values('{schema}', " + ", ".join(records) + ")"


def dwd_source(rows, status_type="Nullable(Int32)"):
    columns = [
        ("metric_datetime", "DateTime('UTC')", START), ("agent_id", "Int64", 1),
        ("server_name", "String", "server"), ("model_name", "String", "model"),
        ("model_id", "Int64", 2), ("agent_name", "String", "agent"),
        ("node_id", "Int64", 3), ("node_ip", "String", "node"),
        ("sync_flag", "Int8", 0), ("result_code", status_type, 200),
    ]
    columns += [(name, "Nullable(Int64)", value) for name, value in (
        ("prefill_time", 1000), ("decode_time_average", 100), ("input_tokens", 10),
        ("output_tokens", 5), ("total_tokens", 15), ("total_time", 3000),
    )]
    return values_source(columns, rows)


def dws_source(rows):
    sql = request_sql("5min", "dws")
    names = [name.strip() for name in sql[sql.index("(") + 1:sql.index(")")].split(",")]
    columns = []
    for name in names:
        if name == "window_time":
            kind, default = "DateTime('UTC')", START
        elif name in ("server_name", "model_name", "agent_name"):
            kind, default = "String", name
        elif name.endswith("_array"):
            kind, default = ("Array(String)", ["node"]) if name == "node_ip_array" else ("Array(Int64)", [1])
        elif name in ("agent_id", "model_id", "sync_flag"):
            kind, default = "Int64", 0 if name == "sync_flag" else 1
        else:
            kind, default = "Nullable(Float64)", 0
        columns.append((name, kind, default))
    return values_source(columns, rows)


def source_query(period, layer, source, alias_preference, analyzer=1):
    sql = request_sql(period, layer)
    select = sql[sql.index("SELECT"):]
    query = re.sub(r"modelmate_metric\.\w+", lambda _: source, select, count=1)
    return (query + f" SETTINGS prefer_column_name_to_alias={alias_preference}, "
            f"enable_analyzer={analyzer}")


@pytest.mark.parametrize("alias_preference", [0, 1])
@pytest.mark.parametrize("status_type", ["Nullable(Int32)", "Nullable(String)"])
@pytest.mark.parametrize("analyzer", [0, 1])
def test_five_minute_outcomes_count_requests_and_filter_window(
    clickhouse_local, alias_preference, status_type, analyzer,
):
    rows = [{}, {"result_code": 500}, {"result_code": 200}, {"result_code": None},
            {"metric_datetime": START - 1}, {"metric_datetime": START + 300},
            {"sync_flag": 1}]
    if status_type == "Nullable(String)":
        rows = [dict(row, result_code=str(row.get("result_code", 200)))
                if row.get("result_code", 200) is not None else row for row in rows]
        rows[1]["result_code"] = "timeout"
    query = source_query("5min", "dws", dwd_source(rows, status_type), alias_preference, analyzer)
    row = execute_request(clickhouse_local, query)[0]
    assert (row["request_total_sum"], row["request_success_count"], row["request_failed_count"]) == (4, 2, 1)


@pytest.mark.parametrize("period", ("hour", "day", "month"))
@pytest.mark.parametrize("alias_preference", [0, 1])
@pytest.mark.parametrize("analyzer", [0, 1])
def test_larger_windows_sum_outcomes_instead_of_counting_rollup_rows(
    clickhouse_local, period, alias_preference, analyzer,
):
    step, seconds = {"hour": (300, 3600), "day": (3600, 86400),
                     "month": (86400, 2678400)}[period]
    rows = [
        {"request_total_sum": 7, "request_success_count": 5, "request_failed_count": 2},
        {"window_time": START + step, "request_total_sum": 11,
         "request_success_count": 8, "request_failed_count": 3},
        {"window_time": START + seconds, "request_total_sum": 100, "request_failed_count": 100},
    ]
    query = source_query(period, "dws", dws_source(rows), alias_preference, analyzer)
    row = execute_request(clickhouse_local, query, seconds=seconds)[0]
    assert (row["request_total_sum"], row["request_success_count"], row["request_failed_count"]) == (18, 13, 5)


@pytest.mark.parametrize("period", PERIODS)
@pytest.mark.parametrize("alias_preference", [0, 1])
@pytest.mark.parametrize("analyzer", [0, 1])
def test_agent_metrics_weight_requests_and_convert_milliseconds(
    clickhouse_local, period, alias_preference, analyzer,
):
    rows = [
        {"request_total_sum": 2, "request_success_count": 2, "request_failed_count": 0,
         "input_tokens_sum": 100, "output_tokens_sum": 20, "prefill_time_sum": 1000,
         "decode_time_average_sum": 200, "total_time_sum": 4000},
        {"request_total_sum": 6, "request_success_count": 4, "request_failed_count": 2,
         "input_tokens_sum": 300, "output_tokens_sum": 180, "prefill_time_sum": 15000,
         "decode_time_average_sum": 1400, "total_time_sum": 28000},
    ]
    query = source_query(period, "agent_ads", dws_source(rows), alias_preference, analyzer)
    row = execute_request(clickhouse_local, query)[0]
    expected = dict(zip(METRICS, (25, 75, 2, 4, 50, 25, 8 / 300, 0.2, 2, 400 / 300, 200 / 300)))
    for name, value in expected.items():
        assert row[name] == pytest.approx(value), name
    assert row["prefill_time_avg"] == 2000
    assert row["decode_time_average_avg"] == 200


@pytest.mark.parametrize("period", PERIODS)
@pytest.mark.parametrize("empty", [0, None])
def test_agent_metrics_keep_zero_denominator_policy(clickhouse_local, period, empty):
    source = dws_source([{"request_total_sum": empty, "input_tokens_sum": empty,
                          "output_tokens_sum": empty, "prefill_time_sum": empty,
                          "decode_time_average_sum": empty, "total_time_sum": empty}])
    row = execute_request(clickhouse_local, source_query(period, "agent_ads", source, 0))[0]
    assert all(row[name] == 0 for name in METRICS)


@pytest.mark.parametrize("period", PERIODS)
def test_latency_averages_do_not_divide_by_token_counts(clickhouse_local, period):
    source = dws_source([{"request_total_sum": 2, "prefill_time_sum": 4000,
                          "decode_time_average_sum": 200, "total_time_sum": 8000}])
    row = execute_request(clickhouse_local, source_query(period, "agent_ads", source, 0))[0]
    assert row["time_to_first_token_seconds_avg"] == 2
    assert row["time_per_output_token_seconds_avg"] == pytest.approx(0.1)
    assert row["infer_e2e_request_latency_seconds_avg"] == 4


@pytest.mark.parametrize("seconds", [300, 3600, 82800, 90000, 2678400])
def test_agent_rates_use_elapsed_seconds(clickhouse_local, seconds):
    source = dws_source([{"request_total_sum": 60, "input_tokens_sum": 120, "output_tokens_sum": 180}])
    row = execute_request(
        clickhouse_local, source_query("day", "agent_ads", source, 0), seconds=seconds)[0]
    assert row["infer_request_received_qps"] == pytest.approx(60 / seconds)
    assert row["prompt_tokens_per_s_avg"] == pytest.approx(120 / seconds)
    assert row["generation_tokens_per_s_avg"] == pytest.approx(180 / seconds)


@pytest.mark.parametrize("period", PERIODS)
def test_request_parameters_use_unsigned_epoch_seconds(period):
    for layer in ("dws", "agent_ads"):
        parameters = re.findall(r"\{(?:start_time|end_time):([^}]+)\}", request_sql(period, layer))
        assert parameters and set(parameters) == {"UInt32"}


def test_request_outcomes_and_latencies_survive_the_whole_rollup_chain(clickhouse_local):
    query = source_query("5min", "dws", dwd_source([{}, {"result_code": 500}]), 0)
    for period in ("hour", "day", "month"):
        source = "(" + query.split(" SETTINGS ")[0] + ")"
        query = source_query(period, "dws", source, 0)
    row = execute_request(clickhouse_local, query)[0]
    assert (row["request_total_sum"], row["request_success_count"], row["request_failed_count"]) == (2, 1, 1)
    source = "(" + query.split(" SETTINGS ")[0] + ")"
    row = execute_request(clickhouse_local, source_query("month", "agent_ads", source, 0))[0]
    assert row["request_failed_rate"] == row["request_success_rate"] == 50
    assert row["time_to_first_token_seconds_avg"] == 1
    assert row["time_per_output_token_seconds_avg"] == pytest.approx(0.1)
    assert row["infer_e2e_request_latency_seconds_avg"] == 3
