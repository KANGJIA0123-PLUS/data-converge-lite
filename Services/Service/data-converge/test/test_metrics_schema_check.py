"""Deployment checks must report missing metric columns before release."""

import pytest

from .paths import REPOSITORY_ROOT
from .test_model_metric_sql import clickhouse_local, run_query


CHECK = REPOSITORY_ROOT / "docs/metrics-schema-check.sql"
COLUMNS = ("input_output_token_rate", "request_received_total_qps",
           "unit_token_computepower_consumption")
REQUEST_COUNTS = ("request_failed_count", "request_success_count")
AGENT_COLUMNS = (
    "request_failed_rate", "request_success_rate", "infer_input_output_token_rate",
    "infer_e2e_request_latency_seconds_avg", "infer_input_token_num_length_avg",
    "infer_output_token_num_length_avg", "infer_request_received_qps",
    "time_per_output_token_seconds_avg", "time_to_first_token_seconds_avg",
    "prompt_tokens_per_s_avg", "generation_tokens_per_s_avg",
)


def request_requirements():
    logical = {"t_mops_request_realtime_dwd": ("result_code",)}
    for period in ("5min", "hour", "day", "month"):
        logical[f"t_mops_request_{period}_dws"] = REQUEST_COUNTS
        logical[f"t_mops_agent_{period}_ads"] = AGENT_COLUMNS
    return {table + suffix: columns for table, columns in logical.items()
            for suffix in ("", "_local")}


def check_query():
    return CHECK.read_text(encoding="utf-8").rstrip().rstrip(";") + " FORMAT JSONEachRow"


def test_missing_service_tables_report_all_new_metric_columns(clickhouse_local):
    rows = run_query(clickhouse_local, check_query())
    missing = {(row["table_name"], row["missing_column"]) for row in rows}
    for period in ("5min", "hour", "day", "month"):
        table = f"t_mops__modelmetric_server__{period}_ads"
        assert {(table, column) for column in COLUMNS} <= missing


@pytest.mark.parametrize("present", [COLUMNS[:1], COLUMNS])
def test_service_schema_check_only_reports_missing_columns(clickhouse_local, present):
    table = "t_mops__modelmetric_server__5min_ads"
    fields = ", ".join(f"{column} Nullable(Float32)" for column in present)
    setup = (f"CREATE DATABASE modelmate_metric; CREATE TABLE modelmate_metric.{table} "
             f"({fields}) ENGINE = Memory; ")
    rows = run_query(clickhouse_local, setup + check_query())
    missing = {row["missing_column"] for row in rows if row["table_name"] == table}
    assert missing == set(COLUMNS) - set(present)


def test_missing_request_tables_report_logical_and_local_columns(clickhouse_local):
    requirements = request_requirements()
    rows = run_query(clickhouse_local, check_query())
    missing = {(row["table_name"], row["missing_column"]) for row in rows
               if row["table_name"] in requirements}
    expected = {(table, column) for table, columns in requirements.items()
                for column in columns}
    assert missing == expected


@pytest.mark.parametrize("suffix", ("", "_local"))
@pytest.mark.parametrize("table,columns", (
    ("t_mops_request_realtime_dwd", ("result_code",)),
    ("t_mops_request_hour_dws", REQUEST_COUNTS),
    ("t_mops_agent_day_ads", AGENT_COLUMNS),
))
def test_request_schema_check_preserves_missing_columns(clickhouse_local, suffix, table, columns):
    table += suffix
    present = columns[:-1]
    fields = ["unrelated UInt8"] + [f"{column} Nullable(Float64)" for column in present]
    setup = (f"CREATE DATABASE modelmate_metric; CREATE TABLE modelmate_metric.{table} "
             f"({', '.join(fields)}) ENGINE = Memory; ")
    rows = run_query(clickhouse_local, setup + check_query())
    missing = {row["missing_column"] for row in rows if row["table_name"] == table}
    assert missing == set(columns) - set(present)


@pytest.mark.parametrize("include_local", (False, True))
def test_request_schema_check_requires_local_tables_independently(clickhouse_local, include_local):
    requirements = request_requirements()
    setup = ["CREATE DATABASE modelmate_metric;"]
    for table, columns in requirements.items():
        if table.endswith("_local") and not include_local:
            continue
        fields = ", ".join(f"{column} Nullable(Float64)" for column in columns)
        setup.append(f"CREATE TABLE modelmate_metric.{table} ({fields}) ENGINE = Memory;")
    rows = run_query(clickhouse_local, " ".join(setup) + check_query())
    missing = {(row["table_name"], row["missing_column"]) for row in rows
               if row["table_name"] in requirements}
    expected = {(table, column) for table, columns in requirements.items()
                for column in columns if table.endswith("_local") and not include_local}
    assert missing == expected
