"""Deployment checks must report missing service metric columns before release."""

import pytest

from .paths import REPOSITORY_ROOT
from .test_model_metric_sql import clickhouse_local, run_query


CHECK = REPOSITORY_ROOT / "docs/metrics-schema-check.sql"
COLUMNS = ("input_output_token_rate", "request_received_total_qps",
           "unit_token_computepower_consumption")


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
