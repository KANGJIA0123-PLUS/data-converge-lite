"""Execute nullable metric expressions with the opt-in local ClickHouse runner."""

import re

import pytest

from .test_model_metric_sql import SQL_ROOT, clickhouse_local, node_query, run_query


RATIOS = ("request_failed_rate_avg", "time_per_output_token_seconds_avg")
NODE_RATIOS = (
    "generation_tokens_per_request_avg", "e2e_latency_avg", "request_failed_rate",
    "request_success_rate", "time_to_first_token_seconds_avg",
    "time_per_output_token_seconds_avg",
)


def cast_expression(filename, column):
    sql = re.sub(r"--[^\n]*", "", (SQL_ROOT / filename).read_text())
    alias = re.search(rf"\bAS\s+{column}\b", sql, re.I)
    assert alias, (filename, column)
    prefix = sql[:alias.start()].rstrip()
    starts = list(re.finditer(r"\bCAST\s*\(", prefix, re.I))
    assert starts, (filename, column)
    return prefix[starts[-1].start():]


def ratio_query(period, column, numerator, denominator):
    expression = cast_expression(f"task_model_metric_instance_{period}_dws.sql", column)
    references = re.findall(r"\b([a-z]+)\.(\w+)", expression)
    assert len(set(references)) == 2, expression
    alias = references[0][0]
    names = [name for _, name in references]
    values = ["NULL" if value is None else str(value)
              for value in (numerator, denominator)]
    schema = ", ".join(f"{name} Nullable(Float64)" for name in names)
    source = f"values('{schema}', ({', '.join(values)})) AS {alias}"
    return (f"SELECT value, toTypeName(value) AS value_type FROM "
            f"(SELECT {expression} AS value FROM {source}) FORMAT JSONEachRow")


@pytest.mark.parametrize("period", ("hour", "day", "month"))
@pytest.mark.parametrize("column", RATIOS)
@pytest.mark.parametrize("numerator,denominator", [(0, 0), (None, None), (2, 10)])
def test_dws_ratios_preserve_missing_denominator_without_cast_failure(
    clickhouse_local, period, column, numerator, denominator
):
    row = run_query(clickhouse_local, ratio_query(period, column, numerator, denominator))[0]
    assert row["value_type"] == "Nullable(Float32)"
    if not denominator:
        assert row["value"] is None
    else:
        scale = 100 if column == "request_failed_rate_avg" else 1
        assert row["value"] == pytest.approx(numerator / denominator * scale)


@pytest.mark.parametrize("values,expected", [
    ("(NULL, 1)", None),
    ("(NULL, 1), (0, 1), (100, 1), (100, 0)", 0.5),
])
def test_gpu_utilization_skips_null_measurements(clickhouse_local, values, expected):
    sql = (SQL_ROOT / "task_gpu_realtime_to_5min_dws.sql").read_text()
    expression = re.search(
        r"^\s*(avgIf\(.*)\s+AS ai_card_used_ratio_avg,", sql, re.M
    ).group(1)
    source = "values('ai_core_util Nullable(Float32), ai_card_used UInt8', " + values + ")"
    query = (f"SELECT value, toTypeName(value) AS value_type FROM "
             f"(SELECT {expression} AS value FROM {source} AS r) FORMAT JSONEachRow")
    row = run_query(clickhouse_local, query)[0]
    assert row["value_type"] == "Nullable(Float64)"
    if expected is None:
        assert row["value"] is None
    else:
        assert row["value"] == pytest.approx(expected)


@pytest.mark.parametrize("period", ("5min", "hour", "day", "month"))
def test_nullable_node_ratios_keep_existing_idle_zero_policy(clickhouse_local, period):
    query = node_query(period, [{}]).removesuffix(" FORMAT JSONEachRow")
    columns = ", ".join(NODE_RATIOS)
    types = ", ".join(f"toTypeName({name}) AS {name}_type" for name in NODE_RATIOS)
    row = run_query(
        clickhouse_local, f"SELECT {columns}, {types} FROM ({query}) FORMAT JSONEachRow"
    )[0]
    for name in NODE_RATIOS:
        assert row[name] == 0, name
        assert row[f"{name}_type"] == "Nullable(Float32)", name
