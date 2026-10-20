"""Resource SQL regressions; share the opt-in real ClickHouse local runner."""

import re

import pytest

from .paths import RESOURCES_ROOT
from .test_model_metric_sql import START, clickhouse_local, run_query


SQL_ROOT = RESOURCES_ROOT / "sql"
RESOURCE_SQL = [
    f"task_model_{kind}_{period}_ads.sql"
    for kind in ("instance", "server")
    for period in ("5min", "hour", "day", "month")
]
BUSY_SQL = [name for name in RESOURCE_SQL if "_day_" in name or "_month_" in name]


def aggregate_expression(filename, column):
    sql = (SQL_ROOT / filename).read_text()
    body = sql.split("card_metrics_agg AS (", 1)[1].split("FROM ", 1)[0]
    match = re.search(rf"^\s*(.*?)\s+AS {column}\s*,?\s*$", body, re.M)
    assert match, (filename, column)
    return match.group(1)


@pytest.mark.parametrize("filename", RESOURCE_SQL)
def test_card_count_keeps_same_card_indexes_on_different_nodes(clickhouse_local, filename):
    expression = aggregate_expression(filename, "allocated_npu_num")
    query = f"""SELECT {expression} AS count FROM values(
        'node_ip String, ai_card_index UInt32, chip_id UInt32',
        ('10.0.0.1', 0, 0), ('10.0.0.1', 0, 0), ('10.0.0.2', 0, 0)
    ) AS c FORMAT JSONEachRow"""
    assert run_query(clickhouse_local, query) == [{"count": 2}]


@pytest.mark.parametrize("filename", BUSY_SQL)
def test_day_and_month_preserve_upstream_busy_and_idle_measurements(clickhouse_local, filename):
    columns = [
        f"{period}_time_ai_core_util_{statistic}"
        for period in ("work", "idle")
        for statistic in ("avg", "max", "min")
    ]
    expressions = ", ".join(
        f"{aggregate_expression(filename, column)} AS {column}" for column in columns)
    schema = ["window_time DateTime"] + [
        f"{column} Nullable(Float64)" for column in
        ["ai_core_util_avg", "ai_core_util_max", "ai_core_util_min"] + columns
    ]
    source = (
        "values('" + ", ".join(schema) + "', "
        "('2026-10-01 00:00:00', 50, 100, 0, 80, 90, 60, 20, 40, 10), "
        "('2026-10-01 00:00:00', 30, 60, 0, 40, 50, 30, 10, 15, 5))"
    )
    query = f"SELECT {expressions} FROM {source} AS c FORMAT JSONEachRow"
    assert run_query(clickhouse_local, query) == [dict(zip(columns, (60, 90, 30, 15, 40, 5)))]


@pytest.mark.parametrize("period", ("5min", "hour", "day", "month"))
def test_service_windows_use_unsigned_epoch_parameters(period):
    sql = (SQL_ROOT / f"task_model_server_{period}_ads.sql").read_text()
    parameters = re.findall(r"\{(start_time|end_time):([^}]+)\}", sql)
    assert parameters and {kind for _, kind in parameters} == {"UInt32"}


def dws_fixture(source_filename):
    sql = re.sub(r"--[^\n]*", "", (SQL_ROOT / source_filename).read_text())
    header = sql.split("INSERT INTO", 1)[1].split("SELECT", 1)[0]
    columns = [column.strip() for column in header[header.index("(") + 1:header.rindex(")")].split(",")]
    schema, records = [], [[], []]
    for column in columns:
        numeric = column == "sync_flag" or re.search(r"_(avg|max|min|argMax|counter)$", column)
        kind = "DateTime('UTC')" if column == "window_time" else "Float64" if numeric else "String"
        schema.append(f"{column} {kind}")
        for index, record in enumerate(records):
            value = "0" if column == "sync_flag" else "1" if numeric else "'model'"
            if column == "window_time":
                value = str(START)
            elif column == "node_ip":
                value = repr(f"10.0.0.{index + 1}")
            elif column in ("ai_card_id", "ai_card_index", "chip_id"):
                value = "'0'"
            record.append(value)
    definition = ", ".join(schema).replace("'", "\\'")
    rows = ", ".join("(" + ", ".join(record) + ")" for record in records)
    return f"values('{definition}', {rows})"


@pytest.mark.parametrize("period", ("5min", "hour", "day", "month"))
def test_service_query_keeps_scheduled_instant_and_node_card_identity(clickhouse_local, period):
    sql = re.sub(r"--[^\n]*", "", (SQL_ROOT / f"task_model_server_{period}_ads.sql").read_text())
    query = sql[sql.index("WITH"):].rstrip().rstrip(";")
    gpu_sources = {
        "5min": "task_gpu_realtime_to_5min_dws.sql", "hour": "task_gpu_5min_to_hour_dws.sql",
        "day": "task_gpu_hour_to_day_dws.sql", "month": "task_gpu_day_to_month_dws.sql",
    }
    query = query.replace(
        f"modelmate_metric.t_mops_modelinstance_{period}_dws",
        dws_fixture(f"task_model_instance_{period}_dws.sql"))
    query = query.replace(
        f"modelmate_metric.t_mops_gpu_{period}_dws", dws_fixture(gpu_sources[period]))
    query = ("SELECT toUnixTimestamp(window_time) AS instant, timezoneOf(window_time) AS zone, "
             f"allocated_npu_num FROM ({query}) FORMAT JSONEachRow")
    assert run_query(clickhouse_local, query) == [{
        "instant": START, "zone": "Asia/Kathmandu", "allocated_npu_num": 2,
    }]
