"""Exercise the complete hour rollup with both ClickHouse analyzers."""

import json
import re
import subprocess

import pytest

from .paths import RESOURCES_ROOT
from .test_model_metric_sql import START, clickhouse_local


SQL_FILE = RESOURCES_ROOT / "sql" / "task_model_metric_instance_hour_dws.sql"
STRING_COLUMNS = {
    "server_name", "modelinstance_id", "process_role", "group_id",
    "modelinstance_name", "framework_type", "framework_version", "deploy_info",
    "model_name", "model_id", "node_id", "node_ip", "node_name", "node_type",
    "dev_mfr", "dev_model", "dev_status_code", "dev_status_name",
}


def source_column(name, row):
    if name == "window_time":
        return "DateTime('UTC')", str(START + row["offset"])
    if name in STRING_COLUMNS:
        value = row.get(name, "instance")
        return "String", repr(value)
    if "_bucket_" in name:
        count = row["snapshot"] if name.endswith("_argMax") else row["requests"]
        return "Map(Float64, Float64)", f"map(1.0, {count}.0)"
    if name.endswith("_argMax"):
        return "Float64", str(row["snapshot"])
    metrics = {
        "request_received_total_delta": row["requests"],
        "request_failed_total_delta": 1,
        "prompt_tokens_total_delta": row["requests"] * 10,
        "generation_tokens_total_delta": row["requests"] * 20,
        "time_to_first_token_seconds_count_delta": row["requests"],
        "time_to_first_token_seconds_sum_delta": row["requests"] * 2,
    }
    return "Float64", str(metrics.get(name, 0))


def source_rows():
    rows = []
    for offset, name, snapshot, requests in (
            (0, "old", 1000, (3, 5)), (3300, "latest", 37, (7, 11))):
        for group_index, count in enumerate(requests):
            row = {"offset": offset, "modelinstance_name": name,
                   "group_id": f"group-{group_index}", "snapshot": snapshot,
                   "requests": count}
            # Both TP ranks report the same group workload.
            rows.extend([dict(row), dict(row)])
    return rows


def hour_query(alias_preference, analyzer):
    sql = re.sub(r"--[^\n]*", "", SQL_FILE.read_text())
    sql = sql[sql.index("SELECT"):].rstrip().rstrip(";")
    columns = sorted(set(re.findall(r"\bf\.(\w+)", sql)))
    rows = source_rows()
    schema = ", ".join(f"{name} {source_column(name, rows[0])[0]}" for name in columns)
    records = ["(" + ", ".join(source_column(name, row)[1] for name in columns) + ")"
               for row in rows]
    source = "values('" + schema.replace("'", "\\'") + "', " + ", ".join(records) + ")"
    sql = sql.replace("modelmate_metric.t_mops__modelmetric_instance__5min_dws", source)
    return (sql + f" SETTINGS prefer_column_name_to_alias={alias_preference}, "
            f"enable_analyzer={analyzer} FORMAT JSONEachRow")


def run_hour_query(command, query):
    result = subprocess.run(
        command + ["--query", query, "--param_start_time", str(START),
                   "--param_end_time", str(START + 3600),
                   "--param_system_timezone", "Asia/Kathmandu"],
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stderr
    return [json.loads(line) for line in result.stdout.splitlines() if line.strip()]


@pytest.mark.parametrize("alias_preference", [0, 1])
@pytest.mark.parametrize("analyzer", [0, 1])
def test_hour_latest_snapshots_and_group_deltas(clickhouse_local, alias_preference, analyzer):
    rows = run_hour_query(clickhouse_local, hour_query(alias_preference, analyzer))
    assert len(rows) == 1
    row = rows[0]
    assert row["window_time"] == "2026-09-09 06:45:00"
    assert row["modelinstance_name"] == "latest"
    # A counter reset makes selecting the latest window distinguishable from max().
    for name, value in row.items():
        if name.endswith("_argMax"):
            assert value == ({"1": 37} if "_bucket_" in name else 37), name
    expected = {
        "request_received_total_delta": 26, "request_failed_total_delta": 4,
        "request_success_total_delta": 22, "prompt_tokens_total_delta": 260,
        "generation_tokens_total_delta": 520,
        "request_received_total_qpm_avg": 26 / 60,
        "time_to_first_token_seconds_avg": 2,
        "time_to_first_token_seconds_count_delta": 26,
        "time_to_first_token_seconds_sum_delta": 52,
    }
    for name, value in expected.items():
        assert row[name] == pytest.approx(value), name
    assert row["time_to_first_token_seconds_bucket_delta"] == {"1": 26}
