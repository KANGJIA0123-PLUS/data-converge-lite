import re
from pathlib import Path

import yaml

RESOURCES = Path(__file__).resolve().parents[1] / "resources"


def test_all_52_company_sql_rules_have_dynamic_windows():
    rules = yaml.safe_load((RESOURCES / "config/convergence_rules.yaml").read_text())["convergence_rules"]
    assert len(rules) == 52
    assert len({rule["taskname"] for rule in rules}) == 52
    for rule in rules:
        sql = (RESOURCES / "sql" / (rule["sql_id"] + ".sql")).read_text()
        assert "{start_time:DateTime}" in sql, rule["taskname"]
        assert "{end_time:DateTime}" in sql, rule["taskname"]
        assert not re.search(r"WHERE\s+\w+\.window_time\s*>=\s*'2026-", sql, re.I)


def test_resource_server_day_has_one_e2e_count_delta_expression():
    sql = (RESOURCES / "sql/task_resource_server_day_ads.sql").read_text()
    expression = "sum(s.engine_e2e_request_latency_seconds_count_delta) AS engine_e2e_request_latency_seconds_count_delta"
    assert sql.count(expression) == 1
