import re
from pathlib import Path

import yaml

RESOURCES = Path(__file__).resolve().parents[1] / "resources"


def test_all_company_sql_rules_have_matching_files_and_dynamic_windows():
    rules = yaml.safe_load((RESOURCES / "config/convergence_rules.yaml").read_text())["convergence_rules"]
    assert len(rules) == 48
    assert len({rule["taskname"] for rule in rules}) == 48
    assert {rule["sql_id"] for rule in rules} == {p.stem for p in (RESOURCES / "sql").glob("*.sql")}
    for rule in rules:
        sql = (RESOURCES / "sql" / (rule["sql_id"] + ".sql")).read_text()
        assert "{start_time:DateTime}" in sql, rule["taskname"]
        assert "{end_time:DateTime}" in sql, rule["taskname"]
        assert not re.search(r"WHERE\s+\w+\.window_time\s*>=\s*'2026-", sql, re.I)
