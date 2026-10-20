import re

import yaml

from .paths import RESOURCES_ROOT


def test_all_company_sql_rules_have_matching_files_and_dynamic_windows():
    rules = yaml.safe_load((RESOURCES_ROOT / "config/convergence_rules.yaml").read_text())["convergence_rules"]
    assert len(rules) == 60
    assert len({rule["taskname"] for rule in rules}) == 60
    assert {rule["sql_id"] for rule in rules} == {p.stem for p in (RESOURCES_ROOT / "sql").glob("*.sql")}
    for rule in rules:
        sql = (RESOURCES_ROOT / "sql" / (rule["sql_id"] + ".sql")).read_text()
        assert "toDateTime({start_time:UInt32}, 'UTC')" in sql, rule["taskname"]
        assert "toDateTime({end_time:UInt32}, 'UTC')" in sql, rule["taskname"]
        assert not re.search(r"WHERE\s+\w+\.window_time\s*>=\s*'2026-", sql, re.I)


def test_all_sql_working_hour_calculations_use_the_system_timezone():
    for path in sorted((RESOURCES_ROOT / "sql").glob("*.sql")):
        sql = re.sub(r"--[^\n]*", "", path.read_text())
        assert not re.search(r"\btoDate\s*\(", sql), path.name
        for call in re.findall(r"\btoHour\([^)]*\)", sql):
            assert call.endswith(", {system_timezone:String})"), (path.name, call)


def test_sql_buckets_follow_the_exact_scheduled_window_at_offset_changes():
    for path in sorted((RESOURCES_ROOT / "sql").glob("*.sql")):
        sql = re.sub(r"--[^\n]*", "", path.read_text())
        assert not re.search(r"\btoStartOf\w+\s*\(", sql), path.name
        if "toDateTime({start_time:UInt32}, {system_timezone:String})" in sql:
            # A window_time alias must not turn source filtering or argMax into a constant.
            assert not re.search(r"\b(?:WHERE|AND)\s+window_time\b", sql), path.name
            assert not re.search(r"\bargMax\([^)]*,\s*window_time\)", sql), path.name
