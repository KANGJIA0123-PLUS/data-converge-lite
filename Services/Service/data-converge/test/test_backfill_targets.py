"""Recomputation must delete the same table/window that packaged SQL inserts."""

import re
from datetime import datetime, timezone
from pathlib import Path

import pytest
import yaml

from data_converge.config.loader import ConfigLoader
from data_converge.dag import DAGBuilder
from data_converge.sql_execution import runner as runner_module


RESOURCES = Path(__file__).resolve().parents[1] / "resources"
APP_CONFIG = RESOURCES / "config/app_config.yaml"
SQL_DIRECTORY = RESOURCES / "sql"
BUILDER = DAGBuilder(ConfigLoader(RESOURCES / "config/convergence_rules.yaml",
                                sql_dir=SQL_DIRECTORY))
DAG = BUILDER.build()
DATABASE = yaml.safe_load(APP_CONFIG.read_text(encoding="utf-8"))["clickhouse"]["database"]


@pytest.mark.parametrize("taskname", DAG.topological_order)
def test_every_packaged_task_cleans_its_actual_insert_target(taskname, monkeypatch):
    config = {"clickhouse": {"host": "localhost", "port": 8123, "database": DATABASE,
                             "username": "test", "password": "test-password"}}
    runner = runner_module.SQLTaskRunner(APP_CONFIG, SQL_DIRECTORY, config=config)
    instance = BUILDER.create_instance(DAG, taskname, datetime(2026, 10, 1, tzinfo=timezone.utc))
    submissions = []
    monkeypatch.setattr(runner_module, "execute_sql", lambda **values: submissions.append(values))

    runner(instance)

    assert len(submissions) == 2
    cleanup, aggregation = submissions
    sql_without_comments = re.sub(r"--[^\n]*", "", aggregation["sql"])
    insert = re.search(r"\bINSERT\s+INTO\s+([\w.]+)", sql_without_comments, re.IGNORECASE)
    assert insert is not None, taskname
    target = re.search(r"\bALTER\s+TABLE\s+([\w.]+)\s+DELETE\b", cleanup["sql"])
    assert target is not None, taskname
    qualified_cleanup = target[1] if "." in target[1] else f"{DATABASE}.{target[1]}"
    assert qualified_cleanup == f"{insert[1]}_local", taskname
    remainder = sql_without_comments[insert.end():].lstrip()
    if remainder.startswith("("):
        columns = remainder[1:].split(")", 1)[0].split(",")
        assert instance.task.cleanup_column in {column.strip() for column in columns}
    else:
        assert re.search(r"\bAS\s+" + re.escape(instance.task.cleanup_column) + r"\b",
                         remainder, re.IGNORECASE)
    for submission in submissions:
        assert submission["start_time"] == instance.data_start
        assert submission["end_time"] == instance.data_end
