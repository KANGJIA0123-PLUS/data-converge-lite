"""Cleanup follows the actual INSERT target, before any destructive request."""

import logging

import pytest

from data_converge.errors import TaskExecutionError
from data_converge.sql_execution import runner as runner_module

from .test_sql_runner_debug import make_instance, make_runner


@pytest.mark.parametrize("sql, local_table", [
    ("INSERT INTO actual SELECT 1", "actual_local"),
    ("INSERT INTO other_db.actual SELECT 1", "other_db.actual_local"),
    ("insert\ninto Other_DB.Actual SELECT 1", "Other_DB.Actual_local"),
    ("INSERT INTO TABLE other_db.actual SELECT 1", "other_db.actual_local"),
    ("-- INSERT INTO decoy SELECT 1\nINSERT INTO actual SELECT 1", "actual_local"),
    ("/* INSERT INTO decoy SELECT 1 */ INSERT INTO actual SELECT 1", "actual_local"),
    ("/* outer /* nested */ INSERT INTO decoy */ INSERT INTO actual SELECT 1", "actual_local"),
    ("// INSERT INTO decoy\nINSERT INTO actual SELECT 1", "actual_local"),
    ("#! INSERT INTO decoy\n# INSERT INTO decoy\nINSERT INTO actual SELECT 1", "actual_local"),
    ("INSERT/* comment */INTO other_db /* db */ . /* table */ actual SELECT 1",
     "other_db.actual_local"),
    ("INSERT INTO actual(window_time) SELECT 1", "actual_local"),
    ("INSERT INTO actual WITH 1 AS n SELECT n", "actual_local"),
    ("INSERT INTO actual VALUES (1)", "actual_local"),
    ("INSERT INTO actual SETTINGS async_insert=0 SELECT 1", "actual_local"),
    ("INSERT INTO `other-db`.`actual-table` SELECT 1", "`other-db`.`actual-table_local`"),
    ('INSERT INTO "other.db"."actual.table" SELECT 1', '"other.db"."actual.table_local"'),
    (r"INSERT INTO `db`.`a\`b` SELECT 1", r"`db`.`a\`b_local`"),
    ('INSERT INTO "db"."a""b" SELECT 1', '"db"."a""b_local"'),
])
def test_cleanup_uses_insert_table_and_database(tmp_path, monkeypatch, caplog, sql, local_table):
    runner = make_runner(tmp_path, debug_enabled=True)
    (tmp_path / "hour.sql").write_text(sql, encoding="utf-8")
    submissions = []
    monkeypatch.setattr(runner_module, "execute_sql", lambda **values: submissions.append(values))
    caplog.set_level(logging.DEBUG, logger="data_converge")
    instance = make_instance()  # Config deliberately names a different table: target.

    runner(instance)

    assert len(submissions) == 2
    assert submissions[0]["sql"] == (
        f"ALTER TABLE {local_table} DELETE WHERE window_time >= {{start_time:DateTime}} "
        "AND window_time < {end_time:DateTime} SETTINGS mutations_sync=2")
    assert submissions[1]["sql"] == sql
    assert f"Idempotent pre-cleanup: table={local_table}," in caplog.text
    assert sql not in caplog.text
    for submission in submissions:
        assert submission["start_time"] == instance.data_start
        assert submission["end_time"] == instance.data_end
        assert submission["query_id"] == instance.query_id


@pytest.mark.parametrize("sql", [
    "SELECT 'INSERT INTO decoy SELECT 1'",
    "-- INSERT INTO decoy SELECT 1\nSELECT 1",
    "/* INSERT INTO decoy */ SELECT 1",
    "/* unclosed INSERT INTO decoy SELECT 1",
    "/* outer /* nested */ INSERT INTO decoy SELECT 1",
    "INSERT INTO",
    "INSERT INTO db.",
    "INSERT INTO db.table.extra SELECT 1",
    "INSERT INTO table-name SELECT 1",
    "INSERT INTO 123table SELECT 1",
    "INSERT INTO `unfinished SELECT 1",
    "INSERT INTO 'string_target' SELECT 1",
    "INSERT INTO FUNCTION remote('host', 'db', 'table') SELECT 1",
    "INSERT INTO TABLE FUNCTION remote('host', 'db', 'table') SELECT 1",
    "INSERT INTO {table:Identifier} SELECT 1",
    "INSERT INTO target /* unclosed",
])
def test_unrecognized_insert_fails_before_cleanup(tmp_path, monkeypatch, caplog, sql):
    runner = make_runner(tmp_path, debug_enabled=True)
    (tmp_path / "hour.sql").write_text(sql, encoding="utf-8")
    monkeypatch.setattr(runner_module, "execute_sql", lambda **_: pytest.fail("SQL submitted"))
    caplog.set_level(logging.DEBUG, logger="data_converge")

    with pytest.raises(TaskExecutionError, match="^SQL_PREFLIGHT_FAILED$") as caught:
        runner(make_instance())

    assert not caught.value.retryable and not caught.value.outcome_unknown
    assert caught.value.query_id == "runner-attempt"
    assert "phase=cleanup" not in caplog.text and "phase=aggregate" not in caplog.text


def test_changed_sql_keeps_configuration_error_before_target_parsing(tmp_path, monkeypatch):
    runner = make_runner(tmp_path)
    runner.expected_sql_hashes = {"hourly_task": "approved-digest"}
    (tmp_path / "hour.sql").write_text("SELECT 1", encoding="utf-8")
    monkeypatch.setattr(runner_module, "execute_sql", lambda **_: pytest.fail("SQL submitted"))

    with pytest.raises(TaskExecutionError, match="^SQL_CONFIGURATION_CHANGED$") as caught:
        runner(make_instance())

    assert not caught.value.retryable and not caught.value.outcome_unknown
    assert caught.value.query_id == "runner-attempt"
