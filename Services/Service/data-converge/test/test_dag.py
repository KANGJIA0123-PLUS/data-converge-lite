from datetime import datetime, timedelta
import subprocess
import sys
import unittest

import pytest

from data_converge.dag import DAGBuilder, DAGExecutor, TaskStatus


class FakeConfigLoader:
    def validate(self):
        return []

    def get_all_rules(self):
        return [
            {
                "taskname": "five_min",
                "description": "5 minute DWS",
                "source_tables": ["realtime_dwd"],
                "target_table": "five_min_dws",
                "aggregation_interval": "PT5M",
                "cron": "1,6,11,16,21,26,31,36,41,46,51,56 * * * *",
                "depends_on": [],
                "sql_id": "five_min",
            },
            {
                "taskname": "hour",
                "description": "hour DWS",
                "source_tables": ["five_min_dws"],
                "target_table": "hour_dws",
                "aggregation_interval": "PT1H",
                "cron": "6 * * * *",
                "depends_on": ["five_min"],
                "sql_id": "hour",
            },
        ]


class DAGTestCase(unittest.TestCase):
    def test_hour_instance_keeps_lineage_without_waiting_for_five_minute_windows(self):
        builder = DAGBuilder(FakeConfigLoader())
        dag = builder.build()
        five_min = builder.create_instance(dag, "five_min", datetime(2026, 9, 8, 10, 1))
        hour = builder.create_instance(dag, "hour", datetime(2026, 9, 8, 10, 6))

        self.assertEqual(five_min.data_start, datetime(2026, 9, 8, 9, 55))
        self.assertEqual(five_min.data_end, datetime(2026, 9, 8, 10, 0))
        self.assertEqual(hour.data_start, datetime(2026, 9, 8, 9, 0))
        self.assertEqual(hour.data_end, datetime(2026, 9, 8, 10, 0))
        self.assertEqual(hour.upstream_instance_ids, ())
        self.assertEqual(dag.dependencies["hour"], ("five_min",))

    def test_executor_runs_in_topological_order(self):
        builder = DAGBuilder(FakeConfigLoader())
        dag = builder.build()
        instances = [builder.create_instance(dag, "five_min", datetime(2026, 9, 8, 9, 6) + timedelta(minutes=5 * n)) for n in range(12)]
        instances.append(builder.create_instance(dag, "hour", datetime(2026, 9, 8, 10, 6)))
        calls = []

        report = DAGExecutor(dag, lambda instance: calls.append(instance.taskname)).execute(instances)

        self.assertEqual(calls, ["five_min"] * 12 + ["hour"])
        self.assertEqual(len(report.succeeded), 13)

    def test_executor_blocks_same_period_downstream_after_failure(self):
        loader = FakeConfigLoader()
        rules = loader.get_all_rules()
        rules[1]["aggregation_interval"] = "PT5M"
        loader.get_all_rules = lambda: rules
        builder = DAGBuilder(loader)
        dag = builder.build()
        instances = builder.create_batch_instances(dag, datetime(2026, 9, 8, 10, 0))

        def fail_five_min(instance):
            if instance.taskname == "five_min":
                raise RuntimeError("boom")

        report = DAGExecutor(dag, fail_five_min).execute(instances)

        self.assertEqual(report.instances[instances[0].instance_id].status, TaskStatus.UNKNOWN)
        self.assertEqual(report.instances[instances[1].instance_id].status, TaskStatus.BLOCKED)

    def test_cycle_is_rejected(self):
        loader = FakeConfigLoader()
        rules = loader.get_all_rules()
        rules[0]["depends_on"] = ["hour"]
        loader.get_all_rules = lambda: rules

        with self.assertRaisesRegex(ValueError, "cycle"):
            DAGBuilder(loader).build()


if __name__ == "__main__":
    unittest.main()


def test_calendar_month_keeps_leap_february_window_without_daily_gate():
    loader = FakeConfigLoader()
    rules = loader.get_all_rules()
    rules[0]["aggregation_interval"] = "P1D"
    rules[1]["aggregation_interval"] = "P1M"
    loader.get_all_rules = lambda: rules
    builder = DAGBuilder(loader)
    instance = builder.create_instance(builder.build(), "hour", datetime(2024, 3, 1, 0, 20))
    assert instance.data_start == datetime(2024, 2, 1)
    assert instance.data_end == datetime(2024, 3, 1)
    assert instance.upstream_instance_ids == ()


@pytest.mark.parametrize("parent_interval, child_interval", [
    ("PT5M", "PT1H"), ("PT1H", "P1D"), ("P1D", "P1M"),
])
def test_cross_period_child_executes_without_parent_history(parent_interval, child_interval):
    loader = FakeConfigLoader()
    rules = loader.get_all_rules()
    rules[0]["aggregation_interval"] = parent_interval
    rules[1]["aggregation_interval"] = child_interval
    loader.get_all_rules = lambda: rules
    builder = DAGBuilder(loader)
    dag = builder.build()
    instances = builder.create_batch_instances(dag, datetime(2026, 11, 1), ["hour"])
    calls = []
    report = DAGExecutor(dag, lambda instance: calls.append(instance.instance_id)).execute(instances)
    assert calls == ["hour@2026-11-01T00:00:00"]
    assert [instance.instance_id for instance in report.succeeded] == calls
    assert dag.dependencies["hour"] == ("five_min",)


def test_mixed_parents_wait_only_for_same_period_success():
    loader = FakeConfigLoader()
    rules = loader.get_all_rules()
    rules.append(dict(rules[1], taskname="hour_ads", depends_on=["five_min", "hour"]))
    loader.get_all_rules = lambda: rules
    builder = DAGBuilder(loader)
    dag = builder.build()
    now = datetime(2026, 11, 1, 0, 6)
    calls = []
    executor = DAGExecutor(dag, lambda instance: calls.append(instance.taskname))
    ads = builder.create_instance(dag, "hour_ads", now)
    assert ads.upstream_instance_ids == ("hour@2026-11-01T00:00:00",)
    assert executor.execute([ads]).blocked == [ads]
    executor.execute([builder.create_instance(dag, "hour", now)])
    assert executor.execute([ads]).succeeded == [ads]
    assert calls == ["hour", "hour_ads"]


def test_cross_period_dependency_still_rejects_incompatible_windows():
    loader = FakeConfigLoader()
    rules = loader.get_all_rules()
    rules[0]["aggregation_interval"] = "PT1H"
    rules[1]["aggregation_interval"] = "PT5M"
    loader.get_all_rules = lambda: rules
    builder = DAGBuilder(loader)
    with pytest.raises(ValueError, match="Incompatible aggregation windows"):
        builder.create_instance(builder.build(), "hour", datetime(2026, 11, 1, 0, 6))


def test_successful_instance_is_not_executed_twice_in_memory():
    builder = DAGBuilder(FakeConfigLoader())
    dag = builder.build()
    calls = []
    executor = DAGExecutor(dag, lambda instance: calls.append(instance.instance_id))
    for _ in range(2):
        executor.execute([builder.create_instance(dag, "five_min", datetime(2026, 9, 8, 10, 1))])
    assert calls == ["five_min@2026-09-08T10:00:00"]


@pytest.mark.parametrize("retryable, unknown, status", [
    (False, False, TaskStatus.FAILED),
    (True, False, TaskStatus.FAILED),
    (False, True, TaskStatus.UNKNOWN),
    (True, True, TaskStatus.UNKNOWN),
])
def test_runner_error_contract_preserves_sql_compatibility(retryable, unknown, status):
    from data_converge.errors import TaskExecutionError
    from data_converge.sql_execution import SQLExecutionError
    from data_converge.sql_execution.client import SQLExecutionError as ClientError

    assert SQLExecutionError is ClientError
    builder = DAGBuilder(FakeConfigLoader())
    dag = builder.build()
    for error_type in (TaskExecutionError, SQLExecutionError):
        error = error_type("SAFE_FAILURE", retryable=retryable,
                           outcome_unknown=unknown, query_id="query-1")

        def fail(instance):
            raise error

        instance = builder.create_instance(dag, "five_min", datetime(2026, 9, 8, 10, 1))
        executor = DAGExecutor(dag, fail)
        result = executor.execute([instance])
        assert result.instances[instance.instance_id].status == status
        assert instance.error == "SAFE_FAILURE"
        assert executor._memory[instance.instance_id]["retryable"] is (retryable and not unknown)
        assert error.query_id == "query-1"


def test_unknown_runner_failure_does_not_load_sql_transport():
    result = subprocess.run([sys.executable, "-c", """
import sys
from data_converge.dag import DAGExecutor, TaskStatus
assert DAGExecutor._classify_error(RuntimeError('private details')) == (
    TaskStatus.UNKNOWN, False, 'UNEXPECTED_RUNNER_ERROR:RuntimeError')
assert 'data_converge.sql_execution.client' not in sys.modules
"""], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
