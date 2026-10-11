from datetime import datetime, timedelta
import unittest

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
    def test_hour_instance_depends_on_all_twelve_five_minute_windows(self):
        builder = DAGBuilder(FakeConfigLoader())
        dag = builder.build()
        five_min = builder.create_instance(dag, "five_min", datetime(2026, 9, 8, 10, 1))
        hour = builder.create_instance(dag, "hour", datetime(2026, 9, 8, 10, 6))

        self.assertEqual(five_min.data_start, datetime(2026, 9, 8, 9, 55))
        self.assertEqual(five_min.data_end, datetime(2026, 9, 8, 10, 0))
        self.assertEqual(hour.data_start, datetime(2026, 9, 8, 9, 0))
        self.assertEqual(hour.data_end, datetime(2026, 9, 8, 10, 0))
        self.assertEqual(len(hour.upstream_instance_ids), 12)
        self.assertEqual(hour.upstream_instance_ids[0], "five_min@2026-09-08T09:05:00")
        self.assertEqual(hour.upstream_instance_ids[-1], five_min.instance_id)

    def test_executor_runs_in_topological_order(self):
        builder = DAGBuilder(FakeConfigLoader())
        dag = builder.build()
        instances = [builder.create_instance(dag, "five_min", datetime(2026, 9, 8, 9, 6) + timedelta(minutes=5 * n)) for n in range(12)]
        instances.append(builder.create_instance(dag, "hour", datetime(2026, 9, 8, 10, 6)))
        calls = []

        report = DAGExecutor(dag, lambda instance: calls.append(instance.taskname)).execute(instances)

        self.assertEqual(calls, ["five_min"] * 12 + ["hour"])
        self.assertEqual(len(report.succeeded), 13)

    def test_executor_blocks_downstream_after_failure(self):
        builder = DAGBuilder(FakeConfigLoader())
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


def test_calendar_month_requires_every_day_in_leap_february():
    loader = FakeConfigLoader()
    rules = loader.get_all_rules()
    rules[0]["aggregation_interval"] = "P1D"
    rules[1]["aggregation_interval"] = "P1M"
    loader.get_all_rules = lambda: rules
    builder = DAGBuilder(loader)
    instance = builder.create_instance(builder.build(), "hour", datetime(2024, 3, 1, 0, 20))
    assert len(instance.upstream_instance_ids) == 29
    assert instance.upstream_instance_ids[0] == "five_min@2024-02-02T00:00:00"
    assert instance.upstream_instance_ids[-1] == "five_min@2024-03-01T00:00:00"


def test_successful_instance_is_not_executed_twice_in_memory():
    builder = DAGBuilder(FakeConfigLoader())
    dag = builder.build()
    calls = []
    executor = DAGExecutor(dag, lambda instance: calls.append(instance.instance_id))
    for _ in range(2):
        executor.execute([builder.create_instance(dag, "five_min", datetime(2026, 9, 8, 10, 1))])
    assert calls == ["five_min@2026-09-08T10:00:00"]
