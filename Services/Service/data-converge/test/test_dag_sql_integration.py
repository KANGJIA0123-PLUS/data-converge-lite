from datetime import datetime
import threading
import time

from data_converge.dag import DAGBuilder, DAGExecutor, TaskStatus
from data_converge.sql_execution import runner as runner_module


class FakeConfigLoader:
    """提供测试用 DAG 配置。"""

    def __init__(self, rules):
        self.rules = rules

    def validate(self):
        return []

    def get_all_rules(self):
        return self.rules


def make_rules():
    return [
        {
            "taskname": "task_parent",
            "description": "上游任务",
            "source_tables": ["source_parent"],
            "target_table": "target_parent",
            "aggregation_interval": "PT1H",
            "cron": "3 * * * *",
            "depends_on": [],
            "sql_id": "task_parent",
        },
        {
            "taskname": "task_child",
            "description": "下游任务",
            "source_tables": ["target_parent"],
            "target_table": "target_child",
            "aggregation_interval": "PT1H",
            "cron": "5 * * * *",
            "depends_on": ["task_parent"],
            "sql_id": "task_child",
        },
    ]


def prepare_sql_files(tmp_path):
    sql_directory = tmp_path / "sql"
    sql_directory.mkdir()

    (sql_directory / "task_parent.sql").write_text(
        """
        INSERT INTO target_parent
        SELECT *
        FROM source_parent
        WHERE window_time >= {start_time:DateTime}
          AND window_time < {end_time:DateTime}
        """,
        encoding="utf-8",
    )

    (sql_directory / "task_child.sql").write_text(
        """
        INSERT INTO target_child
        SELECT *
        FROM target_parent
        WHERE window_time >= {start_time:DateTime}
          AND window_time < {end_time:DateTime}
        """,
        encoding="utf-8",
    )

    return sql_directory


def test_dag_executor_runs_sql_tasks_in_dependency_order(
    tmp_path,
    monkeypatch,
):
    """SQL 成功时，上下游任务应按照依赖顺序执行成功。"""
    builder = DAGBuilder(FakeConfigLoader(make_rules()))
    dag = builder.build()

    instances = builder.create_batch_instances(
        dag=dag,
        data_end=datetime(2026, 9, 9, 2, 0, 0),
    )

    sql_directory = prepare_sql_files(tmp_path)
    executed_sql = []

    monkeypatch.setattr(
        runner_module,
        "load_config",
        lambda config_path: {"clickhouse": {}},
    )

    def fake_execute_sql(**kwargs):
        executed_sql.append(kwargs["sql"])
        return []

    monkeypatch.setattr(
        runner_module,
        "execute_sql",
        fake_execute_sql,
    )

    sql_runner = runner_module.SQLTaskRunner(
        config_path=tmp_path / "app_config.yaml",
        sql_directory=sql_directory,
    )

    executor = DAGExecutor(
        dag=dag,
        runner=sql_runner,
    )

    report = executor.execute(instances)

    assert len(report.succeeded) == 2
    assert len(report.failed) == 0
    assert len(report.blocked) == 0

    assert report.succeeded[0].status == TaskStatus.SUCCESS
    assert report.succeeded[1].status == TaskStatus.SUCCESS

    assert len(executed_sql) == 2
    assert "source_parent" in executed_sql[0]
    assert "target_parent" in executed_sql[1]


def test_unknown_sql_outcome_blocks_downstream_task(
    tmp_path,
    monkeypatch,
):
    """上游异常没有确定未写入证据时，应UNKNOWN并阻塞下游。"""
    builder = DAGBuilder(FakeConfigLoader(make_rules()))
    dag = builder.build()

    instances = builder.create_batch_instances(
        dag=dag,
        data_end=datetime(2026, 9, 9, 2, 0, 0),
    )

    sql_directory = prepare_sql_files(tmp_path)
    execution_count = 0

    monkeypatch.setattr(
        runner_module,
        "load_config",
        lambda config_path: {"clickhouse": {}},
    )

    def fake_execute_sql(**kwargs):
        nonlocal execution_count
        execution_count += 1

        if "source_parent" in kwargs["sql"]:
            raise RuntimeError("模拟 ClickHouse 执行失败")

        return []

    monkeypatch.setattr(
        runner_module,
        "execute_sql",
        fake_execute_sql,
    )

    sql_runner = runner_module.SQLTaskRunner(
        config_path=tmp_path / "app_config.yaml",
        sql_directory=sql_directory,
    )

    executor = DAGExecutor(
        dag=dag,
        runner=sql_runner,
    )

    report = executor.execute(instances)

    assert len(report.succeeded) == 0
    assert len(report.unknown) == 1
    assert len(report.blocked) == 1

    assert report.unknown[0].taskname == "task_parent"
    assert report.unknown[0].status == TaskStatus.UNKNOWN
    assert report.unknown[0].error == "UNEXPECTED_RUNNER_ERROR:RuntimeError"

    assert report.blocked[0].taskname == "task_child"
    assert report.blocked[0].status == TaskStatus.BLOCKED

    # 下游被阻塞，因此 SQLTaskRunner 实际只执行一次
    assert execution_count == 1


def test_independent_tasks_run_concurrently_before_downstream():
    """两个独立上游应并发执行，下游应等待二者成功。"""
    rules = [
        {
            "taskname": "task_parent_a",
            "description": "上游A",
            "source_tables": ["source_a"],
            "target_table": "target_a",
            "aggregation_interval": "PT5M",
            "cron": "1,6,11,16,21,26,31,36,41,46,51,56 * * * *",
            "depends_on": [],
            "sql_id": "task_parent_a",
        },
        {
            "taskname": "task_parent_b",
            "description": "上游B",
            "source_tables": ["source_b"],
            "target_table": "target_b",
            "aggregation_interval": "PT5M",
            "cron": "1,6,11,16,21,26,31,36,41,46,51,56 * * * *",
            "depends_on": [],
            "sql_id": "task_parent_b",
        },
        {
            "taskname": "task_child",
            "description": "下游",
            "source_tables": ["target_a", "target_b"],
            "target_table": "target_child",
            "aggregation_interval": "PT5M",
            "cron": "1,6,11,16,21,26,31,36,41,46,51,56 * * * *",
            "depends_on": [
                "task_parent_a",
                "task_parent_b",
            ],
            "sql_id": "task_child",
        },
    ]

    builder = DAGBuilder(FakeConfigLoader(rules))
    dag = builder.build()

    instances = builder.create_batch_instances(
        dag=dag,
        data_end=datetime(2026, 9, 9, 10, 0, 0),
    )

    # 如果两个上游不是并发运行，Barrier 会超时。
    parent_barrier = threading.Barrier(2)
    completed_parents = set()
    execution_order = []
    lock = threading.Lock()

    def runner(instance):
        if instance.taskname in {
            "task_parent_a",
            "task_parent_b",
        }:
            parent_barrier.wait(timeout=2)

            with lock:
                completed_parents.add(instance.taskname)
                execution_order.append(instance.taskname)

            time.sleep(0.01)
            return

        # 下游执行时，两个上游必须都已结束。
        assert completed_parents == {
            "task_parent_a",
            "task_parent_b",
        }

        with lock:
            execution_order.append(instance.taskname)

    executor = DAGExecutor(
        dag=dag,
        runner=runner,
        max_concurrent_tasks=2,
    )

    report = executor.execute(instances)

    assert len(report.succeeded) == 3
    assert len(report.failed) == 0
    assert len(report.blocked) == 0

    assert set(execution_order[:2]) == {
        "task_parent_a",
        "task_parent_b",
    }
    assert execution_order[2] == "task_child"
