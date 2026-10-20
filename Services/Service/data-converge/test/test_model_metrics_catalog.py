"""New ADS outputs must be scheduled after every SQL input is produced."""

from datetime import datetime

import pytest

from data_converge.config import ConfigLoader
from data_converge.dag import DAGBuilder

from .paths import RESOURCES_ROOT


@pytest.fixture
def shipped_graph():
    loader = ConfigLoader(RESOURCES_ROOT / "config/convergence_rules.yaml",
                          RESOURCES_ROOT / "sql")
    loader.load()
    builder = DAGBuilder(loader)
    return builder, builder.build()


@pytest.mark.parametrize("period,gpu_parent", [
    ("5min", "task_gpu_realtime_to_5min_dws"),
    ("hour", "task_gpu_5min_to_hour_dws"),
    ("day", "task_gpu_hour_to_day_dws"),
    ("month", "task_gpu_day_to_month_dws"),
])
def test_model_service_output_waits_for_instances_and_cards(shipped_graph, period, gpu_parent):
    builder, dag = shipped_graph
    name = f"task_model_server_{period}_ads"
    instance = builder.create_instance(dag, name, datetime(2026, 11, 1))
    assert set(instance.upstream_instance_ids) == {
        f"task_model_instance_{period}_dws@2026-11-01T00:00:00",
        f"{gpu_parent}@2026-11-01T00:00:00",
    }
    assert instance.task.target_table == f"t_mops__modelinstance_server__{period}_ads"
    assert instance.task.sql_id == name


@pytest.mark.parametrize("period", ["5min", "hour", "day", "month"])
def test_metric_output_waits_for_card_allocation(shipped_graph, period):
    builder, dag = shipped_graph
    name = f"task_model_metric_instance_{period}_ads"
    instance = builder.create_instance(dag, name, datetime(2026, 11, 1))
    assert set(instance.upstream_instance_ids) == {
        f"task_model_metric_instance_{period}_dws@2026-11-01T00:00:00",
        f"task_model_instance_{period}_ads@2026-11-01T00:00:00",
    }
    assert f"t_mops_modelinstance_{period}_ads" in instance.task.source_tables
