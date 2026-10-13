"""配置管理模块测试"""

import pytest
import yaml
from pathlib import Path
from data_converge.config import ConfigLoader, CronValidator


@pytest.mark.parametrize('policy', [
    {'max_attempts': True},
    {'poll_interval_seconds': 0},
    {'default_timeout_seconds': 301},
])
def test_application_rejects_invalid_scheduling_policy(policy):
    from data_converge.config.application import StartupError, settings

    with pytest.raises(StartupError):
        settings({'scheduler': policy})


class TestCronValidator:
    """Cron表达式验证器测试"""

    def test_validate_valid_simple(self):
        """验证简单有效的cron表达式"""
        assert CronValidator.validate("*/5 * * * *")[0] is True
        assert CronValidator.validate("5 * * * *")[0] is True
        assert CronValidator.validate("5 0 * * *")[0] is True

    def test_validate_valid_with_range(self):
        """验证带范围的cron表达式"""
        assert CronValidator.validate("0 9-17 * * *")[0] is True
        assert CronValidator.validate("*/15 0,12 * * *")[0] is True

    def test_validate_invalid_field_count(self):
        """验证字段数量不对"""
        valid, err = CronValidator.validate("*/5 * * *")
        assert valid is False
        assert "5 fields" in err

    def test_validate_invalid_range(self):
        """验证无效的范围"""
        valid, err = CronValidator.validate("60 * * * *")
        assert valid is False
        assert "minute" in err

    def test_validate_invalid_char(self):
        """验证包含无效字符"""
        valid, err = CronValidator.validate("abc * * * *")
        assert valid is False

    def test_validate_special_exprs(self):
        """验证特殊表达式"""
        assert CronValidator.validate("@hourly")[0] is True
        assert CronValidator.validate("@daily")[0] is True


class TestConfigLoader:
    """配置加载器测试类"""

    def test_load_config_from_yaml(self, tmp_path):
        """从YAML文件加载配置"""
        config_data = {
            "convergence_rules": [
                {
                    "taskname": "task_gpu_realtime_to_5min_dws",
                    "source_tables": ["t_mops_gpu_realtime_dwd"],
                    "target_table": "t_mops_gpu_5min_dws",
                    "aggregation_interval": "PT5M",
                    "cron": "*/5 * * * *",
                    "sql_id": "task_gpu_realtime_to_5min_dws"
                }
            ]
        }
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(yaml.dump(config_data))

        loader = ConfigLoader(config_file)
        loaded_config = loader.load()

        assert "convergence_rules" in loaded_config
        assert len(loaded_config["convergence_rules"]) == 1

    def test_get_rule_by_taskname(self, tmp_path):
        """根据任务名获取汇聚规则"""
        config_data = {
            "convergence_rules": [
                {
                    "taskname": "task_gpu_realtime_to_5min_dws",
                    "source_tables": ["t_mops_gpu_realtime_dwd"],
                    "target_table": "t_mops_gpu_5min_dws",
                    "aggregation_interval": "PT5M",
                    "cron": "*/5 * * * *",
                    "sql_id": "task_gpu_realtime_to_5min_dws"
                }
            ]
        }
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(yaml.dump(config_data))

        loader = ConfigLoader(config_file)
        rule = loader.get_rule_by_taskname("task_gpu_realtime_to_5min_dws")

        assert rule is not None
        assert "t_mops_gpu_realtime_dwd" in rule["source_tables"]
        assert rule["aggregation_interval"] == "PT5M"
        assert rule["cron"] == "*/5 * * * *"

    def test_get_nonexistent_rule(self, tmp_path):
        """获取不存在的规则返回None"""
        config_data = {"convergence_rules": []}
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(yaml.dump(config_data))

        loader = ConfigLoader(config_file)
        rule = loader.get_rule_by_taskname("nonexistent")

        assert rule is None

    def test_build_task_dependencies(self, tmp_path):
        """构建任务DAG依赖关系（基于taskname）"""
        config_data = {
            "convergence_rules": [
                {
                    "taskname": "task_gpu_realtime_to_5min_dws",
                    "source_tables": ["t_mops_gpu_realtime_dwd"],
                    "target_table": "t_mops_gpu_5min_dws",
                    "depends_on": []
                },
                {
                    "taskname": "task_gpu_5min_to_hour_dws",
                    "source_tables": ["t_mops_gpu_5min_dws"],
                    "target_table": "t_mops_gpu_hour_dws",
                    "depends_on": ["task_gpu_realtime_to_5min_dws"]
                }
            ]
        }
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(yaml.dump(config_data))

        loader = ConfigLoader(config_file)
        deps = loader.build_task_dependencies()

        # 验证depends_on是基于taskname的
        assert deps["task_gpu_realtime_to_5min_dws"] == []
        assert deps["task_gpu_5min_to_hour_dws"] == ["task_gpu_realtime_to_5min_dws"]

    def test_get_upstream_tasks(self, tmp_path):
        """获取任务的上游任务"""
        config_data = {
            "convergence_rules": [
                {
                    "taskname": "task_a",
                    "depends_on": []
                },
                {
                    "taskname": "task_b",
                    "depends_on": ["task_a"]
                },
                {
                    "taskname": "task_c",
                    "depends_on": ["task_a"]
                }
            ]
        }
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(yaml.dump(config_data))

        loader = ConfigLoader(config_file)
        upstream = loader.get_upstream_tasks("task_b")

        assert "task_a" in upstream

    def test_get_downstream_tasks(self, tmp_path):
        """获取任务的下游任务"""
        config_data = {
            "convergence_rules": [
                {
                    "taskname": "task_a",
                    "depends_on": []
                },
                {
                    "taskname": "task_b",
                    "depends_on": ["task_a"]
                },
                {
                    "taskname": "task_c",
                    "depends_on": ["task_a"]
                }
            ]
        }
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(yaml.dump(config_data))

        loader = ConfigLoader(config_file)
        downstream = loader.get_downstream_tasks("task_a")

        assert "task_b" in downstream
        assert "task_c" in downstream

    def test_load_sql_by_sql_id(self, tmp_path):
        """根据sql_id加载SQL文件"""
        sql_dir = tmp_path / "sql"
        sql_dir.mkdir()

        sql_file = sql_dir / "task_test.sql"
        sql_file.write_text("SELECT * FROM table WHERE dt >= '{start_time}' AND dt < '{end_time}'")

        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(yaml.dump({"convergence_rules": []}))

        loader = ConfigLoader(config_file, sql_dir=sql_dir)
        sql = loader.load_sql("task_test")

        assert "SELECT * FROM table" in sql
        assert "{start_time}" in sql

    def test_get_sql_with_params(self, tmp_path):
        """获取填充参数后的SQL"""
        sql_dir = tmp_path / "sql"
        sql_dir.mkdir()

        sql_file = sql_dir / "task_test.sql"
        sql_file.write_text("SELECT * FROM table WHERE dt >= '{start_time}' AND dt < '{end_time}'")

        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(yaml.dump({"convergence_rules": []}))

        loader = ConfigLoader(config_file, sql_dir=sql_dir)
        sql = loader.get_sql_with_params("task_test", "2026-08-01", "2026-08-02")

        assert "2026-08-01" in sql
        assert "2026-08-02" in sql
        assert "{start_time}" not in sql

    def test_load_sql_file_not_found(self, tmp_path):
        """SQL文件不存在时抛出异常"""
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(yaml.dump({"convergence_rules": []}))

        loader = ConfigLoader(config_file)
        with pytest.raises(FileNotFoundError):
            loader.load_sql("nonexistent_sql")

    def test_get_all_tasknames(self, tmp_path):
        """获取所有任务名"""
        config_data = {
            "convergence_rules": [
                {"taskname": "task_gpu_realtime_to_5min_dws"},
                {"taskname": "task_gpu_5min_to_hour_dws"},
            ]
        }
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(yaml.dump(config_data))

        loader = ConfigLoader(config_file)
        tasknames = loader.get_all_tasknames()

        assert "task_gpu_realtime_to_5min_dws" in tasknames
        assert "task_gpu_5min_to_hour_dws" in tasknames

    def test_get_task_cron(self, tmp_path):
        """获取任务的cron表达式"""
        config_data = {
            "convergence_rules": [
                {
                    "taskname": "task_gpu_realtime_to_5min_dws",
                    "cron": "*/5 * * * *"
                }
            ]
        }
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(yaml.dump(config_data))

        loader = ConfigLoader(config_file)
        cron = loader.get_task_cron("task_gpu_realtime_to_5min_dws")

        assert cron == "*/5 * * * *"

    def test_get_task_interval(self, tmp_path):
        """获取任务的聚合时间间隔"""
        config_data = {
            "convergence_rules": [
                {
                    "taskname": "task_gpu_realtime_to_5min_dws",
                    "aggregation_interval": "PT5M"
                }
            ]
        }
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(yaml.dump(config_data))

        loader = ConfigLoader(config_file)
        interval = loader.get_task_interval("task_gpu_realtime_to_5min_dws")

        assert interval == "PT5M"

    def test_validate_missing_taskname(self, tmp_path):
        """验证缺少taskname的配置"""
        config_data = {
            "convergence_rules": [
                {
                    "source_tables": ["t_mops_gpu_realtime_dwd"],
                    "target_table": "t_mops_gpu_5min_dws",
                    "aggregation_interval": "PT5M",
                    "cron": "*/5 * * * *",
                    "sql_id": "task_test"
                }
            ]
        }
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(yaml.dump(config_data))

        loader = ConfigLoader(config_file)
        errors = loader.validate()

        assert any("taskname" in err for err in errors)

    def test_validate_missing_source_tables(self, tmp_path):
        """验证缺少source_tables的配置"""
        config_data = {
            "convergence_rules": [
                {
                    "taskname": "task_test",
                    "target_table": "t_mops_gpu_5min_dws",
                    "aggregation_interval": "PT5M",
                    "cron": "*/5 * * * *",
                    "sql_id": "task_test"
                }
            ]
        }
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(yaml.dump(config_data))

        loader = ConfigLoader(config_file)
        errors = loader.validate()

        assert any("source_tables" in err for err in errors)

    def test_validate_empty_source_tables(self, tmp_path):
        """验证source_tables为空列表"""
        config_data = {
            "convergence_rules": [
                {
                    "taskname": "task_test",
                    "source_tables": [],
                    "target_table": "t_mops_gpu_5min_dws",
                    "aggregation_interval": "PT5M",
                    "cron": "*/5 * * * *",
                    "sql_id": "task_test"
                }
            ]
        }
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(yaml.dump(config_data))

        loader = ConfigLoader(config_file)
        errors = loader.validate()

        assert any("source_tables" in err for err in errors)

    def test_validate_missing_target_table(self, tmp_path):
        """验证缺少target_table的配置"""
        config_data = {
            "convergence_rules": [
                {
                    "taskname": "task_test",
                    "source_tables": ["t_mops_gpu_realtime_dwd"],
                    "aggregation_interval": "PT5M",
                    "cron": "*/5 * * * *",
                    "sql_id": "task_test"
                }
            ]
        }
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(yaml.dump(config_data))

        loader = ConfigLoader(config_file)
        errors = loader.validate()

        assert any("target_table" in err for err in errors)

    def test_validate_missing_aggregation_interval(self, tmp_path):
        """验证缺少aggregation_interval的配置"""
        config_data = {
            "convergence_rules": [
                {
                    "taskname": "task_test",
                    "source_tables": ["t_mops_gpu_realtime_dwd"],
                    "target_table": "t_mops_gpu_5min_dws",
                    "cron": "*/5 * * * *",
                    "sql_id": "task_test"
                }
            ]
        }
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(yaml.dump(config_data))

        loader = ConfigLoader(config_file)
        errors = loader.validate()

        assert any("aggregation_interval" in err for err in errors)

    def test_validate_missing_cron(self, tmp_path):
        """验证缺少cron的配置"""
        config_data = {
            "convergence_rules": [
                {
                    "taskname": "task_test",
                    "source_tables": ["t_mops_gpu_realtime_dwd"],
                    "target_table": "t_mops_gpu_5min_dws",
                    "aggregation_interval": "PT5M",
                    "sql_id": "task_test"
                }
            ]
        }
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(yaml.dump(config_data))

        loader = ConfigLoader(config_file)
        errors = loader.validate()

        assert any("cron" in err for err in errors)

    def test_validate_invalid_cron(self, tmp_path):
        """验证无效的cron表达式"""
        config_data = {
            "convergence_rules": [
                {
                    "taskname": "task_test",
                    "source_tables": ["t_mops_gpu_realtime_dwd"],
                    "target_table": "t_mops_gpu_5min_dws",
                    "aggregation_interval": "PT5M",
                    "cron": "invalid_cron",
                    "sql_id": "task_test"
                }
            ]
        }
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(yaml.dump(config_data))

        loader = ConfigLoader(config_file)
        errors = loader.validate()

        assert any("cron" in err.lower() for err in errors)

    def test_validate_missing_sql_id(self, tmp_path):
        """验证缺少sql_id的配置"""
        config_data = {
            "convergence_rules": [
                {
                    "taskname": "task_test",
                    "source_tables": ["t_mops_gpu_realtime_dwd"],
                    "target_table": "t_mops_gpu_5min_dws",
                    "aggregation_interval": "PT5M",
                    "cron": "*/5 * * * *"
                }
            ]
        }
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(yaml.dump(config_data))

        loader = ConfigLoader(config_file)
        errors = loader.validate()

        assert any("sql_id" in err for err in errors)

    def test_validate_depends_on_unknown_task(self, tmp_path):
        """验证depends_on引用不存在的taskname"""
        config_data = {
            "convergence_rules": [
                {
                    "taskname": "task_a",
                    "source_tables": ["t_mops_gpu_realtime_dwd"],
                    "target_table": "t_mops_gpu_5min_dws",
                    "aggregation_interval": "PT5M",
                    "cron": "*/5 * * * *",
                    "sql_id": "task_a",
                    "depends_on": ["nonexistent_task"]
                }
            ]
        }
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(yaml.dump(config_data))

        loader = ConfigLoader(config_file)
        errors = loader.validate()

        assert any("nonexistent_task" in err for err in errors)

    def test_validate_sql_file_not_found(self, tmp_path):
        """验证sql_id对应的SQL文件不存在"""
        config_data = {
            "convergence_rules": [
                {
                    "taskname": "task_test",
                    "source_tables": ["t_mops_gpu_realtime_dwd"],
                    "target_table": "t_mops_gpu_5min_dws",
                    "aggregation_interval": "PT5M",
                    "cron": "*/5 * * * *",
                    "sql_id": "nonexistent_sql"
                }
            ]
        }
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(yaml.dump(config_data))

        loader = ConfigLoader(config_file)
        errors = loader.validate()

        assert any("not found" in err for err in errors)

    def test_validate_success_with_all_required_fields(self, tmp_path):
        """验证所有必填字段都存在时验证通过"""
        config_data = {
            "convergence_rules": [
                {
                    "taskname": "task_test",
                    "source_tables": ["t_mops_gpu_realtime_dwd"],
                    "target_table": "t_mops_gpu_5min_dws",
                    "aggregation_interval": "PT5M",
                    "cron": "*/5 * * * *",
                    "sql_id": "task_test"
                }
            ]
        }
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(yaml.dump(config_data))

        loader = ConfigLoader(config_file)
        errors = loader.validate()

        # 应该有task_test和sql_id not found错误（因为SQL文件不存在）
        assert len(errors) == 1
        assert "not found" in errors[0]

    def test_validate_duplicate_taskname(self, tmp_path):
        """验证重复的taskname"""
        config_data = {
            "convergence_rules": [
                {
                    "taskname": "task_test",
                    "source_tables": ["t_mops_gpu_realtime_dwd"],
                    "target_table": "t_mops_gpu_5min_dws",
                    "aggregation_interval": "PT5M",
                    "cron": "*/5 * * * *",
                    "sql_id": "task_test"
                },
                {
                    "taskname": "task_test",
                    "source_tables": ["t_mops_gpu_5min_dws"],
                    "target_table": "t_mops_gpu_hour_dws",
                    "aggregation_interval": "PT1H",
                    "cron": "5 * * * *",
                    "sql_id": "task_test_2"
                }
            ]
        }
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(yaml.dump(config_data))

        loader = ConfigLoader(config_file)
        errors = loader.validate()

        assert any("duplicate" in err.lower() for err in errors)

    def test_get_task_by_interval(self, tmp_path):
        """获取指定时间间隔的任务"""
        config_data = {
            "convergence_rules": [
                {
                    "taskname": "task_5min",
                    "source_tables": ["t_mops_gpu_realtime_dwd"],
                    "target_table": "t_mops_gpu_5min_dws",
                    "aggregation_interval": "PT5M",
                    "cron": "*/5 * * * *",
                    "sql_id": "task_5min"
                },
                {
                    "taskname": "task_1hour",
                    "source_tables": ["t_mops_gpu_5min_dws"],
                    "target_table": "t_mops_gpu_hour_dws",
                    "aggregation_interval": "PT1H",
                    "cron": "5 * * * *",
                    "sql_id": "task_1hour"
                },
                {
                    "taskname": "task_5min_2",
                    "source_tables": ["t_mops_node_realtime_dwd"],
                    "target_table": "t_mops_node_5min_dws",
                    "aggregation_interval": "PT5M",
                    "cron": "*/5 * * * *",
                    "sql_id": "task_5min_2"
                },
            ]
        }
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(yaml.dump(config_data))

        loader = ConfigLoader(config_file)
        tasks = loader.get_task_by_interval("PT5M")

        assert len(tasks) == 2
        assert all(t["aggregation_interval"] == "PT5M" for t in tasks)
