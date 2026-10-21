"""配置管理模块补充测试：覆盖 test_config.py 尚未覆盖的 ConfigLoader / CronValidator 函数"""

import yaml

from data_converge.config import ConfigLoader, CronValidator


def _write_config(tmp_path, rules):
    """在临时目录写一个 config YAML 和配套的 sql 文件，返回 (config_path, sql_dir)。"""
    config_file = tmp_path / "config.yaml"
    config_file.write_text(yaml.dump({"convergence_rules": rules}))
    sql_dir = tmp_path / "sql"
    sql_dir.mkdir(exist_ok=True)
    for rule in rules:
        sql_id = rule.get("sql_id")
        if sql_id:
            (sql_dir / f"{sql_id}.sql").write_text(
                "SELECT * FROM table WHERE dt >= '{start_time}' AND dt < '{end_time}'"
            )
    return config_file, sql_dir


class TestGetTablesAndTasknames:
    """get_source_tables / get_target_tables / get_all_tasknames 测试"""

    RULES = [
        {"taskname": "task_realtime", "source_tables": ["t_mops_gpu_realtime_dwd"],
         "target_table": "t_mops_gpu_5min_dws", "aggregation_interval": "PT5M",
         "cron": "*/5 * * * *", "sql_id": "task_realtime", "depends_on": []},
        {"taskname": "task_hour", "source_tables": ["t_mops_gpu_5min_dws"],
         "target_table": "t_mops_gpu_hour_dws", "aggregation_interval": "PT1H",
         "cron": "6 * * * *", "sql_id": "task_hour", "depends_on": ["task_realtime"]},
        {"taskname": "task_day", "source_tables": ["t_mops_gpu_hour_dws"],
         "target_table": "t_mops_gpu_day_dws", "aggregation_interval": "P1D",
         "cron": "10 0 * * *", "sql_id": "task_day", "depends_on": ["task_hour"]},
    ]

    def test_get_source_tables_deduplicates(self, tmp_path):
        """多个任务共用同一张源表时，get_source_tables 应去重"""
        rules = [dict(rule) for rule in self.RULES]
        rules[2]["source_tables"] = ["t_mops_gpu_hour_dws", "t_mops_gpu_5min_dws"]
        config_file, _ = _write_config(tmp_path, rules)
        loader = ConfigLoader(config_file)
        assert sorted(loader.get_source_tables()) == [
            "t_mops_gpu_5min_dws", "t_mops_gpu_hour_dws", "t_mops_gpu_realtime_dwd"]

    def test_get_source_tables_empty(self, tmp_path):
        """没有配置任何规则时，get_source_tables 返回空列表"""
        config_file, _ = _write_config(tmp_path, [])
        loader = ConfigLoader(config_file)
        assert loader.get_source_tables() == []

    def test_get_target_tables(self, tmp_path):
        """get_target_tables 返回全部目标表"""
        config_file, _ = _write_config(tmp_path, self.RULES)
        loader = ConfigLoader(config_file)
        assert sorted(loader.get_target_tables()) == [
            "t_mops_gpu_5min_dws", "t_mops_gpu_day_dws", "t_mops_gpu_hour_dws"]

    def test_get_all_tasknames_missing_sql_id_still_returns_taskname(self, tmp_path):
        """get_all_tasknames 只依赖 taskname 字段，即使缺少 sql_id 也应返回任务名"""
        rules = [{"taskname": "no_sql_task", "source_tables": ["t_a"],
                  "target_table": "t_b", "aggregation_interval": "PT5M",
                  "cron": "*/5 * * * *"}]
        config_file, _ = _write_config(tmp_path, rules)
        loader = ConfigLoader(config_file)
        assert loader.get_all_tasknames() == ["no_sql_task"]


class TestDependencyQueries:
    """build_task_dependencies / get_upstream_tasks / get_downstream_tasks 链式依赖测试"""

    RULES = [
        {"taskname": "task_realtime", "source_tables": ["t_a"], "target_table": "t_5min",
         "aggregation_interval": "PT5M", "cron": "*/5 * * * *", "sql_id": "task_realtime",
         "depends_on": []},
        {"taskname": "task_hour", "source_tables": ["t_5min"], "target_table": "t_hour",
         "aggregation_interval": "PT1H", "cron": "6 * * * *", "sql_id": "task_hour",
         "depends_on": ["task_realtime"]},
        {"taskname": "task_day", "source_tables": ["t_hour"], "target_table": "t_day",
         "aggregation_interval": "P1D", "cron": "10 0 * * *", "sql_id": "task_day",
         "depends_on": ["task_hour"]},
    ]

    def test_build_task_dependencies_chain(self, tmp_path):
        """构建三层链条依赖: realtime -> hour -> day"""
        config_file, _ = _write_config(tmp_path, self.RULES)
        loader = ConfigLoader(config_file)
        assert loader.build_task_dependencies() == {
            "task_realtime": [],
            "task_hour": ["task_realtime"],
            "task_day": ["task_hour"],
        }

    def test_get_upstream_tasks_returns_direct_parents_only(self, tmp_path):
        """get_upstream_tasks 只返回直接上游，不递归穿透链条"""
        config_file, _ = _write_config(tmp_path, self.RULES)
        loader = ConfigLoader(config_file)
        assert loader.get_upstream_tasks("task_day") == ["task_hour"]
        assert loader.get_upstream_tasks("task_realtime") == []

    def test_get_upstream_tasks_unknown_task(self, tmp_path):
        """查询不存在的任务名应返回空列表"""
        config_file, _ = _write_config(tmp_path, self.RULES)
        loader = ConfigLoader(config_file)
        assert loader.get_upstream_tasks("task_nonexistent") == []

    def test_get_downstream_tasks_direct_dependants_only(self, tmp_path):
        """get_downstream_tasks 只返回直接下游"""
        config_file, _ = _write_config(tmp_path, self.RULES)
        loader = ConfigLoader(config_file)
        assert loader.get_downstream_tasks("task_realtime") == ["task_hour"]
        assert loader.get_downstream_tasks("task_hour") == ["task_day"]

    def test_get_downstream_tasks_no_dependants(self, tmp_path):
        """没有被依赖的任务应返回空列表"""
        config_file, _ = _write_config(tmp_path, self.RULES)
        loader = ConfigLoader(config_file)
        assert loader.get_downstream_tasks("task_day") == []


class TestIntervalAndSqlBoundaries:
    """get_task_by_interval / get_sql_with_params 边界测试"""

    RULES = [
        {"taskname": "task_5min_a", "source_tables": ["t_a"], "target_table": "t_a_5min",
         "aggregation_interval": "PT5M", "cron": "*/5 * * * *", "sql_id": "task_5min_a"},
        {"taskname": "task_5min_b", "source_tables": ["t_b"], "target_table": "t_b_5min",
         "aggregation_interval": "PT5M", "cron": "*/5 * * * *", "sql_id": "task_5min_b"},
        {"taskname": "task_hour", "source_tables": ["t_a_5min"], "target_table": "t_hour",
         "aggregation_interval": "PT1H", "cron": "6 * * * *", "sql_id": "task_hour"},
    ]

    def test_get_task_by_interval_returns_all_matches(self, tmp_path):
        """PT5M 间隔应返回全部两个 5 分钟任务"""
        config_file, _ = _write_config(tmp_path, self.RULES)
        loader = ConfigLoader(config_file)
        tasks = loader.get_task_by_interval("PT5M")
        assert {t["taskname"] for t in tasks} == {"task_5min_a", "task_5min_b"}

    def test_get_task_by_interval_no_match(self, tmp_path):
        """没有匹配的间隔时返回空列表"""
        config_file, _ = _write_config(tmp_path, self.RULES)
        loader = ConfigLoader(config_file)
        assert loader.get_task_by_interval("PT7M") == []

    def test_get_sql_with_params_replaces_start_and_end_times(self, tmp_path):
        """SQL 模板中的起止时间占位符应替换为给定窗口。"""
        config_file, _ = _write_config(tmp_path, self.RULES)
        loader = ConfigLoader(config_file)
        sql = loader.get_sql_with_params(
            "task_5min_a", "2026-08-01 00:00:00", "2026-08-01 00:05:00")
        assert sql == (
            "SELECT * FROM table WHERE dt >= '2026-08-01 00:00:00' "
            "AND dt < '2026-08-01 00:05:00'"
        )
        assert "{start_time}" not in sql
        assert "{end_time}" not in sql


class TestCronValidatorExtras:
    """CronValidator 补充边界测试"""

    def test_validate_at_midnight_alias(self):
        """@midnight 是 @daily 的别名，应有效"""
        assert CronValidator.validate("@midnight")[0] is True

    def test_validate_all_special_exprs(self):
        """所有特殊表达式都应有效"""
        for expr in ("@yearly", "@annually", "@monthly", "@weekly", "@daily", "@midnight", "@hourly"):
            assert CronValidator.validate(expr)[0] is True

    def test_validate_first_field_steps(self):
        """分钟字段支持 */N 步进"""
        assert CronValidator.validate("*/2 * * * *")[0] is True

    def test_validate_out_of_range_minute(self):
        """分钟字段 60 超出范围应无效"""
        assert CronValidator.validate("60 * * * *")[0] is False

    def test_validate_out_of_range_hour(self):
        """小时字段 24 超出范围应无效"""
        assert CronValidator.validate("0 24 * * *")[0] is False

    def test_validate_out_of_range_weekday(self):
        """星期字段 7 超出范围(0-6)应无效"""
        assert CronValidator.validate("0 0 * * 7")[0] is False

    def test_validate_list_with_step(self):
        """列表与步进结合: 5,10,*/3 应有效"""
        assert CronValidator.validate("5,10,*/3 * * * *")[0] is True

    def test_validate_invalid_range_inverse(self):
        """范围逆序(10-5)应无效"""
        assert CronValidator.validate("10-5 * * * *")[0] is False

    def test_validate_negative_value(self):
        """负数值应无效"""
        assert CronValidator.validate("-1 * * * *")[0] is False

    def test_validate_whitespace_trimmed(self):
        """首尾空白应被裁剪后再校验"""
        assert CronValidator.validate("  5 * * * *  ")[0] is True
