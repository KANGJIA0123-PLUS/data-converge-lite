"""配置管理模块 - 加载和管理汇聚SQL配置"""

import re
import yaml
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple


class CronValidator:
    """Cron表达式验证器"""

    @classmethod
    def validate(cls, cron_expr: str) -> Tuple[bool, Optional[str]]:
        """验证cron表达式格式"""
        if not cron_expr or not cron_expr.strip():
            return False, "cron expression cannot be empty"

        cron_expr = cron_expr.strip()

        special_exprs = {
            '@yearly', '@annually', '@monthly', '@weekly',
            '@daily', '@midnight', '@hourly'
        }
        if cron_expr in special_exprs:
            return True, None

        parts = cron_expr.split()
        if len(parts) != 5:
            return False, f"expected 5 fields, got {len(parts)}"

        field_names = ['minute', 'hour', 'day', 'month', 'weekday']
        field_ranges = [(0, 59), (0, 23), (1, 31), (1, 12), (0, 6)]

        for i, (part, name, (min_val, max_val)) in enumerate(
            zip(parts, field_names, field_ranges)
        ):
            if not cls._validate_field(part, min_val, max_val):
                return False, f"invalid {name} field: {part}"

        return True, None

    @classmethod
    def _validate_field(cls, field: str, min_val: int, max_val: int) -> bool:
        """验证单个cron字段"""
        if not re.match(r'^[\d\*\/\-,]+$', field):
            return False

        for part in field.split(','):
            if '-' in part:
                if not re.match(r'^\d+-\d+$', part):
                    return False
                start, end = map(int, part.split('-'))
                if start < min_val or end > max_val or start > end:
                    return False
            elif '/' in part:
                if not re.match(r'^(\d+|\*)\/\d+$', part):
                    return False
            elif part != '*' and not part.isdigit():
                return False
            elif part.isdigit():
                val = int(part)
                if val < min_val or val > max_val:
                    return False

        return True


class ConfigLoader:
    """汇聚配置加载器

    配置结构说明:
    - taskname: 任务唯一标识，静态定义
    - depends_on: 依赖的任务taskname列表
    - cron: 任务触发时间
    - aggregation_interval: 数据时间窗口(ISO 8601 Duration)

    DAG依赖关系基于taskname和depends_on构建，
    每次执行时taskname + batch_id构成唯一任务实例。
    """

    def __init__(self, config_path: Path, sql_dir: Optional[Path] = None):
        """初始化配置加载器

        Args:
            config_path: 配置文件路径
            sql_dir: SQL文件目录，默认在配置文件同级目录的sql文件夹
        """
        self.config_path = Path(config_path)
        self.sql_dir = sql_dir or self.config_path.parent / "sql"
        self._config: Dict[str, Any] = {}

    def load(self) -> Dict[str, Any]:
        """加载配置文件"""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config file not found: {self.config_path}")

        with open(self.config_path, "r", encoding="utf-8") as f:
            self._config = yaml.safe_load(f)

        return self._config

    def validate(self) -> List[str]:
        """验证配置完整性

        必填字段检查:
        - taskname: 任务唯一标识
        - source_tables: 源表列表（至少一个）
        - target_table: 目标表
        - aggregation_interval: 聚合时间窗口(ISO 8601 Duration)
        - cron: 任务触发时间(cron表达式)
        - sql_id: SQL文件标识符
        """
        errors = []
        rules = self.get_all_rules()
        tasknames = set()

        for idx, rule in enumerate(rules):
            taskname = rule.get("taskname")
            rule_path = f"convergence_rules[{idx}]"

            # taskname必填
            if not taskname:
                errors.append(f"{rule_path}: missing required field 'taskname'")
                continue

            if taskname in tasknames:
                errors.append(f"{rule_path}: duplicate taskname '{taskname}'")
            tasknames.add(taskname)

            self._validate_rule_fields(rule, taskname, errors)

        # 二次遍历检查depends_on引用的完整性
        for rule in rules:
            taskname = rule.get("taskname")
            if not taskname:
                continue
            depends_on = rule.get("depends_on", [])
            for dep in depends_on:
                if dep not in tasknames:
                    errors.append(f"Task '{taskname}': depends_on references unknown taskname '{dep}'")

        return errors

    def _validate_rule_fields(self, rule: Dict[str, Any], taskname: str, errors: List[str]) -> None:
        """按字段顺序收集单条规则的诊断，不提前终止校验。"""
        # source_tables必填（列表，非空）
        source_tables = rule.get("source_tables")
        if not source_tables:
            errors.append(f"Task '{taskname}': missing required field 'source_tables'")
        elif not isinstance(source_tables, list):
            errors.append(f"Task '{taskname}': 'source_tables' must be a list")

        # target_table必填
        if not rule.get("target_table"):
            errors.append(f"Task '{taskname}': missing required field 'target_table'")

        # aggregation_interval必填
        if not rule.get("aggregation_interval"):
            errors.append(f"Task '{taskname}': missing required field 'aggregation_interval'")

        # cron必填
        cron = rule.get("cron")
        if not cron:
            errors.append(f"Task '{taskname}': missing required field 'cron'")
        else:
            is_valid, err = CronValidator.validate(cron)
            if not is_valid:
                errors.append(f"Task '{taskname}': invalid cron '{cron}': {err}")

        # sql_id必填
        if not rule.get("sql_id"):
            errors.append(f"Task '{taskname}': missing required field 'sql_id'")
        else:
            sql_id = rule.get("sql_id")
            sql_file = self.sql_dir / f"{sql_id}.sql"
            if not sql_file.exists():
                errors.append(f"Task '{taskname}': sql_id '{sql_id}' file not found: {sql_file}")

    def get_rule_by_taskname(self, taskname: str) -> Optional[Dict[str, Any]]:
        """根据任务名获取汇聚规则"""
        for rule in self.get_all_rules():
            if rule.get("taskname") == taskname:
                return rule
        return None

    def get_all_rules(self) -> List[Dict[str, Any]]:
        """获取所有汇聚规则"""
        if not self._config:
            self.load()

        return self._config.get("convergence_rules", [])

    def build_task_dependencies(self) -> Dict[str, List[str]]:
        """构建任务DAG依赖关系

        基于depends_on字段构建任务依赖图。
        DAG调度顺序严格按照depends_on指定的taskname依赖关系执行。

        Returns:
            依赖关系字典，key是taskname，value是依赖的taskname列表
        """
        dependencies: Dict[str, List[str]] = {}
        for rule in self.get_all_rules():
            taskname = rule.get("taskname")
            depends_on = rule.get("depends_on", [])

            if taskname:
                dependencies[taskname] = depends_on

        return dependencies

    def get_upstream_tasks(self, taskname: str) -> List[str]:
        """获取任务的所有上游任务

        Args:
            taskname: 任务名称

        Returns:
            上游任务列表（直接依赖的任务）
        """
        deps = self.build_task_dependencies()
        return deps.get(taskname, [])

    def get_downstream_tasks(self, taskname: str) -> List[str]:
        """获取任务的所有下游任务

        Args:
            taskname: 任务名称

        Returns:
            下游任务列表（依赖该任务的任务）
        """
        deps = self.build_task_dependencies()
        downstream = []
        for task, parents in deps.items():
            if taskname in parents:
                downstream.append(task)
        return downstream

    def get_task_interval(self, taskname: str) -> Optional[str]:
        """获取任务的聚合时间间隔

        Args:
            taskname: 任务名称

        Returns:
            ISO 8601 Duration格式的时间间隔
        """
        rule = self.get_rule_by_taskname(taskname)
        return rule.get("aggregation_interval") if rule else None

    def get_task_cron(self, taskname: str) -> Optional[str]:
        """获取任务的cron表达式

        Args:
            taskname: 任务名称

        Returns:
            cron表达式
        """
        rule = self.get_rule_by_taskname(taskname)
        return rule.get("cron") if rule else None

    def get_task_by_interval(self, interval: str) -> List[Dict[str, Any]]:
        """获取指定时间间隔的任务

        Args:
            interval: ISO 8601 Duration格式的时间间隔

        Returns:
            匹配的任务列表
        """
        return [rule for rule in self.get_all_rules() if rule.get("aggregation_interval") == interval]

    def load_sql(self, sql_id: str) -> str:
        """根据sql_id加载SQL语句"""
        sql_file = self.sql_dir / f"{sql_id}.sql"
        if not sql_file.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_file}")

        with open(sql_file, "r", encoding="utf-8") as f:
            return f.read()

    def get_sql_with_params(self, sql_id: str, start_time: str, end_time: str) -> str:
        """获取填充参数后的SQL语句"""
        sql = self.load_sql(sql_id)
        return sql.format(start_time=start_time, end_time=end_time)

    def get_source_tables(self) -> List[str]:
        """获取所有源表"""
        sources = set()
        for rule in self.get_all_rules():
            source_tables = rule.get("source_tables", [])
            for source in source_tables:
                sources.add(source)

        return list(sources)

    def get_target_tables(self) -> List[str]:
        """获取所有目标表"""
        targets = set()
        for rule in self.get_all_rules():
            if rule.get("target_table"):
                targets.add(rule["target_table"])

        return list(targets)

    def get_all_tasknames(self) -> List[str]:
        """获取所有任务名"""
        return [rule.get("taskname") for rule in self.get_all_rules() if rule.get("taskname")]
