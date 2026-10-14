"""Execute the SQL associated with a DAG task instance."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from data_converge.dag import TaskInstance

from .client import execute_sql, load_sql
from .config import load_config, resolve_credentials


class SQLTaskRunner:
    """Load and execute SQL for one DAG task instance."""

    def __init__(
        self,
        config_path: Path,
        sql_directory: Path,
        timeout: int = 300,
        *,
        config: dict[str, Any] | None = None,
    ):
        self.config_path = Path(config_path)
        self.sql_directory = Path(sql_directory)
        self.timeout = timeout

        # Reuse the validated startup snapshot so binding and execution agree.
        self.config: dict[str, Any] = (
            load_config(self.config_path) if config is None else resolve_credentials(config)
        )

    def __call__(self, instance: TaskInstance) -> None:
        sql_id = instance.task.sql_id
        sql_file = self.sql_directory / f"{sql_id}.sql"
        sql = load_sql(sql_file)

        execute_sql(
            config=self.config,
            sql=sql,
            timeout=self.timeout,
            start_time=instance.data_start,
            end_time=instance.data_end,
            query_id=getattr(instance, "query_id", None),
        )
