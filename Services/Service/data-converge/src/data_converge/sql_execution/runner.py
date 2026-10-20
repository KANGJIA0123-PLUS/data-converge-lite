"""Execute the SQL associated with a DAG task instance."""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from typing import Any

from data_converge.dag import TaskInstance
from data_converge.errors import TaskExecutionError, safe_error_details

from .client import execute_sql, load_sql
from .config import load_config, resolve_credentials


logger = logging.getLogger(__name__)


class SQLTaskRunner:
    """Load and execute SQL for one DAG task instance with idempotency.

    Before executing the aggregation SQL, the runner issues an
    ``ALTER TABLE <target>_local DELETE`` on the underlying local table
    for the same data window. This prevents duplicate rows when a task
    is retried or recovered after a crash — the cleanup is idempotent
    because deleting rows that do not exist is a no-op.
    """

    def __init__(
        self,
        config_path: Path,
        sql_directory: Path,
        timeout: int = 300,
        *,
        config: dict[str, Any] | None = None,
        expected_sql_hashes: dict[str, str] | None = None,
    ):
        self.config_path = Path(config_path)
        self.sql_directory = Path(sql_directory)
        self.timeout = timeout
        self.expected_sql_hashes = None if expected_sql_hashes is None else dict(expected_sql_hashes)

        # Reuse the validated startup snapshot so binding and execution agree.
        self.config: dict[str, Any] = (
            load_config(self.config_path) if config is None else resolve_credentials(config)
        )

    def __call__(self, instance: TaskInstance) -> None:
        sql_id = instance.task.sql_id
        sql_file = self.sql_directory / f"{sql_id}.sql"
        try:
            sql = load_sql(sql_file)
        except (OSError, ValueError) as error:
            # File loading completes before any SQL can reach ClickHouse.
            raise TaskExecutionError(
                "SQL_PREFLIGHT_FAILED", retryable=False, outcome_unknown=False,
                query_id=getattr(instance, "query_id", None),
                diagnostic=safe_error_details(error),
            ) from None

        if (self.expected_sql_hashes is not None
                and self.expected_sql_hashes.get(instance.taskname)
                != hashlib.sha256(sql.encode("utf-8")).hexdigest()):
            raise TaskExecutionError(
                "SQL_CONFIGURATION_CHANGED", retryable=False, outcome_unknown=False,
                query_id=getattr(instance, "query_id", None),
            )

        # D3: Idempotent pre-cleanup - DELETE before INSERT to prevent duplicates
        cleanup_sql = self._build_cleanup_sql(instance)
        logger.info(
            "Idempotent pre-cleanup: table=%s_local, column=%s, window=[%s, %s)",
            instance.task.target_table,
            instance.task.cleanup_column,
            instance.data_start,
            instance.data_end,
        )
        try:
            execute_sql(config=self.config,sql=cleanup_sql,timeout=self.timeout,start_time=instance.data_start,end_time=instance.data_end,query_id=getattr(instance, "query_id", None),)
        except Exception as e:
            logger.error("Pre-cleanup failed: %s", str(e))
            raise TaskExecutionError(
                "IDEMPOTENT_CLEANUP_FAILED",
                retryable=True,
                outcome_unknown=True,
                query_id=getattr(instance, "query_id", None),
                diagnostic=safe_error_details(e),
            ) from None

        # Execute aggregation SQL
        logger.info(
            "Executing aggregation SQL: task=%s, window=[%s, %s)",
            instance.taskname,
            instance.data_start,
            instance.data_end,
        )
        execute_sql(
            config=self.config,
            sql=sql,
            timeout=self.timeout,
            start_time=instance.data_start,
            end_time=instance.data_end,
            query_id=getattr(instance, "query_id", None),
        )

    @staticmethod
    def _build_cleanup_sql(instance: TaskInstance) -> str:
        """Build idempotent cleanup SQL using ALTER TABLE ... DELETE on local table.

        ClickHouse's Distributed engine does not support mutations directly,
        so we target the underlying _local MergeTree table. The
        ``mutations_sync=2`` setting ensures the DELETE completes synchronously
        before the subsequent INSERT runs, preventing duplicate rows.
        """
        target_table = instance.task.target_table
        cleanup_column = instance.task.cleanup_column
        local_table = f"{target_table}_local"

        return (
            f"ALTER TABLE {local_table} DELETE "
            f"WHERE {cleanup_column} >= {{start_time:DateTime}} "
            f"AND {cleanup_column} < {{end_time:DateTime}} "
            f"SETTINGS mutations_sync=2"
        )
