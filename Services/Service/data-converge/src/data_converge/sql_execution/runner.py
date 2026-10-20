"""Execute the SQL associated with a DAG task instance."""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from typing import Any

from data_converge.dag import TaskInstance
from data_converge.errors import TaskExecutionError, safe_error_details

from . import debug
from .client import SQLExecutionError, execute_sql, load_sql
from .config import load_config, resolve_credentials
from .targets import cleanup_table_from_sql


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
        sql_file = self.sql_directory / f"{instance.task.sql_id}.sql"
        sql, local_table = self._load_verified_sql(instance, sql_file)
        self._cleanup(instance, sql_file, local_table)

        # Execute aggregation SQL
        self._log_phase(instance, sql_file, "aggregate")
        logger.info(
            "Executing aggregation SQL: task=%s, window=[%s, %s)",
            instance.taskname,
            instance.data_start,
            instance.data_end,
        )
        try:
            execute_sql(
                config=self.config,
                sql=sql,
                timeout=self.timeout,
                start_time=instance.data_start,
                end_time=instance.data_end,
                query_id=getattr(instance, "query_id", None),
            )
        except Exception as error:
            self._log_failure(instance, sql_file, "aggregate", error)
            raise

    def _load_verified_sql(self, instance: TaskInstance, sql_file: Path) -> tuple[str, str]:
        """Read and verify the approved SQL before any query can be submitted."""
        self._log_phase(instance, sql_file, "preflight")
        try:
            sql = load_sql(sql_file)
            if (self.expected_sql_hashes is not None
                    and self.expected_sql_hashes.get(instance.taskname)
                    != hashlib.sha256(sql.encode("utf-8")).hexdigest()):
                raise TaskExecutionError(
                    "SQL_CONFIGURATION_CHANGED", retryable=False, outcome_unknown=False,
                    query_id=getattr(instance, "query_id", None),
                )
            local_table = cleanup_table_from_sql(sql)
        except TaskExecutionError as error:
            self._log_failure(instance, sql_file, "preflight", error)
            raise
        except (OSError, ValueError) as error:
            # File and target validation complete before any SQL reaches ClickHouse.
            self._log_failure(instance, sql_file, "preflight", error)
            raise TaskExecutionError(
                "SQL_PREFLIGHT_FAILED", retryable=False, outcome_unknown=False,
                query_id=getattr(instance, "query_id", None),
                diagnostic=safe_error_details(error),
            ) from None
        return sql, local_table

    def _cleanup(self, instance: TaskInstance, sql_file: Path, local_table: str) -> None:
        """Complete idempotent cleanup before the aggregation query starts."""
        self._log_phase(instance, sql_file, "cleanup")
        # D3: Idempotent pre-cleanup - DELETE before INSERT to prevent duplicates
        cleanup_sql = self._build_cleanup_sql(instance, local_table)
        logger.info(
            "Idempotent pre-cleanup: table=%s, column=%s, window=[%s, %s)",
            local_table,
            instance.task.cleanup_column,
            instance.data_start,
            instance.data_end,
        )
        try:
            execute_sql(
                config=self.config, sql=cleanup_sql, timeout=self.timeout,
                start_time=instance.data_start, end_time=instance.data_end,
                query_id=getattr(instance, "query_id", None),
            )
        except Exception as error:
            diagnostic = safe_error_details(error)
            self._log_failure(instance, sql_file, "cleanup", error)
            raise TaskExecutionError(
                "IDEMPOTENT_CLEANUP_FAILED",
                retryable=True,
                outcome_unknown=True,
                query_id=getattr(instance, "query_id", None),
                diagnostic=diagnostic,
            ) from None

    def _log_failure(self, instance: TaskInstance, sql_file: Path,
                     phase: str, error: Exception) -> None:
        """Keep task identity beside safe evidence without repeating failed SQL."""
        logger.error(
            "SQL_TASK_FAILED phase=%s taskname=%s instance_id=%s sql_file=%s "
            "query_id=%s window=[%s, %s) diagnostic=%s",
            phase,
            getattr(instance, "taskname", None),
            getattr(instance, "instance_id", None),
            sql_file,
            getattr(instance, "query_id", None),
            instance.data_start,
            instance.data_end,
            safe_error_details(error),
        )
        # The client already logged the original cause and failed request.
        if not isinstance(error, SQLExecutionError):
            debug.log_error(self.config, getattr(instance, "query_id", None), error)

    def _log_phase(self, instance: TaskInstance, sql_file: Path, phase: str) -> None:
        """Associate client diagnostics with a task without repeating SQL."""
        if not debug.enabled(self.config):
            return
        logger.debug(
            "SQL_TASK phase=%s taskname=%s sql_file=%s query_id=%s window=[%s, %s)",
            phase,
            instance.taskname,
            sql_file,
            getattr(instance, "query_id", None),
            instance.data_start,
            instance.data_end,
        )

    @staticmethod
    def _build_cleanup_sql(instance: TaskInstance, local_table: str) -> str:
        """Build idempotent cleanup SQL using ALTER TABLE ... DELETE on local table.

        ClickHouse's Distributed engine does not support mutations directly,
        so we target the underlying _local MergeTree table. The
        ``mutations_sync=2`` setting ensures the DELETE completes synchronously
        before the subsequent INSERT runs, preventing duplicate rows. The local
        table is derived from the verified INSERT, preserving its database.
        """
        cleanup_column = instance.task.cleanup_column

        return (
            f"ALTER TABLE {local_table} DELETE "
            f"WHERE {cleanup_column} >= {{start_time:DateTime}} "
            f"AND {cleanup_column} < {{end_time:DateTime}} "
            f"SETTINGS mutations_sync=2"
        )
