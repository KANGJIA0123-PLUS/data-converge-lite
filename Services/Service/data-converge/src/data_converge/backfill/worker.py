"""Consume one manual window between normal scheduling rounds."""

from datetime import datetime
import logging
import sqlite3
from zoneinfo import ZoneInfo

from data_converge.dag import DAGExecutor, TaskStatus
from data_converge.errors import safe_error_details
from .plan import plan_revision
from .queue import Queue

logger = logging.getLogger(__name__)


class BackfillWorker:
    def __init__(self, definition, store, runner, *, stop_check=lambda: False):
        self.definition = definition
        self.store = store
        self.runner = runner
        self.stop_check = stop_check
        self.timezone = ZoneInfo(definition.options.timezone)
        self.revision = plan_revision(definition)
        self.path = store.path.parent / "backfill.sqlite3"

    def __call__(self):
        if self.stop_check() or not self.path.exists():
            return
        # Queue failures stop manual progress only. State persistence failures in
        # run_one propagate and stop dispatch, as with ordinary task execution.
        try:
            queue = Queue(self.path)
        except (OSError, ValueError, sqlite3.Error) as error:
            self._queue_error(error)
            return
        with queue:
            self.run_one(queue, tolerate_queue_errors=True)

    def run_one(self, queue, *, tolerate_queue_errors=False):
        if self.stop_check():
            return
        item = self._queue_operation(queue.next_item, tolerate_queue_errors)
        if item is None:
            return
        previous = self.store.get_backfill(item["item_id"])
        if previous is None:
            result = self._process(queue, item, tolerate_queue_errors)
        elif previous["status"] == "RUNNING":
            raise RuntimeError("BACKFILL_DISPATCH_OUTCOME_UNCONFIRMED")
        else:
            result = previous
        if result is None:
            return
        self._queue_operation(
            lambda: queue.complete(item["item_id"], result["status"],
                                   query_id=result.get("query_id"), error=result.get("error")),
            tolerate_queue_errors)
        logger.info("BACKFILL_WINDOW_FINISHED job_id=%s instance=%s status=%s query_id=%s error=%s",
                    item["job_id"], item["instance_id"], result["status"],
                    result.get("query_id"), result.get("error"))

    def _process(self, queue, item, tolerate):
        if item["revision"] != self.revision:
            return self._blocked("BACKFILL_CONFIGURATION_CHANGED")
        try:
            instance = self._instance(item)
        except (KeyError, ValueError, TypeError):
            return self._blocked("BACKFILL_WINDOW_CHANGED")
        if self.store.get_status(instance.instance_id) == TaskStatus.SUCCESS and not item["recompute"]:
            return self.store.begin_backfill(item["item_id"], instance, recompute=False)
        dependencies = self._queue_operation(
            lambda: {upstream: queue.item_status(item["job_id"], upstream)
                     for upstream in instance.upstream_instance_ids}, tolerate)
        if dependencies is None:
            return None
        for upstream, selected in dependencies.items():
            if (selected is not None and selected not in ("SUCCESS", "SKIPPED")
                    or selected is None and self.store.get_status(upstream) != TaskStatus.SUCCESS):
                return self._blocked("BACKFILL_UPSTREAM_NOT_SUCCESSFUL")
        if self.stop_check():
            # Keep queued; no claim or SQL when leadership/shutdown changed.
            return None
        result = self.store.begin_backfill(item["item_id"], instance, recompute=item["recompute"])
        if result["status"] != "RUNNING":
            return result
        instance.query_id = result["query_id"]
        self._execute(instance, item)
        return self.store.get_backfill(item["item_id"])

    def _instance(self, item):
        scheduled = datetime.fromisoformat(item["scheduled_time"]).astimezone(self.timezone)
        instance = self.definition.builder.create_instance(self.definition.dag, item["taskname"], scheduled)
        if instance.instance_id != item["instance_id"]:
            raise ValueError("BACKFILL_WINDOW_CHANGED")
        for field in ("data_start", "data_end"):
            if getattr(instance, field).isoformat() != item[field]:
                raise ValueError("BACKFILL_WINDOW_CHANGED")
        if list(instance.upstream_instance_ids) != item["upstream_instance_ids"]:
            raise ValueError("BACKFILL_WINDOW_CHANGED")
        if instance.data_end.timestamp() > datetime.now(self.timezone).timestamp():
            raise ValueError("BACKFILL_END_IN_FUTURE")
        return instance

    def _execute(self, instance, item):
        logger.info("BACKFILL_WINDOW_STARTED job_id=%s instance=%s query_id=%s",
                    item["job_id"], instance.instance_id, instance.query_id)
        diagnostic = None
        try:
            self.runner(instance)
        except Exception as error:
            status, _, reason = DAGExecutor._classify_error(error)
            diagnostic = safe_error_details(error)
        else:
            status, reason = TaskStatus.SUCCESS, None
        # Explicit requests never spill into the ordinary automatic retry queue.
        self.store.finish(instance.instance_id, status, error=reason, retryable=False,
                          diagnostic=diagnostic)

    @staticmethod
    def _blocked(reason):
        return {"status": "BLOCKED", "error": reason}

    @staticmethod
    def _queue_operation(operation, tolerate):
        try:
            return operation()
        except (OSError, ValueError, sqlite3.Error) as error:
            if not tolerate:
                raise
            BackfillWorker._queue_error(error)
            return None

    @staticmethod
    def _queue_error(error):
        logger.error("BACKFILL_QUEUE_UNAVAILABLE diagnostic=%s", safe_error_details(error))
