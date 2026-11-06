"""Consume one manual window between normal scheduling rounds."""

from datetime import datetime
import logging
import sqlite3
from zoneinfo import ZoneInfo

from data_converge.dag import DAGExecutor, TaskStatus
from data_converge.errors import safe_error_details
from .plan import plan_revision
from .queue import Queue
from .preaggregate import validate_eligibility

logger = logging.getLogger(__name__)

# Longer than the 1800-second maximum task timeout: a local RUNNING row may still
# be the previous writer's in-flight SQL, so keep the request queued before
# declaring it blocked for manual attention.
RUNNING_HOLD_SECONDS = 35 * 60


class BackfillWorker:
    def __init__(self, definition, store, runner, *, stop_check=lambda: False, clock=None):
        self.definition = definition
        self.store = store
        self.runner = runner
        self.stop_check = stop_check
        self.timezone = ZoneInfo(definition.options.timezone)
        self.clock = clock or (lambda: datetime.now(self.timezone))
        self.revision = plan_revision(definition)
        self.path = store.path.parent / "backfill.sqlite3"
        # Imported/prepared RUNNING rows have no started_at; age them from the
        # first observation in this worker so a takeover still gets a full hold.
        self._running_since = {}

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
        previous = self._receipt(item)
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
        logger.info("BACKFILL_WINDOW_FINISHED execution_kind=%s job_id=%s instance=%s status=%s query_id=%s error=%s",
                    item["execution_kind"], item["job_id"], item["instance_id"], result["status"],
                    result.get("query_id"), result.get("error"))

    def _process(self, queue, item, tolerate):
        if item["execution_kind"] == "BACKFILL" and item["revision"] != self.revision:
            return self._blocked("BACKFILL_CONFIGURATION_CHANGED")
        try:
            instance = self._instance(item)
        except (KeyError, ValueError, TypeError):
            return self._blocked("BACKFILL_WINDOW_CHANGED")
        if item["execution_kind"] == "PREAGGREGATE":
            return self._process_preaggregate(queue, item, instance, tolerate)
        age = self._running_age(instance)
        if age is not None:
            return (self._blocked("BACKFILL_INSTANCE_RUNNING")
                    if age >= RUNNING_HOLD_SECONDS else None)
        if instance.instance_id in self.store.held_orphans():
            # A previous leader's query may still be writing this window; the
            # orphan guard releases it once ClickHouse confirms it stopped.
            return None
        if self.store.get_status(instance.instance_id) == TaskStatus.SUCCESS and not item["recompute"]:
            return self.store.begin_backfill(item["item_id"], instance, recompute=False)
        dependencies = self._queue_operation(
            lambda: {upstream: queue.item_result(item["job_id"], upstream)
                     for upstream in instance.upstream_instance_ids}, tolerate)
        if dependencies is None:
            return None
        if not self._dependencies_successful(item, dependencies):
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

    def _running_age(self, instance):
        """Seconds since a local RUNNING attempt started, or None when not RUNNING."""
        row = self.store.get_instance(instance.instance_id)
        if row is None or row["status"] != TaskStatus.RUNNING.value:
            self._running_since.pop(instance.instance_id, None)
            return None
        started = row.get("started_at")
        if started:
            self._running_since.pop(instance.instance_id, None)
            return (self.clock() - datetime.fromisoformat(started)).total_seconds()
        now = self.clock()
        first_seen = self._running_since.setdefault(instance.instance_id, now)
        return (now - first_seen).total_seconds()

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
        if (item["execution_kind"] == "BACKFILL"
                and instance.data_end.timestamp() > self.clock().timestamp()):
            raise ValueError("BACKFILL_END_IN_FUTURE")
        return instance

    def _execute(self, instance, item):
        logger.info("BACKFILL_WINDOW_STARTED execution_kind=%s job_id=%s instance=%s "
                    "query_id=%s window=[%s,%s) formal_scheduled_time=%s",
                    item["execution_kind"], item["job_id"], instance.instance_id, instance.query_id,
                    instance.data_start, instance.data_end, item.get("formal_scheduled_time"))
        diagnostic = None
        try:
            self.runner(instance)
        except Exception as error:
            status, _, reason = DAGExecutor._classify_error(error)
            diagnostic = safe_error_details(error)
        else:
            status, reason = TaskStatus.SUCCESS, None
        # Explicit requests never spill into the ordinary automatic retry queue.
        if item["execution_kind"] == "PREAGGREGATE":
            self.store.finish_preaggregate(item["item_id"], status, error=reason,
                                           diagnostic=diagnostic, now=self.clock())
        else:
            self.store.finish(instance.instance_id, status, error=reason, retryable=False,
                              diagnostic=diagnostic)

    def _receipt(self, item):
        getter = (self.store.get_preaggregate if item["execution_kind"] == "PREAGGREGATE"
                  else self.store.get_backfill)
        return getter(item["item_id"])

    def _process_preaggregate(self, queue, item, instance, tolerate):
        now = self.clock()
        if instance.data_end.timestamp() <= now.timestamp():
            return self.store.begin_preaggregate(item, instance, now=now)
        if item["revision"] != self.revision:
            return self._blocked("BACKFILL_CONFIGURATION_CHANGED")
        try:
            validate_eligibility(self.definition, self.store, instance, now, item)
        except ValueError as error:
            return self._blocked(str(error))
        dependencies = self._queue_operation(
            lambda: {upstream: queue.item_result(item["job_id"], upstream)
                     for upstream in instance.upstream_instance_ids}, tolerate)
        if dependencies is None:
            return None
        if not self._dependencies_successful(item, dependencies):
            return self._blocked("BACKFILL_UPSTREAM_NOT_SUCCESSFUL")
        if self.stop_check():
            return None
        # Repeat using the actual dispatch clock: queue reads may cross data_end.
        now = self.clock()
        if instance.data_end.timestamp() > now.timestamp():
            try:
                validate_eligibility(self.definition, self.store, instance, now, item)
            except ValueError as error:
                return self._blocked(str(error))
        result = self.store.begin_preaggregate(item, instance, now=now)
        if result["status"] != "RUNNING":
            return result
        instance.query_id = result["query_id"]
        self._execute(instance, item)
        return self.store.get_preaggregate(item["item_id"])

    def _dependencies_successful(self, item, dependencies):
        for upstream, selected in dependencies.items():
            formal = self.store.get_status(upstream) == TaskStatus.SUCCESS
            if selected is not None:
                if selected["status"] == "SUCCESS":
                    continue
                if (selected["status"] == "SKIPPED"
                        and selected["execution_kind"] == "BACKFILL" and formal):
                    continue
                return False
            if self.store.preaggregate_blocker(upstream):
                return False
            if formal:
                continue
            if item["execution_kind"] != "PREAGGREGATE":
                return False
            latest = self.store.latest_preaggregate(upstream)
            if (latest is None or latest["revision"] != item["revision"]
                    or latest.get("effective_status", latest["status"]) != "SUCCESS"):
                return False
        return True

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
