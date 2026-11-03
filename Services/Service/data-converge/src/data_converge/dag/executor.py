"""Execute complete dependency windows with durable duplicate suppression."""

from __future__ import annotations

from collections import Counter
from concurrent.futures import (ALL_COMPLETED, FIRST_COMPLETED, Future, ThreadPoolExecutor,
                                wait as wait_futures)
from dataclasses import dataclass
from datetime import datetime, timedelta
import time
from typing import Callable, Dict, Iterable, List, Mapping, Optional

from data_converge.errors import TaskExecutionError, safe_error_details

from .memory import InMemoryExecutionRepository
from .models import DAG, TaskInstance, TaskStatus
from .ports import CheckpointRepository, ExecutionRepository, TaskRunner
from .retry import can_start_attempt
from .builder import shift_elapsed, timeline_time


@dataclass(frozen=True)
class ExecutionReport:
    instances: Mapping[str, TaskInstance]

    @property
    def succeeded(self) -> List[TaskInstance]:
        return [i for i in self.instances.values() if i.status == TaskStatus.SUCCESS]

    @property
    def failed(self) -> List[TaskInstance]:
        return [i for i in self.instances.values() if i.status == TaskStatus.FAILED]

    @property
    def blocked(self) -> List[TaskInstance]:
        return [i for i in self.instances.values() if i.status == TaskStatus.BLOCKED]

    @property
    def unknown(self) -> List[TaskInstance]:
        return [i for i in self.instances.values() if i.status == TaskStatus.UNKNOWN]


class DAGExecutor:
    """Dispatch ready windows without a batch barrier.

    The caller's thread owns the repository: workers only run the task, while
    claims, dispatch commits and finishes happen on the calling thread. With
    ``wait=False`` submitted work keeps running across calls, so one long task
    never stalls later scheduling rounds; ``drain()`` collects it on shutdown.
    """

    def __init__(self, dag: DAG, runner: TaskRunner,
                 known_statuses: Optional[Dict[str, TaskStatus]] = None,
                 max_concurrent_tasks: int = 1, *, state_store: ExecutionRepository | None = None,
                 max_attempts: int = 3, retry_delay_seconds: float = 30,
                 readiness_check: Callable[[TaskInstance, datetime], str | None] | None = None,
                 task_parallelism: int = 1):
        if max_concurrent_tasks <= 0 or max_attempts <= 0 or retry_delay_seconds < 0:
            raise ValueError("Concurrency and attempts must be positive; retry delay cannot be negative")
        if type(task_parallelism) is not int or task_parallelism <= 0:
            raise ValueError("task_parallelism must be a positive integer")
        self.dag = dag
        self.runner = runner
        self.known_statuses = known_statuses if known_statuses is not None else {}
        self.max_concurrent_tasks = max_concurrent_tasks
        self.task_parallelism = task_parallelism
        self.state_store = state_store
        self.max_attempts = max_attempts
        self.retry_delay_seconds = retry_delay_seconds
        self.readiness_check = readiness_check
        self.cross_period_check = None
        memory = InMemoryExecutionRepository(self.known_statuses)
        self.repository: ExecutionRepository = state_store if state_store is not None else memory
        self.checkpoints = self.repository if isinstance(self.repository, CheckpointRepository) else None
        self._memory = memory.records
        self._pool: ThreadPoolExecutor | None = None
        self._inflight: Dict[Future, TaskInstance] = {}

    @property
    def inflight_count(self) -> int:
        return len(self._inflight)

    def execute(self, instances: Iterable[TaskInstance], *, now: datetime | None = None,
                max_executions: int | None = None,
                should_stop: Callable[[], bool] | None = None,
                wait: bool = True) -> ExecutionReport:
        instance_list = list(instances)
        by_id = {instance.instance_id: instance for instance in instance_list}
        if len(by_id) != len(instance_list):
            raise ValueError("Duplicate instance_id in execute()")
        finished = self._collect(block=False)
        if not instance_list:
            return ExecutionReport(finished)
        now = now or self._now(instance_list[0])
        self.repository.enqueue_and_advance(instance_list, {})
        for instance in instance_list:
            self._restore(instance)
        order = {name: index for index, name in enumerate(self.dag.topological_order)}
        instance_list.sort(key=lambda i: (timeline_time(i.data_end), order[i.taskname]))
        budget = max_executions if max_executions is not None else len(instance_list)
        self._dispatch_loop(instance_list, by_id, now, budget, should_stop, wait, finished)
        for instance in instance_list:
            if instance.status not in (TaskStatus.PENDING, TaskStatus.BLOCKED):
                continue
            if not self._upstreams_succeeded(instance, by_id):
                self._mark_blocked(instance, self._build_blocked_reason(instance, by_id))
        return ExecutionReport({**finished, **by_id})

    def _dispatch_loop(self, instance_list, by_id, initial_now, budget, should_stop, wait, finished):
        attempted: set[str] = set()
        started = time.monotonic()
        while True:
            if should_stop and should_stop():
                break
            now = shift_elapsed(initial_now, timedelta(seconds=max(0.0, time.monotonic() - started)))
            ready = self._select_ready(instance_list, by_id, attempted, now, budget)
            if ready:
                claimed = [i for i in ready if self._claim(i, now)]
                attempted.update(i.instance_id for i in ready)
                if claimed:
                    self._submit_instances(claimed, now, should_stop)
                continue
            if not wait or not self._inflight:
                break
            finished.update(self._collect(block=True))
        if wait:
            finished.update(self._collect(block=True, all_done=True))

    def _select_ready(self, instance_list, by_id, attempted, now, budget):
        capacity = min(self.max_concurrent_tasks - len(self._inflight), budget - len(attempted))
        running = Counter(instance.taskname for instance in self._inflight.values())
        ready = []
        for instance in instance_list:
            if len(ready) >= capacity:
                break
            if running[instance.taskname] >= self.task_parallelism:
                continue
            if instance.instance_id in attempted or not self._eligible(instance, now):
                continue
            if self._ready(instance, by_id, now):
                ready.append(instance)
                running[instance.taskname] += 1
        return ready

    def drain(self) -> ExecutionReport:
        """Wait for submitted work and persist every result before releasing state."""
        finished = self._collect(block=True, all_done=True)
        if self._pool is not None:
            self._pool.shutdown(wait=True)
            self._pool = None
        return ExecutionReport(finished)

    def _collect(self, *, block: bool, all_done: bool = False) -> Dict[str, TaskInstance]:
        finished: Dict[str, TaskInstance] = {}
        while self._inflight:
            if block:
                done, _ = wait_futures(tuple(self._inflight), return_when=(
                    ALL_COMPLETED if all_done else FIRST_COMPLETED))
            else:
                done = [future for future in self._inflight if future.done()]
            if not done:
                break
            for future in done:
                instance = self._inflight.pop(future)
                self._finish_future(future, instance)
                finished[instance.instance_id] = instance
            if not all_done:
                break
        return finished

    def _ready(self, instance, by_id, now):
        if not self._upstreams_succeeded(instance, by_id):
            return False
        for check in (self.readiness_check, self.cross_period_check):
            reason = check(instance, now) if check is not None else None
            if reason is not None:
                self._mark_blocked(instance, reason)
                return False
        return True

    def _mark_blocked(self, instance: TaskInstance, reason: str) -> None:
        instance.status = TaskStatus.BLOCKED
        instance.error = reason
        self.repository.mark_blocked(instance.instance_id, reason)

    def _restore(self, instance: TaskInstance) -> None:
        record = self.repository.get_instance(instance.instance_id)
        if record:
            instance.status = TaskStatus(record["status"])
            instance.error = record.get("error")
            instance.query_id = record.get("query_id")
            for attr in ("started_at", "finished_at"):
                value = record.get(attr)
                setattr(instance, attr, datetime.fromisoformat(value) if isinstance(value, str) else value)
        else:
            status = self.repository.get_status(instance.instance_id)
            if status is not None:
                instance.status = status

    def _eligible(self, instance: TaskInstance, now: datetime) -> bool:
        record = None
        reserved_pending = self.checkpoints is not None and instance.status == TaskStatus.PENDING
        if reserved_pending or instance.status in (TaskStatus.BLOCKED, TaskStatus.FAILED):
            record = self.repository.get_instance(instance.instance_id)
        if reserved_pending and record and record['attempts'] >= self.max_attempts:
            return False
        return can_start_attempt(
            instance.status, record.get("attempts", 0) if record else 0,
            record.get("retryable", False) if record else False,
            record.get("next_retry_at") if record else None,
            now=now, max_attempts=self.max_attempts)

    def _claim(self, instance: TaskInstance, now: datetime) -> bool:
        claim = self.checkpoints.prepare_attempt if self.checkpoints is not None else self.repository.claim
        claimed = claim(instance.instance_id, now=now, max_attempts=self.max_attempts)
        self._restore(instance)
        return claimed

    def _submit_instances(self, instances, now, should_stop):
        if self._pool is None:
            self._pool = ThreadPoolExecutor(max_workers=self.max_concurrent_tasks,
                                            thread_name_prefix="dag-task")
        for instance in instances:
            if self.checkpoints is not None:
                if should_stop and should_stop():
                    break
                # This commit is the boundary after which recovery must assume
                # that the external side effect may have happened.
                self.checkpoints.mark_dispatched(instance.instance_id, instance.query_id, now=now)
                self._restore(instance)
            self._inflight[self._pool.submit(self.runner, instance)] = instance

    def _finish_future(self, future: Future, instance: TaskInstance) -> None:
        retryable = False
        diagnostic = None
        try:
            future.result()
        except Exception as exc:
            instance.status, retryable, instance.error = self._classify_error(exc)
            diagnostic = safe_error_details(exc)
        else:
            instance.status = TaskStatus.SUCCESS
            instance.error = None
        instance.finished_at = self._now(instance)
        next_retry_at = shift_elapsed(
            instance.finished_at, timedelta(seconds=self.retry_delay_seconds)) if retryable else None
        # A failed success commit must stop dispatch, leaving RUNNING for recovery.
        # Keep persistence outside the runner exception handler.
        self.repository.finish(instance.instance_id, instance.status, error=instance.error,
                               retryable=retryable, next_retry_at=next_retry_at,
                               now=instance.finished_at, diagnostic=diagnostic)

    @staticmethod
    def _classify_error(exc: Exception) -> tuple[TaskStatus, bool, str]:
        if isinstance(exc, TaskExecutionError):
            if exc.outcome_unknown:
                return TaskStatus.UNKNOWN, False, str(exc)
            return TaskStatus.FAILED, exc.retryable, str(exc)
        return TaskStatus.UNKNOWN, False, f"UNEXPECTED_RUNNER_ERROR:{type(exc).__name__}"

    def _status(self, instance_id: str, current: Mapping[str, TaskInstance]) -> TaskStatus | None:
        if instance_id in current:
            return current[instance_id].status
        return self.repository.get_status(instance_id)

    def _upstreams_succeeded(self, instance: TaskInstance,
                            current_instances: Mapping[str, TaskInstance]) -> bool:
        return all(self._status(parent, current_instances) == TaskStatus.SUCCESS
                   for parent in instance.upstream_instance_ids)

    def _build_blocked_reason(self, instance: TaskInstance,
                              current_instances: Mapping[str, TaskInstance]) -> str:
        details = []
        for parent in instance.upstream_instance_ids:
            status = self._status(parent, current_instances)
            if status != TaskStatus.SUCCESS:
                details.append(f"{parent}={status.value if status else 'NOT_FOUND'}")
        return "Upstream task instances are not successful: " + ", ".join(details)

    @staticmethod
    def _now(instance: TaskInstance) -> datetime:
        return datetime.now(tz=instance.data_end.tzinfo)
