"""Jump over a long outage: run the current batch first, optionally resume history.

A task is "behind" when its cursor lags ``now`` by more than its period's
threshold. All tasks then jump to one common, period-aligned boundary so
upstream and downstream skip the same span; a child never aggregates a partly
skipped parent window. Skipped spans are logged with the exact backfill
command. In ``current_then_resume`` mode they are queued for the existing
background backfill worker (lower priority than normal scheduling). History
older than ``skip_max_resume_days`` is dropped, not replayed.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import logging

from data_converge.dag.builder import align_data_end, subtract_interval, timeline_time

logger = logging.getLogger(__name__)
INTERVAL_ORDER = ('PT5M', 'PT1H', 'P1D', 'P1M')


@dataclass(frozen=True)
class SkipDecision:
    jump_to: datetime                 # common boundary; windows ending after it still run
    skipped: dict[str, tuple[datetime, datetime]]   # task -> [from, to)
    resume_from: datetime | None      # earliest history to replay, None when dropped


def windows_behind(cursor: datetime, now: datetime, interval: str) -> int:
    count, end = 0, align_data_end(now, interval)
    while timeline_time(end) > timeline_time(cursor) and count < 100_000:
        end = subtract_interval(end, interval)
        count += 1
    return count


def plan_skip(dag, cursors: dict, now: datetime, options) -> SkipDecision | None:
    """Return a jump when any task lags beyond its threshold, else None."""
    if not options.skip_batches:
        return None
    behind = {name: windows_behind(cursors[name], now, dag.tasks[name].aggregation_interval)
              for name in dag.topological_order if cursors.get(name) is not None}
    if not any(count > options.skip_threshold_batches[dag.tasks[name].aggregation_interval]
               for name, count in behind.items()):
        return None
    # Keep the newest complete window of the coarsest active period, so every
    # finer task restarts on a boundary its children can cover completely.
    coarsest = max((dag.tasks[name].aggregation_interval for name in behind), key=INTERVAL_ORDER.index)
    jump_to = subtract_interval(align_data_end(now, coarsest), coarsest)
    skipped = {name: (cursors[name], jump_to) for name in behind
               if timeline_time(cursors[name]) < timeline_time(jump_to)}
    if not skipped:
        return None
    oldest = min((span[0] for span in skipped.values()), key=timeline_time)
    resume_from = None
    if options.skip_mode == 'current_then_resume':
        limit = now - timedelta(days=options.skip_max_resume_days)
        resume_from = max(oldest, limit, key=timeline_time)
    return SkipDecision(jump_to, skipped, resume_from)


def log_skip(decision: SkipDecision, mode: str) -> None:
    for name, (start, end) in sorted(decision.skipped.items()):
        logger.warning('BATCH_SKIPPED task=%s from=%s to=%s mode=%s backfill="dconv --backfill --task %s '
                       '--from %s --to %s"', name, start.isoformat(), end.isoformat(), mode,
                       name, start.isoformat(), end.isoformat())
    if decision.resume_from is None and mode == 'current_then_resume':
        logger.warning('BATCH_SKIP_RESUME_DROPPED: history older than the resume limit is not replayed')


def queue_resume(definition, state_directory, decision: SkipDecision) -> str | None:
    """Queue skipped history (bounded by the resume limit) for background backfill."""
    from data_converge.backfill.plan import build_plan
    from data_converge.backfill.queue import Queue

    selected = [name for name in definition.dag.topological_order if name in decision.skipped]
    if not selected or decision.resume_from is None:
        return None
    start = decision.resume_from
    end = decision.jump_to
    plan = None
    try:
        plan = build_plan(definition, _aligned(definition, selected, start).isoformat(),
                          end.isoformat(), all_tasks=True)
        plan['items'] = [item for item in plan['items'] if item['taskname'] in selected]
        plan['total'] = len(plan['items'])
        if not plan['items']:
            return None
        with Queue(state_directory / 'backfill.sqlite3', create=True) as queue:
            job = queue.submit(plan, request_id=f'skip-{int(start.timestamp())}-{int(end.timestamp())}')
        logger.warning('BATCH_SKIP_RESUME_QUEUED job_id=%s windows=%s from=%s to=%s',
                       job, plan['total'], start.isoformat(), end.isoformat())
        return job
    except Exception as error:  # Queueing history must never stop current scheduling.
        logger.error('BATCH_SKIP_RESUME_FAILED error=%s', type(error).__name__)
        return None


def _aligned(definition, names, start: datetime) -> datetime:
    """Move the start forward to a boundary every selected period accepts."""
    coarsest = max((definition.dag.tasks[name].aggregation_interval for name in names),
                   key=INTERVAL_ORDER.index)
    boundary = align_data_end(start, coarsest)
    return boundary if timeline_time(boundary) >= timeline_time(start) else start
