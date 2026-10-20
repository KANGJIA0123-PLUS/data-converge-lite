"""Combine chronological and wall-calendar cron candidates across clock changes."""

from datetime import datetime, timedelta, timezone

from apscheduler.triggers.cron import CronTrigger

from data_converge.dag.builder import first_valid_time, timeline_time, valid_wall_times


def _real_candidates(value: datetime | None, move_missing: bool) -> tuple[datetime, ...]:
    if value is None:
        return ()
    candidates = valid_wall_times(value)
    if not candidates and move_missing:
        return (first_valid_time(value),)
    return candidates


def _rollback_candidates(
    trigger: CronTrigger, wall_trigger: CronTrigger, now: datetime,
    values: tuple[datetime | None, ...],
) -> tuple[datetime, ...]:
    """Include future repeated occurrences whose wall clock precedes ``now``."""
    rollback = max(
        [timedelta()] + [now.utcoffset() - value.utcoffset()
                        for value in values if value is not None
                        and timeline_time(value) >= timeline_time(now)])
    if not rollback:
        return ()
    wall_now = now.replace(tzinfo=timezone.utc, fold=0)
    wall = wall_trigger.get_next_fire_time(None, wall_now - rollback)
    candidates = []
    while wall is not None and wall <= wall_now:
        candidates.extend(valid_wall_times(wall.replace(tzinfo=trigger.timezone)))
        wall = wall_trigger.get_next_fire_time(None, wall + timedelta(microseconds=1))
    return tuple(candidates)


def next_fire_time(
    trigger: CronTrigger, wall_trigger: CronTrigger, now: datetime,
    previous: datetime | None = None, *, move_missing: bool = False,
) -> datetime | None:
    """Preserve repeated hours and prevent UTC arithmetic from skipping dates."""
    while True:
        actual = trigger.get_next_fire_time(previous, now)
        wall_now = now.replace(tzinfo=timezone.utc, fold=0)
        nominal = wall_trigger.get_next_fire_time(None, wall_now)
        wall = nominal.replace(tzinfo=trigger.timezone) if nominal is not None else None
        real = [
            candidate for value in (actual, wall)
            for candidate in _real_candidates(value, move_missing)
        ]
        real.extend(_rollback_candidates(trigger, wall_trigger, now, (actual, wall)))
        candidates = [candidate for candidate in real
                      if timeline_time(candidate) >= timeline_time(now)]
        if candidates:
            return min(candidates, key=timeline_time)
        if wall is None:
            return None
        # A cron restricted to a missing wall time resumes after the clock gap.
        now = first_valid_time(wall) + timedelta(microseconds=1)
        previous = None
