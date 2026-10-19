"""Readiness evidence, including windows with no recorded signal at all."""
from datetime import datetime, timedelta

from data_converge.dwd_ready.message import DWD_TYPES
from .report import TZ


def parse_window(value):
    instant = datetime.fromisoformat(value)
    if instant.tzinfo is None or instant.utcoffset() is None:
        raise ValueError('WINDOW_REQUIRES_TIMEZONE')
    instant = instant.astimezone(TZ)
    if instant.minute % 5 or instant.second or instant.microsecond:
        raise ValueError('WINDOW_REQUIRES_FIVE_MINUTE_BOUNDARY')
    return instant


def window_starts(coverage, now, limit, selected=None):
    if selected is not None:
        return [selected]
    end = now.replace(minute=now.minute - now.minute % 5, second=0, microsecond=0)
    return [start for index in range(limit)
            if (start := end - timedelta(minutes=5 * (index + 1))) >= coverage]


def window_evidence(store, start, coverage, now, warning_seconds, *, cutover=None):
    rows = store.signals(start)
    received = sorted(row['dwd_type'] for row in rows if row['dwd_type'] in DWD_TYPES)
    missing = sorted(DWD_TYPES - set(received))
    end = start + timedelta(minutes=5)
    waited = max(0, int((now - end).total_seconds()))
    status = 'READY' if not missing else 'WAITING'
    if start < coverage:
        status = 'BEFORE_COVERAGE'
    elif end > now:
        status = 'WINDOW_OPEN'
    times = [datetime.fromisoformat(row['received_at']).astimezone(TZ) for row in rows]
    completed = max(times) if times and not missing else None
    evidence = dict(window_start=start.isoformat(), window_end=end.isoformat(), status=status,
                received=received, missing=missing, wait_seconds=waited if missing else 0,
                warning_due=status == 'WAITING' and waited >= warning_seconds,
                last_received_at=max(times).isoformat() if times else None,
                ready_delay_seconds=round((completed - end).total_seconds(), 3) if completed else None)
    if cutover is not None and start < cutover:
        evidence.update(historical=True, recovery_cutover=cutover.isoformat(), warning_due=False)
    return evidence


def list_windows(store, ready, *, limit=20, selected=None, now=None):
    now = now or datetime.now(TZ)
    coverage = parse_window(store.get_meta('coverage_start'))
    recovery = store.legacy_recovery()
    cutover = parse_window(recovery['cutover']) if recovery is not None else None
    current_start = max(coverage, cutover) if cutover is not None else coverage
    return [window_evidence(store, start, coverage, now, ready['wait_warning_seconds'], cutover=cutover)
            for start in window_starts(current_start, now, limit, selected)]
