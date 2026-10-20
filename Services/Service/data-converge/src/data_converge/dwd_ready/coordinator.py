"""Persist producer readiness before acknowledging, and gate all five-minute SQL."""

from __future__ import annotations

import logging
import time
from collections import OrderedDict
from datetime import timedelta, timezone

from .message import DWD_TYPES, ReadyMessageError, parse_message
from .ports import ReadinessState, ReadySignalSource, SignalSourceError


logger = logging.getLogger(__name__)


class ReadyCoordinator:
    """Runs on the scheduler's thread, which owns the SQLite writer connection.

    Signals prove source readiness only. The existing SQL executor is still the
    sole authority for SUCCESS, retry, UNKNOWN and duplicate suppression.
    """

    def __init__(self, store: ReadinessState, source: ReadySignalSource, coverage_start, *, batch_size=100,
                 wait_warning_seconds=60, should_stop=None):
        self.store = store
        self.source = source
        self.coverage_start = coverage_start
        self.batch_size = batch_size
        self.wait_warning_seconds = wait_warning_seconds
        self.should_stop = should_stop or (lambda: False)
        self._warnings = OrderedDict()

    def poll(self, now):
        now = now.astimezone(timezone.utc)
        completed = set()
        # Limit both message count and time between dispatch/stop checks. One
        # in-progress Redis command retains its configured socket timeout.
        started = time.monotonic()
        deadline = started + 5
        for index in range(self.batch_size):
            if self.should_stop() or time.monotonic() >= deadline:
                break
            try:
                raw = self.source.receive(block=index == 0)
            except SignalSourceError:
                logger.warning('DWD_REDIS_UNAVAILABLE: waiting for source readiness')
                break
            if raw is None:
                break
            if self.should_stop():
                break  # Keep the message in the processing list for next startup.
            try:
                message = parse_message(raw)
            except ReadyMessageError:
                logger.warning('DWD_MESSAGE_INVALID: malformed signal discarded')
            else:
                start = message.window_start
                # Blocking intake may cross a five-minute boundary. Advance the
                # supplied clock, including explicit --now evaluation clocks.
                received_at = now + timedelta(seconds=time.monotonic() - started)
                if start < self.coverage_start:
                    logger.info('DWD_BEFORE_COVERAGE: signal ignored')
                elif start > received_at or start + timedelta(minutes=5) > received_at:
                    logger.warning('DWD_FUTURE_WINDOW: premature signal discarded')
                else:
                    # A database failure intentionally escapes and stops dispatch.
                    # It must never acknowledge a signal that was not committed.
                    self.store.record_dwd_signal(start, message.data_type, received_at)
                    if DWD_TYPES <= self.store.get_dwd_types(start):
                        completed.add(start)
            try:
                self.source.ack(raw)
            except SignalSourceError:
                # Committed facts are idempotent; recovery can persist them again.
                logger.warning('DWD_REDIS_ACK_PENDING: durable signal will be replayed')
                break
        return sorted(completed)

    def reason(self, instance, now):
        if instance.task.aggregation_interval != 'PT5M':
            return None
        now = now.astimezone(timezone.utc)
        window_start = instance.data_start.astimezone(timezone.utc)
        missing = sorted(DWD_TYPES - self.store.get_dwd_types(instance.data_start))
        if not missing:
            self._warnings.pop(window_start, None)
            return None
        if now >= instance.data_end.astimezone(timezone.utc) + timedelta(seconds=self.wait_warning_seconds):
            previous = self._warnings.get(window_start)
            if previous is None or now >= previous + timedelta(seconds=self.wait_warning_seconds):
                self._warn_timeout(instance, now, missing)
                self._warnings[window_start] = now
                self._warnings.move_to_end(window_start)
                if len(self._warnings) > 1024:
                    self._warnings.popitem(last=False)
        return 'DWD_NOT_READY:' + ','.join(missing)

    def _warn_timeout(self, instance, now, missing):
        start = instance.data_start.isoformat()
        waited = int((now - instance.data_end.astimezone(timezone.utc)).total_seconds())
        received = sorted(DWD_TYPES - set(missing))
        logger.warning(
            'DWD_WAIT_TIMEOUT: window_start=%s window_end=%s waited_seconds=%s timeout_seconds=%s '
            'received=%s missing=%s; local readiness evidence is incomplete '
            '(does not prove upstream data is absent); execution remains blocked; '
            'diagnostic_args="--dwd-status --window \'%s\' --format json" '
            '(read-only; use the same service launcher and --config)',
            start, instance.data_end.isoformat(), waited, self.wait_warning_seconds,
            ','.join(received) or 'NONE', ','.join(missing), start,
        )

    def close(self):
        self.source.close()
