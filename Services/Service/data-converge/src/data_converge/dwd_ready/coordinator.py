"""Persist producer readiness before acknowledging, and gate all five-minute SQL."""

from __future__ import annotations

import logging
import time
from collections import OrderedDict
from datetime import timedelta

from .message import DWD_TYPES, ReadyMessageError, parse_message
from .redis_source import RedisSignalError


logger = logging.getLogger(__name__)


class ReadyCoordinator:
    """Runs on the scheduler's thread, which owns the SQLite writer connection.

    Signals prove source readiness only. The existing SQL executor is still the
    sole authority for SUCCESS, retry, UNKNOWN and duplicate suppression.
    """

    def __init__(self, store, source, coverage_start, *, batch_size=100,
                 wait_warning_seconds=60, should_stop=None):
        self.store = store
        self.source = source
        self.coverage_start = coverage_start
        self.batch_size = batch_size
        self.wait_warning_seconds = wait_warning_seconds
        self.should_stop = should_stop or (lambda: False)
        self._warnings = OrderedDict()

    def poll(self, now):
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
            except RedisSignalError:
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
            except RedisSignalError:
                # Committed facts are idempotent; recovery can persist them again.
                logger.warning('DWD_REDIS_ACK_PENDING: durable signal will be replayed')
                break
        return sorted(completed)

    def reason(self, instance, now):
        if instance.task.aggregation_interval != 'PT5M':
            return None
        missing = sorted(DWD_TYPES - self.store.get_dwd_types(instance.data_start))
        if not missing:
            self._warnings.pop(instance.data_start, None)
            return None
        if now >= instance.data_end + timedelta(seconds=self.wait_warning_seconds):
            previous = self._warnings.get(instance.data_start)
            if previous is None or now >= previous + timedelta(seconds=self.wait_warning_seconds):
                logger.warning('DWD_WAIT_TIMEOUT: window=%s missing=%s; execution remains blocked',
                               instance.data_start.isoformat(), ','.join(missing))
                self._warnings[instance.data_start] = now
                self._warnings.move_to_end(instance.data_start)
                if len(self._warnings) > 1024:
                    self._warnings.popitem(last=False)
        return 'DWD_NOT_READY:' + ','.join(missing)

    def close(self):
        self.source.close()
