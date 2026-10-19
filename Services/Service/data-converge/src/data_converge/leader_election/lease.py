"""Conservative local admission deadline and irreversible Redis lease loss."""

from __future__ import annotations

import logging
import math
import threading
import time
import uuid
from typing import Callable

from .elector import LeaderElector, LeadershipError, ttl_milliseconds

logger = logging.getLogger(__name__)


class LeadershipLease:
    """Own one renewal thread; the caller closes its dedicated Redis client last.

    A lease gates new work only. The persistent local file lock must stay held
    until in-flight SQL has drained, including after lease loss.
    """

    def __init__(self, elector: LeaderElector, ttl_seconds: float = 30,
                 renew_interval_seconds: float = 5, *, clock: Callable[[], float] = time.monotonic):
        ttl_milliseconds(ttl_seconds)
        validity = ttl_seconds - (ttl_seconds * 0.01 + 0.002)
        if (type(renew_interval_seconds) not in (int, float)
                or not math.isfinite(renew_interval_seconds)
                or not 0 < renew_interval_seconds < validity):
            raise ValueError('LEADERSHIP_SETTINGS_INVALID')
        self.elector = elector
        self.ttl_seconds = ttl_seconds
        self.renew_interval_seconds = renew_interval_seconds
        self._validity = validity
        self._clock = clock
        self._state_lock = threading.Lock()
        self._command_lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = None
        self._token = None
        self._deadline = 0.0
        self._acquired = False
        self._lost = False
        self._closed = False

    def _mark_lost(self) -> None:
        with self._state_lock:
            self._lost = True
            self._stop.set()

    def _expired_locked(self) -> bool:
        if self._acquired and self._clock() >= self._deadline:
            self._lost = True
            self._stop.set()
        return self._lost or self._closed

    @property
    def lost(self) -> bool:
        with self._state_lock:
            return self._expired_locked()

    def assert_owned(self) -> None:
        """Never perform Redis I/O or wait for a blocked renewal command here."""
        with self._state_lock:
            if self._expired_locked() or not self._acquired:
                raise LeadershipError('LEADERSHIP_LOST')

    def acquire(self) -> bool:
        with self._command_lock:
            with self._state_lock:
                if self._expired_locked():
                    raise LeadershipError('LEADERSHIP_LOST')
                if self._acquired:
                    raise LeadershipError('LEADERSHIP_ALREADY_ACQUIRED')
                self._token = uuid.uuid4().hex
            started = self._clock()
            try:
                acquired = self.elector.elect_as_leader(self._token, ttl=self.ttl_seconds)
            except Exception:
                self._mark_lost()
                raise LeadershipError('LEADERSHIP_REDIS_FAILED') from None
            if not acquired:
                return False
            self._start_renewal(started)
            return True

    def _start_renewal(self, started: float) -> None:
        with self._state_lock:
            self._deadline = started + self._validity
            self._acquired = True
            if self._expired_locked():
                raise LeadershipError('LEADERSHIP_LOST')
            self._thread = threading.Thread(target=self._run, name='leader-renewal', daemon=True)
            try:
                self._thread.start()
            except Exception:
                self._lost = True
                self._stop.set()
                raise LeadershipError('LEADERSHIP_RENEWAL_START_FAILED') from None

    def _run(self) -> None:
        while not self._stop.wait(self.renew_interval_seconds):
            try:
                self._renew_once()
            except Exception:
                self._mark_lost()
                return

    def _renew_once(self) -> None:
        with self._command_lock:
            self.assert_owned()
            started = self._clock()
            renewed = self.elector.renew_leadership(self._token, ttl=self.ttl_seconds)
            with self._state_lock:
                # A response after the old deadline cannot restore ownership.
                if self._expired_locked() or not renewed or self._clock() >= started + self._validity:
                    self._lost = True
                    self._stop.set()
                else:
                    self._deadline = started + self._validity

    def bind_ledger(self, ledger_id: str, *, require_existing: bool = False) -> None:
        self._ledger_command(lambda: self.elector.bind_ledger(
            self._token, ledger_id, require_existing=require_existing))

    def check_ledger(self, ledger_id: str | None, *, require_existing: bool = False) -> None:
        """Read-only preflight; normal binding still owns registration writes."""
        self._ledger_command(lambda: self.elector.check_ledger(
            self._token, ledger_id, require_existing=require_existing))

    def _ledger_command(self, operation: Callable[[], bool]) -> None:
        with self._command_lock:
            self.assert_owned()
            try:
                valid = operation()
                self.assert_owned()
                if not valid:
                    raise LeadershipError('LEADERSHIP_LOST')
            except LeadershipError:
                self._mark_lost()
                raise
            except Exception:
                self._mark_lost()
                raise LeadershipError('LEADERSHIP_REDIS_FAILED') from None

    def close(self) -> None:
        with self._state_lock:
            if self._closed:
                return
            self._closed = True
            self._stop.set()
        if self._thread is not None and self._thread.ident is not None:
            self._thread.join()
        with self._command_lock:
            if self._token is not None:
                try:
                    self.elector.release_leadership(self._token)
                except Exception:
                    # The bounded lease expires; cleanup never replaces a prior error.
                    logger.warning('LEADERSHIP_RELEASE_FAILED')

    def __enter__(self) -> LeadershipLease:
        return self

    def __exit__(self, *_):
        self.close()
