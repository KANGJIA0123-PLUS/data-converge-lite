"""Admit one lease holder and serialize access to local state before recovery."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager, ExitStack
import logging
import signal
import time

from data_converge.config.application import ApplicationDefinition
from data_converge.leader_election import LeadershipError
from data_converge.state import StateStore

from .factory import create_leadership
from .ports import WriterLease

logger = logging.getLogger(__name__)
LeaseFactory = Callable[[ApplicationDefinition], AbstractContextManager[WriterLease]]


class _RetryableAcquisition(Exception):
    """Internal marker: a failed acquisition must not fail the standby process."""


def _stop_waiting(_signum, _frame):
    raise SystemExit(0)


def _acquire(lease: WriterLease, *, wait: bool) -> bool:
    try:
        acquired = lease.acquire()
    except LeadershipError as error:
        if wait and str(error) == 'LEADERSHIP_REDIS_FAILED':
            raise _RetryableAcquisition from None
        raise
    if not acquired and not wait:
        raise LeadershipError('LEADERSHIP_BUSY')
    return acquired


@contextmanager
def _waiting_lease(definition: ApplicationDefinition,
                   factory: LeaseFactory) -> Iterator[WriterLease]:
    """Replace a lease poisoned by a transient failure before trying again.

    ``LeadershipLease.acquire`` marks itself lost when Redis is unreachable, even
    though no ownership was established. Retrying that instance would keep
    raising ``LEADERSHIP_LOST``, so each attempt owns a fresh lease/connection.
    """
    waiting, attempts = False, 0
    while True:
        try:
            with factory(definition) as lease:
                if _acquire(lease, wait=True):
                    yield lease
                    return
                if not waiting:
                    logger.info('LEADERSHIP_STANDBY: waiting for the active writer')
                    waiting = True
        except _RetryableAcquisition:
            attempts += 1
            waiting = True
            logger.warning('LEADERSHIP_REDIS_RETRY: attempt=%s retry_in_seconds=%s',
                           attempts, definition.leadership.standby_poll_seconds)
        time.sleep(definition.leadership.standby_poll_seconds)


@contextmanager
def admit_writer(definition: ApplicationDefinition, *, wait: bool = False,
                 factory: LeaseFactory = create_leadership) -> Iterator[WriterLease | None]:
    if not definition.leadership.enabled:
        yield None
        return
    with ExitStack() as cleanup:
        if wait:
            for signum in (signal.SIGTERM, signal.SIGINT):
                previous = signal.signal(signum, _stop_waiting)
                cleanup.callback(signal.signal, signum, previous)
            lease = cleanup.enter_context(_waiting_lease(definition, factory))
        else:
            lease = cleanup.enter_context(factory(definition))
            _acquire(lease, wait=False)
        lease.assert_owned()
        logger.info('LEADERSHIP_ACQUIRED')
        yield lease


def acquire_state(store: StateStore, *, bootstrap: bool, lease: WriterLease | None) -> bool:
    while True:
        if lease is not None:
            lease.assert_owned()
        try:
            if bootstrap:
                return store.acquire_startup_lock()
            store.acquire_lock()
            return False
        except RuntimeError as error:
            if not bootstrap or lease is None or not isinstance(error.__cause__, BlockingIOError):
                raise
            # The previous writer may still be draining submitted SQL. Its file
            # lock remains authoritative even after Redis ownership has expired.
            time.sleep(0.2)
