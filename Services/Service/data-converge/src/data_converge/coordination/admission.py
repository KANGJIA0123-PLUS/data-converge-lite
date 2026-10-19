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


def _stop_waiting(_signum, _frame):
    raise SystemExit(0)


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
        lease = cleanup.enter_context(factory(definition))
        waiting = False
        while not lease.acquire():
            if not wait:
                raise LeadershipError('LEADERSHIP_BUSY')
            if not waiting:
                logger.info('LEADERSHIP_STANDBY: waiting for the active writer')
                waiting = True
            time.sleep(definition.leadership.standby_poll_seconds)
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
