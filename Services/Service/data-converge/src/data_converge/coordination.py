"""Lease admission around the existing single-writer durable ledger."""
from contextlib import closing, contextmanager, ExitStack
import logging
import signal
import time
import uuid
from typing import Protocol

from data_converge.dwd_ready.redis_source import create_source
from data_converge.errors import TaskExecutionError
from data_converge.leader_election import LeadershipError, LeadershipLease

logger = logging.getLogger(__name__)


class WriterLease(Protocol):
    """Minimal ownership contract used by application and execution adapters."""

    def acquire(self) -> bool: ...
    def assert_owned(self) -> None: ...
    def bind_ledger(self, ledger_id: str, *, require_existing: bool = False) -> None: ...

    @property
    def lost(self) -> bool: ...


@contextmanager
def create_leadership(definition):
    # A separate source owns a separate pool. No BRPOP runs on this connection.
    policy = definition.leadership
    with closing(create_source(definition.config['dwd_ready'])) as source:
        elector = source.create_leader_elector(policy.cluster_name)
        with LeadershipLease(elector, ttl_seconds=policy.ttl_seconds,
                             renew_interval_seconds=policy.renew_interval_seconds) as lease:
            yield lease


def _stop_waiting(_signum, _frame):
    raise SystemExit(0)


@contextmanager
def admit_writer(definition, *, wait=False, factory=create_leadership):
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


def bind_writer(store, lease, cluster_name):
    if lease is None:
        if store.get_meta('leadership_cluster') is not None:
            raise LeadershipError('LEADERSHIP_REQUIRED_FOR_LEDGER')
        return
    lease.assert_owned()
    previous = store.get_meta('leadership_cluster')
    if previous is not None and previous != cluster_name:
        raise LeadershipError('LEADERSHIP_CLUSTER_CHANGED')
    identity = store.get_meta('leadership_ledger_id')
    if identity is None:
        identity = uuid.uuid4().hex
        store.set_meta('leadership_ledger_id', identity)
    lease.bind_ledger(identity, require_existing=previous is not None)
    lease.assert_owned()
    store.set_meta('leadership_cluster', cluster_name)


def acquire_state(store, *, bootstrap, lease):
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
            # A previous writer may still be draining submitted SQL after losing
            # its lease. Its file lock remains authoritative until it exits.
            time.sleep(0.2)


class GuardedRunner:
    def __init__(self, runner, lease):
        self.runner = runner
        self.lease = lease

    def __call__(self, instance):
        try:
            self.lease.assert_owned()
        except LeadershipError:
            raise TaskExecutionError('LEADERSHIP_LOST_BEFORE_SUBMIT', retryable=True,
                                     query_id=instance.query_id) from None
        self.runner(instance)


class GuardedSignalSource:
    def __init__(self, source, lease):
        self.source = source
        self.lease = lease

    def receive(self, block=True):
        self.lease.assert_owned()
        return self.source.receive(block)

    def ack(self, raw):
        self.lease.assert_owned()
        return self.source.ack(raw)

    def close(self):
        self.source.close()
