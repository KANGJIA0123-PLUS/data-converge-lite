"""Decorate existing adapters with lease checks immediately before side effects."""

from data_converge.dag.models import TaskInstance
from data_converge.dag.ports import TaskRunner
from data_converge.dwd_ready.ports import ReadySignalSource
from data_converge.errors import TaskExecutionError
from data_converge.leader_election import LeadershipError

from .ports import WriterLease


class GuardedRunner:
    """Reject unsubmitted work after lease loss; preserve runner error semantics."""

    def __init__(self, runner: TaskRunner, lease: WriterLease):
        self.runner = runner
        self.lease = lease

    def __call__(self, instance: TaskInstance) -> None:
        try:
            self.lease.assert_owned()
        except LeadershipError:
            raise TaskExecutionError('LEADERSHIP_LOST_BEFORE_SUBMIT', retryable=True,
                                     query_id=instance.query_id) from None
        self.runner(instance)


class GuardedSignalSource:
    """Guard receive and acknowledgment; keep the source's durability contract."""

    def __init__(self, source: ReadySignalSource, lease: WriterLease):
        self.source = source
        self.lease = lease

    def receive(self, block: bool = True) -> str | bytes | None:
        self.lease.assert_owned()
        return self.source.receive(block)

    def ack(self, raw: str | bytes) -> int | None:
        self.lease.assert_owned()
        return self.source.ack(raw)

    def close(self) -> None:
        self.source.close()
