"""Compose a lease with a dedicated Redis connection, independent of DWD queues."""

from collections.abc import Iterator
from contextlib import closing, contextmanager

from data_converge.config.application import ApplicationDefinition
from data_converge.infrastructure.redis import create_connection
from data_converge.leader_election import LeaderElector, LeadershipLease


@contextmanager
def create_leadership(definition: ApplicationDefinition) -> Iterator[LeadershipLease]:
    """Stop renewal and release ownership before disconnecting the owned pool."""
    policy = definition.leadership
    with closing(create_connection(definition.config['dwd_ready'])) as connection:
        elector = LeaderElector(policy.cluster_name, client=connection.client)
        with LeadershipLease(elector, ttl_seconds=policy.ttl_seconds,
                             renew_interval_seconds=policy.renew_interval_seconds) as lease:
            yield lease
