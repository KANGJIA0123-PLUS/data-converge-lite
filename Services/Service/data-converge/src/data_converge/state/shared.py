"""Attach fenced Redis state to a ledger for the lifetime of one leadership term."""

from __future__ import annotations

from contextlib import ExitStack, closing, contextmanager
import logging

from data_converge.infrastructure.redis import create_connection
from data_converge.infrastructure.redis.pools import RedisPools

from .redis_mirror import RedisStateMirror, RedisStateSettings

logger = logging.getLogger(__name__)


def shared_state_enabled(definition) -> bool:
    settings = RedisStateSettings.from_mapping(definition.config.get('redis_state'))
    return (settings.enabled and definition.leadership.enabled
            and definition.config['dwd_ready'].get('enabled') is True)


@contextmanager
def attach_shared_state(definition, store, lease, *, connection_factory=create_connection,
                        killer=None):
    """Import the previous leader's state, then mirror every write while yielding.

    ``killer(query_ids)`` cancels SQL the previous leader left running so the
    idempotent rerun does not race with it (best effort; failures are logged).
    """
    # Leases without a fencing token (tests, legacy adapters) cannot fence writes.
    if (lease is None or not shared_state_enabled(definition)
            or not hasattr(type(lease), 'token') or not hasattr(type(lease), 'leader_key')):
        yield None
        return
    settings = RedisStateSettings.from_mapping(definition.config.get('redis_state'))
    pools = RedisPools.from_mapping(definition.config.get('redis_pools'))
    ready = definition.config['dwd_ready']
    with ExitStack() as stack:
        writer = stack.enter_context(closing(connection_factory(pools.connection_config(ready, 'log'))))
        reader = stack.enter_context(closing(connection_factory(pools.connection_config(ready, 'status'))))
        mirror = RedisStateMirror(
            writer.client, reader.client, cluster=definition.leadership.cluster_name,
            leader_key=lease.leader_key, token_source=lambda: lease.token, settings=settings)
        intervals = {name: task.aggregation_interval for name, task in definition.dag.tasks.items()}
        store.attach_mirror(mirror, intervals)
        try:
            _take_over(store, mirror, killer)
            yield mirror
        finally:
            store.attach_mirror(None, {})


def _take_over(store, mirror, killer) -> None:
    initial = _running_query_ids(mirror)
    _kill_previous(initial, killer)
    if not mirror.read_cursors():
        seeded = store.publish_local_state()
        flushed = store.flush_pending()
        logger.info('REDIS_STATE_SEEDED: instances=%s pending_flushed=%s', seeded, flushed)
        return
    seeded = store.publish_missing_local_state()
    counts = store.import_shared_state()
    imported = _merge_query_ids([], counts.pop('interrupted_query_ids', []))
    interrupted = _merge_query_ids(initial, imported)
    logger.info('REDIS_STATE_IMPORTED: %s missing_seeded=%s interrupted=%s',
                counts, seeded, len(interrupted))
    # 导入扫描可能发现未出现在 running 集合中的旧 query，补杀这些新增 ID。
    _kill_previous([query for query in imported if query not in initial], killer)
    flushed = store.flush_pending()
    if flushed:
        logger.info('REDIS_STATE_PENDING_FLUSHED: instances=%s', flushed)


def _running_query_ids(mirror) -> list[str]:
    """Read query ids before local state can replace shared RUNNING rows."""
    if not hasattr(mirror, 'running_instances'):
        return []
    return _merge_query_ids([], [row.get('query_id') for row in mirror.running_instances()])


def _merge_query_ids(left: list[str], right: list[str]) -> list[str]:
    return list(dict.fromkeys(query for query in (*left, *right) if query))


def _kill_previous(query_ids: list[str], killer) -> None:
    if not query_ids or killer is None:
        return
    try:
        killer(query_ids)
    except Exception:
        logger.warning('REDIS_STATE_KILL_PREVIOUS_FAILED: count=%s', len(query_ids))
