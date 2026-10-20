"""Single-consumer Redis list transport; acknowledge only after durable storage."""

from __future__ import annotations

import logging

from data_converge.infrastructure.redis import (
    RedisConnection, RedisConnectionError, create_connection,
)

from .ports import SignalSourceError

logger = logging.getLogger(__name__)


class RedisSignalError(SignalSourceError):
    """Safe, fixed Redis diagnostic without SDK exception text or raw messages."""


class RedisSignalSource:
    def __init__(self, client, ready_config, *, sentinel_clients=(), identity=None, connection=None):
        self._connection = connection if connection is not None else RedisConnection(
            client, sentinel_clients=sentinel_clients, identity=identity)
        self._client = self._connection.client
        self.queue_key = ready_config["queue_key"]
        self.processing_key = ready_config["processing_key"]
        self.block_seconds = ready_config["block_seconds"]
        self._closed = False
        self._sentinel_clients = self._connection.sentinel_clients
        self.identity = self._connection.identity

    def create_leader_elector(self, cluster_name):
        """Compatibility adapter; production leadership owns a RedisConnection directly."""
        from data_converge.leader_election import LeaderElector

        if self._closed:
            raise RedisSignalError('DWD_READY_REDIS_CLOSED')
        return LeaderElector(cluster_name, client=self._client)

    def inspect(self) -> dict:
        """Read connectivity and list metadata without fetching or moving messages."""
        if self._closed:
            raise RedisSignalError("DWD_READY_REDIS_CLOSED") from None
        result = {'ping': self._client.ping() is True}
        known_types = ('none', 'list', 'string', 'hash', 'set', 'zset', 'stream')
        for name, key in (('queue', self.queue_key), ('processing', self.processing_key)):
            kind = self._client.type(key)
            if isinstance(kind, bytes):
                kind = kind.decode('ascii', errors='replace')
            kind = kind if kind in known_types else 'unknown'
            length = 0 if kind == 'none' else None
            if kind == 'list':
                length = self._client.llen(key)
                if type(length) is not int or length < 0:
                    raise RedisSignalError("DWD_READY_REDIS_INSPECT_FAILED") from None
            result[name] = {'key': key, 'type': kind, 'length': length}
        return result

    def receive(self, block=True) -> str | bytes | None:
        if self._closed:
            raise RedisSignalError("DWD_READY_REDIS_CLOSED") from None
        try:
            raw = self._client.lindex(self.processing_key, -1)
            if raw is None:
                if block:
                    raw = self._client.brpoplpush(self.queue_key, self.processing_key, timeout=self.block_seconds)
                else:
                    raw = self._client.rpoplpush(self.queue_key, self.processing_key)
            if raw is not None and not isinstance(raw, (str, bytes)):
                raise ValueError
            return raw
        except Exception:
            raise RedisSignalError("DWD_READY_REDIS_RECEIVE_FAILED") from None

    def ack(self, raw: str | bytes) -> int:
        if self._closed:
            raise RedisSignalError("DWD_READY_REDIS_CLOSED") from None
        try:
            if not isinstance(raw, (str, bytes)):
                raise ValueError
            result = self._client.lrem(self.processing_key, 1, raw)
            if type(result) is not int or result not in (0, 1):
                raise ValueError
            return result
        except Exception:
            raise RedisSignalError("DWD_READY_REDIS_ACK_FAILED") from None

    def close(self):
        if not self._closed:
            self._closed = True
            self._connection.close()


def bind_sentinel_identity(store, identity):
    if identity is None:
        return
    previous = store.get_meta('dwd_redis_sentinel_binding')
    if previous is not None and previous != identity:
        logger.warning('DWD_REDIS_BINDING_CHECK_DISABLED: issue=DC-STATE-002 reason=BINDING_MISMATCH')
    if previous is None:
        store.set_meta('dwd_redis_sentinel_binding', identity)


def create_source(ready_config: dict) -> RedisSignalSource:
    """Adapt an owned Redis connection to the durable readiness-list protocol."""
    connection = None
    try:
        connection = create_connection(ready_config)
        return RedisSignalSource(connection.client, ready_config, connection=connection)
    except RedisConnectionError as error:
        raise RedisSignalError(str(error)) from None
    except Exception:
        if connection is not None:
            connection.close()
        raise RedisSignalError('DWD_READY_REDIS_CREATE_FAILED') from None
