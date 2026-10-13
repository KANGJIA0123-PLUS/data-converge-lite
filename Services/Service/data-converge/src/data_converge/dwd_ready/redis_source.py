"""Single-consumer Redis list transport; acknowledge only after durable storage."""

from __future__ import annotations

import os

from .psm_crypto import create_psm_codec
from .redis_auth import make_connect_callback
from .redis_credentials import load_sentinel_credentials, sentinel_identity


class RedisSignalError(RuntimeError):
    """Safe, fixed Redis diagnostic without SDK exception text or raw messages."""


class RedisSignalSource:
    def __init__(self, client, ready_config, *, sentinel_clients=(), identity=None):
        self._client = client
        self.queue_key = ready_config["queue_key"]
        self.processing_key = ready_config["processing_key"]
        self.block_seconds = ready_config["block_seconds"]
        self._closed = False
        self._sentinel_clients = tuple(sentinel_clients)
        self.identity = identity

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
            for client in (self._client, *self._sentinel_clients):
                _close_client(client)


def _close_client(client):
    # redis-py 5 Sentinel.master_for does not transfer pool ownership to Redis.
    # Explicitly disconnect our dedicated pool as well as closing the client.
    try:
        client.close()
    except Exception:
        pass
    try:
        client.connection_pool.disconnect()
    except Exception:
        pass


def bind_sentinel_identity(store, identity):
    if identity is None:
        return
    previous = store.get_meta('dwd_redis_sentinel_binding')
    if previous is not None and previous != identity:
        raise ValueError('DWD_REDIS_BINDING_CHANGED') from None
    if previous is None:
        store.set_meta('dwd_redis_sentinel_binding', identity)


def _connection_options(connection, major):
    from redis.backoff import NoBackoff
    from redis.retry import Retry

    options = {name: connection[name] for name in ("ssl", "socket_timeout", "socket_connect_timeout")}
    options.update(retry=Retry(NoBackoff(), 0),
                   retry_on_error=[], decode_responses=False, health_check_interval=0,
                   max_connections=1)
    if major == 5:
        options["retry_on_timeout"] = False
    if connection["ssl"]:
        options.update(ssl_cert_reqs="required" if connection["ssl_verify"] else "none",
                       ssl_check_hostname=connection["ssl_verify"])
    return options


def _add_direct_credentials(connection, options):
    password = os.environ.get(connection["password_env"])
    if not isinstance(password, str) or not password or '\0' in password:
        raise ValueError
    options.update(password=password, **{name: connection[name] for name in ("host", "port", "username")})


def _close_startup_clients(client, sentinel_clients):
    if client is not None:
        _close_client(client)
    for sentinel_client in sentinel_clients:
        _close_client(sentinel_client)


def create_source(ready_config: dict) -> RedisSignalSource:
    """Read credentials only here; disable implicit command retry in redis-py 5/6."""
    client = None
    sentinel_clients = ()
    failure_code = "DWD_READY_REDIS_CREATE_FAILED"
    try:
        if ready_config.get("enabled") is not True:
            raise ValueError
        import redis

        major = int(redis.__version__.split(".", 1)[0])
        if major not in (5, 6):
            raise ValueError
        connection = ready_config["redis"]
        options = _connection_options(connection, major)
        if connection["mode"] == "sentinel":
            from redis.sentinel import Sentinel

            failure_code = "DWD_READY_REDIS_CREDENTIAL_FAILED"
            credentials = load_sentinel_credentials(connection)
            failure_code = "DWD_READY_REDIS_PSM_FAILED"
            codec = create_psm_codec(connection['root_key_file'], connection['work_key_file'])
            options.update(password=credentials.password, protocol=2, socket_keepalive=True)
            sentinel_options = dict(options, redis_connect_func=make_connect_callback(codec, select_db=False))
            data_options = dict(options, redis_connect_func=make_connect_callback(codec, select_db=True))
            failure_code = "DWD_READY_REDIS_CREATE_FAILED"
            manager = Sentinel(credentials.sentinels, sentinel_kwargs=sentinel_options, **data_options)
            sentinel_clients = manager.sentinels
            client = manager.master_for(connection['service_name'], db=connection['db'])
            return RedisSignalSource(client, ready_config, sentinel_clients=sentinel_clients,
                                     identity=sentinel_identity(connection, credentials))
        _add_direct_credentials(connection, options)
        if connection["mode"] == "cluster":
            from redis.cluster import REDIS_ALLOWED_KEYS
            if connection["ssl"] and connection["ssl_verify"] and "ssl_check_hostname" not in REDIS_ALLOWED_KEYS:
                failure_code = "DWD_READY_REDIS_TLS_CLIENT_UNSUPPORTED"
                raise ValueError
            # Redis 5 has a separate cluster retry counter; Redis 6 uses Retry.
            if major == 5:
                options["cluster_error_retry_attempts"] = 0
            client = redis.RedisCluster(**options)
            # MOVED/ASK has a separate loop even when connection retries are zero.
            # A later explicit poll may recover after topology refresh; do not loop.
            client.RedisClusterRequestTTL = 1
        else:
            client = redis.Redis(db=connection["db"], **options)
        return RedisSignalSource(client, ready_config)
    except Exception:
        _close_startup_clients(client, sentinel_clients)
        raise RedisSignalError(failure_code) from None
