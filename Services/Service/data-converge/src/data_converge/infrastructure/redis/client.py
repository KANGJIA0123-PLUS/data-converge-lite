"""Construct and own dedicated Redis pools for queues, leases and diagnostics."""

from __future__ import annotations

import logging
import os

from .auth import make_connect_callback
from .credentials import load_sentinel_credentials, sentinel_identity
from .crypto import create_psm_codec

logger = logging.getLogger(__name__)


class RedisConnectionError(RuntimeError):
    """Fixed diagnostic code without SDK exception text or credentials."""


class RedisConnection:
    """One caller owns the data pool and all associated discovery pools."""

    def __init__(self, client, *, sentinel_clients=(), identity=None):
        self.client = client
        self.sentinel_clients = tuple(sentinel_clients)
        self.identity = identity
        self._closed = False

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for client in (self.client, *self.sentinel_clients):
            _close_client(client)


def _close_client(client) -> None:
    # Sentinel.master_for does not transfer its pool ownership to Redis in v5.
    # Disconnect explicitly even if close fails; cluster clients own their pools.
    try:
        client.close()
    except Exception:
        logger.warning('DWD_READY_REDIS_CLOSE_FAILED')
    try:
        pool = getattr(client, 'connection_pool', None)
        if pool is not None:
            pool.disconnect()
    except Exception:
        logger.warning('DWD_READY_REDIS_DISCONNECT_FAILED')


def _connection_options(connection: dict, major: int) -> dict:
    from redis.backoff import NoBackoff
    from redis.retry import Retry

    options = {name: connection[name] for name in ('ssl', 'socket_timeout', 'socket_connect_timeout')}
    # Queue consumers keep one exclusive socket; named pools (leader/status/log)
    # are sized from configuration above their maximum concurrent use.
    options.update(retry=Retry(NoBackoff(), 0), retry_on_error=[], decode_responses=False,
                   health_check_interval=0, max_connections=connection.get('max_connections', 1))
    if major == 5:
        options['retry_on_timeout'] = False
    if connection['ssl']:
        options.update(ssl_cert_reqs='required' if connection['ssl_verify'] else 'none',
                       ssl_check_hostname=connection['ssl_verify'])
    return options


def _sentinel_connection(connection: dict, options: dict) -> RedisConnection:
    from redis.sentinel import Sentinel

    failure_code = 'DWD_READY_REDIS_CREDENTIAL_FAILED'
    client = None
    sentinel_clients = ()
    try:
        credentials = load_sentinel_credentials(connection)
        failure_code = 'DWD_READY_REDIS_PSM_FAILED'
        codec = create_psm_codec(connection['root_key_file'], connection['work_key_file'])
        options.update(password=credentials.password, protocol=2, socket_keepalive=True)
        sentinel_options = dict(options, redis_connect_func=make_connect_callback(codec, select_db=False))
        data_options = dict(options, redis_connect_func=make_connect_callback(codec, select_db=True))
        failure_code = 'DWD_READY_REDIS_CREATE_FAILED'
        manager = Sentinel(credentials.sentinels, sentinel_kwargs=sentinel_options, **data_options)
        sentinel_clients = manager.sentinels
        client = manager.master_for(connection['service_name'], db=connection['db'])
        return RedisConnection(client, sentinel_clients=sentinel_clients,
                               identity=sentinel_identity(connection, credentials))
    except Exception:
        if client is not None:
            _close_client(client)
        for sentinel_client in sentinel_clients:
            _close_client(sentinel_client)
        raise RedisConnectionError(failure_code) from None


def _direct_connection(connection: dict, options: dict, major: int) -> RedisConnection:
    import redis

    password = os.environ.get(connection['password_env'])
    if not isinstance(password, str) or not password or '\0' in password:
        raise ValueError
    options.update(password=password, **{name: connection[name] for name in ('host', 'port', 'username')})
    if connection['mode'] != 'cluster':
        return RedisConnection(redis.Redis(db=connection['db'], **options))
    from redis.cluster import REDIS_ALLOWED_KEYS

    if connection['ssl'] and connection['ssl_verify'] and 'ssl_check_hostname' not in REDIS_ALLOWED_KEYS:
        raise RedisConnectionError('DWD_READY_REDIS_TLS_CLIENT_UNSUPPORTED')
    if major == 5:
        options['cluster_error_retry_attempts'] = 0
    client = redis.RedisCluster(**options)
    try:
        # MOVED/ASK retries are independent of connection retries. One attempt
        # lets the caller decide explicitly whether a later poll is safe.
        client.RedisClusterRequestTTL = 1
        return RedisConnection(client)
    except Exception:
        _close_client(client)
        raise


def create_connection(ready_config: dict) -> RedisConnection:
    """Load credentials on demand; each call owns independent, bounded pools."""
    try:
        if ready_config.get('enabled') is not True:
            raise ValueError
        import redis

        major = int(redis.__version__.split('.', 1)[0])
        if major not in (5, 6):
            raise ValueError
        connection = ready_config['redis']
        options = _connection_options(connection, major)
        if connection['mode'] == 'sentinel':
            return _sentinel_connection(connection, options)
        return _direct_connection(connection, options, major)
    except RedisConnectionError:
        raise
    except Exception:
        raise RedisConnectionError('DWD_READY_REDIS_CREATE_FAILED') from None
