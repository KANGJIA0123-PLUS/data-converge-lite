"""Shared Redis resources stay independent of readiness-list consumers."""

from contextlib import closing

import pytest

from data_converge.dwd_ready.config import load_ready_config


def connection_config(tmp_path):
    return load_ready_config({'dwd_ready': {'enabled': True, 'redis': {
        'mode': 'standalone', 'host': 'localhost', 'port': 6379,
        'db': 5, 'ssl': False, 'password_env': 'SYNTHETIC_REDIS_PASSWORD',
    }}}, tmp_path)


def test_connection_factory_needs_no_queue_fields_and_owns_independent_pools(tmp_path, monkeypatch):
    from data_converge.infrastructure.redis import create_connection

    config = connection_config(tmp_path)
    config = {'enabled': True, 'redis': config['redis']}
    monkeypatch.setenv('SYNTHETIC_REDIS_PASSWORD', 'synthetic')
    with closing(create_connection(config)) as first, closing(create_connection(config)) as second:
        assert first.client.connection_pool is not second.client.connection_pool
        options = first.client.connection_pool.connection_kwargs
        assert options['db'] == 5 and options['retry']._retries == 0
        assert not first.client.connection_pool._in_use_connections
        first.close()
        assert second.client.connection_pool.connection_kwargs['db'] == 5


def test_source_construction_failure_closes_transferred_connection(tmp_path, monkeypatch):
    from data_converge.infrastructure.redis import RedisConnection
    from data_converge.dwd_ready import redis_source

    events = []
    class Client:
        def close(self):
            events.append('closed')
    connection = RedisConnection(Client())
    monkeypatch.setattr(redis_source, 'create_connection', lambda _: connection)
    config = connection_config(tmp_path)
    del config['queue_key']
    with pytest.raises(redis_source.RedisSignalError, match='DWD_READY_REDIS_CREATE_FAILED'):
        redis_source.create_source(config)
    connection.close()
    assert events == ['closed']


def test_sentinel_client_creation_failure_releases_discovery_connections(tmp_path, monkeypatch):
    from data_converge.infrastructure.redis import client as transport
    from data_converge.infrastructure.redis.credentials import SentinelCredentials
    import redis.sentinel

    events = []
    class Client:
        def close(self):
            events.append('closed')
    class Sentinel:
        sentinels = (Client(),)
        def __init__(self, *_args, **_kwargs):
            pass
        def master_for(self, *_args, **_kwargs):
            raise RuntimeError('private endpoint and credential')
    config = {'enabled': True, 'redis': {
        'mode': 'sentinel', 'service_name': 'test', 'db': 5,
        'ssl': False, 'socket_timeout': 3, 'socket_connect_timeout': 2,
        'root_key_file': 'root', 'work_key_file': 'shared',
    }}
    monkeypatch.setattr(redis.sentinel, 'Sentinel', Sentinel)
    monkeypatch.setattr(transport, 'load_sentinel_credentials',
                        lambda _: SentinelCredentials((('localhost', 26379),), 'synthetic'))
    monkeypatch.setattr(transport, 'create_psm_codec', lambda *_: object())
    with pytest.raises(transport.RedisConnectionError) as caught:
        transport.create_connection(config)
    assert str(caught.value) == 'DWD_READY_REDIS_CREATE_FAILED'
    assert caught.value.__suppress_context__ and events == ['closed']
