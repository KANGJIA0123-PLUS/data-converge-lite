"""Explicit online diagnostics read metadata without consuming or leaking data."""

import json
from types import SimpleNamespace

import pytest

from data_converge.dwd_ready.redis_source import RedisSignalError, RedisSignalSource
from data_converge.config.leadership import LeadershipSettings


class ReadOnlyRedis:
    def __init__(self, *, types=None, failure=None):
        self.types = types or {'ready': b'list', 'processing': b'none'}
        self.failure = failure
        self.commands = []
        self.messages = [b'private message body']
        self.closed = False

    def ping(self):
        self.commands.append(('PING',))
        if self.failure:
            raise self.failure
        return True

    def type(self, key):
        self.commands.append(('TYPE', key))
        return self.types[key]

    def llen(self, key):
        self.commands.append(('LLEN', key))
        return len(self.messages) if key == 'ready' else 0

    def close(self):
        self.closed = True


def ready_config(enabled=True):
    return {'enabled': enabled, 'queue_key': 'ready', 'processing_key': 'processing',
            'block_seconds': 1, 'redis': {'mode': 'sentinel', 'db': 5,
                                        'username': 'configured-account', 'ssl': True,
                                        'ssl_verify': True, 'service_name': 'redis-cluster'}}


def definition(enabled=True):
    return SimpleNamespace(leadership=LeadershipSettings(), config={
        'clickhouse': {'host': 'localhost', 'port': 8123, 'database': 'metrics',
                       'username': 'reader', 'password': 'private password',
                       'secure': False, 'verify': True},
        'dwd_ready': ready_config(enabled),
    })


def install_connections(monkeypatch, client):
    from data_converge.diagnostics import connections
    source = RedisSignalSource(client, ready_config())
    monkeypatch.setattr(connections.redis_source, 'create_source', lambda _: source)
    monkeypatch.setattr(connections.sql_client, 'execute_sql', lambda *a, **kw: [[1]])
    return connections, source


def test_inspect_reads_only_types_lengths_and_never_message_bodies():
    client = ReadOnlyRedis()
    report = RedisSignalSource(client, ready_config()).inspect()
    assert report == {'ping': True, 'queue': {'key': 'ready', 'type': 'list', 'length': 1},
                      'processing': {'key': 'processing', 'type': 'none', 'length': 0}}
    assert client.messages == [b'private message body']
    assert client.commands == [('PING',), ('TYPE', 'ready'), ('LLEN', 'ready'),
                               ('TYPE', 'processing')]
    assert 'private' not in json.dumps(report)


def test_inspect_reports_both_key_types_without_llen_on_wrong_type():
    client = ReadOnlyRedis(types={'ready': b'string', 'processing': b'list'})
    report = RedisSignalSource(client, ready_config()).inspect()
    assert report['queue'] == {'key': 'ready', 'type': 'string', 'length': None}
    assert report['processing'] == {'key': 'processing', 'type': 'list', 'length': 0}
    assert ('LLEN', 'ready') not in client.commands
    assert ('LLEN', 'processing') in client.commands


def test_inspect_rejects_closed_source_without_network():
    client = ReadOnlyRedis()
    source = RedisSignalSource(client, ready_config())
    source.close()
    with pytest.raises(RedisSignalError, match='DWD_READY_REDIS_CLOSED'):
        source.inspect()
    assert client.commands == []


def test_connections_close_redis_and_only_submit_bounded_select(monkeypatch):
    client = ReadOnlyRedis()
    connections, _ = install_connections(monkeypatch, client)
    requests = []

    def execute(config, sql, timeout):
        requests.append((sql, timeout, config['clickhouse']['password']))
        return [[1]]

    monkeypatch.setattr(connections.sql_client, 'execute_sql', execute)
    results = connections.check_connections(definition())
    assert all(item['status'] == 'PASS' for item in results)
    assert len(results) == 4
    assert requests == [('SELECT 1', 10, 'private password')]
    assert client.closed and client.messages == [b'private message body']
    assert all(set(item) == {'id', 'status', 'summary', 'evidence', 'action'} for item in results)
    assert 'private' not in json.dumps(results)
    assert results[-1]['evidence']['credential_user'] == 'configured-account'


def test_clickhouse_credential_failure_does_not_block_redis(monkeypatch):
    client = ReadOnlyRedis()
    connections, _ = install_connections(monkeypatch, client)
    config = definition()
    del config.config['clickhouse']['password']

    def fail_password(_):
        raise ValueError('private password and SQL')

    monkeypatch.setattr(connections.sql_config, 'load_password', fail_password)
    results = connections.check_connections(config)
    assert [item['status'] for item in results] == ['FAIL', 'SKIP', 'PASS', 'PASS']
    assert client.closed and ('PING',) in client.commands
    assert 'private' not in json.dumps(results)


def test_clickhouse_query_failure_does_not_block_redis(monkeypatch):
    client = ReadOnlyRedis()
    connections, _ = install_connections(monkeypatch, client)

    def fail_query(*_, **__):
        raise ConnectionRefusedError(61, 'private remote SQL')

    monkeypatch.setattr(connections.sql_client, 'execute_sql', fail_query)
    results = connections.check_connections(definition())
    assert [item['status'] for item in results] == ['PASS', 'FAIL', 'PASS', 'PASS']
    assert results[1]['evidence']['os_errno'] == 61
    assert client.closed and 'private' not in json.dumps(results)


def test_redis_failure_is_redacted_and_client_always_closed(monkeypatch):
    client = ReadOnlyRedis(failure=RuntimeError('private password and message body'))
    connections, _ = install_connections(monkeypatch, client)
    results = connections.check_connections(definition())
    assert [item['status'] for item in results] == ['PASS', 'PASS', 'PASS', 'FAIL']
    assert results[-1]['evidence']['error_type'] == 'RuntimeError'
    assert client.closed and 'private' not in json.dumps(results)


def test_redis_creation_failure_skips_inspection(monkeypatch):
    client = ReadOnlyRedis()
    connections, _ = install_connections(monkeypatch, client)

    def fail_source(_):
        raise RedisSignalError('DWD_READY_REDIS_PSM_FAILED')

    monkeypatch.setattr(connections.redis_source, 'create_source', fail_source)
    results = connections.check_connections(definition())
    assert [item['status'] for item in results] == ['PASS', 'PASS', 'FAIL', 'SKIP']
    assert results[-2]['evidence']['error_code'] == 'DWD_READY_REDIS_PSM_FAILED'
    assert client.commands == []


def test_wrong_redis_key_type_is_failure_and_disabled_redis_is_skipped(monkeypatch):
    client = ReadOnlyRedis(types={'ready': b'string', 'processing': b'none'})
    connections, _ = install_connections(monkeypatch, client)
    results = connections.check_connections(definition())
    assert results[-1]['status'] == 'FAIL'
    assert results[-1]['evidence']['queue']['type'] == 'string'
    client.commands.clear()
    disabled = connections.check_connections(definition(False))
    assert [item['status'] for item in disabled] == ['PASS', 'PASS', 'SKIP', 'SKIP']
    assert client.commands == []


def test_clickhouse_unexpected_success_body_is_failure(monkeypatch):
    client = ReadOnlyRedis()
    connections, _ = install_connections(monkeypatch, client)
    monkeypatch.setattr(connections.sql_client, 'execute_sql', lambda *a, **kw: [])
    results = connections.check_connections(definition())
    assert results[1]['status'] == 'FAIL'
    assert results[-1]['status'] == 'PASS'


def test_unexpected_component_failure_still_checks_the_other_component(monkeypatch):
    client = ReadOnlyRedis()
    connections, _ = install_connections(monkeypatch, client)

    def fail_endpoint(_):
        raise RuntimeError('private endpoint diagnostics')

    monkeypatch.setattr(connections.sql_config, 'connection_origin', fail_endpoint)
    results = connections.check_connections(definition())
    assert results[0]['status'] == 'FAIL'
    assert [item['status'] for item in results[-2:]] == ['PASS', 'PASS']
    assert client.closed and 'private' not in json.dumps(results)


class ReadOnlyLeaderRedis(ReadOnlyRedis):
    def get(self, key):
        self.commands.append(('GET', key))
        return b'private lease token'

    def ttl(self, key):
        self.commands.append(('TTL', key))
        return 20

    def exists(self, key):
        self.commands.append(('EXISTS', key))
        return 1


def test_online_leadership_diagnostic_reads_without_claim_or_renew(monkeypatch):
    from data_converge.diagnostics import connections
    client = ReadOnlyLeaderRedis()
    monkeypatch.setattr(connections.redis_source, 'create_source',
                        lambda _: RedisSignalSource(client, ready_config()))
    value = definition()
    value.leadership = LeadershipSettings(enabled=True)
    result = connections._leadership_check(value)
    assert result['status'] == 'PASS'
    assert [command[0] for command in client.commands] == ['GET', 'TTL', 'EXISTS']
    assert client.closed and 'private' not in json.dumps(result)
