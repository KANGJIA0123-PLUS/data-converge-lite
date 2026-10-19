"""Atomic ownership operations at the Redis boundary."""

from unittest.mock import Mock, call, patch

import pytest

from data_converge.leader_election import LeaderElector


def test_acquire_uses_one_conditional_write_without_preflight_read():
    client = Mock()
    client.set.return_value = True
    with patch('redis.Redis', return_value=client):
        elector = LeaderElector('test_cluster')
    assert elector.elect_as_leader('session-token', ttl=30)
    client.get.assert_not_called()
    assert client.set.call_args.kwargs == {'nx': True, 'px': 30000}


@pytest.mark.parametrize('operation,result,expected', [
    ('renew_leadership', 1, True), ('renew_leadership', 0, False),
    ('release_leadership', 1, True), ('release_leadership', 0, False),
])
def test_old_owner_cannot_mutate_new_owner(operation, result, expected):
    client = Mock()
    client.eval.return_value = result
    elector = LeaderElector('test_cluster', client=client)
    assert getattr(elector, operation)('old-token') is expected
    assert client.eval.call_args.args[1:4] == (1, elector.leader_key, 'old-token')
    client.get.assert_not_called()
    client.expire.assert_not_called()
    client.delete.assert_not_called()


def test_ownership_lifecycle_only_addresses_owner_key():
    client = Mock()
    client.set.return_value = True
    client.get.return_value = 'token'
    client.ttl.return_value = 25
    client.eval.return_value = 1
    elector = LeaderElector('deployment', client=client)
    assert elector.elect_as_leader('token')
    assert elector.is_leader('token')
    assert elector.get_leader_ttl() == 25
    assert elector.renew_leadership('token')
    assert elector.release_leadership('token')
    owner_key = 'data_converge:leader:{deployment}'
    assert client.set.call_args == call(owner_key, 'token', nx=True, px=30000)
    assert client.get.call_args_list == [call(owner_key)]
    assert client.ttl.call_args_list == [call(owner_key)]
    assert [entry.args[1:] for entry in client.eval.call_args_list] == [
        (1, owner_key, 'token', 30000), (1, owner_key, 'token')]
    assert [entry[0] for entry in client.method_calls] == ['set', 'get', 'ttl', 'eval', 'eval']


def test_redis_error_is_fixed_and_does_not_expose_remote_details():
    from data_converge.leader_election import LeadershipError
    client = Mock()
    client.set.side_effect = RuntimeError('private Redis credentials')
    elector = LeaderElector('deployment', client=client)
    with pytest.raises(LeadershipError) as caught:
        elector.elect_as_leader('token')
    assert str(caught.value) == 'LEADERSHIP_REDIS_FAILED'


@pytest.mark.parametrize('cluster', ['', '{nested}', 'bad}name'])
def test_cluster_name_cannot_break_owner_key_slot(cluster):
    with pytest.raises(ValueError):
        LeaderElector(cluster, client=Mock())


def test_injected_byte_client_can_inspect_leadership():
    client = Mock()
    client.get.return_value = b'token'
    elector = LeaderElector('deployment', client=client)
    assert elector.is_leader('token')
