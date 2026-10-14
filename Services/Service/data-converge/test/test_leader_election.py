"""Atomic ownership operations and ledger identity checks at the Redis boundary."""

from unittest.mock import Mock, patch

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


@pytest.mark.parametrize('result,code', [
    (0, None), (1, None), (-1, 'LEADERSHIP_LEDGER_MISMATCH'),
    (-2, 'LEADERSHIP_REGISTRY_MISSING'),
])
def test_binding_checks_ownership_and_existing_ledger(result, code):
    from data_converge.leader_election import LeadershipError
    client = Mock()
    client.eval.return_value = result
    elector = LeaderElector('deployment', client=client)
    if code:
        with pytest.raises(LeadershipError, match=code):
            elector.bind_ledger('token', 'ledger-uuid', require_existing=True)
    else:
        assert elector.bind_ledger('token', 'ledger-uuid', require_existing=True) is bool(result)
    assert client.eval.call_args.args[1:] == (
        2, 'data_converge:leader:{deployment}',
        'data_converge:leader:{deployment}:ledger', 'token', 'ledger-uuid', '1')


def test_redis_error_is_fixed_and_does_not_expose_remote_details():
    from data_converge.leader_election import LeadershipError
    client = Mock()
    client.set.side_effect = RuntimeError('private Redis credentials')
    elector = LeaderElector('deployment', client=client)
    with pytest.raises(LeadershipError) as caught:
        elector.elect_as_leader('token')
    assert str(caught.value) == 'LEADERSHIP_REDIS_FAILED'


@pytest.mark.parametrize('cluster', ['', '{nested}', 'bad}name'])
def test_cluster_name_cannot_break_ledger_key_slot(cluster):
    with pytest.raises(ValueError):
        LeaderElector(cluster, client=Mock())


def test_injected_byte_client_can_inspect_leadership():
    client = Mock()
    client.get.return_value = b'token'
    elector = LeaderElector('deployment', client=client)
    assert elector.is_leader('token')
