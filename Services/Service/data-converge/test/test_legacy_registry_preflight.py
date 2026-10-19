"""Read-only registration admission before a legacy recovery may change SQLite."""

import re
from unittest.mock import Mock

import pytest

from data_converge.leader_election import LeaderElector, LeadershipError, LeadershipLease


@pytest.mark.parametrize('identity,required,argument', [
    (None, False, ''), ('existing-ledger', False, 'existing-ledger'),
    ('existing-ledger', True, 'existing-ledger'),
])
def test_check_passes_optional_identity_and_requirement_in_one_read_only_command(identity, required, argument):
    client = Mock()
    client.eval.return_value = 1
    elector = LeaderElector('deployment', client=client)
    assert elector.check_ledger('owner', identity, require_existing=required)
    script, *arguments = client.eval.call_args.args
    assert arguments == [2, 'data_converge:leader:{deployment}',
                         'data_converge:leader:{deployment}:ledger', 'owner', argument, str(int(required))]
    # The atomic server operation must not register, expire, delete or renew either key.
    assert set(re.findall(r"redis\.call\(['\"]([^'\"]+)", script)) == {'GET'}
    assert [call[0] for call in client.method_calls] == ['eval']


@pytest.mark.parametrize('identity,required,result,code', [
    (None, False, -1, 'LEADERSHIP_LEDGER_MISMATCH'),
    ('existing-ledger', False, -1, 'LEADERSHIP_LEDGER_MISMATCH'),
    ('existing-ledger', True, -2, 'LEADERSHIP_REGISTRY_MISSING'),
    (None, True, -2, 'LEADERSHIP_REGISTRY_MISSING'),
    ('existing-ledger', True, 0, None),
])
def test_conflict_missing_required_registry_and_replaced_owner_never_bind(identity, required, result, code):
    client = Mock()
    client.eval.return_value = result
    elector = LeaderElector('deployment', client=client)
    if code:
        with pytest.raises(LeadershipError, match='^' + code + '$'):
            elector.check_ledger('owner', identity, require_existing=required)
    else:
        assert elector.check_ledger('owner', identity, require_existing=required) is False
    assert [call[0] for call in client.method_calls] == ['eval']


@pytest.mark.parametrize('identity,required', [('', False), (1, False), (False, False), ('ledger', 1)])
def test_invalid_registry_preflight_arguments_reject_without_remote_command(identity, required):
    client = Mock()
    elector = LeaderElector('deployment', client=client)
    with pytest.raises(ValueError, match='^LEADERSHIP_LEDGER_INVALID$'):
        elector.check_ledger('owner', identity, require_existing=required)
    assert client.method_calls == []


def test_preflight_connection_error_is_sanitized_without_attempting_registration():
    client = Mock()
    client.eval.side_effect = OSError('synthetic private endpoint and credential')
    elector = LeaderElector('deployment', client=client)
    with pytest.raises(LeadershipError, match='^LEADERSHIP_REDIS_FAILED$') as caught:
        elector.check_ledger('owner', None)
    assert caught.value.__suppress_context__
    assert [call[0] for call in client.method_calls] == ['eval']


def acquired_lease(result=True, *, clock=None):
    elector = Mock()
    elector.elect_as_leader.return_value = True
    elector.check_ledger.return_value = result
    lease = LeadershipLease(elector, ttl_seconds=60, renew_interval_seconds=30,
                            **({'clock': clock} if clock else {}))
    assert lease.acquire()
    return lease, elector


@pytest.mark.parametrize('identity,required', [(None, False), ('existing-ledger', True)])
def test_lease_check_preserves_owned_lease_without_binding_or_renewal(identity, required):
    lease, elector = acquired_lease()
    with lease:
        lease.check_ledger(identity, require_existing=required)
        lease.assert_owned()
        assert not lease.lost
        assert [call[0] for call in elector.method_calls] == ['elect_as_leader', 'check_ledger']
        assert elector.check_ledger.call_args.args == (lease._token, identity)
        assert elector.check_ledger.call_args.kwargs == {'require_existing': required}


@pytest.mark.parametrize('failure,code', [
    (False, 'LEADERSHIP_LOST'),
    (LeadershipError('LEADERSHIP_LEDGER_MISMATCH'), 'LEADERSHIP_LEDGER_MISMATCH'),
    (LeadershipError('LEADERSHIP_REGISTRY_MISSING'), 'LEADERSHIP_REGISTRY_MISSING'),
    (OSError('synthetic private connection details'), 'LEADERSHIP_REDIS_FAILED'),
])
def test_failed_lease_preflight_irreversibly_stops_writer_admission(failure, code):
    lease, elector = acquired_lease(result=failure)
    if isinstance(failure, Exception):
        elector.check_ledger.side_effect = failure
    with lease:
        with pytest.raises(LeadershipError, match='^' + code + '$'):
            lease.check_ledger('existing-ledger', require_existing=True)
        assert lease.lost
        with pytest.raises(LeadershipError, match='^LEADERSHIP_LOST$'):
            lease.assert_owned()
        assert [call[0] for call in elector.method_calls] == ['elect_as_leader', 'check_ledger']


def test_successful_preflight_after_local_deadline_cannot_restore_ownership():
    clock = [0.0]
    lease, elector = acquired_lease(clock=lambda: clock[0])

    def delayed_success(*_args, **_kwargs):
        clock[0] = 61.0
        return True

    elector.check_ledger.side_effect = delayed_success
    with lease:
        with pytest.raises(LeadershipError, match='^LEADERSHIP_LOST$'):
            lease.check_ledger(None)
        assert lease.lost
