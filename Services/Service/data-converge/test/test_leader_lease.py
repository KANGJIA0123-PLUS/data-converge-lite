"""Lease deadline, irreversible loss and owned-thread cleanup behavior."""

import threading
import time

import pytest


class Elector:
    def __init__(self):
        self.owner = None
        self.tokens = []
        self.renew_calls = 0
        self.renew = lambda: True
        self.bind = lambda: True
        self.on_acquire = lambda: None

    def elect_as_leader(self, token, ttl):
        self.tokens.append(token)
        self.on_acquire()
        if self.owner is not None:
            return False
        self.owner = token
        return True

    def renew_leadership(self, token, ttl):
        self.renew_calls += 1
        return self.owner == token and self.renew()

    def release_leadership(self, token):
        if self.owner != token:
            return False
        self.owner = None
        return True

    def bind_ledger(self, token, ledger_id, require_existing=False):
        return self.owner == token and self.bind()


def wait_until(predicate):
    deadline = time.monotonic() + 2
    while not predicate() and time.monotonic() < deadline:
        time.sleep(0.001)
    assert predicate()


def make_lease(elector, **kwargs):
    from data_converge.leader_election import LeadershipLease
    return LeadershipLease(elector, ttl_seconds=1, renew_interval_seconds=0.01, **kwargs)


def test_contending_attempt_can_retry_with_fresh_session_token():
    elector = Elector()
    elector.owner = 'other'
    with make_lease(elector) as lease:
        assert lease.acquire() is False
        elector.owner = None
        assert lease.acquire() is True
        lease.assert_owned()
    assert len(set(elector.tokens)) == 2
    assert elector.owner is None


def test_acquisition_time_counts_against_local_validity():
    from data_converge.leader_election import LeadershipError
    clock = [0.0]
    elector = Elector()
    elector.on_acquire = lambda: clock.__setitem__(0, 1.1)
    with make_lease(elector, clock=lambda: clock[0]) as lease:
        with pytest.raises(LeadershipError, match='LEADERSHIP_LOST'):
            lease.acquire()
        assert lease.lost
    assert elector.owner is None


@pytest.mark.parametrize('failure', [False, 'exception'])
def test_any_failed_renewal_irreversibly_stops_dispatch(failure):
    from data_converge.leader_election import LeadershipError
    elector = Elector()

    def fail():
        if failure == 'exception':
            raise OSError('private failure')
        return False

    elector.renew = fail
    with make_lease(elector) as lease:
        assert lease.acquire()
        wait_until(lambda: lease.lost)
        with pytest.raises(LeadershipError, match='LEADERSHIP_LOST'):
            lease.assert_owned()
        elector.renew = lambda: True
        with pytest.raises(LeadershipError, match='LEADERSHIP_LOST'):
            lease.acquire()
    assert elector.renew_calls == 1


def test_successful_late_renewal_cannot_resurrect_expired_lease():
    from data_converge.leader_election import LeadershipError
    clock = [0.0]
    elector = Elector()

    def late_success():
        clock[0] = 1.1
        return True

    elector.renew = late_success
    with make_lease(elector, clock=lambda: clock[0]) as lease:
        assert lease.acquire()
        clock[0] = 0.5
        wait_until(lambda: lease.lost)
        with pytest.raises(LeadershipError, match='LEADERSHIP_LOST'):
            lease.assert_owned()


def test_deadline_guard_does_not_wait_for_blocked_redis_renewal():
    from data_converge.leader_election import LeadershipError
    clock, entered, release = [0.0], threading.Event(), threading.Event()
    elector = Elector()

    def blocked():
        entered.set()
        release.wait(2)
        return True

    elector.renew = blocked
    with make_lease(elector, clock=lambda: clock[0]) as lease:
        try:
            assert lease.acquire()
            assert entered.wait(2)
            clock[0] = 1.1
            with pytest.raises(LeadershipError, match='LEADERSHIP_LOST'):
                lease.assert_owned()
        finally:
            release.set()


def test_close_joins_renewal_and_never_releases_replacement_owner():
    elector = Elector()
    lease = make_lease(elector)
    assert lease.acquire()
    wait_until(lambda: elector.renew_calls > 0)
    elector.owner = 'replacement-token'
    lease.close()
    assert elector.owner == 'replacement-token'
    assert not lease._thread.is_alive()
    lease.close()


def test_binding_failure_marks_lease_lost_and_preserves_safe_code():
    from data_converge.leader_election import LeadershipError
    elector = Elector()

    def missing():
        raise LeadershipError('LEADERSHIP_REGISTRY_MISSING')

    elector.bind = missing
    with make_lease(elector) as lease:
        assert lease.acquire()
        with pytest.raises(LeadershipError, match='LEADERSHIP_REGISTRY_MISSING'):
            lease.bind_ledger('ledger-id', require_existing=True)
        assert lease.lost


def test_exceptional_body_still_releases_owned_lease():
    elector = Elector()
    with pytest.raises(RuntimeError, match='body failed'):
        with make_lease(elector) as lease:
            assert lease.acquire()
            raise RuntimeError('body failed')
    assert elector.owner is None
    assert not lease._thread.is_alive()


def test_timely_renewal_extends_admission_beyond_original_deadline():
    clock = [0.0]
    elector = Elector()
    with make_lease(elector, clock=lambda: clock[0]) as lease:
        assert lease.acquire()
        clock[0] = 0.5
        wait_until(lambda: elector.renew_calls > 0)
        lease.bind_ledger('ledger-id')  # Serialize after the completed renewal.
        clock[0] = 1.1
        lease.assert_owned()


def test_uncertain_acquisition_is_released_on_context_cleanup():
    from data_converge.leader_election import LeadershipError
    elector = Elector()

    def response_lost():
        elector.owner = elector.tokens[-1]
        raise OSError('private response lost')

    elector.on_acquire = response_lost
    with make_lease(elector) as lease:
        with pytest.raises(LeadershipError, match='LEADERSHIP_REDIS_FAILED'):
            lease.acquire()
        assert lease.lost
    assert elector.owner is None


def test_release_error_preserves_body_error_and_redacts_logs(caplog):
    elector = Elector()

    def release_error(token):
        raise OSError('private credentials')

    elector.release_leadership = release_error
    with pytest.raises(RuntimeError, match='body failed'):
        with make_lease(elector) as lease:
            assert lease.acquire()
            raise RuntimeError('body failed')
    assert 'LEADERSHIP_RELEASE_FAILED' in caplog.text
    assert 'private credentials' not in caplog.text
    assert not lease._thread.is_alive()
