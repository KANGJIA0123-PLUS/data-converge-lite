"""Standby admission survives transient Redis failures with a fresh lease."""

from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from data_converge.coordination import admission
from data_converge.leader_election import LeadershipError, LeadershipLease


class State:
    def __init__(self, failures):
        self.failures = failures
        self.attempts = 0
        self.leases = 0
        self.owner = None
        self.tokens = []


class Elector:
    def __init__(self, state):
        self.state = state

    def elect_as_leader(self, token, ttl):
        state = self.state
        state.attempts += 1
        state.tokens.append(token)
        if state.attempts <= state.failures:
            raise OSError('private redis detail')
        if state.owner is not None:
            return False
        state.owner = token
        return True

    def renew_leadership(self, token, ttl):
        return self.state.owner == token

    def release_leadership(self, token):
        if self.state.owner != token:
            return False
        self.state.owner = None
        return True


@contextmanager
def factory(state, created):
    state.leases += 1
    with LeadershipLease(Elector(state), ttl_seconds=30, renew_interval_seconds=5) as lease:
        created.append(lease)
        yield lease


def definition():
    return SimpleNamespace(leadership=SimpleNamespace(enabled=True, standby_poll_seconds=2))


def test_standby_replaces_lease_after_transient_redis_failure(monkeypatch, caplog):
    state, created, sleeps = State(failures=2), [], []
    monkeypatch.setattr(admission.time, 'sleep', sleeps.append)
    caplog.set_level('WARNING', logger='data_converge.coordination.admission')
    with admission.admit_writer(definition(), wait=True,
                                factory=lambda _: factory(state, created)) as lease:
        lease.assert_owned()
    assert state.attempts == 3 and state.leases == 3
    assert sleeps == [2, 2]
    assert len(set(state.tokens)) == 3
    assert all(old.lost for old in created[:-1])
    assert state.owner is None
    assert caplog.text.count('LEADERSHIP_REDIS_RETRY:') == 2


def test_non_waiting_admission_still_fails_immediately(monkeypatch):
    state, created, sleeps = State(failures=1), [], []
    monkeypatch.setattr(admission.time, 'sleep', sleeps.append)
    with pytest.raises(LeadershipError, match='LEADERSHIP_REDIS_FAILED'):
        with admission.admit_writer(definition(), wait=False,
                                    factory=lambda _: factory(state, created)):
            pytest.fail('a failed non-waiting admission must not run the body')
    assert state.attempts == 1 and state.leases == 1 and sleeps == []


def test_standby_propagates_non_redis_leadership_failure(monkeypatch):
    sleeps = []

    class FailingLease:
        def acquire(self):
            raise LeadershipError('LEADERSHIP_ALREADY_ACQUIRED')

        def assert_owned(self):
            pytest.fail('a failed admission must not be asserted as owned')

        @property
        def lost(self):
            return False

    @contextmanager
    def failing_factory(_definition):
        yield FailingLease()

    monkeypatch.setattr(admission.time, 'sleep', sleeps.append)
    with pytest.raises(LeadershipError, match='LEADERSHIP_ALREADY_ACQUIRED'):
        with admission.admit_writer(definition(), wait=True, factory=failing_factory):
            pytest.fail('a failed admission must not run the body')
    assert sleeps == []


def test_sigterm_interrupts_the_standby_wait(monkeypatch):
    state, created = State(failures=0), []
    state.owner = 'another-node'

    def stop(_seconds):
        raise SystemExit(0)

    monkeypatch.setattr(admission.time, 'sleep', stop)
    with pytest.raises(SystemExit):
        with admission.admit_writer(definition(), wait=True,
                                    factory=lambda _: factory(state, created)):
            pytest.fail('a terminated standby must not run the body')
    assert len(created) == 1 and created[0].lost
    assert state.owner == 'another-node'
