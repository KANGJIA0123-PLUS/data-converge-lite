"""Lease composition owns its connection without constructing a queue consumer."""

from types import SimpleNamespace

import pytest

from data_converge.config.leadership import LeadershipSettings
from data_converge.coordination import factory


class LeaseClient:
    def __init__(self, events):
        self.events = events

    def set(self, *_args, **_kwargs):
        self.events.append('acquire')
        return True

    def eval(self, *_args):
        self.events.append('release')
        return 1


def owned_connection(events):
    return SimpleNamespace(
        client=LeaseClient(events),
        close=lambda: events.append('disconnect'),
    )


def test_lease_releases_before_disconnect_on_body_failure(monkeypatch):
    from data_converge.dwd_ready import redis_source

    events = []
    definition = SimpleNamespace(leadership=LeadershipSettings(), config={'dwd_ready': {}})
    monkeypatch.setattr(factory, 'create_connection', lambda _: owned_connection(events))
    monkeypatch.setattr(redis_source, 'create_source',
                        lambda _: pytest.fail('leadership must not create a queue consumer'))
    with pytest.raises(RuntimeError, match='body failed'):
        with factory.create_leadership(definition) as lease:
            assert lease.acquire()
            raise RuntimeError('body failed')
    assert events == ['acquire', 'release', 'disconnect']


def test_invalid_lease_configuration_still_closes_connection(monkeypatch):
    events = []
    definition = SimpleNamespace(
        leadership=LeadershipSettings(ttl_seconds=0), config={'dwd_ready': {}},
    )
    monkeypatch.setattr(factory, 'create_connection', lambda _: owned_connection(events))
    with pytest.raises(ValueError):
        with factory.create_leadership(definition):
            pytest.fail('invalid lease must not enter the application')
    assert events == ['disconnect']
