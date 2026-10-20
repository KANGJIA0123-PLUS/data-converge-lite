"""Select deployment timezones without depending on the test host configuration."""

from zoneinfo import ZoneInfo

import pytest

from data_converge import time_policy


@pytest.fixture
def deployment_zone(monkeypatch):
    def select(name):
        monkeypatch.setenv('TZ', name)
        time_policy.system_timezone.cache_clear()
        return ZoneInfo(name)
    yield select
    time_policy.system_timezone.cache_clear()
