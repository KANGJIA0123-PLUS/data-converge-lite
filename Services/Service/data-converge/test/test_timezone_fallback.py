"""System timezone detection failures preserve one consistent Shanghai fallback."""

from datetime import datetime, timezone
import struct
from zoneinfo import ZoneInfo

import pytest
import tzlocal

from data_converge import time_policy


@pytest.fixture(autouse=True)
def reset_timezone_cache(monkeypatch):
    monkeypatch.delenv('TZ', raising=False)
    time_policy.system_timezone.cache_clear()
    yield
    time_policy.system_timezone.cache_clear()


@pytest.mark.parametrize('error_type', [
    OSError, ValueError, RuntimeError, TypeError, AttributeError, KeyError, struct.error,
])
def test_detection_errors_keep_shanghai_time_and_log_once(monkeypatch, caplog, error_type):
    def unavailable():
        raise error_type('private environment details')
    monkeypatch.setattr(tzlocal, 'get_localzone_name', unavailable, raising=False)
    monkeypatch.setattr(time_policy, 'get_localzone', unavailable)
    zone = time_policy.system_timezone()
    assert datetime(2026, 10, 20, tzinfo=timezone.utc).astimezone(zone).isoformat() == (
        '2026-10-20T08:00:00+08:00')
    assert time_policy.system_timezone_name() == 'Asia/Shanghai'
    assert caplog.text.count('SYSTEM_TIMEZONE_FALLBACK') == 1
    assert 'private' not in caplog.text


@pytest.mark.parametrize('configured_name, expected', [
    (None, 'Asia/Shanghai'),
    ('UTC', 'UTC'),
])
def test_absent_configuration_does_not_accept_library_default_utc(
        monkeypatch, caplog, configured_name, expected):
    monkeypatch.setattr(tzlocal, 'get_localzone_name', lambda: configured_name, raising=False)
    monkeypatch.setattr(time_policy, 'get_localzone', lambda: ZoneInfo('UTC'))
    assert time_policy.system_timezone_name() == expected
    assert ('SYSTEM_TIMEZONE_FALLBACK' in caplog.text) == (configured_name is None)


@pytest.mark.parametrize('detected, expected', [
    (ZoneInfo('UTC'), 'UTC'),
    (timezone.utc, 'Asia/Shanghai'),
])
def test_detector_without_name_api_keeps_compatibility(monkeypatch, detected, expected):
    monkeypatch.delattr(tzlocal, 'get_localzone_name', raising=False)
    monkeypatch.setattr(time_policy, 'get_localzone', lambda: detected)
    assert time_policy.system_timezone_name() == expected


def test_invalid_environment_cannot_silently_select_another_system_timezone(monkeypatch):
    monkeypatch.setenv('TZ', 'invalid-private-zone')
    monkeypatch.setattr(tzlocal, 'get_localzone_name', lambda: 'UTC', raising=False)
    monkeypatch.setattr(time_policy, 'get_localzone', lambda: ZoneInfo('UTC'))
    assert time_policy.system_timezone_name() == 'Asia/Shanghai'
