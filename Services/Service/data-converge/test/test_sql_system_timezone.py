"""SQL requests preserve instants across system zones and DST transitions."""

from datetime import datetime
import urllib.parse
from zoneinfo import ZoneInfo

import pytest

from data_converge.sql_execution import client as client_module

from .test_sql_execution import capture_http, make_clickhouse_config


def request_parameters(monkeypatch, zone, start, end):
    monkeypatch.setattr(client_module, "system_timezone", lambda: ZoneInfo(zone), raising=False)
    monkeypatch.setattr(client_module, "system_timezone_name", lambda: zone, raising=False)
    calls, _ = capture_http(monkeypatch)
    client_module.execute_sql(
        make_clickhouse_config(), "SELECT 1", start_time=start, end_time=end)
    request = calls[0][0]
    return urllib.parse.parse_qs(urllib.parse.urlsplit(request.full_url).query)


@pytest.mark.parametrize("start,end", [
    (datetime(2026, 9, 9, 1), datetime(2026, 9, 9, 2)),
    ("2026-09-09 01:00:00", "2026-09-09 02:00:00"),
])
def test_naive_windows_follow_system_zone_and_send_absolute_seconds(monkeypatch, start, end):
    parameters = request_parameters(monkeypatch, "Asia/Kathmandu", start, end)
    assert parameters["param_start_time"] == ["1788894900"]
    assert parameters["param_end_time"] == ["1788898500"]
    assert parameters["param_system_timezone"] == ["Asia/Kathmandu"]


@pytest.mark.parametrize("zone", ["UTC", "Asia/Kathmandu", "America/New_York"])
def test_offset_window_keeps_the_same_instants_in_every_system_zone(monkeypatch, zone):
    parameters = request_parameters(
        monkeypatch, zone, "2026-09-09T01:00:00+00:00", "2026-09-09T02:00:00Z")
    assert parameters["param_start_time"] == ["1788915600"]
    assert parameters["param_end_time"] == ["1788919200"]
    assert parameters["param_system_timezone"] == [zone]


def test_spring_day_keeps_23_hours_between_query_bounds(monkeypatch):
    parameters = request_parameters(
        monkeypatch, "America/New_York", "2026-03-08 00:00:00", "2026-03-09 00:00:00")
    assert parameters["param_start_time"] == ["1772946000"]
    assert parameters["param_end_time"] == ["1773028800"]


def test_repeated_fall_hour_keeps_distinct_query_bounds(monkeypatch):
    zone = ZoneInfo("America/New_York")
    parameters = request_parameters(
        monkeypatch, zone.key, datetime(2026, 11, 1, 1, tzinfo=zone, fold=0),
        datetime(2026, 11, 1, 1, tzinfo=zone, fold=1))
    assert parameters["param_start_time"] == ["1793509200"]
    assert parameters["param_end_time"] == ["1793512800"]
