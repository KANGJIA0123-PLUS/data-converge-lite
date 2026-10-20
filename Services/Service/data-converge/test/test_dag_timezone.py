"""Elapsed subdaily windows and local calendar windows across clock changes."""

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from data_converge.dag import DAGBuilder
from data_converge.dag.builder import calculate_data_window

from .test_scheduler import FakeConfigLoader, make_rules


NEW_YORK = ZoneInfo("America/New_York")


def local_time(value):
    return datetime.fromisoformat(value).astimezone(NEW_YORK)


@pytest.mark.parametrize("interval, scheduled, start, end, seconds", [
    ("PT5M", "2026-03-08T03:01:00-04:00", "2026-03-08T01:55:00-05:00",
     "2026-03-08T03:00:00-04:00", 300),
    ("PT5M", "2026-11-01T01:01:00-05:00", "2026-11-01T01:55:00-04:00",
     "2026-11-01T01:00:00-05:00", 300),
    ("PT1H", "2026-03-08T03:06:00-04:00", "2026-03-08T01:00:00-05:00",
     "2026-03-08T03:00:00-04:00", 3600),
    ("PT1H", "2026-11-01T01:06:00-05:00", "2026-11-01T01:00:00-04:00",
     "2026-11-01T01:00:00-05:00", 3600),
    ("P1D", "2026-03-09T00:20:00-04:00", "2026-03-08T00:00:00-05:00",
     "2026-03-09T00:00:00-04:00", 23 * 3600),
    ("P1D", "2026-11-02T00:20:00-05:00", "2026-11-01T00:00:00-04:00",
     "2026-11-02T00:00:00-05:00", 25 * 3600),
    ("P1M", "2026-04-01T00:20:00-04:00", "2026-03-01T00:00:00-05:00",
     "2026-04-01T00:00:00-04:00", (31 * 24 - 1) * 3600),
])
def test_windows_preserve_elapsed_durations_and_local_calendar_boundaries(
        interval, scheduled, start, end, seconds):
    actual_start, actual_end = calculate_data_window(local_time(scheduled), interval)
    assert actual_start.isoformat() == start
    assert actual_end.isoformat() == end
    assert actual_end.timestamp() - actual_start.timestamp() == seconds


@pytest.mark.parametrize("scheduled, start, end, hours", [
    ("2026-03-09T00:20:00-04:00", "2026-03-08T00:00:00-05:00",
     "2026-03-09T00:00:00-04:00", 23),
    ("2026-11-02T00:20:00-05:00", "2026-11-01T00:00:00-04:00",
     "2026-11-02T00:00:00-05:00", 25),
])
def test_natural_day_keeps_dst_window_without_waiting_for_hourly_successes(
        scheduled, start, end, hours):
    rules = make_rules()
    rules[0]["aggregation_interval"] = "PT1H"
    rules[1]["aggregation_interval"] = "P1D"
    builder = DAGBuilder(FakeConfigLoader(rules))
    instance = builder.create_instance(builder.build(), "task_hour", local_time(scheduled))
    assert instance.data_start.isoformat() == start
    assert instance.data_end.isoformat() == end
    assert instance.data_end.timestamp() - instance.data_start.timestamp() == hours * 3600
    assert instance.upstream_instance_ids == ()


@pytest.mark.parametrize("scheduled, upstream_id", [
    ("2026-11-01T01:03:00-04:00", "task_5min@2026-11-01T01:00:00-04:00"),
    ("2026-11-01T01:03:00-05:00", "task_5min@2026-11-01T01:00:00-05:00"),
])
def test_same_period_dependencies_distinguish_repeated_hour_boundaries(
        scheduled, upstream_id):
    rules = make_rules()
    rules[0]["aggregation_interval"] = "PT1H"
    builder = DAGBuilder(FakeConfigLoader(rules))
    instance = builder.create_instance(
        builder.build(), "task_hour", local_time(scheduled))
    assert instance.upstream_instance_ids == (upstream_id,)


@pytest.mark.parametrize("scheduled, start, end", [
    ("2026-04-05T02:06:00+10:30", "2026-04-05T01:00:00+11:00",
     "2026-04-05T02:00:00+10:30"),
    ("2026-10-04T03:06:00+11:00", "2026-10-04T01:00:00+10:30",
     "2026-10-04T03:00:00+11:00"),
])
def test_half_hour_clock_changes_keep_hour_windows_contiguous(scheduled, start, end):
    instant = datetime.fromisoformat(scheduled).astimezone(ZoneInfo("Australia/Lord_Howe"))
    actual_start, actual_end = calculate_data_window(instant, "PT1H")
    assert actual_start.isoformat() == start
    assert actual_end.isoformat() == end
    assert actual_end.timestamp() - actual_start.timestamp() == 90 * 60


@pytest.mark.parametrize("scheduled, start, end, minutes", [
    ("2026-04-06T00:10:00+10:30", "2026-04-05T00:00:00+11:00",
     "2026-04-06T00:00:00+10:30", 24 * 60 + 30),
    ("2026-10-05T00:10:00+11:00", "2026-10-04T00:00:00+10:30",
     "2026-10-05T00:00:00+11:00", 23 * 60 + 30),
])
def test_half_hour_clock_change_day_keeps_calendar_window_without_waiting_for_hours(
        scheduled, start, end, minutes):
    rules = make_rules()
    rules[0]["aggregation_interval"] = "PT1H"
    rules[1]["aggregation_interval"] = "P1D"
    builder = DAGBuilder(FakeConfigLoader(rules))
    instant = datetime.fromisoformat(scheduled).astimezone(ZoneInfo("Australia/Lord_Howe"))
    instance = builder.create_instance(builder.build(), "task_hour", instant)
    assert instance.data_start.isoformat() == start
    assert instance.data_end.isoformat() == end
    assert instance.data_end.timestamp() - instance.data_start.timestamp() == minutes * 60
    assert instance.upstream_instance_ids == ()


def test_nonexistent_midnight_uses_canonical_boundary_and_upstream_identity():
    rules = make_rules()
    rules[0]["aggregation_interval"] = "P1D"
    rules[1]["aggregation_interval"] = "P1D"
    builder = DAGBuilder(FakeConfigLoader(rules))
    zone = ZoneInfo("America/Santiago")
    instance = builder.create_instance(
        builder.build(), "task_hour", datetime(2026, 9, 6, 1, 10, tzinfo=zone))
    assert instance.instance_id == "task_hour@2026-09-06T01:00:00-03:00"
    assert instance.data_start.isoformat() == "2026-09-05T00:00:00-04:00"
    assert instance.upstream_instance_ids == ("task_5min@2026-09-06T01:00:00-03:00",)


def test_month_keeps_calendar_window_with_skipped_midnight_without_waiting_for_days():
    rules = make_rules()
    rules[0]["aggregation_interval"] = "P1D"
    rules[1]["aggregation_interval"] = "P1M"
    builder = DAGBuilder(FakeConfigLoader(rules))
    instance = builder.create_instance(
        builder.build(), "task_hour", datetime(2026, 10, 1, 0, 20, tzinfo=ZoneInfo("America/Santiago")))
    assert instance.data_start.isoformat() == "2026-09-01T00:00:00-04:00"
    assert instance.data_end.isoformat() == "2026-10-01T00:00:00-03:00"
    assert instance.data_end.timestamp() - instance.data_start.timestamp() == (30 * 24 - 1) * 3600
    assert instance.upstream_instance_ids == ()


@pytest.mark.parametrize("interval, expected", [
    ("PT1H", "2026-07-15T09:30:00+02:00"),
    ("P1D", "2026-07-14T10:30:00+02:00"),
    ("P1M", "2026-06-01T10:30:00+02:00"),
])
def test_explicit_unaligned_manual_batch_preserves_its_existing_window_contract(interval, expected):
    rule = make_rules()[0]
    rule["aggregation_interval"] = interval
    builder = DAGBuilder(FakeConfigLoader([rule]))
    end = datetime(2026, 7, 15, 10, 30, tzinfo=ZoneInfo("Europe/Berlin"))
    instance = builder.create_batch_instances(builder.build(), end)[0]
    assert instance.data_start.isoformat() == expected
    assert instance.data_end == end


def test_calendar_window_moves_back_past_a_wholly_skipped_local_date():
    instant = datetime(2011, 12, 31, 0, 10, tzinfo=ZoneInfo("Pacific/Apia"))
    start, end = calculate_data_window(instant, "P1D")
    assert start.isoformat() == "2011-12-29T00:00:00-10:00"
    assert end.isoformat() == "2011-12-31T00:00:00+14:00"
    assert end.timestamp() - start.timestamp() == 24 * 3600


@pytest.mark.parametrize("scheduled, start, end", [
    ("2026-04-05T02:06:00+10:30", "2026-04-05T01:00:00+11:00",
     "2026-04-05T02:00:00+10:30"),
    ("2026-10-04T03:06:00+11:00", "2026-10-04T01:00:00+10:30",
     "2026-10-04T03:00:00+11:00"),
])
def test_transition_hour_keeps_full_window_without_waiting_for_five_minute_successes(
        scheduled, start, end):
    builder = DAGBuilder(FakeConfigLoader(make_rules()))
    instant = datetime.fromisoformat(scheduled).astimezone(ZoneInfo("Australia/Lord_Howe"))
    instance = builder.create_instance(builder.build(), "task_hour", instant)
    assert instance.data_start.isoformat() == start
    assert instance.data_end.isoformat() == end
    assert instance.data_end.timestamp() - instance.data_start.timestamp() == 90 * 60
    assert instance.upstream_instance_ids == ()


def test_two_hour_rollback_selects_the_immediately_preceding_real_hour():
    instant = datetime.fromisoformat("2026-10-25T01:06:00+00:00").astimezone(
        ZoneInfo("Antarctica/Troll"))
    start, end = calculate_data_window(instant, "PT1H")
    assert start.isoformat() == "2026-10-25T02:00:00+02:00"
    assert end.isoformat() == "2026-10-25T01:00:00+00:00"
    assert end.timestamp() - start.timestamp() == 3600
