"""Diagnostics and daily backups share the deployment's calendar and real clock."""

from contextlib import closing
from datetime import datetime
import pytest

from data_converge.archives.worker import _next_day
from data_converge.diagnostics.archives import archive_check
from data_converge.diagnostics.ledger import DiagnosticLedger
from data_converge.diagnostics.report import render_text, report
from data_converge.diagnostics.windows import parse_window, window_evidence, window_starts
from data_converge.dwd_ready.message import DWD_TYPES
from data_converge.state import StateStore

from .test_archive_diagnostics import make_archive, write_status
from .test_archive_maintenance import database, run
from .timezone_fixtures import deployment_zone


def test_report_identifies_the_effective_system_timezone(deployment_zone):
    deployment_zone('UTC')
    result = report('diagnose', [])
    assert result['timezone'] == 'UTC'
    assert datetime.fromisoformat(result['checked_at']).utcoffset().total_seconds() == 0


def test_text_diagnostics_identify_the_effective_system_timezone(deployment_zone):
    deployment_zone('America/New_York')
    assert 'America/New_York' in render_text(report('diagnose', []))


def test_selected_dwd_window_displays_system_time_without_changing_instant(deployment_zone):
    deployment_zone('America/New_York')
    result = parse_window('2026-11-01T06:00:00+00:00')
    assert result.isoformat() == '2026-11-01T01:00:00-05:00'
    assert result.timestamp() == 1793512800


@pytest.mark.parametrize('now, coverage, expected', [
    ('2026-03-08T07:05:00+00:00', '2026-03-08T06:50:00+00:00', [
        '2026-03-08T03:00:00-04:00', '2026-03-08T01:55:00-05:00',
        '2026-03-08T01:50:00-05:00']),
    ('2026-11-01T06:05:00+00:00', '2026-11-01T05:50:00+00:00', [
        '2026-11-01T01:00:00-05:00', '2026-11-01T01:55:00-04:00',
        '2026-11-01T01:50:00-04:00']),
])
def test_recent_windows_follow_real_five_minutes_across_dst(deployment_zone, now, coverage, expected):
    zone = deployment_zone('America/New_York')
    current = datetime.fromisoformat(now).astimezone(zone)
    start = datetime.fromisoformat(coverage).astimezone(zone)
    assert [item.isoformat() for item in window_starts(start, current, 3)] == expected


def test_waiting_window_uses_elapsed_seconds_through_clock_rollback(database, deployment_zone):
    zone = deployment_zone('America/New_York')
    start = datetime.fromisoformat('2026-11-01T05:55:00+00:00').astimezone(zone)
    now = datetime.fromisoformat('2026-11-01T06:10:00+00:00').astimezone(zone)
    with closing(DiagnosticLedger(database)) as store:
        row = window_evidence(store, start, start, now, 120)
    assert row['window_end'] == '2026-11-01T01:00:00-05:00'
    assert row['status'] == 'WAITING' and row['wait_seconds'] == 600
    assert row['warning_due'] is True


def test_ready_delay_and_last_signal_use_instant_order_through_rollback(database, deployment_zone):
    zone = deployment_zone('America/New_York')
    start = datetime.fromisoformat('2026-11-01T05:55:00+00:00').astimezone(zone)
    with closing(StateStore(database)) as store:
        store.acquire_lock()
        for index, kind in enumerate(sorted(DWD_TYPES)):
            stamp = '2026-11-01T06:01:00+00:00' if index == 0 else '2026-11-01T05:59:00+00:00'
            store.record_dwd_signal(start, kind, datetime.fromisoformat(stamp))
    now = datetime.fromisoformat('2026-11-01T06:10:00+00:00').astimezone(zone)
    with closing(DiagnosticLedger(database)) as store:
        row = window_evidence(store, start, start, now, 120)
    assert row['status'] == 'READY' and row['ready_delay_seconds'] == 60
    assert row['last_received_at'] == '2026-11-01T01:01:00-05:00'
    assert row['wait_seconds'] == 0 and row['warning_due'] is False


def test_daily_backup_uses_system_midnight(database, deployment_zone):
    deployment_zone('UTC')
    first = run(database, datetime.fromisoformat('2026-10-19T15:59:00+00:00'))
    same_day = run(database, datetime.fromisoformat('2026-10-19T16:00:00+00:00'))
    next_day = run(database, datetime.fromisoformat('2026-10-20T00:00:00+00:00'))
    assert (first['created'], same_day['created'], next_day['created']) == (1, 0, 1)
    assert '20261020T000000000000Z' in next_day['latest_archive']


@pytest.mark.parametrize('now, expected', [
    ('2026-03-08T05:00:00+00:00', '2026-03-09T04:00:00+00:00'),
    ('2026-11-01T04:00:00+00:00', '2026-11-02T05:00:00+00:00'),
])
def test_worker_next_day_respects_short_and_long_local_days(deployment_zone, now, expected):
    deployment_zone('America/New_York')
    assert _next_day(datetime.fromisoformat(now)).isoformat() == expected


def test_archive_health_uses_the_same_system_date_as_maintenance(tmp_path, deployment_zone):
    deployment_zone('UTC')
    created = datetime.fromisoformat('2026-10-19T15:59:00+00:00')
    path = make_archive(tmp_path / 'state', created)
    write_status(path, checked_at=created)
    result = archive_check({}, tmp_path / 'state',
                           now=datetime.fromisoformat('2026-10-19T16:00:00+00:00'))
    assert result['status'] == 'PASS'
    assert result['evidence']['timezone'] == 'UTC'
    assert result['evidence']['maintenance_stale'] is False
    tomorrow = archive_check({}, tmp_path / 'state',
                             now=datetime.fromisoformat('2026-10-20T00:00:00+00:00'))
    assert tomorrow['status'] == 'WARN'
    assert tomorrow['evidence']['maintenance_stale'] is True


def seed_rollback_instances(database, status='PENDING'):
    with closing(StateStore(database)) as store:
        store.acquire_lock()
        with store._connection(write=True) as connection:
            for index, stamp in enumerate(('2026-11-01T01:55:00-04:00', '2026-11-01T01:05:00-05:00')):
                connection.execute('INSERT INTO task_instances '
                    '(instance_id, taskname, scheduled_time, data_start, data_end, status, finished_at) '
                    'VALUES (?, ?, ?, ?, ?, ?, ?)',
                    (str(index), 'simple', stamp, stamp, stamp, status, stamp))


def test_ledger_progress_orders_absolute_instants_during_rollback(database):
    seed_rollback_instances(database)
    with closing(DiagnosticLedger(database)) as store:
        result = store.progress()
    assert result == {
        'newest_window_end': '2026-11-01T01:05:00-05:00',
        'latest_finished_at': '2026-11-01T01:05:00-05:00',
        'oldest_active_window': '2026-11-01T01:55:00-04:00',
    }


def test_status_page_returns_the_latest_real_window_during_rollback(database):
    seed_rollback_instances(database)
    with closing(StateStore(database)) as store:
        rows = store.list_instances(limit=1)
    assert [row['instance_id'] for row in rows] == ['1']


def test_failure_pages_order_real_instants_during_rollback(database):
    seed_rollback_instances(database, 'UNKNOWN')
    with closing(DiagnosticLedger(database)) as store:
        counts, first = store.problems(limit=1)
        _, second = store.problems(limit=1, offset=1)
    assert counts == {'UNKNOWN': 2}
    assert [row['instance_id'] for row in first + second] == ['1', '0']
