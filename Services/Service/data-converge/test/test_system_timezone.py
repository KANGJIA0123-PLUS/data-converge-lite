"""Exercise application time policy in fresh processes with deployment TZ values."""

import json
import os
import subprocess
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from .test_main import app, local_http_app
from data_converge.application import next_boundary


def run_in_timezone(zone, script, *arguments):
    result = subprocess.run(
        [sys.executable, '-c', script, *map(str, arguments)],
        env={**os.environ, 'TZ': zone}, capture_output=True, text=True, check=True,
    )
    return json.loads(result.stdout)


@pytest.mark.parametrize('zone, expected', [
    ('UTC', '2026-10-20T00:00:00+00:00'),
    ('Asia/Tokyo', '2026-10-20T09:00:00+09:00'),
    ('America/New_York', '2026-10-19T20:00:00-04:00'),
])
def test_application_time_and_settings_follow_deployment_timezone(zone, expected):
    actual = run_in_timezone(zone, """
import json
from data_converge.application import parse_time
from data_converge.config.application import settings
print(json.dumps({
    'timezone': settings({}).timezone,
    'instant': parse_time('2026-10-20T00:00:00+00:00').isoformat(),
}))
""")
    assert actual == {'timezone': zone, 'instant': expected}


@pytest.mark.parametrize('zone, expected', [
    ('UTC', '2026-10-20T00:00:00+00:00'),
    ('Asia/Tokyo', '2026-10-20T09:00:00+09:00'),
    ('invalid-private-zone', '2026-10-20T08:00:00+08:00'),
])
def test_initialization_records_system_local_coverage(app, zone, expected):
    actual = run_in_timezone(zone, """
import json, sys
from data_converge.application import Application
application = Application(sys.argv[1])
print(json.dumps(application.initialize('2026-10-20T00:00:00+00:00').payload))
""", app)
    assert actual == {'status': 'INITIALIZED', 'coverage_start': expected}


def test_invalid_system_timezone_uses_shanghai_and_logs_a_safe_warning_once():
    actual = run_in_timezone('invalid-private-zone', """
import io, json
from contextlib import redirect_stderr
from data_converge.application import parse_time
from data_converge.config.application import settings
stream = io.StringIO()
with redirect_stderr(stream):
    instant = parse_time('2026-10-20T00:00:00+00:00')
    zone = settings({}).timezone
print(json.dumps({'instant': instant.isoformat(), 'timezone': zone, 'log': stream.getvalue()}))
""")
    assert actual['instant'] == '2026-10-20T08:00:00+08:00'
    assert actual['timezone'] == 'Asia/Shanghai'
    assert actual['log'].count('SYSTEM_TIMEZONE_FALLBACK') == 1
    assert 'private' not in json.dumps(actual)


@pytest.mark.parametrize('start, expected', [
    ((2026, 3, 8, 1, 58), '2026-03-08T03:00:00-04:00'),
    ((2026, 11, 1, 1, 58), '2026-11-01T01:00:00-05:00'),
])
def test_initialization_boundary_advances_real_time_across_clock_changes(start, expected):
    instant = datetime(*start, tzinfo=ZoneInfo('America/New_York'))
    assert next_boundary(instant).isoformat() == expected


def test_manual_run_accepts_later_window_in_repeated_hour(local_http_app):
    config, seen = local_http_app
    actual = run_in_timezone('America/New_York', """
import json, sys
from data_converge.application import Application
application = Application(sys.argv[1])
application.initialize('2026-11-01T01:30:00-04:00')
result = application.run_task('simple', '2026-11-01T01:06:00-05:00')
print(json.dumps(result.payload))
""", config)
    assert actual['instances'][0]['status'] == 'SUCCESS'
    assert len(seen) == 1
