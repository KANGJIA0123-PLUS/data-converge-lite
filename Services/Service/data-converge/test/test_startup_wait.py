"""Exercise startup-only retries with a controlled clock and real error evidence."""

import pytest
import yaml

from data_converge import startup
from data_converge.application import Application
from data_converge.config.startup import StartupSettings
from data_converge.sql_execution.client import SQLExecutionError

from .test_main import app


class Clock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def sleep(self, duration):
        self.sleeps.append(duration)
        self.now += duration


@pytest.fixture
def clock(monkeypatch):
    clock = Clock()
    monkeypatch.setattr(startup, 'monotonic', lambda: clock.now)
    monkeypatch.setattr(startup, 'sleep', clock.sleep)
    return clock


def connection_error():
    return SQLExecutionError('CLICKHOUSE_CONNECT_FAILED', retryable=True,
                             diagnostic={'error_type': 'gaierror', 'os_errno': -2})


def wait(probe, settings=StartupSettings()):
    return startup.wait_for_clickhouse(probe, settings, lambda: False, lambda: None)


def test_installation_dns_tls_and_database_failures_recover_with_capped_backoff(clock, caplog):
    caplog.set_level('INFO', logger='data_converge.startup')
    failures = [connection_error() for _ in range(4)] + [
        SQLExecutionError('CLICKHOUSE_OUTCOME_UNKNOWN', outcome_unknown=True,
                          diagnostic={'error_type': 'SSLEOFError', 'os_errno': 8}),
        SQLExecutionError('CLICKHOUSE_HTTP_FAILED', outcome_unknown=True,
                          diagnostic={'http_status': 404, 'clickhouse_exception_code': 81}),
    ]
    attempts = []

    def probe(timeout):
        attempts.append((clock.now, timeout))
        if failures:
            raise failures.pop(0)

    assert wait(probe)
    assert [instant for instant, _ in attempts] == pytest.approx([0, 2, 6, 14, 30, 60, 90])
    assert all(timeout == 10 for _, timeout in attempts)
    assert max(clock.sleeps) <= 0.2
    assert caplog.text.count('CLICKHOUSE_STARTUP_WAIT:') == 6
    assert 'CLICKHOUSE_STARTUP_READY: attempts=7' in caplog.text


def test_total_budget_clips_backoff_and_query_timeout(clock, caplog):
    attempts = []

    def unavailable(timeout):
        attempts.append((clock.now, timeout))
        raise connection_error()

    with pytest.raises(SQLExecutionError, match='CLICKHOUSE_STARTUP_TIMEOUT') as caught:
        wait(unavailable, StartupSettings(timeout_seconds=5, max_retry_delay_seconds=2))
    assert [instant for instant, _ in attempts] == pytest.approx([0, 2, 4])
    assert [timeout for _, timeout in attempts] == pytest.approx([5, 3, 1])
    assert clock.now == pytest.approx(5)
    assert caught.value.diagnostic['error_type'] == 'gaierror'
    assert 'CLICKHOUSE_STARTUP_TIMEOUT' in caplog.text


@pytest.mark.parametrize('success', [False, True])
def test_inflight_query_consumes_total_budget_without_another_attempt(clock, success):
    attempts = []

    def slow(timeout):
        attempts.append(timeout)
        clock.now += timeout
        if not success:
            raise SQLExecutionError('CLICKHOUSE_OUTCOME_UNKNOWN', outcome_unknown=True)

    with pytest.raises(SQLExecutionError, match='CLICKHOUSE_STARTUP_TIMEOUT'):
        wait(slow, StartupSettings(timeout_seconds=3))
    assert attempts == [3]
    assert clock.sleeps == []


@pytest.mark.parametrize('code,diagnostic', [
    ('CLICKHOUSE_AUTH_FAILED', {'http_status': 401}),
    ('CLICKHOUSE_HTTP_FAILED', {'http_status': 403}),
    ('CLICKHOUSE_HTTP_FAILED', {'http_status': 500, 'clickhouse_exception_code': 516}),
    ('CLICKHOUSE_HTTP_FAILED', {'http_status': 500, 'clickhouse_exception_code': 497}),
    ('CLICKHOUSE_HTTP_FAILED', {'http_status': 500, 'clickhouse_exception_code': 194}),
    ('CLICKHOUSE_HTTP_FAILED', {'http_status': 503, 'clickhouse_exception_code': 62}),
    ('CLICKHOUSE_HTTP_FAILED', {'http_status': 404}),
    ('CLICKHOUSE_OUTCOME_UNKNOWN', {'error_type': 'SSLCertVerificationError'}),
    ('CLICKHOUSE_OUTCOME_UNKNOWN', {'tls_verify_code': 10}),
    ('CLICKHOUSE_OUTCOME_UNKNOWN', {'error_type': 'SSLError'}),
    ('CLICKHOUSE_RESPONSE_INVALID', {}),
    ('CLICKHOUSE_RESPONSE_LIMIT', {}),
])
def test_permanent_failures_escape_without_retry(clock, code, diagnostic):
    error = SQLExecutionError(code, diagnostic=diagnostic, outcome_unknown=True)

    def fail(_timeout):
        raise error

    with pytest.raises(SQLExecutionError) as caught:
        wait(fail)
    assert caught.value is error
    assert clock.sleeps == []


@pytest.mark.parametrize('diagnostic', [
    {}, {'error_type': 'TimeoutError'}, {'error_type': 'ConnectionResetError'},
    {'error_type': 'RemoteDisconnected'}, {'error_type': 'SSLEOFError'},
    {'http_status': 408}, {'http_status': 429}, {'http_status': 500},
    {'http_status': 502}, {'http_status': 503}, {'http_status': 504},
])
def test_transient_unknown_outcomes_are_safe_only_for_startup_read(clock, diagnostic):
    calls = []
    error = SQLExecutionError('CLICKHOUSE_OUTCOME_UNKNOWN', outcome_unknown=True, diagnostic=diagnostic)

    def recover(_timeout):
        calls.append(1)
        if len(calls) == 1:
            raise error

    assert wait(recover)
    assert len(calls) == 2
    assert error.outcome_unknown and not error.retryable


def test_preexisting_stop_prevents_any_connection_or_readiness(clock):
    calls = []
    assert not startup.wait_for_clickhouse(calls.append, StartupSettings(), lambda: True, lambda: None)
    assert calls == clock.sleeps == []


@pytest.mark.parametrize('settings', [
    [], 'invalid', {'unknown': 1}, {'timeout_seconds': 0}, {'timeout_seconds': -1},
    {'timeout_seconds': True}, {'timeout_seconds': float('nan')}, {'timeout_seconds': float('inf')},
    {'probe_timeout_seconds': 0}, {'initial_retry_delay_seconds': 0},
    {'max_retry_delay_seconds': 0}, {'initial_retry_delay_seconds': 31},
])
def test_invalid_policy_is_rejected_before_state_creation(app, settings):
    config = yaml.safe_load(app.read_text())
    config['startup'] = settings
    app.write_text(yaml.safe_dump(config))
    with pytest.raises(ValueError, match='STARTUP_SETTING_INVALID'):
        Application(app).validate()
    assert not (app.parent / 'state').exists()


def test_retry_policy_does_not_change_existing_ledger_binding_or_manifest(app):
    original = Application(app)
    fingerprint, manifest = original.fingerprint, original.manifest
    config = yaml.safe_load(app.read_text())
    config['startup'] = dict(timeout_seconds=900, initial_retry_delay_seconds=1,
                             max_retry_delay_seconds=20, probe_timeout_seconds=5)
    app.write_text(yaml.safe_dump(config))
    changed = Application(app)
    assert changed.validate().exit_code == 0
    assert changed.fingerprint == fingerprint
    assert changed.manifest == manifest
