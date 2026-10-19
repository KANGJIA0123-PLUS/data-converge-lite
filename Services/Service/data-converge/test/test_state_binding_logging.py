"""Explain rejected startup without changing the ledger or exposing its contents."""

from contextlib import closing
from datetime import datetime
import json
import logging
import sqlite3

import pytest
import yaml

from data_converge import main as entry
from data_converge.application import Application
from data_converge.state import StateStore

from .test_main import app, local_http_app


def ledger_dump(path):
    with closing(sqlite3.connect(f'{path.as_uri()}?mode=ro', uri=True)) as connection:
        return list(connection.iterdump())


def event_details(caplog, event):
    records = [row for row in caplog.records if row.getMessage().startswith(event + ' ')]
    assert len(records) == 1
    return json.loads(records[0].getMessage().split('diagnostic=', 1)[1])


def test_changed_binding_logs_evidence_and_preserves_running_work(local_http_app, caplog):
    path, requests = local_http_app
    application = Application(path)
    application.initialize('2026-10-12T10:00:00+08:00')
    definition = application.definition
    instance = definition.builder.create_instance(
        definition.dag, 'simple', datetime.fromisoformat('2026-10-12T10:06:00+08:00'))
    database = path.parent / 'state/state.sqlite3'
    with closing(StateStore(database)) as store:
        store.acquire_lock()
        store.enqueue_and_advance([instance], {})
        assert store.claim(instance.instance_id, now=instance.scheduled_time, max_attempts=3)
    before = ledger_dump(database)
    config = yaml.safe_load(path.read_text())
    config['clickhouse']['database'] = 'private-database-sentinel'
    config['clickhouse']['username'] = 'private-user-sentinel'
    path.write_text(yaml.safe_dump(config))
    assert entry.main(['--config', str(path)]) == 1
    detail = event_details(caplog, 'STATE_BINDING_REJECTED')
    assert detail['reasons'] == ['BINDING_MISMATCH']
    assert detail['stored_binding'] == application.fingerprint
    assert detail['current_binding'] == Application(path).fingerprint
    assert detail['current_task_count'] == 1
    assert detail['config_path'] == str(path) and detail['state_path'] == str(database)
    assert detail['coverage_start_present'] is True
    assert 'STATE_DEPLOYMENT_BINDING_CHANGED_OR_UNINITIALIZED' in caplog.text
    assert 'STATE_BINDING_VALIDATED' not in caplog.text
    assert 'private-database-sentinel' not in caplog.text
    assert 'private-user-sentinel' not in caplog.text
    assert 'synthetic-integration-secret' not in caplog.text
    assert requests == [] and ledger_dump(database) == before


@pytest.mark.parametrize('missing,reasons', [
    (['binding'], ['MISSING_BINDING']),
    (['coverage_start'], ['MISSING_COVERAGE_START']),
    (['binding', 'coverage_start'], ['MISSING_BINDING', 'MISSING_COVERAGE_START']),
])
def test_missing_metadata_is_distinguished_without_reinitializing(app, caplog, missing, reasons):
    Application(app).initialize('2026-10-12T10:00:00+08:00')
    database = app.parent / 'state/state.sqlite3'
    with closing(sqlite3.connect(database)) as connection, connection:
        connection.executemany('DELETE FROM metadata WHERE key=?', [(key,) for key in missing])
    before = ledger_dump(database)
    assert entry.main(['--config', str(app)]) == 1
    detail = event_details(caplog, 'STATE_BINDING_REJECTED')
    assert detail['reasons'] == reasons
    assert detail['coverage_start_present'] == ('coverage_start' not in missing)
    if 'binding' in missing:
        assert detail['stored_binding'] is None
    assert 'STATE_DEPLOYMENT_BINDING_CHANGED_OR_UNINITIALIZED' in caplog.text
    assert 'STATE_BINDING_VALIDATED' not in caplog.text
    assert ledger_dump(database) == before


def test_invalid_metadata_and_control_characters_cannot_inject_log_lines(app, caplog):
    renamed = app.with_name('app\nforged\u2028line.yaml')
    app.rename(renamed)
    Application(renamed).initialize('2026-10-12T10:00:00+08:00')
    with closing(StateStore(app.parent / 'state/state.sqlite3')) as store:
        store.acquire_lock()
        store.set_meta('binding', 'private-binding\nFORGED_LOG')
        store.set_meta('coverage_start', 'private-coverage\nFORGED_LOG')
    assert entry.main(['--config', str(renamed)]) == 1
    detail = event_details(caplog, 'STATE_BINDING_REJECTED')
    assert detail['stored_binding'] is None and detail['stored_binding_present'] is True
    assert detail['config_path'] == str(renamed)
    assert detail['reasons'] == ['BINDING_MISMATCH']
    assert 'private-binding' not in caplog.text and 'private-coverage' not in caplog.text
    record = next(row for row in caplog.records if row.getMessage().startswith('STATE_BINDING_REJECTED'))
    assert '\n' not in record.getMessage() and '\u2028' not in record.getMessage()


def test_valid_binding_logs_success_and_restart_does_not_repeat_sql(local_http_app, caplog, capsys):
    path, requests = local_http_app
    Application(path).initialize('2026-10-12T10:00:00+08:00')
    caplog.set_level(logging.INFO)
    command = ['--config', str(path), '--run-once', '--now', '2026-10-12T10:06:00+08:00']
    for expected_statuses in (['SUCCESS'], []):
        caplog.clear()
        assert entry.main(command) == 0
        detail = event_details(caplog, 'STATE_BINDING_VALIDATED')
        assert detail['reasons'] == []
        assert detail['stored_binding'] == detail['current_binding']
        assert 'STATE_BINDING_REJECTED' not in caplog.text
        result = json.loads(capsys.readouterr().out)
        assert [item['status'] for item in result['instances']] == expected_statuses
    assert len(requests) == 1


def test_invalid_coverage_does_not_log_validation_success(app, caplog):
    Application(app).initialize('2026-10-12T10:00:00+08:00')
    with closing(StateStore(app.parent / 'state/state.sqlite3')) as store:
        store.acquire_lock()
        store.set_meta('coverage_start', 'private-invalid-coverage')
    assert entry.main(['--config', str(app)]) == 1
    assert 'TIME_REQUIRES_ISO8601_WITH_TIMEZONE' in caplog.text
    assert 'STATE_BINDING_VALIDATED' not in caplog.text
    assert 'private-invalid-coverage' not in caplog.text
