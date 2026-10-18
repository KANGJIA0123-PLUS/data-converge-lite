"""Execution events share real SQLite transactions and expose only safe facts."""

import json
import sqlite3
from contextlib import closing
from datetime import datetime

import pytest

from data_converge.state import events


WHEN = datetime.fromisoformat('2026-10-18T15:04:05.123456+08:00')


@pytest.fixture
def ledger(tmp_path):
    path = tmp_path / 'state.sqlite3'
    with closing(sqlite3.connect(path)) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute('''CREATE TABLE task_instances (
            instance_id TEXT PRIMARY KEY, taskname TEXT, query_id TEXT,
            attempts INTEGER, status TEXT, error TEXT, resolution_note TEXT)''')
        with conn:
            conn.execute('INSERT INTO task_instances VALUES (?, ?, ?, ?, ?, ?, ?)',
                         ('root@window', 'root', 'query-1', 0, 'PENDING',
                          'password=private-error', 'private-operator-note'))
        yield path, conn


def test_event_and_business_change_roll_back_together(ledger):
    _, conn = ledger
    with pytest.raises(RuntimeError, match='interrupt transaction'):
        with conn:
            conn.execute("UPDATE task_instances SET status='RUNNING'")
            events.append_event(conn, 'PREPARED', 'root@window', WHEN)
            raise RuntimeError('interrupt transaction')
    assert conn.execute('SELECT status FROM task_instances').fetchone()[0] == 'PENDING'
    assert not events.available(conn)
    assert events.list_events(conn) == []


def test_event_is_not_published_before_the_caller_commits(ledger):
    path, conn = ledger
    with conn:
        conn.execute("UPDATE task_instances SET status='RUNNING', attempts=1")
        events.append_event(conn, 'DISPATCHED', 'root@window', WHEN)
        assert conn.in_transaction
        with closing(sqlite3.connect(path)) as reader:
            assert not events.available(reader)
            assert reader.execute('SELECT status FROM task_instances').fetchone()[0] == 'PENDING'
    with closing(sqlite3.connect(path)) as reader:
        reader.row_factory = sqlite3.Row
        assert events.list_events(reader)[0]['event_type'] == 'DISPATCHED'


def test_event_keeps_a_safe_utc_snapshot_after_current_state_changes(ledger):
    _, conn = ledger
    with conn:
        conn.execute("UPDATE task_instances SET status='RUNNING', attempts=2")
        events.append_event(conn, 'DISPATCHED', 'root@window', WHEN)
    with conn:
        conn.execute("UPDATE task_instances SET status='SUCCESS', query_id='query-2'")
        conn.execute('ALTER TABLE execution_events ADD COLUMN raw_error TEXT')
        conn.execute("UPDATE execution_events SET raw_error='private-future-field'")
    assert events.list_events(conn) == [{
        'event_id': 1, 'schema_version': 1, 'occurred_at': '2026-10-18T07:04:05.123456+00:00',
        'event_type': 'DISPATCHED', 'instance_id': 'root@window', 'taskname': 'root',
        'query_id': 'query-1', 'attempt_number': 2, 'status': 'RUNNING',
    }]
    assert 'private' not in json.dumps(events.list_events(conn))


def test_old_read_only_ledger_does_not_create_event_schema(ledger):
    path, _ = ledger
    before = path.read_bytes()
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)) as reader:
        assert not events.available(reader)
        assert events.list_events(reader, after_event=12, limit=100) == []
    assert path.read_bytes() == before


@pytest.mark.parametrize('after_event,limit', [
    (-1, 20), (True, 20), (1.5, 20), ('1', 20), (0, 0), (0, 101),
    (0, True), (0, 1.5), (0, '20'),
])
def test_invalid_event_pagination_is_rejected_without_creating_schema(ledger, after_event, limit):
    _, conn = ledger
    with pytest.raises(ValueError):
        events.list_events(conn, after_event=after_event, limit=limit)
    assert not events.available(conn)


def test_event_pages_follow_sequence_and_accept_all_protocol_transitions(ledger):
    _, conn = ledger
    kinds = ['PREPARED', 'DISPATCHED', 'SUCCEEDED', 'FAILED', 'OUTCOME_UNKNOWN',
             'RECOVERED_UNSUBMITTED', 'RECOVERY_REQUIRED', 'RESOLVED_SUCCESS', 'RETRY_AUTHORIZED']
    for kind in kinds:
        with conn:
            conn.execute("UPDATE task_instances SET status='RUNNING'")
            events.append_event(conn, kind, 'root@window', WHEN.isoformat())
    first = events.list_events(conn, limit=2)
    second = events.list_events(conn, after_event=first[-1]['event_id'], limit=100)
    assert [row['event_id'] for row in first + second] == list(range(1, 10))
    assert [row['event_type'] for row in first + second] == kinds
    assert events.list_events(conn, after_event=9) == []


def test_cursor_beyond_sqlite_integer_range_is_an_empty_page(ledger):
    _, conn = ledger
    with conn:
        conn.execute("UPDATE task_instances SET status='RUNNING'")
        events.append_event(conn, 'PREPARED', 'root@window', WHEN)
    assert events.list_events(conn, after_event=2**100) == []


@pytest.mark.parametrize('kind,when', [
    ('password=private', WHEN), ('SUCCEEDED\nprivate', WHEN),
    ('PREPARED', datetime(2026, 10, 18)), ('PREPARED', '2026-10-18T12:00:00'),
    ('PREPARED', 'invalid-time'), ('PREPARED', None),
])
def test_invalid_event_metadata_cannot_create_an_event(ledger, kind, when):
    _, conn = ledger
    with pytest.raises(ValueError):
        with conn:
            conn.execute("UPDATE task_instances SET status='RUNNING'")
            events.append_event(conn, kind, 'root@window', when)
    assert not events.available(conn)


def test_append_requires_the_callers_transaction_before_creating_schema(ledger):
    _, conn = ledger
    with pytest.raises(RuntimeError):
        events.append_event(conn, 'PREPARED', 'root@window', WHEN)
    assert not events.available(conn)


def test_unknown_instance_cannot_silently_drop_an_event(ledger):
    _, conn = ledger
    with pytest.raises(KeyError):
        with conn:
            conn.execute("UPDATE task_instances SET status='RUNNING'")
            events.append_event(conn, 'PREPARED', 'missing-instance', WHEN)
    assert not events.available(conn)
