"""Recovery previews and audit exports cannot change execution state."""
import json
from datetime import timedelta

import pytest

from data_converge import main as entry
from data_converge.application import Application

from .test_diagnostics import run_json, seed
from .test_main import app


def test_recovery_plan_classifies_current_reservations_without_mutating(app, capsys, monkeypatch):
    store, instances = seed(app, count=1)
    application = Application(app)
    builder, dag = application.definition.builder, application.definition.dag
    prepared, legacy = [builder.create_instance(dag, 'simple', instances[0].scheduled_time
                        + timedelta(minutes=n)) for n in (5, 10)]
    try:
        store.enqueue_and_advance([prepared, legacy], {})
        assert store.prepare_attempt(prepared.instance_id)
        assert store.claim(legacy.instance_id)
        monkeypatch.setenv('LOG_PATH', str(app.parent / 'readonly-logs'))
        before = store.path.read_bytes()
        code, result = run_json(app, capsys, '--recovery-plan', '--limit', '2')
        assert code == 0 and result['status'] == 'WARN'
        assert result['data']['counts'] == {'AUTO_RESUME': 1, 'RECONCILE': 2}
        assert result['data']['total'] == 3 and result['data']['has_more']
        _, page = run_json(app, capsys, '--recovery-plan', '--offset', '2')
        rows = result['data']['candidates'] + page['data']['candidates']
        actions = {row['instance_id']: row['action'] for row in rows}
        assert actions[prepared.instance_id] == 'AUTO_RESUME'
        assert actions[legacy.instance_id] == actions[instances[0].instance_id] == 'RECONCILE'
        assert store.path.read_bytes() == before
        assert not (app.parent / 'readonly-logs').exists()
    finally:
        store.close()


def test_events_are_ordered_cursor_pages_and_safe_jsonl(app, capsys):
    store, instances = seed(app, count=1)
    try:
        store.resolve(instances[0].instance_id, 'SUCCESS', 'password=private-note')
        before = store.path.read_bytes()
        code, page = run_json(app, capsys, '--events', '--limit', '1')
        assert code == 0 and page['data']['available']
        first = page['data']['events'][0]
        assert first['event_type'] == 'DISPATCHED' and page['data']['has_more']
        cursor = page['data']['next_after_event']
        assert entry.main(['--config', str(app), '--events', '--after-event', str(cursor),
                           '--format', 'jsonl']) == 0
        output = capsys.readouterr().out
        rows = [json.loads(line) for line in output.splitlines()]
        assert [row['event_type'] for row in rows] == ['OUTCOME_UNKNOWN', 'RESOLVED_SUCCESS']
        assert all(row['event_id'] > cursor and row['schema_version'] == 1 for row in rows)
        assert 'private-note' not in output and 'password' not in output
        assert store.path.read_bytes() == before
    finally:
        store.close()


def test_old_ledger_export_does_not_create_event_or_checkpoint_tables(app, capsys):
    store, _ = seed(app, count=1)
    try:
        with store._connection(write=True) as conn:
            conn.execute('DROP TABLE IF EXISTS execution_events')
        before = store.path.read_bytes()
        code, result = run_json(app, capsys, '--events')
        assert code == 0 and result['status'] == 'SKIP'
        assert result['data']['available'] is False and result['data']['events'] == []
        code, plan = run_json(app, capsys, '--recovery-plan')
        assert code == 0 and plan['data']['candidates'][0]['action'] == 'RECONCILE'
        assert store.path.read_bytes() == before
    finally:
        store.close()


def test_jsonl_failure_uses_stderr_without_polluting_export(app, capsys, monkeypatch):
    monkeypatch.setenv('LOG_PATH', str(app.parent / 'logs'))
    assert entry.main(['--config', str(app), '--events', '--format', 'jsonl']) == 1
    captured = capsys.readouterr()
    assert captured.out == ''
    assert json.loads(captured.err)['status'] == 'FAIL'
    assert not (app.parent / 'state').exists() and not (app.parent / 'logs').exists()


def test_recovery_text_explains_preview_and_query_identifier(app, capsys):
    store, _ = seed(app, count=1)
    store.close()
    assert entry.main(['--config', str(app), '--recovery-plan']) == 0
    output = capsys.readouterr().out
    assert '只读' in output and 'RECONCILE' in output and 'query_id=' in output


@pytest.mark.parametrize('options', [
    ['--events', '--after-event', '-1'], ['--history', '--after-event', '0'],
    ['--events', '--offset', '0'], ['--events', '--task', 'simple'],
    ['--recovery-plan', '--format', 'jsonl'], ['--history', '--format', 'jsonl'],
    ['--events', '--online'], ['--recovery-plan', '--run-once'],
])
def test_recovery_invalid_arguments_cannot_start_service(app, options):
    with pytest.raises(SystemExit) as error:
        entry.main(['--config', str(app), *options])
    assert error.value.code == 2
    assert not (app.parent / 'state').exists()
