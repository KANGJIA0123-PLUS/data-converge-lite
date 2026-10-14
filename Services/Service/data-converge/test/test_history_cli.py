"""History queries stay read-only and distinguish attempts from current alerts."""
import json
from datetime import timedelta

import pytest

from data_converge import main as entry
from data_converge.dag import TaskStatus

from .test_diagnostics import run_json, seed
from .test_main import app


def test_history_lists_success_and_unknown_as_successful_read(app, capsys):
    store, instances = seed(app, count=3)
    try:
        before = store.path.read_bytes()
        code, first = run_json(app, capsys, '--history', '--task', 'simple', '--limit', '2')
        assert code == 0 and first['status'] == 'PASS'
        assert [row['instance_id'] for row in first['data']['attempts']] == [
            instances[2].instance_id, instances[1].instance_id]
        assert first['data']['has_more'] and first['data']['offset'] == 0
        code, last = run_json(app, capsys, '--history', '--limit', '2', '--offset', '2')
        assert code == 0 and last['data']['attempts'][0]['status'] == 'UNKNOWN'
        assert not last['data']['has_more']
        code, empty = run_json(app, capsys, '--history', '--task', 'other')
        assert code == 0 and empty['data']['attempts'] == []
        assert store.path.read_bytes() == before
    finally:
        store.close()


def test_inspect_paginates_attempts_and_keeps_resolution_separate(app, capsys):
    store, instances = seed(app, count=1)
    instance = instances[0]
    try:
        store.resolve(instance.instance_id, 'PENDING', 'private operator password')
        store.claim(instance.instance_id, now=instance.data_end + timedelta(seconds=1))
        store.finish(instance.instance_id, TaskStatus.SUCCESS)
        code, result = run_json(app, capsys, '--inspect-instance', instance.instance_id,
                                '--limit', '1', '--offset', '1')
        assert code == 0 and result['data']['instance']['status'] == 'SUCCESS'
        history = result['data']['history']
        assert history['limit'] == 1 and history['offset'] == 1 and not history['has_more']
        old = history['attempts'][0]
        assert old['status'] == 'UNKNOWN'
        assert old['resolution']['resolution'] == 'PENDING'
        assert old['resolution']['note_recorded'] is True
        assert 'private operator password' not in json.dumps(result)
    finally:
        store.close()


def test_history_defaults_to_twenty_attempts(app, capsys):
    store, instances = seed(app, count=21)
    store.close()
    code, result = run_json(app, capsys, '--history')
    assert code == 0 and result['data']['limit'] == 20
    assert len(result['data']['attempts']) == 20 and result['data']['has_more']
    code, result = run_json(app, capsys, '--inspect-instance', instances[-1].instance_id)
    assert code == 0 and result['data']['history']['limit'] == 20


def test_old_ledger_skips_history_without_creating_or_backfilling_it(app, capsys):
    store, instances = seed(app, count=1)
    with store._connection(write=True) as conn:
        conn.execute('DROP TABLE execution_attempts')
        conn.execute('DROP TABLE execution_resolutions')
    try:
        before = store.path.read_bytes()
        code, result = run_json(app, capsys, '--history')
        assert code == 0 and result['status'] == 'SKIP'
        assert result['data']['available'] is False and result['data']['attempts'] == []
        code, detail = run_json(app, capsys, '--inspect-instance', instances[0].instance_id)
        assert code == 1 and detail['data']['instance']['status'] == 'UNKNOWN'
        assert detail['data']['history']['available'] is False
        assert any(item['id'] == 'execution-history' and item['status'] == 'SKIP'
                   for item in detail['checks'])
        assert store.path.read_bytes() == before and not store.history_available()
    finally:
        store.close()


def test_corrupt_history_does_not_hide_current_instance_or_leak_details(app, capsys):
    store, instances = seed(app, count=1)
    with store._connection(write=True) as conn:
        conn.execute('UPDATE execution_attempts SET diagnostic=?', ('private invalid JSON',))
    store.close()
    code, result = run_json(app, capsys, '--inspect-instance', instances[0].instance_id)
    assert code == 1 and result['data']['instance']['status'] == 'UNKNOWN'
    assert any(item['id'] == 'execution-history' and item['status'] == 'FAIL'
               for item in result['checks'])
    assert 'private invalid JSON' not in json.dumps(result)
    code, result = run_json(app, capsys, '--history')
    assert code == 1 and result['status'] == 'FAIL'
    assert 'private invalid JSON' not in json.dumps(result)


def test_history_text_shows_safe_evidence_and_operator_resolution(app, capsys):
    store, instances = seed(app, count=1)
    store.resolve(instances[0].instance_id, 'SUCCESS', 'private resolution note')
    with store._connection(write=True) as conn:
        conn.execute('UPDATE execution_attempts SET diagnostic=?, error=?',
                     (json.dumps({'http_status': 503, 'sql': 'private SQL'}), 'HTTP_FAILED:private error'))
    store.close()
    assert entry.main(['--config', str(app), '--history']) == 0
    text = capsys.readouterr().out
    assert '执行历史' in text and 'UNKNOWN' in text and 'query_id:' in text
    assert '人工处理' in text and 'SUCCESS' in text and 'http_status=503' in text
    assert 'private' not in text


@pytest.mark.parametrize('options', [
    ['--history', '--limit', '101'], ['--history', '--offset', '-1'],
    ['--history', '--online'], ['--history', '--task', ''],
    ['--history', '--run-once'], ['--inspect-instance', 'id', '--task', 'simple'],
])
def test_history_invalid_options_never_start_work(app, options):
    with pytest.raises(SystemExit) as error:
        entry.main(['--config', str(app), *options])
    assert error.value.code == 2
    assert not (app.parent / 'state').exists()


@pytest.fixture
def legacy_history(app):
    store, instances = seed(app, count=2)
    try:
        with store._connection(write=True) as conn:
            conn.execute('DROP TABLE execution_attempts')
            conn.execute('DROP TABLE execution_resolutions')
            conn.execute("UPDATE task_instances SET status='UNKNOWN'")
        for instance in instances:
            store.resolve(instance.instance_id, 'SUCCESS', 'private legacy operator note')
        yield store, instances
    finally:
        store.close()


def test_history_lists_legacy_resolutions_without_inventing_attempts(app, capsys, legacy_history):
    store, instances = legacy_history
    before = store.path.read_bytes()
    code, result = run_json(app, capsys, '--history', '--task', 'simple', '--limit', '1')
    data = result['data']
    assert code == 0 and data['attempts'] == [] and not data['has_more']
    assert data['legacy_resolutions_has_more']
    assert data['legacy_resolutions'][0]['instance_id'] == instances[-1].instance_id
    assert data['legacy_resolutions'][0]['note_recorded'] is True
    assert 'private legacy operator note' not in json.dumps(result)
    code, result = run_json(app, capsys, '--history', '--limit', '1', '--offset', '1')
    assert code == 0 and not result['data']['legacy_resolutions_has_more']
    assert result['data']['legacy_resolutions'][0]['instance_id'] == instances[0].instance_id
    code, result = run_json(app, capsys, '--history', '--task', 'other')
    assert code == 0 and result['data']['legacy_resolutions'] == []
    assert store.path.read_bytes() == before


def test_inspect_and_text_expose_legacy_resolution_separately(app, capsys, legacy_history):
    _, instances = legacy_history
    code, result = run_json(app, capsys, '--inspect-instance', instances[0].instance_id)
    data = result['data']['history']
    assert code == 0 and data['attempts'] == []
    assert len(data['legacy_resolutions']) == 1 and not data['legacy_resolutions_has_more']
    assert data['legacy_resolutions'][0]['resolution'] == 'SUCCESS'
    assert entry.main(['--config', str(app), '--history', '--limit', '1']) == 0
    text = capsys.readouterr().out
    assert '旧版执行的人工处置' in text and 'SUCCESS' in text
    assert 'query_id:' in text and 'private legacy operator note' not in text
