"""Command submission never competes for the service's writer lock."""
import json
from contextlib import closing

import pytest

from data_converge import main as entry
from data_converge.application import Application
from data_converge.backfill.queue import Queue
from data_converge.state import StateStore

from .test_main import app


def options(*extra):
    return ['--backfill', '--task', 'simple', '--from', '2026-10-12T10:00:00+08:00',
            '--to', '2026-10-12T10:10:00+08:00', *extra]


def invoke(app, capsys, *args):
    code = entry.main(['--config', str(app), *args, '--format', 'json'])
    return code, json.loads(capsys.readouterr().out)


def test_dry_run_has_no_logging_state_or_remote_side_effects(app, capsys, monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail('Preview must not configure logging or open the runtime')
    monkeypatch.setattr(entry, 'LoggingSession', forbidden)
    monkeypatch.setattr(Application, '_active_state', forbidden)
    monkeypatch.setenv('LOG_PATH', str(app.parent / 'logs'))
    code, value = invoke(app, capsys, *options('--dry-run', '--recompute'))
    assert code == 0 and value['status'] == 'PREVIEW'
    assert value['data']['total'] == 2 and value['data']['recompute'] is True
    assert not (app.parent / 'state').exists() and not (app.parent / 'logs').exists()


def test_submission_does_not_take_running_service_ledger_lock(app, capsys, monkeypatch):
    application = Application(app)
    application.initialize('2026-10-12T10:00:00+08:00')
    with closing(StateStore(application.directory / 'state.sqlite3')) as store:
        store.acquire_startup_lock()
        before = store.path.read_bytes()
        def forbidden(*args, **kwargs):
            pytest.fail('Submission must not initialize or lock the running service')
        monkeypatch.setattr(Application, '_active_state', forbidden)
        code, value = invoke(app, capsys, *options('--request-id', 'range-1'))
        assert code == 0 and value['status'] == 'QUEUED'
        job = value['data']['job_id']
        assert value['data']['total'] == 2
        assert store.path.read_bytes() == before
        code, repeated = invoke(app, capsys, *options('--request-id', 'range-1'))
        assert repeated['data']['job_id'] == job
    code, result = invoke(app, capsys, '--backfill-status', job, '--limit', '1')
    assert code == 0 and result['data']['has_more']
    assert result['data']['items'][0]['status'] == 'QUEUED'


def test_submit_requires_existing_state_but_does_not_create_it(app, capsys):
    code, value = invoke(app, capsys, *options())
    assert code == 1 and value['error']['error_code'] == 'BACKFILL_STATE_MISSING'
    assert not (app.parent / 'state').exists()


def test_status_missing_queue_and_job_are_clear_and_readonly(app, capsys):
    code, value = invoke(app, capsys, '--backfill-status', 'bf_missing')
    assert code == 1 and value['error']['error_code'] == 'BACKFILL_QUEUE_MISSING'
    assert not (app.parent / 'state').exists()
    directory = app.parent / 'state'
    directory.mkdir()
    with Queue(directory / 'backfill.sqlite3', create=True):
        pass
    code, value = invoke(app, capsys, '--backfill-status', 'bf_missing')
    assert code == 1 and value['error']['error_code'] == 'BACKFILL_JOB_NOT_FOUND'


def test_text_preview_and_submission_explain_no_immediate_sql(app, capsys):
    assert entry.main(['--config', str(app), *options('--dry-run')]) == 0
    preview = capsys.readouterr().out
    assert '预览' in preview and 'simple' in preview and '2' in preview
    Application(app).initialize('2026-10-12T10:00:00+08:00')
    assert entry.main(['--config', str(app), *options()]) == 0
    output = capsys.readouterr().out
    assert 'QUEUED' in output and '后台' in output and '--backfill-status' in output


def test_all_tasks_flag_and_terminal_progress_exit_status(app, capsys):
    code, preview = invoke(app, capsys, '--backfill', '--all-tasks',
                           '--from', '2026-10-12T10:00:00+08:00',
                           '--to', '2026-10-12T10:10:00+08:00', '--dry-run')
    assert code == 0 and preview['data']['selected_tasks'] == ['simple']
    Application(app).initialize('2026-10-12T10:00:00+08:00')
    _, submitted = invoke(app, capsys, *options())
    job = submitted['data']['job_id']
    with Queue(app.parent / 'state/backfill.sqlite3') as queue:
        queue.complete(queue.next_item()['item_id'], 'SUCCESS', query_id='success-id')
        queue.complete(queue.next_item()['item_id'], 'UNKNOWN', query_id='unknown-id')
    code, result = invoke(app, capsys, '--backfill-status', job)
    assert code == 1 and result['status'] == 'COMPLETED_WITH_ERRORS'
    assert result['data']['completed'] == 2
    assert result['data']['items'][1]['query_id'] == 'unknown-id'


def test_text_preview_lists_each_task_window_count_and_omissions():
    from data_converge.backfill.cli import _render

    value = {'status': 'PREVIEW', 'data': {
        'total': 12, 'start': '2026-10-12T10:00:00+08:00',
        'end': '2026-10-12T11:00:00+08:00', 'recompute': False,
        'selected_tasks': ['five_min', 'daily'], 'counts': {'five_min': 12, 'daily': 0},
        'omitted_tasks': ['daily'], 'items': [],
    }}
    rendered = _render(value)
    assert 'five_min：12 个窗口' in rendered
    assert 'daily：0 个窗口' in rendered
    assert '未纳入（范围内无完整窗口）：daily' in rendered


@pytest.mark.parametrize('extra', [
    ['--backfill'], options('--all-tasks'), options('--format', 'jsonl'),
    ['--task', 'simple'], ['--all-tasks'], ['--from', 'bad'], ['--to', 'bad'],
    ['--recompute'], ['--dry-run'], ['--request-id', 'id'],
    options('--limit', '1'), options('--offset', '1'), options('--online'),
    options('--run-once'), options('--request-id', '../bad'),
    ['--backfill', '--task', 'simple', '--from', 'bad', '--to', 'bad'],
    ['--backfill', '--task', 'simple', '--from', '2026-10-12T10:00:00',
     '--to', '2026-10-12T11:00:00+08:00'],
    ['--backfill-status', ''], ['--backfill-status', 'bf_a', '--task', 'simple'],
    ['--backfill-status', 'bf_a', '--dry-run'],
    ['--backfill-status', 'bf_a', '--all-tasks'],
    ['--backfill-status', 'bf_a', '--format', 'jsonl'],
])
def test_bad_arguments_are_rejected_before_config_loading(tmp_path, extra):
    missing = tmp_path / 'missing.yaml'
    with pytest.raises(SystemExit) as error:
        entry.main(['--config', str(missing), *extra])
    assert error.value.code == 2
    assert list(tmp_path.iterdir()) == []


def _leadership_submission(app, monkeypatch, leader=None, failure=None):
    from data_converge.backfill import cli as backfill_cli
    from .test_writer_coordination import enable_leadership

    enable_leadership(app)
    Application(app).initialize('2026-10-12T10:00:00+08:00')

    def read(_definition):
        if failure is not None:
            raise failure
        return leader

    monkeypatch.setattr(backfill_cli, '_read_leader', read)


def test_standby_submission_reports_that_only_the_leader_executes(app, capsys, monkeypatch):
    _leadership_submission(app, monkeypatch)
    assert entry.main(['--config', str(app), *options()]) == 0
    output = capsys.readouterr().out
    assert '请求只保存在本节点，只有本节点成为主节点后才会执行' in output
    assert '当前没有主节点' in output


def test_submission_with_a_known_leader_still_reports_the_local_only_queue(app, capsys, monkeypatch):
    _leadership_submission(app, monkeypatch, leader='another-node')
    code, value = invoke(app, capsys, *options())
    assert code == 0 and value['status'] == 'QUEUED'
    assert value['notices'] == ['请求只保存在本节点，只有本节点成为主节点后才会执行。']


def test_redis_read_failure_warns_without_blocking_submission(app, capsys, monkeypatch):
    _leadership_submission(app, monkeypatch, failure=OSError('redis down'))
    code, value = invoke(app, capsys, *options())
    assert code == 0 and value['status'] == 'QUEUED'
    assert '无法读取 Redis 选主状态' in value['notices'][0]
    assert '请求只保存在本节点' in value['notices'][0]
