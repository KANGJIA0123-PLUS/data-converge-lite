"""Early CLI admission reads existing state without acquiring its writer lock."""
from datetime import datetime
from contextlib import closing

import pytest

from data_converge.application import Application
from data_converge.state import StateStore
from .test_backfill_cli import invoke
from .test_main import app


class FixedClock(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls.fromisoformat('2026-10-21T10:32:00+08:00').astimezone(tz)


@pytest.fixture
def initialized(app, monkeypatch):
    from data_converge.backfill import plan
    application = Application(app)
    application.initialize('2026-10-20T00:00:00+08:00')
    monkeypatch.setattr(plan, 'datetime', FixedClock)
    return application


def options(*extra):
    return ('--backfill', '--preaggregate', '--task', 'simple',
            '--from', '2026-10-21T10:25:00+08:00', '--to', '2026-10-21T10:40:00+08:00', *extra)


def test_early_preview_reads_state_without_schema_or_lock_changes(app, initialized, capsys):
    path = initialized.directory / 'state.sqlite3'
    with closing(StateStore(path)) as store:
        store.acquire_startup_lock()
        before = path.read_bytes()
        code, value = invoke(app, capsys, *options('--dry-run'))
        assert code == 0 and value['status'] == 'PREVIEW'
        assert [item['execution_kind'] for item in value['data']['items']] == ['BACKFILL', 'PREAGGREGATE']
        assert value['data']['omitted_future_count'] == 1
        assert path.read_bytes() == before
    assert not (initialized.directory / 'backfill.sqlite3').exists()


def test_early_submission_and_status_expose_types_and_omissions(app, initialized, capsys):
    code, value = invoke(app, capsys, *options('--request-id', 'early-once'))
    assert code == 0 and value['data']['total'] == 2
    job = value['data']['job_id']
    _, repeated = invoke(app, capsys, *options('--request-id', 'early-once'))
    assert repeated['data']['job_id'] == job
    code, status = invoke(app, capsys, '--backfill-status', job)
    assert code == 0
    assert status['data']['metadata']['omitted_future_count'] == 1
    assert status['data']['items'][1]['execution_kind'] == 'PREAGGREGATE'


def test_early_preview_requires_coverage_state(app, capsys):
    code, value = invoke(app, capsys, *options('--dry-run'))
    assert code == 1 and value['error']['error_code'] == 'BACKFILL_STATE_MISSING'
    assert not (app.parent / 'state').exists()


def test_preaggregate_requires_backfill_before_config_loading(tmp_path):
    from data_converge import main
    with pytest.raises(SystemExit) as error:
        main.main(['--config', str(tmp_path / 'missing'), '--preaggregate'])
    assert error.value.code == 2
