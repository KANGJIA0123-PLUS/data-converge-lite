"""Check platform log routing without starting external services."""

import json
import logging
import sys

import pytest
import yaml

from data_converge import main as entry
from data_converge.application import CommandResult

from .test_main import app


@pytest.fixture(autouse=True)
def clear_log_environment(monkeypatch):
    for name in ('LOG_PATH', 'SERVICE_LOG_PATH', 'PRODUCT_NAME', 'SERVICE_NAME'):
        monkeypatch.delenv(name, raising=False)


@pytest.mark.parametrize('directory,filename,service,expected', [
    ('logs', 'ignored.log', 'data-converge', 'logs/data-converge.log'),
    ('logs', None, 'custom-service', 'logs/custom-service.log'),
    ('logs', None, None, 'logs/data-converge.log'),
    (None, 'platform/service.log', None, 'platform/service.log'),
])
def test_service_errors_reach_configured_log(app, monkeypatch, directory, filename, service, expected):
    for key, value in (('LOG_PATH', directory), ('SERVICE_LOG_PATH', filename)):
        if value:
            monkeypatch.setenv(key, str(app.parent / value))
    if service:
        monkeypatch.setenv('SERVICE_NAME', service)
    assert entry.main(['--config', str(app), '--status']) == 1
    path = app.parent / expected
    assert path.is_file()
    assert 'SERVICE_FAILED:' in path.read_text(encoding='utf-8')


def test_info_logs_append_without_changing_stdout_or_leaking_handlers(app, monkeypatch, capsys):
    monkeypatch.setenv('LOG_PATH', str(app.parent / 'logs'))
    root = logging.getLogger()
    console = logging.StreamHandler(sys.stderr)
    root.addHandler(console)
    handlers, level = root.handlers[:], root.level

    def application(_, *, raw_config):
        logging.getLogger('data_converge.scheduler').info('调度已启动')
        return CommandResult(payload={'status': 'STARTED'})

    monkeypatch.setattr(entry, 'run', application)
    try:
        for _ in range(2):
            assert entry.main(['--config', str(app)]) == 0
            captured = capsys.readouterr()
            assert json.loads(captured.out) == {'status': 'STARTED'}
            assert '调度已启动' in captured.err
            assert root.handlers == handlers
            assert root.level == level
        text = (app.parent / 'logs/data-converge.log').read_text(encoding='utf-8')
        assert text.count('INFO data_converge.scheduler: 调度已启动') == 2
    finally:
        root.removeHandler(console)
        console.close()


def test_external_rotation_keeps_new_messages_in_active_file(app, monkeypatch):
    path = app.parent / 'data-converge.log'
    rotated = app.parent / 'data-converge.log.1'
    monkeypatch.setenv('SERVICE_LOG_PATH', str(path))

    def application(_, *, raw_config):
        logger = logging.getLogger('data_converge.scheduler')
        logger.info('before rotation')
        assert path.is_file()
        path.rename(rotated)
        logger.info('after rotation')
        return CommandResult()

    monkeypatch.setattr(entry, 'run', application)
    assert entry.main(['--config', str(app)]) == 0
    assert 'before rotation' in rotated.read_text()
    assert 'after rotation' not in rotated.read_text()
    assert 'after rotation' in path.read_text()


def test_invalid_log_directory_reports_failure(app, monkeypatch, caplog):
    blocked = app.parent / 'logs'
    blocked.write_text('a file cannot be a log directory')
    monkeypatch.setenv('LOG_PATH', str(blocked))
    assert entry.main(['--config', str(app), '--validate-only']) == 1
    assert 'SERVICE_FAILED: FileExistsError' in caplog.text


def test_local_cli_without_platform_variables_still_works(app, capsys):
    assert entry.main(['--config', str(app), '--validate-only']) == 0
    assert json.loads(capsys.readouterr().out) == {'status': 'VALID', 'tasks': 1}


def test_logging_policy_is_loaded_from_application_config(app, monkeypatch):
    directory = app.parent / 'logs'
    monkeypatch.setenv('LOG_PATH', str(directory))
    config = yaml.safe_load(app.read_text())
    config['logging'] = {'max_bytes': 128, 'backup_count': 2, 'retention_days': 7}
    app.write_text(yaml.safe_dump(config))
    for _ in range(5):
        assert entry.main(['--config', str(app), '--status']) == 1
    archives = list(directory.glob('data-converge.log.*.archive'))
    assert len(archives) == 2
    assert 'SERVICE_FAILED:' in (directory / 'data-converge.log').read_text()


def test_invalid_logging_policy_fails_validation_and_keeps_error_log(app, monkeypatch):
    monkeypatch.setenv('LOG_PATH', str(app.parent / 'logs'))
    config = yaml.safe_load(app.read_text())
    config['logging'] = {'max_bytes': 0}
    app.write_text(yaml.safe_dump(config))
    assert entry.main(['--config', str(app), '--validate-only']) == 1
    assert 'SERVICE_FAILED:' in (app.parent / 'logs/data-converge.log').read_text()


def test_log_close_failure_keeps_exit_code_and_restores_root_state(app, monkeypatch, capsys, caplog):
    monkeypatch.setenv('LOG_PATH', str(app.parent / 'logs'))
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level

    def fail_flush():
        raise OSError('private filesystem details')

    def application(_, *, raw_config):
        for handler in root.handlers:
            if hasattr(handler, 'policy'):
                monkeypatch.setattr(handler.stream, 'flush', fail_flush)
        return CommandResult(exit_code=17)

    monkeypatch.setattr(entry, 'run', application)
    assert entry.main(['--config', str(app)]) == 17
    assert root.handlers == handlers
    assert root.level == level
    assert 'LOGGING_CLOSE_FAILED' in caplog.text
    assert 'private filesystem details' not in caplog.text + capsys.readouterr().err
