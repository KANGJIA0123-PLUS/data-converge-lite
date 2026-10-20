"""Debug logging is opt-in, scoped to this service, and reversible."""

import io
import json
import logging

import pytest
import yaml

from data_converge import main as entry
from data_converge.application import CommandResult
from data_converge.config.application import binding, load_definition
from data_converge.config.evolution import build_manifest
from data_converge.observability.logging import LoggingSession
from data_converge.observability.settings import LoggingSettings

from .test_main import app


@pytest.fixture(autouse=True)
def clean_logging_environment(monkeypatch):
    for name in ('LOG_PATH', 'SERVICE_LOG_PATH', 'PRODUCT_NAME', 'SERVICE_NAME'):
        monkeypatch.delenv(name, raising=False)
    names = ('', 'data_converge', 'data_converge.sql_debug_test', 'debug_test_dependency')
    loggers = [logging.getLogger(name) for name in names]
    before = [(logger, logger.level, logger.handlers[:]) for logger in loggers]
    for logger in loggers:
        logger.setLevel(logging.NOTSET)
    loggers[0].setLevel(logging.WARNING)
    loggers[1].setLevel(logging.ERROR)
    yield
    for logger, level, handlers in before:
        logger.setLevel(level)
        logger.handlers[:] = handlers


def test_default_logging_keeps_info_and_errors_without_debug(tmp_path, monkeypatch):
    monkeypatch.setenv('LOG_PATH', str(tmp_path))
    logger = logging.getLogger('data_converge.sql_debug_test')
    with LoggingSession() as session:
        session.configure(LoggingSettings.from_mapping(None))
        logger.debug('default hidden debug')
        logger.info('default visible info')
        logger.error('default visible error')
    text = (tmp_path / 'data-converge.log').read_text()
    assert 'default hidden debug' not in text
    assert 'default visible info' in text
    assert 'default visible error' in text


def test_reconfigure_toggles_debug_without_enabling_dependency_debug(tmp_path, monkeypatch):
    monkeypatch.setenv('LOG_PATH', str(tmp_path))
    logger = logging.getLogger('data_converge.sql_debug_test')
    dependency = logging.getLogger('debug_test_dependency')
    with LoggingSession() as session:
        for index, enabled in enumerate((False, True, False)):
            session.configure(LoggingSettings.from_mapping({'debug_enabled': enabled}))
            logger.debug('debug phase %s', index)
            logger.info('info phase %s', index)
            logger.error('error phase %s', index)
            dependency.debug('dependency debug must remain hidden')
            assert logging.getLogger().level == logging.INFO
    text = (tmp_path / 'data-converge.log').read_text()
    assert 'debug phase 0' not in text and 'debug phase 2' not in text
    assert text.count('debug phase 1') == 1
    assert text.count('info phase') == 3 and text.count('error phase') == 3
    assert 'dependency debug' not in text


@pytest.mark.parametrize('with_file', [False, True])
def test_session_restores_root_and_application_levels_and_handlers(tmp_path, monkeypatch, with_file):
    if with_file:
        monkeypatch.setenv('LOG_PATH', str(tmp_path))
    root, application = logging.getLogger(), logging.getLogger('data_converge')
    stream = io.StringIO()
    console = logging.StreamHandler(stream)
    console.setLevel(logging.DEBUG)
    application.addHandler(console)
    original = [(logger, logger.level, logger.handlers[:]) for logger in (root, application)]
    try:
        with LoggingSession() as session:
            session.configure(LoggingSettings.from_mapping({'debug_enabled': True}))
            logging.getLogger('data_converge.sql_debug_test').debug('active debug')
        assert 'active debug' in stream.getvalue()
        for logger, level, handlers in original:
            assert logger.level == level
            assert logger.handlers == handlers
        assert console.level == logging.DEBUG
        logging.getLogger('data_converge.sql_debug_test').debug('after cleanup')
        assert 'after cleanup' not in stream.getvalue()
    finally:
        application.removeHandler(console)
        console.close()


@pytest.mark.parametrize('invalid', [None, 0, 1, 'true', 'false', [], {}])
def test_debug_flag_rejects_non_boolean_configuration(invalid):
    with pytest.raises(ValueError, match='^LOGGING_SETTING_INVALID$'):
        LoggingSettings.from_mapping({'debug_enabled': invalid})


@pytest.mark.parametrize('enabled', [False, True])
def test_cli_reads_debug_flag_and_preserves_json_stdout(app, monkeypatch, capsys, enabled):
    monkeypatch.setenv('LOG_PATH', str(app.parent / 'logs'))
    raw = yaml.safe_load(app.read_text())
    raw['logging'] = {'debug_enabled': enabled}
    app.write_text(yaml.safe_dump(raw))

    def application(_, *, raw_config):
        logging.getLogger('data_converge.sql_debug_test').debug('cli debug detail')
        return CommandResult(payload={'status': 'VALID', 'tasks': 1})

    monkeypatch.setattr(entry, 'run', application)
    assert entry.main(['--config', str(app), '--validate-only']) == 0
    assert json.loads(capsys.readouterr().out) == {'status': 'VALID', 'tasks': 1}
    text = (app.parent / 'logs/data-converge.log').read_text()
    assert ('cli debug detail' in text) is enabled


@pytest.mark.parametrize('enabled', [False, True])
def test_diagnose_reports_debug_flag_without_creating_log_files(app, monkeypatch, capsys, enabled):
    directory = app.parent / 'diagnostic-logs'
    monkeypatch.setenv('LOG_PATH', str(directory))
    raw = yaml.safe_load(app.read_text())
    raw['logging'] = {'debug_enabled': enabled}
    app.write_text(yaml.safe_dump(raw))
    entry.main(['--config', str(app), '--diagnose', '--format', 'json'])
    result = json.loads(capsys.readouterr().out)
    item = next(item for item in result['checks'] if item['id'] == 'logging.settings')
    assert item['status'] == 'PASS'
    assert item['evidence']['debug_enabled'] is enabled
    assert not directory.exists()


def test_debug_toggle_does_not_change_binding_or_task_manifest(app):
    raw = yaml.safe_load(app.read_text())
    definitions = []
    for enabled in (False, True):
        raw['logging'] = {'debug_enabled': enabled}
        definitions.append(load_definition(app, raw_config=raw))
    before, after = definitions
    assert binding(before.config, before.dag) == binding(after.config, after.dag)
    assert build_manifest(before) == build_manifest(after)
