"""Exercise the installed short command from a fresh container-like shell."""

import json
from pathlib import Path
import subprocess

import pytest

from data_converge.main import arguments
from .paths import REPOSITORY_ROOT
from .test_packaging import compiled_application, runtime


def command_environment(home, env):
    result = dict(env)
    settings = REPOSITORY_ROOT / 'Services/Package/charts/templates/env_setting_data_converge.conf'
    for line in settings.read_text().splitlines():
        fields = line.split(';')
        if fields[:2] == ['PATH', 'value']:
            result['PATH'] = fields[2].replace('/opt/its/data-converge', str(home))
    return result


def run_dc(runtime, *args, exit_code='0'):
    home, _, capture, env = runtime
    process = subprocess.run(
        ['bash', '--noprofile', '--norc', '-c', 'exec dc "$@"', 'dc-test', *args],
        cwd=home.parent, env=command_environment(home, {**env, 'APP_EXIT_CODE': exit_code}),
        capture_output=True, text=True, timeout=10,
    )
    data = json.loads(capture.read_text()) if capture.exists() else None
    return process, data


def test_new_shell_finds_service_command_before_system_commands(runtime):
    home, _, _, env = runtime
    process = subprocess.run(
        ['bash', '--noprofile', '--norc', '-c', 'command -v dc'], cwd=home.parent,
        env=command_environment(home, env), capture_output=True, text=True, timeout=10,
    )
    assert process.returncode == 0, process.stderr
    assert process.stdout.strip() == str(home / 'bin/dc')


@pytest.mark.parametrize('options', [
    ('--history',), ('--history', '--limit', '50'), ('--diagnose',),
    ('--diagnose', '--online'), ('--failures',), ('--dwd-status',),
])
def test_dc_forwards_queries_and_uses_default_configuration(runtime, options):
    home, _, _, env = runtime
    env.pop('APP_CONF_FILE')
    process, data = run_dc(runtime, *options)
    assert process.returncode == 0, process.stderr
    assert data['argv'] == ['--config', str(home / 'lib/data-converge/resources/config/app_config.yaml'), *options]
    assert data['env_loaded'] == 'yes'


@pytest.mark.parametrize('exit_code', ['0', '1', '2'])
def test_dc_preserves_explicit_config_and_application_exit(runtime, exit_code):
    home, _, _, _ = runtime
    custom = home / 'custom config.yaml'
    process, data = run_dc(runtime, '--config', str(custom), '--history', exit_code=exit_code)
    assert process.returncode == int(exit_code), process.stderr
    assert arguments(data['argv']).config == custom
    assert arguments(data['argv']).history is True


def test_dc_preserves_environment_configuration(runtime):
    _, _, _, env = runtime
    process, data = run_dc(runtime, '--history')
    assert process.returncode == 0, process.stderr
    assert arguments(data['argv']).config == Path(env['APP_CONF_FILE'])


def test_dc_without_arguments_shows_help_instead_of_starting_service(runtime):
    process, data = run_dc(runtime)
    assert process.returncode == 0, process.stderr
    assert data['argv'][-1] == '--help'


def test_dc_reports_python_environment_failure(runtime):
    _, _, _, env = runtime
    env['PYTHON_ENV_FILE'] = '/nonexistent/data-converge/python.env'
    process, data = run_dc(runtime, '--history')
    assert process.returncode == 1
    assert data is None
    assert 'Python environment file does not exist' in process.stderr


def test_startup_restores_short_command_execute_permission(runtime):
    home, _, _, env = runtime
    command = home / 'bin/dc'
    command.write_text('#!/bin/bash\nexit 0\n')
    command.chmod(0o600)
    script = home / 'bin/start.sh'
    # Redirect only the platform's fixed install path into this temporary tree.
    script.write_text(script.read_text().replace('/opt/its/${SERVICE_NAME}', '${SERVICE_HOME}')
                      .replace('main "$@"', 'config_permission'))
    result = subprocess.run(['bash', str(script)], env=env, capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr
    assert command.stat().st_mode & 0o777 == 0o500


def test_dc_runs_packaged_bytecode_with_default_configuration(compiled_application):
    temp, home, _, env = compiled_application
    env = command_environment(home, env)
    env.pop('APP_CONF_FILE')
    result = subprocess.run(['dc', '--validate-only'], cwd=temp, env=env,
                            capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)['tasks'] == 56
    result = subprocess.run(['dc'], cwd=temp, env=env, capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr
    assert '--history' in result.stdout
