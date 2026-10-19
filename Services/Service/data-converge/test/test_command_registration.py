"""Start once, then query from an independent Bash without manually sourcing files."""

import json
from pathlib import Path
import subprocess

import pytest

from .test_packaging import run_script, runtime
from .test_startup import launcher


def fresh_bash(runtime, *shell_options):
    home, _, _, env = runtime
    return subprocess.run(
        ['/bin/bash', *shell_options, '-c',
         'printf "%s\\n" "${USER_SETTING:-missing}"; command -v dconv; dconv --history'],
        cwd=home.parent, env=env, capture_output=True, text=True, timeout=10,
    )


def test_start_enables_new_interactive_terminal_and_preserves_bashrc(launcher):
    home, _, capture, env = launcher
    bashrc = Path(env['HOME']) / '.bashrc'
    original = 'export USER_SETTING=kept'  # No final newline in the user's file.
    bashrc.write_text(original)
    started, _, _, stderr = run_script(launcher, 'start.sh')
    assert started.returncode == 0, stderr
    registered = bashrc.read_text()
    started, _, _, stderr = run_script(launcher, 'start.sh')
    assert started.returncode == 0, stderr
    assert bashrc.read_text() == registered, 'restarting must not duplicate the hook'
    assert registered.startswith(original + '\n')
    capture.unlink()
    result = fresh_bash(launcher, '--noprofile', '-i')
    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == ['kept', str(home / 'bin/dconv')]
    assert json.loads(capture.read_text())['argv'] == ['--config', env['APP_CONF_FILE'], '--history']


@pytest.mark.parametrize('profile_name', ['.bash_profile', '.bash_login', '.profile', None])
def test_start_enables_login_shell_without_shadowing_existing_profile(launcher, profile_name):
    _, _, _, env = launcher
    user_home = Path(env['HOME'])
    if profile_name:
        profile = user_home / profile_name
        profile.write_text('export USER_SETTING=login-kept')
    started, _, _, stderr = run_script(launcher, 'start.sh')
    assert started.returncode == 0, stderr
    result = fresh_bash(launcher, '--login')
    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines()[0] == ('login-kept' if profile_name else 'missing')
    if profile_name != '.bash_profile':
        assert not (user_home / '.bash_profile').exists()
    if profile_name not in ('.bash_profile', '.bash_login'):
        assert not (user_home / '.bash_login').exists()


@pytest.mark.parametrize('blocked', ['readonly', 'symlink', 'directory', 'missing_home'])
def test_registration_problem_warns_without_preventing_service_start(launcher, blocked):
    home, _, _, env = launcher
    bashrc = Path(env['HOME']) / '.bashrc'
    protected = home.parent / 'protected-profile'
    protected.write_text('# unchanged\n')
    if blocked == 'readonly':
        bashrc.write_text('# unchanged\n')
        bashrc.chmod(0o400)
    elif blocked == 'symlink':
        bashrc.symlink_to(protected)
    elif blocked == 'directory':
        bashrc.mkdir()
    else:
        env['HOME'] = str(home.parent / 'missing-home')
    started, data, _, stderr = run_script(launcher, 'start.sh')
    assert started.returncode == 0, stderr
    assert data is not None, 'a convenience command must not block the service'
    assert 'COMMAND_ENV_REGISTRATION_FAILED' in (home / 'start.log').read_text()
    assert protected.read_text() == '# unchanged\n'
    if blocked == 'readonly':
        assert bashrc.read_text() == '# unchanged\n'


def test_generated_profile_is_safe_for_posix_shell(launcher):
    _, _, _, env = launcher
    started, _, _, stderr = run_script(launcher, 'start.sh')
    assert started.returncode == 0, stderr
    result = subprocess.run(
        ['/bin/sh', '-c', 'unset BASH_VERSION; . "$HOME/.profile"'],
        env=env, capture_output=True, text=True, timeout=10,
    )
    assert result.returncode == 0, result.stderr
