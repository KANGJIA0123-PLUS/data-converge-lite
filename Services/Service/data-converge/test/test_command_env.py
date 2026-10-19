"""Source the command path helper without changing the caller's shell state."""

import subprocess

import pytest

from .test_packaging import run_script, runtime
from .test_startup import launcher


def source_environment(home, env, original_path):
    script = ('set -u\nPATH=$2\nshift 2\nset -- "two words" tail\n'
              'original_options=$-\noriginal_dir=$PWD\n'
              'source "$ENV_HELPER" || exit\nsource "$ENV_HELPER" || exit\n'
              '[[ "$PWD" == "$original_dir" && "$-" == "$original_options" ]] || exit 71\n'
              '[[ $# == 2 && "$1" == "two words" && "$2" == tail ]] || exit 72\n'
              'printf "%s\\n" "$PATH"')
    return subprocess.run(
        ['/bin/bash', '--noprofile', '--norc', '-c', script, 'path-test', 'unused', original_path],
        env={**env, 'ENV_HELPER': str(home / 'bin/command_env.sh'), 'CDPATH': str(home.parent)},
        cwd=home.parent, capture_output=True, text=True, timeout=10,
    )


@pytest.mark.parametrize('original', ['/custom/tools:/usr/bin:/bin', '', '/custom/tools::/bin'])
def test_environment_appends_without_replacing_or_duplicating_path(runtime, original):
    home, _, capture, env = runtime
    result = source_environment(home, env, original)
    assert result.returncode == 0, result.stderr
    assert result.stdout.rstrip('\n') == original + (':' if original else '') + str(home / 'bin')
    assert not capture.exists(), 'loading PATH must not start the application'


def test_environment_keeps_existing_service_path_in_place(runtime):
    home, _, _, env = runtime
    original = f'/custom/tools:{home}/bin:/usr/bin:/bin'
    result = source_environment(home, env, original)
    assert result.returncode == 0, result.stderr
    assert result.stdout.rstrip('\n') == original


def test_startup_exports_appended_path_without_changing_platform_tool_priority(launcher):
    home, runtime_bin, _, env = launcher
    original = env['PATH']
    application = home / 'lib/data-converge/src/data_converge/main.py'
    application.write_text('import json, os\nfrom pathlib import Path\n'
                           'Path(os.environ["CAPTURE_FILE"]).write_text(json.dumps({"path": os.environ["PATH"]}))\n')
    process, data, _, stderr = run_script(launcher, 'start.sh')
    assert process.returncode == 0, stderr
    entries = data['path'].split(':')
    assert data['path'].endswith(original + ':' + str(home / 'bin'))
    assert entries.count(str(home / 'bin')) == 1
    assert entries.index(str(runtime_bin)) < entries.index(str(home / 'bin'))
