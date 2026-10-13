"""Check process ownership without requiring the company's container image."""

from contextlib import suppress
import os
from pathlib import Path
import signal
import subprocess
import time

import pytest

from .test_packaging import run_script, runtime


@pytest.fixture
def launcher(runtime):
    home, _, _, _ = runtime
    script = home / "bin/start.sh"
    # Only replace the container's fixed-path setup; run the real service flow.
    script.write_text(script.read_text().replace(
        "#代码入口",
        'config_permission() { :; }\nconfig_python() { :; }\n#代码入口',
    ))
    with (home / "bin/function.sh").open("a") as platform:
        platform.write('log() { printf "%s\\n" "$2" >> "$LOG_FILE"; }\n')
    return runtime


@pytest.mark.parametrize("script", ["start.sh", "start_data_converge.sh"])
@pytest.mark.parametrize("exit_code", ["0", "17"])
def test_launcher_returns_child_status_and_preserves_arguments(launcher, script, exit_code):
    home, _, _, env = launcher
    process, data, _, stderr = run_script(launcher, script, "--label", "two words", exit_code=exit_code)
    assert process.returncode == int(exit_code), stderr
    assert data["argv"] == ["--config", env["APP_CONF_FILE"], "--label", "two words"]
    assert data["env_loaded"] == "yes"
    assert data["pid"] != process.pid
    if script == "start.sh":
        assert "Start service success" in (home / "start.log").read_text()
        assert Path(env["PLATFORM_INIT_RECORD"]).read_text().splitlines() == [
            "init_env", "title:start@data-converge", "pre_check", "init_python", "status:start",
        ]


def wait_for_file(path, process):
    deadline = time.monotonic() + 5
    while not path.exists():
        assert process.poll() is None, "launcher exited before the child finished"
        assert time.monotonic() < deadline, f"child did not create {path.name}"
        time.sleep(0.01)


@pytest.mark.parametrize("script", ["start.sh", "start_data_converge.sh"])
@pytest.mark.parametrize("stop_signal", [signal.SIGTERM, signal.SIGINT])
def test_launcher_waits_for_child_cleanup_after_stop(launcher, script, stop_signal):
    home, _, capture, env = launcher
    application = home / "lib/data-converge/src/data_converge/main.py"
    application.write_text(
        "import os, signal, time\nfrom pathlib import Path\n"
        "marker = Path(os.environ['CAPTURE_FILE'])\n"
        "def stop(signum, frame):\n"
        "    marker.with_suffix('.stopping').touch()\n"
        "signal.signal(signal.SIGTERM, stop)\n"
        "marker.write_text(str(os.getpid()))\n"
        "while not marker.with_suffix('.release').exists():\n"
        "    time.sleep(0.01)\n"
        "marker.with_suffix('.closed').touch()\n"
        "raise SystemExit(23)\n"
    )
    process = subprocess.Popen(
        ["bash", str(home / "bin" / script)], env=env, start_new_session=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    try:
        wait_for_file(capture, process)
        process.send_signal(stop_signal)
        wait_for_file(capture.with_suffix(".stopping"), process)
        assert process.poll() is None
        capture.with_suffix(".release").touch()
        _, stderr = process.communicate(timeout=5)
        assert process.returncode == 23, stderr
        assert capture.with_suffix(".closed").exists()
        with pytest.raises(ProcessLookupError):
            os.kill(int(capture.read_text()), 0)
    finally:
        with suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
        process.communicate(timeout=5)


def test_runtime_setup_failure_does_not_start_child(launcher):
    _, _, _, env = launcher
    env["PYTHON_ENV_FILE"] = "/nonexistent/data-converge/python.env"
    process, data, _, stderr = run_script(launcher, "start_data_converge.sh")
    assert process.returncode == 1
    assert data is None
    assert "Python environment file does not exist" in stderr


def test_launcher_exports_company_log_directory_to_python(launcher):
    home, _, _, env = launcher
    for name in ('LOG_PATH', 'PRODUCT_NAME'):
        env.pop(name, None)
    with (home / 'bin/function.sh').open('a') as platform:
        platform.write('PRODUCT_NAME=MindOpsBOSE\n')
    application = home / 'lib/data-converge/src/data_converge/main.py'
    application.write_text(
        'import json, os\nfrom pathlib import Path\n'
        "data = {key: os.getenv(key) for key in ('LOG_PATH', 'SERVICE_NAME', 'PRODUCT_NAME')}\n"
        "Path(os.environ['CAPTURE_FILE']).write_text(json.dumps(data))\n"
    )
    process, data, _, stderr = run_script(launcher, 'start.sh')
    assert process.returncode == 0, stderr
    assert data == {'LOG_PATH': str(home), 'SERVICE_NAME': 'data-converge', 'PRODUCT_NAME': 'MindOpsBOSE'}
