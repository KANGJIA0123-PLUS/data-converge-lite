"""Exercise the actual archive builder and shell launchers in temporary directories."""

from __future__ import annotations

import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile

import pytest


ROOT = Path(__file__).resolve().parents[4]
BIN = ROOT / "Services/Package/images/bin"
BUILD = ROOT / "tools/build_package.py"


@pytest.fixture
def runtime(tmp_path):
    home = tmp_path / "service home"
    bin_dir = home / "bin"
    shutil.copytree(BIN, bin_dir)
    # The company init_env loads /etc/profile and writes to a platform log path.
    # Keep the launcher real and replace only that external platform boundary.
    (bin_dir / "function.sh").write_text(
        'export SERVICE_NAME=data-converge\n'
        'printf initialized > "$PLATFORM_INIT_RECORD"\n'
        'LOG_FILE="$PLATFORM_INIT_RECORD"\n'
        'show_title() { :; }\nlog() { :; }\n'
        'pre_check_service() { :; }\ninit_python() { :; }\n'
        'service_status_summary() { :; }\ncheck_result() { return "$1"; }\n'
    )
    src = home / "lib/data-converge/src/data_converge"
    src.mkdir(parents=True)
    (src / "__init__.py").write_text("")
    (src / "main.py").write_text(
        "import importlib.util, json, os, sys\n"
        "from pathlib import Path\n"
        "marker = None\n"
        "if importlib.util.find_spec('company_marker'):\n"
        "    import company_marker\n"
        "    marker = company_marker.ARCH\n"
        "data = {'argv': sys.argv[1:], 'pid': os.getpid(), "
        "'pythonpath': os.environ['PYTHONPATH'], 'env_loaded': os.environ.get('GDE_LOADED'), "
        "'arch': marker}\n"
        "Path(os.environ['CAPTURE_FILE']).write_text(json.dumps(data))\n"
        "sys.exit(int(os.environ.get('APP_EXIT_CODE', '0')))\n"
    )
    config = home / "lib/data-converge/resources/config/app config.yaml"
    config.parent.mkdir(parents=True)
    config.write_text("runtime: {}\n")
    runtime_bin = tmp_path / "gde runtime"
    runtime_bin.mkdir()
    (runtime_bin / "python3").symlink_to(sys.executable)
    # Old start.sh must not chmod anything outside the test directory.
    chmod = runtime_bin / "chmod"
    chmod.write_text('#!/bin/sh\necho "unexpected startup chmod" >&2\nexit 92\n')
    chmod.chmod(0o755)
    python_env = tmp_path / "python.env"
    python_env.write_text(
        f'export PATH="{runtime_bin}:$PATH"\nexport GDE_LOADED=yes\n'
    )
    capture = tmp_path / "capture.json"
    env = {
        **os.environ,
        "SERVICE_HOME": str(home),
        "PYTHON_ENV_FILE": str(python_env),
        "APP_CONF_FILE": str(config),
        "CAPTURE_FILE": str(capture),
        "PLATFORM_INIT_RECORD": str(tmp_path / "platform-init"),
        "PYTHONPATH": str(tmp_path / "existing imports"),
        "PATH": f"{runtime_bin}:{os.environ['PATH']}",
    }
    return home, runtime_bin, capture, env


def run_script(runtime, script, *args, exit_code="0"):
    home, _, capture, env = runtime
    process = subprocess.Popen(
        ["bash", str(home / "bin" / script), *args],
        env={**env, "APP_EXIT_CODE": exit_code},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        stdout, stderr = process.communicate(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.communicate()
        pytest.fail("launcher did not exit with its foreground application")
    data = json.loads(capture.read_text()) if capture.exists() else None
    return process, data, stdout, stderr


def dependency_archive(path, package, marker):
    path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(path, "w:gz") as archive:
        payload = f"ARCH = {marker!r}\n".encode()
        member = tarfile.TarInfo(f"{package}/company_marker.py")
        member.size = len(payload)
        archive.addfile(member, io.BytesIO(payload))


def test_start_forwards_config_and_arguments_and_replaces_shell(runtime):
    home, _, _, env = runtime
    process, data, _, stderr = run_script(runtime, "start.sh", "--once", "--label", "two words")
    assert process.returncode == 0, stderr
    assert data["argv"] == ["--config", env["APP_CONF_FILE"], "--once", "--label", "two words"]
    assert data["pid"] == process.pid
    assert data["env_loaded"] == "yes"
    assert data["pythonpath"].split(os.pathsep)[:2] == [
        str(home / "lib/data-converge/src"), str(home / "bin/py/site-packages")
    ]
    assert env["PYTHONPATH"] in data["pythonpath"].split(os.pathsep)
    assert Path(env["PLATFORM_INIT_RECORD"]).read_text() == "initialized"


@pytest.mark.parametrize("script,mode", [
    ("liveness_check.sh", "liveness"), ("readiness_check.sh", "readiness")
])
def test_probe_uses_same_interpreter_and_returns_application_exit(runtime, script, mode):
    _, _, _, env = runtime
    process, data, _, stderr = run_script(runtime, script, exit_code="17")
    assert process.returncode == 17, stderr
    assert data["argv"] == ["--config", env["APP_CONF_FILE"], "--healthcheck", mode]
    assert data["pid"] == process.pid
    assert data["env_loaded"] == "yes"


@pytest.mark.parametrize("machine,expected,other", [
    ("x86_64", "dependency_x86", "dependency_arm"),
    ("aarch64", "dependency_arm", "dependency_x86"),
])
def test_start_unpacks_correct_arch_and_restarts_without_archives(runtime, machine, expected, other):
    home, runtime_bin, _, _ = runtime
    uname = runtime_bin / "uname"
    uname.write_text(f"#!/bin/sh\nprintf '%s\\n' {machine}\n")
    uname.chmod(0o755)
    for name in [expected, other]:
        dependency_archive(home / "scripts" / f"{name}.tar.gz", name, name)
    dependency_archive(home / "bin/py/python_pkg.tar.gz", "site-packages", "site-packages")
    process, data, _, stderr = run_script(runtime, "start.sh")
    assert process.returncode == 0, stderr
    assert (home / "scripts" / expected / "company_marker.py").exists()
    assert not (home / "scripts" / other).exists()
    assert (home / "bin/py/site-packages/company_marker.py").exists()
    assert (home / "scripts" / f"{expected}.tar.gz").exists()
    # The site-package comes before the optional company SDK on PYTHONPATH.
    assert data["arch"] == "site-packages"
    for archive in home.rglob("*.tar.gz"):
        archive.unlink()
    for directory in [home / "bin", home / "bin/py", home / "scripts"]:
        directory.chmod(0o555)
    process, _, _, stderr = run_script(runtime, "start.sh")
    assert process.returncode == 0, stderr


def test_probe_does_not_install_dependencies(runtime):
    home, _, _, _ = runtime
    dependency_archive(home / "bin/py/python_pkg.tar.gz", "site-packages", "site-packages")
    process, _, _, stderr = run_script(runtime, "liveness_check.sh", exit_code="17")
    assert process.returncode == 17, stderr
    assert not (home / "bin/py/site-packages").exists()


def test_package_contains_source_and_all_sql_without_modifying_checkout(tmp_path):
    source = ROOT / "Services/Service/data-converge/src/data_converge/main.py"
    before = hashlib.sha256(source.read_bytes()).hexdigest()
    output = tmp_path / "build outputs/data-converge-lite.tar.gz"
    result = subprocess.run(
        [sys.executable, str(BUILD), "--output", str(output)], capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr
    assert hashlib.sha256(source.read_bytes()).hexdigest() == before
    with tarfile.open(output) as archive:
        members = archive.getnames()
        assert "bin/start.sh" in members
        assert "lib/data-converge/src/data_converge/main.py" in members
        assert "lib/data-converge/resources/config/app_config.yaml" in members
        assert len([p for p in members if p.startswith("lib/data-converge/resources/sql/") and p.endswith(".sql")]) == 52
        assert not any("__pycache__" in p or p.endswith(".pyc") for p in members)
        assert archive.getmember("bin/start.sh").mode & 0o111


def test_builder_rejects_output_that_would_replace_source():
    target = ROOT / "Services/Service/data-converge/resources/config/app_config.yaml"
    before = target.read_bytes()
    result = subprocess.run(
        [sys.executable, str(BUILD), "--output", str(target)], capture_output=True, text=True
    )
    assert result.returncode != 0
    assert target.read_bytes() == before


def test_framework_build_forwards_output_path_without_platform_build_tools(tmp_path):
    output = tmp_path / "custom path/package.tar.gz"
    result = subprocess.run(
        ["bash", str(ROOT / "Services/build_framework/scripts/build.sh"), "--output", str(output)],
        env={**os.environ, "REPOSITORY_PATH": str(ROOT), "PYTHON_BIN": sys.executable},
        capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr
    assert tarfile.is_tarfile(output)
