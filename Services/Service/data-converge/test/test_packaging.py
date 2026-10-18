"""Exercise the company bytecode build and shell launchers in temporary copies."""

from __future__ import annotations

import hashlib
import io
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tarfile

import pytest

from .paths import REPOSITORY_ROOT, RESOURCES_ROOT


ROOT = REPOSITORY_ROOT
BIN = ROOT / "Services/Package/images/bin"
BUILD = ROOT / "Services/build_framework/scripts/build.sh"


def write_platform_boundary(bin_dir):
    # The company init_env loads /etc/profile and writes to a platform log path.
    # Keep the launcher real and replace only that external platform boundary.
    (bin_dir / "function.sh").write_text(
        'export SERVICE_NAME=data-converge\n'
        'printf "init_env\\n" > "$PLATFORM_INIT_RECORD"\n'
        'LOG_FILE="$SERVICE_HOME/start.log"\n'
        'LOG_PATH="$SERVICE_HOME"\n'
        'show_title() { printf "title:%s\\n" "$1" >> "$PLATFORM_INIT_RECORD"; touch "$LOG_FILE"; }\n'
        'log() { :; }\n'
        'pre_check_service() { printf "pre_check\\n" >> "$PLATFORM_INIT_RECORD"; return "${PRECHECK_EXIT_CODE:-0}"; }\n'
        'init_python() { printf "init_python\\n" >> "$PLATFORM_INIT_RECORD"; return "${INIT_PYTHON_EXIT_CODE:-0}"; }\n'
        'service_status_summary() { printf "status:%s\\n" "$1" >> "$PLATFORM_INIT_RECORD"; }\n'
        'check_result() { return "$1"; }\n'
    )


def write_capture_application(home):
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


@pytest.fixture
def runtime(tmp_path):
    home = tmp_path / "service home"
    bin_dir = home / "bin"
    shutil.copytree(BIN, bin_dir)
    write_platform_boundary(bin_dir)
    (bin_dir / "py").mkdir()
    write_capture_application(home)
    config = home / "lib/data-converge/resources/config/app config.yaml"
    config.parent.mkdir(parents=True)
    config.write_text("runtime: {}\n")
    runtime_bin = tmp_path / "gde runtime"
    runtime_bin.mkdir()
    (runtime_bin / "python3").symlink_to(sys.executable)
    # Exercise real permissions, but forbid startup chmod outside this fixture.
    chmod = runtime_bin / "chmod"
    chmod.write_text(
        '#!/bin/bash\n'
        'for target in "$@"; do\n'
        '  case "$target" in\n'
        '    -v|[0-7][0-7][0-7]) ;;\n'
        '    "$SERVICE_HOME"/*) ;;\n'
        '    *) echo "startup chmod escaped test directory" >&2; exit 92 ;;\n'
        '  esac\n'
        'done\n'
        'exec /bin/chmod "$@"\n'
    )
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


def test_start_forwards_config_and_arguments_and_waits_for_child(runtime):
    home, _, _, env = runtime
    process, data, _, stderr = run_script(runtime, "start.sh", "--once", "--label", "two words")
    assert process.returncode == 0, stderr
    assert data["argv"] == ["--config", env["APP_CONF_FILE"], "--once", "--label", "two words"]
    assert data["pid"] != process.pid
    assert data["env_loaded"] == "yes"
    assert data["pythonpath"].split(os.pathsep)[:2] == [
        str(home / "lib/data-converge/src"), str(home / "bin/py/site-packages")
    ]
    assert env["PYTHONPATH"] in data["pythonpath"].split(os.pathsep)
    assert Path(env["PLATFORM_INIT_RECORD"]).read_text().splitlines() == [
        "init_env", "title:start@data-converge", "pre_check", "init_python", "status:start",
    ]


@pytest.mark.parametrize("failure,expected_hooks", [
    ("PRECHECK_EXIT_CODE", ["init_env", "title:start@data-converge", "pre_check"]),
    ("INIT_PYTHON_EXIT_CODE", ["init_env", "title:start@data-converge", "pre_check", "init_python"]),
])
def test_platform_start_failure_prevents_application_and_start_status(runtime, failure, expected_hooks):
    _, _, _, env = runtime
    env[failure] = "47"
    process, data, _, _ = run_script(runtime, "start.sh")
    assert process.returncode == 47
    assert data is None
    assert Path(env["PLATFORM_INIT_RECORD"]).read_text().splitlines() == expected_hooks


def test_platform_start_restores_company_file_permissions(runtime):
    home, _, _, _ = runtime
    package = home / "bin/py/site-packages/example"
    package.mkdir(parents=True)
    module = package / "module.py"
    module.write_text("VALUE = 1\n")
    certificates = [package / ("test" + suffix) for suffix in (".pem", ".crt", ".key")]
    for certificate in certificates:
        certificate.write_text("synthetic")
    process, _, _, stderr = run_script(runtime, "start.sh")
    assert process.returncode == 0, stderr
    assert (home / "start.log").is_file()
    for path in (home / "bin", home / "lib", *home.glob("bin/*.sh")):
        assert path.stat().st_mode & 0o777 == 0o500
    assert package.stat().st_mode & 0o777 == 0o700
    assert module.stat().st_mode & 0o777 == 0o500
    assert all(path.stat().st_mode & 0o777 == 0o400 for path in certificates)


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
def test_business_launcher_unpacks_correct_arch_and_restarts_without_archives(runtime, machine, expected, other):
    home, runtime_bin, _, _ = runtime
    uname = runtime_bin / "uname"
    uname.write_text(f"#!/bin/sh\nprintf '%s\\n' {machine}\n")
    uname.chmod(0o755)
    for name in [expected, other]:
        dependency_archive(home / "scripts" / f"{name}.tar.gz", name, name)
    dependency_archive(home / "bin/py/python_pkg.tar.gz", "site-packages", "site-packages")
    # The platform start.sh separately calls its original init_python, which
    # requires python_pkg.tar.gz; archive-free reuse belongs to the direct helper.
    process, data, _, stderr = run_script(runtime, "start_data_converge.sh")
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
    process, _, _, stderr = run_script(runtime, "start_data_converge.sh")
    assert process.returncode == 0, stderr


def test_probe_does_not_install_dependencies(runtime):
    home, _, _, _ = runtime
    dependency_archive(home / "bin/py/python_pkg.tar.gz", "site-packages", "site-packages")
    process, _, _, stderr = run_script(runtime, "liveness_check.sh", exit_code="17")
    assert process.returncode == 17, stderr
    assert not (home / "bin/py/site-packages").exists()


def prepare_test_exclusion_fixture(repo, application):
    service = repo / application
    # Old image output must be cleaned even when it is absent from the source.
    image = repo / "Services/Package/images/lib/data-converge"
    for directory in ("test", "tests", "src/data_converge/__pycache__", ".pytest_cache"):
        stale = image / directory / "stale.pyc"
        stale.parent.mkdir(parents=True, exist_ok=True)
        stale.write_bytes(b"old build")
    return {path: path.read_bytes() for path in (service / "test").rglob("*.py")}


@pytest.fixture(scope="module")
def compiled_application(tmp_path_factory):
    temp = tmp_path_factory.mktemp("company_build")
    repo = temp / "test" / "repo"
    application = Path("Services/Service/data-converge")
    shutil.copytree(ROOT / application, repo / application,
                    ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "*.egg-info", ".pytest_cache"))
    test_sources = prepare_test_exclusion_fixture(repo, application)
    script = repo / "Services/build_framework/scripts/build.sh"
    script.parent.mkdir(parents=True)
    shutil.copyfile(BUILD, script)
    workspace = temp / "workspace"
    external = workspace / "BuildTool/Services/build_framework/scripts/function.sh"
    external.parent.mkdir(parents=True)
    external.write_text("# The uploaded framework's external BuildTool boundary.\n:\n")
    runtime_bin = temp / "runtime"
    runtime_bin.mkdir()
    # Preserve the active venv's pyvenv.cfg lookup when the launcher calls python3.
    python = runtime_bin / "python3"
    python.write_text('#!/bin/sh\nexec ' + shlex.quote(sys.executable) + ' "$@"\n')
    python.chmod(0o755)
    source = ROOT / application / "src/data_converge/main.py"
    before = hashlib.sha256(source.read_bytes()).hexdigest()
    env = {**os.environ, "REPOSITORY_PATH": str(repo), "WORKSPACE": str(workspace),
           "PATH": f"{runtime_bin}:{os.environ['PATH']}", "PYTHONDONTWRITEBYTECODE": "1"}
    built = subprocess.run(["bash", str(script)], cwd=temp, env=env, capture_output=True, text=True)
    assert built.returncode == 0, built.stderr
    assert test_sources and all(path.read_bytes() == data for path, data in test_sources.items())
    assert not list((repo / application / "test").rglob("*.pyc"))
    assert hashlib.sha256(source.read_bytes()).hexdigest() == before
    home = repo / "Services/Package/images"
    deployed = home / "lib/data-converge"
    assert_compiled_artifacts(deployed)
    shutil.copytree(BIN, home / "bin", dirs_exist_ok=True)
    python_env = temp / "python.env"
    python_env.write_text(f'export PATH="{runtime_bin}:$PATH"\n')
    env.update(SERVICE_HOME=str(home), PYTHON_ENV_FILE=str(python_env),
               APP_CONF_FILE=str(deployed / "resources/config/app_config.yaml"),
               PYTHONPATH=str(deployed / "src"),
               CLICKHOUSE_URL="offline.invalid", CLICKHOUSE_PORT="8443",
               DWD_REDIS_HOST="offline.invalid", DWD_REDIS_PORT="6379")
    return temp, home, deployed, env


def assert_compiled_artifacts(deployed):
    assert not list(deployed.rglob("*.py")), "core is deployed as company-framework bytecode"
    assert (deployed / "src/data_converge/main.pyc").is_file()
    assert (deployed / "src/data_converge/__init__.pyc").is_file()
    expected = {path.name for path in (RESOURCES_ROOT / "sql").glob("*.sql")}
    sql = list((deployed / "resources/sql").glob("*.sql"))
    assert len(expected) == 56
    assert {path.name for path in sql} == expected
    for path in sql:
        assert path.read_bytes() == (RESOURCES_ROOT / "sql" / path.name).read_bytes()
    assert (deployed / "resources/config/app_config.yaml").read_bytes() == (
        RESOURCES_ROOT / "config/app_config.yaml").read_bytes()


def test_original_framework_excludes_tests_from_image(compiled_application):
    _, home, _, _ = compiled_application
    excluded = {'test', 'tests', '__pycache__', '.pytest_cache'}
    assert not [path for path in home.rglob('*') if path.is_dir() and path.name in excluded]
    assert not list(home.rglob('test_*.pyc'))


def test_original_framework_bytecode_loads_all_56_tasks_outside_checkout(compiled_application):
    temp, _, deployed, env = compiled_application
    result = subprocess.run([sys.executable, "-c",
        "from pathlib import Path; from data_converge import main; "
        "assert Path(main.__file__).suffix == '.pyc'; "
        "assert Path(main.__file__).resolve().is_relative_to(Path(__import__('os').environ['PYTHONPATH'])); "
        "raise SystemExit(main.main(['--validate-only']))"],
        cwd=temp, env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["tasks"] == 56
    assert not list(deployed.rglob("*.whl"))


def test_launcher_runs_the_original_framework_bytecode(compiled_application):
    temp, home, _, env = compiled_application
    result = subprocess.run(["bash", str(home / "bin/start_data_converge.sh"), "--validate-only"],
                            cwd=temp, env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["tasks"] == 56


def test_xms_helper_runs_from_original_framework_bytecode(compiled_application):
    from .test_credentials import envelope, make_sdk
    temp, _, _, env = compiled_application
    sdk_dir = temp / "sdk"
    make_sdk(sdk_dir, "def xms_decrypt(ciphertext):\n    return " + repr(envelope()) + "\n")
    cipher = temp / "synthetic-cipher"
    cipher.write_text("synthetic ciphertext")
    result = subprocess.run([sys.executable, "-c",
        "import sys; from pathlib import Path; "
        "from data_converge.sql_execution import credentials, kms_worker; "
        "assert Path(credentials.__file__).suffix == '.pyc'; "
        "assert Path(kms_worker.__file__).suffix == '.pyc'; "
        "assert credentials.load_password({'password_file': sys.argv[1], "
        "'kms_timeout_seconds': 5, 'username': 'user', 'paas_name': 'clickhouse-pms-password'}) == ' exact password '; "
        "print('DECRYPT_OK')", str(cipher)], cwd=temp,
        env={**env, "PYTHONPATH": env["PYTHONPATH"] + os.pathsep + str(sdk_dir)},
        capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "DECRYPT_OK"
