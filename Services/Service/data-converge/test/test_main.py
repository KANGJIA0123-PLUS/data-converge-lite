import json
import os
from datetime import datetime

import pytest
import yaml

from data_converge import main as entry
from data_converge import application as operations
from data_converge import runtime
from data_converge.health import Heartbeat


@pytest.fixture
def app(tmp_path):
    sql = tmp_path / "sql"
    sql.mkdir()
    (sql / "simple.sql").write_text(
        "INSERT INTO target SELECT toDateTime({start_time:UInt32}, 'UTC'), "
        "toDateTime({end_time:UInt32}, 'UTC')")
    rules = tmp_path / "rules.yaml"
    rules.write_text(yaml.safe_dump({"convergence_rules": [{
        "taskname": "simple", "source_tables": ["source"], "target_table": "target",
        "aggregation_interval": "PT5M", "cron": "1,6,11,16,21,26,31,36,41,46,51,56 * * * *",
        "depends_on": [], "sql_id": "simple"}]}))
    path = tmp_path / "app.yaml"
    path.write_text(yaml.safe_dump({
        "clickhouse": {"host": "127.0.0.1", "port": 8123, "database": "example",
                       "username": "example", "password_file": "absent-secret"},
        "state": {"directory": str(tmp_path / "state")},
        "config": {"convergence_rules_path": str(rules), "sql_directory": str(sql)},
        "scheduler": {}}))
    return path


def test_offline_validation_does_not_decrypt_or_create_state(app, monkeypatch, capsys):
    def forbidden(*args, **kwargs):
        pytest.fail("offline validation must not construct SQL runner")
    monkeypatch.setattr(runtime, "SQLTaskRunner", forbidden)
    assert entry.main(["--config", str(app), "--validate-only"]) == 0
    assert not (app.parent / "state").exists()
    assert json.loads(capsys.readouterr().out)["tasks"] == 1


def test_application_returns_results_without_writing_cli_output(app, capsys):
    from data_converge.application import Application

    application = Application(app)
    result = application.validate()
    assert result.exit_code == 0
    assert result.payload == {'status': 'VALID', 'tasks': 1}
    assert capsys.readouterr().out == ''
    assert not (app.parent / 'state').exists()


def test_state_init_is_explicit_and_cannot_reset_coverage(app, capsys):
    args = ["--config", str(app), "--init-state", "--start-time", "2026-10-12T10:00:00+08:00"]
    assert entry.main(args) == 0
    capsys.readouterr()
    assert entry.main(["--config", str(app), "--status"]) == 0
    snapshot = json.loads(capsys.readouterr().out)
    assert snapshot["coverage_start"] == "2026-10-12T10:00:00+08:00"
    assert entry.main(args[:-1] + ["2026-10-12T11:00:00+08:00"]) == 1
    assert entry.main(args) == 0


def test_normal_start_reuses_coverage_and_completed_tasks(local_http_app, monkeypatch):
    path, _ = local_http_app
    application = operations.Application(path)
    application.initialize('2026-10-12T10:00:00+08:00')
    assert application.run_once('2026-10-12T10:06:00+08:00').exit_code == 0
    before = application.status().payload
    monkeypatch.setattr(runtime.TaskScheduler, 'start', lambda self: None)

    assert operations.Application(path).serve().exit_code == 0
    assert operations.Application(path).status().payload == before


@pytest.mark.parametrize('content', [b'', b'not-a-sqlite-ledger'])
def test_normal_start_does_not_rebuild_invalid_database(app, content):
    directory = app.parent / 'state'
    directory.mkdir()
    database = directory / 'state.sqlite3'
    database.write_bytes(content)
    assert entry.main(['--config', str(app)]) == 1
    assert database.read_bytes() == content


def test_normal_start_reports_lost_initialized_state(app, caplog):
    import shutil

    assert entry.main(['--config', str(app), '--init-state']) == 0
    shutil.rmtree(app.parent / 'state')
    assert entry.main(['--config', str(app)]) == 1
    assert 'STATE_DATABASE_MISSING' in caplog.text
    assert not (app.parent / 'state' / 'state.sqlite3').exists()


def test_init_rejects_naive_or_partial_window_start(app):
    for timestamp in ["2026-10-12T10:00:00", "2026-10-12T10:01:00+08:00"]:
        assert entry.main(["--config", str(app), "--init-state", "--start-time", timestamp]) == 1
    assert not (app.parent / "state" / "state.sqlite3").exists()


def test_changed_database_cannot_reuse_state(app):
    assert entry.main(["--config", str(app), "--init-state"]) == 0
    config = yaml.safe_load(app.read_text())
    config["clickhouse"]["database"] = "different"
    app.write_text(yaml.safe_dump(config))
    assert entry.main(["--config", str(app), "--init-state"]) == 1


def test_status_does_not_create_missing_state(app):
    assert entry.main(["--config", str(app), "--status"]) == 1
    assert not (app.parent / "state" / "state.sqlite3").exists()


def test_health_rejects_missing_dead_or_stale_heartbeat(app):
    assert entry.main(["--config", str(app), "--healthcheck", "liveness"]) == 1
    state = app.parent / "state"
    state.mkdir()
    (state / "heartbeat.json").write_text(json.dumps({"pid": os.getpid(), "time": 1, "ready": True}))
    assert entry.main(["--config", str(app), "--healthcheck", "readiness"]) == 1


def test_heartbeat_tracks_readiness_and_removes_on_shutdown(app):
    state = app.parent / "state"
    state.mkdir()
    with Heartbeat(state, interval=0.02) as heartbeat:
        assert entry.main(["--config", str(app), "--healthcheck", "liveness"]) == 0
        assert entry.main(["--config", str(app), "--healthcheck", "readiness"]) == 1
        heartbeat.set_ready(True)
        assert entry.main(["--config", str(app), "--healthcheck", "readiness"]) == 0
    assert entry.main(["--config", str(app), "--healthcheck", "liveness"]) == 1


def test_next_boundary_preserves_an_already_aligned_time():
    value = datetime.fromisoformat("2026-10-12T10:00:00+08:00")
    assert operations.next_boundary(value) == value
    assert operations.next_boundary(value.replace(minute=3)).minute == 5


@pytest.fixture
def local_http_app(app, monkeypatch):
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    import threading
    seen = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = self.rfile.read(int(self.headers['Content-Length'])).decode()
            seen.append((self.path, body))
            payload = b'{"data":[[1]]}' if body == 'SELECT 1' else b''
            self.send_response(200)
            self.send_header('Content-Length', str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    config = yaml.safe_load(app.read_text())
    config['clickhouse'].pop('password_file')
    config['clickhouse'].update(port=server.server_port, password_env='LITE_TEST_PASSWORD')
    app.write_text(yaml.safe_dump(config))
    monkeypatch.setenv('LITE_TEST_PASSWORD', 'synthetic-integration-secret')
    try:
        yield app, seen
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_cli_http_window_runs_once_across_restart(local_http_app, capsys):
    from urllib.parse import parse_qs, urlsplit
    app, seen = local_http_app
    base = ['--config', str(app)]
    assert entry.main(base + ['--init-state', '--start-time', '2026-10-12T10:00:00+08:00']) == 0
    capsys.readouterr()
    run = base + ['--run-once', '--now', '2026-10-12T10:06:00+08:00']
    assert entry.main(run) == 0
    result = json.loads(capsys.readouterr().out)
    assert len(result['instances']) == 1
    assert result['instances'][0]['status'] == 'SUCCESS'
    assert entry.main(run) == 0
    assert len(seen) == 1
    params = parse_qs(urlsplit(seen[0][0]).query)
    assert params['query_id'] == [result['instances'][0]['query_id']]
    assert params['param_start_time'] == ['1791770400']
    assert params['param_end_time'] == ['1791770700']
    assert entry.main(base + ['--run-task', 'simple', '--scheduled-time', '2026-10-12T10:01:00+08:00']) == 1
    assert len(seen) == 1


def test_run_uses_one_configuration_snapshot(local_http_app, monkeypatch):
    app, seen = local_http_app
    base = ['--config', str(app)]
    assert entry.main(base + ['--init-state', '--start-time', '2026-10-12T10:00:00+08:00']) == 0
    read_config = operations.load_app_config

    def replace_file_after_read(path):
        config = read_config(path)
        app.write_text('replacement: not-a-valid-application-config\n')
        return config

    monkeypatch.setattr(operations, 'load_app_config', replace_file_after_read)
    assert entry.main(base + ['--run-once', '--now', '2026-10-12T10:06:00+08:00']) == 0
    assert len(seen) == 1


def enable_ready(app):
    config = yaml.safe_load(app.read_text())
    config['dwd_ready'] = {
        'enabled': True,
        'redis': {'host': '127.0.0.1', 'port': 6379, 'mode': 'standalone', 'db': 0,
                  'password_env': 'UNSET_READY_PASSWORD', 'ssl': False},
    }
    app.write_text(yaml.safe_dump(config))


def test_ready_validation_and_init_are_offline(app, monkeypatch):
    enable_ready(app)

    def forbidden(*_, **__):
        pytest.fail('offline commands must not connect Redis or read its password')

    monkeypatch.setattr(runtime, 'create_source', forbidden)
    assert entry.main(['--config', str(app), '--validate-only']) == 0
    assert entry.main(['--config', str(app), '--init-state']) == 0


def test_shipped_platform_config_validates_all_60_tasks_with_ready_enabled(monkeypatch, capsys):
    for name, value in {'CLICKHOUSE_URL': 'offline.invalid', 'CLICKHOUSE_PORT': '8443',
                        'DWD_REDIS_HOST': 'offline.invalid', 'DWD_REDIS_PORT': '6379'}.items():
        monkeypatch.setenv(name, value)
    assert entry.main(['--config', str(entry.DEFAULT_APP_CONFIG_PATH), '--validate-only']) == 0
    assert json.loads(capsys.readouterr().out)['tasks'] == 60


def test_ready_cli_event_cron_and_manual_share_gate_and_success_ledger(local_http_app, monkeypatch, capsys):
    from .test_dwd_ready_coordinator import Source, signals

    app, seen = local_http_app
    enable_ready(app)
    source = Source(signals()[:4])
    monkeypatch.setattr(runtime, 'create_source', lambda _: source)
    base = ['--config', str(app)]
    assert entry.main(base + ['--init-state', '--start-time', '2026-10-12T10:00:00+08:00']) == 0
    run = base + ['--run-once', '--now', '2026-10-12T10:06:00+08:00']
    assert entry.main(run) == 1
    assert seen == []
    source.messages.extend(signals()[4:])
    assert entry.main(run) == 0
    assert len(seen) == 1
    source.messages.extend(signals())
    assert entry.main(run) == 0
    assert len(seen) == 1
    assert entry.main(base + ['--run-task', 'simple', '--scheduled-time', '2026-10-12T10:06:00+08:00']) == 0
    assert len(seen) == 1
    assert entry.main(base + ['--status']) == 0
    status = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert len(status['dwd_windows'][0]['types']) == 5
    assert source.closed


def test_partial_runtime_startup_releases_source_heartbeat_and_signal_handlers(local_http_app, monkeypatch):
    import signal
    from .test_dwd_ready_coordinator import Source

    app, _ = local_http_app
    enable_ready(app)
    source = Source([])
    handlers = {signum: signal.getsignal(signum) for signum in (signal.SIGTERM, signal.SIGINT)}
    monkeypatch.setattr(runtime, 'create_source', lambda _: source)

    def fail_coordinator(*args, **kwargs):
        raise RuntimeError('coordinator startup failed')

    monkeypatch.setattr(runtime, 'ReadyCoordinator', fail_coordinator)
    base = ['--config', str(app)]
    assert entry.main(base + ['--init-state']) == 0
    assert entry.main(base + ['--run-once']) == 1
    assert source.closed
    assert not (app.parent / 'state' / 'heartbeat.json').exists()
    assert all(signal.getsignal(signum) is handler for signum, handler in handlers.items())


def test_ready_mode_cannot_be_disabled_to_bypass_existing_state(app):
    enable_ready(app)
    assert entry.main(['--config', str(app), '--init-state']) == 0
    config = yaml.safe_load(app.read_text())
    config['dwd_ready'] = {'enabled': False}
    app.write_text(yaml.safe_dump(config))
    assert entry.main(['--config', str(app), '--init-state']) == 1


@pytest.mark.parametrize('preinitialize', [False, True])
def test_real_foreground_service_becomes_ready_and_handles_sigterm(local_http_app, preinitialize):
    import signal
    import subprocess
    import sys
    import time
    from pathlib import Path
    app, seen = local_http_app
    base = ['--config', str(app)]
    if preinitialize:
        assert entry.main(base + ['--init-state']) == 0
    env = os.environ.copy()
    env['PYTHONPATH'] = str(Path(entry.__file__).parent.parent)
    with (app.parent / 'service.log').open('w+') as logs:
        process = subprocess.Popen([sys.executable, '-m', 'data_converge.main', *base],
                                   env=env, stdout=logs, stderr=logs)
        try:
            deadline = time.monotonic() + 8
            while time.monotonic() < deadline:
                if entry.main(base + ['--healthcheck', 'readiness']) == 0:
                    break
                if process.poll() is not None:
                    logs.seek(0)
                    pytest.fail(logs.read())
                time.sleep(0.05)
            else:
                pytest.fail('service never became ready')
            assert any(body == 'SELECT 1' for _, body in seen)
            process.send_signal(signal.SIGTERM)
            assert process.wait(timeout=5) == 0
            assert entry.main(base + ['--healthcheck', 'liveness']) == 1
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)


@pytest.mark.parametrize('invalid_rules', [
    {'convergence_rule': []},
    {'convergence_rules': []},
    {'convergence_rules': [{'taskname': 'simple', 'source_tables': ['source'],
                           'target_table': 'target', 'aggregation_interval': 'PT10M',
                           'cron': '*/10 * * * *', 'sql_id': 'simple'}]},
])
def test_invalid_or_empty_rules_rejected_before_state_init(app, invalid_rules):
    (app.parent / 'rules.yaml').write_text(yaml.safe_dump(invalid_rules))
    assert entry.main(['--config', str(app), '--validate-only']) == 1
    assert entry.main(['--config', str(app), '--init-state']) == 1
    assert not (app.parent / 'state').exists()


def test_sigterm_during_heartbeat_write_cannot_deadlock(local_http_app):
    import subprocess
    import sys
    from pathlib import Path
    app, _ = local_http_app
    assert entry.main(['--config', str(app), '--init-state']) == 0
    script = '''
import json, os, signal, sys, threading
from pathlib import Path
from data_converge.main import main
original = Path.write_text
sent = False
def injected(path, text, *args, **kwargs):
    global sent
    if (path.name == 'heartbeat.tmp' and not sent
            and threading.current_thread() is threading.main_thread()
            and json.loads(text).get('ready')):
        sent = True
        os.kill(os.getpid(), signal.SIGTERM)
    return original(path, text, *args, **kwargs)
Path.write_text = injected
raise SystemExit(main(['--config', sys.argv[1]]))
'''
    env = {**os.environ, 'PYTHONPATH': str(Path(entry.__file__).parent.parent)}
    process = subprocess.Popen([sys.executable, '-c', script, str(app)], env=env,
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        assert process.wait(timeout=3) == 0
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=3)
