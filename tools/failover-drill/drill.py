"""Two-instance failover drill on one PC: real service processes, local Redis, mock ClickHouse.

Scenario
  1. Start Redis and a mock ClickHouse (aggregation INSERTs take a few seconds).
  2. Start instance A and B with the same config but separate state directories
     (as on two nodes). One wins the lease; the other stays standby.
  3. Feed DWD readiness signals so five-minute windows become runnable.
  4. While the leader is mid-SQL, kill -9 it.
  5. The standby takes over: imports shared Redis state, KILLs the interrupted
     query, reruns it, and continues with later windows.
  6. Verify: every window aggregated successfully exactly once in the final
     state, no SUCCESS window was re-executed, the interrupted window was rerun,
     a deposed writer could not write shared state, and pool settings were sent.

Usage (macOS / Linux, Python 3.12 with the service dependencies installed):
    pip install "fakeredis[lua]"        # only if redis-server is not installed
    python3 tools/failover-drill/drill.py
Results are printed and saved to tools/failover-drill/out/report.json.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import socket
import sqlite3
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
SERVICE = ROOT / 'Services/Service/data-converge'
OUT = HERE / 'out'
PY = sys.executable
CLUSTER = 'drill'
DWD_TYPES = ('GPU_REALTIME', 'NODE_REALTIME', 'REQUEST_REALTIME',
             'MODELINSTANCE_REALTIME', 'MODELMETRIC_REALTIME')


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def wait_port(port: int, timeout: float = 15) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with socket.socket() as sock:
            if sock.connect_ex(('127.0.0.1', port)) == 0:
                return
        time.sleep(0.1)
    raise SystemExit(f'port {port} did not open')


def write_rules(directory: Path) -> Path:
    """Two five-minute tasks and one hourly task: same-period and cross-period links."""
    sql = directory / 'sql'
    sql.mkdir(parents=True, exist_ok=True)
    for name, table in (('m5_a', 'db.t_a'), ('m5_b', 'db.t_b'), ('h1', 'db.t_h')):
        (sql / f'{name}.sql').write_text(
            f'INSERT INTO {table} SELECT 1 WHERE window_time >= {{start_time:DateTime}} '
            'AND window_time < {end_time:DateTime}')
    rules = directory / 'rules.yaml'
    five = '1,6,11,16,21,26,31,36,41,46,51,56 * * * *'
    rules.write_text(json.dumps({'convergence_rules': [
        dict(taskname='m5_a', source_tables=['raw'], target_table='t_a', aggregation_interval='PT5M',
             cron=five, depends_on=[], sql_id='m5_a'),
        dict(taskname='m5_b', source_tables=['t_a'], target_table='t_b', aggregation_interval='PT5M',
             cron=five, depends_on=['m5_a'], sql_id='m5_b'),
        dict(taskname='h1', source_tables=['t_b'], target_table='t_h', aggregation_interval='PT1H',
             cron='6 * * * *', depends_on=['m5_b'], sql_id='h1'),
    ]}))
    return rules


def write_config(directory: Path, name: str, redis_port: int, ch_port: int, rules: Path) -> Path:
    config = {
        'logging': {'max_bytes': 1048576, 'backup_count': 5, 'max_total_bytes': 10485760},
        'clickhouse': {'host': '127.0.0.1', 'port': ch_port, 'database': 'db', 'username': 'u',
                       'password_env': 'CH_PASSWORD', 'secure': False, 'verify': True},
        'clickhouse_pool': {'max_connections': 20, 'connect_timeout_seconds': 10},
        'startup': {'timeout_seconds': 30},
        'state': {'directory': str(directory / name / 'state'), 'archives': {'enabled': False}},
        'leadership': {'enabled': True, 'cluster_name': CLUSTER, 'ttl_seconds': 6,
                       'renew_interval_seconds': 1, 'standby_poll_seconds': 1},
        'dwd_ready': {'enabled': True, 'queue_key': 'modelverse:dwd:ready', 'block_seconds': 1,
                      'redis': {'mode': 'standalone', 'host': '127.0.0.1', 'port': redis_port,
                                'db': 0, 'password_env': 'REDIS_PASSWORD', 'ssl': False,
                                'socket_timeout': 3, 'socket_connect_timeout': 2}},
        'redis_state': {'enabled': True},
        'scheduler': {'poll_interval_seconds': 1, 'max_concurrent_tasks': 4, 'task_parallelism': 1,
                      'max_attempts': 3, 'retry_delay_seconds': 1,
                      'cross_period_wait_seconds': {'hour': 20}},
        'config': {'convergence_rules_path': str(rules), 'sql_directory': str(rules.parent / 'sql')},
    }
    path = directory / f'{name}.yaml'
    path.write_text(json.dumps(config))
    return path


def start_instance(name: str, config: Path, start: str) -> subprocess.Popen:
    env = dict(os.environ, PYTHONPATH=str(SERVICE / 'src'), CH_PASSWORD='x',
               DATA_CONVERGE_INSTANCE_ID=name, LOG_PATH=str(OUT / name), SERVICE_NAME='data-converge',
               TZ='Asia/Shanghai')
    log = (OUT / f'{name}.stdout').open('w')
    init = subprocess.run([PY, '-m', 'data_converge.main', '--config', str(config), '--init-state',
                           '--start-time', start], env=env, stdout=log, stderr=subprocess.STDOUT)
    if init.returncode not in (0,):
        raise SystemExit(f'{name} init failed; see {log.name}')
    return subprocess.Popen([PY, '-m', 'data_converge.main', '--config', str(config)],
                            env=env, stdout=log, stderr=subprocess.STDOUT)


def leader_name(client) -> str | None:
    value = client.get(f'data_converge:leader:{{{CLUSTER}}}')
    return value.decode() if value else None


def heartbeat_ready(directory: Path) -> dict[str, bool]:
    result = {}
    for path in (directory / 'A' / '.state.health').glob('*/heartbeat.json'):
        result[path.parent.name] = json.loads(path.read_text()).get('ready', False)
    for path in (directory / 'B' / '.state.health').glob('*/heartbeat.json'):
        result[path.parent.name] = json.loads(path.read_text()).get('ready', False)
    return result


def push_signals(client, windows) -> None:
    for start in windows:
        for kind in DWD_TYPES:
            client.lpush('modelverse:dwd:ready', start.strftime('%Y-%m-%dT%H:%M') + ':' + kind)


def calls(db: Path) -> list[dict]:
    with sqlite3.connect(db) as connection:
        connection.row_factory = sqlite3.Row
        return [dict(row) for row in connection.execute('SELECT * FROM calls ORDER BY at')]


def wait_until(predicate, timeout: float, what: str):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.2)
    raise SystemExit(f'timeout waiting for: {what}')


def shared_state(client) -> dict[str, str]:
    result = {}
    for key in client.scan_iter(match=f'dc:{{{CLUSTER}}}:inst:*'):
        instance = key.decode().split(':inst:', 1)[1]
        result[instance] = client.hget(key, 'status').decode()
    return result


def await_success_then_running(client, minimum: int = 2) -> dict[str, str]:
    """Snapshot shared state after enough windows succeeded and one is RUNNING."""
    wait_until(lambda: sum(1 for value in shared_state(client).values()
                           if value == 'SUCCESS') >= minimum, 180, 'successful windows')
    wait_until(lambda: 'RUNNING' in shared_state(client).values(), 60, 'a RUNNING window')
    time.sleep(1.0)  # Let the in-flight INSERT reach the mock, which delays 4s.
    return shared_state(client)


def fencing_probe(client) -> bool:
    """A stale token must be rejected by the same Lua used by the service."""
    sys.path.insert(0, str(SERVICE / 'src'))
    from data_converge.state.redis_mirror import FencedOut, RedisStateMirror, RedisStateSettings

    mirror = RedisStateMirror(client, client, cluster=CLUSTER,
                              leader_key=f'data_converge:leader:{{{CLUSTER}}}',
                              token_source=lambda: 'deposed-token', settings=RedisStateSettings())
    try:
        mirror.write_instance({'instance_id': 'm5_a@probe', 'status': 'SUCCESS', 'taskname': 'm5_a',
                               'data_end': '2026-01-01T00:05:00+08:00'}, 'PT5M')
    except FencedOut:
        return client.exists(mirror.instance_key('m5_a@probe')) == 0
    return False


def run(processes: list, directory: Path) -> dict:
    import redis
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo

    redis_port, ch_port, password = free_port(), free_port(), 'drill-secret'
    ch_db = directory / 'clickhouse.sqlite3'
    processes.append(subprocess.Popen([PY, str(HERE / 'local_redis.py'), '--port', str(redis_port),
                                       '--password', password]))
    processes.append(subprocess.Popen([PY, str(HERE / 'mock_clickhouse.py'), '--port', str(ch_port),
                                       '--db', str(ch_db), '--delay', '4']))
    wait_port(redis_port)
    wait_port(ch_port)
    client = redis.Redis(port=redis_port, password=password)
    os.environ['REDIS_PASSWORD'] = password
    rules = write_rules(directory)
    zone = ZoneInfo('Asia/Shanghai')
    now = datetime.now(zone)
    start = now.replace(minute=now.minute - now.minute % 5, second=0, microsecond=0) - timedelta(minutes=30)
    windows = [start + timedelta(minutes=5 * index) for index in range(6)]
    # Seed all historical readiness signals before either scheduler starts.
    # Otherwise the leader can mark already-due windows BLOCKED before the
    # drill feeds the queue, and a later signal cannot reopen those windows.
    push_signals(client, windows)
    instances = {}
    for name in ('A', 'B'):
        config = write_config(directory, name, redis_port, ch_port, rules)
        instances[name] = start_instance(name, config, start.isoformat())
        processes.append(instances[name])
        time.sleep(1.5)  # A starts first and should become leader.
    first = wait_until(lambda: leader_name(client), 30, 'a leader')
    # Kill only after real SUCCESS windows exist; otherwise "no successful
    # window rerun" would pass on an empty set. Then interrupt a RUNNING one.
    done_before = await_success_then_running(client)
    victim = 'A' if instances['A'].poll() is None else 'B'
    instances[victim].send_signal(signal.SIGKILL)
    killed_at = time.time()
    survivor = 'B' if victim == 'A' else 'A'
    second = wait_until(lambda: (leader_name(client) or '') != first and leader_name(client), 60,
                        'standby takeover')
    expected = {f'{task}@{(w + timedelta(minutes=5)).isoformat()}'
                for w in windows for task in ('m5_a', 'm5_b')}
    wait_until(lambda: expected <= {k for k, v in shared_state(client).items() if v == 'SUCCESS'},
               180, 'all five-minute windows SUCCESS')
    return dict(redis_port=redis_port, client=client, ch_db=ch_db, first=first, second=second,
                victim=victim, survivor=survivor, killed_at=killed_at, done_before=done_before,
                expected=sorted(expected))


def verify(result: dict) -> dict:
    """Turn raw evidence into pass/fail checks; every check states what it proves."""
    client, history = result['client'], calls(result['ch_db'])
    inserts = [c for c in history if c['kind'] == 'INSERT']
    by_window: dict[tuple, list] = {}
    for call in inserts:
        by_window.setdefault((call['target'], call['start']), []).append(call)
    before_success = {k for k, v in result['done_before'].items() if v == 'SUCCESS'}
    rerun_after_kill = [w for w, items in by_window.items() if len(items) > 1
                        and any(c['at'] >= result['killed_at'] for c in items)]
    final = shared_state(client)
    settings = json.loads(inserts[-1]['settings']) if inserts else {}
    checks = {
        'standby_took_over': result['second'] not in (None, result['first']),
        'success_windows_before_kill': len(before_success) >= 2,
        'all_windows_success_in_shared_state': all(final.get(k) == 'SUCCESS' for k in result['expected']),
        'no_success_window_rerun': not any(
            len(by_window.get((_table(k), _start(k)), [])) > 1 for k in before_success),
        'interrupted_window_rerun_once': len(rerun_after_kill) >= 1 and all(
            len(by_window[w]) == 2 for w in rerun_after_kill),
        'no_running_left': not client.smembers(f'dc:{{{CLUSTER}}}:running'),
        'deposed_writer_fenced': fencing_probe(client),
        'clickhouse_settings_sent': settings.get('max_threads') == '8'
        and settings.get('load_balancing') == 'random' and settings.get('readonly') == '0',
        'five_minute_timeout_applied': settings.get('max_execution_time', '999').isdigit()
        and int(settings.get('max_execution_time', '999')) <= 120,
    }
    return dict(checks=checks, passed=all(checks.values()), leader_before=result['first'],
                leader_after=result['second'], killed=result['victim'],
                insert_calls=len(inserts), windows=len(by_window),
                rerun_windows=[list(w) for w in rerun_after_kill],
                success_before_kill=sorted(before_success))


def _table(instance_id: str) -> str:
    return {'m5_a': 'db.t_a', 'm5_b': 'db.t_b', 'h1': 'db.t_h'}[instance_id.split('@')[0]]


def _start(instance_id: str) -> str:
    from datetime import datetime, timedelta
    end = datetime.fromisoformat(instance_id.split('@')[1])
    span = timedelta(hours=1) if instance_id.startswith('h1') else timedelta(minutes=5)
    return str(int((end - span).timestamp()))


def main() -> int:
    import shutil
    import tempfile

    OUT.mkdir(exist_ok=True)
    directory = Path(tempfile.mkdtemp(prefix='dc-drill-'))
    processes: list = []
    try:
        report = verify(run(processes, directory))
    finally:
        for process in reversed(processes):
            if process.poll() is None:
                process.terminate()
        for process in processes:
            try:
                process.wait(10)
            except subprocess.TimeoutExpired:
                process.kill()
        for name in ('A', 'B'):
            source = directory / name
            if source.exists():
                shutil.copytree(source, OUT / f'{name}-state', dirs_exist_ok=True)
    (OUT / 'report.json').write_text(json.dumps(report, indent=2, ensure_ascii=False))
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
