"""Live deposed-leader fencing drill: stop the leader, let the standby take over,
then resume the old leader and prove its late write is fenced.

Scenario
  1. Start A, wait for its lease, then start standby B.
  2. Wait until A has an in-flight SQL window; SIGSTOP A (lease renewal stops).
  3. B wins the lease after the TTL, imports A's RUNNING row, kills the old
     query (best effort), reruns the window and finishes the remaining work.
  4. SIGCONT A. Its SQL returns, it commits locally and tries to publish with
     the old token; the fenced Lua write must be rejected. B's shared state must
     not change.
  5. Verify A exits, B stays leader, the shared hashes are byte-identical, and
     the old leader's log contains the fencing error.

Usage:
    python3 tools/failover-drill/fencing_drill.py
Results are saved to tools/failover-drill/out/fencing/report.json.
"""

from __future__ import annotations

from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

import drill

HERE = Path(__file__).resolve().parent
OUT = HERE / 'out' / 'fencing'
PY = sys.executable


def log_contains(path: Path, needle: str, timeout: float = 30) -> bool:
    def found():
        return path.exists() and needle in path.read_text(errors='ignore')
    drill.wait_until(found, timeout, f'{needle} in {path.name}')
    return True


def start_cluster(processes: list, directory: Path) -> dict:
    """Start Redis, mock ClickHouse, A (leader) and B (standby)."""
    import redis
    from zoneinfo import ZoneInfo

    redis_port, ch_port, password = drill.free_port(), drill.free_port(), 'drill-secret'
    ch_db = directory / 'clickhouse.sqlite3'
    ready = directory / 'redis.ready'
    processes.append(subprocess.Popen([PY, str(HERE / 'local_redis.py'), '--port', str(redis_port),
                                       '--password', password, '--ready-file', str(ready)]))
    processes.append(subprocess.Popen([PY, str(HERE / 'mock_clickhouse.py'), '--port', str(ch_port),
                                       '--db', str(ch_db), '--delay', '4']))
    drill.wait_file(ready)
    drill.wait_port(redis_port)
    drill.wait_port(ch_port)
    client = redis.Redis(port=redis_port, password=password)
    for _ in range(100):
        try:
            client.ping()
            break
        except Exception:
            time.sleep(0.1)
    else:
        raise SystemExit('redis did not answer PING')
    os.environ['REDIS_PASSWORD'] = password
    rules = drill.write_rules(directory)
    zone = ZoneInfo('Asia/Shanghai')
    now = datetime.now(zone)
    start = now.replace(minute=now.minute - now.minute % 5, second=0, microsecond=0) - timedelta(minutes=30)
    windows = [start + timedelta(minutes=5 * index) for index in range(6)]
    drill.push_signals(client, windows)
    instances = {}
    for name in ('A', 'B'):
        config = drill.write_config(directory, name, redis_port, ch_port, rules)
        instances[name] = drill.start_instance(name, config, start.isoformat())
        processes.append(instances[name])
        if name == 'A':
            def a_leader():
                if instances['A'].poll() is not None:
                    raise SystemExit(f"A exited before the lease; see {OUT / 'A' / 'data-converge.log'}")
                return drill.leader_name(client)
            drill.wait_until(a_leader, 30, 'A lease')
        else:
            time.sleep(1.0)
    log_contains(OUT / 'A' / 'data-converge.log', 'LEADERSHIP_ACQUIRED')
    log_contains(OUT / 'B' / 'data-converge.log', 'LEADERSHIP_STANDBY')
    return dict(client=client, instances=instances, ch_db=ch_db, windows=windows,
                leader=drill.leader_name(client))


def run(processes: list, directory: Path) -> dict:
    state = start_cluster(processes, directory)
    client, instances = state['client'], state['instances']
    victim = instances['A']
    # Wait until A has a real in-flight window; its RUNNING row is published
    # before the SQL is sent, so the snapshot is stable enough to freeze A.
    interrupted = drill.wait_until(
        lambda: [key for key, value in drill.shared_state(client).items() if value == 'RUNNING'],
        60, 'a RUNNING window on A')
    interrupted = sorted(interrupted)
    stopped_at = time.time()
    victim.send_signal(signal.SIGSTOP)
    def took_over():
        current = drill.leader_name(client)
        return current if current and current != state['leader'] else None
    second = drill.wait_until(took_over, 60, 'standby takeover')
    takeover_seconds = round(time.time() - stopped_at, 2)
    expected = {f'{task}@{(window + timedelta(minutes=5)).isoformat()}'
                for window in state['windows'] for task in ('m5_a', 'm5_b')}
    drill.wait_until(
        lambda: expected <= {key for key, value in drill.shared_state(client).items() if value == 'SUCCESS'},
        180, 'all windows after takeover')
    before = {key: client.hgetall(f'dc:{{{drill.CLUSTER}}}:inst:{key}') for key in interrupted}
    victim.send_signal(signal.SIGCONT)
    exit_code, timed_out = _wait_exit(victim, 30)
    after = {key: client.hgetall(f'dc:{{{drill.CLUSTER}}}:inst:{key}') for key in interrupted}
    local_status = local_statuses(directory / 'A' / 'state' / 'state.sqlite3', interrupted)
    log = (OUT / 'A' / 'data-converge.log').read_text(errors='ignore')
    return dict(client=client, instances=instances, leader_before=state['leader'], leader_after=second,
                interrupted=interrupted, before=before, after=after, exit_code=exit_code,
                local_status=local_status, timed_out=timed_out,
                fenced=('FencedOut' in log or 'REDIS_STATE_FENCED' in log),
                takeover_seconds=takeover_seconds, expected=sorted(expected))


def _wait_exit(process, timeout: float) -> tuple[int | None, bool]:
    try:
        return process.wait(timeout), False
    except subprocess.TimeoutExpired:
        return None, True


def local_statuses(database: Path, instance_ids: list[str]) -> dict[str, str | None]:
    """Read the old leader's local ledger after it exits, to prove finish ran."""
    import sqlite3

    with sqlite3.connect(database) as connection:
        rows = {instance_id: connection.execute(
            'SELECT status FROM task_instances WHERE instance_id=?', (instance_id,)).fetchone()
            for instance_id in instance_ids}
    return {instance_id: row[0] if row else None for instance_id, row in rows.items()}


def verify(result: dict) -> dict:
    client, instances = result['client'], result['instances']
    state = drill.shared_state(client)
    unchanged = all(result['before'][key] == result['after'][key] for key in result['interrupted'])
    committed = all(status in ('SUCCESS', 'FAILED', 'UNKNOWN')
                    for status in result['local_status'].values())
    checks = {
        'standby_took_over': result['leader_after'] not in (None, result['leader_before']),
        'interrupted_windows_succeeded': all(state.get(key) == 'SUCCESS' for key in result['interrupted']),
        'old_leader_committed_finish': committed,
        'old_leader_fenced': result['fenced'] and result['exit_code'] not in (None, 0),
        'shared_state_not_overwritten': unchanged,
        'no_running_left': not client.smembers(f'dc:{{{drill.CLUSTER}}}:running'),
        'new_leader_alive': instances['B'].poll() is None,
    }
    return dict(checks=checks, passed=all(checks.values()),
                leader_before=result['leader_before'], leader_after=result['leader_after'],
                stopped_leader='A', interrupted=result['interrupted'],
                local_status=result['local_status'],
                takeover_seconds=result['takeover_seconds'],
                old_leader_exit_code=result['exit_code'], old_leader_timed_out=result['timed_out'],
                fenced_log=result['fenced'])


def main() -> int:
    import shutil
    import tempfile

    if OUT.exists():
        shutil.rmtree(OUT)
    OUT.mkdir(parents=True)
    drill.OUT = OUT  # Reuse drill helpers but keep this scenario's logs separate.
    directory = Path(tempfile.mkdtemp(prefix='dc-fencing-'))
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
