"""Liveness must fail when the writer's scheduler loop stops making progress.

Before: the heartbeat thread kept writing while the scheduler was blocked, so
liveness stayed green forever and the platform never restarted a hung writer.
"""
import json
import time

from data_converge.health import Heartbeat, healthcheck


def test_blocked_scheduler_fails_liveness_after_its_budget(tmp_path, monkeypatch):
    with Heartbeat(tmp_path, interval=0.05) as heartbeat:
        heartbeat.set_ready(True)
        heartbeat.watch_progress(stall_seconds=60)
        time.sleep(0.15)
        assert healthcheck(tmp_path, 'liveness') and healthcheck(tmp_path, 'readiness')
        start = time.time()
        monkeypatch.setattr('data_converge.health.time.time', lambda: start + 61)
        # The heartbeat file is still fresh relative to max_age, but no round completed.
        assert not healthcheck(tmp_path, 'liveness', max_age=120)
        assert not healthcheck(tmp_path, 'readiness', max_age=120)


def test_completed_rounds_keep_liveness_green(tmp_path, monkeypatch):
    with Heartbeat(tmp_path, interval=0.05) as heartbeat:
        heartbeat.watch_progress(stall_seconds=60)
        start = time.time()
        clock = [start]
        monkeypatch.setattr('data_converge.health.time.time', lambda: clock[0])
        for _round in range(5):
            clock[0] += 50
            heartbeat.progress()
            heartbeat.set_ready(True)
            assert healthcheck(tmp_path, 'liveness', max_age=600)


def test_standby_and_drain_are_not_judged_by_scheduler_progress(tmp_path, monkeypatch):
    with Heartbeat(tmp_path, interval=0.05) as heartbeat:
        heartbeat.watch_progress(stall_seconds=60)
        heartbeat.unwatch_progress()   # term ended: draining SQL or waiting as standby
        time.sleep(0.15)
        value = json.loads((tmp_path / 'heartbeat.json').read_text())
        assert 'progress_at' not in value
        start = time.time()
        monkeypatch.setattr('data_converge.health.time.time', lambda: start + 3600)
        heartbeat._write()
        assert healthcheck(tmp_path, 'liveness')


def test_malformed_progress_fields_fail_closed(tmp_path):
    import os

    (tmp_path / 'heartbeat.json').write_text(json.dumps(
        {'pid': os.getpid(), 'time': time.time(), 'ready': True, 'progress_at': 'x', 'stall_seconds': 60}))
    assert not healthcheck(tmp_path, 'liveness')


def test_stall_budget_covers_longest_task_and_poll():
    from types import SimpleNamespace

    from data_converge.runtime import STALL_MARGIN_SECONDS, stall_budget

    options = SimpleNamespace(task_timeout_seconds={'PT5M': 120, 'PT1H': 600, 'P1D': 1800, 'P1M': 1800},
                              poll_interval_seconds=5)
    assert stall_budget(SimpleNamespace(options=options)) == 1800 + 5 + STALL_MARGIN_SECONDS
