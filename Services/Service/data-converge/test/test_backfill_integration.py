"""CLI -> live service writer -> HTTP SQL -> queue and ledger history."""
import json
from datetime import datetime

from data_converge import main as entry
from data_converge.application import Application
from .test_main import app, local_http_app


def submit(path, capsys, *, recompute=False):
    args = ["--config", str(path), "--backfill", "--task", "simple",
            "--from", "2026-10-19T10:00:00+08:00",
            "--to", "2026-10-19T10:05:00+08:00", "--format", "json"]
    assert entry.main(args + (["--recompute"] if recompute else [])) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "QUEUED"
    return result["data"]["job_id"]


def test_commands_submit_while_live_writer_holds_lock_and_recompute_is_explicit(local_http_app, capsys):
    path, requests = local_http_app
    application = Application(path)
    application.initialize()
    with application._active_state() as (store, lease), application._runtime(store, lease) as live:
        job = submit(path, capsys)
        assert requests == []
        live.scheduler.background_worker()
        assert len(requests) == 2
        assert requests[0][1].startswith("ALTER TABLE target_local DELETE")
        assert requests[1][1].startswith("INSERT INTO target")
        submit(path, capsys)
        live.scheduler.background_worker()
        assert len(requests) == 2
        submit(path, capsys, recompute=True)
        live.scheduler.background_worker()
        assert len(requests) == 4
        history = store.list_execution_attempts()
        assert len(history) == 2
        assert len({row["query_id"] for row in history}) == 2
        assert all(row["status"] == "SUCCESS" for row in history)
        assert store.get_cursor("simple") is None
        assert entry.main(["--config", str(path), "--backfill-status", job, "--format", "json"]) == 0
        assert json.loads(capsys.readouterr().out)["status"] == "COMPLETED"


def test_background_worker_is_used_by_the_running_service_loop(local_http_app, capsys):
    path, requests = local_http_app
    application = Application(path)
    application.initialize()
    with application._active_state() as (store, lease), application._runtime(store, lease) as live:
        submit(path, capsys)
        normal = live.scheduler.tick
        manual = live.scheduler.background_worker
        order = []

        def normal_round():
            order.append("normal")
            normal(datetime.fromisoformat(store.get_meta("coverage_start")))

        def manual_round():
            order.append("manual")
            manual()
            live.request_stop()

        live.scheduler.tick = normal_round
        live.scheduler.background_worker = manual_round
        live.serve()
        assert order == ["normal", "manual"]
        assert len(requests) == 3  # Startup probe, DELETE, INSERT.
