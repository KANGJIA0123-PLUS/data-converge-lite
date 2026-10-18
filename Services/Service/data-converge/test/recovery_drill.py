"""Offline protocol demonstration; this does not validate a ClickHouse cluster.

Each child runs the real executor and SQLite ledger, then exits with os._exit.
A separate local SQLite receiver durably records fixture SQL acceptance without
executing ClickHouse SQL. Its table deliberately has no deduplication constraint.
Run with the service src directory on PYTHONPATH; stdout is machine-readable JSON.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import sqlite3
import subprocess
import sys
import tempfile
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from data_converge.dag import DAGBuilder, DAGExecutor
from data_converge.state import StateStore
from data_converge.state.events import list_events


NOW = datetime(2026, 10, 18, 2, 6, tzinfo=timezone.utc)
CRASH_EXIT = 91
CASES = {
    "prepared_before_dispatch": {
        "before": ("RUNNING", 0, 0, []), "recovered": 1,
        "after": ("PENDING", 0, 0, []),
        "replay": ("SUCCESS", 1, 1, ["SUCCESS"]),
        "events": ["PREPARED", "RECOVERED_UNSUBMITTED", "PREPARED", "DISPATCHED", "SUCCEEDED"]},
    "dispatch_before_receiver": {
        "before": ("RUNNING", 1, 0, ["RUNNING"]), "recovered": 1,
        "after": ("UNKNOWN", 1, 0, ["UNKNOWN"]),
        "replay": ("UNKNOWN", 1, 0, ["UNKNOWN"]),
        "events": ["PREPARED", "DISPATCHED", "RECOVERY_REQUIRED"]},
    "accepted_before_finish": {
        "before": ("RUNNING", 1, 1, ["RUNNING"]), "recovered": 1,
        "after": ("UNKNOWN", 1, 1, ["UNKNOWN"]),
        "replay": ("UNKNOWN", 1, 1, ["UNKNOWN"]),
        "events": ["PREPARED", "DISPATCHED", "RECOVERY_REQUIRED"]},
    "success_before_replay": {
        "before": ("SUCCESS", 1, 1, ["SUCCESS"]), "recovered": 0,
        "after": ("SUCCESS", 1, 1, ["SUCCESS"]),
        "replay": ("SUCCESS", 1, 1, ["SUCCESS"]),
        "events": ["PREPARED", "DISPATCHED", "SUCCEEDED"]},
}


def graph():
    rules = [dict(taskname="offline_drill", source_tables=["fixture_source"],
                  target_table="fixture_target", aggregation_interval="PT5M",
                  cron="1,6,11,16,21,26,31,36,41,46,51,56 * * * *",
                  depends_on=[], sql_id="offline_fixture_sql")]
    builder = DAGBuilder(SimpleNamespace(validate=lambda: [], get_all_rules=lambda: rules))
    return builder, builder.build()


class CrashStore(StateStore):
    """Test-only injection after actual ledger commits, before the next action."""

    def __init__(self, path, fault):
        super().__init__(path, create=True)
        self.fault = fault

    def prepare_attempt(self, instance_id, *, now=None, max_attempts=3):
        prepared = super().prepare_attempt(instance_id, now=now, max_attempts=max_attempts)
        if prepared and self.fault == "prepared_before_dispatch":
            os._exit(CRASH_EXIT)
        return prepared

    def mark_dispatched(self, instance_id, query_id, *, now=None):
        super().mark_dispatched(instance_id, query_id, now=now)
        if self.fault == "dispatch_before_receiver":
            os._exit(CRASH_EXIT)


class LocalReceiver:
    """Record acceptance independently of the ledger, including duplicate calls."""

    def __init__(self, path, *, crash=False):
        self.path = path
        self.crash = crash

    def __call__(self, instance):
        with closing(sqlite3.connect(self.path)) as conn:
            conn.execute("PRAGMA synchronous=FULL")
            with conn:
                conn.execute("INSERT INTO accepted(instance_id, query_id, sql_id) VALUES (?, ?, ?)",
                             (instance.instance_id, instance.query_id, instance.task.sql_id))
        if self.crash:
            os._exit(CRASH_EXIT)


def initialize_receiver(path):
    with closing(sqlite3.connect(path)) as conn:
        with conn:
            conn.execute("""CREATE TABLE accepted (
                acceptance_id INTEGER PRIMARY KEY, instance_id TEXT NOT NULL,
                query_id TEXT NOT NULL, sql_id TEXT NOT NULL)""")


def receiver_rows(path):
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(row) for row in conn.execute("SELECT * FROM accepted ORDER BY acceptance_id")]


def run_child(name, directory):
    builder, dag = graph()
    with closing(CrashStore(directory / "ledger.db", name)) as store:
        store.acquire_lock()
        receiver = LocalReceiver(directory / "receiver.db", crash=name == "accepted_before_finish")
        executor = DAGExecutor(dag, receiver, state_store=store)
        executor.execute([builder.create_instance(dag, "offline_drill", NOW)],
                         now=datetime.now(timezone.utc))
        os._exit(CRASH_EXIT)  # Success persisted; bypass Python cleanup too.


def snapshot(store, instance_id, receiver_path):
    row = store.get_instance(instance_id)
    return {
        "status": row["status"], "attempts": row["attempts"],
        "receiver_count": len(receiver_rows(receiver_path)),
        "history_statuses": [item["status"] for item in
                             store.list_execution_attempts(instance_id)],
    }


def expected_state(values):
    status, attempts, received, history = values
    return {"status": status, "attempts": attempts,
            "receiver_count": received, "history_statuses": history}


def expected_result(name):
    case = CASES[name]
    return {
        "exit_code": 91, "before_recovery": expected_state(case["before"]),
        "recovered": case["recovered"], "after_recovery": expected_state(case["after"]),
        "after_replay": expected_state(case["replay"]), "second_recovered": 0,
        "after_second_replay": expected_state(case["replay"]),
        "event_types": case["events"],
    }


def replay(store, directory):
    builder, dag = graph()
    instance = builder.create_instance(dag, "offline_drill", NOW)
    DAGExecutor(dag, LocalReceiver(directory / "receiver.db"), state_store=store).execute(
        [instance], now=datetime.now(timezone.utc))


def collect_evidence(store, instance_id, crash_query_id, receiver_path):
    attempts = store.list_execution_attempts(instance_id)
    fields = ("query_id", "attempt_number", "status", "finished_at", "recovered_at")
    return {
        "crash_query_id": crash_query_id,
        "current_query_id": store.get_instance(instance_id)["query_id"],
        "attempts": [{key: row[key] for key in fields} for row in attempts],
        "receiver": receiver_rows(receiver_path),
        "events": list_events(store._connection(), limit=100),
    }


def recover_and_replay(directory, exit_code):
    builder, dag = graph()
    instance_id = builder.create_instance(dag, "offline_drill", NOW).instance_id
    receiver_path = directory / "receiver.db"
    with closing(StateStore(directory / "ledger.db")) as store:
        store.acquire_lock()
        crash_query_id = store.get_instance(instance_id)["query_id"]
        observed = {"exit_code": exit_code,
                    "before_recovery": snapshot(store, instance_id, receiver_path)}
        observed["recovered"] = store.recover_running()
        observed["after_recovery"] = snapshot(store, instance_id, receiver_path)
        replay(store, directory)
        observed["after_replay"] = snapshot(store, instance_id, receiver_path)
        observed["second_recovered"] = store.recover_running()
        replay(store, directory)
        observed["after_second_replay"] = snapshot(store, instance_id, receiver_path)
        evidence = collect_evidence(store, instance_id, crash_query_id, receiver_path)
        observed["event_types"] = [row["event_type"] for row in evidence["events"]]
    return observed, evidence


def run_case(name, root):
    directory = root / name
    directory.mkdir()
    initialize_receiver(directory / "receiver.db")
    child = subprocess.run(
        [sys.executable, str(Path(__file__).resolve()), "--child", name,
         "--directory", str(directory)], capture_output=True, text=True, timeout=15)
    if child.returncode != CRASH_EXIT:
        raise RuntimeError(f"Crash worker {name} exited {child.returncode}: {child.stderr}")
    observed, evidence = recover_and_replay(directory, child.returncode)
    expected = expected_result(name)
    return {"name": name, "expected": expected, "observed": observed,
            "evidence": evidence, "passed": observed == expected}


def run_drill():
    with tempfile.TemporaryDirectory(prefix="data-converge-recovery-drill-") as temporary:
        root = Path(temporary).resolve()
        cases = [run_case(name, root) for name in CASES]
    return {
        "schema_version": 1, "scope": "offline_protocol_demonstration",
        "clickhouse_cluster_validated": False,
        "receiver_semantics": "local durable acceptance record; no ClickHouse SQL executed",
        "fault_mechanism": "subprocess os._exit(91) after committed boundary",
        "environment": {"python": platform.python_version(), "sqlite": sqlite3.sqlite_version,
                        "platform": platform.platform()},
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "temporary_files_removed": not root.exists(),
        "cases": cases, "passed": all(case["passed"] for case in cases),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--child", choices=tuple(CASES))
    parser.add_argument("--directory", type=Path)
    args = parser.parse_args()
    if args.child:
        if args.directory is None:
            parser.error("--child requires --directory")
        run_child(args.child, args.directory)
    report = run_drill()
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
