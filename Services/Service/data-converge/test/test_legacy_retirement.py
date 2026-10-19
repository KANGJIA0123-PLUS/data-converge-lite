"""Old execution facts remain readable but cannot participate in new execution."""

import json
import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from data_converge.dag.models import TaskStatus
from data_converge.state import StateStore
from data_converge.state import history

from .test_configuration_state import NOW, instance


CUTOVER = NOW + timedelta(hours=1)
MANIFEST = {"schema_version": 1, "tasks": {"root": {"revision": "new"}}}
FACT_TABLES = ("task_instances", "execution_attempts", "dispatch_checkpoints",
               "execution_resolutions", "execution_events", "dwd_ready_signals")


@pytest.fixture
def store(tmp_path):
    value = StateStore(tmp_path / "state.db", create=True)
    value.acquire_lock()
    value.set_meta("binding", "a" * 64)
    value.set_meta("coverage_start", NOW.isoformat())
    yield value
    value.close()


def seed(store):
    names = ("success", "pending", "blocked", "failed", "unknown", "running", "prepared", "dispatched", "resolved")
    items = {name: instance(name) for name in names}
    store.enqueue_and_advance(items.values(), {name: CUTOVER for name in names})
    for name in ("success", "failed", "unknown", "running", "resolved"):
        store.claim(items[name].instance_id, now=NOW)
        if name != "running":
            status = TaskStatus.UNKNOWN if name == "resolved" else TaskStatus(name.upper())
            store.finish(items[name].instance_id, status, retryable=True, now=NOW)
    store.mark_blocked(items["blocked"].instance_id, "waiting")
    store.resolve(items["resolved"].instance_id, "SUCCESS", "operator evidence")
    for name in ("prepared", "dispatched"):
        store.prepare_attempt(items[name].instance_id, now=NOW)
    dispatched = store.get_instance(items["dispatched"].instance_id)
    store.mark_dispatched(dispatched["instance_id"], dispatched["query_id"], now=NOW)
    store.record_dwd_signal(NOW, "signal", NOW)
    return items


def facts(store):
    conn = store._connection()
    names = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    return {name: [tuple(row) for row in conn.execute(f"SELECT * FROM {name} ORDER BY rowid")]
            for name in FACT_TABLES if name in names}


def retire(store, **overrides):
    recovery = dict(schema_version=1, migration_id="recovery-1", cutover=CUTOVER.isoformat(),
                    previous_binding="a" * 64, current_binding="b" * 64, revision="c" * 64,
                    backup_path="/backups/before.db", recovered_at=CUTOVER.isoformat(),
                    identity_verified=False, previous_definition_available=False,
                    historical_counts=store.status_counts())
    arguments = dict(expected_binding="a" * 64, expected_revision=None, manifest=MANIFEST,
                     revision="c" * 64, task_starts={"root": CUTOVER}, binding="b" * 64,
                     migration_id="recovery-1", applied_at=CUTOVER, backup_path="/backups/before.db",
                     retire_existing=True, recovery=recovery)
    store.apply_configuration(**(arguments | overrides))


def test_retirement_preserves_all_original_facts_and_isolates_active_reads(store):
    items = seed(store)
    before = facts(store)
    counts = store.status_counts()
    retire(store)
    store.validate_retirement()
    assert facts(store) == before
    assert store.status_counts() == store.retired_status_counts() == counts
    assert store.active_status_counts() == {}
    assert store.list_pending() == []
    assert store.configuration_constraints(items) == {"running": 0, "tasks": {
        name: {"unresolved": 0, "latest_end": None} for name in items}}
    for item in items.values():
        assert store.get_instance(item.instance_id) is not None
        assert store.get_status(item.instance_id) is None
        assert store.get_cursor(item.taskname) is None
        assert store.get_retirement(item.instance_id)["migration_id"] == "recovery-1"
    assert store.legacy_recovery()["retired_count"] == 9
    assert len(store.legacy_recovery()["retired_id_digest"]) == 64
    assert store.configuration_snapshot()["retirement_id"] == "recovery-1"


def test_recovery_never_rewrites_retired_running_attempts_or_checkpoints(store):
    seed(store)
    before = facts(store)
    retire(store)
    assert store.list_recovery_candidates() == []
    assert store.recovery_counts() == {"AUTO_RESUME": 0, "RECONCILE": 0}
    with store._connection(write=True) as conn:
        history.record_recovery(conn, CUTOVER.isoformat())
    assert store.recover_running() == 0
    assert facts(store) == before
    active = instance("root", CUTOVER)
    store.enqueue_and_advance([active], {})
    store.claim(active.instance_id, now=CUTOVER)
    assert store.recover_running() == 1
    assert store.get_status(active.instance_id) == TaskStatus.UNKNOWN
    for name, rows in before.items():
        assert facts(store)[name][:len(rows)] == rows


@pytest.mark.parametrize("operation", ["enqueue", "claim", "prepare", "dispatch", "finish", "block", "resolve"])
def test_each_write_entrance_rejects_historical_identity_without_partial_writes(store, operation):
    items = seed(store)
    retire(store)
    before = list(store._connection().iterdump())
    prepared = store.get_instance(items["prepared"].instance_id)
    actions = {
        "enqueue": lambda: store.enqueue_and_advance([instance("root", CUTOVER), items["pending"]], {"root": CUTOVER}),
        "claim": lambda: store.claim(items["success"].instance_id),
        "prepare": lambda: store.prepare_attempt(items["pending"].instance_id),
        "dispatch": lambda: store.mark_dispatched(prepared["instance_id"], prepared["query_id"]),
        "finish": lambda: store.finish(items["running"].instance_id, TaskStatus.SUCCESS),
        "block": lambda: store.mark_blocked(items["pending"].instance_id, "changed"),
        "resolve": lambda: store.resolve(items["unknown"].instance_id, "SUCCESS", "changed"),
    }
    with pytest.raises(ValueError, match="^HISTORICAL_INSTANCE_RETIRED$"):
        actions[operation]()
    assert list(store._connection().iterdump()) == before


def test_retirement_audit_failure_rolls_back_mapping_markers_and_all_cursors(store):
    seed(store)
    conn = store._connection(write=True)
    conn.execute("""CREATE TRIGGER reject_new_binding BEFORE UPDATE ON metadata
        WHEN new.key='binding' BEGIN SELECT RAISE(ABORT, 'audit disk failure'); END""")
    before = list(conn.iterdump())
    with pytest.raises(sqlite3.IntegrityError, match="audit disk failure"):
        retire(store)
    assert list(conn.iterdump()) == before
    assert store.legacy_recovery() is None
    assert store.get_meta("retirement_required") is None


def test_later_normal_configuration_migration_preserves_retirement_identity(store):
    items = seed(store)
    retire(store)
    old = store.legacy_recovery()
    store.apply_configuration(expected_binding="b" * 64, expected_revision="c" * 64,
        manifest=MANIFEST, revision="d" * 64, task_starts={"root": CUTOVER}, binding="e" * 64,
        migration_id="normal-2", applied_at=CUTOVER + timedelta(minutes=5))
    store.validate_retirement()
    assert store.configuration_snapshot()["retirement_id"] == "recovery-1"
    assert store.legacy_recovery() == old
    assert store.get_status(items["success"].instance_id) is None


@pytest.mark.parametrize("taskname", ["root", "new-root"])
def test_later_configuration_cannot_move_task_start_before_recovery_cutover(store, taskname):
    seed(store)
    retire(store)
    active = instance("root", CUTOVER)
    store.enqueue_and_advance([active], {"root": CUTOVER + timedelta(minutes=5)})
    before = list(store._connection().iterdump())
    with pytest.raises(ValueError, match="^LEGACY_RETIREMENT_INVALID$"):
        store.apply_configuration(expected_binding="b" * 64, expected_revision="c" * 64,
            manifest=MANIFEST, revision="d" * 64,
            task_starts={"root": CUTOVER, taskname: CUTOVER - timedelta(minutes=5)},
            binding="e" * 64, migration_id="normal-2", applied_at=CUTOVER,
            reset_task_cursors=("root",))
    assert list(store._connection().iterdump()) == before


def test_current_snapshot_start_before_retirement_cutover_is_rejected(store):
    seed(store)
    retire(store)
    current = store.configuration_snapshot()
    corrupted_start = CUTOVER - timedelta(minutes=5)
    assert corrupted_start >= datetime.fromisoformat(store.get_meta("coverage_start"))
    current["task_starts"]["root"] = corrupted_start.isoformat()
    store.set_meta("configuration_snapshot", json.dumps(current))
    before = list(store._connection().iterdump())
    with pytest.raises(ValueError, match="^LEGACY_RETIREMENT_INVALID$"):
        store.validate_retirement()
    assert list(store._connection().iterdump()) == before


@pytest.mark.parametrize("damage", ["mapping_table", "mapping_row", "marker", "audit", "recovery",
                                     "snapshot_marker", "instance", "mapping_identity", "id_digest"])
def test_missing_or_corrupt_retirement_evidence_is_rejected(store, damage):
    items = seed(store)
    retire(store)
    conn = store._connection(write=True)
    with conn:
        if damage == "mapping_table":
            conn.execute("DROP TABLE retired_instances")
        elif damage == "mapping_row":
            conn.execute("DELETE FROM retired_instances WHERE instance_id=?", (items["success"].instance_id,))
        elif damage == "marker":
            conn.execute("DELETE FROM metadata WHERE key='retirement_required'")
        elif damage == "audit":
            conn.execute("DELETE FROM configuration_history WHERE migration_id='recovery-1'")
        elif damage == "recovery":
            conn.execute("DELETE FROM metadata WHERE key='legacy_recovery'")
        elif damage == "snapshot_marker":
            snapshot = store.configuration_snapshot()
            snapshot.pop("retirement_id")
            store.set_meta("configuration_snapshot", json.dumps(snapshot))
        elif damage == "instance":
            conn.execute("DELETE FROM task_instances WHERE instance_id=?", (items["success"].instance_id,))
        elif damage == "mapping_identity":
            conn.execute("UPDATE retired_instances SET migration_id='other'")
        else:
            recovery = store.legacy_recovery()
            recovery["retired_id_digest"] = "0" * 64
            store.set_meta("legacy_recovery", json.dumps(recovery))
    with pytest.raises(ValueError, match="^LEGACY_RETIREMENT_INVALID$"):
        store.validate_retirement()


def test_no_retirement_on_old_read_only_ledger_creates_nothing(store):
    item = instance()
    store.enqueue_and_advance([item], {})
    path = store.path
    store.close()
    before = path.read_bytes()
    reader = StateStore(path)
    try:
        reader.validate_retirement()
        assert reader.legacy_recovery() is None and reader.get_retirement(item.instance_id) is None
        assert reader.active_status_counts() == {"PENDING": 1}
        assert reader.retired_status_counts() == {}
        assert reader.get_status(item.instance_id) == TaskStatus.PENDING
    finally:
        reader.close()
    assert path.read_bytes() == before


def test_summary_compares_offsets_and_includes_future_signal_end(store):
    later = NOW.replace(hour=3, tzinfo=timezone(timedelta(hours=-8)))
    earlier = NOW.replace(hour=18, tzinfo=timezone(timedelta(hours=8)))
    store.enqueue_and_advance([instance(start=later), instance(start=earlier)], {})
    store.record_dwd_signal(later + timedelta(hours=2), "signal", NOW)
    assert store.legacy_window_summary() == {"counts": {"PENDING": 2},
        "latest_end": later + timedelta(minutes=5), "latest_signal_end": later + timedelta(hours=2, minutes=5)}


@pytest.mark.parametrize("field", ["data_end", "window_start"])
def test_summary_rejects_bad_or_naive_persistent_times(store, field):
    store.enqueue_and_advance([instance()], {})
    store.record_dwd_signal(NOW, "signal", NOW)
    conn = store._connection(write=True)
    table = "task_instances" if field == "data_end" else "dwd_ready_signals"
    with conn:
        conn.execute(f"UPDATE {table} SET {field}='2026-10-19T10:00:00'")
    with pytest.raises(ValueError, match="^LEGACY_RETIREMENT_INVALID$"):
        store.legacy_window_summary()


def test_required_missing_configuration_cannot_be_reinterpreted_as_legacy(store):
    seed(store)
    store.set_meta("configuration_required", "1")
    before = list(store._connection().iterdump())
    with pytest.raises(ValueError, match="^LEGACY_RETIREMENT_INVALID$"):
        retire(store)
    assert list(store._connection().iterdump()) == before


@pytest.mark.parametrize("bad_status", ["private-password-value", ""])
def test_summary_does_not_expose_unknown_status_labels(store, bad_status):
    store.enqueue_and_advance([instance()], {})
    with store._connection(write=True) as conn:
        conn.execute("UPDATE task_instances SET status=?", (bad_status,))
    with pytest.raises(ValueError, match="^LEGACY_RETIREMENT_INVALID$"):
        store.legacy_window_summary()


def test_empty_legacy_ledger_still_has_complete_durable_retirement_evidence(store):
    retire(store)
    store.validate_retirement()
    assert store.legacy_recovery()["retired_count"] == 0
    assert store.retired_status_counts() == store.active_status_counts() == {}
    assert store.configuration_snapshot()["retirement_id"] == "recovery-1"
    active = instance("root", CUTOVER)
    store.enqueue_and_advance([active], {})
    store.validate_retirement()
    assert store.get_status(active.instance_id) == TaskStatus.PENDING


def test_repeated_retirement_cannot_replace_original_historical_scope(store):
    seed(store)
    retire(store)
    before = list(store._connection().iterdump())
    with pytest.raises(ValueError, match="^LEGACY_RETIREMENT_INVALID$"):
        retire(store, expected_binding="b" * 64, expected_revision="c" * 64)
    assert list(store._connection().iterdump()) == before


@pytest.mark.parametrize("snapshot", ["null", "[]", '"invalid"', "{"])
def test_unrelated_corrupt_configuration_remains_owned_by_configuration_validation(store, snapshot):
    store.set_meta("configuration_required", "1")
    store.set_meta("configuration_snapshot", snapshot)
    before = list(store._connection().iterdump())
    store.validate_retirement()
    assert list(store._connection().iterdump()) == before
