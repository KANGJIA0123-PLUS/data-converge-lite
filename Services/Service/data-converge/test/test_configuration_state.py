"""Configuration changes preserve ledger facts and reject incomplete writes."""

import sqlite3
import stat
from datetime import datetime, timedelta, timezone

import pytest

from data_converge.dag.models import TaskDefinition, TaskInstance, TaskStatus
from data_converge.state import StateStore


NOW = datetime(2026, 10, 19, 10, tzinfo=timezone.utc)
MANIFEST = {"schema_version": 1, "tasks": {"root": {"revision": "task-v1"}}}


@pytest.fixture
def store(tmp_path):
    value = StateStore(tmp_path / "state.db", create=True)
    value.acquire_lock()
    value.set_meta("binding", "binding-v1")
    value.set_meta("coverage_start", NOW.isoformat())
    yield value
    value.close()


def apply(store, **changes):
    arguments = dict(expected_binding="binding-v1", expected_revision=None,
                     manifest=MANIFEST, revision="revision-v1", task_starts={"root": NOW},
                     binding="binding-v1", migration_id="migration-1", applied_at=NOW)
    store.apply_configuration(**(arguments | changes))


def instance(name="root", start=NOW):
    task = TaskDefinition(name, "", ("raw",), "rollup", "PT5M", "* * * * *", (), name)
    end = start + timedelta(minutes=5)
    return TaskInstance(task, end.isoformat(), end, start, end, ())


def test_legacy_inspection_creates_no_tables_or_files(store):
    path = store.path
    store.close()
    path.with_suffix(".db.lock").unlink()
    before = path.read_bytes()
    reader = StateStore(path)
    try:
        assert reader.configuration_snapshot() is None
        assert reader.configuration_history() == []
        assert reader.get_task_effective_starts() == {}
        assert reader.configuration_constraints(["root"]) == {
            "running": 0, "tasks": {"root": {"unresolved": 0, "latest_end": None}}}
    finally:
        reader.close()
    assert path.read_bytes() == before
    assert sorted(item.name for item in path.parent.iterdir()) == ["state.db"]


def test_snapshot_and_history_survive_reopen_preserving_existing_facts(store):
    item = instance()
    store.enqueue_and_advance([item], {"root": NOW + timedelta(hours=1)})
    store.claim(item.instance_id, now=NOW)
    store.finish(item.instance_id, TaskStatus.UNKNOWN, now=NOW)
    facts = store.get_instance(item.instance_id)
    attempts = store.list_execution_attempts(item.instance_id)
    apply(store, backup_path=store.path.parent / "copy.db")
    path = store.path
    store.close()
    reader = StateStore(path)
    try:
        assert reader.get_task_effective_starts() == {"root": NOW}
        snapshot = reader.configuration_snapshot()
        assert snapshot["manifest"] == MANIFEST
        assert snapshot["revision"] == "revision-v1"
        assert datetime.fromisoformat(snapshot["task_starts"]["root"]) == NOW
        record = reader.configuration_history()[0]
        assert record["migration_id"] == "migration-1"
        assert record["previous_revision"] is None
        assert record["revision"] == "revision-v1"
        assert record["manifest"] == MANIFEST
        assert record["task_starts"] == snapshot["task_starts"]
        assert record["backup_path"] == str(path.parent / "copy.db")
        assert reader.get_instance(item.instance_id) == facts
        assert reader.list_execution_attempts(item.instance_id) == attempts
        assert reader.get_cursor("root") == NOW + timedelta(hours=1)
        assert reader.get_meta("coverage_start") == NOW.isoformat()
    finally:
        reader.close()


def test_history_retains_prior_manifests_and_returns_bounded_newest_records(store):
    apply(store)
    for index in range(2, 23):
        apply(store, expected_revision=f"revision-v{index - 1}", revision=f"revision-v{index}",
              migration_id=f"migration-{index}", manifest={"schema_version": 1, "version": index})
    records = store.configuration_history()
    assert len(records) == 20
    assert records[0]["revision"] == "revision-v22"
    assert records[-1]["revision"] == "revision-v3"
    assert store.configuration_history(100)[-1]["manifest"] == MANIFEST
    assert store.configuration_history(1)[0]["previous_revision"] == "revision-v21"


@pytest.mark.parametrize("limit", [0, -1, 101, True, 1.5])
def test_history_rejects_unbounded_or_invalid_limits(store, limit):
    with pytest.raises(ValueError):
        store.configuration_history(limit)


@pytest.mark.parametrize("field,value", [("expected_binding", "stale"),
                                         ("expected_revision", "stale")])
def test_compare_and_swap_rejects_stale_state_without_any_writes(store, field, value):
    apply(store)
    before = store.configuration_snapshot()
    with pytest.raises(RuntimeError, match="CONFIGURATION_STATE_CHANGED"):
        changes = dict(expected_revision="revision-v1", revision="revision-v2",
                       migration_id="migration-2", binding="binding-v2")
        apply(store, **(changes | {field: value}))
    assert store.configuration_snapshot() == before
    assert store.get_meta("binding") == "binding-v1"
    assert len(store.configuration_history()) == 1


def test_duplicate_migration_id_cannot_overwrite_snapshot_or_history(store):
    apply(store)
    before = store.configuration_snapshot()
    with pytest.raises(sqlite3.IntegrityError):
        apply(store, expected_revision="revision-v1", revision="revision-v2", binding="binding-v2")
    assert store.configuration_snapshot() == before
    assert store.get_meta("binding") == "binding-v1"
    assert len(store.configuration_history()) == 1


def test_failure_rolls_back_metadata_history_and_new_schema(store):
    conn = store._connection(write=True)
    conn.execute("""CREATE TRIGGER reject_binding BEFORE UPDATE ON metadata
        WHEN new.key='binding' BEGIN SELECT RAISE(ABORT, 'disk failure'); END""")
    with pytest.raises(sqlite3.IntegrityError, match="disk failure"):
        apply(store, binding="binding-v2")
    assert store.configuration_snapshot() is None
    assert store.get_meta("configuration_required") is None
    assert store.get_meta("binding") == "binding-v1"
    assert store.get_meta("coverage_start") == NOW.isoformat()
    assert store.configuration_history() == []
    assert conn.execute("SELECT 1 FROM sqlite_master WHERE name='configuration_history'").fetchone() is None


def test_constraints_count_all_unresolved_states_and_compare_actual_times(store):
    later = NOW.replace(hour=3, tzinfo=timezone(timedelta(hours=-8)))
    earlier = NOW.replace(hour=18, tzinfo=timezone(timedelta(hours=8)))
    items = [instance(start=earlier), instance(start=later), instance("other")]
    store.enqueue_and_advance(items, {})
    store.claim(items[0].instance_id, now=NOW)
    store.finish(items[0].instance_id, TaskStatus.SUCCESS, now=NOW)
    store.claim(items[1].instance_id, now=NOW)
    store.finish(items[1].instance_id, TaskStatus.UNKNOWN, now=NOW)
    store.claim(items[2].instance_id, now=NOW)
    result = store.configuration_constraints(["root", "missing"])
    assert result == {"running": 1, "tasks": {
        "root": {"unresolved": 1, "latest_end": later + timedelta(minutes=5)},
        "missing": {"unresolved": 0, "latest_end": None}}}


def test_backup_is_private_complete_and_refuses_to_overwrite(store, tmp_path):
    apply(store)
    store.enqueue_and_advance([instance()], {"root": NOW})
    destination = tmp_path / "backup.db"
    assert store.backup_configuration(destination) == destination
    assert stat.S_IMODE(destination.stat().st_mode) == 0o600
    with sqlite3.connect(destination) as conn:
        assert conn.execute("PRAGMA quick_check").fetchone()[0] == "ok"
        assert conn.execute("SELECT COUNT(*) FROM task_instances").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM configuration_history").fetchone()[0] == 1
    before = destination.read_bytes()
    with pytest.raises(FileExistsError):
        store.backup_configuration(destination)
    assert destination.read_bytes() == before


def test_backup_and_apply_require_writer_lock(store, tmp_path):
    reader = StateStore(store.path)
    try:
        with pytest.raises(RuntimeError, match="lock"):
            reader.backup_configuration(tmp_path / "copy.db")
        with pytest.raises(RuntimeError, match="lock"):
            apply(reader)
        assert not (tmp_path / "copy.db").exists()
    finally:
        reader.close()


def test_backup_failed_quick_check_cleans_only_new_file(store, tmp_path):
    destination = tmp_path / "broken.db"
    preserved = tmp_path / "existing.db"
    preserved.write_bytes(b"original")
    conn = store._connection(write=True)
    with conn:
        conn.execute("CREATE TABLE broken(value INTEGER CHECK(value > 0))")
        conn.execute("PRAGMA ignore_check_constraints=ON")
        conn.execute("INSERT INTO broken VALUES (-1)")
        conn.execute("PRAGMA ignore_check_constraints=OFF")
    with pytest.raises(sqlite3.DatabaseError, match="CONFIGURATION_BACKUP_INVALID"):
        store.backup_configuration(destination)
    assert not destination.exists()
    assert preserved.read_bytes() == b"original"
    assert store.get_meta("binding") == "binding-v1"


def test_enqueue_rejects_early_batch_before_any_insert_or_cursor_write(store):
    apply(store)
    good, early = instance(), instance(start=NOW - timedelta(minutes=5))
    statements = []
    store._connection().set_trace_callback(statements.append)
    with pytest.raises(ValueError, match="TASK_WINDOW_BEFORE_EFFECTIVE_START"):
        store.enqueue_and_advance((value for value in (good, early)), {"root": NOW})
    assert not any(sql.lstrip().upper().startswith("INSERT") for sql in statements)
    assert store.list_instances() == []
    assert store.get_cursor("root") is None


def test_enqueue_consumes_generator_once_and_accepts_boundary_and_legacy_windows(store):
    early = instance(start=NOW - timedelta(minutes=5))
    store.enqueue_and_advance((value for value in [early]), {})
    apply(store)
    item = instance()
    store.enqueue_and_advance((value for value in [item]), {"root": NOW})
    assert store.get_status(early.instance_id) == TaskStatus.PENDING
    assert store.get_status(item.instance_id) == TaskStatus.PENDING
    assert store.get_cursor("root") == NOW


def test_migration_lookup_finds_old_id_outside_bounded_history(store):
    assert store.configuration_migration("missing") is None
    apply(store)
    for index in range(2, 23):
        apply(store, expected_revision=f"revision-v{index - 1}", revision=f"revision-v{index}",
              migration_id=f"migration-{index}")
    assert store.configuration_migration("migration-1")["manifest"] == MANIFEST
    assert store.configuration_migration("missing") is None


@pytest.mark.parametrize("dangling", [False, True])
def test_backup_refuses_symlinks_without_touching_link_or_target(store, tmp_path, dangling):
    target = tmp_path / "target.db"
    if not dangling:
        target.write_bytes(b"preserved")
    destination = tmp_path / "link.db"
    destination.symlink_to(target)
    with pytest.raises(FileExistsError):
        store.backup_configuration(destination)
    assert destination.is_symlink()
    assert target.exists() is not dangling
    if not dangling:
        assert target.read_bytes() == b"preserved"


def test_backup_rejects_active_transaction_without_creating_destination(store, tmp_path):
    destination = tmp_path / "backup.db"
    conn = store._connection(write=True)
    conn.execute("BEGIN IMMEDIATE")
    try:
        with pytest.raises(RuntimeError, match="CONFIGURATION_TRANSACTION_ACTIVE"):
            store.backup_configuration(destination)
        assert not destination.exists()
    finally:
        conn.rollback()


def test_backup_syncs_parent_directory_before_reporting_success(store, tmp_path, monkeypatch):
    from data_converge.state import configuration
    original_sync = configuration.os.fsync
    synchronized = []

    def sync(descriptor):
        synchronized.append(stat.S_ISDIR(configuration.os.fstat(descriptor).st_mode))
        return original_sync(descriptor)

    monkeypatch.setattr(configuration.os, "fsync", sync)
    destination = store.backup_configuration(tmp_path / "durable.db")
    assert synchronized == [False, True]
    with sqlite3.connect(destination) as connection:
        assert connection.execute("SELECT value FROM metadata WHERE key='binding'").fetchone()[0] == "binding-v1"


def test_directory_sync_failure_cleans_backup_and_preserves_source(store, tmp_path, monkeypatch):
    from data_converge.state import configuration
    original_sync = configuration.os.fsync
    before = list(store._connection().iterdump())
    destination = tmp_path / "uncommitted.db"
    failed_descriptors = []

    def fail_directory_sync(descriptor):
        if stat.S_ISDIR(configuration.os.fstat(descriptor).st_mode):
            failed_descriptors.append(descriptor)
            raise OSError("directory sync failed")
        return original_sync(descriptor)

    monkeypatch.setattr(configuration.os, "fsync", fail_directory_sync)
    with pytest.raises(OSError, match="directory sync failed"):
        store.backup_configuration(destination)
    assert not destination.exists()
    assert list(store._connection().iterdump()) == before
    with pytest.raises(OSError):
        configuration.os.fstat(failed_descriptors[0])


def test_legacy_migration_records_that_future_starts_require_configuration(store):
    assert store.get_meta("configuration_required") is None
    apply(store)
    path = store.path
    store.close()
    reader = StateStore(path)
    try:
        assert reader.get_meta("configuration_required") == "1"
        assert reader.configuration_snapshot()["revision"] == "revision-v1"
        assert reader.configuration_migration("migration-1")["revision"] == "revision-v1"
    finally:
        reader.close()


def test_migration_resets_only_requested_cursors_and_backup_keeps_originals(store, tmp_path):
    cursors = {"root": NOW + timedelta(hours=3), "unchanged": NOW, "removed": NOW}
    store.enqueue_and_advance([], cursors)
    destination = store.backup_configuration(tmp_path / "before-migration.db")
    apply(store, reset_task_cursors=("root", "new"), backup_path=destination)
    assert store.get_cursor("root") is None
    assert store.get_cursor("new") is None
    assert store.get_cursor("unchanged") == NOW
    assert store.get_cursor("removed") == NOW
    with sqlite3.connect(destination) as connection:
        old = connection.execute("SELECT next_fire_time FROM schedule_cursors WHERE taskname='root'").fetchone()
        assert datetime.fromisoformat(old[0]) == NOW + timedelta(hours=3)


def test_audit_failure_rolls_back_requested_cursor_resets(store):
    apply(store)
    next_fire = NOW + timedelta(hours=3)
    store.enqueue_and_advance([], {"root": next_fire, "unchanged": NOW})
    before = store.configuration_snapshot()
    with pytest.raises(sqlite3.IntegrityError):
        apply(store, expected_revision="revision-v1", revision="revision-v2",
              binding="binding-v2", reset_task_cursors=("root",))
    assert store.get_cursor("root") == next_fire
    assert store.get_cursor("unchanged") == NOW
    assert store.configuration_snapshot() == before
    assert store.get_meta("binding") == "binding-v1"
    assert len(store.configuration_history()) == 1
