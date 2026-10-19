"""Application startup binds approved configuration without changing legacy facts."""

from contextlib import closing
from datetime import datetime
import json
import sqlite3

import pytest
import yaml

from data_converge.application import Application, initialize_state
from data_converge.config.application import StartupError
from data_converge.config.evolution import manifest_digest
from data_converge.runtime import RuntimeDependencies
from data_converge.state import StateStore

from .test_main import app, local_http_app


START = "2026-10-12T10:00:00+08:00"
DUE = "2026-10-12T10:06:00+08:00"
EFFECTIVE = "2026-10-12T10:10:00+08:00"


def ledger_dump(path):
    with closing(sqlite3.connect(path.parent / "state/state.sqlite3")) as conn:
        return list(conn.iterdump())


def legacy_initialize(application):
    with closing(StateStore(application.directory / "state.sqlite3", create=True)) as store:
        store.acquire_startup_lock()
        initialize_state(store, application.fingerprint, datetime.fromisoformat(START))
        store.mark_initialized()


def reject_execution_factory(*args):
    pytest.fail("Invalid configuration opened execution resources")


@pytest.mark.parametrize("bootstrap", [False, True])
def test_new_ledger_saves_initial_manifest_and_task_starts_before_ready(app, monkeypatch, bootstrap):
    application = Application(app)
    marked = StateStore.mark_initialized

    def verify_before_ready(store):
        assert store.configuration_snapshot() is not None
        marked(store)

    monkeypatch.setattr(StateStore, "mark_initialized", verify_before_ready)
    if bootstrap:
        with application._active_state(bootstrap=True):
            pass
    else:
        application.initialize(START)
    with closing(StateStore(application.directory / "state.sqlite3")) as store:
        snapshot = store.configuration_snapshot()
        assert snapshot["manifest"] == application.manifest
        assert snapshot["revision"] == manifest_digest(application.manifest)
        assert store.get_task_effective_starts() == {
            "simple": datetime.fromisoformat(store.get_meta("coverage_start"))}
        history = store.configuration_history()
        assert len(history) == 1 and history[0]["backup_path"] is None
        assert store.status_counts() == {}


def test_validation_reads_sql_manifest_without_creating_state(app):
    (app.parent / "sql/simple.sql").write_bytes(b"\xff")
    with pytest.raises(StartupError, match="CONFIGURATION_SQL_UNREADABLE"):
        Application(app).validate()
    assert not (app.parent / "state").exists()


@pytest.mark.parametrize("change", ["sql", "connection"])
@pytest.mark.parametrize("operation", ["run_once", "initialize"])
def test_existing_snapshot_rejects_configuration_change_before_resources(app, change, operation):
    Application(app).initialize(START)
    before = ledger_dump(app)
    if change == "sql":
        (app.parent / "sql/simple.sql").write_text("INSERT INTO target SELECT 42")
    else:
        raw = yaml.safe_load(app.read_text())
        raw["clickhouse"]["database"] = "different-dataset"
        app.write_text(yaml.safe_dump(raw))
    application = Application(app, dependencies=RuntimeDependencies(
        execution_factory=reject_execution_factory))
    with pytest.raises(StartupError, match="^CONFIG_MIGRATION_REQUIRED$"):
        getattr(application, operation)()
    assert ledger_dump(app) == before
    assert not (app.parent / "state/heartbeat.json").exists()


def test_reinitialization_does_not_create_another_configuration_revision(app):
    Application(app).initialize(START)
    before = ledger_dump(app)
    Application(app).initialize(START)
    assert ledger_dump(app) == before


def test_legacy_ledger_does_not_automatically_adopt_a_snapshot(local_http_app):
    path, requests = local_http_app
    application = Application(path)
    legacy_initialize(application)
    replacement = "INSERT INTO target SELECT 42"
    (path.parent / "sql/simple.sql").write_text(replacement)
    restarted = Application(path)
    restarted.initialize(START)
    assert restarted.run_once(DUE).exit_code == 0
    assert [sql for _, sql in requests] == [replacement]
    with closing(StateStore(restarted.directory / "state.sqlite3")) as store:
        assert store.configuration_snapshot() is None
        assert store.configuration_history() == []


def test_legacy_binding_failure_keeps_original_error_code(app):
    legacy_initialize(Application(app))
    raw = yaml.safe_load(app.read_text())
    raw["clickhouse"]["database"] = "different-dataset"
    app.write_text(yaml.safe_dump(raw))
    with pytest.raises(StartupError, match="STATE_DEPLOYMENT_BINDING_CHANGED_OR_UNINITIALIZED"):
        Application(app).run_once(DUE)


def test_manual_task_start_is_checked_before_runtime_resources(app):
    application = Application(app)
    application.initialize(START)
    with closing(StateStore(application.directory / "state.sqlite3")) as store:
        store.acquire_lock()
        snapshot = store.configuration_snapshot()
        store.apply_configuration(
            expected_binding=application.fingerprint, expected_revision=snapshot["revision"],
            manifest=snapshot["manifest"], revision=snapshot["revision"],
            task_starts={"simple": datetime.fromisoformat(EFFECTIVE)},
            binding=application.fingerprint, migration_id="task-start", applied_at=datetime.fromisoformat(START))
    before = ledger_dump(app)
    application = Application(app, dependencies=RuntimeDependencies(
        execution_factory=reject_execution_factory))
    with pytest.raises(StartupError, match="TASK_WINDOW_BEFORE_EFFECTIVE_START"):
        application.run_task("simple", DUE)
    assert ledger_dump(app) == before
    assert not (app.parent / "state/heartbeat.json").exists()


@pytest.mark.parametrize("legacy", [False, True])
def test_running_application_does_not_adopt_replaced_sql(local_http_app, legacy):
    path, requests = local_http_app
    application = Application(path)
    if legacy:
        legacy_initialize(application)
    else:
        application.initialize(START)
    application.validate()
    (path.parent / "sql/simple.sql").write_text("INSERT INTO target SELECT 42")

    result = application.run_once(DUE)

    assert result.exit_code == 1 and requests == []
    with closing(StateStore(application.directory / "state.sqlite3")) as store:
        saved = store.list_instances()[0]
        assert saved["error"] == "SQL_CONFIGURATION_CHANGED"
        assert saved["status"] == "FAILED" and not saved["retryable"]


@pytest.mark.parametrize("operation", ["initialize", "run_once"])
def test_initial_snapshot_failure_never_marks_ready_or_restarts_as_legacy(app, monkeypatch, operation):
    def fail_snapshot(*args, **kwargs):
        raise OSError("snapshot persistence failed")

    with monkeypatch.context() as patch:
        patch.setattr(StateStore, "apply_configuration", fail_snapshot)
        with pytest.raises(OSError, match="snapshot persistence failed"):
            Application(app).initialize(START)
    assert (app.parent / ".state.initialized").read_text() == "pending\n"
    with closing(StateStore(app.parent / "state/state.sqlite3")) as store:
        assert store.get_meta("configuration_required") == "1"
    with pytest.raises(StartupError, match="CONFIG_STATE_INVALID"):
        getattr(Application(app), operation)()
    assert (app.parent / ".state.initialized").read_text() == "pending\n"


@pytest.mark.parametrize("starts", [
    {}, {"simple": "2026-10-12T10:00:00"}, {"simple": "invalid"},
    {"simple": "2026-10-12T09:55:00+08:00"},
    {"simple": START, "unknown": START}, {"simple": 42},
])
def test_invalid_persisted_task_starts_are_rejected_before_runtime(app, starts):
    Application(app).initialize(START)
    with closing(StateStore(app.parent / "state/state.sqlite3")) as store:
        store.acquire_lock()
        snapshot = store.configuration_snapshot()
        snapshot["task_starts"] = starts
        store.set_meta("configuration_snapshot", json.dumps(snapshot))
    before = ledger_dump(app)
    application = Application(app, dependencies=RuntimeDependencies(
        execution_factory=reject_execution_factory))
    with pytest.raises(StartupError, match="^CONFIG_STATE_INVALID$"):
        application.run_once(DUE)
    assert ledger_dump(app) == before


def test_interruption_before_initial_marker_cannot_restart_as_legacy(app, monkeypatch):
    def fail_metadata(*args, **kwargs):
        raise OSError("initial metadata write failed")

    with monkeypatch.context() as patch:
        patch.setattr(StateStore, "set_meta", fail_metadata)
        with pytest.raises(OSError, match="initial metadata write failed"):
            Application(app).initialize(START)
    with pytest.raises(StartupError, match="STATE_NOT_INITIALIZED"):
        Application(app).initialize(START)
    assert (app.parent / ".state.initialized").read_text() == "pending\n"
