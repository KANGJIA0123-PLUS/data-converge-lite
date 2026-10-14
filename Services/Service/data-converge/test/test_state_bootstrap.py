"""First-start admission preserves an existing or interrupted local ledger."""

import errno
import os
import shutil
import sqlite3
from contextlib import closing
from pathlib import Path

import pytest

from data_converge.state import StateStore
from data_converge.state import bootstrap
from data_converge.state.bootstrap import StateBootstrap, StateBootstrapError


def mark_ready(store):
    store.set_meta("binding", "deployment")
    store.set_meta("coverage_start", "2026-10-14T10:00:00+08:00")
    store.mark_initialized()


def test_fresh_start_initializes_once_and_preserves_metadata(tmp_path):
    path = tmp_path / "ledger" / "state.sqlite3"
    marker = tmp_path / ".ledger.initialized"
    with closing(StateStore(path, create=True)) as store:
        assert store.acquire_startup_lock() is True
        assert marker.read_text() == "pending\n"
        mark_ready(store)
    assert marker.read_text() == "ready\n"
    with closing(StateStore(path, create=True)) as store:
        assert store.acquire_startup_lock() is False
        assert store.get_meta("coverage_start") == "2026-10-14T10:00:00+08:00"


@pytest.mark.parametrize("remove_directory", [False, True])
def test_known_ledger_loss_is_not_recreated(tmp_path, remove_directory):
    path = tmp_path / "ledger" / "state.sqlite3"
    with closing(StateStore(path, create=True)) as store:
        store.acquire_startup_lock()
        mark_ready(store)
    if remove_directory:
        shutil.rmtree(path.parent)
    else:
        path.unlink()
    with closing(StateStore(path, create=True)) as store:
        with pytest.raises(StateBootstrapError, match="STATE_DATABASE_MISSING"):
            store.acquire_startup_lock()
    assert not path.exists()


def test_existing_directory_residue_prevents_first_start(tmp_path):
    path = tmp_path / "state.sqlite3"
    (tmp_path / "heartbeat.json").write_text("old process")
    with closing(StateStore(path, create=True)) as store:
        with pytest.raises(StateBootstrapError, match="STATE_DIRECTORY_NOT_EMPTY"):
            store.acquire_startup_lock()
    assert not path.exists()


@pytest.mark.parametrize("contents", [b"", b"not a sqlite database"])
def test_existing_invalid_database_is_never_repaired(tmp_path, contents):
    path = tmp_path / "state.sqlite3"
    path.write_bytes(contents)
    with closing(StateStore(path, create=True)) as store:
        with pytest.raises(StateBootstrapError, match="STATE_DATABASE_INVALID"):
            store.acquire_startup_lock()
    assert path.read_bytes() == contents


def test_startup_does_not_add_a_missing_schema_table(tmp_path):
    path = tmp_path / "state.sqlite3"
    with closing(StateStore(path, create=True)) as store:
        store.acquire_lock()
        with store._connection(write=True) as connection:
            connection.execute("DROP TABLE dwd_ready_signals")
    with closing(StateStore(path, create=True)) as store:
        with pytest.raises(StateBootstrapError, match="STATE_DATABASE_INVALID"):
            store.acquire_startup_lock()
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute(
            "SELECT name FROM sqlite_master WHERE name='dwd_ready_signals'"
        ).fetchall() == []


def test_existing_ledger_can_receive_marker_after_validation(tmp_path):
    path = tmp_path / "ledger" / "state.sqlite3"
    with closing(StateStore(path, create=True)) as store:
        store.acquire_lock()
        store.set_meta("binding", "deployment")
        store.set_meta("coverage_start", "2026-10-14T10:00:00+08:00")
    marker = tmp_path / ".ledger.initialized"
    assert not marker.exists()
    with closing(StateStore(path, create=True)) as store:
        assert store.acquire_startup_lock() is False
        store.mark_initialized()
    assert marker.read_text() == "ready\n"


def test_startup_obeys_the_same_single_writer_lock(tmp_path):
    path = tmp_path / "state.sqlite3"
    with closing(StateStore(path, create=True)) as first:
        first.acquire_startup_lock()
        with closing(StateStore(path, create=True)) as second:
            with pytest.raises(RuntimeError, match="state lock"):
                second.acquire_startup_lock()
        mark_ready(first)


def test_interrupted_creation_blocks_reinitialization(tmp_path, monkeypatch):
    path = tmp_path / "ledger" / "state.sqlite3"

    def fail_connection(*args, **kwargs):
        raise OSError("simulated interruption before database creation")

    with monkeypatch.context() as patch:
        patch.setattr(sqlite3, "connect", fail_connection)
        with closing(StateStore(path, create=True)) as store:
            with pytest.raises(OSError):
                store.acquire_startup_lock()
    assert (tmp_path / ".ledger.initialized").read_text() == "pending\n"
    with closing(StateStore(path, create=True)) as store:
        with pytest.raises(StateBootstrapError, match="STATE_DATABASE_MISSING"):
            store.acquire_startup_lock()


def test_unfinished_metadata_cannot_be_marked_initialized(tmp_path):
    path = tmp_path / "ledger" / "state.sqlite3"
    with closing(StateStore(path, create=True)) as store:
        store.acquire_startup_lock()
        with pytest.raises(StateBootstrapError, match="STATE_NOT_INITIALIZED"):
            store.mark_initialized()
    assert (tmp_path / ".ledger.initialized").read_text() == "pending\n"
    with closing(StateStore(path, create=True)) as store:
        assert store.acquire_startup_lock() is False
        assert store.get_meta("binding") is None
        with pytest.raises(StateBootstrapError, match="STATE_NOT_INITIALIZED"):
            store.mark_initialized()


def test_marker_stream_open_failure_closes_descriptor_and_removes_file(tmp_path, monkeypatch):
    state = StateBootstrap(tmp_path / "ledger" / "state.sqlite3")
    state.directory.mkdir()
    state.marker.write_text("pending\n")
    created = []
    create_temporary = bootstrap.tempfile.mkstemp

    def capture_temporary(*args, **kwargs):
        result = create_temporary(*args, **kwargs)
        created.append(result)
        return result

    def fail_open(*args, **kwargs):
        raise OSError("injected stream open failure")

    monkeypatch.setattr(bootstrap.tempfile, "mkstemp", capture_temporary)
    monkeypatch.setattr(bootstrap.os, "fdopen", fail_open)
    try:
        with pytest.raises(OSError, match="injected stream open failure"):
            state.mark_ready()
        descriptor, name = created[0]
        assert not Path(name).exists()
        assert state.marker.read_text() == "pending\n"
        with pytest.raises(OSError) as error:
            os.fstat(descriptor)
        assert error.value.errno == errno.EBADF
    finally:
        for descriptor, _ in created:
            try:
                os.close(descriptor)
            except OSError:
                pass


def test_marker_replace_failure_keeps_marker_and_removes_temporary_file(tmp_path, monkeypatch):
    state = StateBootstrap(tmp_path / "ledger" / "state.sqlite3")
    state.directory.mkdir()
    state.marker.write_text("pending\n")

    def fail_replace(*args, **kwargs):
        raise OSError("injected replace failure")

    monkeypatch.setattr(bootstrap.os, "replace", fail_replace)
    with pytest.raises(OSError, match="injected replace failure"):
        state.mark_ready()
    assert state.marker.read_text() == "pending\n"
    assert sorted(tmp_path.iterdir()) == sorted([state.directory, state.marker])
