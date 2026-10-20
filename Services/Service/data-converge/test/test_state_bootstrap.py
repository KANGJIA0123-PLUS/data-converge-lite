"""First-start admission preserves an existing or interrupted local ledger."""

import errno
import logging
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


def test_fresh_start_initializes_once_and_preserves_metadata(tmp_path, caplog):
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
    assert "STATE_RECREATED" not in caplog.text


@pytest.mark.parametrize("remove_directory", [False, True])
def test_known_ledger_loss_is_recreated_with_history_warning(tmp_path, remove_directory, caplog):
    path = tmp_path / "ledger" / "state.sqlite3"
    with closing(StateStore(path, create=True)) as store:
        store.acquire_startup_lock()
        mark_ready(store)
    if remove_directory:
        shutil.rmtree(path.parent)
    else:
        path.unlink()
    with closing(StateStore(path, create=True)) as store:
        assert store.acquire_startup_lock() is True
        assert store.get_meta("binding") is None
        assert store.get_meta("coverage_start") is None
        assert store.list_instances() == []
        mark_ready(store)
    assert path.is_file()
    assert (tmp_path / ".ledger.initialized").read_text() == "ready\n"
    warnings = [record for record in caplog.records if record.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "STATE_RECREATED" in warnings[0].message
    assert "history_missing" in warnings[0].message
    assert "DC-STATE-002" in warnings[0].message


def test_existing_directory_residue_is_preserved_during_creation(tmp_path):
    path = tmp_path / "state.sqlite3"
    (tmp_path / "heartbeat.json").write_text("old process")
    (tmp_path / "archive").mkdir()
    (tmp_path / "archive" / "old.sqlite3").write_bytes(b"historical backup")
    with closing(StateStore(path, create=True)) as store:
        assert store.acquire_startup_lock() is True
    assert path.is_file()
    assert (tmp_path / "heartbeat.json").read_text() == "old process"
    assert (tmp_path / "archive" / "old.sqlite3").read_bytes() == b"historical backup"


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


def test_interrupted_creation_can_retry_without_an_existing_database(tmp_path, monkeypatch):
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
        assert store.acquire_startup_lock() is True
        mark_ready(store)
    assert (tmp_path / ".ledger.initialized").read_text() == "ready\n"


def test_orphaned_sqlite_sidecars_are_archived_before_database_creation(tmp_path):
    path = tmp_path / "ledger" / "state.sqlite3"
    path.parent.mkdir()
    (tmp_path / ".ledger.initialized").write_text("ready\n")
    lock = Path(str(path) + ".lock")
    lock.write_text("existing lock")
    lock_inode = lock.stat().st_ino
    for suffix in ("-wal", "-shm", "-journal"):
        Path(str(path) + suffix).write_bytes(("old" + suffix).encode())
    with closing(StateStore(path, create=True)) as store:
        assert store.acquire_startup_lock() is True
        assert store.list_instances() == []
    archived = list(path.parent.glob(".state.sqlite3.orphans.*"))
    assert len(archived) == 1
    for suffix in ("-wal", "-shm", "-journal"):
        assert (archived[0] / ("state.sqlite3" + suffix)).read_bytes() == ("old" + suffix).encode()
        assert not Path(str(path) + suffix).exists()
    assert lock.stat().st_ino == lock_inode
    assert lock.read_text() == "existing lock"


@pytest.mark.parametrize("target_exists", [False, True])
def test_missing_database_rejects_marker_symlink_without_touching_target(tmp_path, target_exists):
    path = tmp_path / "ledger" / "state.sqlite3"
    target = tmp_path / "external"
    if target_exists:
        target.write_text("leave untouched")
    marker = tmp_path / ".ledger.initialized"
    marker.symlink_to(target)
    with closing(StateStore(path, create=True)) as store:
        with pytest.raises(StateBootstrapError, match="STATE_DATABASE_INVALID"):
            store.acquire_startup_lock()
    assert marker.is_symlink()
    assert target.exists() is target_exists
    if target_exists:
        assert target.read_text() == "leave untouched"
    assert not path.exists()


def test_failed_orphan_archive_does_not_create_database(tmp_path, monkeypatch, caplog):
    path = tmp_path / "ledger" / "state.sqlite3"
    path.parent.mkdir()
    sidecar = Path(str(path) + "-wal")
    sidecar.write_bytes(b"old wal")
    (tmp_path / ".ledger.initialized").write_text("ready\n")

    def fail_rename(*args, **kwargs):
        raise OSError("injected archive failure")

    monkeypatch.setattr(bootstrap.os, "rename", fail_rename)
    with closing(StateStore(path, create=True)) as store:
        with pytest.raises(OSError, match="injected archive failure"):
            store.acquire_startup_lock()
    assert not path.exists()
    assert sidecar.read_bytes() == b"old wal"
    assert "STATE_RECREATED" not in caplog.text


def test_noncreating_startup_cannot_recreate_database_lost_after_open(tmp_path):
    path = tmp_path / "ledger" / "state.sqlite3"
    with closing(StateStore(path, create=True)) as initial:
        initial.acquire_lock()
    with closing(StateStore(path)) as reader:
        path.unlink()
        with pytest.raises(FileNotFoundError):
            reader.acquire_startup_lock()
    assert not path.exists()
    assert not (tmp_path / ".ledger.initialized").exists()


def test_missing_database_does_not_archive_before_owning_writer_lock(tmp_path):
    path = tmp_path / "ledger" / "state.sqlite3"
    with closing(StateStore(path, create=True)) as writer:
        writer.acquire_startup_lock()
        mark_ready(writer)
        path.unlink()
        sidecar = Path(str(path) + "-wal")
        sidecar.write_bytes(b"old wal")
        with closing(StateStore(path, create=True)) as contender:
            with pytest.raises(RuntimeError, match="state lock"):
                contender.acquire_startup_lock()
        assert not path.exists()
        assert sidecar.read_bytes() == b"old wal"
        assert list(path.parent.glob(".state.sqlite3.orphans.*")) == []


def test_failed_recreation_keeps_archived_sidecars_and_releases_lock(tmp_path, monkeypatch, caplog):
    path = tmp_path / "ledger" / "state.sqlite3"
    path.parent.mkdir()
    sidecar = Path(str(path) + "-wal")
    sidecar.write_bytes(b"historical wal")

    def fail_connection(*args, **kwargs):
        raise OSError("injected database creation failure")

    with closing(StateStore(path, create=True)) as failed:
        with monkeypatch.context() as patch:
            patch.setattr(sqlite3, "connect", fail_connection)
            with pytest.raises(OSError, match="injected database creation failure"):
                failed.acquire_startup_lock()
        assert not path.exists()
        archived = list(path.parent.glob(".state.sqlite3.orphans.*/state.sqlite3-wal"))
        assert len(archived) == 1
        assert archived[0].read_bytes() == b"historical wal"
        assert "STATE_RECREATED" not in caplog.text
        with closing(StateStore(path, create=True)) as replacement:
            assert replacement.acquire_startup_lock() is True
    assert archived[0].read_bytes() == b"historical wal"


def test_partial_orphan_archive_preserves_both_attempts(tmp_path, monkeypatch):
    path = tmp_path / "ledger" / "state.sqlite3"
    path.parent.mkdir()
    wal, shm = Path(str(path) + "-wal"), Path(str(path) + "-shm")
    wal.write_bytes(b"old wal")
    shm.write_bytes(b"old shm")
    rename = bootstrap.os.rename

    def fail_second_rename(source, destination):
        if source == shm:
            raise OSError("injected second archive failure")
        rename(source, destination)

    with closing(StateStore(path, create=True)) as failed:
        with monkeypatch.context() as patch:
            patch.setattr(bootstrap.os, "rename", fail_second_rename)
            with pytest.raises(OSError, match="injected second archive failure"):
                failed.acquire_startup_lock()
        assert not path.exists()
        assert shm.read_bytes() == b"old shm"
        first = list(path.parent.glob(".state.sqlite3.orphans.*"))[0]
        assert (first / "state.sqlite3-wal").read_bytes() == b"old wal"
        with closing(StateStore(path, create=True)) as retry:
            assert retry.acquire_startup_lock() is True
    archives = list(path.parent.glob(".state.sqlite3.orphans.*"))
    assert len(archives) == 2
    assert (first / "state.sqlite3-wal").read_bytes() == b"old wal"
    second = next(directory for directory in archives if directory != first)
    assert (second / "state.sqlite3-shm").read_bytes() == b"old shm"
    assert not wal.exists() and not shm.exists()


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
