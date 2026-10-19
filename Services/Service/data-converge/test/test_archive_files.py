"""Archive file operations preserve live ledgers and publish verified snapshots."""

import gzip
import hashlib
import os
import sqlite3
import stat
import threading
from pathlib import Path

import pytest

from data_converge.archives import files
from data_converge.state import StateStore


def check():
    return None


@pytest.fixture
def database(tmp_path):
    path = tmp_path / "state.sqlite3"
    store = StateStore(path, create=True)
    store.acquire_lock()
    store.set_meta("binding", "test-binding")
    store.set_meta("coverage_start", "2026-10-19T00:00:00+08:00")
    store.close()
    return path


def read_binding(path):
    connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
    try:
        return connection.execute("SELECT value FROM metadata WHERE key='binding'").fetchone()[0]
    finally:
        connection.close()


def test_snapshot_copies_committed_wal_without_changing_live_database(database, tmp_path):
    writer = sqlite3.connect(database)
    writer.execute("PRAGMA journal_mode=WAL")
    writer.execute("UPDATE metadata SET value='in-wal' WHERE key='binding'")
    writer.commit()
    before = database.read_bytes()
    wal = Path(str(database) + "-wal").read_bytes()
    destination = tmp_path / "backup.sqlite3"
    try:
        assert files.create_snapshot(database, destination, check) == destination
        assert read_binding(destination) == "in-wal"
        assert database.read_bytes() == before
        assert Path(str(database) + "-wal").read_bytes() == wal
        assert stat.S_IMODE(destination.stat().st_mode) == 0o600
    finally:
        writer.close()


def test_snapshot_refuses_overwrite_and_leaves_foreign_file(database, tmp_path):
    destination = tmp_path / "foreign"
    destination.write_bytes(b"foreign")
    with pytest.raises(ValueError, match="ARCHIVE_FILE_INVALID"):
        files.create_snapshot(database, destination, check)
    assert destination.read_bytes() == b"foreign"


def test_compression_roundtrips_sqlite_and_preserves_source(database, tmp_path):
    original = database.read_bytes()
    destination = tmp_path / "backup.sqlite3.gz"
    result = files.compress_snapshot(database, destination, check)
    assert gzip.decompress(destination.read_bytes()) == original
    assert database.read_bytes() == original
    assert result == {"original_bytes": len(original), "compressed_bytes": destination.stat().st_size}
    assert stat.S_IMODE(destination.stat().st_mode) == 0o600
    assert files.verify_compressed(destination, check) == {
        **result, "sha256": hashlib.sha256(original).hexdigest()}
    assert files.compress_snapshot(database, destination, check) == result
    assert sorted(path.name for path in tmp_path.iterdir()) == [
        "backup.sqlite3.gz", "state.sqlite3", "state.sqlite3.lock"]


def test_existing_different_archive_is_not_overwritten(database, tmp_path):
    destination = tmp_path / "backup.gz"
    files.compress_snapshot(database, destination, check)
    before = destination.read_bytes()
    connection = sqlite3.connect(database)
    with connection:
        connection.execute("UPDATE metadata SET value='changed' WHERE key='binding'")
    connection.close()
    with pytest.raises(ValueError, match="ARCHIVE_CONTENT_INVALID"):
        files.compress_snapshot(database, destination, check)
    assert destination.read_bytes() == before


@pytest.mark.parametrize("kind", ["symlink", "hardlink", "directory"])
def test_regular_file_and_removal_refuse_unsafe_paths(database, tmp_path, kind):
    unsafe = tmp_path / "unsafe"
    if kind == "symlink":
        unsafe.symlink_to(database)
    elif kind == "hardlink":
        os.link(database, unsafe)
    else:
        unsafe.mkdir()
    with pytest.raises(ValueError, match="ARCHIVE_FILE_INVALID"):
        files.regular_file(unsafe)
    assert files.remove_file(unsafe) is False
    assert os.path.lexists(unsafe)
    assert database.exists()


def test_remove_file_refuses_replacement_or_changed_content(tmp_path):
    path = tmp_path / "managed"
    path.write_bytes(b"old")
    expected = path.stat()
    replacement = tmp_path / "replacement"
    replacement.write_bytes(b"replacement")
    replacement.replace(path)
    assert files.remove_file(path, expected=expected) is False
    changed = path.stat()
    path.write_bytes(b"edited after inspection")
    assert files.remove_file(path, expected=changed) is False
    assert files.remove_file(path, expected=path.stat()) is True
    assert files.remove_file(path) is False
    assert files.regular_file(path) is None


@pytest.mark.parametrize("content", [b"not gzip", gzip.compress(b"not SQLite")])
def test_verification_rejects_invalid_content_without_leaking_tempfiles(tmp_path, content):
    path = tmp_path / "invalid.gz"
    path.write_bytes(content)
    with pytest.raises(ValueError, match="ARCHIVE_CONTENT_INVALID"):
        files.verify_compressed(path, check)
    assert list(tmp_path.iterdir()) == [path]


def test_verification_rejects_valid_sqlite_without_ledger_tables(tmp_path):
    foreign = tmp_path / "foreign.db"
    connection = sqlite3.connect(foreign)
    connection.execute("CREATE TABLE unrelated (value TEXT)")
    connection.close()
    path = tmp_path / "foreign.gz"
    path.write_bytes(gzip.compress(foreign.read_bytes()))
    with pytest.raises(ValueError, match="ARCHIVE_CONTENT_INVALID"):
        files.verify_compressed(path, check)


def test_verification_checks_gzip_crc(database, tmp_path):
    path = tmp_path / "corrupt.gz"
    content = bytearray(gzip.compress(database.read_bytes()))
    content[-8] ^= 1
    path.write_bytes(content)
    with pytest.raises(ValueError, match="ARCHIVE_CONTENT_INVALID"):
        files.verify_compressed(path, check)


def test_cancellation_removes_own_outputs_and_preserves_source(database, tmp_path):
    calls = []

    def cancel():
        calls.append(True)
        if len(calls) >= 3:
            raise ValueError("ARCHIVE_CANCELLED")

    before = database.read_bytes()
    destination = tmp_path / "cancelled.gz"
    with pytest.raises(ValueError, match="ARCHIVE_CANCELLED"):
        files.compress_snapshot(database, destination, cancel)
    assert database.read_bytes() == before
    assert not destination.exists()
    assert not list(tmp_path.glob(".*"))


def test_snapshot_backup_cancellation_cleans_destination(database, tmp_path):
    calls = []

    def cancel():
        calls.append(True)
        if len(calls) >= 2:
            raise ValueError("ARCHIVE_CANCELLED")

    destination = tmp_path / "cancelled.sqlite3"
    with pytest.raises(ValueError, match="ARCHIVE_CANCELLED"):
        files.create_snapshot(database, destination, cancel)
    assert not destination.exists()
    assert not list(tmp_path.glob(".*"))


@pytest.mark.parametrize("operation", ["create_snapshot", "compress_snapshot"])
def test_publication_failure_preserves_source_and_removes_temporaries(
        database, tmp_path, monkeypatch, operation):
    before = database.read_bytes()
    destination = tmp_path / "failed"

    def fail_link(*args, **kwargs):
        raise OSError("simulated publication failure")

    monkeypatch.setattr(files.os, "link", fail_link)
    with pytest.raises(OSError, match="simulated publication failure"):
        getattr(files, operation)(database, destination, check)
    assert database.read_bytes() == before
    assert not destination.exists()
    assert not list(tmp_path.glob(".archive-*"))


def test_compression_detects_source_modified_during_read(database, tmp_path):
    calls = []
    destination = tmp_path / "changed.gz"

    def mutate():
        calls.append(True)
        if len(calls) == 2:
            with database.open("ab") as stream:
                stream.write(b"changed during read")

    with pytest.raises(ValueError, match="ARCHIVE_FILE_CHANGED"):
        files.compress_snapshot(database, destination, mutate)
    assert not destination.exists()
    assert not list(tmp_path.glob(".archive-*"))


@pytest.mark.parametrize("operation", ["create_snapshot", "compress_snapshot"])
@pytest.mark.parametrize("kind", ["symlink", "hardlink"])
def test_archive_operations_refuse_linked_source(database, tmp_path, operation, kind):
    source = tmp_path / "linked"
    if kind == "symlink":
        source.symlink_to(database)
    else:
        os.link(database, source)
    destination = tmp_path / "archive"
    with pytest.raises(ValueError, match="ARCHIVE_FILE_INVALID"):
        getattr(files, operation)(source, destination, check)
    assert not destination.exists()


def test_archive_does_not_overwrite_destination_created_before_publication(
        database, tmp_path, monkeypatch):
    destination = tmp_path / "raced.gz"
    original_link = os.link

    def publish_with_competitor(source, target, **kwargs):
        destination.write_bytes(b"competitor")
        return original_link(source, target, **kwargs)

    monkeypatch.setattr(files.os, "link", publish_with_competitor)
    with pytest.raises(ValueError, match="ARCHIVE_FILE_INVALID"):
        files.compress_snapshot(database, destination, check)
    assert destination.read_bytes() == b"competitor"
    assert not list(tmp_path.glob(".archive-*"))


def test_verification_rejects_views_in_place_of_ledger_tables(tmp_path):
    source = tmp_path / "foreign.sqlite3"
    connection = sqlite3.connect(source)
    connection.executescript("""
        CREATE TABLE foreign_data (value TEXT);
        CREATE VIEW metadata AS SELECT value AS key, value FROM foreign_data;
        CREATE VIEW task_instances AS SELECT value AS instance_id, value AS taskname,
            value AS status, value AS data_start, value AS data_end FROM foreign_data;
    """)
    connection.close()
    archive = tmp_path / "foreign.gz"
    archive.write_bytes(gzip.compress(source.read_bytes()))
    with pytest.raises(ValueError, match="ARCHIVE_CONTENT_INVALID"):
        files.verify_compressed(archive, check)


def test_wal_writer_can_commit_while_backup_is_copying(database, tmp_path):
    copying, committed, ready, release = (threading.Event() for _ in range(4))
    errors = []

    def write_during_backup():
        connection = sqlite3.connect(database)
        try:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("INSERT INTO metadata VALUES ('padding', ?)", ("x" * 3000000,))
            connection.commit()
            ready.set()
            assert copying.wait(3)
            connection.execute("UPDATE metadata SET value='concurrent' WHERE key='binding'")
            connection.commit()
            committed.set()
            assert release.wait(3)
        except BaseException as error:
            errors.append(error)
            ready.set()
            committed.set()
        finally:
            connection.close()

    calls = []

    def await_commit():
        calls.append(True)
        if len(calls) == 2:
            copying.set()
            assert committed.wait(3)

    writer = threading.Thread(target=write_during_backup)
    writer.start()
    try:
        assert ready.wait(3)
        destination = tmp_path / "consistent.sqlite3"
        files.create_snapshot(database, destination, await_commit)
        assert read_binding(destination) == "concurrent"
        assert not errors
    finally:
        release.set()
        writer.join(3)
    assert not writer.is_alive()


def test_publication_rolls_back_own_link_if_temporary_unlink_fails(database, tmp_path, monkeypatch):
    destination = tmp_path / "failed.gz"
    original_unlink = Path.unlink
    failed = []

    def fail_once(path, *args, **kwargs):
        if path.name.startswith(".archive-") and path.stat().st_nlink == 2 and not failed:
            failed.append(True)
            raise OSError("temporary unlink failed")
        return original_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", fail_once)
    with pytest.raises(OSError, match="temporary unlink failed"):
        files.compress_snapshot(database, destination, check)
    assert not destination.exists()
    assert not list(tmp_path.glob(".archive-*"))
    assert database.exists()


def test_sqlite_integrity_check_propagates_cancellation(database):
    connection = sqlite3.connect(database)
    connection.executemany("INSERT INTO metadata VALUES (?, ?)",
                           [(f"padding-{number}", "value") for number in range(5000)])
    connection.commit()
    calls = []

    def cancel():
        calls.append(True)
        if len(calls) > 1:
            raise ValueError("ARCHIVE_CANCELLED")

    try:
        with pytest.raises(ValueError, match="ARCHIVE_CANCELLED"):
            files._sqlite_check(connection, cancel)
    finally:
        connection.close()
    assert len(calls) == 2


def test_verification_detects_archive_changed_during_sqlite_check(database, tmp_path, monkeypatch):
    archive = tmp_path / "changed.gz"
    files.compress_snapshot(database, archive, check)
    original_validate = files._validate_sqlite

    def validate_then_change(path, callback):
        original_validate(path, callback)
        with archive.open("ab") as output:
            output.write(b"changed")

    monkeypatch.setattr(files, "_validate_sqlite", validate_then_change)
    with pytest.raises(ValueError, match="ARCHIVE_FILE_CHANGED"):
        files.verify_compressed(archive, check)


def test_existing_archive_is_rechecked_after_comparing_source(database, tmp_path, monkeypatch):
    archive = tmp_path / "existing.gz"
    files.compress_snapshot(database, archive, check)
    original_compare = files._matches_source

    def compare_then_change(source, compressed, callback):
        original_compare(source, compressed, callback)
        with archive.open("ab") as output:
            output.write(b"changed")

    monkeypatch.setattr(files, "_matches_source", compare_then_change)
    with pytest.raises(ValueError, match="ARCHIVE_FILE_CHANGED"):
        files.compress_snapshot(database, archive, check)
