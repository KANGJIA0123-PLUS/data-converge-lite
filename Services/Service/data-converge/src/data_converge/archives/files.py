"""Consistent SQLite snapshots and verified, exclusive archive publication."""

from __future__ import annotations

import gzip
import hashlib
import os
import sqlite3
import stat
import tempfile
import zlib
from contextlib import contextmanager
from pathlib import Path
from typing import Callable


_CHUNK_BYTES = 1024 * 1024
_BACKUP_PAGES = 128
Check = Callable[[], None]


def regular_file(path: Path) -> os.stat_result | None:
    """Return an ordinary, singly linked file's identity without following links."""
    try:
        current = path.lstat()
    except FileNotFoundError:
        return None
    if not stat.S_ISREG(current.st_mode) or current.st_nlink != 1:
        raise ValueError("ARCHIVE_FILE_INVALID")
    return current


def _same_identity(first: os.stat_result, second: os.stat_result) -> bool:
    return (first.st_dev, first.st_ino) == (second.st_dev, second.st_ino)


def _same_contents(first: os.stat_result, second: os.stat_result) -> bool:
    return _same_identity(first, second) and (
        first.st_size, first.st_mtime_ns) == (second.st_size, second.st_mtime_ns)


def _unchanged(path: Path, expected: os.stat_result, *, contents=True) -> None:
    current = regular_file(path)
    matches = _same_contents if contents else _same_identity
    if current is None or not matches(current, expected):
        raise ValueError("ARCHIVE_FILE_CHANGED")


def _fsync_directory(directory: Path) -> None:
    descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def remove_file(path: Path, *, expected: os.stat_result | None = None) -> bool:
    """Remove only a still-matching regular file inside a caller-owned directory."""
    try:
        current = regular_file(path)
    except ValueError:
        return False
    if current is None or (expected is not None and not _same_contents(current, expected)):
        return False
    try:
        path.unlink()
    except FileNotFoundError:
        return False
    _fsync_directory(path.parent)
    return True


def _remove_owned(path: Path, identity: os.stat_result) -> None:
    try:
        current = regular_file(path)
    except ValueError:
        return
    if current is not None and _same_identity(current, identity):
        remove_file(path, expected=current)


@contextmanager
def _temporary(directory: Path):
    descriptor, name = tempfile.mkstemp(prefix=".archive-", dir=directory)
    path = Path(name)
    try:
        identity = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    try:
        yield path, identity
    finally:
        _remove_owned(path, identity)


@contextmanager
def _reader(path: Path):
    expected = regular_file(path)
    if expected is None:
        raise ValueError("ARCHIVE_FILE_INVALID")
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        opened = os.fstat(descriptor)
        if opened.st_nlink != 1 or not _same_contents(opened, expected):
            raise ValueError("ARCHIVE_FILE_CHANGED")
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            yield stream, expected
        _unchanged(path, expected)
    finally:
        os.close(descriptor)


def _sqlite_check(connection: sqlite3.Connection, check: Check) -> None:
    cancelled = []

    def progress():
        try:
            check()
        except BaseException as error:
            cancelled.append(error)
            return 1
        return 0

    connection.set_progress_handler(progress, 1000)
    try:
        check()
        if connection.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
            raise ValueError("ARCHIVE_CONTENT_INVALID")
        tables = connection.execute("SELECT name FROM sqlite_master WHERE type='table' "
                                    "AND name IN ('metadata', 'task_instances')").fetchall()
        if {row[0] for row in tables} != {"metadata", "task_instances"}:
            raise ValueError("ARCHIVE_CONTENT_INVALID")
        connection.execute("SELECT key, value FROM metadata LIMIT 1").fetchall()
        connection.execute("SELECT instance_id, taskname, status, data_start, data_end "
                           "FROM task_instances LIMIT 1").fetchall()
    except sqlite3.DatabaseError:
        if cancelled:
            raise cancelled[0]
        raise ValueError("ARCHIVE_CONTENT_INVALID") from None
    finally:
        connection.set_progress_handler(None, 0)


def _validate_sqlite(path: Path, check: Check) -> None:
    connection = sqlite3.connect(path.absolute().as_uri() + "?mode=ro&immutable=1", uri=True)
    try:
        _sqlite_check(connection, check)
    finally:
        connection.close()


def _backup(database: Path, temporary: Path, check: Check) -> None:
    source = sqlite3.connect(database.absolute().as_uri() + "?mode=ro", uri=True)
    try:
        target = sqlite3.connect(temporary.absolute().as_uri() + "?mode=rw", uri=True)
        try:
            source.backup(target, pages=_BACKUP_PAGES, sleep=0.05,
                          progress=lambda status, remaining, total: check())
            _sqlite_check(target, check)
        finally:
            target.close()
    except sqlite3.DatabaseError:
        raise ValueError("ARCHIVE_CONTENT_INVALID") from None
    finally:
        source.close()


def _rollback_publication(destination: Path, identity: os.stat_result) -> None:
    # Our exclusive publication can have two links until the temporary is removed.
    try:
        current = destination.lstat()
    except FileNotFoundError:
        return
    if stat.S_ISREG(current.st_mode) and _same_identity(current, identity):
        destination.unlink()
        _fsync_directory(destination.parent)


def _publish(temporary: Path, destination: Path, identity: os.stat_result) -> None:
    _unchanged(temporary, identity, contents=False)
    with _reader(temporary) as (stream, current):
        os.fsync(stream.fileno())
    try:
        os.link(temporary, destination, follow_symlinks=False)
    except FileExistsError:
        raise ValueError("ARCHIVE_FILE_INVALID") from None
    try:
        temporary.unlink()
        _fsync_directory(destination.parent)
    except BaseException:
        _rollback_publication(destination, current)
        raise


def create_snapshot(database: Path, destination: Path, check: Check) -> Path:
    """Back up committed SQLite pages, including WAL, without sharing a connection."""
    check()
    expected = regular_file(database)
    if expected is None or os.path.lexists(destination):
        raise ValueError("ARCHIVE_FILE_INVALID")
    with _temporary(destination.parent) as (temporary, identity):
        _backup(database, temporary, check)
        _unchanged(database, expected, contents=False)
        check()
        _publish(temporary, destination, identity)
    return destination


def _copy_chunks(source, target, check: Check) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    while True:
        check()
        chunk = source.read(_CHUNK_BYTES)
        if not chunk:
            return size, digest.hexdigest()
        size += len(chunk)
        digest.update(chunk)
        if target is not None:
            target.write(chunk)


def _decompress(path: Path, temporary: Path, check: Check) -> dict:
    with _reader(path) as (source, expected):
        with temporary.open("wb") as output:
            with gzip.GzipFile(fileobj=source, mode="rb") as compressed:
                size, digest = _copy_chunks(compressed, output, check)
    return {"original_bytes": size, "compressed_bytes": expected.st_size, "sha256": digest}


def verify_compressed(path: Path, check: Check) -> dict:
    """Verify gzip CRC, decompressed bytes, and SQLite's base ledger schema."""
    check()
    expected = regular_file(path)
    if expected is None:
        raise ValueError("ARCHIVE_FILE_INVALID")
    with _temporary(path.parent) as (temporary, identity):
        try:
            result = _decompress(path, temporary, check)
        except (gzip.BadGzipFile, EOFError, zlib.error):
            raise ValueError("ARCHIVE_CONTENT_INVALID") from None
        _unchanged(temporary, identity, contents=False)
        _validate_sqlite(temporary, check)
        _unchanged(path, expected)
    return result


def _matches_source(source: Path, compressed: dict, check: Check) -> None:
    with _reader(source) as (stream, expected):
        size, digest = _copy_chunks(stream, None, check)
    if size != compressed["original_bytes"] or digest != compressed["sha256"]:
        raise ValueError("ARCHIVE_CONTENT_INVALID")


def _compress(source: Path, temporary: Path, check: Check) -> tuple[int, str]:
    with _reader(source) as (input_stream, expected):
        with temporary.open("wb") as output:
            with gzip.GzipFile(filename="", fileobj=output, mode="wb", mtime=0) as compressed:
                result = _copy_chunks(input_stream, compressed, check)
    return result


def compress_snapshot(source: Path, destination: Path, check: Check) -> dict:
    """Publish a verified gzip without overwriting files or removing its source."""
    check()
    expected = regular_file(source)
    if expected is None:
        raise ValueError("ARCHIVE_FILE_INVALID")
    existing = regular_file(destination)
    if existing is not None:
        result = verify_compressed(destination, check)
        _matches_source(source, result, check)
        _unchanged(destination, existing)
    else:
        with _temporary(destination.parent) as (temporary, identity):
            size, digest = _compress(source, temporary, check)
            result = verify_compressed(temporary, check)
            if (size, digest) != (result["original_bytes"], result["sha256"]):
                raise ValueError("ARCHIVE_CONTENT_INVALID")
            _unchanged(source, expected)
            check()
            _publish(temporary, destination, identity)
    _unchanged(source, expected)
    return {name: result[name] for name in ("original_bytes", "compressed_bytes")}
