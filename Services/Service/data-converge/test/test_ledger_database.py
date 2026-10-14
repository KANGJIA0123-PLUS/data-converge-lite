"""The ledger owner releases its resources on both normal and failed startup."""

import sqlite3
from contextlib import closing

import pytest

from data_converge.state import StateStore
from data_converge.state.bootstrap import StateBootstrapError


class CloseFailureConnection(sqlite3.Connection):
    def close(self):
        super().close()
        raise OSError("injected connection close failure")


def test_database_close_failure_releases_writer_lock(tmp_path, monkeypatch):
    path = tmp_path / "state.sqlite3"
    connect = sqlite3.connect
    with monkeypatch.context() as patch:
        patch.setattr(sqlite3, "connect", lambda *args, **kwargs:
                      connect(*args, **kwargs, factory=CloseFailureConnection))
        store = StateStore(path, create=True)
        store.acquire_lock()
    with pytest.raises(OSError, match="connection close failure"):
        store.close()
    with closing(StateStore(path)) as replacement:
        replacement.acquire_lock()
        replacement.set_meta("writer", "replacement")
        assert replacement.get_meta("writer") == "replacement"
    store.close()


def test_database_closes_before_another_writer_can_acquire_lock(tmp_path, monkeypatch):
    path = tmp_path / "state.sqlite3"
    connect = sqlite3.connect

    class ClosingConnection(sqlite3.Connection):
        def close(self):
            try:
                with closing(StateStore(path)) as contender:
                    with pytest.raises(RuntimeError, match="state lock"):
                        contender.acquire_lock()
            finally:
                super().close()

    with monkeypatch.context() as patch:
        patch.setattr(sqlite3, "connect", lambda *args, **kwargs:
                      connect(*args, **kwargs, factory=ClosingConnection))
        store = StateStore(path, create=True)
        store.acquire_lock()
    store.close()
    with closing(StateStore(path)) as replacement:
        replacement.acquire_lock()


def test_open_failure_releases_writer_lock(tmp_path, monkeypatch):
    path = tmp_path / "state.sqlite3"
    with closing(StateStore(path, create=True)) as initialized:
        initialized.acquire_lock()

    def fail_open(*args, **kwargs):
        raise OSError("injected database open failure")

    with closing(StateStore(path)) as store:
        with monkeypatch.context() as patch:
            patch.setattr(sqlite3, "connect", fail_open)
            with pytest.raises(OSError, match="database open failure"):
                store.acquire_lock()
        with closing(StateStore(path)) as replacement:
            replacement.acquire_lock()


def test_invalid_startup_schema_releases_file_lock_without_repair(tmp_path):
    path = tmp_path / "state.sqlite3"
    with closing(StateStore(path, create=True)) as initialized:
        initialized.acquire_lock()
        with initialized._connection(write=True) as connection:
            connection.execute("DROP TABLE dwd_ready_signals")
    before = path.read_bytes()
    with closing(StateStore(path)) as first, closing(StateStore(path)) as second:
        for store in (first, second):
            with pytest.raises(StateBootstrapError, match="STATE_DATABASE_INVALID"):
                store.acquire_startup_lock()
    assert path.read_bytes() == before


def test_reader_upgrade_closes_read_connection_before_writing(tmp_path):
    path = tmp_path / "state.sqlite3"
    with closing(StateStore(path, create=True)) as initialized:
        initialized.acquire_lock()
    with closing(StateStore(path)) as store:
        reader = store._connection()
        store.acquire_lock()
        with pytest.raises(sqlite3.ProgrammingError, match="closed"):
            reader.execute("SELECT 1")
        store.set_meta("writer", "upgraded")
        assert store.get_meta("writer") == "upgraded"
