"""An early result cannot be promoted by a premature formal execution."""
from contextlib import closing
from datetime import datetime, timedelta

import pytest

from data_converge.application import Application
from data_converge.config.application import StartupError
from data_converge.dag import TaskStatus
from data_converge.runtime import RuntimeDependencies, ExecutionAdapter
from data_converge.state import StateStore
from .test_main import app
from .test_preaggregate_state import candidate
from .test_preaggregate_state_configuration import seed_early
from .test_state import NOW, open_store


class FrozenDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return NOW.astimezone(tz) if tz else NOW.replace(tzinfo=None)


def successful_early(store):
    item, instance = candidate()
    store.begin_preaggregate(item, instance, now=NOW)
    store.finish_preaggregate(item["item_id"], TaskStatus.SUCCESS, now=NOW)
    return item, instance


def test_formal_claim_prepare_and_backfill_wait_until_early_window_ends(tmp_path):
    with closing(open_store(tmp_path / "state.db")) as store:
        item, instance = successful_early(store)
        store.enqueue_and_advance([instance], {})
        before = store.get_instance(instance.instance_id)
        assert not store.claim(instance.instance_id, now=NOW)
        assert not store.prepare_attempt(instance.instance_id, now=NOW)
        result = store.begin_backfill("formal:0", instance, recompute=True, now=NOW)
        assert result["status"] == "BLOCKED"
        assert result["error"] == "PREAGGREGATE_FORMAL_WINDOW_NOT_ENDED"
        assert store.get_instance(instance.instance_id) == before
        assert store.claim(instance.instance_id, now=instance.data_end)
        store.finish(instance.instance_id, TaskStatus.SUCCESS, now=instance.data_end)
        assert store.get_preaggregate(item["item_id"])["covered_at"] is not None


def test_dispatch_rechecks_execution_clock_before_consuming_attempt(tmp_path):
    with closing(open_store(tmp_path / "state.db")) as store:
        _, instance = successful_early(store)
        store.enqueue_and_advance([instance], {})
        assert store.prepare_attempt(instance.instance_id, now=instance.data_end)
        query_id = store.get_instance(instance.instance_id)["query_id"]
        with pytest.raises(ValueError, match="PREAGGREGATE_FORMAL_WINDOW_NOT_ENDED"):
            store.mark_dispatched(instance.instance_id, query_id, now=NOW)
        assert store.get_instance(instance.instance_id)["attempts"] == 0
        store.mark_dispatched(instance.instance_id, query_id, now=instance.data_end)
        assert store.get_instance(instance.instance_id)["attempts"] == 1


def test_premature_success_cannot_release_coverage_or_create_formal_success(tmp_path):
    with closing(open_store(tmp_path / "state.db")) as store:
        item, instance = successful_early(store)
        store.enqueue_and_advance([instance], {})
        assert store.claim(instance.instance_id, now=instance.data_end)
        with pytest.raises(ValueError, match="PREAGGREGATE_FORMAL_WINDOW_NOT_ENDED"):
            store.finish(instance.instance_id, TaskStatus.SUCCESS, now=NOW)
        assert store.get_status(instance.instance_id) == TaskStatus.RUNNING
        assert store.get_preaggregate(item["item_id"])["covered_at"] is None
        store.finish(instance.instance_id, TaskStatus.SUCCESS, now=instance.data_end)
        assert store.get_status(instance.instance_id) == TaskStatus.SUCCESS


def test_formal_confirmation_cannot_promote_unknown_before_early_window_ends(tmp_path, monkeypatch):
    with closing(open_store(tmp_path / "state.db")) as store:
        _, instance = successful_early(store)
        store.enqueue_and_advance([instance], {})
        store.claim(instance.instance_id, now=instance.data_end)
        store.finish(instance.instance_id, TaskStatus.UNKNOWN, now=instance.data_end)
        monkeypatch.setattr("data_converge.state.store.datetime", FrozenDateTime)
        with pytest.raises(ValueError, match="PREAGGREGATE_FORMAL_WINDOW_NOT_ENDED"):
            store.resolve(instance.instance_id, "SUCCESS", "Checked query")
        assert store.get_status(instance.instance_id) == TaskStatus.UNKNOWN


@pytest.mark.parametrize("operation", ["run_task", "run_once"])
def test_future_formal_command_rejected_before_runtime_and_instance_insert(app, monkeypatch, operation):
    original, item, instance = seed_early(app)
    calls = []
    dependencies = RuntimeDependencies(execution_factory=lambda *args: ExecutionAdapter(
        lambda value: calls.append(value.instance_id), lambda: None))
    application = Application(app, dependencies=dependencies)
    monkeypatch.setattr("data_converge.application.datetime", FrozenDateTime)
    arguments = [instance.scheduled_time.isoformat()]
    if operation == "run_task":
        arguments.insert(0, instance.taskname)
    with pytest.raises(StartupError, match="PREAGGREGATE_FORMAL_WINDOW_NOT_ENDED"):
        getattr(application, operation)(*arguments)
    assert calls == []
    with closing(StateStore(original.directory / "state.sqlite3")) as store:
        assert store.get_instance(instance.instance_id) is None
        assert store.get_preaggregate(item["item_id"])["covered_at"] is None


def test_future_run_once_without_early_records_keeps_existing_behavior(app, monkeypatch):
    calls = []
    dependencies = RuntimeDependencies(execution_factory=lambda *args: ExecutionAdapter(
        lambda value: calls.append(value.instance_id), lambda: None))
    application = Application(app, dependencies=dependencies)
    application.initialize(NOW.replace(minute=0).isoformat())
    monkeypatch.setattr("data_converge.application.datetime", FrozenDateTime)
    assert application.run_once((NOW + timedelta(minutes=5)).isoformat()).exit_code == 0
    assert len(calls) == 2


def test_open_early_window_does_not_stop_other_due_normal_windows(app, monkeypatch):
    _, item, instance = seed_early(app)
    calls = []
    dependencies = RuntimeDependencies(execution_factory=lambda *args: ExecutionAdapter(
        lambda value: calls.append(value.instance_id), lambda: None))
    application = Application(app, dependencies=dependencies)
    monkeypatch.setattr("data_converge.application.datetime", FrozenDateTime)
    monkeypatch.setattr("data_converge.scheduler.service.datetime", FrozenDateTime)
    assert application.run_once().exit_code == 0
    assert calls == ["simple@2026-10-12T10:05:00+08:00"]
    with closing(StateStore(application.directory / "state.sqlite3")) as store:
        assert store.get_instance(instance.instance_id) is None
        assert store.get_preaggregate(item["item_id"])["covered_at"] is None
