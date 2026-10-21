"""An early write keeps its formal coverage promise across configuration entry points."""
from contextlib import closing
from copy import deepcopy
from datetime import timedelta

import pytest
import yaml

from data_converge.application import Application
from data_converge.backfill.preaggregate import configuration_evidence
from data_converge.config.application import StartupError
from data_converge.dag import TaskStatus
from data_converge.state import StateStore
from .test_main import app
from .test_state import NOW


def seed_early(path, status=TaskStatus.SUCCESS):
    application = Application(path)
    application.initialize(NOW.replace(minute=0).isoformat())
    instance = application.definition.builder.create_instance(
        application.definition.dag, "simple", NOW + timedelta(minutes=5))
    item = {"item_id": "early:0", "revision": "request-revision",
            "formal_scheduled_time": instance.scheduled_time.isoformat(),
            "config_evidence": configuration_evidence(application.definition, "simple")}
    with closing(StateStore(application.directory / "state.sqlite3")) as store:
        store.acquire_lock()
        assert hasattr(store, "begin_preaggregate")
        store.begin_preaggregate(item, instance, now=NOW)
        store.finish_preaggregate(item["item_id"], status, now=NOW)
    return application, item, instance


@pytest.mark.parametrize("change", ["sql", "target", "cron", "cleanup", "database", "removed"])
def test_direct_configuration_change_cannot_abandon_uncovered_early_write(app, change):
    application, _, _ = seed_early(app)
    rules_path = app.parent / "rules.yaml"
    rules = yaml.safe_load(rules_path.read_text())
    if change == "sql":
        (app.parent / "sql/simple.sql").write_text("INSERT INTO target SELECT 42")
    elif change == "database":
        raw = yaml.safe_load(app.read_text())
        raw["clickhouse"]["database"] = "different"
        app.write_text(yaml.safe_dump(raw))
    else:
        field, value = {"target": ("target_table", "other"), "cron": ("cron", "2 * * * *"),
                        "cleanup": ("cleanup_column", "other_time"),
                        "removed": ("taskname", "replacement")}[change]
        rules["convergence_rules"][0][field] = value
        rules_path.write_text(yaml.safe_dump(rules))
    with pytest.raises(StartupError, match="PREAGGREGATE_COVERAGE"):
        Application(app).initialize()
    assert application.status().payload["counts"] == {}


def test_unrelated_task_and_wait_settings_can_change_without_losing_coverage(app):
    application, _, _ = seed_early(app)
    rules_path = app.parent / "rules.yaml"
    rules = yaml.safe_load(rules_path.read_text())
    other = dict(rules["convergence_rules"][0], taskname="other", target_table="other")
    rules["convergence_rules"].append(other)
    rules_path.write_text(yaml.safe_dump(rules))
    raw = yaml.safe_load(app.read_text())
    raw["scheduler"]["cross_period_wait_seconds"] = 0
    app.write_text(yaml.safe_dump(raw))
    assert Application(app).initialize().exit_code == 0
    assert application.status().payload["counts"] == {}


@pytest.mark.parametrize("change", ["manifest", "effective", "coverage", "cursor"])
def test_write_entry_rejects_lost_coverage_before_any_configuration_mutation(app, change):
    application, _, instance = seed_early(app)
    with closing(StateStore(application.directory / "state.sqlite3")) as store:
        store.acquire_lock()
        before = store.configuration_snapshot()
        manifest = deepcopy(application.manifest)
        starts = store.get_task_effective_starts()
        if change == "manifest":
            manifest["tasks"]["simple"]["definition"]["target_table"] = "other"
        elif change == "effective":
            starts["simple"] = instance.data_end
        elif change == "coverage":
            store.set_meta("coverage_start", instance.data_end.isoformat())
        else:
            with pytest.raises(ValueError, match="PREAGGREGATE_COVERAGE"):
                store.enqueue_and_advance([], {"simple": instance.scheduled_time + timedelta(minutes=5)})
            assert store.get_cursor("simple") is None
            return
        with pytest.raises(ValueError, match="PREAGGREGATE_COVERAGE"):
            store.apply_configuration(expected_binding=store.get_meta("binding"),
                expected_revision=before["revision"], manifest=manifest, revision="new",
                task_starts=starts, binding="new", migration_id="new", applied_at=NOW)
        assert store.configuration_snapshot() == before
        assert len(store.configuration_history()) == 1


def test_actual_formal_success_releases_configuration_obligation(app):
    application, item, instance = seed_early(app)
    with closing(StateStore(application.directory / "state.sqlite3")) as store:
        store.acquire_lock()
        store.enqueue_and_advance([instance], {"simple": instance.scheduled_time + timedelta(minutes=5)})
        assert store.claim(instance.instance_id, now=instance.scheduled_time)
        store.finish(instance.instance_id, TaskStatus.SUCCESS, now=instance.scheduled_time)
        assert store.get_preaggregate(item["item_id"])["covered_at"]
    (app.parent / "sql/simple.sql").write_text("INSERT INTO target SELECT 42")
    assert Application(app).initialize().exit_code == 0


def test_migration_rejects_lost_coverage_before_creating_backup(app):
    from data_converge.configuration_migration.service import preview_config, apply_config
    application, _, instance = seed_early(app)
    (app.parent / "sql/simple.sql").write_text("INSERT INTO target SELECT 42")
    target = Application(app)
    options = dict(effective_from=instance.data_start.isoformat(), allow_backfill=True)
    path = application.directory / "state.sqlite3"
    before = path.read_bytes()
    plan = preview_config(target, **options)
    assert path.read_bytes() == before
    with pytest.raises(StartupError, match="PREAGGREGATE_COVERAGE"):
        apply_config(target, plan["plan_id"], **options)
    assert not (app.parent / "configuration-backups").exists()
    assert path.read_bytes() == before


@pytest.mark.parametrize("resolution", ["success", "retry"])
def test_manual_review_does_not_release_future_formal_coverage_promise(app, resolution):
    application, item, instance = seed_early(app, status=TaskStatus.UNKNOWN)
    application.resolve_preaggregate(item["item_id"], resolution, "Checked both queries ended")
    with closing(StateStore(application.directory / "state.sqlite3")) as store:
        assert store.get_status(instance.instance_id) is None
        assert store.get_preaggregate(item["item_id"])["covered_at"] is None
    (app.parent / "sql/simple.sql").write_text("INSERT INTO target SELECT 42")
    with pytest.raises(StartupError, match="PREAGGREGATE_COVERAGE"):
        Application(app).initialize()
