"""Keep valid historical rows without letting changed rules stop current work."""

from datetime import timedelta

import pytest

from data_converge.dag import DAGBuilder, DAGExecutor
from data_converge.scheduler import TaskScheduler

from .test_configuration_scheduling import EFFECTIVE, configured_scheduler
from .test_scheduler import FakeConfigLoader, make_rules


@pytest.mark.parametrize("change", ["removed", "window"])
def test_stale_pending_record_is_retained_and_warned_once(tmp_path, caplog, change):
    with configured_scheduler(tmp_path, None, coverage=EFFECTIVE) as (old, store, calls):
        previous = old.dag_builder.create_instance(
            old.dag, "task_5min", EFFECTIVE + timedelta(minutes=6))
        store.enqueue_and_advance([previous], {})
        saved = store.get_instance(previous.instance_id)
        rule = dict(make_rules()[0])
        if change == "removed":
            rule.update(taskname="replacement", sql_id="replacement")
        else:
            rule.update(aggregation_interval="PT1H", cron="3 * * * *")
        builder = DAGBuilder(FakeConfigLoader([rule]))
        dag = builder.build()
        executor = DAGExecutor(dag, lambda item: calls.append(item.instance_id), state_store=store)
        scheduler = TaskScheduler(dag, builder, executor, start_time=EFFECTIVE)
        scheduled = EFFECTIVE + timedelta(hours=1, minutes=3)
        current = builder.create_instance(dag, rule["taskname"], scheduled)
        store.enqueue_and_advance([current], {})
        scheduler.run_pending(scheduled)
        scheduler.run_pending(scheduled)
        assert store.get_instance(previous.instance_id) == saved
        expected = ("replacement@2026-10-19T11:00:00+08:00" if change == "removed"
                    else "task_5min@2026-10-19T11:00:00+08:00")
        assert calls == [expected]
        assert store.get_instance(expected)["status"] == "SUCCESS"
        assert caplog.text.count("PERSISTED_WINDOW_SKIPPED") == 1


@pytest.mark.parametrize("field", ["scheduled_time", "data_start", "data_end"])
@pytest.mark.parametrize("value", ["invalid", "2026-10-19T10:00:00"])
def test_corrupt_pending_timestamps_still_fail(tmp_path, field, value):
    with configured_scheduler(tmp_path, None, coverage=EFFECTIVE) as (scheduler, store, calls):
        instance = scheduler.dag_builder.create_instance(
            scheduler.dag, "task_5min", EFFECTIVE + timedelta(minutes=6))
        store.enqueue_and_advance([instance], {})
        with store._connection(write=True) as connection:
            connection.execute(f"UPDATE task_instances SET {field}=?", (value,))
        with pytest.raises(ValueError):
            scheduler.run_pending(EFFECTIVE + timedelta(minutes=6))
        assert calls == []
        assert store.get_instance(instance.instance_id)["attempts"] == 0


def test_changed_cron_realigns_old_cursor_before_enqueuing(tmp_path):
    rule = dict(make_rules()[0], cron="2,7,12,17,22,27,32,37,42,47,52,57 * * * *")
    with configured_scheduler(tmp_path, None, coverage=EFFECTIVE, rules=[rule]) as (scheduler, store, calls):
        store.enqueue_and_advance([], {"task_5min": EFFECTIVE + timedelta(minutes=6)})
        scheduler.tick(EFFECTIVE + timedelta(minutes=6))
        assert calls == [] and store.status_counts() == {}
        assert store.get_cursor("task_5min") == EFFECTIVE + timedelta(minutes=7)
        scheduler.tick(EFFECTIVE + timedelta(minutes=7))
        assert calls == ["task_5min@2026-10-19T10:05:00+08:00"]
