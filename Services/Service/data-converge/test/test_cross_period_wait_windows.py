"""Calendar completeness follows real parent boundaries, including clock changes."""

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from data_converge.dag import DAGBuilder
from data_converge.dag.builder import required_cross_period_ids
from .test_scheduler import FakeConfigLoader, make_rules


@pytest.mark.parametrize('zone,end,parent,child,count,first,last', [
    ('Asia/Shanghai', '2026-02-01T00:10:00', 'P1D', 'P1M', 31,
     '2026-01-02T00:00:00+08:00', '2026-02-01T00:00:00+08:00'),
    ('America/New_York', '2026-03-09T00:10:00', 'PT1H', 'P1D', 23,
     '2026-03-08T01:00:00-05:00', '2026-03-09T00:00:00-04:00'),
    ('America/New_York', '2026-11-02T00:10:00', 'PT1H', 'P1D', 25,
     '2026-11-01T01:00:00-04:00', '2026-11-02T00:00:00-05:00'),
    ('Asia/Shanghai', '2026-10-02T01:07:00', 'PT5M', 'PT1H', 12,
     '2026-10-02T00:05:00+08:00', '2026-10-02T01:00:00+08:00'),
])
def test_cross_period_enumerates_actual_parent_windows(zone, end, parent, child, count, first, last):
    rules = make_rules()
    rules[0]['aggregation_interval'] = parent
    rules[1]['aggregation_interval'] = child
    builder = DAGBuilder(FakeConfigLoader(rules))
    dag = builder.build()
    instance = builder.create_instance(dag, 'task_hour', datetime.fromisoformat(end).replace(tzinfo=ZoneInfo(zone)))
    parents = required_cross_period_ids(dag, instance.task, instance.data_start, instance.data_end)
    assert instance.upstream_instance_ids == ()
    assert len(parents) == count and len(set(parents)) == count
    assert parents[0] == 'task_5min@' + first
    assert parents[-1] == 'task_5min@' + last


def test_same_period_hard_dependencies_are_not_cross_period_waits():
    rules = make_rules()
    rules[1]['aggregation_interval'] = 'PT5M'
    builder = DAGBuilder(FakeConfigLoader(rules))
    dag = builder.build()
    end = datetime.fromisoformat('2026-10-02T01:05:00+08:00')
    instance = builder.create_instance(dag, 'task_hour', end)
    assert instance.upstream_instance_ids == ('task_5min@2026-10-02T01:05:00+08:00',)
    assert required_cross_period_ids(dag, instance.task, instance.data_start, instance.data_end) == ()
