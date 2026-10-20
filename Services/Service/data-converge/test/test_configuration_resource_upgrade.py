"""Exercise four-task additions using the current catalog without remote resources."""

from contextlib import closing
from datetime import datetime
import shutil
import sqlite3

import pytest
import yaml

from data_converge.application import Application
from data_converge.configuration_migration.service import apply_config, preview_config
from data_converge.dag import TaskStatus
from data_converge.state import StateStore

from .paths import RESOURCES_ROOT
from .test_main import app
from .test_configuration_migration import preserve_config


def configure_real_rules(path, prefix):
    rules = yaml.safe_load((RESOURCES_ROOT / 'config/convergence_rules.yaml').read_text())
    added = [rule['taskname'] for rule in rules['convergence_rules']
             if rule['taskname'].startswith(prefix)]
    original = {'convergence_rules': [rule for rule in rules['convergence_rules']
                                      if rule['taskname'] not in added]}
    shutil.copytree(RESOURCES_ROOT / 'sql', path.parent / 'sql', dirs_exist_ok=True)
    (path.parent / 'rules.yaml').write_text(yaml.safe_dump(original))
    assert len(original['convergence_rules']) == 56 and len(added) == 4
    return rules, sorted(added)


def preserve_real_facts(application):
    definition = application.definition
    instant = datetime.fromisoformat('2026-10-19T09:31:00+08:00')
    names = ['task_modelmetric_server_5min_dws', 'task_node_5min_dws']
    candidates = [name for name in definition.dag.tasks
                  if definition.dag.tasks[name].aggregation_interval == 'PT5M']
    names = [name for name in names if name in candidates]
    names += [name for name in candidates if name not in names][:2 - len(names)]
    with closing(StateStore(application.directory / 'state.sqlite3')) as store:
        store.acquire_lock()
        for name, status in zip(names, [TaskStatus.SUCCESS, TaskStatus.UNKNOWN]):
            item = definition.builder.create_instance(definition.dag, name, instant)
            store.enqueue_and_advance([item], {name: instant})
            store.claim(item.instance_id, now=instant)
            store.finish(item.instance_id, status)


@pytest.mark.parametrize("prefix", ["task_resource_server_", "task_model_server_"])
def test_real_task_addition_migrates_legacy_without_clearing_unknown(app, prefix):
    rules, added = configure_real_rules(app, prefix)
    original = Application(app)
    original.initialize('2026-10-16T18:10:00+08:00')
    preserve_real_facts(original)
    previous = preserve_config(app)
    with closing(sqlite3.connect(original.directory / 'state.sqlite3')) as conn, conn:
        conn.execute("DELETE FROM metadata WHERE key IN ('configuration_snapshot','configuration_required')")
        conn.execute('DROP TABLE configuration_history')
    before = original.status().payload
    (app.parent / 'rules.yaml').write_text(yaml.safe_dump(rules))
    target = Application(app)
    options = dict(previous_config=previous, effective_from='2026-10-20T00:00:00+08:00',
                   allow_backfill=True)
    plan = preview_config(target, **options)
    assert plan['changes']['added'] == added and plan['changes']['changed'] == []
    assert plan['blockers'] == [] and len(target.definition.dag.tasks) == 60
    apply_config(target, plan['plan_id'], **options, accept_legacy_baseline=True)
    assert target.status().payload == before
    with closing(StateStore(target.directory / 'state.sqlite3')) as store:
        assert store.get_meta('coverage_start') == '2026-10-16T18:10:00+08:00'
        assert store.status_counts() == {'SUCCESS': 1, 'UNKNOWN': 1}
        assert len(store.get_task_effective_starts()) == 60
