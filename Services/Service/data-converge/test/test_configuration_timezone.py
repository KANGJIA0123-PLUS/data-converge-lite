"""Configuration effective starts are ordered by instant through repeated hours."""

from contextlib import closing
from datetime import datetime

import pytest

from data_converge.application import Application
from data_converge.config.application import StartupError
from data_converge.config.evolution import manifest_digest, validate_configuration_snapshot
from data_converge.config.state_binding import fenced_binding
from data_converge.configuration_migration.planner import _task_blockers
from data_converge.configuration_migration.service import preview_config
from data_converge.state import StateStore

from .test_configuration_migration import add_task
from .test_main import app
from .timezone_fixtures import deployment_zone


@pytest.mark.parametrize('coverage, start, accepted', [
    ('2026-11-01T01:55:00-04:00', '2026-11-01T01:05:00-05:00', True),
    ('2026-11-01T01:05:00-05:00', '2026-11-01T01:55:00-04:00', False),
])
def test_snapshot_start_cannot_be_misordered_by_repeated_hour(
        app, deployment_zone, coverage, start, accepted):
    zone = deployment_zone('America/New_York')
    manifest = Application(app).manifest
    snapshot = {'manifest': manifest, 'revision': manifest_digest(manifest),
                'task_starts': {'simple': start}}
    lower_bound = datetime.fromisoformat(coverage).astimezone(zone)
    if accepted:
        _, _, starts = validate_configuration_snapshot(snapshot, lower_bound)
        assert starts['simple'].isoformat() == start
    else:
        with pytest.raises(StartupError, match='CONFIG_STATE_INVALID'):
            validate_configuration_snapshot(snapshot, lower_bound)


@pytest.mark.parametrize('coverage, effective, expected', [
    ('2026-11-01T01:55:00-04:00', '2026-11-01T01:05:00-05:00', []),
    ('2026-11-01T01:05:00-05:00', '2026-11-01T01:55:00-04:00',
     [{'code': 'CONFIG_EFFECTIVE_BEFORE_COVERAGE'}]),
])
def test_preview_compares_effective_start_with_actual_coverage(
        app, deployment_zone, coverage, effective, expected):
    deployment_zone('America/New_York')
    Application(app).initialize(coverage)
    add_task(app)
    plan = preview_config(Application(app), effective_from=effective, allow_backfill=True)
    assert plan['blockers'] == expected
    assert plan['task_starts']['added'] == effective


@pytest.mark.parametrize('latest, effective, expected', [
    ('2026-11-01T01:55:00-04:00', '2026-11-01T01:05:00-05:00', []),
    ('2026-11-01T01:05:00-05:00', '2026-11-01T01:55:00-04:00',
     [{'code': 'CONFIG_TASK_WINDOW_OVERLAP', 'task': 'simple'}]),
])
def test_overlap_blocker_uses_real_order_for_system_zone_datetimes(
        deployment_zone, latest, effective, expected):
    zone = deployment_zone('America/New_York')
    latest_end = datetime.fromisoformat(latest).astimezone(zone)
    start = datetime.fromisoformat(effective).astimezone(zone)
    coverage = datetime.fromisoformat('2026-11-01T00:00:00-04:00').astimezone(zone)
    constraints = {'running': 0, 'tasks': {'simple': {'unresolved': 0, 'latest_end': latest_end}}}
    assert _task_blockers(constraints, {'removed': []}, start, coverage) == expected


def create_recovered_ledger(app, coverage, cutover):
    application = Application(app)
    manifest, previous = application.manifest, application.fingerprint
    revision, current = manifest_digest(manifest), fenced_binding(previous)
    start = datetime.fromisoformat(cutover)
    recovery = dict(schema_version=1, migration_id='timezone-recovery', cutover=cutover,
                    previous_binding=previous, current_binding=current, revision=revision,
                    backup_path='/backups/before.db', recovered_at=cutover, identity_verified=False,
                    previous_definition_available=False, historical_counts={})
    with closing(StateStore(application.directory / 'state.sqlite3', create=True)) as store:
        store.acquire_lock()
        store.set_meta('binding', previous)
        store.set_meta('coverage_start', coverage)
        store.apply_configuration(
            expected_binding=previous, expected_revision=None, manifest=manifest, revision=revision,
            task_starts={'simple': start}, binding=current, migration_id='timezone-recovery',
            applied_at=start, backup_path='/backups/before.db', retire_existing=True, recovery=recovery)


@pytest.mark.parametrize('effective, expected', [
    ('2026-11-01T01:00:00-05:00', [{'code': 'CONFIG_EFFECTIVE_BEFORE_COVERAGE'}]),
    ('2026-11-01T01:05:00-05:00', []),
])
def test_recovered_preview_uses_the_later_cutover_in_repeated_hour(
        app, deployment_zone, effective, expected):
    deployment_zone('America/New_York')
    cutover = '2026-11-01T01:05:00-05:00'
    create_recovered_ledger(app, '2026-11-01T01:55:00-04:00', cutover)
    add_task(app)
    plan = preview_config(Application(app), effective_from=effective, allow_backfill=True)
    assert plan['recovery_cutover'] == cutover
    assert plan['blockers'] == expected
