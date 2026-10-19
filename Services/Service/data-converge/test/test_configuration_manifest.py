"""Configuration snapshots detect execution semantics without storing secrets."""

from copy import deepcopy
from dataclasses import replace
import hashlib
import json

import pytest
import yaml

from data_converge.config.application import StartupError, load_definition
from data_converge.config.evolution import (
    build_manifest, compare_manifests, manifest_digest, validate_manifest,
)

from .test_main import app


def task_rule(name, dependencies=()):
    return {'taskname': name, 'source_tables': ['source'], 'target_table': name,
            'aggregation_interval': 'PT5M', 'cron': '*/5 * * * *',
            'depends_on': list(dependencies), 'sql_id': name}


def definition_with_tasks(app, rules):
    (app.parent / 'rules.yaml').write_text(yaml.safe_dump({'convergence_rules': rules}))
    for rule in rules:
        sql = app.parent / 'sql' / (rule['sql_id'] + '.sql')
        if not sql.exists():
            sql.write_text('INSERT INTO target SELECT 1')
    return load_definition(app)


def manifest_with_tasks(app, rules):
    return build_manifest(definition_with_tasks(app, rules))


def test_manifest_has_explicit_schema_and_stripped_sql_hash(app):
    (app.parent / 'sql/simple.sql').write_text('\n  SELECT 1 \n')
    manifest = build_manifest(load_definition(app))
    assert set(manifest) == {
        'schema_version', 'dataset_id', 'connection', 'readiness', 'coordination', 'tasks'}
    assert manifest['schema_version'] == 1 and manifest['dataset_id'] is None
    assert manifest['connection'] == {
        'endpoint': 'http://127.0.0.1:8123/', 'database': 'example', 'username': 'example'}
    assert manifest['readiness'] == {'enabled': False}
    assert manifest['coordination'] == {
        'enabled': False, 'cluster_name': 'data-converge-modelmate-metric'}
    task = manifest['tasks']['simple']
    assert set(task) == {'definition', 'sql_sha256', 'revision'}
    assert set(task['definition']) == {
        'taskname', 'source_tables', 'target_table', 'aggregation_interval',
        'cron', 'depends_on', 'sql_id'}
    assert task['sql_sha256'] == hashlib.sha256(b'SELECT 1').hexdigest()
    assert validate_manifest(manifest) is None


def test_four_added_tasks_do_not_change_existing_fifty_two_revisions(app):
    rules = [task_rule(f'task_{number:02d}') for number in range(52)]
    previous = manifest_with_tasks(app, rules)
    current = manifest_with_tasks(app, rules + [task_rule(f'task_{n}') for n in range(52, 56)])
    assert compare_manifests(previous, current) == {
        'added': ['task_52', 'task_53', 'task_54', 'task_55'], 'changed': [], 'removed': [],
        'connection_changed': False, 'readiness_changed': False,
        'coordination_changed': False, 'dataset_changed': False}
    assert all(current['tasks'][name] == value for name, value in previous['tasks'].items())


def test_sql_body_change_propagates_through_all_downstream_revisions(app):
    rules = [task_rule('root'), task_rule('middle', ['root']),
             task_rule('leaf', ['middle']), task_rule('unrelated')]
    previous = manifest_with_tasks(app, rules)
    (app.parent / 'sql/root.sql').write_text('INSERT INTO target SELECT 2')
    current = manifest_with_tasks(app, rules)
    assert compare_manifests(previous, current)['changed'] == ['leaf', 'middle', 'root']
    assert current['tasks']['middle']['sql_sha256'] == previous['tasks']['middle']['sql_sha256']
    assert current['tasks']['unrelated'] == previous['tasks']['unrelated']
    assert manifest_digest(current) != manifest_digest(previous)


def test_definition_change_propagates_and_removals_are_sorted(app):
    rules = [task_rule('root'), task_rule('child', ['root']), task_rule('z'), task_rule('a')]
    previous = manifest_with_tasks(app, rules)
    rules[0]['cron'] = '1 * * * *'
    current = manifest_with_tasks(app, rules[:2])
    diff = compare_manifests(previous, current)
    assert diff['changed'] == ['child', 'root']
    assert diff['removed'] == ['a', 'z']


def test_sql_outer_whitespace_does_not_change_approved_revision(app):
    previous = manifest_with_tasks(app, [task_rule('root')])
    (app.parent / 'sql/root.sql').write_text('\n INSERT INTO target SELECT 1 \t\n')
    current = manifest_with_tasks(app, [task_rule('root')])
    assert manifest_digest(current) == manifest_digest(previous)


def test_runtime_description_password_and_internal_fields_are_excluded(app):
    definition = load_definition(app)
    previous = build_manifest(definition)
    definition.config['clickhouse'].update(password='private-password', password_file='/rotated')
    definition.config.update(logging={'level': 'DEBUG'}, private_sdk_object=object())
    definition.config['state']['directory'] = '/different-runtime-directory'
    task = replace(definition.dag.tasks['simple'], description='private-description')
    definition = replace(definition, dag=replace(definition.dag, tasks={'simple': task}),
                         options=replace(definition.options, max_concurrent_tasks=12))
    current = build_manifest(definition)
    assert current == previous
    serialized = json.dumps(current)
    assert 'private-' not in serialized and 'password' not in serialized
    assert 'INSERT INTO' not in serialized


def test_task_and_dependency_order_do_not_change_revisions_or_digest(app):
    rules = [task_rule('a'), task_rule('b'), task_rule('child', ['b', 'a'])]
    previous = manifest_with_tasks(app, rules)
    rules[-1]['depends_on'].reverse()
    current = manifest_with_tasks(app, list(reversed(rules)))
    reordered = dict(reversed(list(current.items())))
    assert previous == current
    assert manifest_digest(previous) == manifest_digest(reordered)


def test_identity_changes_have_separate_diff_flags(app):
    definition = load_definition(app)
    previous = build_manifest(definition)
    definition.config['clickhouse']['database'] = 'changed'
    definition.config['state']['dataset_id'] = 'dataset.production-1'
    definition = replace(definition, leadership=replace(definition.leadership, enabled=True))
    definition.config['dwd_ready'] = {
        'enabled': True, 'queue_key': 'ready', 'redis': {
            'mode': 'standalone', 'db': 1, 'host': 'redis', 'port': 6379}}
    diff = compare_manifests(previous, build_manifest(definition))
    assert diff == {'added': [], 'changed': [], 'removed': [], 'connection_changed': True,
                    'readiness_changed': True, 'coordination_changed': True, 'dataset_changed': True}


@pytest.mark.parametrize('mode', ['standalone', 'cluster'])
def test_direct_readiness_keeps_only_queue_and_redis_location(app, mode):
    definition = load_definition(app)
    definition.config['dwd_ready'] = {
        'enabled': True, 'queue_key': 'ready', 'batch_size': 42, 'redis': {
            'mode': mode, 'db': 0, 'host': 'redis', 'port': 6379,
            'password': 'private-secret', 'password_env': 'PRIVATE_PASSWORD',
            'ssl': True, 'socket_timeout': 3, 'client': object()}}
    readiness = build_manifest(definition)['readiness']
    assert readiness == {'enabled': True, 'queue_key': 'ready', 'redis': {
        'mode': mode, 'db': 0, 'host': 'redis', 'port': 6379}}


def test_sentinel_readiness_excludes_credentials_keys_and_sdk_objects(app):
    definition = load_definition(app)
    definition.config['dwd_ready'] = {
        'enabled': True, 'queue_key': 'ready', 'redis': {
            'mode': 'sentinel', 'db': 0, 'service_name': 'service', 'username': 'admin',
            'paas_name': 'paas', 'password_file': '/pms/location',
            'password': 'private-secret', 'root_key_file': '/private-key',
            'work_key_file': '/private-work', 'sdk': object()}}
    manifest = build_manifest(definition)
    assert manifest['readiness']['redis'] == {
        'mode': 'sentinel', 'db': 0, 'service_name': 'service', 'username': 'admin',
        'paas_name': 'paas', 'password_file': '/pms/location'}
    assert 'private' not in json.dumps(manifest)
    assert validate_manifest(manifest) is None


@pytest.mark.parametrize('dataset', ['', 'a' * 129, '中文', 'a b', 'a\n', 123, True, {}])
def test_invalid_dataset_id_raises_fixed_credential_free_error(app, dataset):
    definition = load_definition(app)
    definition.config['state']['dataset_id'] = dataset
    with pytest.raises(StartupError, match='^DATASET_ID_INVALID$'):
        build_manifest(definition)


@pytest.mark.parametrize('dataset', [None, 'A_0.-', 'a' * 128])
def test_dataset_id_accepts_only_optional_bounded_ascii_identifier(app, dataset):
    definition = load_definition(app)
    definition.config['state']['dataset_id'] = dataset
    assert build_manifest(definition)['dataset_id'] == dataset


@pytest.mark.parametrize('mutation', ['schema', 'extra', 'task_extra', 'revision', 'sql_hash',
                                      'missing_dependency', 'cycle', 'taskname', 'dataset'])
def test_stored_manifest_validation_rejects_corruption_without_echoing_it(app, mutation):
    manifest = manifest_with_tasks(app, [task_rule('root'), task_rule('child', ['root'])])
    corrupt = deepcopy(manifest)
    task = corrupt['tasks']['root']
    if mutation == 'schema':
        corrupt['schema_version'] = 2
    elif mutation == 'extra':
        corrupt['password'] = 'private-secret'
    elif mutation == 'task_extra':
        task['definition']['password'] = 'private-secret'
    elif mutation == 'revision':
        task['revision'] = '0' * 64
    elif mutation == 'sql_hash':
        task['sql_sha256'] = 'private-secret'
    elif mutation == 'missing_dependency':
        task['definition']['depends_on'] = ['private-missing']
    elif mutation == 'cycle':
        task['definition']['depends_on'] = ['child']
    elif mutation == 'taskname':
        task['definition']['taskname'] = 'private-other'
    else:
        corrupt['dataset_id'] = 'private-invalid\n'
    with pytest.raises(StartupError, match='^CONFIGURATION_MANIFEST_INVALID$'):
        validate_manifest(corrupt)


def test_missing_sql_raises_fixed_error_without_exposing_path(app):
    definition = load_definition(app)
    (app.parent / 'sql/simple.sql').unlink()
    with pytest.raises(StartupError, match='^CONFIGURATION_SQL_UNREADABLE$'):
        build_manifest(definition)
