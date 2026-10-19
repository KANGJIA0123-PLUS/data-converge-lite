"""Verify stored snapshots or an explicitly supplied legacy configuration."""

from pathlib import Path

from data_converge.config.application import StartupError, binding, load_app_config, load_definition
from data_converge.config.evolution import build_manifest, validate_configuration_snapshot


def previous_definition(path):
    path = Path(path).resolve()
    raw = load_app_config(path)
    section = dict(raw.get('config', {}))
    defaults = {'convergence_rules_path': 'config/convergence_rules.yaml', 'sql_directory': 'sql'}
    for key, fallback in defaults.items():
        value = section.get(key)
        if value is None or (isinstance(value, str) and value.startswith('resources/')):
            if path.parent.name != 'config' or path.parent.parent.name != 'resources':
                raise StartupError('CONFIG_PREVIOUS_RESOURCES_REQUIRED')
            suffix = fallback if value is None else value[len('resources/'):]
            section[key] = str(path.parent.parent / suffix)
    raw['config'] = section
    return load_definition(path, raw_config=raw)


def stored_baseline(store, coverage):
    try:
        snapshot = store.configuration_snapshot()
    except (TypeError, ValueError):
        raise StartupError('CONFIG_STATE_INVALID') from None
    if snapshot is None:
        return None
    return validate_configuration_snapshot(snapshot, coverage)


def load_baseline(store, coverage, previous_config):
    current = stored_baseline(store, coverage)
    if current is not None:
        return current
    if store.get_meta('configuration_required') is not None:
        raise StartupError('CONFIG_STATE_INVALID')
    if previous_config is None:
        raise StartupError('CONFIG_PREVIOUS_CONFIG_REQUIRED')
    definition = previous_definition(previous_config)
    if binding(definition.config, definition.dag) != store.get_meta('binding'):
        raise StartupError('CONFIG_PREVIOUS_BINDING_MISMATCH')
    manifest = build_manifest(definition)
    return manifest, None, {name: coverage for name in manifest['tasks']}
