"""Waiting policy validates independently from business configuration identity."""

import pytest
import yaml

from data_converge.application import Application
from data_converge.config.application import StartupError, settings
from .test_main import app


def test_omitted_wait_policy_defaults_to_enabled_five_minutes():
    options = settings({})
    assert getattr(options, 'cross_period_wait_enabled', None) is True
    assert getattr(options, 'cross_period_wait_seconds', None) == 300
    assert getattr(options, 'cross_period_wait_overrides', None) == {}


@pytest.mark.parametrize('key,value', [
    ('cross_period_wait_enabled', 1), ('cross_period_wait_enabled', 'true'),
    ('cross_period_wait_seconds', True), ('cross_period_wait_seconds', -1),
    ('cross_period_wait_seconds', 3.0), ('cross_period_wait_seconds', '300'),
    ('cross_period_wait_overrides', []), ('cross_period_wait_overrides', {'x': True}),
    ('cross_period_wait_overrides', {'x': -1}), ('cross_period_wait_overrides', {'x': 1.5}),
    ('cross_period_wait_overrides', {'x': '300'}),
])
def test_invalid_wait_policy_rejected(key, value):
    with pytest.raises(StartupError, match='SCHEDULER_SETTING_INVALID'):
        settings({'scheduler': {key: value}})


def test_unknown_wait_override_task_is_rejected(app):
    raw = yaml.safe_load(app.read_text())
    raw['scheduler'] = {'cross_period_wait_overrides': {'absent': 10}}
    app.write_text(yaml.safe_dump(raw))
    with pytest.raises(StartupError, match='CROSS_PERIOD_WAIT_TASK_UNKNOWN'):
        Application(app).validate()


def test_wait_changes_preserve_business_identity_and_allow_zero(app):
    original = Application(app)
    original.validate()
    raw = yaml.safe_load(app.read_text())
    raw['scheduler'] = {'cross_period_wait_enabled': False,
                        'cross_period_wait_seconds': 0,
                        'cross_period_wait_overrides': {'simple': 5}}
    app.write_text(yaml.safe_dump(raw))
    changed = Application(app)
    changed.validate()
    assert changed.fingerprint == original.fingerprint
    assert changed.manifest == original.manifest
    assert changed.definition.options.cross_period_wait_seconds == 0
