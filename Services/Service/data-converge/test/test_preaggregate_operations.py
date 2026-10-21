"""Manual confirmation of preliminary writes must have an explicit CLI mode."""
import pytest

from data_converge import main as entry


def test_preaggregate_resolution_arguments_are_explicit():
    args = entry.arguments(['--resolve-preaggregate', 'bf_example:0',
                            '--resolution', 'retry', '--note', 'Both remote phases ended'])
    assert args.resolve_preaggregate == 'bf_example:0'
    assert args.resolution == 'retry'
    assert not args.resolve_instance and not args.backfill and not args.run_once


@pytest.mark.parametrize('extra', [
    ['--resolve-preaggregate', 'bf_example:0'],
    ['--resolve-preaggregate', 'bf_example:0', '--resolution', 'retry'],
    ['--resolve-preaggregate', 'bf_example:0', '--note', 'checked'],
    ['--resolve-preaggregate', 'bf_example:0', '--resolution', 'retry', '--note', ' '],
    ['--resolve-preaggregate', '../bad', '--resolution', 'success', '--note', 'checked'],
    ['--resolve-preaggregate', 'bf_example:0', '--run-once'],
])
def test_bad_preaggregate_resolution_rejected_before_config(tmp_path, extra):
    with pytest.raises(SystemExit) as error:
        entry.main(['--config', str(tmp_path / 'missing.yaml'), *extra])
    assert error.value.code == 2
    assert list(tmp_path.iterdir()) == []


def test_original_instance_resolution_arguments_still_work():
    args = entry.arguments(['--resolve-instance', 'task@2026-10-21T00:00:00+08:00',
                            '--resolution', 'retry', '--note', 'Remote execution stopped'])
    assert args.resolve_instance.startswith('task@')
    assert args.resolution == 'retry'


def test_help_describes_preaggregate_resolution(capsys):
    with pytest.raises(SystemExit) as result:
        entry.arguments(['--help'])
    assert result.value.code == 0
    text = capsys.readouterr().out
    assert '--resolve-preaggregate' in text
    assert '正式' in text and '提前' in text
@pytest.mark.parametrize('code', [
    'CROSS_PERIOD_WAIT_TASK_UNKNOWN', 'PREAGGREGATE_FORMAL_WINDOW_NOT_ENDED',
    'PREAGGREGATE_COVERAGE_TASK_CHANGED',
    'PREAGGREGATE_COVERAGE_DATASET_CHANGED', 'PREAGGREGATE_COVERAGE_TIMEZONE_CHANGED',
    'PREAGGREGATE_COVERAGE_CLEANUP_CHANGED', 'PREAGGREGATE_COVERAGE_START_CHANGED',
    'PREAGGREGATE_COVERAGE_EFFECTIVE_CHANGED', 'PREAGGREGATE_COVERAGE_RETIRED',
    'PREAGGREGATE_COVERAGE_CURSOR_PASSED', 'PREAGGREGATE_NOT_UNKNOWN',
    'PREAGGREGATE_UNRESOLVED',
])
def test_new_operation_errors_keep_safe_code_without_debug(code):
    from data_converge.errors import safe_error_details
    plain = code in ('CROSS_PERIOD_WAIT_TASK_UNKNOWN', 'PREAGGREGATE_FORMAL_WINDOW_NOT_ENDED')
    value = code if plain else code + ':bf_example:0'
    result = safe_error_details(ValueError(value))
    assert result['error_code'] == code
    assert 'bf_example' not in str(result)


def test_new_operation_error_does_not_leak_untrusted_suffix():
    from data_converge.errors import safe_error_details
    result = safe_error_details(ValueError('PREAGGREGATE_NOT_UNKNOWN:secret password'))
    assert result == {'error_type': 'ValueError'}
