"""Operator text output stays readable: aligned tables, local times, quiet passes, grouped help."""
import io
import json

import pytest

from data_converge import main as entry
from data_converge.diagnostics import terminal
from data_converge.diagnostics.report import check, render_text, report
from .test_diagnostics import seed
from .test_main import app  # noqa: F401  (fixture)
from .timezone_fixtures import deployment_zone  # noqa: F401  (fixture)


def test_table_aligns_cjk_and_ascii_by_display_width():
    lines = terminal.table(['状态', 'task'], [['成功', 'a'], ['FAILED', None]])
    width = terminal.display_width
    assert width(lines[1][:lines[1].index(' a')]) == width(lines[2][:lines[2].index(' -')])
    assert lines[2].endswith('-')


def test_times_render_in_deployment_zone_without_microseconds(deployment_zone):
    deployment_zone('Asia/Shanghai')
    assert terminal.local_time('2026-11-05T06:40:00.123456+00:00') == '2026-11-05 14:40:00'
    assert terminal.window('2026-11-05T14:35:00+08:00', '2026-11-05T14:40:00+08:00') == '2026-11-05 14:35 → 14:40'
    assert terminal.window('2026-11-05T23:00:00+08:00', '2026-11-06T00:00:00+08:00').endswith('2026-11-06 00:00')
    assert terminal.duration('2026-11-05T14:40:00+08:00', '2026-11-05T14:43:05+08:00') == '3m05s'
    assert terminal.local_time(None) == '-'


def test_colour_only_for_interactive_terminals(monkeypatch):
    class Tty(io.StringIO):
        def isatty(self):
            return True

    monkeypatch.delenv('NO_COLOR', raising=False)
    monkeypatch.setenv('TERM', 'xterm')
    assert terminal.colour_enabled(Tty()) and not terminal.colour_enabled(io.StringIO())
    monkeypatch.setenv('NO_COLOR', '1')
    assert not terminal.colour_enabled(Tty())


def test_passing_evidence_is_hidden_unless_verbose_and_problems_come_first():
    value = report('diagnose', [
        check('config', 'PASS', '应用配置可读取', {'path': '/pass-evidence'}),
        check('heartbeat', 'FAIL', '心跳缺失', {'path': '/fail-evidence'}, '核对服务进程'),
    ])
    text = render_text(value)
    assert text.index('心跳缺失') < text.index('应用配置可读取')
    assert '/fail-evidence' in text and '核对服务进程' in text and '/pass-evidence' not in text
    assert '{"' not in text
    assert '/pass-evidence' in render_text(value, verbose=True)


def test_failures_text_is_a_table_with_next_step(app, capsys):  # noqa: F811
    store, instances = seed(app, count=2)
    store.close()
    assert entry.main(['--config', str(app), '--failures']) == 1
    text = capsys.readouterr().out
    header = next(line for line in text.splitlines() if '错误码' in line and '窗口' in line)
    assert '状态' in header and '任务' in header
    assert instances[0].instance_id in text and 'query_id' in text
    assert '→' in text and '下一步' in text and '--inspect-instance' in text
    assert '.000000' not in text


def test_json_output_is_unchanged_by_text_layout(app, capsys):  # noqa: F811
    store, _ = seed(app, count=1)
    store.close()
    assert entry.main(['--config', str(app), '--failures', '--format', 'json', '--verbose']) == 1
    value = json.loads(capsys.readouterr().out)
    assert set(value) == {'schema_version', 'command', 'status', 'checked_at', 'timezone', 'summary', 'checks', 'data'}


def test_help_groups_options_by_purpose(capsys):
    with pytest.raises(SystemExit) as result:
        entry.arguments(['--help'])
    assert result.value.code == 0
    text = capsys.readouterr().out
    order = [text.index(title) for title in ('排查查询（只读）:', '修复操作（会改状态）:', '配置变更:', '其他与调试:')]
    assert order == sorted(order)
    assert text.index('--failures', text.index('排查查询')) < text.index('修复操作')
    for phrase in ('skip_batches', 'current_then_resume', 'task_timeout_seconds', 'task_parallelism',
                   '--resolve-mirror-conflict', 'LEADERSHIP_BUSY'):
        assert phrase in text
    assert 'start_data_converge.sh --' not in text


@pytest.mark.parametrize('args, hint', [
    (['--failures', '--history'], '每次只能使用一个主命令'),
    (['--dwd-status', '--window', '2026-11-05T14:33:00'], '对齐五分钟'),
    (['--verbose'], '完整用法'),
])
def test_argument_errors_add_a_chinese_hint(app, capsys, args, hint):  # noqa: F811
    with pytest.raises(SystemExit) as error:
        entry.main(['--config', str(app), *args])
    assert error.value.code == 2
    err = capsys.readouterr().err
    assert 'dconv: error:' in err and hint in err
    assert not (app.parent / 'state').exists()
