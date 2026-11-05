"""Terminal layout for operator text output: local times, aligned tables, optional colour.

Colour is used only for an interactive terminal; pipes, files, tests and
``NO_COLOR`` / ``TERM=dumb`` always receive plain text.
"""
from datetime import datetime
import os
import sys
import unicodedata

from data_converge.time_policy import system_timezone

COLOURS = {'red': '31', 'green': '32', 'yellow': '33', 'blue': '34', 'cyan': '36',
           'grey': '90', 'bold': '1'}
STATUS_STYLE = {
    'PASS': ('✓', 'green'), 'SUCCESS': ('✓', 'green'), 'READY': ('✓', 'green'),
    'COMPLETED': ('✓', 'green'), 'SUCCEEDED': ('✓', 'green'), 'AUTO_RESUME': ('↻', 'cyan'),
    'WARN': ('!', 'yellow'), 'WAITING': ('…', 'yellow'), 'RUNNING': ('…', 'blue'),
    'PENDING': ('·', 'grey'), 'QUEUED': ('·', 'grey'), 'PLANNED': ('·', 'grey'),
    'BLOCKED': ('!', 'yellow'), 'RECONCILE': ('?', 'yellow'), 'PREVIEW': ('·', 'cyan'),
    'FAIL': ('✗', 'red'), 'FAILED': ('✗', 'red'), 'UNKNOWN': ('?', 'red'),
    'OUTCOME_UNKNOWN': ('?', 'red'), 'COMPLETED_WITH_ERRORS': ('✗', 'red'),
    'SKIP': ('-', 'grey'),
}


def colour_enabled(stream=None):
    stream = stream or sys.stdout
    if os.environ.get('NO_COLOR') or os.environ.get('TERM') == 'dumb':
        return False
    return bool(getattr(stream, 'isatty', lambda: False)())


def paint(text, colour):
    if not colour or not colour_enabled():
        return text
    return f"\x1b[{COLOURS[colour]}m{text}\x1b[0m"


def badge(status, width=8):
    """Icon plus padded status word, coloured by severity."""
    icon, colour = STATUS_STYLE.get(status, ('·', None))
    return paint(f"{icon} {pad(status, width)}", colour)


def display_width(text):
    return sum(2 if unicodedata.east_asian_width(char) in 'WF' else 1 for char in str(text))


def pad(text, width):
    text = str(text)
    return text + ' ' * max(0, width - display_width(text))


def table(headers, rows, indent='  '):
    """Left-aligned columns sized by display width; CJK counts as two cells."""
    rows = [[('-' if cell in (None, '') else str(cell)) for cell in row] for row in rows]
    widths = [max([display_width(header)] + [display_width(row[index]) for row in rows])
              for index, header in enumerate(headers)]

    def line(cells):
        return (indent + '  '.join(pad(cell, width) for cell, width in zip(cells, widths))).rstrip()

    return [paint(line(headers), 'grey')] + [line(row) for row in rows]


def heading(title):
    return paint(f"── {title} " + '─' * max(4, 56 - display_width(title)), 'bold')


def local_time(value, *, seconds=True, date=True):
    """Render any stored ISO time in the deployment zone without microseconds."""
    if not value:
        return '-'
    try:
        moment = datetime.fromisoformat(str(value))
    except ValueError:
        return str(value)
    if moment.tzinfo is not None:
        moment = moment.astimezone(system_timezone())
    pattern = ('%Y-%m-%d ' if date else '') + ('%H:%M:%S' if seconds else '%H:%M')
    return moment.strftime(pattern)


def window(start, end):
    """``2026-11-05 14:35 → 14:40``; the date repeats only when it changes."""
    left, right = local_time(start, seconds=False), local_time(end, seconds=False)
    if left[:10] == right[:10] and len(right) > 11:
        right = right[11:]
    return f"{left} → {right}"


def duration(start, end):
    try:
        seconds = (datetime.fromisoformat(end) - datetime.fromisoformat(start)).total_seconds()
    except (TypeError, ValueError):
        return '-'
    return seconds_text(seconds)


def seconds_text(seconds):
    if not seconds or seconds < 0:
        return '-'
    if seconds < 10:
        return f"{seconds:.1f}s"
    if seconds < 60:
        return f"{int(seconds)}s"
    minutes, rest = divmod(int(seconds), 60)
    return f"{minutes}m{rest:02d}s" if minutes < 60 else f"{minutes // 60}h{minutes % 60:02d}m"


def evidence_lines(evidence, indent='      ', width=100):
    """Compact ``key=value`` pairs wrapped at ``width`` cells instead of a JSON blob."""
    if not evidence:
        return []
    lines, current = [], ''
    for key, value in evidence.items():
        item = f"{key}={_scalar(value)}"
        if current and display_width(indent + current + '  ' + item) > width:
            lines.append(current)
            current = item
        else:
            current = f"{current}  {item}" if current else item
    lines.append(current)
    return [paint(indent + line, 'grey') for line in lines]


def _scalar(value):
    if isinstance(value, dict):
        return ', '.join(f"{key}={_scalar(item)}" for key, item in value.items()) or '{}'
    if isinstance(value, (list, tuple)):
        return ','.join(_scalar(item) for item in value) or '[]'
    if value is None:
        return '-'
    return str(value)
