"""Render the application's bound parameters for diagnostics, never execution."""

import re


_NAMES = frozenset({"start_time", "end_time", "system_timezone"})
_EPOCH = re.compile(r"[0-9]{1,10}")
_COMMENT = re.compile(r"/\*|\*/")
_PARAMETER = re.compile(r"\{\s*([A-Za-z_][0-9A-Za-z_]*)\s*:\s*([^{}]+)\}")
_TOKEN = re.compile(
    r"(?P<parameter>\{[^{}]*\})"
    r"|(?P<quote>['\"`])|(?P<line>--|//|\#!|\# )|(?P<block>/\*)"
    r"|(?P<heredoc>\$[A-Za-z_0-9]*\$)"
)


def sql_string_literal(value: str) -> str:
    """Quote diagnostic text using ClickHouse string literal escapes."""
    escapes = {"\\": "\\\\", "'": "\\'", "\n": "\\n", "\r": "\\r", "\t": "\\t",
               "\0": "\\0", "\b": "\\b", "\f": "\\f", "\a": "\\a", "\v": "\\v"}
    return "'" + "".join(escapes.get(character, character) for character in value) + "'"


def _parameter(name, kind, parameters):
    values = parameters.get("param_" + name, ())
    if name not in _NAMES or len(values) != 1:
        return None
    value = values[0]
    if kind == "String":
        # HTTP parameters use ClickHouse's escaped format. Current IANA names
        # need no decoding; don't guess at escaped or malformed custom values.
        if "\\" in value or any(ord(character) < 32 or ord(character) == 127 for character in value):
            return None
        return sql_string_literal(value)
    if kind in {"UInt32", "DateTime"} and _EPOCH.fullmatch(value) and int(value) <= 4294967295:
        # Retain the parameter's type and, for DateTime, the server's timezone.
        return f"CAST({int(value)} AS {kind})"
    return None


def _quoted_end(sql, position, quote):
    position += 1
    while position < len(sql):
        if sql[position] == "\\":
            position += 2
        elif sql[position] == quote:
            if sql.startswith(quote * 2, position):
                position += 2
            else:
                return position + 1
        else:
            position += 1
    return len(sql)


def _protected_end(sql, match):
    if match.group("quote"):
        return _quoted_end(sql, match.start(), match.group("quote"))
    if match.group("line"):
        end = sql.find("\n", match.end())
        return len(sql) if end < 0 else end
    if match.group("heredoc"):
        end = sql.find(match.group(), match.end())
        return len(sql) if end < 0 else end + len(match.group())
    depth = 1
    for marker in _COMMENT.finditer(sql, match.end()):
        depth += 1 if marker.group() == "/*" else -1
        if depth == 0:
            return marker.end()
    return len(sql)


def render_sql(sql: str, parameters: dict[str, list[str]]) -> tuple[str, list[str]]:
    """Expand supported active placeholders; flag any that cannot be rendered.

    SQL literals, identifiers and comments remain verbatim, including malformed
    unterminated regions. Unknown types/values retain their original placeholder.
    The returned SQL is a diagnostic equivalent, not a replacement HTTP body.
    """
    parts, unresolved = [], set()
    position = 0
    while match := _TOKEN.search(sql, position):
        parts.append(sql[position:match.start()])
        if match.group("parameter"):
            parameter = _PARAMETER.fullmatch(match.group())
            replacement = None if parameter is None else _parameter(
                parameter.group(1), parameter.group(2).strip(), parameters)
            if replacement is None:
                unresolved.add(match.group())
            parts.append(match.group() if replacement is None else replacement)
            position = match.end()
        else:
            position = _protected_end(sql, match)
            parts.append(sql[match.start():position])
    parts.append(sql[position:])
    return "".join(parts), sorted(unresolved)
