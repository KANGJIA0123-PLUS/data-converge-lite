"""Read only the leading INSERT target; never search comments or query bodies."""

import re


_IDENTIFIER = re.compile(
    r'''[A-Za-z_][0-9A-Za-z_]*|`(?:[^`\\]|\\.|``)+`|"(?:[^"\\]|\\.|"")+"''',
    re.DOTALL,
)
_COMMENT_BOUNDARY = re.compile(r"/\*|\*/")


def _skip_trivia(sql: str, position: int) -> int:
    """Skip whitespace and ClickHouse comments, including nested block comments."""
    while position < len(sql):
        if sql[position].isspace():
            position += 1
        elif sql.startswith(("--", "//", "#!", "# "), position):
            end = sql.find("\n", position)
            position = len(sql) if end < 0 else end + 1
        elif sql.startswith("/*", position):
            depth = 1
            for marker in _COMMENT_BOUNDARY.finditer(sql, position + 2):
                depth += 1 if marker.group() == "/*" else -1
                if depth == 0:
                    position = marker.end()
                    break
            else:
                raise ValueError("Unclosed comment before INSERT target")
        else:
            break
    return position


def _next_token(sql: str, position: int) -> tuple[str, int]:
    position = _skip_trivia(sql, position)
    match = _IDENTIFIER.match(sql, position)
    if match is not None:
        return match.group(), match.end()
    return sql[position:position + 1], position + 1


def cleanup_table_from_sql(sql: str) -> str:
    """Resolve INSERT INTO [TABLE] [database.]table to its corresponding _local table.

    Preserve identifier quoting and explicit database names. Unsupported targets
    (such as table functions or parameters) must fail before cleanup is submitted.
    This reads the INSERT header only, not the complete SQL grammar.
    """
    position = 0
    for keyword in ("INSERT", "INTO"):
        token, position = _next_token(sql, position)
        if token.upper() != keyword:
            raise ValueError("Expected leading INSERT INTO statement")
    token, position = _next_token(sql, position)
    if token.upper() == "TABLE":
        token, position = _next_token(sql, position)
    if token.upper() == "FUNCTION" or not _IDENTIFIER.fullmatch(token):
        raise ValueError("Expected a literal INSERT target table")
    parts = [token]
    token, position = _next_token(sql, position)
    if token == ".":
        token, position = _next_token(sql, position)
        if not _IDENTIFIER.fullmatch(token):
            raise ValueError("Expected a table after the database qualifier")
        parts.append(token)
        token, position = _next_token(sql, position)
    if token != "(" and token.upper() not in {"SELECT", "WITH", "VALUES", "FORMAT", "SETTINGS"}:
        raise ValueError("Unsupported INSERT target or missing query body")
    table = parts[-1]
    parts[-1] = table[:-1] + "_local" + table[-1] if table[0] in '`"' else table + "_local"
    return ".".join(parts)
