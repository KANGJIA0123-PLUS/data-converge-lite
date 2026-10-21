"""Default failure diagnostics, separate from public errors and persisted evidence."""

from __future__ import annotations

import base64
import http.client
import json
import logging
import re
import traceback
import urllib.parse

from data_converge.errors import safe_error_details

from .render import render_sql, sql_string_literal


logger = logging.getLogger(__name__)
MAX_DEBUG_BODY_BYTES = 65536
_PARAMETERS = frozenset({
    "database", "query_id", "default_format", "wait_end_of_query", "max_execution_time",
    "param_start_time", "param_end_time", "param_system_timezone",
})
_SECRET_ASSIGNMENT = re.compile(
    r"""(?i)(\b(?:password|passwd|pwd|secret|token|access_token|api_key|authorization|"""
    r"""proxy-authorization|x-clickhouse-key)\b["']?\s*[:=]\s*)"""
    r"""(?:"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'|[^\s,;&}\]]+)""")
_AUTH_TOKEN = re.compile(r"(?i)\b(?:Basic|Bearer)\s+[A-Za-z0-9._~+/-]+=*")
# Bound scheme scanning so repeated punctuation in remote text stays linear.
_URL_USERINFO = re.compile(r"(?i)(\b[a-z][a-z0-9+.-]{0,31}://)[^/\s@]+@")


def enabled(config):
    """Require an explicit boolean opt-in and an enabled application logger."""
    settings = config.get("logging", {})
    return (isinstance(settings, dict) and settings.get("debug_enabled") is True
            and logger.isEnabledFor(logging.DEBUG))


def _known_secrets(config):
    credentials = config.get("clickhouse", {})
    password = credentials.get("password")
    if not isinstance(password, str) or not password:
        return ()
    username = credentials.get("username", "")
    token = base64.b64encode(f"{username}:{password}".encode()).decode("ascii")
    secrets = {password, urllib.parse.quote(password, safe=""), urllib.parse.quote_plus(password),
               json.dumps(password, ensure_ascii=True)[1:-1],
               json.dumps(password, ensure_ascii=False)[1:-1],
               sql_string_literal(password)[1:-1], token}
    return sorted(secrets, key=len, reverse=True)


def _redact(config, text):
    for secret in _known_secrets(config):
        text = text.replace(secret, "[REDACTED]")
    text = _URL_USERINFO.sub(r"\1[REDACTED]@", text)
    text = _AUTH_TOKEN.sub("[REDACTED_AUTH]", text)
    return _SECRET_ASSIGNMENT.sub(r"\1[REDACTED]", text)


def log_request(config, request):
    """Render a failed POST using its approved wire parameters, never headers."""
    if not logger.isEnabledFor(logging.ERROR):
        return
    parameters = urllib.parse.parse_qs(urllib.parse.urlsplit(request.full_url).query,
                                       keep_blank_values=True)
    parameters = {key: values for key, values in parameters.items() if key in _PARAMETERS}
    query_id = parameters.get("query_id", [""])[0]
    sql = request.data.decode("utf-8", errors="replace")
    sql, unresolved = render_sql(sql, parameters)
    message = (f"SQL_REQUEST query_id={query_id} sql_rendered={str(not unresolved).lower()} "
               f"unresolved_parameters={json.dumps(unresolved, ensure_ascii=False)} "
               f"parameters={json.dumps(parameters, ensure_ascii=False, sort_keys=True)}\nsql={sql}")
    logger.error("%s", _redact(config, message))


def log_error(config, query_id, error, *, body=None):
    """Log useful failure details by default; keep Python stacks opt-in."""
    if not logger.isEnabledFor(logging.ERROR):
        return
    response, truncated = _response_text(config, body)
    details = json.dumps(safe_error_details(error), sort_keys=True)
    message = (f"SQL_ERROR query_id={query_id} error_type={type(error).__name__} "
               f"body_truncated={truncated} diagnostic={details}\nmessage={error}")
    if response is not None:
        message += f"\nresponse_body={response}"
    logger.error("%s", _redact(config, message))
    if enabled(config):
        stack = "".join(traceback.format_exception(type(error), error, error.__traceback__, chain=False))
        logger.debug("%s", _redact(config, f"SQL_TRACEBACK query_id={query_id}\ntraceback={stack}"))


def _response_text(config, body):
    if body is None:
        return None, False
    truncated = len(body) > MAX_DEBUG_BODY_BYTES
    response = body[:MAX_DEBUG_BODY_BYTES]
    if isinstance(response, bytes):
        response = response.decode("utf-8", errors="replace")
    response = _redact(config, response)
    if truncated:
        # A credential may cross the cutoff, leaving only an unmatchable prefix.
        # Mask complete secrets first, then omit a conservative overlap at the
        # boundary; do not read extra response bytes or extend the deadline.
        overlap = max((len(secret) for secret in _known_secrets(config)), default=1) - 1
        if overlap:
            response = response[:-overlap] + "[REDACTED_TRUNCATED_TAIL]"
    return response, truncated


def log_http_error(config, query_id, error):
    """Read bounded error evidence while the caller owns the active deadline."""
    if not logger.isEnabledFor(logging.ERROR):
        return
    body = None
    try:
        body = error.read(MAX_DEBUG_BODY_BYTES + 1)
    except (OSError, http.client.HTTPException, ValueError) as read_error:
        message = (f"SQL_ERROR query_id={query_id} body_read_failed={type(read_error).__name__} "
                   f"message={read_error}")
        logger.error("%s", _redact(config, message))
    log_error(config, query_id, error, body=body)
