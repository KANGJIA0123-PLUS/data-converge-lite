"""Validate ClickHouse settings independently of network execution."""

from __future__ import annotations

import os
import re
import urllib.parse
from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml

from .credentials import load_password, validate_provider


def _expand(value):
    if not isinstance(value, str):
        return value

    def replace(match):
        result = os.environ.get(match.group(1))
        if not result:
            raise ValueError("CLICKHOUSE_ENV_MISSING") from None
        return result

    value = re.sub(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}", replace, value)
    if "$" in value:
        raise ValueError("CLICKHOUSE_ENV_INVALID") from None
    return value


def connection_parameters(clickhouse):
    """Validate the authority before assembling a URL with private headers."""
    try:
        host = clickhouse["host"]
        port = clickhouse["port"]
        if not isinstance(host, str) or not host or any(c.isspace() or ord(c) < 32 for c in host):
            raise ValueError
        if type(port) is not int:
            if not isinstance(port, str) or not re.fullmatch(r"[0-9]+", port):
                raise ValueError
            port = int(port)
        if not 1 <= port <= 65535 or any(char in host for char in "?#\\%"):
            raise ValueError
        has_scheme = "://" in host
        parsed = urllib.parse.urlsplit(host if has_scheme else "//" + host)
        secure = clickhouse.get("secure", parsed.scheme == "https" if has_scheme else False)
        verify = clickhouse.get("verify", True)
        if not isinstance(secure, bool) or not isinstance(verify, bool):
            raise ValueError
        scheme = "https" if secure else "http"
        if has_scheme and parsed.scheme != scheme:
            raise ValueError
        if (not parsed.hostname or parsed.username is not None or parsed.password is not None
                or parsed.path not in ("", "/") or parsed.query or parsed.fragment
                or parsed.port not in (None, port)):
            raise ValueError
        hostname = parsed.hostname
        if not re.fullmatch(r"[A-Za-z0-9_.:-]+", hostname):
            raise ValueError
        authority = f"[{hostname}]" if ":" in hostname else hostname
        return f"{scheme}://{authority}:{port}/", port, secure, verify
    except (ValueError, KeyError, TypeError):
        raise ValueError("CLICKHOUSE_CONNECTION_INVALID") from None


def connection_origin(clickhouse: dict[str, Any]) -> str:
    """Return the validated endpoint used for execution and deployment binding."""
    return connection_parameters(clickhouse)[0]


def load_config(config_path: Path, *, resolve_password: bool = True) -> dict[str, Any]:
    """Read and validate a configuration, optionally resolving its credential."""
    config_path = Path(config_path)
    try:
        with config_path.open("r", encoding="utf-8") as stream:
            config = yaml.safe_load(stream)
    except (OSError, UnicodeError, yaml.YAMLError):
        raise ValueError("CONFIG_UNREADABLE") from None
    return validate_config(config, config_path, resolve_password=resolve_password)


def validate_config(config: dict[str, Any], config_path: Path, *,
                    resolve_password: bool = True) -> dict[str, Any]:
    """Validate an independent snapshot without rereading the configuration file."""
    if not isinstance(config, dict) or not isinstance(config.get("clickhouse"), dict):
        raise ValueError("CLICKHOUSE_CONFIG_MISSING") from None
    config = deepcopy(config)
    clickhouse = config["clickhouse"]
    for field in ("host", "port"):
        clickhouse[field] = _expand(clickhouse.get(field))
    _, clickhouse["port"], clickhouse["secure"], clickhouse["verify"] = connection_parameters(clickhouse)
    for field in ("database", "username"):
        value = clickhouse.get(field)
        if not isinstance(value, str) or not value.strip() or any(ord(c) < 32 for c in value):
            raise ValueError("CLICKHOUSE_IDENTITY_INVALID") from None
    if ":" in clickhouse["username"]:
        raise ValueError("CLICKHOUSE_IDENTITY_INVALID") from None
    validate_provider(clickhouse, Path(config_path).parent)
    return resolve_credentials(config) if resolve_password else config


def resolve_credentials(config: dict[str, Any]) -> dict[str, Any]:
    """Resolve a validated snapshot's password without modifying the snapshot."""
    resolved = deepcopy(config)
    clickhouse = resolved["clickhouse"]
    if "password" not in clickhouse:
        clickhouse["password"] = load_password(clickhouse)
    return resolved
