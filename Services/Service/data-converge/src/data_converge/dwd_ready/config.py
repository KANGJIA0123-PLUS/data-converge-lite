"""Offline validation; importing this module never imports Redis or reads secrets."""

from __future__ import annotations

import binascii
import hashlib
import ipaddress
import math
import os
import re
from pathlib import Path

_ENV_NAME = r"[A-Za-z_][A-Za-z0-9_]*"


def _invalid(code="DWD_READY_CONFIG_INVALID"):
    raise ValueError(code) from None


def _expand(value):
    if not isinstance(value, str):
        return value

    def replace(match):
        expanded = os.environ.get(match.group(1))
        if not expanded:
            _invalid("DWD_READY_ENV_MISSING")
        return expanded

    expanded = re.sub(r"\$\{(" + _ENV_NAME + r")\}", replace, value)
    if "$" in expanded:
        _invalid("DWD_READY_ENV_INVALID")
    return expanded


def _text(value):
    return isinstance(value, str) and bool(value) and not any(c.isspace() or ord(c) < 32 for c in value)


def _number(value, upper=None):
    if type(value) not in (int, float) or not math.isfinite(value) or value <= 0 or (upper is not None and value > upper):
        _invalid()
    return value


def _tag(key):
    start = key.find("{")
    end = key.find("}", start + 1) if start >= 0 else -1
    return key[start + 1:end] if end > start + 1 else key


def _slot(key):
    return binascii.crc_hqx(_tag(key).encode("utf-8"), 0) % 16384


def load_ready_config(raw: dict, config_directory: Path) -> dict:
    """Read the complete app config, without resolving the password environment."""
    if not isinstance(raw, dict):
        _invalid()
    section = raw.get("dwd_ready", {})
    allowed = {"enabled", "redis", "queue_key", "block_seconds", "batch_size", "wait_warning_seconds"}
    if not isinstance(section, dict) or set(section) - allowed:
        _invalid()
    enabled = section.get("enabled", False)
    if type(enabled) is not bool:
        _invalid()
    queue = section.get("queue_key", "modelverse:dwd:ready")
    if not _text(queue) or len(queue) > 1024:
        _invalid()
    tag = _tag(queue)
    processing = "{" + tag + "}:data-converge:processing"
    try:
        if tag != queue:
            # Several source queues may share one cluster tag. Keep their pending
            # messages separate while preserving the default untagged queue key.
            processing += ":" + hashlib.sha256(queue.encode("utf-8")).hexdigest()
        if processing == queue or _slot(queue) != _slot(processing):
            _invalid("DWD_READY_QUEUE_SLOT_INVALID")
    except UnicodeError:
        _invalid()
    block = _number(section.get("block_seconds", 1), 2)
    batch = section.get("batch_size", 100)
    if type(batch) is not int or not 1 <= batch <= 1000:
        _invalid()
    warning = _number(section.get("wait_warning_seconds", 60))
    config = dict(enabled=enabled, redis={}, queue_key=queue, processing_key=processing,
                  block_seconds=block, batch_size=batch, wait_warning_seconds=warning)
    if not enabled:
        return config
    connection = section.get("redis")
    common = {"mode", "db", "username", "ssl", "ssl_verify", "socket_timeout", "socket_connect_timeout"}
    sentinel_fields = {"service_name", "password_file", "paas_name", "root_key_file", "work_key_file",
                       "kms_timeout_seconds"}
    mode = connection.get("mode") if isinstance(connection, dict) else None
    fields = common | (sentinel_fields if mode == "sentinel" else {"host", "port", "password_env"})
    if not isinstance(connection, dict) or set(connection) - fields:
        _invalid()
    db = connection.get("db", 0)
    if mode not in ("standalone", "cluster", "sentinel") or type(db) is not int or db < 0:
        _invalid()
    if mode == "cluster" and db != 0:
        _invalid("DWD_READY_CLUSTER_REQUIRES_DB_ZERO")
    username = connection.get("username")
    if username is not None and not _text(username):
        _invalid()
    secure, verify = connection.get("ssl", True), connection.get("ssl_verify", True)
    if type(secure) is not bool or type(verify) is not bool:
        _invalid()
    socket_timeout = _number(connection.get("socket_timeout", 3), 5)
    connect_timeout = _number(connection.get("socket_connect_timeout", 2), 5)
    if socket_timeout <= block:
        _invalid("DWD_READY_SOCKET_TIMEOUT_TOO_SHORT")
    result = dict(mode=mode, db=db, username=username, ssl=secure, ssl_verify=verify,
                  socket_timeout=socket_timeout, socket_connect_timeout=connect_timeout)
    if mode == "sentinel":
        service = connection.get("service_name")
        paas_name = connection.get("paas_name")
        if not _text(service) or not _text(username) or (paas_name is not None and not _text(paas_name)):
            _invalid()
        result.update(service_name=service, paas_name=paas_name,
                      kms_timeout_seconds=_number(connection.get("kms_timeout_seconds", 30), 300))
        for key in ("password_file", "root_key_file", "work_key_file"):
            value = _expand(connection.get(key))
            if not isinstance(value, str) or not value.strip() or any(ord(c) < 32 for c in value):
                _invalid()
            path = Path(value)
            result[key] = str(path if path.is_absolute() else (config_directory / path).resolve())
    else:
        host = _expand(connection.get("host"))
        if not _text(host):
            _invalid()
        if ":" in host:
            try:
                host = str(ipaddress.IPv6Address(host.strip("[]")))
            except ValueError:
                _invalid()
        elif not re.fullmatch(r"[A-Za-z0-9_.-]+", host):
            _invalid()
        port = _expand(connection.get("port"))
        if isinstance(port, str) and re.fullmatch(r"[0-9]+", port):
            port = int(port)
        if type(port) is not int or not 1 <= port <= 65535:
            _invalid()
        password_env = connection.get("password_env")
        if not isinstance(password_env, str) or not re.fullmatch(_ENV_NAME, password_env):
            _invalid()
        result.update(host=host, port=port, password_env=password_env)
    config["redis"] = result
    return config
