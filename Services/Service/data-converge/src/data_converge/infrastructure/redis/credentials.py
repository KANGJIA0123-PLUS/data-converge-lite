"""Resolve the platform Redis PMS document using the existing isolated XMS helper."""

import base64
import hashlib
import ipaddress
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

from data_converge.sql_execution.credentials import (
    MAX_SECRET_BYTES, _invalid_constant, _unique_pairs, run_decryptor,
)


@dataclass(frozen=True)
class SentinelCredentials:
    sentinels: tuple[tuple[str, int], ...]
    password: str = field(repr=False)


def _address(value):
    value = value.strip()
    if value.startswith('['):
        match = re.fullmatch(r'\[([^\]]+)\]:([0-9]+)', value)
        if not match:
            raise ValueError
        host = str(ipaddress.IPv6Address(match[1]))
        port = int(match[2])
    else:
        match = re.fullmatch(r'([A-Za-z0-9_.-]+):([0-9]+)', value)
        if not match or len(match[1]) > 253:
            raise ValueError
        host, port = match[1].lower(), int(match[2])
    if not 1 <= port <= 65535:
        raise ValueError
    return host, port


def parse_sentinel_credentials(plaintext, username, paas_name=None):
    """Select one PMS service/account, never assume that paasconfig[0] is Redis."""
    try:
        if not isinstance(plaintext, str) or not 0 < len(plaintext.encode()) <= MAX_SECRET_BYTES:
            raise ValueError
        document = json.loads(base64.b64decode(plaintext, validate=True).decode('utf-8'),
                              object_pairs_hook=_unique_pairs, parse_constant=_invalid_constant)
        items = document['paasconfig']
        if not isinstance(items, list) or any(not isinstance(item, dict) for item in items):
            raise ValueError
        if paas_name is not None:
            matches = [item for item in items if item.get('name') == paas_name]
        else:
            matches = [item for item in items if 'IPS' in item and isinstance(item.get('info'), list)
                       and any(isinstance(account, dict) and account.get('user') == username
                               for account in item['info'])]
        if len(matches) != 1:
            raise ValueError
        item = matches[0]
        accounts = item['info']
        if not isinstance(accounts, list) or any(not isinstance(account, dict) for account in accounts):
            raise ValueError
        accounts = [account for account in accounts if account.get('user') == username]
        if len(accounts) != 1:
            raise ValueError
        password = accounts[0]['password']
        if not isinstance(password, str) or not password or '\0' in password:
            raise ValueError
        ips = item['IPS']
        if not isinstance(ips, str) or not 0 < len(ips) <= 4096:
            raise ValueError
        parts = ips.split(',')
        if not 1 <= len(parts) <= 8:
            raise ValueError
        endpoints = tuple(_address(part) for part in parts)
        if len(set(endpoints)) != len(endpoints):
            raise ValueError
        return SentinelCredentials(endpoints, password)
    except (ValueError, TypeError, KeyError, RecursionError):
        raise ValueError('DWD_REDIS_PMS_INVALID') from None


def load_sentinel_credentials(connection):
    """Decrypt once at startup; no secrets in YAML, process arguments or diagnostics."""
    try:
        with Path(connection['password_file']).open('rb') as stream:
            ciphertext = stream.read(MAX_SECRET_BYTES + 1)
        plaintext = run_decryptor([sys.executable, '-m', 'data_converge.sql_execution.kms_worker'],
                                 ciphertext, connection['kms_timeout_seconds'])
        return parse_sentinel_credentials(plaintext, connection['username'], connection.get('paas_name'))
    except Exception:
        raise ValueError('DWD_REDIS_CREDENTIAL_FAILED') from None


def sentinel_identity(connection, credentials):
    """Bind the durable ledger to its logical source without hashing password material."""
    identity = {'sentinels': sorted(credentials.sentinels), 'service_name': connection['service_name'],
                'db': connection['db'], 'username': connection['username']}
    return hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
