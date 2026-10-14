"""Per-connection company Redis challenge authentication for redis-py 5 and 6."""

from __future__ import annotations

import hmac
import re

_HEX = re.compile(r"[0-9a-fA-F]+")
_MAX_NONCE_CHARS = 256
_MAX_TOKEN_CHARS = 16_384


def _cipher_hex(value):
    if (not isinstance(value, str) or not 0 < len(value) <= _MAX_TOKEN_CHARS
            or len(value) % 2 or not _HEX.fullmatch(value)):
        raise ValueError
    return value


def _client_nonce():
    # Keep the SDK's length semantics: the supplied reference uses this exact
    # API, and does not specify whether 32 means bytes or hexadecimal digits.
    from psf.security_api.random_security import gen_random_hex_string

    value = gen_random_hex_string(32)
    if (not isinstance(value, str) or not 0 < len(value) <= _MAX_NONCE_CHARS
            or not _HEX.fullmatch(value)):
        raise ValueError
    return value


def _read_challenge(connection):
    from redis.exceptions import ResponseError

    try:
        connection.read_response()
    except ResponseError as error:
        return str(error)
    # The company challenge is an error reply, never a success/bulk reply.
    raise ValueError


def _authenticate(connection, codec):
    password = connection.password
    if not isinstance(password, str) or not password:
        raise ValueError
    client_nonce = _client_nonce()
    connection.send_command("AUTH", "<" + client_nonce, check_health=False)
    challenge = _read_challenge(connection)

    # redis-py's RESP parser normally removes "ERR ". Some supported parsers
    # retain it. Strip exactly one complete prefix; never discard a cipher digit.
    if len(challenge) > _MAX_TOKEN_CHARS + len("ERR >"):
        raise ValueError
    if challenge.startswith("ERR >"):
        challenge = challenge[len("ERR >"):]
    elif challenge.startswith(">"):
        challenge = challenge[1:]
    else:
        raise ValueError
    plaintext = codec.decrypt(_cipher_hex(challenge))
    if not isinstance(plaintext, str) or len(plaintext) > 2 * _MAX_NONCE_CHARS + 1:
        raise ValueError
    pieces = plaintext.split("@")
    if len(pieces) != 2:
        raise ValueError
    server_nonce, returned_nonce = pieces
    if (not 0 < len(server_nonce) <= _MAX_NONCE_CHARS or not server_nonce.isascii()
            or not server_nonce.isprintable() or not returned_nonce.isascii()
            or not hmac.compare_digest(returned_nonce, client_nonce)):
        raise ValueError
    response = _cipher_hex(codec.encrypt(client_nonce + "@" + server_nonce + "@" + password))
    # PMS username selects a password account, and is not a Redis AUTH argument.
    connection.send_command("AUTH", ">" + response, check_health=False)
    if connection.read_response() not in (b"OK", "OK"):
        raise ValueError


def make_connect_callback(codec, *, select_db: bool):
    """Build a redis_connect_func; each new socket performs a fresh challenge.

    The codec provides encrypt(str) -> hex and decrypt(hex) -> str. Sentinel
    connections must use select_db=False; only data connections select a DB.
    SDK and server errors are replaced with fixed, credential-free diagnostics.
    """
    from redis.exceptions import AuthenticationError
    from redis.exceptions import ConnectionError as RedisConnectionError
    from redis.exceptions import TimeoutError as RedisTimeoutError

    def connect(connection):
        try:
            connection._parser.on_connect(connection)
        except Exception:
            raise RedisConnectionError("DWD_READY_REDIS_CONNECT_FAILED") from None
        try:
            _authenticate(connection, codec)
        except AuthenticationError:
            raise AuthenticationError("DWD_READY_REDIS_AUTH_FAILED") from None
        except (RedisConnectionError, RedisTimeoutError, OSError):
            raise RedisConnectionError("DWD_READY_REDIS_CONNECT_FAILED") from None
        except Exception:
            raise AuthenticationError("DWD_READY_REDIS_AUTH_FAILED") from None

        try:
            if connection.client_name:
                connection.send_command("CLIENT", "SETNAME", connection.client_name, check_health=False)
                if connection.read_response() not in (b"OK", "OK"):
                    raise ValueError
            if select_db and connection.db:
                connection.send_command("SELECT", connection.db, check_health=False)
                if connection.read_response() not in (b"OK", "OK"):
                    raise ValueError
        except Exception:
            raise RedisConnectionError("DWD_READY_REDIS_SESSION_FAILED") from None

    return connect
