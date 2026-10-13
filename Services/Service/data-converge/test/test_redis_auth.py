"""Company AUTH protocol, including redis-py parsing on a real loopback socket."""

import queue
import socket
import subprocess
import sys
import threading
import traceback
import types
from collections import deque
from pathlib import Path

import pytest
from redis import AuthenticationError, ConnectionError, ResponseError
from redis.connection import Connection


PASSWORD = "private-password-with-@-separator"
CLIENT_NONCE = "abcdef0123456789" * 2
SERVER_NONCE = "fedcba9876543210" * 2


def test_offline_main_and_config_import_without_redis_or_crypto_sdks(tmp_path):
    source_dir = Path(__file__).resolve().parents[1] / "src"
    script = """
import builtins
import sys
from pathlib import Path

sys.path.insert(0, sys.argv[1])
original_import = builtins.__import__
blocked = {'redis', 'psf', 'Crypto'}
def guarded_import(name, *args, **kwargs):
    if name.split('.')[0] in blocked:
        raise AssertionError('Offline loading imported an online dependency: ' + name)
    return original_import(name, *args, **kwargs)
builtins.__import__ = guarded_import

from data_converge import main
from data_converge.dwd_ready.config import load_ready_config
assert callable(main.load_definition)
assert load_ready_config({}, Path(sys.argv[2]))['enabled'] is False
ready = load_ready_config({'dwd_ready': {'enabled': True, 'redis': {
    'mode': 'sentinel', 'service_name': 'redis-cluster', 'db': 5,
    'username': 'ossdbuser', 'password_file': 'unread-password',
    'root_key_file': 'unread-root', 'work_key_file': 'unread-work', 'ssl': False,
}}}, Path(sys.argv[2]))
assert ready['redis']['mode'] == 'sentinel' and ready['redis']['db'] == 5
assert not (blocked & set(sys.modules))
"""
    result = subprocess.run([sys.executable, "-I", "-c", script, str(source_dir), str(tmp_path)],
                            capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr


class HexCodec:
    """An intentionally transparent synthetic codec; no production keys involved."""

    def encrypt(self, value):
        return value.encode().hex()

    def decrypt(self, value):
        return bytes.fromhex(value).decode()


@pytest.fixture
def nonce_sdk(monkeypatch):
    calls = []
    values = deque([CLIENT_NONCE])
    module = types.ModuleType("psf.security_api.random_security")

    def generate(length):
        calls.append(length)
        return values.popleft()

    module.gen_random_hex_string = generate
    monkeypatch.setitem(sys.modules, "psf.security_api.random_security", module)
    return calls, values


class ScriptedConnection:
    def __init__(self, replies, *, password=PASSWORD, db=5, client_name=None):
        self.password = password
        self.username = "pms-account-must-not-be-sent"
        self.db = db
        self.client_name = client_name
        self.replies = deque(replies)
        self.events = []
        self._parser = types.SimpleNamespace(on_connect=self.initialize_parser)

    def initialize_parser(self, connection):
        assert connection is self
        self.events.append("parser")

    def send_command(self, *args, **kwargs):
        assert self.events and self.events[0] == "parser"
        # Even when callers enable health checks, authentication cannot send PING.
        assert kwargs == {"check_health": False}
        self.events.append(args)

    def read_response(self):
        reply = self.replies.popleft()
        if isinstance(reply, Exception):
            raise reply
        return reply


def challenge(value=None, prefix=">"):
    plain = value if value is not None else SERVER_NONCE + "@" + CLIENT_NONCE
    return ResponseError(prefix + plain.encode().hex())


def invoke(connection, *, codec=None, select_db=True):
    from data_converge.dwd_ready.redis_auth import make_connect_callback

    make_connect_callback(codec or HexCodec(), select_db=select_db)(connection)


@pytest.mark.parametrize("prefix", [">", "ERR >"])
@pytest.mark.parametrize("ok", [b"OK", "OK"])
def test_auth_preserves_first_cipher_digit_and_selects_after_auth(nonce_sdk, prefix, ok):
    connection = ScriptedConnection([challenge(prefix=prefix), ok, ok, ok], client_name="worker")
    invoke(connection)
    commands = connection.events
    assert commands[0] == "parser"
    assert commands[1] == ("AUTH", "<" + CLIENT_NONCE)
    assert commands[2][0] == "AUTH" and len(commands[2]) == 2
    assert HexCodec().decrypt(commands[2][1][1:]) == CLIENT_NONCE + "@" + SERVER_NONCE + "@" + PASSWORD
    assert commands[3:] == [("CLIENT", "SETNAME", "worker"), ("SELECT", 5)]
    assert nonce_sdk[0] == [32]


@pytest.mark.parametrize("select_db, db", [(False, 5), (False, 0), (True, 0)])
def test_sentinel_and_default_database_never_select(nonce_sdk, select_db, db):
    connection = ScriptedConnection([challenge(), b"OK"], db=db)
    invoke(connection, select_db=select_db)
    assert len(connection.events) == 3


@pytest.mark.parametrize("reply", [b"OK", b">aabb", None, ResponseError(""), ResponseError(">"),
    ResponseError("ERR >"), ResponseError("ERR aabb"), ResponseError("ERR>aa"),
    ResponseError("WRONGPASS >aabb"), ResponseError(">>aabb"), ResponseError(">abc"),
    ResponseError(">aaz1"), ResponseError(">aa bb"), ResponseError(">aabb\n"),
    pytest.param(ResponseError(">" + "ab" * 100000), id="oversized")])
def test_invalid_challenge_does_not_send_second_auth(nonce_sdk, reply):
    connection = ScriptedConnection([reply])
    with pytest.raises(AuthenticationError):
        invoke(connection)
    assert len(connection.events) == 2


@pytest.mark.parametrize("plain", [SERVER_NONCE + "@wrong-nonce", SERVER_NONCE, "@" + CLIENT_NONCE,
    SERVER_NONCE + "@" + CLIENT_NONCE + "@extra", SERVER_NONCE + "@",
    pytest.param("x" * 100000 + "@" + CLIENT_NONCE, id="oversized")])
def test_invalid_peer_nonce_stops_before_password_response(nonce_sdk, plain):
    connection = ScriptedConnection([challenge(plain)])
    with pytest.raises(AuthenticationError):
        invoke(connection)
    assert len(connection.events) == 2


@pytest.mark.parametrize("password", [None, "", b"private-password"])
def test_missing_or_nontext_password_fails_closed(nonce_sdk, password):
    connection = ScriptedConnection([], password=password)
    with pytest.raises(AuthenticationError):
        invoke(connection)
    assert connection.events == ["parser"]


@pytest.mark.parametrize("nonce", [None, "", "not-hex", pytest.param("ab" * 100000, id="oversized")])
def test_invalid_sdk_nonce_fails_before_first_auth(nonce_sdk, nonce):
    nonce_sdk[1].clear()
    nonce_sdk[1].append(nonce)
    connection = ScriptedConnection([])
    with pytest.raises(AuthenticationError):
        invoke(connection)
    assert connection.events == ["parser"]


@pytest.mark.parametrize("reply", [b"NO", None, ResponseError(PASSWORD), AuthenticationError(PASSWORD)])
def test_auth_rejection_never_selects_or_falls_back_to_plaintext(nonce_sdk, reply):
    connection = ScriptedConnection([challenge(), reply])
    with pytest.raises(AuthenticationError) as caught:
        invoke(connection)
    assert len(connection.events) == 3
    assert PASSWORD not in "".join(traceback.format_exception(caught.value))
    assert caught.value.__suppress_context__


@pytest.mark.parametrize("operation", ["decrypt", "encrypt"])
def test_codec_errors_do_not_expose_plaintext_or_exception_context(nonce_sdk, operation):
    codec = HexCodec()

    def fail(value):
        raise ValueError(PASSWORD + value)

    setattr(codec, operation, fail)
    connection = ScriptedConnection([challenge()])
    with pytest.raises(AuthenticationError) as caught:
        invoke(connection, codec=codec)
    rendered = "".join(traceback.format_exception(caught.value))
    assert PASSWORD not in rendered and CLIENT_NONCE not in rendered and SERVER_NONCE not in rendered
    assert caught.value.__suppress_context__


@pytest.mark.parametrize("cipher", [None, "", "not-hex", "a", pytest.param("ab" * 100000, id="oversized")])
def test_invalid_encrypted_response_is_not_sent(nonce_sdk, cipher):
    codec = HexCodec()
    codec.encrypt = lambda value: cipher
    connection = ScriptedConnection([challenge()])
    with pytest.raises(AuthenticationError):
        invoke(connection, codec=codec)
    assert len(connection.events) == 2


@pytest.mark.parametrize("client_name", [None, "worker"])
def test_post_auth_failures_are_safe_connection_errors(nonce_sdk, client_name):
    connection = ScriptedConnection([challenge(), b"OK", ResponseError(PASSWORD)], client_name=client_name)
    with pytest.raises(ConnectionError) as caught:
        invoke(connection)
    assert not isinstance(caught.value, AuthenticationError)
    assert PASSWORD not in "".join(traceback.format_exception(caught.value))
    assert caught.value.__suppress_context__


def test_io_failure_is_safe_and_does_not_retry_auth(nonce_sdk):
    connection = ScriptedConnection([ConnectionError(PASSWORD)])
    with pytest.raises(ConnectionError) as caught:
        invoke(connection)
    assert not isinstance(caught.value, AuthenticationError)
    assert PASSWORD not in "".join(traceback.format_exception(caught.value))
    assert len(connection.events) == 2


def read_command(stream):
    header = stream.readline()
    assert header.startswith(b"*")
    command = []
    for _ in range(int(header[1:])):
        length = stream.readline()
        assert length.startswith(b"$")
        command.append(stream.read(int(length[1:])))
        assert stream.read(2) == b"\r\n"
    return command


def serve_reauthenticated_connections(listener, nonces, prefix, select_db, observed, failures):
    try:
        for expected_nonce in nonces:
            peer, _ = listener.accept()
            with peer, peer.makefile("rb") as stream:
                peer.settimeout(3)
                first = read_command(stream)
                assert first == [b"AUTH", ("<" + expected_nonce).encode()]
                observed.put(first)
                token = (SERVER_NONCE + "@" + expected_nonce).encode().hex().encode()
                peer.sendall(prefix + token + b"\r\n")
                second = read_command(stream)
                assert len(second) == 2 and second[0] == b"AUTH" and second[1].startswith(b">")
                assert PASSWORD.encode() not in second[1]
                assert bytes.fromhex(second[1][1:].decode()).decode() == expected_nonce + "@" + SERVER_NONCE + "@" + PASSWORD
                observed.put(second)
                peer.sendall(b"+OK\r\n")
                if select_db:
                    assert read_command(stream) == [b"SELECT", b"5"]
                    peer.sendall(b"+OK\r\n")
                assert read_command(stream) == [b"PING"]
                peer.sendall(b"+PONG\r\n")
    except BaseException as error:
        failures.put(error)


@pytest.mark.parametrize("prefix", [b"-ERR >", b"->"])
@pytest.mark.parametrize("select_db", [False, True])
def test_real_resp_socket_reauthenticates_with_fresh_nonce_on_reconnect(nonce_sdk, prefix, select_db):
    from data_converge.dwd_ready.redis_auth import make_connect_callback

    next_nonce = "0123456789abcdef" * 2
    nonce_sdk[1].append(next_nonce)
    observed = queue.Queue()
    failures = queue.Queue()
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(2)
    listener.settimeout(3)
    thread = threading.Thread(
        target=serve_reauthenticated_connections,
        args=(listener, (CLIENT_NONCE, next_nonce), prefix, select_db, observed, failures),
        daemon=True,
    )
    thread.start()
    connection = Connection(host="127.0.0.1", port=listener.getsockname()[1], password=PASSWORD,
        username="pms-user", db=5, socket_timeout=2, socket_connect_timeout=2,
        health_check_interval=60, redis_connect_func=make_connect_callback(HexCodec(), select_db=select_db))
    try:
        for _ in range(2):
            connection.connect()
            connection.send_command("PING", check_health=False)
            assert connection.read_response() == b"PONG"
            connection.disconnect()
        thread.join(timeout=4)
        assert not thread.is_alive()
        if not failures.empty():
            raise failures.get()
        assert observed.qsize() == 4 and nonce_sdk[0] == [32, 32]
    finally:
        connection.disconnect()
        listener.close()
        thread.join(timeout=4)
