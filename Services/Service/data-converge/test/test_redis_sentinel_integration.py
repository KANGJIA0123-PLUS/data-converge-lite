"""Real redis-py Sentinel connections against small local RESP protocol peers.

Only company SDK boundaries are replaced. Shared list state represents the
post-replication state available on a promoted master; this is not a replication
test and does not require redis-server or company credentials.
"""

import base64
import json
import socket
import sys
import threading
import types
from contextlib import ExitStack

import pytest


PASSWORD = "synthetic-private-password"
QUEUE = b"modelverse:dwd:ready"
PROCESSING = b"{modelverse:dwd:ready}:data-converge:processing"
MESSAGE = b"2026-10-12T12:00:GPU_REALTIME"
DROP_CONNECTION = object()


class SyntheticCodec:
    def encrypt(self, value):
        return value.encode().hex()

    def decrypt(self, value):
        return bytes.fromhex(value).decode()


def resp(value):
    if value is None:
        return b"$-1\r\n"
    if isinstance(value, int):
        return b":" + str(value).encode() + b"\r\n"
    if isinstance(value, list):
        return b"*" + str(len(value)).encode() + b"\r\n" + b"".join(map(resp, value))
    return b"$" + str(len(value)).encode() + b"\r\n" + value + b"\r\n"


def read_command(stream):
    header = stream.readline()
    if not header:
        return None
    assert header.startswith(b"*")
    result = []
    for _ in range(int(header[1:])):
        length = stream.readline()
        assert length.startswith(b"$")
        result.append(stream.read(int(length[1:])))
        assert stream.read(2) == b"\r\n"
    return result


class RespPeer:
    def __init__(self, role, command_handler):
        self.role = role
        self.command_handler = command_handler
        self.commands = []
        self.nonces = []
        self.failures = []
        self.sessions = []
        self.clients = set()
        self.condition = threading.Condition()
        self.stopped = threading.Event()
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(8)
        self.listener.settimeout(0.1)
        self.address = self.listener.getsockname()
        self.acceptor = threading.Thread(target=self.accept, daemon=True)

    def __enter__(self):
        self.acceptor.start()
        return self

    def __exit__(self, *exc):
        self.stopped.set()
        self.listener.close()
        with self.condition:
            for client in tuple(self.clients):
                try:
                    client.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                client.close()
        self.acceptor.join(timeout=2)
        for session in self.sessions:
            session.join(timeout=2)

    def accept(self):
        while not self.stopped.is_set():
            try:
                client, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            with self.condition:
                self.clients.add(client)
            thread = threading.Thread(target=self.serve, args=(client,), daemon=True)
            self.sessions.append(thread)
            thread.start()

    def serve(self, client):
        authenticated = False
        selected = False
        client_nonce = None
        try:
            with client, client.makefile("rb") as stream:
                client.settimeout(5)
                while True:
                    command = read_command(stream)
                    if command is None:
                        return
                    self.commands.append(command)
                    if command[0] == b"AUTH":
                        assert len(command) == 2, "PMS username is not a Redis AUTH argument"
                        if command[1].startswith(b"<"):
                            assert client_nonce is None and not authenticated
                            client_nonce = command[1][1:].decode()
                            self.nonces.append(client_nonce)
                            challenge = ("fedcba9876543210@" + client_nonce).encode().hex().encode()
                            client.sendall(b"-ERR >" + challenge + b"\r\n")
                        else:
                            assert client_nonce is not None and not authenticated
                            assert command[1].startswith(b">")
                            assert PASSWORD.encode() not in command[1]
                            plain = bytes.fromhex(command[1][1:].decode()).decode()
                            assert plain == client_nonce + "@fedcba9876543210@" + PASSWORD
                            authenticated = True
                            client.sendall(b"+OK\r\n")
                    elif command[0] == b"SELECT":
                        assert self.role == "data" and authenticated and command == [b"SELECT", b"5"]
                        selected = True
                        client.sendall(b"+OK\r\n")
                    else:
                        assert authenticated and (self.role == "sentinel" or selected)
                        response = self.command_handler(command)
                        if response is DROP_CONNECTION:
                            return
                        client.sendall(response)
        except BaseException as error:
            if not self.stopped.is_set():
                self.failures.append(error)
        finally:
            with self.condition:
                self.clients.discard(client)
                self.condition.notify_all()

    def wait_closed(self):
        with self.condition:
            return self.condition.wait_for(lambda: not self.clients, timeout=2)


def patch_private_sdks(monkeypatch):
    from data_converge.dwd_ready import redis_source as source_module

    # Replace only private SDK boundaries; config, PMS selection and redis-py remain real.
    nonce_module = types.ModuleType("psf.security_api.random_security")
    nonce_calls = []

    def nonce(length):
        nonce_calls.append(length)
        return f"{len(nonce_calls):032x}"

    nonce_module.gen_random_hex_string = nonce
    monkeypatch.setitem(sys.modules, "psf.security_api.random_security", nonce_module)
    monkeypatch.setattr(source_module, "create_psm_codec", lambda root, work: SyntheticCodec())
    return nonce_calls


class FailoverScenario:
    def __init__(self, failure):
        self.failure = failure
        self.lists = {QUEUE: [MESSAGE], PROCESSING: []}
        self.state = {"old_master_demoted": False, "master": None, "discoveries": 0}

    def data_command(self, command, *, old_master):
        lists, state = self.lists, self.state
        if command[0] == b"LREM" and old_master and state["old_master_demoted"]:
            return b"-READONLY previous master demoted\r\n" if self.failure == "readonly" else DROP_CONNECTION
        if command[0] == b"LINDEX":
            assert command == [b"LINDEX", PROCESSING, b"-1"]
            return resp(lists[PROCESSING][-1] if lists[PROCESSING] else None)
        if command[0] in (b"RPOPLPUSH", b"BRPOPLPUSH"):
            assert command[1:3] == [QUEUE, PROCESSING]
            value = lists[QUEUE].pop() if lists[QUEUE] else None
            if value is not None:
                lists[PROCESSING].insert(0, value)
            return resp(value)
        if command[0] == b"LREM":
            assert command == [b"LREM", PROCESSING, b"1", MESSAGE]
            found = MESSAGE in lists[PROCESSING]
            if found:
                lists[PROCESSING].remove(MESSAGE)
            return resp(int(found))
        raise AssertionError("Unexpected data command")

    def sentinel_command(self, command):
        assert command == [b"SENTINEL", b"MASTERS"]
        self.state["discoveries"] += 1
        host, port = self.state["master"]
        return resp([[b"name", b"redis-cluster", b"ip", host.encode(), b"port", str(port).encode(),
                      b"flags", b"master", b"num-other-sentinels", b"0"]])


def create_sentinel_source(tmp_path, monkeypatch, address, account_config, selected_user):
    from data_converge.dwd_ready.config import load_ready_config
    from data_converge.dwd_ready import redis_credentials as credentials_module
    from data_converge.dwd_ready import redis_source as source_module

    host, port = address
    document = base64.b64encode(json.dumps({"paasconfig": [{
        "name": "synthetic-redis", "IPS": f"{host}:{port}", "info": [
            {"user": name, "password": PASSWORD if name == selected_user else "wrong-account-password"}
            for name in ("ossdbuser", "admin")
        ],
    }]}).encode()).decode()
    (tmp_path / "synthetic-pwd").write_bytes(b"synthetic-ciphertext")
    monkeypatch.setattr(credentials_module, "run_decryptor", lambda *_: document)
    raw = {"enabled": True, "queue_key": QUEUE.decode(),
        "block_seconds": 1, "redis": {"mode": "sentinel", "db": 5, "service_name": "redis-cluster",
            **account_config, "password_file": "synthetic-pwd", "paas_name": None,
            "root_key_file": "root.key", "work_key_file": "shared.key", "ssl": False,
            "ssl_verify": True, "socket_timeout": 2, "socket_connect_timeout": 2, "kms_timeout_seconds": 30}}
    config = load_ready_config({"dwd_ready": raw}, tmp_path)
    return source_module.create_source(config)


def assert_initial_delivery(source, old_master, sentinel, scenario):
    assert source.receive(block=True) == MESSAGE
    assert scenario.lists == {QUEUE: [], PROCESSING: [MESSAGE]}
    assert scenario.state["discoveries"] == 1
    assert [cmd[0] for cmd in old_master.commands[:2]] == [b"AUTH", b"AUTH"]
    assert old_master.commands[2] == [b"SELECT", b"5"]
    assert not any(cmd[0] == b"SELECT" for cmd in sentinel.commands)


def assert_failover_recovery(source, old_master, new_master, scenario):
    from data_converge.dwd_ready import redis_source as source_module

    scenario.state.update(old_master_demoted=True, master=new_master.address)
    with pytest.raises(source_module.RedisSignalError, match="DWD_READY_REDIS_ACK_FAILED"):
        source.ack(MESSAGE)
    # The failing write has no automatic retry; the durable entry stays.
    assert len([cmd for cmd in old_master.commands if cmd[0] == b"LREM"]) == 1
    assert new_master.commands == [] and scenario.lists[PROCESSING] == [MESSAGE]
    assert source.receive(block=False) == MESSAGE
    assert scenario.state["discoveries"] == 2
    assert new_master.commands[2:4] == [[b"SELECT", b"5"], [b"LINDEX", PROCESSING, b"-1"]]
    assert source.ack(MESSAGE) == 1
    assert source.receive(block=False) is None
    assert scenario.lists == {QUEUE: [], PROCESSING: []}


@pytest.mark.parametrize("failure", ["readonly", "disconnect"])
@pytest.mark.parametrize("account_config,selected_user", [
    ({}, "admin"), ({"username": "admin"}, "admin"), ({"username": "ossdbuser"}, "ossdbuser"),
])
def test_real_sentinel_recovers_processing_after_master_failover(
        tmp_path, monkeypatch, failure, account_config, selected_user):
    nonce_calls = patch_private_sdks(monkeypatch)
    scenario = FailoverScenario(failure)
    with ExitStack() as stack:
        old_master = stack.enter_context(RespPeer("data", lambda cmd: scenario.data_command(cmd, old_master=True)))
        new_master = stack.enter_context(RespPeer("data", lambda cmd: scenario.data_command(cmd, old_master=False)))
        sentinel = stack.enter_context(RespPeer("sentinel", scenario.sentinel_command))
        scenario.state["master"] = old_master.address
        source = create_sentinel_source(tmp_path, monkeypatch, sentinel.address, account_config, selected_user)
        try:
            assert_initial_delivery(source, old_master, sentinel, scenario)
            assert_failover_recovery(source, old_master, new_master, scenario)
            all_nonces = sentinel.nonces + old_master.nonces + new_master.nonces
            assert len(all_nonces) == len(set(all_nonces)) == 3
            assert nonce_calls == [32, 32, 32]
        finally:
            source.close()
        assert all(peer.wait_closed() for peer in (sentinel, old_master, new_master))
        for peer in (sentinel, old_master, new_master):
            assert not peer.failures, peer.failures
