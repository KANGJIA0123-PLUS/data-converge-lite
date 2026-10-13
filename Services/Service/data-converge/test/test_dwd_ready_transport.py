"""DWD wire protocol and durable Redis-list transfer, using synthetic data."""

import logging
import os
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

TYPES = ("GPU_REALTIME", "NODE_REALTIME", "REQUEST_REALTIME", "MODELINSTANCE_REALTIME", "MODELMETRIC_REALTIME")


def enabled_config(**changes):
    section = {"enabled": True, "redis": {"host": "${READY_REDIS_HOST}", "port": "${READY_REDIS_PORT}",
                                          "mode": "standalone", "password_env": "READY_REDIS_PASSWORD"}}
    section.update(changes)
    return {"dwd_ready": section}


@pytest.fixture(autouse=True)
def connection_environment(monkeypatch):
    monkeypatch.setenv("READY_REDIS_HOST", "127.0.0.1")
    monkeypatch.setenv("READY_REDIS_PORT", "6379")
    monkeypatch.delenv("READY_REDIS_PASSWORD", raising=False)


def load(raw, tmp_path):
    from data_converge.dwd_ready.config import load_ready_config
    return load_ready_config(raw, tmp_path)


@pytest.mark.parametrize("data_type", TYPES)
@pytest.mark.parametrize("as_bytes", [False, True])
def test_codec_matches_producer_format_and_local_timezone(data_type, as_bytes):
    from data_converge.dwd_ready.message import DWD_TYPES, encode_message, parse_message
    text = "2025-08-01T10:05:" + data_type
    parsed = parse_message(text.encode() if as_bytes else text)
    assert set(DWD_TYPES) == set(TYPES)
    assert parsed.window_start == datetime(2025, 8, 1, 10, 5, tzinfo=ZoneInfo("Asia/Shanghai"))
    assert parsed.window_start.tzinfo.key == "Asia/Shanghai"
    assert parsed.data_type == data_type
    assert encode_message(parsed.window_start, parsed.data_type) == text


@pytest.mark.parametrize("raw", [None, 5, b"\xff", "", "private poison", "2025-08-01T10:05",
    "2025-08-01T10:05:PRIVATE", "2025-08-01T10:05:gpu_realtime", "2025-08-01T10:05:GPU_REALTIME\n",
    " 2025-08-01T10:05:GPU_REALTIME", "2025-8-1T10:05:GPU_REALTIME", "2025-08-01 10:05:GPU_REALTIME",
    "2025-08-01T10:05:00:GPU_REALTIME", "2025-08-01T10:05+08:00:GPU_REALTIME",
    "2025-08-01T10:05Z:GPU_REALTIME", "2025-08-01T10:06:GPU_REALTIME", "2025-02-29T10:05:GPU_REALTIME",
    "2025-08-01T24:00:GPU_REALTIME", "0000-08-01T10:05:GPU_REALTIME", "x" * 10000])
def test_codec_rejects_poison_without_echoing_payload(raw):
    from data_converge.dwd_ready.message import ReadyMessageError, parse_message
    with pytest.raises(ReadyMessageError) as caught:
        parse_message(raw)
    assert "private" not in str(caught.value).lower() and len(str(caught.value)) < 100
    assert caught.value.__suppress_context__


def test_codec_calendar_boundaries_and_aware_encoding():
    from data_converge.dwd_ready.message import ReadyMessageError, encode_message, parse_message
    assert parse_message("2024-02-29T23:55:GPU_REALTIME").window_start.day == 29
    assert encode_message(datetime(2025, 7, 31, 16, tzinfo=timezone.utc), "NODE_REALTIME") == "2025-08-01T00:00:NODE_REALTIME"
    for value in [datetime(2025, 8, 1, 10, 5), datetime(2025, 8, 1, 10, 5, 1, tzinfo=ZoneInfo("Asia/Shanghai"))]:
        with pytest.raises(ReadyMessageError):
            encode_message(value, "GPU_REALTIME")


def test_disabled_configuration_has_no_redis_environment_or_password_dependency(tmp_path, monkeypatch):
    monkeypatch.delenv("READY_REDIS_HOST")
    assert load({}, tmp_path)["enabled"] is False
    assert load(enabled_config(enabled=False), tmp_path)["enabled"] is False


def test_offline_configuration_has_safe_defaults_and_no_password_read(tmp_path):
    config = load(enabled_config(), tmp_path)
    assert config["enabled"] is True
    assert config["redis"] == {"host": "127.0.0.1", "port": 6379, "mode": "standalone", "db": 0,
                               "username": None, "password_env": "READY_REDIS_PASSWORD", "ssl": True,
                               "ssl_verify": True, "socket_timeout": 3, "socket_connect_timeout": 2}
    assert config["queue_key"] == "modelverse:dwd:ready"
    assert config["processing_key"] == "{modelverse:dwd:ready}:data-converge:processing"
    assert config["block_seconds"] == 1 and config["batch_size"] == 100
    assert config["wait_warning_seconds"] == 60
    assert "password" not in config["redis"]


@pytest.mark.parametrize("changes", [{"enabled": "true"}, {"batch_size": 0}, {"batch_size": 1001},
    {"batch_size": True}, {"block_seconds": 0}, {"block_seconds": 3}, {"wait_warning_seconds": 0},
    {"wait_warning_seconds": float("nan")}, {"queue_key": ""}, {"queue_key": "private\nkey"},
    {"queue_key": "queue{}broken"},
    {"processing_key": "private:override"}])
def test_ready_configuration_rejects_invalid_values(tmp_path, changes):
    with pytest.raises(ValueError) as caught:
        load(enabled_config(**changes), tmp_path)
    assert "private" not in str(caught.value)


@pytest.mark.parametrize("changes", [{"host": "${MISSING_READY_ENV}"}, {"host": "redis://user:private@localhost"},
    {"host": "localhost/path"}, {"port": True}, {"port": "private"}, {"port": 65536},
    {"mode": "sentinel"}, {"mode": "cluster", "db": 5}, {"db": -1}, {"db": True},
    {"password": "private"}, {"password_env": "private-invalid-name"}, {"password_env": None},
    {"url": "redis://user:private@localhost"}, {"ssl": "true"}, {"ssl_verify": "false"},
    {"socket_timeout": 6}, {"socket_connect_timeout": 0}, {"socket_timeout": 1},
    {"username": "private\nuser"}])
def test_redis_configuration_rejects_unsafe_or_unbounded_settings(tmp_path, changes):
    raw = enabled_config()
    raw["dwd_ready"]["redis"].update(changes)
    with pytest.raises(ValueError) as caught:
        load(raw, tmp_path)
    assert "private" not in str(caught.value)


@pytest.mark.parametrize("queue, expected", [("modelverse:dwd:ready", "{modelverse:dwd:ready}:data-converge:processing"),
    ("app:{tenant}:ready", "{tenant}:data-converge:processing:"),
    ("app:{{tenant}:ready", "{{tenant}:data-converge:processing:")])
def test_processing_list_shares_real_redis_hash_slot(tmp_path, queue, expected):
    from redis.crc import key_slot
    config = load(enabled_config(queue_key=queue), tmp_path)
    if "{" in queue:
        assert config["processing_key"].startswith(expected)
        assert len(config["processing_key"]) == len(expected) + 64
    else:
        assert config["processing_key"] == expected
    assert key_slot(queue.encode()) == key_slot(config["processing_key"].encode())


class FakeRedis:
    """List operations retain Redis LPUSH/right-pop ordering and exact raw bytes."""
    def __init__(self):
        self.lists = defaultdict(list)
        self.operations = []
        self.fail = None
        self.closed = 0

    def _call(self, *args):
        self.operations.append(args)
        if self.fail == args[0]:
            raise RuntimeError("private SDK credentials and payload")

    def lindex(self, key, index):
        self._call("lindex", key, index)
        return self.lists[key][index] if self.lists[key] else None

    def rpoplpush(self, source, destination):
        self._call("rpoplpush", source, destination)
        if not self.lists[source]:
            return None
        value = self.lists[source].pop()
        self.lists[destination].insert(0, value)
        return value

    def brpoplpush(self, source, destination, timeout):
        self._call("brpoplpush", source, destination, timeout)
        if not self.lists[source]:
            return None
        value = self.lists[source].pop()
        self.lists[destination].insert(0, value)
        return value

    def lrem(self, key, count, raw):
        self._call("lrem", key, count, raw)
        if raw not in self.lists[key]:
            return 0
        self.lists[key].remove(raw)
        return 1

    def close(self):
        self.closed += 1
        if self.fail == "close":
            raise RuntimeError("private close error")


def make_source(tmp_path, client=None):
    from data_converge.dwd_ready.redis_source import RedisSignalSource
    config = load(enabled_config(), tmp_path)
    client = client or FakeRedis()
    return RedisSignalSource(client, config), client, config


def test_receive_recovers_pending_before_removing_new_queue_items(tmp_path):
    source, client, config = make_source(tmp_path)
    client.lists[config["queue_key"]] = [b"next"]
    client.lists[config["processing_key"]] = [b"new pending", b"old pending"]
    assert source.receive() == b"old pending"
    assert source.receive(block=False) == b"old pending"
    assert client.lists[config["queue_key"]] == [b"next"]
    assert source.ack(b"old pending") == 1
    assert source.receive() == b"new pending"
    assert source.ack(b"new pending") == 1
    assert source.receive() == b"next"
    assert client.lists[config["processing_key"]] == [b"next"]
    assert client.operations[-1] == ("brpoplpush", config["queue_key"], config["processing_key"], 1)


def test_nonblocking_transfer_and_empty_timeout_preserve_unacked_data(tmp_path):
    source, client, config = make_source(tmp_path)
    assert source.receive() is None
    client.lists[config["queue_key"]] = [b"last", b"first"]
    assert source.receive(block=False) == b"first"
    assert client.operations[-1][0] == "rpoplpush"
    recovered, _, _ = make_source(tmp_path, client)
    assert recovered.receive() == b"first"
    assert recovered.ack(b"first") == 1
    assert recovered.receive(block=False) == b"last"


@pytest.mark.parametrize("operation", ["lindex", "brpoplpush", "rpoplpush", "lrem"])
def test_redis_errors_are_redacted_and_unacked_data_is_retained(tmp_path, operation):
    from data_converge.dwd_ready.redis_source import RedisSignalError
    source, client, config = make_source(tmp_path)
    client.fail = operation
    if operation == "lrem":
        client.lists[config["processing_key"]] = [b"private raw message"]
    with pytest.raises(RedisSignalError) as caught:
        source.ack(b"private raw message") if operation == "lrem" else source.receive(block=operation != "rpoplpush")
    assert "private" not in str(caught.value) and caught.value.__suppress_context__
    if operation == "lrem":
        assert client.lists[config["processing_key"]] == [b"private raw message"]


def test_close_is_idempotent_and_never_leaks_sdk_errors(tmp_path, capsys):
    from data_converge.dwd_ready.redis_source import RedisSignalError
    source, client, _ = make_source(tmp_path)
    client.fail = "close"
    source.close()
    source.close()
    assert client.closed == 1
    with pytest.raises(RedisSignalError):
        source.receive()
    captured = capsys.readouterr()
    assert "private" not in captured.out + captured.err


class CleanupRedis:
    def __init__(self, name, events, failed_operations=()):
        self.name = name
        self.events = events
        self.failed_operations = failed_operations
        self.connection_pool = self

    def close(self):
        self.events.append((self.name, "close"))
        if "close" in self.failed_operations:
            raise RuntimeError("private close credentials")

    def disconnect(self):
        self.events.append((self.name, "disconnect"))
        if "disconnect" in self.failed_operations:
            raise RuntimeError("private disconnect credentials")


@pytest.mark.parametrize("failed_operations, expected_codes", [
    ((), []),
    (("close",), ["DWD_READY_REDIS_CLOSE_FAILED"]),
    (("disconnect",), ["DWD_READY_REDIS_DISCONNECT_FAILED"]),
    (("close", "disconnect"), ["DWD_READY_REDIS_CLOSE_FAILED", "DWD_READY_REDIS_DISCONNECT_FAILED"]),
])
def test_cleanup_warns_safely_and_continues_through_all_clients(tmp_path, caplog, failed_operations, expected_codes):
    from data_converge.dwd_ready.redis_source import RedisSignalSource
    events = []
    client = CleanupRedis("data", events, failed_operations)
    sentinels = (CleanupRedis("sentinel1", events, failed_operations), CleanupRedis("sentinel2", events))
    source = RedisSignalSource(client, load(enabled_config(), tmp_path), sentinel_clients=sentinels)
    with caplog.at_level(logging.WARNING, logger="data_converge.dwd_ready.redis_source"):
        source.close()
        source.close()
    assert events == [("data", "close"), ("data", "disconnect"), ("sentinel1", "close"),
                      ("sentinel1", "disconnect"), ("sentinel2", "close"), ("sentinel2", "disconnect")]
    assert [record.getMessage() for record in caplog.records] == expected_codes * 2
    assert all(record.levelno == logging.WARNING and record.exc_info is None for record in caplog.records)
    assert "private" not in caplog.text


def test_client_without_dedicated_pool_closes_without_false_warning(tmp_path, caplog):
    source, client, _ = make_source(tmp_path)
    with caplog.at_level(logging.WARNING, logger="data_converge.dwd_ready.redis_source"):
        source.close()
    assert client.closed == 1
    assert not caplog.records


@pytest.mark.parametrize("mode", ["standalone", "cluster"])
def test_factory_reads_password_at_creation_and_disables_driver_retries(tmp_path, monkeypatch, mode):
    import redis
    from data_converge.dwd_ready.redis_source import RedisSignalError, create_source
    raw = enabled_config()
    raw["dwd_ready"]["redis"]["mode"] = mode
    # Older redis-py 5 Cluster TLS cannot preserve hostname verification; that
    # rejection has its own test. Keep this test about credentials and retries.
    raw["dwd_ready"]["redis"]["ssl"] = mode != "cluster"
    config = load(raw, tmp_path)
    with pytest.raises(RedisSignalError):
        create_source(config)
    monkeypatch.setenv("READY_REDIS_PASSWORD", " private exact password ")
    captured, client = {}, FakeRedis()
    def create(**kwargs):
        captured.update(kwargs)
        return client
    monkeypatch.setattr(redis, "Redis" if mode == "standalone" else "RedisCluster", create)
    source = create_source(config)
    assert captured["password"] == " private exact password "
    assert captured["retry"]._retries == 0 and captured.get("retry_on_timeout", False) is False
    assert captured["decode_responses"] is False and captured["ssl"] is (mode != "cluster")
    if mode != "cluster":
        assert captured["ssl_cert_reqs"] == "required" and captured["ssl_check_hostname"] is True
    assert captured["socket_timeout"] == 3 and captured["socket_connect_timeout"] == 2
    assert ("db" in captured) is (mode == "standalone")
    if mode == "cluster":
        assert client.RedisClusterRequestTTL == 1
    assert "password" not in config["redis"]
    source.close()


def test_factory_does_not_echo_constructor_failure_or_accept_unsupported_sdk(tmp_path, monkeypatch):
    import redis
    from data_converge.dwd_ready.redis_source import RedisSignalError, create_source
    config = load(enabled_config(), tmp_path)
    monkeypatch.setenv("READY_REDIS_PASSWORD", "private")
    def broken(**kwargs):
        raise RuntimeError("private SDK configuration")
    monkeypatch.setattr(redis, "Redis", broken)
    with pytest.raises(RedisSignalError) as caught:
        create_source(config)
    assert "private" not in str(caught.value) and caught.value.__suppress_context__
    monkeypatch.setattr(redis, "__version__", "7.0.0")
    with pytest.raises(RedisSignalError):
        create_source(config)


def test_real_standalone_client_configuration_is_lazy_and_has_zero_retries(tmp_path, monkeypatch):
    from data_converge.dwd_ready.redis_source import create_source
    raw = enabled_config()
    raw["dwd_ready"]["redis"].update(ssl=False, db=5)
    config = load(raw, tmp_path)
    monkeypatch.setenv("READY_REDIS_PASSWORD", "synthetic test password")
    source = create_source(config)
    try:
        options = source._client.connection_pool.connection_kwargs
        assert options["db"] == 5 and options["retry"]._retries == 0
        assert options["socket_timeout"] == 3 and options["socket_connect_timeout"] == 2
        assert options["decode_responses"] is False
        assert not source._client.connection_pool._in_use_connections
    finally:
        source.close()


def test_missing_or_malformed_mode_is_rejected_instead_of_inferred(tmp_path):
    raw = enabled_config()
    del raw["dwd_ready"]["redis"]["mode"]
    with pytest.raises(ValueError):
        load(raw, tmp_path)


def test_redis_five_cluster_uses_both_retry_switches(tmp_path, monkeypatch):
    import redis
    from data_converge.dwd_ready.redis_source import create_source
    raw = enabled_config()
    raw["dwd_ready"]["redis"]["mode"] = "cluster"
    raw["dwd_ready"]["redis"]["ssl"] = False
    config = load(raw, tmp_path)
    monkeypatch.setenv("READY_REDIS_PASSWORD", "synthetic")
    monkeypatch.setattr(redis, "__version__", "5.0.8")
    captured = {}
    def create(**kwargs):
        captured.update(kwargs)
        return FakeRedis()
    monkeypatch.setattr(redis, "RedisCluster", create)
    source = create_source(config)
    assert captured["retry"]._retries == 0
    assert captured["cluster_error_retry_attempts"] == 0
    source.close()


@pytest.fixture
def local_redis_server(tmp_path):
    """Optional actual Redis integration, always restricted to an ephemeral localhost port."""
    import shutil
    import socket
    import subprocess
    import time
    import redis

    executable = shutil.which("redis-server")
    if executable is None:
        pytest.skip("redis-server is unavailable; real Redis loopback integration was not run")
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    process = subprocess.Popen([executable, "--bind", "127.0.0.1", "--port", str(port), "--protected-mode", "yes",
                                "--save", "", "--appendonly", "no", "--dir", str(tmp_path)],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    admin = redis.Redis(host="127.0.0.1", port=port, socket_connect_timeout=0.2, socket_timeout=0.2)
    try:
        deadline = time.monotonic() + 3
        while True:
            if process.poll() is not None:
                pytest.fail("local test Redis exited before readiness")
            try:
                if admin.ping():
                    break
            except redis.RedisError:
                if time.monotonic() >= deadline:
                    pytest.fail("local test Redis did not become ready")
                time.sleep(0.02)
        yield port, admin
    finally:
        admin.close()
        process.terminate()
        try:
            process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=2)


def test_real_redis_transfer_recovery_ack_and_block_timeout(local_redis_server, tmp_path, monkeypatch):
    from data_converge.dwd_ready.redis_source import create_source
    import redis
    import time

    port, admin = local_redis_server
    # Configure authentication over the local connection, never in process argv.
    password = "synthetic test password"
    admin.config_set("requirepass", password)
    producer = redis.Redis(host="127.0.0.1", port=port, password=password, socket_timeout=1)
    monkeypatch.setenv("READY_REDIS_PASSWORD", password)
    raw = enabled_config(block_seconds=0.1)
    raw["dwd_ready"]["redis"].update(port=port, ssl=False, socket_timeout=1)
    config = load(raw, tmp_path)
    source = create_source(config)
    try:
        producer.lpush(config["queue_key"], b"2025-08-01T10:05:GPU_REALTIME")
        producer.lpush(config["queue_key"], b"2025-08-01T10:05:NODE_REALTIME")
        message = source.receive()
        assert message == b"2025-08-01T10:05:GPU_REALTIME"
        source.close()
        source = create_source(config)
        assert source.receive(block=False) == message
        assert producer.llen(config["queue_key"]) == 1
        assert source.ack(message) == 1
        next_message = source.receive(block=False)
        assert next_message == b"2025-08-01T10:05:NODE_REALTIME"
        assert source.ack(next_message) == 1
        started = time.monotonic()
        assert source.receive() is None
        assert 0.05 <= time.monotonic() - started < 1
        assert producer.llen(config["processing_key"]) == 0
    finally:
        source.close()
        producer.close()


def test_custom_queues_with_same_tag_have_separate_processing_lists(tmp_path):
    from redis.crc import key_slot
    first = load(enabled_config(queue_key="{shared}:readyA"), tmp_path)
    second = load(enabled_config(queue_key="{shared}:readyB"), tmp_path)
    assert first["processing_key"] != second["processing_key"]
    for config in (first, second):
        assert config["processing_key"].startswith("{shared}:data-converge:processing:")
        assert key_slot(config["queue_key"].encode()) == key_slot(config["processing_key"].encode())


def test_cluster_tls_fails_before_connect_if_driver_drops_hostname_verification(tmp_path, monkeypatch):
    import redis
    import redis.cluster
    from data_converge.dwd_ready.redis_source import RedisSignalError, create_source
    raw = enabled_config()
    raw["dwd_ready"]["redis"]["mode"] = "cluster"
    config = load(raw, tmp_path)
    monkeypatch.setenv("READY_REDIS_PASSWORD", "private")
    monkeypatch.setattr(redis.cluster, "REDIS_ALLOWED_KEYS",
                        tuple(key for key in redis.cluster.REDIS_ALLOWED_KEYS if key != "ssl_check_hostname"))
    connections = []
    def create(**kwargs):
        connections.append(kwargs)
        return FakeRedis()
    monkeypatch.setattr(redis, "RedisCluster", create)
    with pytest.raises(RedisSignalError, match="TLS_CLIENT_UNSUPPORTED"):
        create_source(config)
    assert not connections
