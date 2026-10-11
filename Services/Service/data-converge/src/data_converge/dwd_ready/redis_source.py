"""Single-consumer Redis list transport; acknowledge only after durable storage."""

from __future__ import annotations

import os


class RedisSignalError(RuntimeError):
    """Safe, fixed Redis diagnostic without SDK exception text or raw messages."""


class RedisSignalSource:
    def __init__(self, client, ready_config):
        self._client = client
        self.queue_key = ready_config["queue_key"]
        self.processing_key = ready_config["processing_key"]
        self.block_seconds = ready_config["block_seconds"]
        self._closed = False

    def receive(self, block=True) -> str | bytes | None:
        if self._closed:
            raise RedisSignalError("DWD_READY_REDIS_CLOSED") from None
        try:
            raw = self._client.lindex(self.processing_key, -1)
            if raw is None:
                if block:
                    raw = self._client.brpoplpush(self.queue_key, self.processing_key, timeout=self.block_seconds)
                else:
                    raw = self._client.rpoplpush(self.queue_key, self.processing_key)
            if raw is not None and not isinstance(raw, (str, bytes)):
                raise ValueError
            return raw
        except Exception:
            raise RedisSignalError("DWD_READY_REDIS_RECEIVE_FAILED") from None

    def ack(self, raw: str | bytes) -> int:
        if self._closed:
            raise RedisSignalError("DWD_READY_REDIS_CLOSED") from None
        try:
            if not isinstance(raw, (str, bytes)):
                raise ValueError
            result = self._client.lrem(self.processing_key, 1, raw)
            if type(result) is not int or result not in (0, 1):
                raise ValueError
            return result
        except Exception:
            raise RedisSignalError("DWD_READY_REDIS_ACK_FAILED") from None

    def close(self):
        if not self._closed:
            self._closed = True
            try:
                self._client.close()
            except Exception:
                # Cleanup must not replace a useful failure with private SDK text.
                pass


def create_source(ready_config: dict) -> RedisSignalSource:
    """Read credentials only here; disable implicit command retry in redis-py 5/6."""
    client = None
    failure_code = "DWD_READY_REDIS_CREATE_FAILED"
    try:
        if ready_config.get("enabled") is not True:
            raise ValueError
        import redis
        from redis.backoff import NoBackoff
        from redis.retry import Retry

        major = int(redis.__version__.split(".", 1)[0])
        if major not in (5, 6):
            raise ValueError
        connection = ready_config["redis"]
        password = os.environ.get(connection["password_env"])
        if not isinstance(password, str) or not password:
            raise ValueError
        options = {name: connection[name] for name in ("host", "port", "username", "ssl", "socket_timeout", "socket_connect_timeout")}
        options.update(password=password, retry=Retry(NoBackoff(), 0),
                       retry_on_error=[], decode_responses=False, health_check_interval=0,
                       max_connections=1)
        if major == 5:
            options["retry_on_timeout"] = False
        if connection["ssl"]:
            options.update(ssl_cert_reqs="required" if connection["ssl_verify"] else "none",
                           ssl_check_hostname=connection["ssl_verify"])
        if connection["mode"] == "cluster":
            from redis.cluster import REDIS_ALLOWED_KEYS
            if connection["ssl"] and connection["ssl_verify"] and "ssl_check_hostname" not in REDIS_ALLOWED_KEYS:
                failure_code = "DWD_READY_REDIS_TLS_CLIENT_UNSUPPORTED"
                raise ValueError
            # Redis 5 has a separate cluster retry counter; Redis 6 uses Retry.
            if major == 5:
                options["cluster_error_retry_attempts"] = 0
            client = redis.RedisCluster(**options)
            # MOVED/ASK has a separate loop even when connection retries are zero.
            # A later explicit poll may recover after topology refresh; do not loop.
            client.RedisClusterRequestTTL = 1
        else:
            client = redis.Redis(db=connection["db"], **options)
        return RedisSignalSource(client, ready_config)
    except Exception:
        if client is not None:
            try:
                client.close()
            except Exception:
                pass
        raise RedisSignalError(failure_code) from None
