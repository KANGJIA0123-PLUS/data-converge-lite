"""Atomic Redis ownership operations; these do not fence ClickHouse writes."""

from __future__ import annotations

import math


RENEW_SCRIPT = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
return redis.call('PEXPIRE', KEYS[1], ARGV[2])
"""
RELEASE_SCRIPT = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
return redis.call('DEL', KEYS[1])
"""


class LeadershipError(RuntimeError):
    """A fixed diagnostic code without Redis exceptions or connection details."""


def ttl_milliseconds(ttl: float) -> int:
    if type(ttl) not in (int, float) or not math.isfinite(ttl) or ttl <= 0:
        raise ValueError('LEADERSHIP_SETTINGS_INVALID')
    return math.ceil(ttl * 1000)


class LeaderElector:
    """Use a dedicated, bounded-timeout client owned and closed by the caller."""

    LEADER_KEY_PREFIX = 'data_converge:leader:'

    def __init__(self, cluster_name: str, redis_host: str = 'localhost',
                 redis_port: int = 6379, *, client=None):
        if (not isinstance(cluster_name, str) or not cluster_name.strip()
                or any(character in cluster_name for character in '{}')):
            raise ValueError('LEADERSHIP_SETTINGS_INVALID')
        self.cluster_name = cluster_name
        self.leader_key = f'{self.LEADER_KEY_PREFIX}{{{cluster_name}}}'
        self.redis_client = client if client is not None else self._create_client(redis_host, redis_port)

    @staticmethod
    def _create_client(redis_host, redis_port):
        import redis
        from redis.backoff import NoBackoff
        from redis.retry import Retry

        return redis.Redis(
            host=redis_host, port=redis_port, decode_responses=True,
            socket_timeout=3, socket_connect_timeout=2,
            retry=Retry(NoBackoff(), 0), retry_on_error=[])

    def _command(self, operation, *args, **kwargs):
        try:
            return operation(*args, **kwargs)
        except Exception:
            raise LeadershipError('LEADERSHIP_REDIS_FAILED') from None

    def elect_as_leader(self, node_id: str, ttl: float = 30) -> bool:
        """The caller supplies a fresh token for every acquisition attempt."""
        return self._command(self.redis_client.set, self.leader_key, node_id,
                             nx=True, px=ttl_milliseconds(ttl)) is True

    def renew_leadership(self, node_id: str, ttl: float = 30) -> bool:
        return self._command(self.redis_client.eval, RENEW_SCRIPT, 1,
                             self.leader_key, node_id, ttl_milliseconds(ttl)) == 1

    def release_leadership(self, node_id: str) -> bool:
        return self._command(self.redis_client.eval, RELEASE_SCRIPT, 1,
                             self.leader_key, node_id) == 1

    def get_current_leader(self) -> str | None:
        value = self._command(self.redis_client.get, self.leader_key)
        return value.decode('utf-8', errors='replace') if isinstance(value, bytes) else value

    def is_leader(self, node_id: str) -> bool:
        """Inspection only; callers must not use this read as a write guard."""
        return self.get_current_leader() == node_id

    def get_leader_ttl(self) -> int:
        return self._command(self.redis_client.ttl, self.leader_key)
