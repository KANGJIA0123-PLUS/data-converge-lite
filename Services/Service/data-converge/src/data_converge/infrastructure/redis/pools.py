"""Three independent, configurable Redis pools sharing one authenticated endpoint.

Design: leadership uses a small stable pool; DWD/status reads match dispatch
parallelism; status writes are frequent and light. Each pool owns its sockets,
so a slow read cannot delay lease renewal or a fenced state write.
"""

from __future__ import annotations

from dataclasses import dataclass, field

POOL_NAMES = ('leader', 'status', 'log')

# Longer than the design samples: Sentinel + PSM challenge authentication needs
# several round trips, and a timed-out renewal must not trigger a failover.
DEFAULTS = {
    'leader': dict(max_connections=3, socket_timeout=3.0, socket_connect_timeout=2.0),
    'status': dict(max_connections=15, socket_timeout=5.0, socket_connect_timeout=3.0),
    'log': dict(max_connections=5, socket_timeout=3.0, socket_connect_timeout=3.0),
}


class RedisPoolConfigError(ValueError):
    """Fixed public code for invalid pool settings."""


@dataclass(frozen=True)
class PoolSettings:
    max_connections: int
    socket_timeout: float
    socket_connect_timeout: float

    def __post_init__(self):
        if type(self.max_connections) is not int or not 1 <= self.max_connections <= 100:
            raise RedisPoolConfigError('REDIS_POOL_CONFIG_INVALID')
        for value in (self.socket_timeout, self.socket_connect_timeout):
            if type(value) not in (int, float) or not 0 < value <= 60:
                raise RedisPoolConfigError('REDIS_POOL_CONFIG_INVALID')


@dataclass(frozen=True)
class RedisPools:
    leader: PoolSettings = field(default_factory=lambda: PoolSettings(**DEFAULTS['leader']))
    status: PoolSettings = field(default_factory=lambda: PoolSettings(**DEFAULTS['status']))
    log: PoolSettings = field(default_factory=lambda: PoolSettings(**DEFAULTS['log']))

    @classmethod
    def from_mapping(cls, value) -> RedisPools:
        if value is None:
            return cls()
        if not isinstance(value, dict) or set(value) - set(POOL_NAMES):
            raise RedisPoolConfigError('REDIS_POOL_CONFIG_INVALID')
        pools = {}
        for name in POOL_NAMES:
            raw = value.get(name, {})
            if not isinstance(raw, dict) or set(raw) - set(DEFAULTS[name]):
                raise RedisPoolConfigError('REDIS_POOL_CONFIG_INVALID')
            pools[name] = PoolSettings(**{**DEFAULTS[name], **raw})
        return cls(**pools)

    def connection_config(self, ready_config: dict, name: str) -> dict:
        """Copy the DWD endpoint/auth section, replacing only socket and pool options."""
        pool = getattr(self, name)
        if not isinstance(ready_config.get('redis'), dict) or not ready_config['redis']:
            return ready_config  # Disabled readiness: let create_connection report it.
        redis_section = dict(ready_config['redis'], socket_timeout=pool.socket_timeout,
                             socket_connect_timeout=pool.socket_connect_timeout,
                             max_connections=pool.max_connections)
        return dict(ready_config, redis=redis_section)
