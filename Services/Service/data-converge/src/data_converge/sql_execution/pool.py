"""Aggregation-only ClickHouse pool policy: concurrency, connect timeout, settings.

ClickHouse is reached over HTTP, so the "pool" bounds concurrent aggregation
queries with a semaphore (exporter queries use their own client and are not
affected) and attaches per-query server settings as URL parameters.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
import threading

DEFAULT_SETTINGS = {
    'max_threads': 8,               # 单条汇聚 SQL 的内部并行线程数
    'readonly': 0,                  # 允许写入
    'load_balancing': 'random',     # 集群读请求均衡分发
    'max_memory_usage': 10_000_000_000,  # 单次查询内存上限 10GB，防止 OOM
}
# max_execution_time is set per query from the task's period timeout.
ALLOWED = {'max_threads', 'readonly', 'load_balancing', 'max_memory_usage'}


class ClickHousePoolError(ValueError):
    """Fixed public code for invalid pool settings."""


@dataclass(frozen=True)
class ClickHousePool:
    max_connections: int = 20
    connect_timeout_seconds: float = 10.0
    settings: dict = field(default_factory=lambda: dict(DEFAULT_SETTINGS))
    _gate: threading.BoundedSemaphore = field(init=False, repr=False, compare=False)

    def __post_init__(self):
        if type(self.max_connections) is not int or not 1 <= self.max_connections <= 200:
            raise ClickHousePoolError('CLICKHOUSE_POOL_CONFIG_INVALID')
        timeout = self.connect_timeout_seconds
        if type(timeout) not in (int, float) or not 0 < timeout <= 120:
            raise ClickHousePoolError('CLICKHOUSE_POOL_CONFIG_INVALID')
        if not isinstance(self.settings, dict) or set(self.settings) - ALLOWED:
            raise ClickHousePoolError('CLICKHOUSE_POOL_CONFIG_INVALID')
        for name, value in self.settings.items():
            valid = (isinstance(value, str) and value.replace('_', '').isalnum()) if name == 'load_balancing' \
                else (type(value) is int and value >= 0)
            if not valid:
                raise ClickHousePoolError('CLICKHOUSE_POOL_CONFIG_INVALID')
        object.__setattr__(self, '_gate', threading.BoundedSemaphore(self.max_connections))

    @classmethod
    def from_mapping(cls, value) -> ClickHousePool:
        if value is None:
            return cls()
        names = {'max_connections', 'connect_timeout_seconds', 'settings'}
        if not isinstance(value, dict) or set(value) - names:
            raise ClickHousePoolError('CLICKHOUSE_POOL_CONFIG_INVALID')
        options = dict(value)
        if 'settings' in options:
            if not isinstance(options['settings'], dict):
                raise ClickHousePoolError('CLICKHOUSE_POOL_CONFIG_INVALID')
            options['settings'] = {**DEFAULT_SETTINGS, **options['settings']}
        return cls(**options)

    @contextmanager
    def slot(self, timeout: float):
        """Wait for a free connection within the query's own budget."""
        if not self._gate.acquire(timeout=max(0.0, timeout)):
            raise TimeoutError('CLICKHOUSE_POOL_EXHAUSTED')
        try:
            yield
        finally:
            self._gate.release()

    def query_parameters(self) -> dict[str, str]:
        return {name: str(value) for name, value in self.settings.items()}
