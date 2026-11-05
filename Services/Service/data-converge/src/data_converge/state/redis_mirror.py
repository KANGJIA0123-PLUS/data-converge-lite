"""Shared, fenced task state in Redis so a standby can resume where the leader stopped.

Layout (all keys share the ``{cluster}`` hash tag so Lua runs in one slot):

    dc:{cluster}:inst:<instance_id>   HASH  status, attempts, query_id, taskname,
                                            data_start, data_end, updated_at  (period TTL)
    dc:{cluster}:cursor               HASH  taskname -> next_fire_time        (no TTL)
    dc:{cluster}:dwd:<window_start>   SET   ready DWD types                   (period TTL)
    dc:{cluster}:running              SET   instance ids in RUNNING            (no TTL)

Every write runs a Lua script that first compares the leader key's value with
this process's lease token. A deposed leader whose SQL finishes late cannot
overwrite the new leader's state; that is the fencing guarantee.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import logging

logger = logging.getLogger(__name__)

PERIOD_NAMES = {'5min': 'PT5M', 'hour': 'PT1H', 'day': 'P1D', 'month': 'P1M'}
# Design table: days for 5min/hour/day, months for month (31-day months).
TTL_DEFAULTS = {'PT5M': 1, 'PT1H': 1, 'P1D': 3, 'P1M': 3}
TTL_RANGES = {'PT5M': (1, 7), 'PT1H': (1, 7), 'P1D': (1, 10), 'P1M': (1, 12)}
TTL_UNIT_DAYS = {'PT5M': 1, 'PT1H': 1, 'P1D': 1, 'P1M': 31}

FENCED_HSET = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return -1 end
redis.call('HSET', KEYS[2], unpack(ARGV, 4))
if tonumber(ARGV[2]) > 0 then redis.call('PEXPIREAT', KEYS[2], ARGV[2]) end
if ARGV[3] == 'RUNNING' then redis.call('SADD', KEYS[3], KEYS[2])
else redis.call('SREM', KEYS[3], KEYS[2]) end
return 1
"""
FENCED_CURSOR = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return -1 end
redis.call('HSET', KEYS[2], unpack(ARGV, 2))
return 1
"""
FENCED_SADD = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return -1 end
redis.call('SADD', KEYS[2], ARGV[3])
if tonumber(ARGV[2]) > 0 then redis.call('PEXPIREAT', KEYS[2], ARGV[2]) end
return 1
"""


class StateSyncError(RuntimeError):
    """Fixed public code; never includes Redis exception text."""


class FencedOut(StateSyncError):
    """The lease token no longer matches: this process must stop writing."""


@dataclass(frozen=True)
class RedisStateSettings:
    enabled: bool = True
    ttl: dict = field(default_factory=lambda: dict(TTL_DEFAULTS))

    @classmethod
    def from_mapping(cls, value) -> RedisStateSettings:
        if value is None:
            return cls()
        if not isinstance(value, dict) or set(value) - {'enabled', 'ttl'}:
            raise ValueError('REDIS_STATE_CONFIG_INVALID')
        enabled = value.get('enabled', True)
        raw = value.get('ttl', {})
        if not isinstance(enabled, bool) or not isinstance(raw, dict) or set(raw) - set(PERIOD_NAMES):
            raise ValueError('REDIS_STATE_CONFIG_INVALID')
        ttl = dict(TTL_DEFAULTS)
        for name, amount in raw.items():
            period = PERIOD_NAMES[name]
            low, high = TTL_RANGES[period]
            if type(amount) is not int or not low <= amount <= high:
                raise ValueError('REDIS_STATE_CONFIG_INVALID')
            ttl[period] = amount
        return cls(enabled, ttl)

    def expires_at(self, interval: str, data_end: datetime) -> datetime:
        """Age from the window end, not the write time, so late reads still find parents."""
        return data_end + timedelta(days=self.ttl[interval] * TTL_UNIT_DAYS[interval])


# A write for a window already past retention (catch-up, backfill) must still be
# visible long enough for takeover and dependency checks.
MIN_RETENTION = timedelta(hours=6)


def _ms(value: datetime) -> int:
    return int(value.astimezone(timezone.utc).timestamp() * 1000)


def _expiry_ms(expires: datetime) -> int:
    return _ms(max(expires, datetime.now(timezone.utc) + MIN_RETENTION))


class RedisStateMirror:
    """Fenced writer plus reader over one leader token; owned by the scheduler thread."""

    def __init__(self, write_client, read_client, *, cluster: str, leader_key: str,
                 token_source, settings: RedisStateSettings):
        self.write = write_client
        self.read = read_client
        self.prefix = f'dc:{{{cluster}}}'
        self.leader_key = leader_key
        self.token_source = token_source
        self.settings = settings
        self._scripts = {name: write_client.register_script(body) for name, body in
                         (('hset', FENCED_HSET), ('cursor', FENCED_CURSOR), ('sadd', FENCED_SADD))}

    def instance_key(self, instance_id: str) -> str:
        return f'{self.prefix}:inst:{instance_id}'

    def _run(self, name, keys, args):
        try:
            result = self._scripts[name](keys=keys, args=args)
        except Exception:
            raise StateSyncError('REDIS_STATE_WRITE_FAILED') from None
        if result == -1:
            raise FencedOut('REDIS_STATE_FENCED')
        return result

    def write_instance(self, row: dict, interval: str) -> None:
        end = datetime.fromisoformat(row['data_end'])
        expires = _expiry_ms(self.settings.expires_at(interval, end))
        fields = {key: '' if row.get(key) is None else str(row[key]) for key in
                  ('status', 'attempts', 'retryable', 'next_retry_at', 'query_id',
                   'taskname', 'scheduled_time', 'data_start', 'data_end', 'error')}
        fields['updated_at'] = datetime.now(timezone.utc).isoformat()
        flat = [item for pair in fields.items() for item in pair]
        self._run('hset', [self.leader_key, self.instance_key(row['instance_id']),
                           f'{self.prefix}:running'],
                  [self.token_source(), expires, fields['status'], *flat])

    def write_cursors(self, cursors: dict) -> None:
        if not cursors:
            return
        flat = [item for name, value in cursors.items() for item in (name, value.isoformat())]
        self._run('cursor', [self.leader_key, f'{self.prefix}:cursor'], [self.token_source(), *flat])

    def write_dwd(self, window_start: datetime, dwd_type: str) -> None:
        expires = _expiry_ms(self.settings.expires_at('PT5M', window_start + timedelta(minutes=5)))
        key = f'{self.prefix}:dwd:{window_start.astimezone(timezone.utc).isoformat()}'
        self._run('sadd', [self.leader_key, key], [self.token_source(), expires, dwd_type])

    def read_instance(self, instance_id: str) -> dict | None:
        try:
            value = self.read.hgetall(self.instance_key(instance_id))
        except Exception:
            raise StateSyncError('REDIS_STATE_READ_FAILED') from None
        return {_text(k): _text(v) for k, v in value.items()} if value else None

    def read_cursors(self) -> dict[str, datetime]:
        try:
            value = self.read.hgetall(f'{self.prefix}:cursor')
        except Exception:
            raise StateSyncError('REDIS_STATE_READ_FAILED') from None
        return {_text(k): datetime.fromisoformat(_text(v)) for k, v in value.items()}

    def read_dwd_types(self, window_start: datetime) -> set[str]:
        key = f'{self.prefix}:dwd:{window_start.astimezone(timezone.utc).isoformat()}'
        try:
            return {_text(item) for item in self.read.smembers(key)}
        except Exception:
            raise StateSyncError('REDIS_STATE_READ_FAILED') from None

    def scan_dwd(self, batch: int = 500):
        """Yield (window_start, types) for every live readiness set."""
        pattern = f'{self.prefix}:dwd:*'
        try:
            for key in self.read.scan_iter(match=pattern, count=batch):
                key = _text(key)
                start = datetime.fromisoformat(key.split(':dwd:', 1)[1])
                yield start, {_text(item) for item in self.read.smembers(key)}
        except Exception:
            raise StateSyncError('REDIS_STATE_READ_FAILED') from None

    def running_instances(self) -> list[dict]:
        """Instances a previous leader left RUNNING; expired hashes are dropped."""
        try:
            keys = [_text(item) for item in self.read.smembers(f'{self.prefix}:running')]
        except Exception:
            raise StateSyncError('REDIS_STATE_READ_FAILED') from None
        result = []
        for key in keys:
            row = self.read_instance(key.rsplit(':inst:', 1)[-1])
            if row is not None and row.get('status') == 'RUNNING':
                result.append(row | {'instance_id': key.rsplit(':inst:', 1)[-1]})
        return result

    def scan_instances(self, batch: int = 500):
        """Yield every live instance hash (used once per takeover to import state)."""
        pattern = f'{self.prefix}:inst:*'
        try:
            for key in self.read.scan_iter(match=pattern, count=batch):
                key = _text(key)
                row = self.read_instance(key.rsplit(':inst:', 1)[-1])
                if row is not None:
                    yield row | {'instance_id': key.rsplit(':inst:', 1)[-1]}
        except StateSyncError:
            raise
        except Exception:
            raise StateSyncError('REDIS_STATE_READ_FAILED') from None

    def scan_instance_ids(self, batch: int = 500):
        """Yield live instance ids without reading each hash."""
        pattern = f'{self.prefix}:inst:*'
        try:
            for key in self.read.scan_iter(match=pattern, count=batch):
                yield _text(key).rsplit(':inst:', 1)[-1]
        except Exception:
            raise StateSyncError('REDIS_STATE_READ_FAILED') from None


def _text(value) -> str:
    return value.decode('utf-8') if isinstance(value, bytes) else value
