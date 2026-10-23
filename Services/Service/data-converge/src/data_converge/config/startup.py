"""Bounded waiting policy for the service's read-only ClickHouse startup check."""

from dataclasses import dataclass, fields
import math


@dataclass(frozen=True)
class StartupSettings:
    timeout_seconds: float = 600.0
    initial_retry_delay_seconds: float = 2.0
    max_retry_delay_seconds: float = 30.0
    probe_timeout_seconds: float = 10.0

    def __post_init__(self):
        for option in fields(self):
            value = getattr(self, option.name)
            if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
                raise ValueError('STARTUP_SETTING_INVALID')
        if self.initial_retry_delay_seconds > self.max_retry_delay_seconds:
            raise ValueError('STARTUP_SETTING_INVALID')

    @classmethod
    def from_mapping(cls, value):
        if value is None:
            return cls()
        if not isinstance(value, dict) or value.keys() - {item.name for item in fields(cls)}:
            raise ValueError('STARTUP_SETTING_INVALID')
        return cls(**value)
