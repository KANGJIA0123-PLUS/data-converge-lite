"""Validated, immutable service log retention settings."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True)
class LoggingSettings:
    max_bytes: int = 20 * 1024 * 1024
    backup_count: int = 10
    retention_days: int = 7

    def __post_init__(self):
        for value in (self.max_bytes, self.backup_count, self.retention_days):
            if type(value) is not int or value <= 0:
                raise ValueError('LOGGING_SETTING_INVALID')

    @classmethod
    def from_mapping(cls, value: Mapping | None) -> LoggingSettings:
        if value is None:
            return cls()
        names = {'max_bytes', 'backup_count', 'retention_days'}
        if not isinstance(value, Mapping) or set(value) - names:
            raise ValueError('LOGGING_SETTING_INVALID')
        return cls(**value)
