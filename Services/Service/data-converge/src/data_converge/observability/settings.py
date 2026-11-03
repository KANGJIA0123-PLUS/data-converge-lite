"""Validated, immutable service log retention settings."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True)
class LoggingSettings:
    """Design policy: rotate at 10 MiB, gzip, keep 50 archives within 500 MiB.

    Aging runs only when a new compressed archive is produced.
    """

    max_bytes: int = 10 * 1024 * 1024
    backup_count: int = 50
    max_total_bytes: int = 500 * 1024 * 1024
    debug_enabled: bool = False

    def __post_init__(self):
        for value in (self.max_bytes, self.backup_count, self.max_total_bytes):
            if type(value) is not int or value <= 0:
                raise ValueError('LOGGING_SETTING_INVALID')
        if not isinstance(self.debug_enabled, bool):
            raise ValueError('LOGGING_SETTING_INVALID')

    @classmethod
    def from_mapping(cls, value: Mapping | None) -> LoggingSettings:
        if value is None:
            return cls()
        names = {'max_bytes', 'backup_count', 'max_total_bytes', 'debug_enabled'}
        if not isinstance(value, Mapping) or set(value) - names:
            raise ValueError('LOGGING_SETTING_INVALID')
        return cls(**value)
