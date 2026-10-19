"""Archive policy is operational configuration, separate from task identity."""

from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True)
class ArchiveSettings:
    enabled: bool = True
    retention_days: int = 30

    def __post_init__(self):
        if (type(self.enabled) is not bool or type(self.retention_days) is not int
                or self.retention_days < 1):
            raise ValueError('ARCHIVE_SETTINGS_INVALID')

    @classmethod
    def from_mapping(cls, value):
        if value is None:
            return cls()
        if not isinstance(value, Mapping) or set(value) - {'enabled', 'retention_days'}:
            raise ValueError('ARCHIVE_SETTINGS_INVALID')
        return cls(**value)
