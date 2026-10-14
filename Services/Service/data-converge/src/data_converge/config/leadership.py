"""Validated lease policy; Redis authentication stays in the existing adapter."""
from dataclasses import dataclass, fields
import re


@dataclass(frozen=True)
class LeadershipSettings:
    enabled: bool = False
    cluster_name: str = 'data-converge-modelmate-metric'
    ttl_seconds: int = 30
    renew_interval_seconds: int = 5
    standby_poll_seconds: int = 2

    @classmethod
    def from_config(cls, config):
        values = config.get('leadership', {})
        names = {item.name for item in fields(cls)}
        if not isinstance(values, dict) or values.keys() - names:
            raise ValueError('LEADERSHIP_CONFIG_INVALID')
        result = cls(**values)
        result.validate(config['dwd_ready'])
        return result

    def validate(self, ready):
        if not isinstance(self.enabled, bool):
            raise ValueError('LEADERSHIP_CONFIG_INVALID')
        if not isinstance(self.cluster_name, str) or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,100}', self.cluster_name):
            raise ValueError('LEADERSHIP_CONFIG_INVALID')
        for value in (self.ttl_seconds, self.renew_interval_seconds, self.standby_poll_seconds):
            if type(value) is not int or not 1 <= value <= 300:
                raise ValueError('LEADERSHIP_CONFIG_INVALID')
        if self.renew_interval_seconds * 3 >= self.ttl_seconds:
            raise ValueError('LEADERSHIP_CONFIG_INVALID')
        if self.enabled and (not ready['enabled'] or ready['redis']['mode'] == 'cluster'):
            raise ValueError('LEADERSHIP_REQUIRES_STANDALONE_OR_SENTINEL')
