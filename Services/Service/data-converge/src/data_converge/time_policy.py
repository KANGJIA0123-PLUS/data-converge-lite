"""Resolve the deployment's system timezone once, retaining its calendar rules."""

from functools import lru_cache
from zoneinfo import ZoneInfo

from tzlocal import get_localzone


@lru_cache(maxsize=1)
def system_timezone() -> ZoneInfo:
    """Use TZ or operating-system zone data; never freeze today's UTC offset."""
    try:
        # Resolve the name again so downstream SQL can use the same IANA zone.
        # An anonymous /etc/localtime requires the deployment to provide TZ.
        return ZoneInfo(get_localzone().key)
    except (AttributeError, KeyError, OSError, ValueError):
        raise ValueError('SYSTEM_TIMEZONE_UNRESOLVED') from None


def system_timezone_name() -> str:
    return system_timezone().key
