"""Resolve the deployment's system timezone once, retaining its calendar rules."""

from functools import lru_cache
import logging
import os
from zoneinfo import ZoneInfo

import tzlocal
from tzlocal import get_localzone

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def system_timezone() -> ZoneInfo:
    """Use the system zone, falling back once to Shanghai if detection fails."""
    try:
        configured = os.environ.get('TZ')
        if configured:
            name = configured.removeprefix(':')
            if os.path.isabs(name):
                name = get_localzone().key
        else:
            # Older tzlocal releases do not expose the optional name API.
            reader = getattr(tzlocal, 'get_localzone_name', None)
            name = reader() if reader is not None else get_localzone().key
        if not name:
            raise ValueError('No system timezone name')
        return ZoneInfo(name)
    except Exception as error:
        # Detection is best-effort; do not log raw environment or file contents.
        logger.warning('SYSTEM_TIMEZONE_FALLBACK: timezone=Asia/Shanghai error_type=%s',
                       type(error).__name__)
        return ZoneInfo('Asia/Shanghai')


def system_timezone_name() -> str:
    return system_timezone().key
