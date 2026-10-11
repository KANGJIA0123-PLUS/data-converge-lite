"""Strict codec for the producer's five-minute DWD readiness signals."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

DWD_TYPES = frozenset({"GPU_REALTIME", "NODE_REALTIME", "REQUEST_REALTIME",
                       "MODELINSTANCE_REALTIME", "MODELMETRIC_REALTIME"})
SHANGHAI = ZoneInfo("Asia/Shanghai")


class ReadyMessageError(ValueError):
    """A rejected message; diagnostics never contain its raw payload."""


@dataclass(frozen=True, slots=True)
class ReadyMessage:
    window_start: datetime
    data_type: str


def parse_message(raw: str | bytes) -> ReadyMessage:
    """Require exactly YYYY-MM-DDTHH:mm:TYPE, with no offsets or seconds."""
    try:
        if not isinstance(raw, (str, bytes)) or not 0 < len(raw) <= 64:
            raise ValueError
        text = raw.decode("utf-8") if isinstance(raw, bytes) else raw
        timestamp, data_type = text.rsplit(":", 1)
        if data_type not in DWD_TYPES or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}", timestamp):
            raise ValueError
        start = datetime.fromisoformat(timestamp).replace(tzinfo=SHANGHAI)
        if start.minute % 5:
            raise ValueError
        return ReadyMessage(start, data_type)
    except (ValueError, TypeError, UnicodeError):
        raise ReadyMessageError("DWD_MESSAGE_INVALID") from None


def encode_message(window_start: datetime, data_type: str) -> str:
    """Format an aware, aligned timestamp in the producer's Shanghai timezone."""
    try:
        if not isinstance(window_start, datetime) or window_start.utcoffset() is None:
            raise ValueError
        start = window_start.astimezone(SHANGHAI)
        if start.second or start.microsecond or start.minute % 5:
            raise ValueError
        text = f"{start.year:04d}-{start.month:02d}-{start.day:02d}T{start.hour:02d}:{start.minute:02d}:{data_type}"
        parse_message(text)
        return text
    except (ValueError, TypeError, OverflowError):
        raise ReadyMessageError("DWD_MESSAGE_INVALID") from None
