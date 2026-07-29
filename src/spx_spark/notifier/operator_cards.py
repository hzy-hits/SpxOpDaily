"""Small, deterministic helpers for human-facing execution cards."""

from __future__ import annotations

import math
import re
from datetime import datetime, timezone
from typing import Mapping
from zoneinfo import ZoneInfo


BEIJING = ZoneInfo("Asia/Shanghai")
_OPTION_ID_RE = re.compile(
    r"(?:^|:)SPXW:(?P<expiry>\d{8}):(?P<strike>\d+(?:\.\d+)?):(?P<right>[CP])$",
    re.IGNORECASE,
)


def option_contract_label(
    contract_id: object,
    *,
    fallback: object = None,
) -> str:
    """Return an exact-expiry operator label from a canonical SPXW id."""

    match = _OPTION_ID_RE.search(str(contract_id or ""))
    if match is None:
        return str(fallback or "SPXW 合约（到期日缺失）")
    expiry = match.group("expiry")
    strike = float(match.group("strike"))
    return (
        f"SPXW {expiry[4:6]}-{expiry[6:8]} "
        f"{strike:g}{match.group('right').upper()}"
    )


def option_contract_right(contract_id: object) -> str | None:
    match = _OPTION_ID_RE.search(str(contract_id or ""))
    return match.group("right").upper() if match is not None else None


def beijing_time(value: object, *, seconds: bool = False) -> str:
    parsed = parse_time(value)
    if parsed is None:
        return "-"
    pattern = "%H:%M:%S" if seconds else "%H:%M"
    return parsed.astimezone(BEIJING).strftime(pattern) + " 北京时间"


def remaining_seconds(expires_at: object, *, now: datetime) -> int | None:
    parsed = parse_time(expires_at)
    if parsed is None:
        return None
    remaining = (parsed - as_utc(now)).total_seconds()
    return max(int(math.floor(remaining)), 0)


def parse_time(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return as_utc(value)
    if not isinstance(value, str) or not value:
        return None
    try:
        return as_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
    except ValueError:
        return None


def as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def decision_now(payload: Mapping[str, object]) -> datetime:
    for field in ("evaluated_at", "quote_source_at", "occurred_at"):
        parsed = parse_time(payload.get(field))
        if parsed is not None:
            return parsed
    return datetime.now(tz=timezone.utc)
