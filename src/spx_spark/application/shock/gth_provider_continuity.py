"""Provider-switch hysteresis for one continuous GTH price coordinate."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Mapping


def evaluate_provider_switch(
    raw_candidate: object,
    *,
    active_provider: str,
    incoming_provider: str,
    at: datetime,
    hold_seconds: int,
) -> dict[str, object]:
    """Require one alternate provider to persist before changing coordinates."""

    now = _utc(at)
    if not active_provider or active_provider == incoming_provider:
        return {"status": "stable", "switch": False, "candidate": None}
    candidate = raw_candidate if isinstance(raw_candidate, Mapping) else {}
    first_seen_at = _time(candidate.get("first_seen_at"))
    if (
        str(candidate.get("provider") or "") != incoming_provider
        or first_seen_at is None
        or first_seen_at > now
    ):
        first_seen_at = now
        sample_count = 1
    else:
        sample_count = int(candidate.get("sample_count") or 0) + 1
    held_seconds = max(0.0, (now - first_seen_at).total_seconds())
    next_candidate = {
        "provider": incoming_provider,
        "first_seen_at": first_seen_at.isoformat(),
        "last_seen_at": now.isoformat(),
        "sample_count": sample_count,
        "held_seconds": held_seconds,
    }
    switch = held_seconds >= max(0, hold_seconds)
    return {
        "status": "switch" if switch else "holding",
        "switch": switch,
        "candidate": None if switch else next_candidate,
        "held_seconds": held_seconds,
    }


def _time(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timezone-aware datetime required")
    return value.astimezone(timezone.utc)


__all__ = ["evaluate_provider_switch"]
