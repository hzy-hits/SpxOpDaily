"""Intraday call-bias loading for conditional order-map plays."""

from __future__ import annotations

from datetime import datetime, timezone

from spx_spark.application.order_map.level_decision_shadow import load_level_decision_shadow
from spx_spark.application.order_map.models import level_decision_play
from spx_spark.application.market_features.state import load_json, projection_paths
from spx_spark.config import StorageSettings
from spx_spark.settings import load_app_settings


def load_intraday_call_bias(*, now: datetime) -> dict[str, object] | None:
    """Adapt an explicitly promoted level decision into an order-map bias."""

    storage = StorageSettings.from_env()
    decision = load_level_decision_shadow(storage)
    if decision.get("phase") != "confirmed" or decision.get("actionable") is not True:
        return None
    thesis = str(decision.get("thesis") or "")
    direction = str(decision.get("direction") or "")
    play = level_decision_play(thesis, direction)
    if thesis == "breakout" and not _breakout_filter_allows(
        storage,
        event_id=str(decision.get("event_id") or ""),
    ):
        return None
    level = _finite(decision.get("level"))
    expiry = str(decision.get("expiry") or "")
    expires_at = _parse_datetime(decision.get("expires_at"))
    if play is None or level is None or not expiry or expires_at is None:
        return None
    now_utc = _utc(now)
    if now_utc > expires_at:
        return None
    buffer_points = load_app_settings().level_decision.break_buffer_points
    invalidation = level - buffer_points if direction == "up" else level + buffer_points
    return {
        "status": "confirmed",
        "formal_signal": True,
        "actionable": True,
        "play": play,
        "thesis": thesis,
        "direction": direction,
        "level_kind": decision.get("level_kind"),
        "level": level,
        "invalidation_level": invalidation,
        "expiry": expiry,
        "event_id": decision.get("event_id"),
        "confirmed_at": decision.get("phase_at"),
        "expires_at": expires_at.isoformat(),
    }


def _breakout_filter_allows(storage: StorageSettings, *, event_id: str) -> bool:
    context = load_json(projection_paths(storage.data_root)["decision"])
    breakout_filter = context.get("breakout_filter")
    return bool(
        isinstance(breakout_filter, dict)
        and breakout_filter.get("event_id") == event_id
        and breakout_filter.get("verdict") == "supported"
        and breakout_filter.get("actionable") is True
    )


def _finite(value: object) -> float | None:
    if not isinstance(value, int | float):
        return None
    parsed = float(value)
    return parsed if parsed == parsed else None


def _parse_datetime(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return _utc(parsed.replace(tzinfo=parsed.tzinfo or timezone.utc))


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("bias timestamp must be timezone-aware")
    return value.astimezone(timezone.utc)
