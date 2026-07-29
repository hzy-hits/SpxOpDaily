"""Causal, identity-matched rolling-path selection for the convexity radar."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

from spx_spark.analytics.options.pricing import finite_float


PATH_MODIFIER_MAX_AGE_SECONDS = 15.0 * 60.0
PATH_FALLBACK_SCHEMA = "spring_gamma_v3_path_fallback.v1"


def select_rolling_path_modifier(
    *,
    current: Mapping[str, Any],
    fallback: Mapping[str, Any],
    shadow: Mapping[str, Any],
    now: datetime,
) -> Mapping[str, Any]:
    """Prefer the current path, then a bounded read-only durable fallback."""

    if _current_path_usable(current, shadow=shadow, now=now):
        return current
    if _fallback_path_usable(fallback, shadow=shadow, now=now):
        return _mapping(fallback.get("rolling_path_percentiles"))
    return _unusable_path_view(current)


def _current_path_usable(
    path: Mapping[str, Any],
    *,
    shadow: Mapping[str, Any],
    now: datetime,
) -> bool:
    if not _path_rank_values_usable(path):
        return False
    observed_at = _aware_datetime(shadow.get("as_of"))
    latest_bar_end = _aware_datetime(path.get("latest_bar_end"))
    if observed_at is None or latest_bar_end is None:
        return False
    if path.get("action_authority") != "none":
        return False
    if latest_bar_end > observed_at:
        return False
    age_seconds = (_as_utc(now) - latest_bar_end).total_seconds()
    return 0 <= age_seconds <= PATH_MODIFIER_MAX_AGE_SECONDS


def _fallback_path_usable(
    fallback: Mapping[str, Any],
    *,
    shadow: Mapping[str, Any],
    now: datetime,
) -> bool:
    if fallback.get("schema_version") != PATH_FALLBACK_SCHEMA:
        return False
    if (
        fallback.get("action_authority") != "none"
        or fallback.get("actionable") is not False
        or fallback.get("automatic_ordering") is not False
    ):
        return False
    if str(fallback.get("session_id") or "") != str(shadow.get("session_id") or ""):
        return False
    if str(fallback.get("expiry") or "") != str(shadow.get("expiry") or ""):
        return False
    path = _mapping(fallback.get("rolling_path_percentiles"))
    if (
        not _path_rank_values_usable(path)
        or path.get("action_authority") != "none"
        or path.get("input_quality") != "stale_fallback"
        or path.get("confidence") != "low"
    ):
        return False
    evaluated_at = _as_utc(now)
    source_as_of = _aware_datetime(fallback.get("source_as_of"))
    latest_bar_end = _aware_datetime(
        path.get("source_latest_bar_end")
        or fallback.get("source_latest_bar_end")
        or path.get("latest_bar_end")
    )
    if source_as_of is None or latest_bar_end is None:
        return False
    if source_as_of > evaluated_at or latest_bar_end > source_as_of:
        return False
    return (
        0
        <= (evaluated_at - latest_bar_end).total_seconds()
        <= PATH_MODIFIER_MAX_AGE_SECONDS
    )


def _path_rank_values_usable(path: Mapping[str, Any]) -> bool:
    if str(path.get("status") or "") not in {"ready", "provisional"}:
        return False
    if (_number(path.get("sample_count")) or 0) < 5:
        return False
    dip = _mapping(path.get("dip"))
    rally = _mapping(path.get("rally"))
    return bool(
        _number(dip.get("shrunk_percentile")) is not None
        and _number(rally.get("shrunk_percentile")) is not None
    )


def _unusable_path_view(path: Mapping[str, Any]) -> Mapping[str, Any]:
    status = str(path.get("status") or "")
    if status not in {"ready", "provisional"}:
        return path
    return {
        "status": "unavailable",
        "reason": "rolling_path_freshness_or_identity_invalid",
        "confidence": "unavailable",
        "sample_count": 0,
        "minimum_sessions": path.get("minimum_sessions"),
        "target_sessions": path.get("target_sessions"),
        "input_quality": "unavailable",
        "action_authority": "none",
    }


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _number(value: object) -> float | None:
    return finite_float(value)


def _aware_datetime(value: object) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    return value.astimezone(timezone.utc)


__all__ = ["select_rolling_path_modifier"]
