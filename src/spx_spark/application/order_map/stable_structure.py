"""Fifteen-minute hysteresis for OI/GEX wall structure.

Raw chain analytics remain available for audit. This module owns the stable
decision surface so a small OI/GEX reshuffle cannot move an active wall on
every realtime tick.
"""

from __future__ import annotations

from datetime import datetime, timezone
from statistics import median
from typing import Mapping


LEVEL_NAMES = ("put_wall", "flip_low", "flip_high", "call_wall")


def advance_stable_structure(
    previous: Mapping[str, object] | None,
    live: Mapping[str, object] | None,
    *,
    now: datetime,
    interval_seconds: int,
    required_confirmations: int,
    band_half_width_points: float,
    switch_min_points: float,
) -> tuple[dict[str, object], dict[str, object] | None]:
    """Return durable stability metadata and the promoted structure."""

    now = _utc(now)
    state = dict(previous or {})
    stable = _mapping(state.get("stable"))
    if stable and not stable.get("promoted_at"):
        stable = _promoted(stable, now=now, band=band_half_width_points)
        state["stable"] = stable
    if not live or not _levels(live):
        return _public_state(state, stable, now=now, band=band_half_width_points), stable

    live_row = dict(live)
    live_levels = _levels(live_row)
    bucket = int(now.timestamp()) // interval_seconds
    if not stable:
        stable = _promoted(live_row, now=now, band=band_half_width_points)
        state = {
            "schema_version": 1,
            "stable": stable,
            "candidate": None,
            "last_bucket": bucket,
            "promotion_reason": "bootstrap_first_usable_structure",
        }
        return _public_state(state, stable, now=now, band=band_half_width_points), stable

    stable_levels = _levels(stable)
    if not _materially_different(stable_levels, live_levels, threshold=switch_min_points):
        stable["last_confirmed_at"] = now.isoformat()
        stable["duration_seconds"] = max(
            (now - (_datetime(stable.get("promoted_at")) or now)).total_seconds(), 0.0
        )
        stable["confirmation_count"] = int(stable.get("confirmation_count") or 1) + (
            1 if state.get("last_bucket") != bucket else 0
        )
        state.update({"stable": stable, "candidate": None, "last_bucket": bucket})
        return _public_state(state, stable, now=now, band=band_half_width_points), stable

    candidate = _mapping(state.get("candidate"))
    samples = [dict(item) for item in candidate.get("samples") or [] if isinstance(item, Mapping)]
    candidate_levels = _levels(candidate)
    if not candidate_levels or _materially_different(
        candidate_levels, live_levels, threshold=band_half_width_points
    ):
        samples = []
    if not samples or int(samples[-1].get("bucket") or -1) != bucket:
        samples.append({"bucket": bucket, "levels": live_levels, "at": now.isoformat()})
    samples = samples[-required_confirmations:]
    candidate = {
        **live_row,
        "levels": _median_levels(samples),
        "samples": samples,
        "confirmation_count": len(samples),
        "required_confirmations": required_confirmations,
        "first_seen_at": samples[0]["at"],
        "last_seen_at": samples[-1]["at"],
    }
    if len(samples) >= required_confirmations:
        stable = _promoted(candidate, now=now, band=band_half_width_points)
        state.update(
            {
                "stable": stable,
                "candidate": None,
                "last_bucket": bucket,
                "promotion_reason": "multi_bucket_confirmation",
            }
        )
    else:
        state.update({"candidate": candidate, "last_bucket": bucket})
    return _public_state(state, stable, now=now, band=band_half_width_points), stable


def _public_state(
    state: Mapping[str, object],
    stable: Mapping[str, object] | None,
    *,
    now: datetime,
    band: float,
) -> dict[str, object]:
    result = dict(state)
    if stable:
        stable_row = dict(stable)
        stable_row["level_bands"] = _bands(_levels(stable_row), band)
        promoted = _datetime(stable_row.get("promoted_at"))
        stable_row["duration_seconds"] = max((now - (promoted or now)).total_seconds(), 0.0)
        result["stable"] = stable_row
    result["updated_at"] = now.isoformat()
    return result


def _promoted(source: Mapping[str, object], *, now: datetime, band: float) -> dict[str, object]:
    levels = _levels(source)
    return {
        key: value
        for key, value in dict(source).items()
        if key not in {"samples", "level_bands"}
    } | {
        "levels": levels,
        "level_bands": _bands(levels, band),
        "promoted_at": now.isoformat(),
        "last_confirmed_at": now.isoformat(),
        "confirmation_count": int(source.get("confirmation_count") or 1),
        "duration_seconds": 0.0,
        "source": "stable_15m_oi_gex",
    }


def _materially_different(
    left: Mapping[str, float], right: Mapping[str, float], *, threshold: float
) -> bool:
    common = set(left) & set(right)
    if not common or set(left) != set(right):
        return True
    return any(abs(left[name] - right[name]) >= threshold for name in common)


def _median_levels(samples: list[dict[str, object]]) -> dict[str, float]:
    result: dict[str, float] = {}
    for name in LEVEL_NAMES:
        values: list[float] = []
        for row in samples:
            levels = row.get("levels")
            if isinstance(levels, Mapping) and isinstance(levels.get(name), int | float):
                values.append(float(levels[name]))
        if values:
            result[name] = float(median(values))
    return result


def _levels(value: Mapping[str, object] | None) -> dict[str, float]:
    raw = (value or {}).get("levels")
    if not isinstance(raw, Mapping):
        return {}
    return {
        name: float(raw[name])
        for name in LEVEL_NAMES
        if isinstance(raw.get(name), int | float)
    }


def _bands(levels: Mapping[str, float], half_width: float) -> dict[str, dict[str, float]]:
    return {
        name: {
            "center": round(value, 2),
            "low": round(value - half_width, 2),
            "high": round(value + half_width, 2),
        }
        for name, value in levels.items()
    }


def _mapping(value: object) -> dict[str, object]:
    return dict(value) if isinstance(value, Mapping) else {}


def _datetime(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return _utc(parsed.replace(tzinfo=parsed.tzinfo or timezone.utc))


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("stable structure timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)
