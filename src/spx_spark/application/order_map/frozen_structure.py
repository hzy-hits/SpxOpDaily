"""Fail-closed presentation of the last same-expiry option structure."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping

from spx_spark.analytics.options.pricing import finite_float
from spx_spark.marketdata import FUTURE_TIMESTAMP_TOLERANCE_SECONDS, as_utc


def option_structure_frame_is_live(payload: Mapping[str, Any], *, now: datetime) -> bool:
    """Require typed frame quality and bounded age before labeling structure live."""

    frame = _mapping(payload.get("option_structure_frame"))
    structure = _mapping(frame.get("structure"))
    l1 = _mapping(frame.get("l1"))
    if (
        str(frame.get("quality") or "").lower() != "ready"
        or str(l1.get("quality") or "").lower() != "ready"
        or structure.get("frozen") is True
    ):
        return False
    frame_as_of = _aware_datetime(frame.get("as_of"))
    max_age_seconds = finite_float(_mapping(frame.get("diagnostics")).get("max_quote_age_seconds"))
    if frame_as_of is None or max_age_seconds is None or max_age_seconds <= 0:
        return False
    age_seconds = (as_utc(now) - frame_as_of).total_seconds()
    return -FUTURE_TIMESTAMP_TOLERANCE_SECONDS <= age_seconds <= max_age_seconds


def attach_frozen_option_structure(
    payload: dict[str, Any],
    option_frame: dict[str, Any],
) -> None:
    structure = (
        option_frame.get("structure") if isinstance(option_frame.get("structure"), dict) else {}
    )
    if structure.get("frozen") is not True:
        return
    payload["frozen_option_structure"] = {
        "source": structure.get("source"),
        "as_of": structure.get("frozen_as_of"),
        "expiry": option_frame.get("front_expiry"),
    }
    for key in ("gamma_state", "zero_gamma", "flip_zone", "max_pain"):
        current = payload.get(key)
        if current is None or current == "unknown":
            payload[key] = structure.get(key)
    current_ladder = (
        payload.get("wall_ladder") if isinstance(payload.get("wall_ladder"), dict) else {}
    )
    if any(current_ladder.get(key) for key in ("put_walls", "call_walls")):
        return
    payload["wall_ladder"] = {
        "put_walls": _frozen_wall_rungs(structure.get("put_walls"), right="C"),
        "call_walls": _frozen_wall_rungs(structure.get("call_walls"), right="P"),
    }


def _frozen_wall_rungs(value: object, *, right: str) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    rows: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        strike = finite_float(item.get("strike"))
        if strike is None:
            continue
        rows.append(
            {
                "strike": strike,
                "gex": finite_float(item.get("gex")),
                "open_interest": finite_float(item.get("open_interest")),
                "volume": finite_float(item.get("volume")),
                "option_strike": int(round(strike)),
                "option_right": right,
                "current_mid": None,
                "projected_mid": None,
                "limit_aggressive": None,
                "limit_conservative": None,
                "degraded": True,
                "frozen": True,
            }
        )
    return rows


def _aware_datetime(value: object) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return as_utc(parsed)


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}
