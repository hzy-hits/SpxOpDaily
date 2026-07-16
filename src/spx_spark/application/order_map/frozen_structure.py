"""Fail-closed presentation of the last same-expiry option structure."""

from __future__ import annotations

from typing import Any

from spx_spark.analytics.options.pricing import finite_float


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
