"""Fail-closed guards for independently updated decision projections."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


LEVEL_KEYS = ("put_wall", "flip_low", "flip_high", "call_wall")


def coherent_level_decision(
    decision: Mapping[str, object] | None,
    *,
    expiry: str | None,
    structure: Mapping[str, object] | None,
    max_level_drift_points: float,
) -> dict[str, object]:
    """Return a decision only when it belongs to the current option surface."""

    row = dict(decision or {})
    current_levels = _structure_levels(structure)
    previous_levels = _decision_levels(row)
    reason = _inconsistency_reason(
        row,
        expiry=expiry,
        current_levels=current_levels,
        max_level_drift_points=max_level_drift_points,
    )
    if reason is None:
        row["snapshot_consistent"] = True
        return row

    retain_frozen_context = not current_levels and bool(previous_levels)
    contextual_levels = previous_levels if retain_frozen_context else current_levels
    contextual_expiry = str(expiry or row.get("expiry") or "") or None
    return {
        **row,
        "phase": "far",
        "thesis": "none",
        "direction": None,
        "event_id": None,
        "level_kind": None,
        "level": None,
        "trigger_level": None,
        "formal_signal": False,
        "level_path_confirmed": False,
        "actionable": False,
        "quality_ok": False,
        "quality_reason": reason,
        "reason": reason,
        "expiry": contextual_expiry,
        "levels": contextual_levels,
        "level_bands": dict(row.get("level_bands") or {}) if retain_frozen_context else {},
        "level_source": (
            "frozen_last_usable_structure"
            if retain_frozen_context
            else row.get("level_source")
        ),
        "snapshot_consistent": False,
    }


def coherent_level_decision_for_frame(
    decision: Mapping[str, object] | None,
    *,
    option_frame: Mapping[str, object],
    fallback_expiry: str | None,
    max_level_drift_points: float,
) -> dict[str, object]:
    structure = option_frame.get("structure")
    return coherent_level_decision(
        decision,
        expiry=str(option_frame.get("front_expiry") or fallback_expiry or "") or None,
        structure=structure if isinstance(structure, Mapping) else {},
        max_level_drift_points=max_level_drift_points,
    )


def coherent_decision_context(
    context: Mapping[str, object] | None,
    *,
    market_frame: Mapping[str, object],
    option_frame: Mapping[str, object],
) -> dict[str, object]:
    if decision_context_matches_frames(
        context,
        market_frame=market_frame,
        option_frame=option_frame,
    ):
        return dict(context or {})
    return {
        "market_frame_id": market_frame.get("frame_id"),
        "option_frame_id": option_frame.get("frame_id"),
        "invalidations": ["decision_projection_mismatch"],
        "regime_decision": {},
        "breakout_filter": {},
        "trade_intent": {
            "status": "observing",
            "block_reasons": ["decision_projection_mismatch"],
        },
    }


def apply_decision_projections(
    payload: dict[str, Any],
    *,
    level_decision: Mapping[str, object] | None,
    market_frame: Mapping[str, object],
    option_frame: Mapping[str, object],
    decision_context: Mapping[str, object] | None,
    max_level_drift_points: float,
) -> None:
    """Attach one coherent projection generation to an order-map payload."""

    decision = coherent_level_decision_for_frame(
        level_decision,
        option_frame=option_frame,
        fallback_expiry=str(payload.get("expiry") or "") or None,
        max_level_drift_points=max_level_drift_points,
    )
    payload["level_decision"] = decision
    if payload.get("research_only") is True:
        spot = _number(decision.get("spot"))
        source = decision.get("spot_source")
        if spot is not None and isinstance(source, str):
            payload["context_reference"] = {
                "price": spot,
                "source": source,
                "executable": False,
            }

    context = coherent_decision_context(
        decision_context,
        market_frame=market_frame,
        option_frame=option_frame,
    )
    payload["decision_context"] = context
    payload["regime_decision"] = context.get("regime_decision", {})
    payload["breakout_filter"] = context.get("breakout_filter", {})
    payload["trade_intent"] = context.get("trade_intent", {})


def decision_context_matches_frames(
    context: Mapping[str, object] | None,
    *,
    market_frame: Mapping[str, object] | None,
    option_frame: Mapping[str, object] | None,
) -> bool:
    """Require one context generation to reference the projections being rendered."""

    if not isinstance(context, Mapping):
        return False
    market_id = str((market_frame or {}).get("frame_id") or "")
    option_id = str((option_frame or {}).get("frame_id") or "")
    return bool(
        market_id
        and option_id
        and str(context.get("market_frame_id") or "") == market_id
        and str(context.get("option_frame_id") or "") == option_id
    )


def _inconsistency_reason(
    decision: Mapping[str, object],
    *,
    expiry: str | None,
    current_levels: Mapping[str, float],
    max_level_drift_points: float,
) -> str | None:
    decision_expiry = str(decision.get("expiry") or "")
    current_expiry = str(expiry or "")
    if current_expiry and decision_expiry != current_expiry:
        return "decision_snapshot_expiry_mismatch"
    if not current_levels and _decision_levels(decision):
        return "decision_snapshot_structure_unavailable"

    phase = str(decision.get("phase") or "far")
    if phase == "far":
        return None
    kind = str(decision.get("level_kind") or "")
    level = _number(decision.get("level"))
    current_level = current_levels.get(kind)
    if not kind or level is None or current_level is None:
        return "decision_snapshot_level_unavailable"
    if abs(level - current_level) > max_level_drift_points:
        return "decision_snapshot_level_drift"
    return None


def _structure_levels(structure: Mapping[str, object] | None) -> dict[str, float]:
    if not isinstance(structure, Mapping):
        return {}
    result: dict[str, float] = {}
    for key in ("put_wall", "call_wall"):
        value = _number(structure.get(key))
        if value is not None:
            result[key] = value
    flip = structure.get("flip_zone")
    if isinstance(flip, list | tuple) and len(flip) >= 2:
        values = [value for value in (_number(flip[0]), _number(flip[1])) if value is not None]
        if len(values) == 2:
            low, high = sorted(values)
            result["flip_low"] = low
            result["flip_high"] = high
    for key in LEVEL_KEYS:
        value = _number(structure.get(key))
        if value is not None:
            result[key] = value
    return result


def _decision_levels(decision: Mapping[str, object]) -> dict[str, float]:
    levels = decision.get("levels")
    if not isinstance(levels, Mapping):
        return {}
    return {
        key: float(value)
        for key in LEVEL_KEYS
        if isinstance((value := levels.get(key)), int | float)
    }


def _number(value: object) -> float | None:
    return float(value) if isinstance(value, int | float) else None
