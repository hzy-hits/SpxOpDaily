"""Projection of the wall/flip machine into the shock monitor contract."""

from __future__ import annotations

import math
from datetime import datetime
from typing import Mapping

from spx_spark.intraday_strategy import (
    IntradayPathDecision,
    IntradayPathSignal,
    IntradayStructure,
    signed_gex_sign_method,
)
from spx_spark.marketdata import as_utc


def project_level_decision_machine(
    monitor_state: Mapping[str, object],
    level_decision: Mapping[str, object],
    structure: IntradayStructure,
    *,
    now: datetime,
    level_buffer_points: float,
) -> tuple[dict[str, object], IntradayPathDecision, tuple[IntradayPathSignal, ...]]:
    """Project the authoritative machine into the shock monitor contract."""

    state = dict(monitor_state)
    state.pop("call_strategy", None)
    phase = str(level_decision.get("phase") or "far")
    thesis = str(level_decision.get("thesis") or "none")
    direction = str(level_decision.get("direction") or "")
    level_kind = str(level_decision.get("level_kind") or "")
    level = _finite(level_decision.get("level"))
    event_id = str(level_decision.get("event_id") or "") or None
    confirmed = phase == "confirmed" and direction in {"up", "down"}
    actionable = confirmed and level_decision.get("actionable") is True
    terminal = phase in {"far", "invalidated", "expired"}
    play = (
        f"level_decision:{thesis}:{level_kind}:{direction}"
        if event_id and level is not None
        else None
    )
    invalidation = None
    if level is not None and direction == "up":
        invalidation = level - level_buffer_points
    elif level is not None and direction == "down":
        invalidation = level + level_buffer_points
    reasons = tuple(
        row
        for row in (
            str(level_decision.get("reason") or ""),
            "mutually_exclusive_level_machine",
            "legacy_intraday_wall_flip_retired",
        )
        if row
    )
    state["level_strategy"] = {
        "source": "level_decision_machine",
        "phase": phase,
        "thesis": thesis,
        "direction": direction or None,
        "level_kind": level_kind or None,
        "event_id": event_id,
        "updated_at": as_utc(now).isoformat(),
        "actionable": actionable,
        "formal_signal": actionable,
    }
    return (
        state,
        IntradayPathDecision(
            status="confirmed" if actionable else "neutral" if terminal else "watch",
            play=play,
            event_id=event_id,
            source_event_id=None,
            level=level,
            invalidation_level=invalidation,
            confirmed_at=(
                str(level_decision.get("phase_at") or "") or None
                if confirmed
                else None
            ),
            expires_at=str(level_decision.get("expires_at") or "") or None,
            gamma_state=structure.gamma_state,
            signed_gex_proxy_ratio=structure.net_gamma_ratio,
            signed_gex_sign_method=signed_gex_sign_method(structure.gex_weighting),
            dealer_position_sign="unknown",
            reasons=reasons,
            blocks=() if actionable else (f"level_phase:{phase}",),
        ),
        (),
    )


def _finite(value: object) -> float | None:
    if not isinstance(value, int | float):
        return None
    parsed = float(value)
    return parsed if math.isfinite(parsed) else None
