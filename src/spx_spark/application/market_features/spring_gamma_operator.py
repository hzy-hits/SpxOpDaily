"""Bounded Spring Gamma context retained in non-authoritative research payloads."""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Mapping


MAX_MODEL_AGE_SECONDS = 150.0


def spring_gamma_operator_view(
    shadow: Mapping[str, object] | None,
    *,
    now: datetime,
    expected_expiry: str,
) -> dict[str, object]:
    """Return a fresh, non-authoritative model view for causal research joins."""

    base: dict[str, object] = {
        "status": "unavailable",
        "bias": None,
        "preferred_side": None,
        "score": None,
        "as_of": None,
        "non_blocking": True,
        "execution_authority": False,
    }
    if not isinstance(shadow, Mapping):
        return base
    if (
        shadow.get("actionable") is True
        or shadow.get("automatic_ordering") is True
        or str(shadow.get("action_authority") or "none") != "none"
    ):
        return {**base, "status": "unsafe"}
    as_of = _time(shadow.get("as_of"))
    age_seconds = (now - as_of).total_seconds() if as_of is not None else None
    if (
        age_seconds is None
        or age_seconds < -1.0
        or age_seconds > MAX_MODEL_AGE_SECONDS
        or str(shadow.get("expiry") or "") != expected_expiry
    ):
        return {**base, "status": "stale"}

    direction = shadow.get("direction")
    direction = direction if isinstance(direction, Mapping) else {}
    decision = str(direction.get("decision") or "").lower()
    score = _number(direction.get("composite_score"))
    status = str(shadow.get("status") or "").lower()
    if status == "ready" and decision in {"up", "down"}:
        return {
            **base,
            "status": "ready",
            "bias": decision,
            "preferred_side": "CALL" if decision == "up" else "PUT",
            "score": round(score, 2) if score is not None else None,
            "as_of": as_of.isoformat(),
        }
    if status == "abstain" or decision == "abstain":
        return {
            **base,
            "status": "abstain",
            "score": round(score, 2) if score is not None else None,
            "as_of": as_of.isoformat(),
        }
    return {**base, "status": "unavailable", "as_of": as_of.isoformat()}


def _number(value: object) -> float | None:
    if not isinstance(value, int | float):
        return None
    parsed = float(value)
    return parsed if math.isfinite(parsed) else None


def _time(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
