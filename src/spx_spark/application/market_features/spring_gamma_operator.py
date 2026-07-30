"""Bounded Spring Gamma context for human Gamma-plan cards."""

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
    """Return a fresh, non-authoritative model view for one operator card."""

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


def spring_gamma_operator_line(
    view: Mapping[str, object] | None,
    *,
    ticket_side: str | None = None,
) -> str:
    """Render one short model line without exposing internal gate enums."""

    model = view if isinstance(view, Mapping) else {}
    if model.get("status") == "ready":
        preferred = str(model.get("preferred_side") or "")
        bias = "偏多" if preferred == "CALL" else "偏空"
        score = _number(model.get("score"))
        score_text = f"（{score:.2f}）" if score is not None else ""
        if ticket_side in {"CALL", "PUT"}:
            relation = "与本卡同向" if ticket_side == preferred else "与本卡背离"
        else:
            relation = f"{preferred} 路径优先"
        return f"模型  Spring Gamma {bias}{score_text} · {relation} · 只作排序，不作门禁"
    if model.get("status") == "abstain":
        return "模型  Spring Gamma 暂无方向 · 不改变 Gamma 触发"
    return "模型  Spring Gamma 数据未就绪 · 不阻断 Gamma 计划"


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
