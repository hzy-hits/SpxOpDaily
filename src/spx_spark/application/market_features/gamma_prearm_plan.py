"""Event-driven, two-sided preparation cards for approaching Gamma levels."""

from __future__ import annotations

import hashlib
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

from spx_spark.application.market_features.spring_gamma_operator import (
    spring_gamma_operator_view,
)
from spx_spark.application.market_features.prior_rth_context import (
    prior_session_signal_view,
)
from spx_spark.config import StorageSettings
from spx_spark.state_io import (
    atomic_write_json_secure,
    exclusive_state_lock,
    read_json_object,
)


CONTRACT_VERSION = "gamma_prearm_plan.v1"
REPRICING_MAX_AGE_SECONDS = 45.0
PREPARATION_PHASES = frozenset({"approaching", "break_pending", "reject_pending"})


def evaluate_gamma_prearm_plan(
    repricing: Mapping[str, object],
    level_decision: Mapping[str, object],
    *,
    now: datetime,
    spring_gamma: Mapping[str, object] | None = None,
    prior_session: Mapping[str, object] | None = None,
    gth_position_fraction: float | None = None,
    invalidation_buffer_points: float = 3.0,
) -> dict[str, object]:
    """Build one conditional plan before price reaches a frozen Gamma level."""

    now = _utc(now)
    phase = str(level_decision.get("phase") or "")
    source_event_id = str(level_decision.get("event_id") or "")
    generation = _generation(level_decision)
    base: dict[str, object] = {
        "schema_version": 1,
        "contract_version": CONTRACT_VERSION,
        "kind": "gamma_level_prearm_plan",
        "status": "inactive",
        "plan_id": None,
        "source_event_id": source_event_id or None,
        "reentry_generation": generation,
        "evaluated_at": now.isoformat(),
        "execution_eligible": False,
        "automatic_ordering": False,
        "broker_submission_allowed": False,
        "operator_action": "prepare_only",
        "block_reasons": [],
    }
    if phase not in PREPARATION_PHASES:
        return {**base, "block_reasons": ["level_not_approaching"]}
    if repricing.get("status") != "repriced" or repricing.get("phase") != phase:
        reason = (
            "approach_repricing_unavailable"
            if phase == "approaching"
            else "preparation_repricing_unavailable"
        )
        return {**base, "status": "blocked", "block_reasons": [reason]}
    if str(repricing.get("event_id") or "") != source_event_id:
        reason = (
            "approach_event_mismatch"
            if phase == "approaching"
            else "preparation_event_mismatch"
        )
        return {**base, "status": "blocked", "block_reasons": [reason]}
    repriced_at = _time(repricing.get("as_of"))
    age_seconds = (now - repriced_at).total_seconds() if repriced_at is not None else None
    if (
        age_seconds is None
        or age_seconds < -1.0
        or age_seconds > REPRICING_MAX_AGE_SECONDS
    ):
        reason = (
            "approach_repricing_stale"
            if phase == "approaching"
            else "preparation_repricing_stale"
        )
        return {**base, "status": "blocked", "block_reasons": [reason]}

    active_play = _active_play(level_decision) if phase != "approaching" else None
    candidates = [
        _plan_path(
            item,
            level=_number(repricing.get("spx_level")),
            invalidation_buffer_points=invalidation_buffer_points,
            geometry=(
                repricing.get("path_geometries", {}).get(str(item.get("play") or ""))
                if isinstance(repricing.get("path_geometries"), Mapping)
                else None
            ),
        )
        for item in repricing.get("candidates") or []
        if isinstance(item, Mapping)
        and item.get("execution_quote_status") == "executable"
        and (active_play is None or item.get("play") == active_play)
    ]
    paths = sorted(
        (item for item in candidates if item is not None),
        key=lambda item: (str(item["side"]), str(item["play"])),
    )
    if not paths:
        return {**base, "status": "blocked", "block_reasons": ["prearm_quote_unavailable"]}

    level = _number(repricing.get("spx_level"))
    spot = _number(repricing.get("pricing_spot"))
    expiry = str(repricing.get("expiry") or "")
    level_kind = str(repricing.get("level_kind") or "")
    if level is None or spot is None or len(expiry) != 8 or not level_kind:
        return {**base, "status": "blocked", "block_reasons": ["prearm_coordinate_incomplete"]}

    identity = "|".join(
        (
            CONTRACT_VERSION,
            expiry,
            level_kind,
            f"{level:.4f}",
        )
    )
    plan_id = "gamma-prearm:" + hashlib.sha256(identity.encode()).hexdigest()[:24]
    spring_gamma_view = spring_gamma_operator_view(
        spring_gamma,
        now=now,
        expected_expiry=expiry,
    )
    paths = [
        {
            **item,
            "prior_session_chase_risk": prior_session_signal_view(
                prior_session,
                direction="up" if item["side"] == "CALL" else "down",
                gth_position_fraction=gth_position_fraction,
            ).get("chase_risk"),
        }
        for item in paths
    ]
    prior_session_view = prior_session_signal_view(
        prior_session,
        gth_position_fraction=gth_position_fraction,
    )
    gamma_context = repricing.get("gamma_context")
    return {
        **base,
        "status": "prearm_ready",
        "notification_stage": phase,
        "plan_id": plan_id,
        "level_kind": level_kind,
        "level": level,
        "current_spx": spot,
        "distance_points": round(abs(spot - level), 2),
        "expiry": expiry,
        "paths": paths,
        "trigger_coordinate": (
            dict(repricing.get("trigger_coordinate"))
            if isinstance(repricing.get("trigger_coordinate"), Mapping)
            else {}
        ),
        "touch_time_estimate": (
            dict(repricing.get("touch_time_estimate"))
            if isinstance(repricing.get("touch_time_estimate"), Mapping)
            else {}
        ),
        "spring_gamma": spring_gamma_view,
        "gamma_context": (
            dict(gamma_context) if isinstance(gamma_context, Mapping) else {}
        ),
        "prior_session": prior_session_view,
        "block_reasons": [],
    }


def process_gamma_prearm_plan(
    storage: StorageSettings,
    repricing: Mapping[str, object],
    level_decision: Mapping[str, object],
    *,
    now: datetime,
    spring_gamma: Mapping[str, object] | None = None,
    prior_session: Mapping[str, object] | None = None,
    gth_position_fraction: float | None = None,
    invalidation_buffer_points: float = 3.0,
) -> dict[str, object]:
    """Persist a Gamma preparation plan for Desk Map/research only."""

    now = _utc(now)
    plan = evaluate_gamma_prearm_plan(
        repricing,
        level_decision,
        now=now,
        spring_gamma=spring_gamma,
        prior_session=prior_session,
        gth_position_fraction=gth_position_fraction,
        invalidation_buffer_points=invalidation_buffer_points,
    )
    state_path = Path(storage.data_root) / "latest" / "gamma_prearm_plan_state.json"
    projection_path = Path(storage.data_root) / "latest" / "gamma_prearm_plan.json"
    with exclusive_state_lock(state_path):
        state = read_json_object(state_path)
        accepted = {
            str(item)
            for item in state.get("accepted_notification_event_ids") or []
            if item
        }
        settled = {
            str(item)
            for item in state.get("settled_notification_event_ids") or []
            if item
        }
        state.update(
            {
                "schema_version": 1,
                "updated_at": now.isoformat(),
                "last_plan": dict(plan),
                "accepted_notification_event_ids": sorted(accepted)[-200:],
                "settled_notification_event_ids": sorted(settled)[-200:],
                # Retire any intent left by the former human pre-arm lane.
                "pending_notifications": [],
            }
        )
        atomic_write_json_secure(state_path, state)
        atomic_write_json_secure(projection_path, plan)
    return {
        **plan,
        "notification_attempted": False,
        "notification_accepted": False,
        "notification_outcome": "unified_strategy_decision_owned",
    }


def _plan_path(
    candidate: Mapping[str, object],
    *,
    level: float | None,
    invalidation_buffer_points: float,
    geometry: object = None,
) -> dict[str, object] | None:
    play = str(candidate.get("play") or "")
    contract_id = str(candidate.get("contract_id") or "")
    side = str(candidate.get("right") or "")
    if play not in {
        "level_breakout_call",
        "level_breakout_put",
        "level_fade_call",
        "level_fade_put",
    } or not contract_id or side not in {"C", "P"}:
        return None
    return {
        "play": play,
        "side": "CALL" if side == "C" else "PUT",
        "contract_id": contract_id,
        "condition": _condition(play),
        "decision_bid": _number(candidate.get("execution_bid")),
        "decision_ask": _number(candidate.get("execution_ask")),
        "projected_low": _number(candidate.get("projection_range_low")),
        "projected_mid": _number(candidate.get("projected_mid")),
        "projected_high": _number(candidate.get("projection_range_high")),
        "limit_conservative": _number(candidate.get("limit_conservative")),
        "limit_aggressive": _number(candidate.get("limit_aggressive")),
        "frontrun_level": _number(candidate.get("frontrun_level")),
        "frontrun_limit": _number(candidate.get("frontrun_limit")),
        "touch_eta_minutes": _number(candidate.get("touch_eta_minutes")),
        "quote_provider": candidate.get("execution_quote_provider"),
        "invalidation_spx": (
            round(
                level - invalidation_buffer_points
                if side == "C"
                else level + invalidation_buffer_points,
                2,
            )
            if level is not None
            else None
        ),
        "confirmation_geometry": dict(geometry) if isinstance(geometry, Mapping) else None,
    }


def _generation(value: Mapping[str, object]) -> int:
    generation = value.get("reentry_generation", 0)
    if isinstance(generation, int) and not isinstance(generation, bool):
        return max(generation, 0)
    return 0


def _condition(play: str) -> str:
    return {
        "level_breakout_call": "向上接受并站稳",
        "level_breakout_put": "向下接受并保持",
        "level_fade_call": "下沿拒绝并收复",
        "level_fade_put": "上沿拒绝并回落",
    }[play]


def _active_play(level_decision: Mapping[str, object]) -> str | None:
    thesis = str(level_decision.get("thesis") or "")
    direction = str(level_decision.get("direction") or "")
    return {
        ("breakout", "up"): "level_breakout_call",
        ("breakout", "down"): "level_breakout_put",
        ("fade", "up"): "level_fade_call",
        ("fade", "down"): "level_fade_put",
    }.get((thesis, direction))


def _number(value: object) -> float | None:
    if not isinstance(value, int | float):
        return None
    parsed = float(value)
    return parsed if math.isfinite(parsed) and parsed >= 0 else None


def _time(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return _utc(parsed)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
