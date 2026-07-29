"""Deterministic, report-only status for the two supported Put hypotheses."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from spx_spark.analytics.options.pricing import finite_float
from spx_spark.application.market_features.trade_intent import (
    FLIP_LOW_BREAKDOWN_PUT_MANUAL_LANE,
    UPPER_REJECTION_PUT_MANUAL_LANE,
    live_trade_intent_authority_issues,
)
from spx_spark.application.order_map.convexity_idea_quality import (
    select_rth_market_state,
)


SCHEMA_VERSION = "put_candidate_report.v1"
_SUPPORTED_SETUPS = (
    {
        "setup": "flip_low_breakdown",
        "label": "Flip Low跌破",
        "level_kinds": ("flip_low",),
        "thesis": "breakout",
        "expected_play": "level_breakout_put",
    },
    {
        "setup": "call_wall_or_flip_high_rejection",
        "label": "Call Wall/Flip High拒绝",
        "level_kinds": ("call_wall", "flip_high"),
        "thesis": "fade",
        "expected_play": "level_fade_put",
    },
)


def build_put_candidate_report(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Build a bounded status model without changing strategy authority."""

    decision = _mapping(payload.get("level_decision"))
    intent = _mapping(payload.get("trade_intent"))
    levels = _mapping(decision.get("levels"))
    market_state = _rth_market_state(payload)
    moving_averages = _moving_average_context(payload, market_state=market_state)
    rows = [
        _supported_candidate(
            spec,
            decision=decision,
            intent=intent,
            levels=levels,
            market_state=market_state,
            moving_averages=moving_averages,
            regime=_mapping(payload.get("regime_decision")),
        )
        for spec in _SUPPORTED_SETUPS
    ]
    rows.append(_disabled_put_wall_candidate(decision, levels))
    return {
        "schema_version": SCHEMA_VERSION,
        "candidates": rows,
        "action_authority": "none",
        "automatic_ordering": False,
        "semantics": (
            "wall_signal_execution_eligibility_and_priority_are_separate;"
            "put_wall_breakdown_is_disabled_unsupported"
        ),
    }


def put_candidate_report_lines(payload: Mapping[str, Any]) -> list[str]:
    """Render all three Put rows even when no execution path is available."""

    report = build_put_candidate_report(payload)
    lines: list[str] = []
    for row in report["candidates"]:
        wall = _mapping(row.get("wall_signal"))
        execution = _mapping(row.get("execution_eligible"))
        priority = _mapping(row.get("priority"))
        wall_reason = str(wall.get("reason") or "")
        execution_reason = str(execution.get("reason") or "")
        priority_reason = str(priority.get("reason") or "")
        levels = _display_levels(row.get("levels"))
        lines.append(
            f"Put候选[{row.get('setup')}] {row.get('label')}  "
            f"WALL_SIGNAL={wall.get('status') or 'UNAVAILABLE'}"
            f"({levels};{wall_reason or '-'}) · "
            f"EXECUTION_ELIGIBLE={'YES' if execution.get('eligible') is True else 'NO'}"
            f"({execution_reason or '-'}) · "
            f"PRIORITY={priority.get('status') or 'LOW'}"
            f"({priority_reason or '-'})　只读"
        )
    return lines


def put_wall_breakdown_report_disabled(payload: Mapping[str, Any]) -> bool:
    """Return whether the current decision is the unsupported Put-wall breakdown."""

    decision = _mapping(payload.get("level_decision"))
    return bool(
        str(decision.get("level_kind") or "").lower() == "put_wall"
        and str(decision.get("thesis") or "").lower() == "breakout"
        and str(decision.get("direction") or "").lower() == "down"
    )


def presentable_plan_candidates(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Return plans backed by a supported live-authority TradeIntent."""

    if put_wall_breakdown_report_disabled(payload):
        return []
    rows = payload.get("plan_candidates")
    if not isinstance(rows, list):
        return []
    intent = _mapping(payload.get("trade_intent"))
    put_authorized = not live_trade_intent_authority_issues(intent)
    return [
        dict(row)
        for row in rows
        if isinstance(row, Mapping)
        and not _put_wall_plan(row)
        and (not _put_plan(row) or put_authorized)
    ]


def _put_wall_plan(candidate: Mapping[str, Any]) -> bool:
    level_kind = str(candidate.get("level_kind") or "").strip().lower()
    level_label = str(candidate.get("level_label") or "").strip().lower()
    return bool(
        level_kind == "put_wall" or level_label == "put_wall" or level_label.startswith("put_wall ")
    )


def _put_plan(candidate: Mapping[str, Any]) -> bool:
    right = str(candidate.get("right") or "").upper()
    play = str(candidate.get("play") or "")
    return bool(
        right == "P"
        or play
        in {
            "level_breakout_put",
            "level_fade_put",
            "flip_breakdown_put",
            "call_wall_fade_put",
        }
    )


def _supported_candidate(
    spec: Mapping[str, Any],
    *,
    decision: Mapping[str, Any],
    intent: Mapping[str, Any],
    levels: Mapping[str, Any],
    market_state: Mapping[str, Any],
    moving_averages: Mapping[str, Any],
    regime: Mapping[str, Any],
) -> dict[str, Any]:
    level_kinds = tuple(str(value) for value in spec["level_kinds"])
    phase = str(decision.get("phase") or "far").lower()
    decision_kind = str(decision.get("level_kind") or "").lower()
    matched = bool(
        decision_kind in level_kinds
        and str(decision.get("thesis") or "").lower() == spec["thesis"]
        and str(decision.get("direction") or "").lower() == "down"
    )
    display_levels = [
        {"kind": kind, "level": finite_float(levels.get(kind))} for kind in level_kinds
    ]
    available = any(row["level"] is not None for row in display_levels)
    wall_status = phase.upper() if matched else "WATCH" if available else "UNAVAILABLE"
    wall_reason = (
        "matching_downside_lifecycle"
        if matched
        else "await_matching_downside_lifecycle"
        if available
        else "structure_level_unavailable"
    )
    eligible, execution_reason = _execution_eligibility(
        spec,
        decision=decision,
        intent=intent,
        matched=matched,
        phase=phase,
    )
    priority, priority_reason = _priority(
        str(spec["setup"]),
        matched=matched,
        phase=phase,
        eligible=eligible,
        market_state=market_state,
        moving_averages=moving_averages,
        regime=regime,
    )
    return {
        "setup": spec["setup"],
        "label": spec["label"],
        "level_kinds": list(level_kinds),
        "levels": display_levels,
        "wall_signal": {
            "status": wall_status,
            "matched": matched,
            "phase": phase,
            "event_id": decision.get("event_id"),
            "formal_signal": decision.get("formal_signal") is True,
            "reason": wall_reason,
        },
        "execution_eligible": {
            "eligible": eligible,
            "status": "eligible" if eligible else "ineligible",
            "reason": execution_reason,
            "source": "persisted_trade_intent_only",
        },
        "priority": {
            "status": priority,
            "reason": priority_reason,
            "authority": "soft_report_priority_only",
        },
        "action_authority": "none",
        "automatic_ordering": False,
    }


def _disabled_put_wall_candidate(
    decision: Mapping[str, Any],
    levels: Mapping[str, Any],
) -> dict[str, Any]:
    phase = str(decision.get("phase") or "far").lower()
    matched = bool(
        str(decision.get("level_kind") or "").lower() == "put_wall"
        and str(decision.get("thesis") or "").lower() == "breakout"
        and str(decision.get("direction") or "").lower() == "down"
    )
    level = finite_float(levels.get("put_wall"))
    wall_status = phase.upper() if matched else "WATCH" if level is not None else "UNAVAILABLE"
    return {
        "setup": "put_wall_breakdown",
        "label": "Put Wall跌破",
        "level_kinds": ["put_wall"],
        "levels": [{"kind": "put_wall", "level": level}],
        "wall_signal": {
            "status": wall_status,
            "matched": matched,
            "phase": phase,
            "event_id": decision.get("event_id") if matched else None,
            "formal_signal": bool(matched and decision.get("formal_signal") is True),
            "reason": (
                "matching_downside_lifecycle"
                if matched
                else "await_matching_downside_lifecycle"
                if level is not None
                else "structure_level_unavailable"
            ),
        },
        "execution_eligible": {
            "eligible": False,
            "status": "ineligible",
            "reason": "disabled_unsupported",
            "source": "report_policy",
        },
        "priority": {
            "status": "UNSUPPORTED",
            "reason": "not_combined_with_flip_or_upper_rejection",
            "authority": "report_policy",
        },
        "action_authority": "none",
        "automatic_ordering": False,
    }


def _execution_eligibility(
    spec: Mapping[str, Any],
    *,
    decision: Mapping[str, Any],
    intent: Mapping[str, Any],
    matched: bool,
    phase: str,
) -> tuple[bool, str]:
    if not matched:
        return False, "no_matching_wall_signal"
    if phase != "confirmed":
        return False, "wall_signal_not_confirmed"
    status = str(intent.get("status") or "unavailable").lower()
    if status != "trade_ready":
        reasons = intent.get("block_reasons")
        if isinstance(reasons, list):
            first = next((str(reason) for reason in reasons if str(reason)), None)
            if first:
                return False, first
        return False, f"trade_intent_{status}"
    if intent.get("shadow_mode") is not False:
        return False, "trade_intent_shadow_mode"
    if intent.get("execution_eligible") is not True:
        return False, "trade_intent_execution_authority_missing"
    if intent.get("quote_observation_eligible") is not False:
        return False, "trade_intent_quote_observation_only"
    if intent.get("automatic_ordering") is not False:
        return False, "trade_intent_automatic_ordering_contract_invalid"
    if str(intent.get("direction") or "").lower() != "down":
        return False, "intent_direction_mismatch"
    if str(intent.get("play") or "") != spec["expected_play"]:
        return False, "intent_play_mismatch"
    expected_lane = (
        FLIP_LOW_BREAKDOWN_PUT_MANUAL_LANE
        if spec["setup"] == "flip_low_breakdown"
        else UPPER_REJECTION_PUT_MANUAL_LANE
    )
    if str(intent.get("strategy_lane") or "") != expected_lane:
        return False, "intent_strategy_lane_mismatch"
    authority_issues = live_trade_intent_authority_issues(intent)
    if authority_issues:
        return False, authority_issues[0]
    decision_event = str(decision.get("event_id") or "")
    intent_event = str(intent.get("event_id") or "")
    if not decision_event or intent_event != decision_event:
        return False, "intent_event_mismatch"
    return True, "manual_ready_exact_quote"


def _priority(
    setup: str,
    *,
    matched: bool,
    phase: str,
    eligible: bool,
    market_state: Mapping[str, Any],
    moving_averages: Mapping[str, Any],
    regime: Mapping[str, Any],
) -> tuple[str, str]:
    if not matched:
        return "LOW", "no_matching_wall_signal"
    if phase != "confirmed":
        return "WATCH", "lifecycle_not_confirmed"

    ma_state = str(moving_averages.get("regime_state") or "")
    ma_direction = str(moving_averages.get("regime_direction") or "")
    if ma_state == "TREND_EXTENDED" and ma_direction == "down":
        return "NO_CHASE", "ma_trend_extended_down"

    state = str(market_state.get("state") or market_state.get("market_state") or "")
    direction_score = finite_float(market_state.get("D"))
    quality = _mapping(market_state.get("Q"))
    quality_name = str(quality.get("quality") or "")
    if setup == "flip_low_breakdown":
        if (
            state == "TREND_DOWN"
            and direction_score is not None
            and direction_score <= -6
            and quality_name in {"high", "trend"}
        ):
            return "HIGH", "dqv_clean_downtrend"
        if state in {"TREND_UP", "HIGH_VOL_CHOP", "LOW_VOL_RANGE", "LOW_VOL_PIN"}:
            return "LOW", f"dqv_{state.lower()}"
    else:
        if state == "LOW_VOL_RANGE" or str(regime.get("mode") or "") == "mean_reverting":
            return "HIGH", "mean_reversion_context"
        if state == "TREND_UP" and quality_name in {"high", "trend"}:
            return "LOW", "clean_uptrend_opposes_rejection"
    if eligible:
        return "NORMAL", "execution_ready_without_calibrated_priority_edge"
    return "NORMAL", "confirmed_wall_signal"


def _rth_market_state(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    state, _source = select_rth_market_state(payload)
    return state


def _moving_average_context(
    payload: Mapping[str, Any],
    *,
    market_state: Mapping[str, Any],
) -> Mapping[str, Any]:
    intent = _mapping(payload.get("trade_intent"))
    direct = _mapping(intent.get("moving_average_context"))
    if direct:
        return direct
    lineage = _mapping(market_state.get("input_lineage"))
    diagnostics = _mapping(lineage.get("diagnostics"))
    return _mapping(diagnostics.get("moving_averages"))


def _display_levels(value: object) -> str:
    if not isinstance(value, list):
        return "levels unavailable"
    parts: list[str] = []
    for row in value:
        if not isinstance(row, Mapping):
            continue
        level = finite_float(row.get("level"))
        parts.append(
            f"{row.get('kind') or '-'} {level:.2f}"
            if level is not None
            else f"{row.get('kind') or '-'} -"
        )
    return " / ".join(parts) if parts else "levels unavailable"


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}
