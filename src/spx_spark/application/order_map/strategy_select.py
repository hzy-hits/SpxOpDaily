"""Single strategy-decision authority for the Order Map payload."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timezone
from dataclasses import asdict
from pathlib import Path
from typing import Any

from spx_spark.analytics.options.density import summarize_strike_surface_shape
from spx_spark.analytics.options.strategy_payoff import management_policy_for_candidate
from spx_spark.application.market_features.physical_close_convergence import (
    estimate_physical_close_convergence,
)
from spx_spark.application.order_map.candidate_factory import (
    candidate_generation_reasons,
    enumerate_candidates,
)
from spx_spark.application.order_map.event_settlement_vertical import (
    enumerate_event_settlement_candidates,
    event_settlement_generation_reason,
)
from spx_spark.application.order_map.guidance import build_price_action_playbook
from spx_spark.application.order_map.iron_condor import (
    IRON_CONDOR_TYPE,
    HUMAN_SESSION_STATE_KEY,
    build_iron_condor_map,
    enumerate_iron_condor_candidates,
    iron_condor_session_state,
)
from spx_spark.application.order_map.path_distribution import (
    attach_iron_condor_path_distribution,
    attach_path_distribution,
)
from spx_spark.application.order_map.strategy_edge_model import (
    apply_strategy_edge_authority,
    reject_adverse_forward_path,
)
from spx_spark.application.order_map.strategy_facts import build_market_fact_pack
from spx_spark.application.order_map.strategy_regime import (
    DEFAULT_STRATEGY_POLICY,
    StrategyPolicy,
    assess_regime,
    pin_stable_next_step_text,
)
from spx_spark.application.order_map.strategy_ranker import (
    GthDirectionLock,
    RankResult,
    apply_winner_stick,
    candidate_direction_lock_reason,
    gth_direction_lock,
    outbox_accepted_strategy_cards,
    rank_candidates,
    session_committed_direction,
    session_direction_lock,
)
from spx_spark.settings.strategy_distribution import StrategyDistributionSettings
from spx_spark.storage import LatestState


def build_strategy_decision(
    payload: Mapping[str, Any], latest: LatestState, now: datetime, *,
    data_root: str | Path | None = None,
    probability_settings: StrategyDistributionSettings | None = None,
) -> dict[str, Any]:
    facts = build_market_fact_pack(payload, latest, now)
    facts["strategy_distribution_settings"] = asdict(probability_settings) if probability_settings else None
    facts["iron_condor_authority"] = _iron_condor_session_authority(facts)
    facts[HUMAN_SESSION_STATE_KEY] = iron_condor_session_state(
        payload, facts, (), now=_utc(now)
    )
    if data_root is not None:
        try:
            trading_date = date.fromisoformat(str(facts.get("session_date") or ""))
        except ValueError:
            trading_date = None
        if trading_date is not None:
            facts["close_convergence"] = estimate_physical_close_convergence(
                data_root,
                now=_utc(now),
                trading_date=trading_date,
            ).to_dict()
    regime = assess_regime(facts)
    facts["rth_environment"] = dict(
        regime.get("rth_environment")
        if isinstance(regime.get("rth_environment"), Mapping)
        else {}
    )
    facts["price_action_playbook"] = build_price_action_playbook(facts, regime)
    committed = _rth_committed_direction(
        facts,
        payload=payload,
        policy=DEFAULT_STRATEGY_POLICY,
    )
    if committed:
        facts["session_committed_direction"] = committed
    reasons = _gate_reasons(facts, regime)
    generation_reasons: list[str] = []
    rows: list[dict[str, Any]] = []
    rank = RankResult(passed=[], near_misses=[], gate_audit=[])
    iron_condor_map = build_iron_condor_map(
        payload,
        facts,
        latest,
        now=_utc(now),
        policy=DEFAULT_STRATEGY_POLICY,
    )
    if not reasons:
        iron_condor_rows = enumerate_iron_condor_candidates(
            payload,
            facts,
            latest,
            now=_utc(now),
            policy=DEFAULT_STRATEGY_POLICY,
        )
        facts[HUMAN_SESSION_STATE_KEY] = iron_condor_session_state(
            payload, facts, iron_condor_rows, now=_utc(now)
        )
        rows = [
            *enumerate_event_settlement_candidates(
                payload,
                facts,
                latest,
                now=_utc(now),
                policy=DEFAULT_STRATEGY_POLICY,
            ),
            *enumerate_candidates(
                payload,
                facts,
                regime,
                latest,
                now=_utc(now),
                policy=DEFAULT_STRATEGY_POLICY,
            ),
            *iron_condor_rows,
        ]
        if rows:
            rank = rank_candidates(
                rows,
                facts,
                regime,
                policy=DEFAULT_STRATEGY_POLICY,
                data_root=data_root,
                probability_settings=probability_settings,
                now=_utc(now),
            )
            edge_authority = apply_strategy_edge_authority(
                rank.passed,
                facts,
                regime,
                data_root=data_root,
                now=_utc(now),
            )
            rank = RankResult(
                passed=edge_authority.passed,
                near_misses=[*edge_authority.rejected, *rank.near_misses][:3],
                gate_audit=[*rank.gate_audit, *edge_authority.rejected],
            )
            stick_reason: str | None = None
            if rank.passed:
                session_mode = str(_map(facts.get("session")).get("mode") or "")
                winner_lock = _session_direction_lock(
                    facts, now=_utc(now), policy=DEFAULT_STRATEGY_POLICY, payload=payload,
                )
                locked_out = [
                    {**row, "rejection_reasons": [reason]}
                    for row in rank.passed
                    if (reason := candidate_direction_lock_reason(
                        row, winner_lock, session_mode=session_mode))
                ]
                stuck, stick_reason = apply_winner_stick(
                    rank.passed,
                    winner_lock,
                    session_mode=session_mode,
                )
                rank = RankResult(
                    passed=stuck,
                    near_misses=[*locked_out, *rank.near_misses],
                    gate_audit=[*rank.gate_audit, *locked_out],
                )
            path_evaluations = 0
            while rank.passed and path_evaluations < 3:
                path_evaluations += 1
                winner, shadows, iron_condor_map = _attach_winner_path_distributions(
                    facts,
                    rank.passed,
                    iron_condor_map,
                    data_root=data_root,
                    probability_settings=probability_settings,
                    now=_utc(now),
                )
                path_rejected = reject_adverse_forward_path(
                    winner,
                    policy=DEFAULT_STRATEGY_POLICY,
                )
                if path_rejected:
                    rank = RankResult(
                        passed=rank.passed[1:],
                        near_misses=[path_rejected, *rank.near_misses],
                        gate_audit=[*rank.gate_audit, path_rejected],
                    )
                    stick_reason = None
                else:
                    rank.passed[0] = winner
                    shadow_candidates, shadow_candidates_skipped = _shadow_candidates(
                        shadows
                    )
                    funnel = _rejection_funnel(
                        facts,
                        rows,
                        rank,
                        reasons=[],
                        generation_reasons=generation_reasons,
                        manual_candidate=True,
                    )
                    return _with_iron_condor_map(
                        _candidate_decision(
                            facts,
                            {**regime, "entry_state": "GOOD_LOCATION"},
                            winner,
                            candidates_considered=_candidate_summaries(rank),
                            shadow_candidates=shadow_candidates,
                            shadow_candidates_skipped=shadow_candidates_skipped,
                            rejection_funnel=funnel,
                        ),
                        iron_condor_map,
                    )
            reasons = [stick_reason] if stick_reason else _rank_reasons(rank)
            if rank.passed:
                reasons.append("path_evaluation_budget_exhausted")
                rank.gate_audit.extend(
                    {**row, "rejection_reasons": ["path_not_evaluated_budget"]}
                    for row in rank.passed
                )
                rank.passed = []
        else:
            event_reason = event_settlement_generation_reason(
                payload, now=_utc(now)
            )
            generation_reasons = list(
                dict.fromkeys(
                    [
                        *candidate_generation_reasons(
                            payload,
                            facts,
                            regime,
                            latest,
                            now=_utc(now),
                            policy=DEFAULT_STRATEGY_POLICY,
                        ),
                        *([event_reason] if event_reason else ()),
                    ]
                )
            )
            reasons = generation_reasons
    regime = {**regime, "entry_state": _entry_state(facts, reasons, rows, rank)}
    iron_condor_map = _attach_iron_condor_only_paths(
        facts,
        iron_condor_map,
        data_root=data_root,
        probability_settings=probability_settings,
        now=_utc(now),
    )
    return _with_iron_condor_map(
        _no_trade_decision(
            facts,
            regime,
            reasons,
            nearest_candidates=rank.near_misses,
            candidates_considered=_candidate_summaries(rank) if rows else [],
            rejection_funnel=_rejection_funnel(
                facts,
                rows,
                rank,
                reasons=reasons,
                generation_reasons=generation_reasons,
                manual_candidate=False,
            ),
        ),
        iron_condor_map,
    )


def _gate_reasons(facts: Mapping[str, Any], regime: Mapping[str, Any]) -> list[str]:
    del regime
    global_capability = _map(_map(facts.get("capabilities")).get("global"))
    reasons = [
        str(reason)
        for reason in global_capability.get("reasons") or ()
        if str(reason) != "macro_entry_not_authorized"
    ]
    return list(dict.fromkeys(reasons))


def _base_decision(facts: Mapping[str, Any], regime: Mapping[str, Any], identity: object) -> dict[str, Any]:
    from spx_spark.settings.loader import runtime_git_sha

    return {
        "schema_version": "strategy_decision.v2",
        "decision_id": f"strategy:{_hash(identity)[:24]}",
        "policy_version": DEFAULT_STRATEGY_POLICY.policy_version,
        "runtime_git_sha": runtime_git_sha(),
        "decision_at": facts["decision_at"],
        "available_at": facts["available_at"],
        "session_date": facts.get("session_date"),
        "market_facts": facts,
        "regime": regime,
        "probability_evidence": _probability_evidence(facts),
        "automatic_ordering": False,
        "position_status": "UNKNOWN",
    }


def _probability_evidence(facts: Mapping[str, Any]) -> dict[str, Any]:
    probability = _map(facts.get("probability"))
    effective = max(_number(probability.get("n_effective")) or 0.0, 0.0)
    return {
        "q": _number(probability.get("q")), "p_empirical": _number(probability.get("p_empirical")),
        "p_interval_low": _number(probability.get("p_interval_low")),
        "n_raw": int(_number(probability.get("n_raw")) or 0), "n_effective": round(effective, 6),
        "shrinkage_weight": round(effective / (effective + 20.0), 6),
        "historical_sessions": list(probability.get("historical_sessions") or ()),
    }


def _candidate_decision(
    facts: Mapping[str, Any],
    regime: Mapping[str, Any],
    candidate: Mapping[str, Any],
    *,
    candidates_considered: list[dict[str, Any]],
    shadow_candidates: list[dict[str, Any]],
    shadow_candidates_skipped: list[dict[str, Any]],
    rejection_funnel: Mapping[str, Any],
) -> dict[str, Any]:
    legs = candidate.get("legs") or (candidate.get("long"), candidate.get("short"))
    available = max(str(facts["available_at"]), *(str(_map(leg).get("source_at") or "") for leg in legs))
    economics = _map(candidate.get("economics"))
    surface_shape = _surface_shape_summary(facts)
    result = _base_decision(facts, regime, (facts["decision_at"], available, candidate["opportunity_id"], candidate["quote"]))
    geometry_source = _decision_geometry_source(candidate)
    quote = _map(candidate.get("quote"))
    credit_entry = str(candidate.get("strategy_type") or "") == "IRON_CONDOR"
    management_plan = dict(_map(candidate.get("management_plan")))
    targets = (
        [
            {
                "instrument": "SPXW_COMBO_BUYBACK",
                "price": round(float(quote.get("credit")) * 0.50, 4),
                "kind": "take_profit",
            }
        ]
        if credit_entry and _number(quote.get("credit")) is not None
        else [{"instrument": "SPX", "price": candidate["target_spx"]}]
    )
    result.update({
        "available_at": available,
        "management_contract": asdict(management_policy_for_candidate(candidate)),
        "geometry_source": geometry_source,
        "candidates_considered": candidates_considered[:5],
        "probability_evidence": dict(_map(candidate.get("probability_evidence"))),
        "decision_type": candidate["strategy_type"],
        "candidate": dict(candidate),
        "shadow_candidates": shadow_candidates,
        "shadow_candidates_skipped": shadow_candidates_skipped,
        "rejection_funnel": dict(rejection_funnel),
        "desk_view": {"state": regime["path_state"], "direction": candidate["direction"],
                      "conclusion": "MANUAL CANDIDATE", "reason": candidate["setup_kind"],
                      "shape": surface_shape["desk_line"],
                      "surface_shape": surface_shape},
        "why_not": {"nearest_candidate": None, "reasons": [], "reauthorize_on": None,
                    "surface_shape": surface_shape},
        "execution": {
            "action": "MANUAL_LIMIT",
            "order_type": "NET_CREDIT_LIMIT" if credit_entry else "NET_DEBIT_LIMIT",
            "limit": quote.get("credit") if credit_entry else quote.get("ask"),
            "quote_valid_until": candidate["quote_valid_until"],
            "opportunity_valid_until": candidate["opportunity_valid_until"],
            "automatic_ordering": False, "manual_action_only": True,
            "management_plan": management_plan,
        },
        "risk": {"max_loss": round(float(economics["max_loss_points"]) * 100, 2),
                 "invalidation": {"instrument": "SPX", "price": candidate["invalidation_spx"]},
                 "management_plan": management_plan},
        "targets": targets,
        "data_quality": {**dict(_map(facts.get("quality"))), "quote": "ready"},
        "action_authority": "manual",
    })
    return result


def _no_trade_decision(
    facts: Mapping[str, Any], regime: Mapping[str, Any], reasons: list[str], *,
    nearest_candidates: list[Mapping[str, Any]] | None = None,
    candidates_considered: list[dict[str, Any]] | None = None,
    rejection_funnel: Mapping[str, Any],
) -> dict[str, Any]:
    reasons = list(dict.fromkeys(reasons or ["no_supported_strategy_candidate"]))
    surface_shape = _surface_shape_summary(facts)
    primary = _primary_blocker(reasons)
    reasons = list(
        dict.fromkeys([*reasons, *(str(reason) for reason in surface_shape["why_reasons"])])
    )
    nearest_candidates = list(nearest_candidates or ())
    nearest_candidate = nearest_candidates[0] if nearest_candidates else None
    shadow = dict(nearest_candidate or {})
    if shadow:
        shadow.update({"shadow_only": True, "rejection_reasons": shadow.get("rejection_reasons") or reasons})
    shadows = []
    for item in nearest_candidates[:3]:
        row = dict(item)
        row.update({"shadow_only": True, "rejection_reasons": row.get("rejection_reasons") or reasons})
        shadows.append(row)
    legs = (
        shadow.get("legs") or (shadow.get("long"), shadow.get("short"))
        if shadow
        else ()
    )
    available_times = [
        timestamp
        for timestamp in (
            _time(facts["available_at"]),
            *(_time(_map(leg).get("source_at")) for leg in legs),
        )
        if timestamp is not None
    ]
    available = max(available_times).isoformat()
    result = _base_decision(
        facts,
        regime,
        (facts["decision_at"], available, regime, reasons, shadow.get("opportunity_id")),
    )
    refresh = "刷新 SPXW 两腿双边报价后重新计算" if "quote_refresh_required" in reasons else "等待价格触发、结构赔率和执行价格同时通过"
    if regime.get("terminal_state") == "PIN_STABLE":
        refresh = pin_stable_next_step_text(_number(facts.get("minutes_to_close")))
    result.update({
        "available_at": available,
        "geometry_source": _decision_geometry_source(shadow),
        "candidates_considered": list(candidates_considered or ())[:5],
        "decision_type": "NO_TRADE",
        "candidate": None,
        "shadow_candidates": [],
        "shadow_candidates_skipped": [],
        "rejection_funnel": dict(rejection_funnel),
        "desk_view": {"state": regime["path_state"], "direction": regime.get("path_direction"),
                      "conclusion": "NO TRADE", "reason": primary,
                      "shape": surface_shape["desk_line"],
                      "surface_shape": surface_shape},
        "why_not": {"nearest_candidate": shadow or None, "nearest_candidates": shadows,
                    "reasons": reasons, "primary_blocker": primary,
                    "reauthorize_on": refresh,
                    "surface_shape": surface_shape},
        "execution": {"action": "WAIT", "order_type": None, "limit": None,
                      "automatic_ordering": False, "manual_action_only": True},
        "risk": {"max_loss": None, "invalidation": "没有候选被授权，不建立风险敞口"},
        "targets": [],
        "data_quality": _map(facts.get("quality")),
        "action_authority": "none",
    })
    return result


def _rank_reasons(rank: RankResult) -> list[str]:
    for candidate in rank.near_misses:
        reasons = [str(reason) for reason in candidate.get("rejection_reasons") or ()]
        if reasons:
            if "spread_leg_quote_stale" in reasons and "quote_refresh_required" not in reasons:
                reasons.insert(0, "quote_refresh_required")
            return list(dict.fromkeys(reasons))
    return ["no_supported_strategy_candidate"]


def _primary_blocker(reasons: Sequence[str]) -> str:
    gate_reasons = [
        str(reason)
        for reason in reasons
        if str(reason).strip() and not str(reason).startswith("surface_shape_")
    ]
    return gate_reasons[0] if gate_reasons else "no_supported_strategy_candidate"


def _entry_state(
    facts: Mapping[str, Any],
    reasons: Sequence[str],
    rows: Sequence[Mapping[str, Any]],
    rank: RankResult,
) -> str:
    reason_set = {str(reason) for reason in reasons}
    if reason_set & {
        "direction_valid_but_entry_too_late",
        "es_volume_momentum_too_late",
        "es_volume_momentum_extended_without_fresh_break",
    }:
        return "LATE_CHASE"
    if rows and not rank.passed:
        return "POOR_ASYMMETRY"
    missing = {
        "spx_price_unavailable",
        "vertical_path_inputs_unavailable",
        "trend_pullback_path_unevaluable",
        "es_volume_momentum_unevaluable",
        "path_inputs_unavailable",
        "market_frame_not_ready",
    }
    if reason_set & missing:
        return "INSUFFICIENT_DATA"
    if _map(_map(facts.get("capabilities")).get("path")).get("ready") is not True:
        return "INSUFFICIENT_DATA"
    return "INSUFFICIENT_DATA"


def _rejection_funnel(
    facts: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    rank: RankResult,
    *,
    reasons: Sequence[str],
    generation_reasons: Sequence[str],
    manual_candidate: bool,
) -> dict[str, Any]:
    global_capability = _map(_map(facts.get("capabilities")).get("global"))
    facts_ready = all(
        global_capability.get(key) is True
        for key in ("session_legal", "coordinate_ready", "market_frame_ready")
    )
    setup_absence_reasons = {
        "confirmed_price_trigger_unavailable",
        "price_trigger_not_aligned_with_supported_setup",
        "price_trigger_conflicts_with_established_path",
        "gth_confirmed_level_candidate_unavailable",
        "gth_dip_reclaim_evidence_unavailable",
        "trend_background_cannot_authorize_entry",
        "rth_entry_window_not_open",
        "rth_entry_window_too_late",
        "rth_setup_invalidated",
        "trend_pullback_path_not_confirmed",
        "trend_pullback_path_unevaluable",
        "es_volume_momentum_unavailable",
        "es_volume_not_elevated",
        "es_volume_momentum_direction_flat",
        "es_volume_momentum_break_reclaimed",
        "es_volume_momentum_not_aligned",
        "es_volume_momentum_opposes_15m_without_fresh_break",
        "es_volume_momentum_extended_without_fresh_break",
        "es_volume_momentum_too_weak",
        "es_volume_momentum_unevaluable",
        "es_volume_momentum_too_late",
        "event_settlement_exact_two_leg_quote_unavailable",
        "vertical_path_inputs_unavailable",
        "spx_price_unavailable",
        "session_not_open_for_spxw_strategy",
        "market_frame_not_ready",
        "vertical_expiry_unavailable",
        "vertical_exact_two_leg_quote_unavailable",
    }
    rth_setups = [_map(row) for row in facts.get("rth_setups") or ()]
    live_states = {
        str(row.get("state") or "")
        for row in rth_setups
        if str(row.get("state") or "") in {"SETUP_DETECTED", "ENTRY_WINDOW_OPEN", "ENTRY_TOO_LATE"}
    }
    setup_detected = bool(rows) or bool(live_states) or bool(
        generation_reasons
        and not any(str(reason) in setup_absence_reasons for reason in generation_reasons)
    )
    entry_window_open = (
        bool(rows) or "ENTRY_WINDOW_OPEN" in live_states
    ) and not {
        "direction_valid_but_entry_too_late",
        "trend_pullback_path_not_confirmed",
        "trend_pullback_path_unevaluable",
    }.intersection(map(str, reasons))
    exact_quote_ready = sum(
        _map(row.get("quote")).get("status") == "ready" for row in rows
    )
    hard_gate_pass = len(rank.passed)
    if manual_candidate:
        stage = "manual_card_pending"
        delivery_status = "pending_delivery"
    elif hard_gate_pass:
        stage = "hard_gate_pass"
        delivery_status = "not_selected"
    elif exact_quote_ready:
        stage = "exact_quote_ready"
        delivery_status = "not_applicable"
    elif rows:
        stage = "candidate_enumerated"
        delivery_status = "not_applicable"
    elif entry_window_open:
        stage = "entry_window_open"
        delivery_status = "not_applicable"
    elif setup_detected:
        stage = "setup_detected"
        delivery_status = "not_applicable"
    elif facts_ready:
        stage = "facts_ready"
        delivery_status = "not_applicable"
    else:
        stage = "cycles"
        delivery_status = "not_applicable"
    return {
        "cycles": 1,
        "candidate_evaluations": rank.gate_audit,
        "facts_ready": int(facts_ready),
        "setup_detected": int(setup_detected),
        "entry_window_open": int(entry_window_open),
        "candidate_enumerated": len(rows),
        "exact_quote_ready": exact_quote_ready,
        "hard_gate_pass": hard_gate_pass,
        "manual_card_delivered": 0,
        "manual_card_delivery_status": delivery_status,
        "current_stage": stage,
        "pending_confirmation": int("SETUP_DETECTED" in live_states),
        "primary_blocker": _primary_blocker(reasons),
        "event_settlement_considered": int(
            any(
                _map(row).get("setup_kind") == "EVENT_SETTLEMENT_THRESHOLD"
                for row in rows
            )
            or "event_settlement_exact_two_leg_quote_unavailable" in map(str, reasons)
            or "event_settlement_exact_two_leg_quote_unavailable" in map(str, generation_reasons)
        ),
    }


def _candidate_summaries(rank: RankResult) -> list[dict[str, Any]]:
    return [_candidate_summary(candidate) for candidate in (*rank.passed, *rank.near_misses)][:5]


def _candidate_summary(candidate: Mapping[str, Any]) -> dict[str, Any]:
    summary = {
        "candidate_id": candidate.get("candidate_id"),
        "strategy_type": candidate.get("strategy_type"),
        "strikes": _candidate_strikes(candidate),
        "score": _candidate_score(candidate),
        "gate_failures": list(candidate.get("failed_gates") or ()),
    }
    for key in ("selection_score_base", "surface_decision_modifier"):
        if key in candidate:
            summary[key] = candidate.get(key)
    return summary


def _surface_shape_summary(facts: Mapping[str, Any]) -> dict[str, object]:
    return summarize_strike_surface_shape(
        _map(_map(facts.get("structure")).get("strike_differential_context"))
    )


def _candidate_strikes(candidate: Mapping[str, Any]) -> list[float]:
    if candidate.get("legs"):
        return [
            float(strike)
            for leg in candidate.get("legs") or ()
            if (strike := _number(_map(leg).get("strike"))) is not None
        ]
    return [
        float(strike)
        for leg in (candidate.get("long"), candidate.get("short"))
        if (strike := _number(_map(leg).get("strike"))) is not None
    ]


def _candidate_score(candidate: Mapping[str, Any]) -> float:
    edge = _map(_map(candidate.get("edge")).get("strategy_edge"))
    lower = _number(edge.get("pnl_residual_q10_points"))
    loss = _number(_map(candidate.get("economics")).get("max_loss_points"))
    if lower is not None and loss is not None and loss > 0:
        return round(lower / loss, 6)
    utility = _number(_map(candidate.get("utility")).get("utility"))
    if utility is not None:
        return round(utility, 6)
    return round(float(candidate.get("selection_score") or 0.0), 6)


def _decision_geometry_source(candidate: Mapping[str, Any]) -> str:
    source = candidate.get("geometry_source")
    if source in {
        "preaverage_local_scale_first_passage",
        "rth_20delta_fixed10_iron_condor",
    }:
        return source
    return "confirmation_geometry" if source == "confirmation_geometry" else "facts_wall_ladder_fallback"


def _shadow_candidates(
    candidates: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    persisted: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for candidate in candidates:
        reason = _shadow_candidate_skip_reason(candidate)
        if reason is None:
            persisted.append(dict(candidate))
            continue
        skipped.append(
            {
                "candidate_id": candidate.get("candidate_id"),
                "reason": reason,
            }
        )
    return persisted, skipped


def _shadow_candidate_skip_reason(candidate: Mapping[str, Any]) -> str | None:
    quote = _map(candidate.get("quote"))
    if _number(quote.get("bid")) is None or _number(quote.get("ask")) is None:
        return "candidate_quote_incomplete"
    raw_legs = candidate.get("legs")
    if isinstance(raw_legs, Sequence) and not isinstance(raw_legs, (str, bytes)):
        legs = tuple(_map(item) for item in raw_legs)
    else:
        legs = (_map(candidate.get("long")), _map(candidate.get("short")))
    if not legs or any(not leg for leg in legs):
        return "candidate_legs_incomplete"
    for leg in legs:
        if not str(leg.get("contract_id") or "").strip():
            return "candidate_legs_incomplete"
        if _number(leg.get("bid")) is None or _number(leg.get("ask")) is None:
            return "candidate_legs_incomplete"
        if _time(leg.get("source_at")) is None:
            return "candidate_legs_incomplete"
    return None


def _hash(value: object) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(encoded.encode()).hexdigest()


def _attach_winner_path_distributions(
    facts: Mapping[str, Any],
    passed: Sequence[Mapping[str, Any]],
    iron_condor_map: Mapping[str, Any],
    *,
    data_root: str | Path | None,
    probability_settings: StrategyDistributionSettings | None,
    now: datetime,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    first = dict(passed[0])
    if str(first.get("strategy_type") or "") == IRON_CONDOR_TYPE:
        winner = first
    else:
        winner = attach_path_distribution(
            first,
            facts,
            data_root=data_root,
            probability_settings=probability_settings,
            now=now,
        )
    # First slice: winner + iron-condor map only. Shadow cards stay rank-only.
    shadows = [dict(row) for row in passed[1:3]]
    return (
        winner,
        shadows,
        attach_iron_condor_path_distribution(
            iron_condor_map,
            facts,
            data_root=data_root,
            probability_settings=probability_settings,
            now=now,
        ),
    )


def _attach_iron_condor_only_paths(
    facts: Mapping[str, Any],
    iron_condor_map: Mapping[str, Any],
    *,
    data_root: str | Path | None,
    probability_settings: StrategyDistributionSettings | None,
    now: datetime,
) -> dict[str, Any]:
    return attach_iron_condor_path_distribution(
        iron_condor_map,
        facts,
        data_root=data_root,
        probability_settings=probability_settings,
        now=now,
    )


def _session_direction_lock(
    facts: Mapping[str, Any],
    *,
    now: datetime,
    policy: StrategyPolicy,
    payload: Mapping[str, Any],
):
    session_mode = str(_map(facts.get("session")).get("mode") or "")
    if session_mode == "gth":
        return _gth_direction_lock(facts, now=now, policy=policy)
    if session_mode == "rth":
        return _rth_direction_lock(
            facts,
            now=now,
            policy=policy,
            payload=payload,
        )
    return None


def _gth_direction_lock(
    facts: Mapping[str, Any],
    *,
    now: datetime,
    policy: StrategyPolicy,
):
    if str(_map(facts.get("session")).get("mode") or "") != "gth":
        return None
    session_date = str(facts.get("session_date") or "")
    if not session_date or policy.gth_winner_stick_seconds <= 0:
        return None
    rows = _accepted_session_cards(session_date)
    if rows is None:
        return None
    return gth_direction_lock(
        rows,
        now=now,
        stick_seconds=policy.gth_winner_stick_seconds,
    )


def _rth_direction_lock(
    facts: Mapping[str, Any],
    *,
    now: datetime,
    policy: StrategyPolicy,
    payload: Mapping[str, Any],
):
    injected = _injected_direction_lock(payload.get("session_direction_lock"))
    if injected is not None:
        return injected
    if str(_map(facts.get("session")).get("mode") or "") != "rth":
        return None
    session_date = str(facts.get("session_date") or "")
    if not session_date or policy.rth_winner_stick_seconds <= 0:
        return None
    rows = _accepted_session_cards(session_date)
    if rows is None:
        return None
    return session_direction_lock(
        rows,
        now=now,
        stick_seconds=policy.rth_winner_stick_seconds,
        session_mode="rth",
    )


def _rth_committed_direction(
    facts: Mapping[str, Any],
    *,
    payload: Mapping[str, Any],
    policy: StrategyPolicy,
) -> str | None:
    del policy
    injected = str(payload.get("session_committed_direction") or "").upper()
    if injected in {"UP", "DOWN"}:
        return injected
    if str(_map(facts.get("session")).get("mode") or "") != "rth":
        return None
    session_date = str(facts.get("session_date") or "")
    if not session_date:
        return None
    rows = _accepted_session_cards(session_date)
    if rows is None:
        return None
    return session_committed_direction(rows, session_mode="rth")


def _injected_direction_lock(raw: object) -> GthDirectionLock | None:
    item = _map(raw)
    direction = str(item.get("direction") or "").upper()
    opportunity_id = str(item.get("opportunity_id") or "").strip()
    started_at = item.get("started_at")
    if direction not in {"UP", "DOWN", "NEUTRAL"} or not opportunity_id:
        return None
    if isinstance(started_at, datetime):
        started = started_at if started_at.tzinfo else started_at.replace(tzinfo=timezone.utc)
        return GthDirectionLock(
            direction=direction,
            opportunity_id=opportunity_id,
            started_at=started.astimezone(timezone.utc),
        )
    if isinstance(started_at, str) and started_at:
        try:
            parsed = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return GthDirectionLock(
            direction=direction,
            opportunity_id=opportunity_id,
            started_at=parsed.astimezone(timezone.utc),
        )
    return None


def _accepted_session_cards(session_date: str):
    # Unit tests pin this flag and must not read the host operational sqlite.
    isolated = os.getenv("SPX_SPARK_DISABLE_RUNTIME_OVERRIDES", "").strip().lower()
    if isolated in {"1", "true", "yes"}:
        return None
    try:
        from spx_spark.config import NotificationSettings
        from spx_spark.infrastructure.operational_db import recent_selected_strategy_cards
        from spx_spark.notifier.dispatcher import notification_event_exists

        rows = recent_selected_strategy_cards(session_date=session_date)
        settings = NotificationSettings.from_env()
        return outbox_accepted_strategy_cards(
            rows,
            event_exists=lambda event_id: notification_event_exists(settings, event_id),
        )
    except (OSError, ValueError):
        return None
    except Exception:  # noqa: BLE001 - unmigrated sqlite must not block ranking
        return None


def _iron_condor_session_authority(
    facts: Mapping[str, Any],
) -> dict[str, Any]:
    """Fail closed unless the ledger proves this session-mode quota is free."""

    session_mode = str(_map(facts.get("session")).get("mode") or "").lower()
    if session_mode not in {"rth", "gth"}:
        return {"status": "map_only", "accepted_count": None}
    session_date = str(facts.get("session_date") or "")
    if not session_date:
        return {"status": "unavailable", "accepted_count": None}
    rows = _accepted_session_cards(session_date)
    if rows is None:
        return {"status": "unavailable", "accepted_count": None}
    accepted = sum(
        1
        for row in rows
        if str(row.get("session_mode") or "").lower() == session_mode
        and str(row.get("setup_kind") or "") == "IRON_CONDOR_DELTA"
    )
    return {"status": "ready", "accepted_count": accepted}


def _with_iron_condor_map(
    decision: dict[str, Any], iron_condor_map: Mapping[str, Any]
) -> dict[str, Any]:
    decision["iron_condor_map"] = dict(iron_condor_map)
    lockfile = Path(__file__).resolve().parents[4] / "uv.lock"
    decision["decision_manifest"] = {
        "knowledge_cutoff": decision["decision_at"],
        "code_revision": decision.get("runtime_git_sha"),
        "dependency_lock_sha256": hashlib.sha256(lockfile.read_bytes()).hexdigest() if lockfile.exists() else None,
        "strategy_contract": asdict(DEFAULT_STRATEGY_POLICY),
        "facts_sha256": _hash(decision["market_facts"]),
        "content_sha256": _hash(decision),
    }
    return decision


def _map(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _number(value: object) -> float | None:
    return float(value) if isinstance(value, int | float) else None


def _time(value: object) -> datetime | None:
    if not isinstance(value, (str, datetime)):
        return None
    try:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("strategy decision time must be timezone-aware")
    return value.astimezone(timezone.utc)
