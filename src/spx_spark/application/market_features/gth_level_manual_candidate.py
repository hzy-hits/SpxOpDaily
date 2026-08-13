"""Manual-only GTH candidates from confirmed structural level paths."""

from __future__ import annotations

import hashlib
import math
from datetime import datetime, timedelta
from typing import Mapping

from spx_spark.application.market_features.gth_level_candidate_runtime import (
    TERMINAL_RECEIPT_CHECK_MAX_SECONDS as TERMINAL_RECEIPT_CHECK_MAX_SECONDS,
    TERMINAL_RECEIPT_CHECK_MIN_SECONDS as TERMINAL_RECEIPT_CHECK_MIN_SECONDS,
    TERMINAL_RECEIPT_RECOVERY_MAX_SECONDS as TERMINAL_RECEIPT_RECOVERY_MAX_SECONDS,
    _apply_active_plan_coherence as _apply_active_plan_coherence,
    _external_ready_receipt as _external_ready_receipt,
    _gate_record as _gate_record,
    _replay_candidate_record as _replay_candidate_record,
    persist_gth_level_manual_candidate,
)
from spx_spark.application.market_features.gth_manual_candidate import (
    EDGE_AUTHORITY_REQUIRED,
    EDGE_AUTHORITY_UNAVAILABLE_REASON,
    NET_DEBIT_PRICE_INCREMENT,
    _blocked,
    _direct_es_reference,
    _gth_bbo_contract_snapshot,
    _gth_end,
)
from spx_spark.application.market_features.gth_candidate_lifecycle import (
    classify_source_lifecycle,
)
from spx_spark.application.market_features.gth_trend_entry_source import (
    build_candidate_policy_version,
    candidate_geometry_context,
    candidate_trigger_coordinate,
    confirmation_baseline,
    is_es_trend_source,
    manual_source_expiry,
    manual_source_path_fields,
    resolve_gth_manual_source,
    spxw_contract_id,
)
from spx_spark.application.market_features.play_outcome_stats import (
    PlayOutcomeStats,
    historical_edge_blockers,
    play_stats_payload,
)
from spx_spark.application.market_features.prior_rth_context import (
    prior_session_signal_view,
)
from spx_spark.application.market_features.spring_gamma_operator import (
    spring_gamma_operator_view,
)
from spx_spark.application.market_features.virtual_strategy_spread import (
    spread_snapshot_decision,
)
from spx_spark.application.market_features.virtual_strategy_state import (
    flush_pending_notifications,
)
from spx_spark.application.market_features.virtual_strategy_support import (
    _number,
    _time,
    _utc,
)
from spx_spark.config import NotificationSettings, StorageSettings
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import Provider
from spx_spark.notifier.dispatcher import cancel_pending_notification
from spx_spark.options_map import actionable_chain_implied_reference
from spx_spark.settings.market_features import MarketFeatureSettings
from spx_spark.state_io import (
    atomic_write_json_secure,
    read_json_object as _read_json_object,
)
from spx_spark.storage import LatestState


read_json_object = _read_json_object


CONTRACT_VERSION = "gth_level_manual_candidate.v1"
SPREAD_MIN_WIDTH_POINTS = 5.0
SPREAD_DEFAULT_WIDTH_POINTS = 25.0
SPREAD_MAX_WIDTH_POINTS = 40.0
def _operator_edge_authority() -> tuple[str, str | None]:
    """Return the production authority for promoting a structure to operator READY.

    Exact NBBO and expiry payoff geometry are observations, not evidence that a
    15-minute plan has positive net expectancy.  This deliberately has no
    config escape hatch: a future implementation must bind a validated,
    causal first-touch/time-stop net-PnL artifact before returning the required
    closed authority value.
    """

    return "none", EDGE_AUTHORITY_UNAVAILABLE_REASON


def evaluate_gth_level_manual_candidate(
    latest: LatestState,
    level_decision: Mapping[str, object],
    *,
    trend_state: Mapping[str, object] | None = None,
    spring_gamma: Mapping[str, object] | None = None,
    macro_event: Mapping[str, object],
    now: datetime,
    policy: MarketFeatureSettings,
    new_entries_allowed: bool,
    new_entries_block_reason: str,
    prior_session: Mapping[str, object] | None = None,
    gth_position_fraction: float | None = None,
    play_stats: PlayOutcomeStats | None = None,
) -> dict[str, object]:
    """Build one GTH manual vertical from a level path, session-advance, or trend transition."""
    now = _utc(now)
    trend_state = trend_state if isinstance(trend_state, Mapping) else {}
    trend_regime = str(trend_state.get("regime") or "unknown")
    (
        source_mode,
        source,
        source_id,
        source_kind,
        generation,
        trend_source_reasons,
        source_tombstone_id,
    ) = resolve_gth_manual_source(
        level_decision,
        trend_state,
        now=now,
        ttl_seconds=policy.gth_manual_candidate_ttl_seconds,
        max_source_lag_seconds=policy.gth_manual_candidate_quote_max_age_seconds,
    )
    candidate_policy_version = build_candidate_policy_version(
        source_mode,
        policy,
        contract_version=CONTRACT_VERSION,
        spread_widths=(
            SPREAD_MIN_WIDTH_POINTS,
            SPREAD_DEFAULT_WIDTH_POINTS,
            SPREAD_MAX_WIDTH_POINTS,
        ),
    )
    base: dict[str, object] = {
        "schema_version": 1,
        "kind": "gth_spxw_level_manual_spread_candidate",
        "contract_version": CONTRACT_VERSION,
        "candidate_id": None,
        "policy_version": candidate_policy_version,
        "source_signal_id": source_id or None,
        "source_tombstone_id": source_tombstone_id,
        "source_event_id": source.get("source_event_id") if is_es_trend_source(source_mode) else source_id,
        "reentry_generation": generation,
        "source_kind": source_kind,
        "evaluated_at": now.isoformat(),
        "status": "observing",
        "candidate_scope": "manual_live",
        "execution_mode": "manual_only",
        "manual_action_eligible": False,
        "selector_evidence_eligible": False,
        "operator_notification_eligible": False,
        "execution_eligible": False,
        "automatic_ordering": False,
        "simulation_only": False,
        "broker_submission_allowed": False,
        "rth_trade_ready_authority": False,
        "broker_position_effect": "none",
        "must_requote_before_submit": True,
        "account_gth_permission_status": "unverified",
        "quantity": None,
        "quantity_policy": "operator_selected",
        "trend_regime": trend_regime,
        "source_lifecycle_class": classify_source_lifecycle(
            {
                "source_signal_id": source_id,
                "block_reasons": trend_source_reasons,
            }
        ),
        "historical_edge_authority": (
            "negative_safety_veto_only"
            if policy.gth_negative_play_stats_veto_enabled
            else "diagnostic_only"
        ),
        "edge_authority": "none",
        "edge_authority_required": EDGE_AUTHORITY_REQUIRED,
        "edge_authority_reason": EDGE_AUTHORITY_UNAVAILABLE_REASON,
        "expiry_payoff_ratio_role": "diagnostic_only",
        "block_reasons": [],
        "signal_absence_reason": (None if source_id else "no_level_or_trend_source_signal"),
        "gate_contract": {
            "version": "manual_signal_gate.v1",
            "hard_gates": [
                "confirmed_directional_source",
                "causal_current_session_directional_source",
                "inside_to_outside_breakout_crossing",
                "breakout_extension_before_retest",
                "mandatory_breakout_retest",
                "gth_session",
                "fresh_ibkr_spxw_two_leg_quote",
                "usable_spx_or_es_basis_coordinate",
                "coherent_risk_geometry",
                "gth_breakout_trend_alignment",
                "validated_first_touch_time_stop_net_pnl_edge_authority",
                "sufficiently_sampled_negative_history_veto",
                "prior_session_chase_control",
                "signal_ttl",
            ],
            "hard_block_reasons": [],
            "diagnostics": [],
        },
    }
    if not policy.gth_manual_candidate_enabled:
        return {**base, "status": "disabled", "block_reasons": ["disabled"]}
    if not source_id:
        return _blocked(base, trend_source_reasons or ["gth_level_not_confirmed_or_near"])
    reasons: list[str] = list(trend_source_reasons) if is_es_trend_source(source_mode) else []
    ranking_diagnostics: list[str] = []
    if not DEFAULT_MARKET_CALENDAR.is_spx_gth_open(now):
        reasons.append("spx_gth_session_required")
    if macro_event.get("entry_allowed") is not True:
        base["macro_event_warning"] = str(macro_event.get("mode") or "entry_not_allowed")
    if not new_entries_allowed:
        base["provider_incident_warning"] = new_entries_block_reason
        reasons.append("provider_entry_control_blocked")
    thesis, direction, level_kind = manual_source_path_fields(source_mode, level_decision, source)
    if source_mode == "level" and thesis == "breakout":
        if _time(source.get("breakout_inside_seen_at")) is None:
            reasons.append("breakout_inside_crossing_evidence_missing")
        if _time(source.get("breakout_extension_seen_at")) is None:
            reasons.append("breakout_extension_evidence_missing")
        if _time(source.get("breakout_retest_seen_at")) is None:
            reasons.append("breakout_retest_evidence_missing")
    if (
        source_mode == "level"
        and thesis == "breakout"
        and (
            (direction == "down" and trend_regime == "bullish")
            or (direction == "up" and trend_regime == "bearish")
        )
    ):
        reasons.append("gth_trend_regime_opposes_breakout")
    historical_diagnostics = historical_edge_blockers(
        play_stats,
        minimum_winrate=policy.play_stats_min_winrate,
    )
    base["historical_edge_diagnostics"] = historical_diagnostics
    if (
        policy.gth_negative_play_stats_veto_enabled
        and play_stats is not None
        and play_stats.sample_count >= policy.play_stats_min_samples
    ):
        reasons.extend(
            reason
            for reason in historical_diagnostics
            if reason
            in {
                "historical_average_return_non_positive",
                "historical_median_return_non_positive",
            }
        )
    if policy.play_stats_hard_gate_enabled:
        reasons.extend(historical_diagnostics)
    if play_stats is not None:
        base["play_stats"] = play_stats_payload(play_stats)
    if (
        thesis == "breakout"
        and direction == "down"
        and level_kind in {"flip_low", "put_wall", "trend"}
    ):
        right, position_type = "P", "put_debit_spread"
        path_kind = (
            "trend_advance_put"
            if source_mode == "trend_advance"
            else "trend_transition_put"
            if source_mode == "trend"
            else "flip_low_breakdown_put"
            if level_kind == "flip_low"
            else "put_wall_breakdown_put"
        )
    elif thesis == "fade" and direction == "up" and level_kind in {"put_wall", "flip_low"}:
        right, position_type = "C", "call_debit_spread"
        path_kind = "lower_rejection_call"
    elif (
        thesis == "breakout"
        and direction == "up"
        and level_kind in {"flip_high", "call_wall", "trend"}
    ):
        right, position_type = "C", "call_debit_spread"
        path_kind = (
            "trend_advance_call"
            if source_mode == "trend_advance"
            else "trend_transition_call"
            if source_mode == "trend"
            else "upper_acceptance_call"
        )
    else:
        return _blocked(base, [*reasons, "unsupported_gth_level_path"])
    session_date = DEFAULT_MARKET_CALENDAR.research_expiry(now)
    expiry = (
        str(level_decision.get("expiry") or "")
        if source_mode == "level"
        else session_date.strftime("%Y%m%d")
    )
    spring_gamma_view = spring_gamma_operator_view(
        spring_gamma,
        now=now,
        expected_expiry=expiry,
    )
    if expiry != session_date.strftime("%Y%m%d"):
        reasons.append("signal_session_mismatch")
    source_expires_at = manual_source_expiry(
        source_mode,
        source,
        ttl_seconds=policy.gth_manual_candidate_ttl_seconds,
    )
    if source_expires_at is None:
        reasons.append("source_expiry_unavailable")
    elif source_expires_at <= now:
        reasons.append("source_signal_expired")
    levels = level_decision.get("levels") if source_mode == "level" else {}
    levels = levels if isinstance(levels, Mapping) else {}
    parity = (
        actionable_chain_implied_reference(
            latest,
            expiry=expiry,
            as_of=now,
            required_provider=Provider.IBKR,
            max_age_seconds=policy.gth_manual_candidate_quote_max_age_seconds,
            max_leg_skew_seconds=policy.provider_sync_tolerance_seconds,
            min_pair_count=policy.gth_manual_candidate_min_parity_pairs,
            max_dispersion_points=policy.gth_manual_candidate_max_parity_dispersion_points,
            max_pair_interval_points=policy.gth_manual_candidate_max_parity_interval_points,
        )
        if is_es_trend_source(source_mode)
        else None
    )
    if is_es_trend_source(source_mode) and parity is None:
        reasons.append("chain_implied_target_unavailable")
    source_provider = str(source.get("provider") or "").lower()
    es_providers = (
        (Provider.SCHWAB, Provider.IBKR)
        if is_es_trend_source(source_mode) and source_provider == Provider.SCHWAB.value
        else (Provider.IBKR, Provider.SCHWAB)
        if is_es_trend_source(source_mode)
        else (Provider.IBKR,)
    )
    es_reference = _direct_es_reference(
        latest,
        now=now,
        max_age_seconds=policy.gth_manual_candidate_quote_max_age_seconds,
        providers=es_providers,
    )
    geometry_context, geometry_reasons = candidate_geometry_context(
        source_mode,
        source,
        level_decision,
        levels,
        level_kind,
        parity,
        es_reference,
        invalidation_buffer_points=policy.trade_invalidation_buffer_points,
        target_distance_points=SPREAD_DEFAULT_WIDTH_POINTS,
    )
    reasons.extend(geometry_reasons)
    selection_spx = _number(geometry_context.get("selection_spx"))
    trigger_level = _number(geometry_context.get("trigger_level"))
    basis = _number(geometry_context.get("basis_points"))
    trend_geometry = geometry_context.get("trend_geometry")
    trend_geometry = trend_geometry if isinstance(trend_geometry, Mapping) else None
    if selection_spx is None or selection_spx <= 0 or trigger_level is None:
        return _blocked(base, reasons)
    long_strike = round(selection_spx / 5.0) * 5.0
    if right == "P":
        structural_target = _number(levels.get("put_wall")) if source_mode == "level" else None
        target_spx = (
            float(trend_geometry["target_spx"])
            if trend_geometry is not None
            else structural_target
            if structural_target is not None and structural_target < trigger_level
            else trigger_level - SPREAD_DEFAULT_WIDTH_POINTS
        )
        short_strike = max(
            round(target_spx / 5.0) * 5.0,
            long_strike - SPREAD_MAX_WIDTH_POINTS,
        )
        if short_strike >= long_strike:
            short_strike = long_strike - SPREAD_MIN_WIDTH_POINTS
        invalidation_spx = (
            float(trend_geometry["invalidation_spx"])
            if trend_geometry is not None
            else (_number(levels.get("flip_high")) or trigger_level)
            + policy.trade_invalidation_buffer_points
        )
        width = long_strike - short_strike
        target_wall_kind = "put_wall" if structural_target is not None else "time_stop"
    else:
        structural_target = (
            (
                _number(levels.get("flip_low"))
                if path_kind == "lower_rejection_call" and level_kind == "put_wall"
                else _number(levels.get("call_wall"))
            )
            if source_mode == "level"
            else None
        )
        target_spx = (
            float(trend_geometry["target_spx"])
            if trend_geometry is not None
            else structural_target
            if structural_target is not None and structural_target > trigger_level
            else trigger_level + SPREAD_DEFAULT_WIDTH_POINTS
        )
        short_strike = min(
            round(target_spx / 5.0) * 5.0,
            long_strike + SPREAD_MAX_WIDTH_POINTS,
        )
        if short_strike <= long_strike:
            short_strike = long_strike + SPREAD_MIN_WIDTH_POINTS
        invalidation_anchor = (
            float(trend_geometry["invalidation_spx"])
            if trend_geometry is not None
            else trigger_level
            if path_kind == "lower_rejection_call"
            else _number(levels.get("flip_low")) or trigger_level
        )
        invalidation_spx = (
            invalidation_anchor
            if trend_geometry is not None
            else invalidation_anchor - policy.trade_invalidation_buffer_points
        )
        width = short_strike - long_strike
        target_wall_kind = (
            "flip_low"
            if path_kind == "lower_rejection_call" and level_kind == "put_wall"
            else "call_wall"
            if structural_target is not None
            else "time_stop"
        )
    if width < SPREAD_MIN_WIDTH_POINTS or width > SPREAD_MAX_WIDTH_POINTS:
        reasons.append("spread_width_invalid")
    long_contract_id = spxw_contract_id(expiry, long_strike, right)
    short_contract_id = spxw_contract_id(expiry, short_strike, right)
    identity_parts = [CONTRACT_VERSION, candidate_policy_version, path_kind]
    if is_es_trend_source(source_mode):
        identity_parts.append(source_id)
    else:
        identity_parts.extend((long_contract_id, short_contract_id))
    # Re-entries get distinct IDs; generation-zero retains replay compatibility.
    if generation > 0:
        identity_parts.extend(("reentry", str(generation), source_id))
    identity = "|".join(identity_parts)
    base["candidate_id"] = "gth-level-manual:" + hashlib.sha256(identity.encode()).hexdigest()[:24]
    snapshot, quote_reasons = spread_snapshot_decision(
        latest,
        long_contract_id=long_contract_id,
        short_contract_id=short_contract_id,
        now=now,
        max_quote_age_seconds=policy.gth_manual_candidate_quote_max_age_seconds,
        max_quote_skew_seconds=policy.provider_sync_tolerance_seconds,
        required_provider=Provider.IBKR.value,
        contract_snapshot=_gth_bbo_contract_snapshot,
    )
    reasons.extend(quote_reasons)
    bid = _number(snapshot.get("bid"))
    mid = _number(snapshot.get("mid"))
    ask = _number(snapshot.get("ask"))
    if bid is None or mid is None or ask is None or not 0 <= bid <= mid <= ask:
        reasons.append("spread_net_nbbo_invalid")
    else:
        if ask >= width * policy.gth_manual_candidate_max_debit_fraction:
            reasons.append("spread_debit_risk_cap_exceeded")
        if ask - bid > width * policy.gth_manual_candidate_max_net_spread_fraction:
            reasons.append("spread_net_market_too_wide")
    entry_limit = (
        math.ceil(ask / NET_DEBIT_PRICE_INCREMENT - 1e-12) * NET_DEBIT_PRICE_INCREMENT
        if ask is not None
        else None
    )
    if entry_limit is None or entry_limit <= 0 or entry_limit >= width:
        reasons.append("spread_entry_limit_invalid")
    if parity is None:
        parity = actionable_chain_implied_reference(
            latest,
            expiry=expiry,
            as_of=now,
            required_provider=Provider.IBKR,
            max_age_seconds=policy.gth_manual_candidate_quote_max_age_seconds,
            max_leg_skew_seconds=policy.provider_sync_tolerance_seconds,
            min_pair_count=policy.gth_manual_candidate_min_parity_pairs,
            max_dispersion_points=policy.gth_manual_candidate_max_parity_dispersion_points,
            max_pair_interval_points=policy.gth_manual_candidate_max_parity_interval_points,
        )
    if parity is None:
        reasons.append("chain_implied_target_unavailable")
    elif right == "P":
        if (
            target_spx
            >= float(parity["lower_bound"]) - policy.gth_manual_candidate_target_room_buffer_points
        ):
            reasons.append("target_room_below_parity_uncertainty_bound")
        if float(parity["upper_bound"]) >= invalidation_spx:
            reasons.append("invalidation_reached_before_candidate")
    else:
        if (
            target_spx
            <= float(parity["upper_bound"]) + policy.gth_manual_candidate_target_room_buffer_points
        ):
            reasons.append("target_room_below_parity_uncertainty_bound")
        if float(parity["lower_bound"]) <= invalidation_spx:
            reasons.append("invalidation_reached_before_candidate")
    invalidation_es = invalidation_spx + basis if basis is not None else None
    if es_reference is not None and invalidation_es is not None:
        es_price = float(es_reference["price"])
        if (right == "P" and es_price >= invalidation_es) or (
            right == "C" and es_price <= invalidation_es
        ):
            reasons.append("es_direction_not_held")
    gth_end = _gth_end(now)
    candidate_cutoff = (
        gth_end - timedelta(seconds=policy.gth_manual_candidate_close_buffer_seconds)
        if gth_end is not None
        else None
    )
    if candidate_cutoff is None or now >= candidate_cutoff:
        reasons.append("gth_entry_clock_closed")
    reward_risk = (
        (width - entry_limit) / entry_limit
        if entry_limit is not None and 0 < entry_limit < width
        else None
    )
    if reward_risk is None:
        reasons.append("spread_reward_risk_unavailable")
    elif reward_risk < policy.gth_manual_candidate_min_reward_risk:
        ranking_diagnostics.append("expiry_payoff_ratio_below_diagnostic_floor")
    prior_session_view = prior_session_signal_view(
        prior_session,
        direction=direction,
        gth_position_fraction=gth_position_fraction,
    )
    chase_risk = str(prior_session_view.get("chase_risk") or "")
    if chase_risk == "high":
        reasons.append("prior_session_same_direction_chase_risk_high")
    elif chase_risk == "elevated":
        ranking_diagnostics.append("prior_session_same_direction_chase_risk_elevated")
    base["ranking_diagnostics"] = list(dict.fromkeys(ranking_diagnostics))
    base["gate_contract"] = {
        **base["gate_contract"],
        "diagnostics": [
            *base["ranking_diagnostics"],
            *(
                [str(base["provider_incident_warning"])]
                if base.get("provider_incident_warning")
                else []
            ),
            *([str(base["macro_event_warning"])] if base.get("macro_event_warning") else []),
        ],
    }
    if (
        reasons
        or bid is None
        or mid is None
        or ask is None
        or entry_limit is None
        or parity is None
        or es_reference is None
        or invalidation_es is None
    ):
        return _blocked(
            {
                **base,
                "path_kind": path_kind,
                "direction": direction,
                "position_type": position_type,
                "long_contract_id": long_contract_id,
                "short_contract_id": short_contract_id,
                "exact_spread_snapshot": snapshot or None,
                "target_coordinate": parity,
                "invalidation_coordinate": es_reference,
            },
            reasons,
        )
    valid_until = min(
        item
        for item in (
            now + timedelta(seconds=policy.gth_manual_candidate_ttl_seconds),
            source_expires_at,
            candidate_cutoff,
        )
        if item is not None
    )
    if valid_until <= now:
        return _blocked(base, ["candidate_ttl_elapsed"])
    exit_at = min(now + timedelta(minutes=policy.trade_time_stop_minutes), gth_end)
    max_loss = entry_limit * 100.0
    max_profit = (width - entry_limit) * 100.0
    edge_authority, edge_authority_reason = _operator_edge_authority()
    operator_ready = edge_authority == EDGE_AUTHORITY_REQUIRED and edge_authority_reason is None
    selector_only = not operator_ready
    selector_block_reason = edge_authority_reason or EDGE_AUTHORITY_UNAVAILABLE_REASON
    return {
        **base,
        "status": "manual_ready" if operator_ready else "selector_candidate",
        "candidate_scope": "manual_live" if operator_ready else "research_watch",
        "execution_mode": "manual_only" if operator_ready else "observe_only",
        "manual_action_eligible": operator_ready,
        "selector_evidence_eligible": selector_only,
        "operator_action": "manual_limit_only" if operator_ready else "observe_only",
        "operator_notification_eligible": operator_ready,
        "edge_authority": edge_authority,
        "edge_authority_required": EDGE_AUTHORITY_REQUIRED,
        "edge_authority_reason": edge_authority_reason,
        "expiry_payoff_ratio_role": "diagnostic_only",
        "expiry": expiry,
        "valid_until": valid_until.isoformat(),
        "path_kind": path_kind,
        "direction": direction,
        "position_type": position_type,
        "long_contract_id": long_contract_id,
        "short_contract_id": short_contract_id,
        "contract_id": f"{long_contract_id}|-{short_contract_id}",
        "entry_limit": entry_limit,
        "suggested_debit": entry_limit,
        "max_debit": entry_limit,
        "price_increment": NET_DEBIT_PRICE_INCREMENT,
        "price_increment_source": "gth_manual_net_debit_policy",
        "order_type": "NET_DEBIT_LIMIT",
        "entry_rule": "manual_debit_limit_at_or_below_decision_ask",
        "quote_basis": "synthetic_from_leg_nbbo",
        "synthetic_combo_warning": "not_native_combo_bbo",
        "decision_bid": bid,
        "decision_mid": mid,
        "decision_ask": ask,
        "outcome_baselines": {
            "confirmation_time": confirmation_baseline(
                source_mode, source, level_decision, parity, trend_geometry
            ),
            "displayed_ask": {
                "at": now.isoformat(),
                "bid": bid,
                "mid": mid,
                "ask": ask,
                "entry_limit": entry_limit,
                "semantics": "synthetic_leg_nbbo_not_native_combo_fill",
            },
            "quote_reached": {
                "source": "gth_candidate_entry_observation",
                "semantics": "displayed_synthetic_ask_reached_limit_not_broker_fill",
            },
        },
        "spread_width_points": width,
        "max_loss_per_spread": round(max_loss, 2),
        "max_profit_per_spread": round(max_profit, 2),
        "breakeven_spx_at_expiry": round(
            long_strike - entry_limit if right == "P" else long_strike + entry_limit,
            2,
        ),
        "reward_risk_at_limit": round(reward_risk, 4),
        "trigger_level": trigger_level,
        "trigger_coordinate": candidate_trigger_coordinate(
            level_decision, source, trigger_level, trend_geometry
        ),
        "current_parity_spx": float(parity["price"]),
        "current_parity_lower_bound": float(parity["lower_bound"]),
        "current_parity_upper_bound": float(parity["upper_bound"]),
        "target_spx": target_spx,
        "target_wall_kind": target_wall_kind,
        "target_coordinate": parity,
        "invalidation_spx": round(invalidation_spx, 2),
        "invalidation_es": round(invalidation_es, 2),
        "invalidation_coordinate": es_reference,
        "trend_anchor_geometry": trend_geometry,
        "exit_at": exit_at.isoformat(),
        "exact_spread_snapshot": snapshot,
        "spring_gamma": spring_gamma_view,
        "prior_session": prior_session_view,
        "block_reasons": ([] if operator_ready else [selector_block_reason]),
        "signal_absence_reason": None if operator_ready else edge_authority_reason,
        "gate_contract": {
            **base["gate_contract"],
            "hard_block_reasons": [],
            "operator_ready_block_reasons": ([] if operator_ready else [selector_block_reason]),
        },
    }


def process_gth_level_manual_candidate(
    storage: StorageSettings,
    latest: LatestState,
    level_decision: Mapping[str, object],
    *,
    trend_state: Mapping[str, object] | None = None,
    spring_gamma: Mapping[str, object] | None = None,
    macro_event: Mapping[str, object],
    now: datetime,
    policy: MarketFeatureSettings,
    new_entries_allowed: bool,
    new_entries_block_reason: str,
    prior_session: Mapping[str, object] | None = None,
    gth_position_fraction: float | None = None,
    play_stats: PlayOutcomeStats | None = None,
    notification: NotificationSettings | None = None,
    operator_authority: bool = True,
) -> dict[str, object]:
    candidate = evaluate_gth_level_manual_candidate(
        latest,
        level_decision,
        trend_state=trend_state,
        spring_gamma=spring_gamma,
        macro_event=macro_event,
        now=now,
        policy=policy,
        new_entries_allowed=new_entries_allowed,
        new_entries_block_reason=new_entries_block_reason,
        prior_session=prior_session,
        gth_position_fraction=gth_position_fraction,
        play_stats=play_stats,
    )
    if not operator_authority and candidate.get("status") == "manual_ready":
        candidate = {
            **candidate,
            "status": "selector_candidate",
            "candidate_scope": "research_watch",
            "execution_mode": "observe_only",
            "selector_evidence_eligible": True,
            "manual_action_eligible": False,
            "operator_action": "observe_only",
            "operator_notification_eligible": False,
            "execution_eligible": False,
            "action_authority": "none",
            "signal_absence_reason": "strategy_decision_is_final_candidate_owner",
        }
    return _persist_candidate(
        storage,
        candidate,
        now=_utc(now),
        notification=notification,
    )


def _persist_candidate(
    storage: StorageSettings,
    candidate: Mapping[str, object],
    *,
    now: datetime,
    notification: NotificationSettings | None,
) -> dict[str, object]:
    """Compatibility facade for the durable candidate lifecycle runtime."""
    return persist_gth_level_manual_candidate(
        storage,
        candidate,
        now=now,
        notification=notification,
        flush_pending_notifications_fn=flush_pending_notifications,
        cancel_pending_notification_fn=cancel_pending_notification,
        external_ready_receipt_fn=_external_ready_receipt,
        atomic_write_json_secure_fn=atomic_write_json_secure,
    )
