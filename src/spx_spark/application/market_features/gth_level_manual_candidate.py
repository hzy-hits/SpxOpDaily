"""Manual-only GTH candidates from confirmed structural level paths."""

from __future__ import annotations

import hashlib
import math
from datetime import datetime, timedelta
from pathlib import Path
from typing import Mapping

from spx_spark.application.market_features.gth_manual_candidate import (
    NET_DEBIT_PRICE_INCREMENT,
    _blocked,
    _direct_es_reference,
    _gth_bbo_contract_snapshot,
    _gth_end,
    _notification_intent,
    _quote_remaining_seconds,
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
from spx_spark.marketdata import InstrumentId, Provider
from spx_spark.notifier.dispatcher import cancel_pending_notification
from spx_spark.options_map import actionable_chain_implied_reference
from spx_spark.settings.market_features import MarketFeatureSettings
from spx_spark.state_io import (
    append_jsonl_secure,
    atomic_write_json_secure,
    exclusive_state_lock,
    read_json_object,
)
from spx_spark.storage import LatestState
from spx_spark.strategy_contract import policy_version


CONTRACT_VERSION = "gth_level_manual_candidate.v1"
SPREAD_MIN_WIDTH_POINTS = 5.0
SPREAD_DEFAULT_WIDTH_POINTS = 25.0
SPREAD_MAX_WIDTH_POINTS = 40.0


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
    """Build one GTH manual vertical from a confirmed Gamma level path."""

    now = _utc(now)
    # Direct ES trend events are context, not entry locations. They may veto a
    # breakout that fights the established multi-horizon regime, but can never
    # create a candidate without a confirmed wall/flip lifecycle.
    trend_state = trend_state if isinstance(trend_state, Mapping) else {}
    trend_regime = str(trend_state.get("regime") or "unknown")
    level_source_expiry = _time(level_decision.get("expires_at"))
    level_source_ready = bool(
        level_decision.get("formal_signal") is True
        and str(level_decision.get("phase") or "") == "confirmed"
        and level_decision.get("quality_ok") is True
        and level_source_expiry is not None
        and level_source_expiry > now
    )
    source_mode = "level" if level_source_ready else "none"
    source = level_decision if source_mode == "level" else {}
    source_id = str(source.get("event_id") or "")
    source_kind = "gth_confirmed_level_path" if source_mode == "level" else None
    candidate_policy_version = policy_version(
        CONTRACT_VERSION,
        {
            "quote_max_age_seconds": policy.gth_manual_candidate_quote_max_age_seconds,
            "ttl_seconds": policy.gth_manual_candidate_ttl_seconds,
            "max_debit_fraction": policy.gth_manual_candidate_max_debit_fraction,
            "max_net_spread_fraction": (policy.gth_manual_candidate_max_net_spread_fraction),
            "min_parity_pairs": policy.gth_manual_candidate_min_parity_pairs,
            "target_room_buffer_points": (policy.gth_manual_candidate_target_room_buffer_points),
            "min_reward_risk": policy.gth_manual_candidate_min_reward_risk,
            "invalidation_buffer_points": policy.trade_invalidation_buffer_points,
            "time_stop_minutes": policy.trade_time_stop_minutes,
            "spread_width_points": {
                "min": SPREAD_MIN_WIDTH_POINTS,
                "default": SPREAD_DEFAULT_WIDTH_POINTS,
                "max": SPREAD_MAX_WIDTH_POINTS,
            },
        },
    )
    base: dict[str, object] = {
        "schema_version": 1,
        "kind": "gth_spxw_level_manual_spread_candidate",
        "contract_version": CONTRACT_VERSION,
        "candidate_id": None,
        "policy_version": candidate_policy_version,
        "source_signal_id": source_id or None,
        "source_kind": source_kind,
        "evaluated_at": now.isoformat(),
        "status": "observing",
        "candidate_scope": "manual_live",
        "execution_mode": "manual_only",
        "manual_action_eligible": False,
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
        "block_reasons": [],
        "signal_absence_reason": (None if source_id else "no_level_or_trend_source_signal"),
        "gate_contract": {
            "version": "manual_signal_gate.v1",
            "hard_gates": [
                "confirmed_directional_source",
                "gth_session",
                "fresh_ibkr_spxw_two_leg_quote",
                "usable_spx_or_es_basis_coordinate",
                "coherent_risk_geometry",
                "gth_breakout_trend_alignment",
                "positive_historical_edge_when_sampled",
                "minimum_spread_reward_risk",
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
        return _blocked(base, ["source_signal_unavailable"])

    reasons: list[str] = []
    ranking_diagnostics: list[str] = []
    if not DEFAULT_MARKET_CALENDAR.is_spx_gth_open(now):
        reasons.append("spx_gth_session_required")
    if macro_event.get("entry_allowed") is not True:
        base["macro_event_warning"] = str(macro_event.get("mode") or "entry_not_allowed")
    if not new_entries_allowed:
        base["provider_incident_warning"] = new_entries_block_reason

    thesis = str(level_decision.get("thesis") or "")
    direction = str(level_decision.get("direction") or "")
    level_kind = str(level_decision.get("level_kind") or "")
    if thesis == "breakout" and (
        (direction == "down" and trend_regime == "bullish")
        or (direction == "up" and trend_regime == "bearish")
    ):
        reasons.append("gth_trend_regime_opposes_breakout")
    reasons.extend(
        historical_edge_blockers(
            play_stats,
            minimum_winrate=policy.play_stats_min_winrate,
        )
    )
    if play_stats is not None:
        base["play_stats"] = play_stats_payload(play_stats)
    if (
        thesis == "breakout"
        and direction == "down"
        and level_kind in {"flip_low", "put_wall"}
    ):
        right, position_type = "P", "put_debit_spread"
        path_kind = (
            "flip_low_breakdown_put"
            if level_kind == "flip_low"
            else "put_wall_breakdown_put"
        )
    elif thesis == "fade" and direction == "up" and level_kind in {"put_wall", "flip_low"}:
        right, position_type = "C", "call_debit_spread"
        path_kind = "lower_rejection_call"
    elif (
        thesis == "breakout"
        and direction == "up"
        and level_kind in {"flip_high", "call_wall"}
    ):
        right, position_type = "C", "call_debit_spread"
        path_kind = "upper_acceptance_call"
    else:
        return _blocked(base, [*reasons, "unsupported_gth_level_path"])

    session_date = DEFAULT_MARKET_CALENDAR.research_expiry(now)
    expiry = str(level_decision.get("expiry") or "")
    spring_gamma_view = spring_gamma_operator_view(
        spring_gamma,
        now=now,
        expected_expiry=expiry,
    )
    if expiry != session_date.strftime("%Y%m%d"):
        reasons.append("signal_session_mismatch")
    source_expires_at = _time(level_decision.get("expires_at"))
    if source_expires_at is None:
        reasons.append("source_expiry_unavailable")
    elif source_expires_at <= now:
        reasons.append("source_signal_expired")

    levels = level_decision.get("levels")
    levels = levels if isinstance(levels, Mapping) else {}
    trigger_level = _number(level_decision.get("level"))
    if trigger_level is None:
        trigger_level = _number(levels.get(level_kind))
    if trigger_level is None or trigger_level <= 0:
        return _blocked(base, [*reasons, "trigger_level_unavailable"])
    long_strike = round(trigger_level / 5.0) * 5.0
    if right == "P":
        structural_target = _number(levels.get("put_wall"))
        target_spx = (
            structural_target
            if structural_target is not None and structural_target < trigger_level
            else trigger_level - SPREAD_DEFAULT_WIDTH_POINTS
        )
        short_strike = max(
            round(target_spx / 5.0) * 5.0,
            long_strike - SPREAD_MAX_WIDTH_POINTS,
        )
        if short_strike >= long_strike:
            short_strike = long_strike - SPREAD_MIN_WIDTH_POINTS
        basis = _number(level_decision.get("es_basis_points"))
        invalidation_spx = (
            (_number(levels.get("flip_high")) or trigger_level)
            + policy.trade_invalidation_buffer_points
        )
        width = long_strike - short_strike
        target_wall_kind = "put_wall" if structural_target is not None else "time_stop"
    else:
        structural_target = (
            _number(levels.get("flip_low"))
            if path_kind == "lower_rejection_call" and level_kind == "put_wall"
            else _number(levels.get("call_wall"))
        )
        target_spx = (
            structural_target
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
            trigger_level
            if path_kind == "lower_rejection_call"
            else _number(levels.get("flip_low")) or trigger_level
        )
        invalidation_spx = invalidation_anchor - policy.trade_invalidation_buffer_points
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

    long_contract_id = _contract_id(expiry, long_strike, right)
    short_contract_id = _contract_id(expiry, short_strike, right)
    identity = "|".join(
        (
            CONTRACT_VERSION,
            candidate_policy_version,
            path_kind,
            long_contract_id,
            short_contract_id,
        )
    )
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

    es_reference = _direct_es_reference(
        latest,
        now=now,
        max_age_seconds=policy.gth_manual_candidate_quote_max_age_seconds,
    )
    basis = _number(level_decision.get("es_basis_points"))
    if es_reference is None:
        reasons.append("direct_es_invalidation_unavailable")
    if basis is None:
        reasons.append("es_basis_unavailable")
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
        reasons.append("spread_reward_risk_insufficient")
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

    quote_remaining = _quote_remaining_seconds(
        snapshot,
        parity=parity,
        es_reference=es_reference,
        now=now,
        max_age_seconds=policy.gth_manual_candidate_quote_max_age_seconds,
    )
    valid_until = min(
        item
        for item in (
            now
            + timedelta(
                seconds=min(
                    policy.gth_manual_candidate_ttl_seconds,
                    quote_remaining,
                )
            ),
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
    return {
        **base,
        "status": "manual_ready",
        "manual_action_eligible": True,
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
        "spread_width_points": width,
        "max_loss_per_spread": round(max_loss, 2),
        "max_profit_per_spread": round(max_profit, 2),
        "breakeven_spx_at_expiry": round(
            long_strike - entry_limit if right == "P" else long_strike + entry_limit,
            2,
        ),
        "reward_risk_at_limit": round(reward_risk, 4),
        "trigger_level": trigger_level,
        "trigger_coordinate": dict(
            level_decision.get("trigger_coordinate")
            if isinstance(level_decision.get("trigger_coordinate"), Mapping)
            else {}
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
        "exit_at": exit_at.isoformat(),
        "exact_spread_snapshot": snapshot,
        "spring_gamma": spring_gamma_view,
        "prior_session": prior_session_view,
        "block_reasons": [],
        "signal_absence_reason": None,
        "gate_contract": {
            **base["gate_contract"],
            "hard_block_reasons": [],
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
    state_path = Path(storage.data_root) / "latest" / "gth_level_manual_candidate_state.json"
    projection_path = Path(storage.data_root) / "latest" / "gth_level_manual_candidate.json"
    candidate = dict(candidate)
    notification_event_id: str | None = None
    settings = notification or NotificationSettings.from_env()
    with exclusive_state_lock(state_path):
        state = read_json_object(state_path)
        active_plan = (
            dict(state.get("active_manual_plan"))
            if isinstance(state.get("active_manual_plan"), Mapping)
            else {}
        )
        candidate, active_plan = _apply_active_plan_coherence(
            candidate,
            active_plan,
            now=now,
        )
        notification_event_id = (
            f"{candidate['candidate_id']}:ready"
            if candidate.get("status") == "manual_ready"
            else None
        )
        gate_record_key, gate_record = _gate_record(candidate, now=now)
        if state.get("last_gate_record_key") != gate_record_key:
            append_jsonl_secure(
                Path(storage.data_root)
                / "features"
                / "gth_manual_signal_gates"
                / f"date={now.date().isoformat()}"
                / "events.jsonl",
                gate_record,
            )
        accepted = {
            str(item)
            for item in (
                list(state.get("accepted_notification_event_ids") or [])
                + list(state.get("notified_event_ids") or [])
            )
            if item
        }
        settled = {str(item) for item in state.get("settled_notification_event_ids") or [] if item}
        pending = [
            dict(item)
            for item in state.get("pending_notifications") or []
            if isinstance(item, Mapping)
        ]
        lifecycle_events = {
            str(item.get("event_id") or ""): str(item.get("source_signal_id") or "")
            for item in state.get("notification_lifecycle_events") or []
            if isinstance(item, Mapping) and item.get("event_id") and item.get("source_signal_id")
        }
        cancellation_pending = {
            str(item)
            for item in state.get("pending_notification_cancellation_event_ids") or []
            if item
        }
        previous = state.get("last_candidate")
        if isinstance(previous, Mapping):
            candidate_id = str(previous.get("candidate_id") or "")
            source_id = str(previous.get("source_signal_id") or "")
            if candidate_id and source_id:
                lifecycle_events.setdefault(f"{candidate_id}:ready", source_id)
        for item in pending:
            event_id = str(item.get("event_id") or "")
            source_id = str(item.get("source_signal_id") or "")
            if event_id and source_id:
                lifecycle_events.setdefault(event_id, source_id)
        if notification_event_id:
            lifecycle_events.setdefault(
                notification_event_id,
                str(candidate.get("source_signal_id") or ""),
            )
        if candidate.get("status") != "manual_ready":
            cancellation_pending.update(lifecycle_events)
        for event_id in sorted(cancellation_pending):
            try:
                cancel_pending_notification(
                    settings,
                    event_id,
                    now=now,
                    reason="source_candidate_no_longer_manual_ready",
                )
            except Exception:
                continue
            cancellation_pending.discard(event_id)
            settled.add(event_id)
            accepted.discard(event_id)
            lifecycle_events.pop(event_id, None)
            pending = [item for item in pending if str(item.get("event_id") or "") != event_id]
        pending_ids = {str(item.get("event_id") or "") for item in pending}
        if (
            notification_event_id
            and not cancellation_pending
            and notification_event_id not in accepted
            and notification_event_id not in settled
            and notification_event_id not in pending_ids
        ):
            pending.append(
                _notification_intent(
                    candidate,
                    event_id=notification_event_id,
                    now=now,
                )
            )
        state.update(
            {
                "schema_version": 1,
                "updated_at": now.isoformat(),
                "last_gate_record_key": gate_record_key,
                "last_candidate": dict(candidate),
                "accepted_notification_event_ids": sorted(accepted)[-200:],
                "settled_notification_event_ids": sorted(settled)[-200:],
                "pending_notifications": pending,
                "notification_lifecycle_events": [
                    {"event_id": event_id, "source_signal_id": source_id}
                    for event_id, source_id in sorted(lifecycle_events.items())[-200:]
                ],
                "pending_notification_cancellation_event_ids": sorted(cancellation_pending)[-200:],
                "active_manual_plan": active_plan,
            }
        )
        atomic_write_json_secure(state_path, state)
        atomic_write_json_secure(projection_path, candidate)
    result = {"attempted": False, "accepted": False}
    if notification_event_id:
        result = flush_pending_notifications(
            state_path,
            settings=settings,
            now=now,
            only_event_id=notification_event_id,
        )
    if notification_event_id and result.get("accepted") is True:
        with exclusive_state_lock(state_path):
            state = read_json_object(state_path)
            state["active_manual_plan"] = {
                "candidate_id": candidate.get("candidate_id"),
                "direction": candidate.get("direction"),
                "path_kind": candidate.get("path_kind"),
                "invalidation_spx": candidate.get("invalidation_spx"),
                "target_spx": candidate.get("target_spx"),
                "exit_at": candidate.get("exit_at"),
                "activated_at": now.isoformat(),
            }
            atomic_write_json_secure(state_path, state)
    return {
        **candidate,
        "notification_attempted": bool(result.get("attempted")),
        "notification_accepted": bool(result.get("accepted")),
        "notification_outcome": result.get("outcome"),
    }


def _apply_active_plan_coherence(
    candidate: Mapping[str, object],
    active_plan: Mapping[str, object],
    *,
    now: datetime,
) -> tuple[dict[str, object], dict[str, object]]:
    """Prevent an opposite card until the prior operator plan is resolved."""

    candidate = dict(candidate)
    active = dict(active_plan)
    if candidate.get("status") != "manual_ready" or not active:
        return candidate, active
    direction = str(candidate.get("direction") or "")
    active_direction = str(active.get("direction") or "")
    if direction not in {"up", "down"} or active_direction not in {"up", "down"}:
        return candidate, active
    if direction == active_direction:
        return candidate, active
    release_reason = _active_plan_release_reason(
        active,
        current_spx=_number(candidate.get("current_parity_spx")),
        now=now,
    )
    if release_reason is None:
        reasons = [
            *[str(item) for item in candidate.get("block_reasons") or ()],
            "opposite_signal_conflicts_with_active_plan",
        ]
        gate_contract = candidate.get("gate_contract")
        gate_contract = dict(gate_contract) if isinstance(gate_contract, Mapping) else {}
        return (
            {
                **candidate,
                "status": "blocked",
                "manual_action_eligible": False,
                "signal_absence_reason": "active_manual_plan_not_invalidated",
                "block_reasons": list(dict.fromkeys(reasons)),
                "active_manual_plan": active,
                "gate_contract": {
                    **gate_contract,
                    "hard_block_reasons": list(dict.fromkeys(reasons)),
                },
            },
            active,
        )
    return (
        {
            **candidate,
            "replaces_prior_plan": {
                "candidate_id": active.get("candidate_id"),
                "direction": active_direction,
                "release_reason": release_reason,
            },
        },
        {},
    )


def _active_plan_release_reason(
    active_plan: Mapping[str, object],
    *,
    current_spx: float | None,
    now: datetime,
) -> str | None:
    exit_at = _time(active_plan.get("exit_at"))
    if exit_at is not None and now >= exit_at:
        return "time_exit_elapsed"
    if current_spx is None:
        return None
    direction = str(active_plan.get("direction") or "")
    invalidation = _number(active_plan.get("invalidation_spx"))
    target = _number(active_plan.get("target_spx"))
    if direction == "down":
        if invalidation is not None and current_spx >= invalidation:
            return "prior_put_invalidated"
        if target is not None and current_spx <= target:
            return "prior_put_target_reached"
    elif direction == "up":
        if invalidation is not None and current_spx <= invalidation:
            return "prior_call_invalidated"
        if target is not None and current_spx >= target:
            return "prior_call_target_reached"
    return None


def _gate_record(
    candidate: Mapping[str, object],
    *,
    now: datetime,
) -> tuple[str, dict[str, object]]:
    identity = "|".join(
        (
            now.date().isoformat(),
            str(candidate.get("source_signal_id") or "none"),
            str(candidate.get("candidate_id") or "none"),
            str(candidate.get("status") or "unknown"),
            str(candidate.get("path_kind") or "none"),
            ",".join(str(item) for item in candidate.get("block_reasons") or ()),
        )
    )
    digest = hashlib.sha256(identity.encode()).hexdigest()[:24]
    gate_contract = candidate.get("gate_contract")
    return digest, {
        "schema_version": 1,
        "record_id": f"gth-manual-gate:{digest}",
        "recorded_at": now.isoformat(),
        "source_signal_id": candidate.get("source_signal_id"),
        "source_kind": candidate.get("source_kind"),
        "candidate_id": candidate.get("candidate_id"),
        "status": candidate.get("status"),
        "path_kind": candidate.get("path_kind"),
        "direction": candidate.get("direction"),
        "long_contract_id": candidate.get("long_contract_id"),
        "short_contract_id": candidate.get("short_contract_id"),
        "signal_absence_reason": candidate.get("signal_absence_reason"),
        "block_reasons": list(candidate.get("block_reasons") or ()),
        "gate_contract": (dict(gate_contract) if isinstance(gate_contract, Mapping) else None),
        "session_quote_provider": "ibkr",
    }


def _contract_id(expiry: str, strike: float, right: str) -> str:
    return InstrumentId.option(
        "SPX",
        expiry=expiry,
        strike=strike,
        right=right,
        trading_class="SPXW",
    ).canonical_id
