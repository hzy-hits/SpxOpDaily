"""Deterministic conversion from a confirmed level path to one executable intent."""

from __future__ import annotations

import hashlib
import math
from datetime import datetime, time, timedelta, timezone
from typing import Mapping
from zoneinfo import ZoneInfo

from spx_spark.application.market_features.models import (
    DecisionContext,
    FrameQuality,
    MinuteMarketFrame,
    OptionStructureFrame,
)
from spx_spark.application.market_features.play_outcome_stats import PlayOutcomeStats
from spx_spark.application.order_map.execution_quote import evaluate_execution_quote
from spx_spark.application.order_map.models import level_decision_play
from spx_spark.application.order_map.pricing import expiry_close_utc, round_to_tick
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.settings.market_features import MarketFeatureSettings
from spx_spark.settings.order_map import OrderMapPolicy
from spx_spark.storage import LatestState
from spx_spark.strategy_contract import (
    policy_version,
    strategy_contract_issues,
    strategy_event_fields,
)


HARD_CONTEXT_INVALIDATIONS = frozenset(
    {
        "es_path_unavailable",
        "option_structure_unavailable",
        "es_spy_direction_divergent",
        "hot_option_liquidity_low",
    }
)

ET = ZoneInfo("America/New_York")
ENTRY_WINDOW_START_ET = time(9, 45)
HARD_EXIT_ET = time(13, 0)
CALL_BREAKOUT_PILOT_LANE = "long_0dte_rth_upside_breakout_pilot"
FLIP_LOW_BREAKDOWN_PUT_SHADOW_LANE = "long_0dte_rth_flip_low_breakdown_put_shadow"
UPPER_REJECTION_PUT_SHADOW_LANE = "long_0dte_rth_upper_rejection_put_shadow"
PUT_WALL_BREAKDOWN_DISABLED_LANE = "long_0dte_rth_put_wall_breakdown_disabled"
TRADE_INTENT_CONTRACT_VERSION = "rth_lanes_0945_1300_put_shadow.v1"


def live_trade_intent_authority_issues(
    intent: Mapping[str, object],
) -> tuple[str, ...]:
    """Return fail-closed reasons before any live-plan or virtual consumer."""

    issues: list[str] = []
    if intent.get("status") != "trade_ready":
        issues.append("trade_intent_not_trade_ready")
    if intent.get("execution_eligible") is not True:
        issues.append("trade_intent_execution_authority_missing")
    if intent.get("quote_observation_eligible") is not False:
        issues.append("trade_intent_quote_observation_only")
    if intent.get("shadow_mode") is not False:
        issues.append("trade_intent_shadow_mode")
    if intent.get("automatic_ordering") is not False:
        issues.append("trade_intent_automatic_ordering_contract_invalid")
    if intent.get("strategy_lane") in {
        FLIP_LOW_BREAKDOWN_PUT_SHADOW_LANE,
        UPPER_REJECTION_PUT_SHADOW_LANE,
        PUT_WALL_BREAKDOWN_DISABLED_LANE,
    }:
        issues.append("put_lane_live_execution_forbidden")
    if intent.get("strategy_lane") not in {
        CALL_BREAKOUT_PILOT_LANE,
        "rth_confirmed_level",
    }:
        issues.append("trade_intent_live_lane_not_approved")
    if intent.get("direction") != "up":
        issues.append("trade_intent_live_direction_not_call")
    if not str(intent.get("contract_id") or "").endswith(":C"):
        issues.append("trade_intent_live_contract_not_call")
    return tuple(dict.fromkeys(issues))


def trade_intent_policy_version(
    feature_policy: MarketFeatureSettings,
    order_policy: OrderMapPolicy,
) -> str:
    """Hash settings together with the executable lane and clock contract."""

    return policy_version(
        "rth_trade_intent.v3",
        {
            "market_features": feature_policy,
            "order_map": order_policy,
            "decision_contract": {
                "version": TRADE_INTENT_CONTRACT_VERSION,
                "entry_window_start_et": ENTRY_WINDOW_START_ET.isoformat(timespec="minutes"),
                "entry_window_end_et": HARD_EXIT_ET.isoformat(timespec="minutes"),
                "entry_window_end_inclusive": False,
                "hard_exit_et": HARD_EXIT_ET.isoformat(timespec="minutes"),
                "call_trade_ready_lane": CALL_BREAKOUT_PILOT_LANE,
                "put_shadow_lanes": (
                    FLIP_LOW_BREAKDOWN_PUT_SHADOW_LANE,
                    UPPER_REJECTION_PUT_SHADOW_LANE,
                ),
                "put_shadow_status": "shadow_ready",
                "put_shadow_execution_eligible": False,
                "put_shadow_quote_observation_eligible": True,
                "put_wall_breakdown_lane": PUT_WALL_BREAKDOWN_DISABLED_LANE,
                "put_wall_breakdown_enabled": False,
            },
        },
    )


def evaluate_trade_intent(
    context: DecisionContext,
    market: MinuteMarketFrame,
    options: OptionStructureFrame,
    latest: LatestState,
    repricing: Mapping[str, object],
    *,
    now: datetime,
    feature_policy: MarketFeatureSettings,
    order_policy: OrderMapPolicy,
    play_stats: PlayOutcomeStats | None = None,
) -> dict[str, object]:
    """Fail closed unless the signal, direction and live option quote all agree."""

    now = _utc(now)
    level = context.level_decision
    pilot_enabled = feature_policy.trade_confirmed_pilot_enabled
    event_id = str(level.get("event_id") or "")
    phase = str(level.get("phase") or "far")
    thesis = str(level.get("thesis") or "none")
    direction = str(level.get("direction") or "")
    level_kind = str(level.get("level_kind") or "")
    trigger_level = _number(level.get("level"))
    event_expires_at = _datetime(level.get("expires_at"))
    entry_window_start_at, hard_exit_at = _strategy_window(now)
    valid_until = min(event_expires_at, hard_exit_at) if event_expires_at is not None else None
    strategy_lane, put_shadow_lane, priority, pilot_scope_blocker = _pilot_scope(
        pilot_enabled=pilot_enabled,
        thesis=thesis,
        direction=direction,
        level_kind=level_kind,
    )
    raw_coordinate = level.get("trigger_coordinate")
    coordinate = dict(raw_coordinate) if isinstance(raw_coordinate, Mapping) else None
    intent_policy_version = trade_intent_policy_version(feature_policy, order_policy)
    play = level_decision_play(thesis, direction)
    moving_average_context = _moving_average_context(market)
    semantic_scope = (
        "|".join((context.session_id, play, f"{trigger_level:.4f}"))
        if play is not None and trigger_level is not None
        else None
    )
    base: dict[str, object] = {
        **strategy_event_fields(
            policy_version_value=intent_policy_version,
            valid_until=valid_until,
            coordinate=coordinate,
            block_reasons=(),
        ),
        "status": "observing",
        "event_id": event_id or None,
        "context_id": context.context_id,
        "session_id": context.session_id,
        "phase": phase,
        "thesis": thesis,
        "direction": direction or None,
        "level_kind": level_kind or None,
        "semantic_scope": semantic_scope,
        "evaluated_at": now.isoformat(),
        "block_reasons": [],
        "strategy_lane": strategy_lane,
        "pilot_mode": pilot_enabled,
        "shadow_mode": put_shadow_lane,
        "wall_signal": (
            "present"
            if event_id and phase == "confirmed" and thesis in {"breakout", "fade"}
            else "absent"
        ),
        "execution_eligible": False,
        "quote_observation_eligible": False,
        "priority": priority,
        "trade_intent_contract_version": TRADE_INTENT_CONTRACT_VERSION,
        "entry_window_start_at": entry_window_start_at.isoformat(),
        "hard_exit_at": hard_exit_at.isoformat(),
        "moving_average_context": moving_average_context,
    }
    if not event_id or phase != "confirmed" or thesis not in {"breakout", "fade"}:
        return base

    reasons: list[str] = []
    if not DEFAULT_MARKET_CALENDAR.is_rth_open(now):
        reasons.append("rth_session_required")
    if now < entry_window_start_at:
        reasons.append("strategy_entry_window_not_open")
    elif now >= hard_exit_at:
        reasons.append("strategy_entry_window_closed")
    if level.get("formal_signal_enabled") is not True:
        reasons.append("formal_signal_disabled")
    if level.get("formal_signal") is not True:
        reasons.append("formal_signal_unavailable")
    if level.get("quality_ok") is not True:
        reasons.append("level_observation_quality_failed")
    reasons.extend(
        issue
        for issue in strategy_contract_issues(
            base,
            require_actionable_coordinate=True,
        )
        if issue.startswith("coordinate_")
    )
    if direction not in {"up", "down"}:
        reasons.append("direction_unavailable")
    if pilot_scope_blocker is not None:
        reasons.append(pilot_scope_blocker)
    direction_sign = 1 if direction == "up" else -1

    confirmed_at = _datetime(level.get("phase_at") or level.get("confirmed_at"))
    if confirmed_at is None:
        reasons.append("confirmed_at_unavailable")
        confirmation_age = None
    else:
        if not DEFAULT_MARKET_CALENDAR.is_rth_open(confirmed_at):
            reasons.append("rth_confirmation_required")
        confirmation_age = max((now - confirmed_at).total_seconds(), 0.0)
        if confirmation_age < feature_policy.trade_follow_through_seconds:
            reasons.append("follow_through_hold_pending")

    if event_expires_at is None:
        reasons.append("level_event_expiry_unavailable")
    elif now >= event_expires_at:
        reasons.append("level_event_expired")

    reasons.extend(
        _market_anchor_blockers(
            context,
            market,
            options,
            now=now,
            policy=feature_policy,
            pilot_enabled=pilot_enabled,
        )
    )

    spot = _number(level.get("spot"))
    expected_move = _number(options.volatility.get("expected_move_points_0dte"))
    if expected_move is None and not pilot_enabled:
        reasons.append("expected_move_unavailable")
    expiry_close_at = expiry_close_utc(options.front_expiry or "")
    if expiry_close_at is None:
        reasons.append("expiry_close_unavailable")
    elif now >= expiry_close_at:
        reasons.append("expiry_closed")
    follow_threshold = max(
        feature_policy.trade_follow_through_min_points,
        (expected_move or 0.0) * feature_policy.trade_follow_through_em_fraction,
    )
    follow_move = (
        direction_sign * (spot - trigger_level)
        if spot is not None and trigger_level is not None
        else None
    )
    if follow_move is None:
        reasons.append("follow_through_price_unavailable")
    elif follow_move < follow_threshold:
        reasons.append("follow_through_distance_pending")

    hard_invalidations = (
        HARD_CONTEXT_INVALIDATIONS - {"es_spy_direction_divergent", "hot_option_liquidity_low"}
        if pilot_enabled
        else HARD_CONTEXT_INVALIDATIONS
    )
    reasons.extend(item for item in context.invalidations if item in hard_invalidations)
    if context.macro_event.get("mode") == "pre_event":
        reasons.append("macro_event_pre_release_entry_block")
    reasons.extend(
        _direction_blockers(
            context,
            market,
            thesis=thesis,
            direction=direction,
            pilot_enabled=pilot_enabled,
        )
    )
    pilot_diagnostics = (
        _pilot_diagnostics(
            context,
            market,
            options,
            thesis=thesis,
            direction=direction,
        )
        if pilot_enabled
        else []
    )

    candidate = _matching_candidate(
        repricing,
        event_id=event_id,
        play=play,
        now=now,
        max_age_seconds=feature_policy.trade_repricing_max_age_seconds,
        future_tolerance_seconds=feature_policy.provider_sync_tolerance_seconds,
        expected_expiry=options.front_expiry,
        reasons=reasons,
    )
    quote = None
    quote_gate = None
    if candidate is not None:
        contract_id = str(candidate.get("contract_id") or "")
        quote = latest.best_quote(contract_id) if contract_id else None
        if quote is None:
            reasons.append("execution_quote_unavailable")
        else:
            quote_gate = evaluate_execution_quote(
                quote,
                latest.quotes,
                as_of=now,
                policy=order_policy,
            )
            reasons.extend(quote_gate.reasons)
            if (
                quote_gate.transport_age_seconds is None
                or quote_gate.transport_age_seconds > feature_policy.trade_quote_max_age_seconds
            ):
                reasons.append("trade_transport_quote_stale")
            if (
                quote_gate.source_age_seconds is None
                or quote_gate.source_age_seconds > feature_policy.trade_quote_max_age_seconds
            ):
                reasons.append("trade_source_quote_stale")
            expected_right = "C" if direction == "up" else "P"
            candidate_right = str(candidate.get("right") or "").upper()
            quote_right = quote.instrument.right.value if quote.instrument.right else ""
            if candidate_right != expected_right or quote_right != expected_right:
                reasons.append("contract_direction_mismatch")
            if quote.instrument.expiry != options.front_expiry:
                reasons.append("contract_expiry_mismatch")
            reasons.extend(
                _timestamp_blockers(
                    source_at=quote.quote_time or quote.trade_time,
                    transport_at=quote.last_update_at or quote.received_at,
                    now=now,
                    max_age_seconds=feature_policy.trade_quote_max_age_seconds,
                    future_tolerance_seconds=feature_policy.provider_sync_tolerance_seconds,
                    prefix="trade_quote",
                )
            )

    invalidation = (
        trigger_level - direction_sign * feature_policy.trade_invalidation_buffer_points
        if trigger_level is not None
        else None
    )
    target = (
        _target_spx(
            options,
            spot=spot,
            trigger_level=trigger_level,
            direction=direction_sign,
            expected_move=expected_move,
            policy=feature_policy,
        )
        if spot is not None and trigger_level is not None
        else None
    )
    target_room = (
        direction_sign * (target - spot) if target is not None and spot is not None else None
    )
    invalidation_distance = (
        direction_sign * (spot - invalidation)
        if spot is not None and invalidation is not None
        else None
    )
    reward_risk = (
        target_room / invalidation_distance
        if target_room is not None
        and invalidation_distance is not None
        and invalidation_distance > 0
        else None
    )
    if target_room is None:
        reasons.append("target_room_unavailable")
    elif target_room < feature_policy.trade_min_target_room_points:
        reasons.append("remaining_target_room_insufficient")
    if invalidation_distance is None or invalidation_distance <= 0:
        reasons.append("invalidation_distance_unavailable")
    elif reward_risk is None or reward_risk < feature_policy.trade_min_reward_risk:
        reasons.append("remaining_reward_risk_insufficient")

    unique_reasons = list(dict.fromkeys(reasons))
    if unique_reasons or candidate is None or quote is None or quote_gate is None:
        return {
            **base,
            "status": "blocked",
            "play": play,
            "confirmation_age_seconds": confirmation_age,
            "follow_through_points": follow_move,
            "follow_through_required_points": follow_threshold,
            "spx_spot": spot,
            "trigger_level": trigger_level,
            "invalidation_spx": (round(invalidation, 2) if invalidation is not None else None),
            "target_spx": round(target, 2) if target is not None else None,
            "remaining_target_room_points": target_room,
            "invalidation_distance_points": invalidation_distance,
            "remaining_reward_risk": reward_risk,
            "pilot_diagnostics": pilot_diagnostics,
            "block_reasons": unique_reasons or ["candidate_unavailable"],
        }

    bid = quote_gate.bid
    ask = quote_gate.ask
    mid = quote_gate.mid
    if bid is None or ask is None or mid is None:
        return {**base, "status": "blocked", "block_reasons": ["not_two_sided"]}
    entry_limit = round_to_tick(
        min(mid, bid + feature_policy.trade_entry_spread_fraction * (ask - bid))
    )
    assert invalidation is not None
    assert target is not None
    intent_expires_at = now + timedelta(
        seconds=min(
            feature_policy.trade_entry_window_seconds,
            feature_policy.trade_intent_ttl_seconds,
        )
    )
    if event_expires_at is not None:
        intent_expires_at = min(intent_expires_at, event_expires_at)
    assert expiry_close_at is not None
    intent_expires_at = min(intent_expires_at, expiry_close_at)
    intent_expires_at = min(intent_expires_at, hard_exit_at)
    time_stop_at = min(
        now + timedelta(minutes=feature_policy.trade_time_stop_minutes),
        expiry_close_at,
        hard_exit_at,
    )
    contract_id = str(candidate["contract_id"])
    assert semantic_scope is not None
    semantic_key = "|".join((semantic_scope, contract_id))
    token = semantic_key
    intent_id = "intent:" + hashlib.sha256(token.encode()).hexdigest()[:24]
    source_at = quote.quote_time or quote.trade_time or quote.last_update_at or quote.received_at
    payload: dict[str, object] = {
        **base,
        **strategy_event_fields(
            policy_version_value=intent_policy_version,
            valid_until=intent_expires_at,
            coordinate=coordinate,
            block_reasons=(),
        ),
        "status": "shadow_ready" if put_shadow_lane else "trade_ready",
        "execution_eligible": not put_shadow_lane,
        "quote_observation_eligible": put_shadow_lane,
        "intent_id": intent_id,
        "semantic_key": semantic_key,
        "play": play,
        "contract_id": contract_id,
        "contract_label": _contract_label(candidate),
        "provider": quote.provider.value,
        "quote_source_at": _utc(source_at).isoformat(),
        "decision_bid": bid,
        "decision_ask": ask,
        "decision_mid": mid,
        "entry_limit": entry_limit,
        "entry_rule": "bid_plus_spread_fraction_capped_at_mid",
        "entry_spread_fraction": feature_policy.trade_entry_spread_fraction,
        "spx_spot": spot,
        "trigger_level": trigger_level,
        "invalidation_spx": round(invalidation, 2),
        "target_spx": round(target, 2),
        "remaining_target_room_points": target_room,
        "invalidation_distance_points": invalidation_distance,
        "remaining_reward_risk": reward_risk,
        "confirmation_age_seconds": confirmation_age,
        "follow_through_points": follow_move,
        "follow_through_required_points": follow_threshold,
        "time_stop_at": time_stop_at.isoformat(),
        "expires_at": intent_expires_at.isoformat(),
        "max_loss_per_contract": round(entry_limit * 100.0, 2),
        "quantity": None,
        "quantity_policy": "operator_selected",
        "automatic_ordering": False,
        "promotion_status": "collecting_shadow" if put_shadow_lane else "reviewed_pilot",
        "pilot_diagnostics": pilot_diagnostics,
        "evidence": _evidence(context),
        "block_reasons": [],
    }
    if play_stats is not None:
        payload["play_stats"] = _play_stats_payload(play_stats)
    return payload


def _direction_blockers(
    context: DecisionContext,
    market: MinuteMarketFrame,
    *,
    thesis: str,
    direction: str,
    pilot_enabled: bool = False,
) -> list[str]:
    reasons: list[str] = []
    sign = 1 if direction == "up" else -1
    episode = context.session_episode
    episode_phase = str(episode.get("phase") or "observing")
    episode_direction = str(episode.get("reversal_direction") or "")
    if (
        episode_phase in {"v_reversal_confirmed", "recovery"}
        and episode_direction in {"up", "down"}
        and episode_direction != direction
    ):
        reasons.append("session_episode_direction_conflict")
    regime = context.regime_decision
    regime_direction = str(regime.get("direction") or "none")
    if (
        regime.get("mode") == "trending"
        and regime_direction in {"up", "down"}
        and regime_direction != direction
    ):
        reasons.append("regime_direction_conflict")
    if pilot_enabled:
        if thesis == "breakout":
            breakout_verdict = str(context.breakout_filter.get("verdict") or "unavailable")
            if breakout_verdict == "blocked":
                reasons.append("breakout_filter_blocked")
        one_minute = _number(market.es.get("return_1m_points"))
        five_minute = _number(market.es.get("return_5m_points"))
        if (
            one_minute is not None
            and five_minute is not None
            and one_minute * sign <= 0
            and five_minute * sign <= 0
        ):
            reasons.append("es_1m_5m_jointly_oppose_direction")
        return reasons
    if thesis == "breakout":
        breakout = context.breakout_filter
        if breakout.get("verdict") != "supported" or breakout.get("actionable") is not True:
            reasons.append("breakout_filter_not_supported")
    elif str(regime.get("mode") or "") != "mean_reverting":
        reasons.append("fade_regime_not_mean_reverting")

    for horizon in ("return_1m_points", "return_5m_points"):
        value = _number(market.es.get(horizon))
        if value is None:
            reasons.append(f"es_{horizon}_unavailable")
        elif value * sign <= 0:
            reasons.append(f"es_{horizon}_opposes_direction")

    price_volume = str(market.volume.get("price_volume_alignment_5m") or "unavailable")
    price_return_5m = _number(market.es.get("return_5m_points"))
    if (
        price_volume != "price_volume_aligned"
        or price_return_5m is None
        or price_return_5m * sign <= 0
    ):
        reasons.append("price_volume_not_directionally_aligned")

    cross = str(market.cross_asset.get("es_spy_direction_confirmation_15m") or "unavailable")
    if DEFAULT_MARKET_CALENDAR.is_rth_open(context.as_of):
        es_return_15m = _number(market.es.get("return_15m_points"))
        if cross != "confirmed":
            reasons.append(
                "es_spy_direction_divergent"
                if cross == "divergent"
                else "rth_spy_confirmation_unavailable"
            )
        elif es_return_15m is None or es_return_15m * sign <= 0:
            reasons.append("es_spy_confirmation_opposes_direction")
    return reasons


def _matching_candidate(
    repricing: Mapping[str, object],
    *,
    event_id: str,
    play: str | None,
    now: datetime,
    max_age_seconds: float,
    future_tolerance_seconds: float,
    expected_expiry: str | None,
    reasons: list[str],
) -> Mapping[str, object] | None:
    if str(repricing.get("event_id") or "") != event_id:
        reasons.append("repricing_event_mismatch")
        return None
    observed_at = _datetime(repricing.get("as_of"))
    observed_age = (now - observed_at).total_seconds() if observed_at is not None else None
    if observed_age is None or observed_age > max_age_seconds:
        reasons.append("repricing_stale")
        return None
    if observed_age < -future_tolerance_seconds:
        reasons.append("repricing_timestamp_in_future")
        return None
    if expected_expiry is None or str(repricing.get("expiry") or "") != expected_expiry:
        reasons.append("repricing_expiry_mismatch")
        return None
    candidates = [item for item in repricing.get("candidates") or [] if isinstance(item, Mapping)]
    matches = [item for item in candidates if item.get("play") == play]
    if len(matches) != 1:
        reasons.append("unique_direction_candidate_unavailable")
        return None
    candidate = matches[0]
    if candidate.get("execution_quote_status") != "executable":
        reasons.append("repricing_quote_not_executable")
        reasons.extend(str(item) for item in candidate.get("execution_quote_reasons") or [])
    return candidate


def _market_anchor_blockers(
    context: DecisionContext,
    market: MinuteMarketFrame,
    options: OptionStructureFrame,
    *,
    now: datetime,
    policy: MarketFeatureSettings,
    pilot_enabled: bool = False,
) -> list[str]:
    reasons: list[str] = []
    if market.quality is not FrameQuality.READY:
        reasons.append("market_frame_not_ready")
    if options.quality is not FrameQuality.READY:
        reasons.append("option_structure_not_ready")
    if options.l1.quality is not FrameQuality.READY and not pilot_enabled:
        reasons.append("option_l1_not_ready")
    expected_expiry = context.session_id.replace("-", "")
    level_expiry = str(context.level_decision.get("expiry") or "")
    if options.front_expiry != expected_expiry or level_expiry != expected_expiry:
        reasons.append("decision_session_expiry_mismatch")
    if market.session_id != context.session_id:
        reasons.append("market_session_mismatch")
    level_kind = str(context.level_decision.get("level_kind") or "")
    frozen_level = _number(context.level_decision.get("level"))
    live_level = _current_structure_level(options, level_kind)
    if frozen_level is None or live_level is None:
        reasons.append("current_trigger_level_unavailable")
    elif abs(live_level - frozen_level) > policy.trade_structure_drift_points:
        reasons.append("trigger_structure_drift")
    reasons.extend(
        _timestamp_blockers(
            source_at=_datetime(market.es.get("source_at")),
            transport_at=_datetime(market.es.get("transport_at")),
            now=now,
            max_age_seconds=policy.trade_market_anchor_max_age_seconds,
            future_tolerance_seconds=policy.provider_sync_tolerance_seconds,
            prefix="es_anchor",
        )
    )
    observed_at = _datetime(market.es.get("observed_at"))
    level_updated_at = _datetime(context.level_decision.get("updated_at"))
    for label, timestamp in (
        ("es_anchor_observation", observed_at),
        ("level_observation", level_updated_at),
    ):
        reasons.extend(
            _single_timestamp_blockers(
                timestamp,
                now=now,
                max_age_seconds=policy.trade_market_anchor_max_age_seconds,
                future_tolerance_seconds=policy.provider_sync_tolerance_seconds,
                prefix=label,
            )
        )
    return reasons


def _pilot_diagnostics(
    context: DecisionContext,
    market: MinuteMarketFrame,
    options: OptionStructureFrame,
    *,
    thesis: str,
    direction: str,
) -> list[str]:
    """Return redundant context checks as labels, not pilot vetoes."""

    sign = 1 if direction == "up" else -1
    diagnostics: list[str] = []
    if thesis == "breakout":
        breakout = context.breakout_filter
        breakout_verdict = str(breakout.get("verdict") or "unavailable")
        if breakout_verdict == "blocked":
            diagnostics.append("breakout_filter_blocked")
        elif breakout_verdict != "supported" or breakout.get("actionable") is not True:
            diagnostics.append("breakout_filter_not_supported")
    for horizon in ("return_1m_points", "return_5m_points"):
        value = _number(market.es.get(horizon))
        if value is None:
            diagnostics.append(f"es_{horizon}_unavailable")
        elif value * sign <= 0:
            diagnostics.append(f"es_{horizon}_opposes_direction")
    if market.volume.get("price_volume_alignment_5m") != "price_volume_aligned":
        diagnostics.append("price_volume_not_directionally_aligned")
    cross = str(market.cross_asset.get("es_spy_direction_confirmation_15m") or "unavailable")
    if cross != "confirmed":
        diagnostics.append(
            "es_spy_direction_divergent"
            if cross == "divergent"
            else "rth_spy_confirmation_unavailable"
        )
    if options.l1.quality is not FrameQuality.READY:
        diagnostics.append("option_l1_not_ready")
    if _number(options.volatility.get("expected_move_points_0dte")) is None:
        diagnostics.append("expected_move_unavailable")
    if "hot_option_liquidity_low" in context.invalidations:
        diagnostics.append("hot_option_liquidity_low")
    return list(dict.fromkeys(diagnostics))


def _pilot_scope(
    *,
    pilot_enabled: bool,
    thesis: str,
    direction: str,
    level_kind: str,
) -> tuple[str, bool, str, str | None]:
    if thesis == "breakout" and direction == "down" and level_kind == "flip_low":
        return FLIP_LOW_BREAKDOWN_PUT_SHADOW_LANE, True, "normal", None
    if thesis == "fade" and direction == "down" and level_kind in {"call_wall", "flip_high"}:
        return UPPER_REJECTION_PUT_SHADOW_LANE, True, "normal", None
    if thesis == "breakout" and direction == "down" and level_kind == "put_wall":
        return (
            PUT_WALL_BREAKDOWN_DISABLED_LANE,
            False,
            "disabled",
            "put_wall_breakdown_disabled",
        )
    if not pilot_enabled:
        return "rth_confirmed_level", False, "normal", None
    if thesis == "breakout" and direction == "up":
        return CALL_BREAKOUT_PILOT_LANE, False, "high", None
    return CALL_BREAKOUT_PILOT_LANE, False, "disabled", "pilot_scope_upside_breakout_only"


def _strategy_window(now: datetime) -> tuple[datetime, datetime]:
    local = _utc(now).astimezone(ET)
    start = datetime.combine(local.date(), ENTRY_WINDOW_START_ET, tzinfo=ET)
    hard_exit = datetime.combine(local.date(), HARD_EXIT_ET, tzinfo=ET)
    return start.astimezone(timezone.utc), hard_exit.astimezone(timezone.utc)


def _moving_average_context(market: MinuteMarketFrame) -> dict[str, object]:
    state = market.diagnostics.get("rth_market_state")
    if not isinstance(state, Mapping):
        return {}
    lineage = state.get("input_lineage")
    if not isinstance(lineage, Mapping):
        return {}
    diagnostics = lineage.get("diagnostics")
    if not isinstance(diagnostics, Mapping):
        return {}
    moving = diagnostics.get("moving_averages")
    if not isinstance(moving, Mapping):
        return {}
    allowed = (
        "status",
        "timeframe",
        "session",
        "price",
        "sma20",
        "sma50",
        "sma200",
        "atr_5m",
        "distance_to_sma20_points",
        "distance_to_sma50_points",
        "distance_to_sma200_points",
        "distance_to_sma50_atr",
        "distance_to_sma200_atr",
        "ma50_slope_3_atr",
        "ma50_slope_6_atr",
        "ma200_slope_3_atr",
        "ma200_slope_6_atr",
        "ma50_ma200_spread_points",
        "ma50_ma200_spread_atr",
        "spread_change_3_atr",
        "cross_direction",
        "bars_since_cross",
        "cross_persistent_2_bars",
        "cross_fresh",
        "regime_state",
        "regime_direction",
        "same_direction_convexity",
        "thresholds",
        "relation",
        "latest_bar_end",
        "contract_identity",
        "es_spx_basis_points",
        "basis_contract_identity",
        "basis_contract_identity_matches_sma",
        "spx_equivalent_sma20",
        "spx_equivalent_sma50",
        "spx_equivalent_sma200",
        "projection_method",
        "spx_projection_near_line",
        "spx_projection_near_line_tolerance_points",
        "action_authority",
    )
    return {key: moving.get(key) for key in allowed if key in moving}


def _current_structure_level(
    options: OptionStructureFrame,
    level_kind: str,
) -> float | None:
    if level_kind in {"put_wall", "call_wall"}:
        return _number(options.structure.get(level_kind))
    flip_zone = options.structure.get("flip_zone")
    if not isinstance(flip_zone, list | tuple) or len(flip_zone) < 2:
        return None
    if level_kind == "flip_low":
        return _number(flip_zone[0])
    if level_kind == "flip_high":
        return _number(flip_zone[1])
    return None


def _timestamp_blockers(
    *,
    source_at: datetime | None,
    transport_at: datetime | None,
    now: datetime,
    max_age_seconds: float,
    future_tolerance_seconds: float,
    prefix: str,
) -> list[str]:
    reasons: list[str] = []
    for label, timestamp in (("source", source_at), ("transport", transport_at)):
        reasons.extend(
            _single_timestamp_blockers(
                timestamp,
                now=now,
                max_age_seconds=max_age_seconds,
                future_tolerance_seconds=future_tolerance_seconds,
                prefix=f"{prefix}_{label}",
            )
        )
    return reasons


def _single_timestamp_blockers(
    timestamp: datetime | None,
    *,
    now: datetime,
    max_age_seconds: float,
    future_tolerance_seconds: float,
    prefix: str,
) -> list[str]:
    if timestamp is None:
        return [f"{prefix}_timestamp_unavailable"]
    age = (now - timestamp).total_seconds()
    if age > max_age_seconds:
        return [f"{prefix}_stale"]
    if age < -future_tolerance_seconds:
        return [f"{prefix}_timestamp_in_future"]
    return []


def _target_spx(
    options: OptionStructureFrame,
    *,
    spot: float,
    trigger_level: float,
    direction: int,
    expected_move: float | None,
    policy: MarketFeatureSettings,
) -> float:
    key = "call_walls" if direction > 0 else "put_walls"
    outward: list[float] = []
    for row in options.structure.get(key) or []:
        if not isinstance(row, Mapping):
            continue
        strike = _number(row.get("strike"))
        if strike is not None and direction * (strike - trigger_level) > 0:
            outward.append(strike)
    if outward:
        return min(outward, key=lambda value: direction * (value - trigger_level))
    distance = max(5.0, (expected_move or 0.0) * policy.trade_target_em_fraction)
    return spot + direction * distance


def _contract_label(candidate: Mapping[str, object]) -> str:
    strike = _number(candidate.get("strike"))
    right = str(candidate.get("right") or "")
    return (
        f"SPXW {strike:g}{right}" if strike is not None and right else str(candidate["contract_id"])
    )


def _play_stats_payload(stats: PlayOutcomeStats) -> dict[str, object]:
    try:
        horizon_seconds: object = int(stats.horizon)
    except (TypeError, ValueError):
        horizon_seconds = stats.horizon
    return {
        "play": stats.play,
        "level_kind": stats.level_kind,
        "window_days": stats.window_days,
        "horizon_seconds": horizon_seconds,
        "sample_count": stats.sample_count,
        "winrate": round(stats.winrate, 4),
        "avg_return_fraction": round(stats.avg_return, 6),
        "median_return_fraction": round(stats.median_return, 6),
    }


def _evidence(context: DecisionContext) -> list[str]:
    breakout = context.breakout_filter
    regime = context.regime_decision
    return list(
        dict.fromkeys(
            [
                *(str(item) for item in breakout.get("evidence") or []),
                *(str(item) for item in regime.get("evidence") or []),
            ]
        )
    )[:12]


def _number(value: object) -> float | None:
    if not isinstance(value, int | float):
        return None
    parsed = float(value)
    return parsed if math.isfinite(parsed) else None


def _datetime(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return _utc(datetime.fromisoformat(value))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
