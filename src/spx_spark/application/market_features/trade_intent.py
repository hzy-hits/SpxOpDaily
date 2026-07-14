"""Deterministic conversion from a confirmed level path to one executable intent."""

from __future__ import annotations

import hashlib
import math
from datetime import datetime, timedelta, timezone
from typing import Mapping

from spx_spark.application.market_features.models import (
    DecisionContext,
    FrameQuality,
    MinuteMarketFrame,
    OptionStructureFrame,
)
from spx_spark.application.order_map.execution_quote import evaluate_execution_quote
from spx_spark.application.order_map.models import level_decision_play
from spx_spark.application.order_map.pricing import round_to_tick
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.settings.market_features import MarketFeatureSettings
from spx_spark.settings.order_map import OrderMapPolicy
from spx_spark.storage import LatestState


HARD_CONTEXT_INVALIDATIONS = frozenset(
    {
        "es_path_unavailable",
        "option_structure_unavailable",
        "es_spy_direction_divergent",
        "hot_option_liquidity_low",
    }
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
) -> dict[str, object]:
    """Fail closed unless the signal, direction and live option quote all agree."""

    now = _utc(now)
    level = context.level_decision
    event_id = str(level.get("event_id") or "")
    phase = str(level.get("phase") or "far")
    thesis = str(level.get("thesis") or "none")
    direction = str(level.get("direction") or "")
    trigger_level = _number(level.get("level"))
    play = level_decision_play(thesis, direction)
    semantic_scope = (
        "|".join((context.session_id, play, f"{trigger_level:.4f}"))
        if play is not None and trigger_level is not None
        else None
    )
    base: dict[str, object] = {
        "schema_version": 1,
        "status": "observing",
        "event_id": event_id or None,
        "context_id": context.context_id,
        "session_id": context.session_id,
        "phase": phase,
        "thesis": thesis,
        "direction": direction or None,
        "semantic_scope": semantic_scope,
        "evaluated_at": now.isoformat(),
        "block_reasons": [],
    }
    if not event_id or phase != "confirmed" or thesis not in {"breakout", "fade"}:
        return base

    reasons: list[str] = []
    if level.get("formal_signal_enabled") is not True:
        reasons.append("formal_signal_disabled")
    if level.get("formal_signal") is not True:
        reasons.append("formal_signal_unavailable")
    if level.get("quality_ok") is not True:
        reasons.append("level_observation_quality_failed")
    if direction not in {"up", "down"}:
        reasons.append("direction_unavailable")
    direction_sign = 1 if direction == "up" else -1

    confirmed_at = _datetime(level.get("phase_at") or level.get("confirmed_at"))
    if confirmed_at is None:
        reasons.append("confirmed_at_unavailable")
        confirmation_age = None
    else:
        confirmation_age = max((now - confirmed_at).total_seconds(), 0.0)
        if confirmation_age < feature_policy.trade_follow_through_seconds:
            reasons.append("follow_through_hold_pending")

    event_expires_at = _datetime(level.get("expires_at"))
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
        )
    )

    spot = _number(level.get("spot"))
    expected_move = _number(options.volatility.get("expected_move_points_0dte"))
    if expected_move is None:
        reasons.append("expected_move_unavailable")
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

    reasons.extend(
        item for item in context.invalidations if item in HARD_CONTEXT_INVALIDATIONS
    )
    reasons.extend(_direction_blockers(context, market, thesis=thesis, direction=direction))

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

    unique_reasons = list(dict.fromkeys(reasons))
    if unique_reasons or candidate is None or quote is None or quote_gate is None:
        return {
            **base,
            "status": "blocked",
            "play": play,
            "confirmation_age_seconds": confirmation_age,
            "follow_through_points": follow_move,
            "follow_through_required_points": follow_threshold,
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
    invalidation = trigger_level - direction_sign * feature_policy.trade_invalidation_buffer_points
    target = _target_spx(
        options,
        spot=spot,
        trigger_level=trigger_level,
        direction=direction_sign,
        expected_move=expected_move,
        policy=feature_policy,
    )
    intent_expires_at = now + timedelta(seconds=feature_policy.trade_intent_ttl_seconds)
    if event_expires_at is not None:
        intent_expires_at = min(intent_expires_at, event_expires_at)
    time_stop_at = now + timedelta(minutes=feature_policy.trade_time_stop_minutes)
    contract_id = str(candidate["contract_id"])
    assert semantic_scope is not None
    semantic_key = "|".join((semantic_scope, contract_id))
    token = semantic_key
    intent_id = "intent:" + hashlib.sha256(token.encode()).hexdigest()[:24]
    source_at = quote.quote_time or quote.trade_time or quote.last_update_at or quote.received_at
    return {
        **base,
        "status": "trade_ready",
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
        "confirmation_age_seconds": confirmation_age,
        "follow_through_points": follow_move,
        "follow_through_required_points": follow_threshold,
        "time_stop_at": time_stop_at.isoformat(),
        "expires_at": intent_expires_at.isoformat(),
        "max_loss_per_contract": round(entry_limit * 100.0, 2),
        "quantity": None,
        "quantity_policy": "operator_selected",
        "automatic_ordering": False,
        "evidence": _evidence(context),
        "block_reasons": [],
    }


def _direction_blockers(
    context: DecisionContext,
    market: MinuteMarketFrame,
    *,
    thesis: str,
    direction: str,
) -> list[str]:
    reasons: list[str] = []
    sign = 1 if direction == "up" else -1
    regime = context.regime_decision
    regime_direction = str(regime.get("direction") or "none")
    if (
        regime.get("mode") == "trending"
        and regime_direction in {"up", "down"}
        and regime_direction != direction
    ):
        reasons.append("regime_direction_conflict")
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
    if price_volume != "price_volume_aligned" or price_return_5m is None or price_return_5m * sign <= 0:
        reasons.append("price_volume_not_directionally_aligned")

    cross = str(
        market.cross_asset.get("es_spy_direction_confirmation_15m") or "unavailable"
    )
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
) -> list[str]:
    reasons: list[str] = []
    if market.quality is not FrameQuality.READY:
        reasons.append("market_frame_not_ready")
    if options.quality is not FrameQuality.READY:
        reasons.append("option_structure_not_ready")
    if options.l1.quality is not FrameQuality.READY:
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
    return f"SPXW {strike:g}{right}" if strike is not None and right else str(candidate["contract_id"])


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
