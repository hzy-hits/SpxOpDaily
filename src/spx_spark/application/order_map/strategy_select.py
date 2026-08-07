"""Single strategy-decision authority for the Order Map payload."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Any

from spx_spark.analytics.options.strategy_payoff import (
    butterfly_economics,
    conservative_butterfly_bbo,
    conservative_vertical_bbo,
    vertical_economics,
    vertical_entry_quality,
)
from spx_spark.application.order_map.strategy_facts import build_market_fact_pack
from spx_spark.application.order_map.strategy_regime import (
    DEFAULT_STRATEGY_POLICY,
    StrategyPolicy,
    assess_regime,
)
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import InstrumentId
from spx_spark.application.market_features.market import quote_source_at
from spx_spark.storage import LatestState


def build_strategy_decision(
    payload: Mapping[str, Any], latest: LatestState, now: datetime
) -> dict[str, Any]:
    facts = build_market_fact_pack(payload, latest, now)
    regime = assess_regime(facts)
    reasons = _gate_reasons(facts, regime)
    candidate = None
    if not reasons:
        if regime.get("terminal_state") == "PIN_STABLE":
            candidate, reasons = _select_butterfly(payload, facts, regime, latest, _utc(now))
        else:
            candidate, reasons = _select_vertical(
                payload, facts, regime, latest=latest, now=_utc(now),
                policy=DEFAULT_STRATEGY_POLICY
            )
    if candidate:
        candidate, utility_reasons = _utility_gate(candidate, facts, regime)
        if candidate:
            return _candidate_decision(facts, {**regime, "entry_state": "GOOD_LOCATION"}, candidate)
        reasons.extend(utility_reasons)
    if "direction_valid_but_entry_too_late" in reasons:
        regime = {**regime, "entry_state": "LATE_CHASE"}
    return _no_trade_decision(facts, regime, reasons)


def _select_butterfly(
    payload: Mapping[str, Any], facts: Mapping[str, Any], regime: Mapping[str, Any],
    latest: LatestState, now: datetime,
) -> tuple[dict[str, Any] | None, list[str]]:
    frame, pin = _map(payload.get("option_structure_frame")), _map(regime.get("pin"))
    expiry, rows = str(frame.get("front_expiry") or ""), []
    if not expiry:
        return None, ["butterfly_expiry_unavailable"]
    for ranked in pin.get("top_centers") or ():
        center = _number(_map(ranked).get("center"))
        if center is None:
            continue
        for width in (5.0, 10.0, 15.0, 20.0):
            for right in ("C", "P"):
                legs = [_option_leg(latest, expiry, strike, right) for strike in (center - width, center, center + width)]
                if any(not leg for leg in legs):
                    continue
                quote = conservative_butterfly_bbo(*legs, now=now)
                if quote.get("status") != "ready":
                    continue
                try:
                    economics = butterfly_economics(center=center, width=width, net_debit=float(quote["ask"]))
                except ValueError:
                    continue
                score = float(_map(ranked).get("score") or 0) + 0.05 * min(
                    float(economics["max_gain_points"]) / float(economics["max_loss_points"]), 3
                ) - 0.01 * width / 5
                rows.append((score, center, width, right, legs, quote, economics))
    if not rows:
        return None, ["butterfly_three_leg_bbo_unavailable"]
    score, center, width, right, legs, quote, economics = max(rows, key=lambda row: row[0])
    source_times = [_time(value) for value in quote.get("source_times") or ()]
    quote_valid = min(value for value in source_times if value) + timedelta(seconds=DEFAULT_STRATEGY_POLICY.quote_max_age_seconds)
    identity = (facts.get("session_date"), center, width, right, *(leg["contract_id"] for leg in legs))
    return {
        "strategy_type": f"{right == 'C' and 'CALL' or 'PUT'}_BUTTERFLY",
        "setup_kind": "STABLE_PIN", "direction": "NEUTRAL",
        "opportunity_id": f"strategy-opportunity:{_hash(identity)[:24]}",
        "target_spx": center, "invalidation_spx": [center - width, center + width],
        "center": center, "width": width, "right": right, "legs": legs,
        "quote": quote, "economics": economics, "selection_score": round(score, 4),
        "pin": pin, "quote_valid_until": quote_valid.isoformat(),
        "opportunity_valid_until": (now + timedelta(seconds=DEFAULT_STRATEGY_POLICY.opportunity_ttl_seconds)).isoformat(),
        "source": "stable_pin_butterfly", "automatic_ordering": False, "manual_action_only": True,
    }, []


def _gate_reasons(facts: Mapping[str, Any], regime: Mapping[str, Any]) -> list[str]:
    quality, event = _map(facts.get("quality")), _map(facts.get("event"))
    reasons = list(quality.get("reasons") or ()) if quality.get("status") != "ready" else []
    if regime.get("event_state") in {"SCHEDULED_EVENT_RISK", "POST_EVENT_DISCOVERY"}:
        reasons.append(f"event_gate:{str(regime['event_state']).lower()}")
    if event.get("entry_allowed") is not True:
        reasons.append("macro_entry_not_authorized")
    return list(dict.fromkeys(map(str, reasons)))


def _select_vertical(
    payload: Mapping[str, Any], facts: Mapping[str, Any], regime: Mapping[str, Any], *,
    latest: LatestState, now: datetime, policy: StrategyPolicy,
) -> tuple[dict[str, Any] | None, list[str]]:
    if DEFAULT_MARKET_CALENDAR.is_rth_open(now):
        evidence, reasons = _rth_evidence(payload, facts, regime, latest)
    elif DEFAULT_MARKET_CALENDAR.is_spx_gth_open(now):
        evidence, reasons = _gth_evidence(facts)
    else:
        return None, ["session_not_open_for_spxw_strategy"]
    if not evidence:
        return None, reasons

    long, short = _map(evidence.get("long")), _map(evidence.get("short"))
    bbo = conservative_vertical_bbo(long, short, now=now,
        max_quote_age_seconds=policy.quote_max_age_seconds,
        max_source_skew_seconds=policy.quote_max_skew_seconds)
    if bbo.get("status") != "ready":
        reasons = list(map(str, bbo.get("reasons") or ()))
        if "spread_leg_quote_stale" in reasons:
            reasons.insert(0, "quote_refresh_required")
        return None, reasons

    strikes = (_number(long.get("strike")), _number(short.get("strike")))
    right = str(long.get("right") or "").upper()
    if None in strikes or right not in {"C", "P"}:
        return None, ["vertical_contract_geometry_unavailable"]
    try:
        economics = vertical_economics(long_strike=float(strikes[0]),
            short_strike=float(strikes[1]), net_debit=float(bbo["ask"]), right=right)
    except ValueError:
        return None, ["vertical_contract_geometry_invalid"]

    path, spot = _map(facts.get("path")), _number(_map(facts.get("spot")).get("spx"))
    geometry = (spot, _number(path.get("atr_5m")), _number(evidence.get("target_spx")),
                _number(evidence.get("invalidation_spx")))
    if None in geometry:
        return None, ["entry_quality_atr_or_geometry_unavailable"]
    entry_quality, reasons = vertical_entry_quality(
        spot=float(geometry[0]), atr=float(geometry[1]), target=float(geometry[2]),
        stop=float(geometry[3]), trigger=_number(evidence.get("trigger_level")),
        direction=str(evidence["direction"]), setup_kind=str(evidence["setup_kind"]),
        distance_to_vwap_points=_number(path.get("distance_to_vwap_points")),
        impulse_15m_points=_number(path.get("impulse_15m_points")),
        debit_fraction=float(economics["debit_fraction_of_width"]),
        thresholds=policy.entry_quality_kwargs(),
    )
    if reasons:
        return None, reasons

    quote_times = [_time(leg.get("source_at")) for leg in (long, short)]
    if any(item is None for item in quote_times):
        return None, ["spread_leg_source_time_missing"]
    quote_valid = min(item for item in quote_times if item) + timedelta(seconds=policy.quote_max_age_seconds)
    opportunity_valid = now + timedelta(seconds=policy.opportunity_ttl_seconds)
    if source_valid := _time(evidence.get("valid_until")):
        opportunity_valid = min(opportunity_valid, source_valid)
    if opportunity_valid <= now:
        return None, ["source_opportunity_expired"]

    identity = {"session_date": facts.get("session_date"), "setup_kind": evidence["setup_kind"],
                "direction": evidence["direction"], "trigger_level": evidence.get("trigger_level"),
                "long_contract_id": long.get("contract_id"), "short_contract_id": short.get("contract_id")}
    return {
        "strategy_type": f"{'CALL' if right == 'C' else 'PUT'}_DEBIT_VERTICAL",
        **{key: evidence.get(key) for key in (
            "setup_kind", "direction", "trigger_level", "target_spx",
            "invalidation_spx", "source",
        )},
        "right": right, "opportunity_id": f"strategy-opportunity:{_hash(identity)[:24]}",
        "long": dict(long), "short": dict(short), "quote": bbo, "economics": economics,
        "entry_quality": entry_quality, "quote_valid_until": quote_valid.isoformat(),
        "opportunity_valid_until": opportunity_valid.isoformat(), "automatic_ordering": False,
        "manual_action_only": True,
    }, []


def _rth_evidence(
    payload: Mapping[str, Any], facts: Mapping[str, Any], regime: Mapping[str, Any],
    latest: LatestState,
) -> tuple[dict[str, Any] | None, list[str]]:
    trigger = _map(facts.get("trigger"))
    direction, thesis = _direction(trigger.get("direction")), str(trigger.get("thesis") or "").lower()
    if trigger.get("phase") != "confirmed" or not direction:
        return None, ["confirmed_price_trigger_unavailable"]
    if thesis == "fade":
        setup = "FAILED_BREAK_RECLAIM"
    elif thesis == "breakout" and (regime.get("path_state"), regime.get("path_direction")) == ("TREND", direction):
        setup = "TREND_PULLBACK"
    else:
        return None, ["price_trigger_not_aligned_with_supported_setup"]
    source = "call_skew_spread_shadow" if direction == "UP" else "put_skew_spread_shadow"
    shadow, spread = _map(payload.get(source)), _map(_map(payload.get(source)).get("candidate"))
    if shadow.get("status") != "candidate" or not spread:
        spread = _intent_spread(payload.get("trade_intent"), latest)
        source = "legacy_trade_intent_trigger_only"
    if not spread:
        return None, ["vertical_exact_spread_unavailable"]
    target, stop = _structural_geometry(facts, direction, _number(trigger.get("level")))
    if target is None or stop is None:
        return None, ["vertical_target_or_invalidation_unavailable"]
    return {
        "setup_kind": setup, "direction": direction,
        "trigger_level": _number(trigger.get("level")), "target_spx": target,
        "invalidation_spx": stop, "long": _map(spread.get("long")),
        "short": _map(spread.get("short")), "source": source,
    }, []


def _intent_spread(value: object, latest: LatestState) -> Mapping[str, Any]:
    intent = _map(value)
    if intent.get("status") != "trade_ready":
        return {}
    parts = str(intent.get("contract_id") or "").split(":")
    if len(parts) < 6 or parts[-1] not in {"C", "P"}:
        return {}
    try:
        strike = float(parts[-2])
    except ValueError:
        return {}
    expiry, right = parts[-3], parts[-1]
    short_strike = strike + 10.0 if right == "C" else strike - 10.0
    long, short = _option_leg(latest, expiry, strike, right), _option_leg(latest, expiry, short_strike, right)
    return {"long": long, "short": short} if long and short else {}


def _gth_evidence(facts: Mapping[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
    evidence = _map(facts.get("gth_evidence"))
    eligible = evidence.get("manual_action_eligible") is True or evidence.get("selector_evidence_eligible") is True
    if evidence.get("status") not in {"manual_ready", "selector_candidate"} or not eligible:
        return None, list(map(str, evidence.get("block_reasons") or ())) or ["gth_confirmed_level_candidate_unavailable"]
    path_kind = str(evidence.get("path_kind") or "")
    if path_kind.startswith("trend_transition_"):
        return None, ["trend_background_cannot_authorize_entry"]
    direction = _direction(evidence.get("direction"))
    if not direction:
        return None, ["gth_candidate_direction_unavailable"]
    snapshot = _map(evidence.get("exact_spread_snapshot"))
    target, stop = _number(evidence.get("target_spx")), _number(evidence.get("invalidation_spx"))
    if target is None or stop is None:
        return None, ["gth_spx_target_or_invalidation_unavailable"]
    setup = "FAILED_BREAK_RECLAIM" if any(
        token in path_kind for token in ("rejection", "reclaim", "dip")
    ) else "TREND_PULLBACK"
    return {
        "setup_kind": setup, "direction": direction,
        "trigger_level": _number(evidence.get("trigger_level")), "target_spx": target,
        "invalidation_spx": stop,
        "long": _gth_leg(snapshot.get("long"), evidence.get("long_contract_id")),
        "short": _gth_leg(snapshot.get("short"), evidence.get("short_contract_id")),
        "valid_until": evidence.get("valid_until"),
        "source": "gth_level_manual_candidate",
    }, []


def _structural_geometry(
    facts: Mapping[str, Any], direction: str, trigger: float | None
) -> tuple[float | None, float | None]:
    spot, structure = _number(_map(facts.get("spot")).get("spx")), _map(facts.get("structure"))
    if spot is None:
        return None, None
    target = _number(structure.get("call_wall" if direction == "UP" else "put_wall"))
    stop = trigger
    if stop is None or (direction == "UP" and stop >= spot) or (direction == "DOWN" and stop <= spot):
        levels = [_number(structure.get("put_wall")), *_flip_values(structure.get("flip_zone")),
                  _number(structure.get("zero_gamma")), _number(structure.get("call_wall"))]
        if direction == "UP":
            stop = max((value for value in levels if value is not None and value < spot), default=None)
        else:
            stop = min((value for value in levels if value is not None and value > spot), default=None)
    return target, stop


def _base_decision(facts: Mapping[str, Any], regime: Mapping[str, Any], identity: object) -> dict[str, Any]:
    return {
        "schema_version": "strategy_decision.v1",
        "decision_id": f"strategy:{_hash(identity)[:24]}",
        "policy_version": DEFAULT_STRATEGY_POLICY.policy_version,
        "decision_at": facts["decision_at"],
        "available_at": facts["available_at"],
        "session_date": facts.get("session_date"),
        "market_facts": facts,
        "regime": regime,
        "probability_evidence": _probability_evidence(facts),
        "automatic_ordering": False,
    }


def _utility_gate(
    candidate: Mapping[str, Any], facts: Mapping[str, Any], regime: Mapping[str, Any]
) -> tuple[dict[str, Any] | None, list[str]]:
    evidence = _probability_evidence(facts)
    event = _map(_map(facts.get("probability")).get("event"))
    expected_kind = {
        "UP": "terminal_above", "DOWN": "terminal_below", "NEUTRAL": "terminal_between"
    }.get(str(candidate.get("direction")))
    if event.get("kind") != expected_kind:
        return None, ["candidate_probability_event_mismatch"]
    q, p, low = (_number(evidence.get(key)) for key in ("q", "p_empirical", "p_interval_low"))
    if q is None or p is None or low is None:
        return None, ["candidate_probability_unavailable"]
    weight = float(evidence["shrinkage_weight"])
    probability, conservative = (weight * value + (1.0 - weight) * q for value in (p, low))
    economics, quote = _map(candidate.get("economics")), _map(candidate.get("quote"))
    gain, loss = (_number(economics.get(key)) for key in ("max_gain_points", "max_loss_points"))
    if gain is None or loss is None or gain <= 0.0 or loss <= 0.0:
        return None, ["candidate_payoff_unavailable"]
    expected = probability * gain - (1.0 - probability) * loss
    lower_bound = conservative * gain - (1.0 - conservative) * loss
    friction = min(abs(float(quote.get("ask", 0.0)) - float(quote.get("bid", 0.0))) / loss, 1.0)
    uncertainty, migration = 1.0 - weight, float(_map(regime.get("pin")).get("depin_risk") or 0.0)
    utility = expected / loss - 0.75 - 0.25 * friction - 0.25 * uncertainty - 0.5 * migration
    scoring = {
        "event_probability": round(probability, 6), "conservative_probability": round(conservative, 6),
        "expected_net_pnl": round(expected * 100.0, 2), "conservative_lower_bound": round(lower_bound * 100.0, 2),
        "p10_net_pnl": round(-loss * 100.0, 2),
        "p50_net_pnl": round((gain if probability >= 0.5 else -loss) * 100.0, 2),
        "p90_net_pnl": round((gain if probability >= 0.1 else -loss) * 100.0, 2),
        "expected_shortfall_10": round(loss * 100.0, 2), "utility": round(utility, 6),
        "liquidity_penalty": round(friction, 6), "model_uncertainty": round(uncertainty, 6),
        "method": "binary_payoff_bootstrap_bound.v1",
    }
    if utility <= 0.0 or lower_bound <= 0.0:
        return None, ["candidate_utility_not_positive"]
    return {**candidate, "probability_evidence": evidence, "utility": scoring}, []


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
    facts: Mapping[str, Any], regime: Mapping[str, Any], candidate: Mapping[str, Any]
) -> dict[str, Any]:
    legs = candidate.get("legs") or (candidate.get("long"), candidate.get("short"))
    available = max(str(facts["available_at"]), *(str(_map(leg).get("source_at") or "") for leg in legs))
    economics = _map(candidate.get("economics"))
    result = _base_decision(facts, regime, (facts["decision_at"], available, candidate["opportunity_id"], candidate["quote"]))
    result.update({
        "available_at": available,
        "decision_type": candidate["strategy_type"],
        "candidate": dict(candidate),
        "desk_view": {"state": regime["path_state"], "direction": candidate["direction"],
                      "conclusion": "MANUAL CANDIDATE", "reason": candidate["setup_kind"]},
        "why_not": {"nearest_candidate": None, "reasons": [], "reauthorize_on": None},
        "execution": {"action": "MANUAL_LIMIT", "order_type": "NET_DEBIT_LIMIT",
                      "limit": _map(candidate.get("quote")).get("ask"),
                      "quote_valid_until": candidate["quote_valid_until"],
                      "opportunity_valid_until": candidate["opportunity_valid_until"],
                      "automatic_ordering": False, "manual_action_only": True},
        "risk": {"max_loss": round(float(economics["max_loss_points"]) * 100, 2),
                 "invalidation": {"instrument": "SPX", "price": candidate["invalidation_spx"]}},
        "targets": [{"instrument": "SPX", "price": candidate["target_spx"]}],
        "data_quality": {**dict(_map(facts.get("quality"))), "quote": "ready"},
        "action_authority": "manual",
    })
    return result


def _no_trade_decision(
    facts: Mapping[str, Any], regime: Mapping[str, Any], reasons: list[str]
) -> dict[str, Any]:
    reasons = list(dict.fromkeys(reasons or ["no_supported_strategy_candidate"]))
    result = _base_decision(facts, regime, (facts["decision_at"], facts["available_at"], regime, reasons))
    refresh = "刷新 SPXW 两腿双边报价后重新计算" if "quote_refresh_required" in reasons else "等待价格触发、结构赔率和执行价格同时通过"
    result.update({
        "decision_type": "NO_TRADE",
        "candidate": None,
        "desk_view": {"state": regime["path_state"], "direction": regime.get("path_direction"),
                      "conclusion": "NO TRADE", "reason": reasons[0]},
        "why_not": {"nearest_candidate": None, "reasons": reasons,
                    "reauthorize_on": refresh},
        "execution": {"action": "WAIT", "order_type": None, "limit": None,
                      "automatic_ordering": False, "manual_action_only": True},
        "risk": {"max_loss": None, "invalidation": "没有候选被授权，不建立风险敞口"},
        "targets": [],
        "data_quality": _map(facts.get("quality")),
        "action_authority": "none",
    })
    return result


def _gth_leg(value: object, contract_id: object) -> dict[str, Any]:
    leg, parts = dict(_map(value)), str(contract_id or "").split(":")
    if len(parts) >= 2:
        try:
            leg["strike"] = float(parts[-2])
        except ValueError:
            pass
        leg["right"] = parts[-1].upper()
    leg["contract_id"] = contract_id
    return leg


def _option_leg(latest: LatestState, expiry: str, strike: float, right: str) -> dict[str, Any]:
    contract_id = InstrumentId.option("SPX", expiry=expiry, strike=strike, right=right, trading_class="SPXW").canonical_id
    quote = latest.best_quote(contract_id)
    if quote is None:
        return {}
    return {"contract_id": contract_id, "strike": strike, "right": right,
            "provider": quote.provider.value, "bid": quote.bid, "ask": quote.ask,
            "source_at": quote_source_at(quote).isoformat()}


def _flip_values(value: object) -> list[float | None]:
    if isinstance(value, (list, tuple)):
        return [_number(item) for item in value[:2]]
    mapped = _map(value)
    return [_number(mapped.get("low")), _number(mapped.get("high"))] if mapped else []


def _direction(value: object) -> str | None:
    normalized = str(value or "").upper()
    return normalized if normalized in {"UP", "DOWN"} else None


def _hash(value: object) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(encoded.encode()).hexdigest()


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
