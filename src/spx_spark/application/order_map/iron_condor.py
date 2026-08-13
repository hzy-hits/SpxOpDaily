"""Always-on 25Δ/5Δ iron condor map, recomputed from live SPXW quotes."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta, timezone
from typing import Any

from spx_spark.analytics.options.strategy_payoff import (
    conservative_iron_condor_bbo,
    iron_condor_economics,
)
from spx_spark.application.order_map.candidate_factory import (
    _candidate_id,
    _gth_quote_policy,
    _hash,
    _map,
    _number,
    _quote_valid_until,
    _session_option_legs,
    nearest_abs_delta_strike,
)
from spx_spark.application.order_map.strategy_regime import StrategyPolicy
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import Provider
from spx_spark.storage import LatestState

IRON_CONDOR_DELTA = "IRON_CONDOR_DELTA"
IRON_CONDOR_TYPE = "IRON_CONDOR"
SHORT_ABS_DELTA = 0.25
LONG_ABS_DELTA = 0.05
SHORT_DELTA_TOLERANCE = 0.08
LONG_DELTA_TOLERANCE = 0.04
MIN_CREDIT_FRACTION = 0.15
MAX_CREDIT_FRACTION = 0.55


def build_iron_condor_map(
    payload: Mapping[str, Any],
    facts: Mapping[str, Any],
    latest: LatestState,
    *,
    now: datetime,
    policy: StrategyPolicy,
) -> dict[str, Any]:
    """Return the current 25Δ/5Δ iron condor structure, even when not tradable."""

    now = _utc(now)
    session_policy, providers, session_reason = _session_quote_policy(now, policy)
    if session_policy is None:
        return _unavailable_map(session_reason)
    expiry = str(
        _map(payload.get("option_structure_frame")).get("front_expiry")
        or payload.get("expiry")
        or ""
    )
    spot = _number(_map(facts.get("spot")).get("spx"))
    if not expiry:
        return _unavailable_map("vertical_expiry_unavailable")
    if spot is None:
        return _unavailable_map("spx_price_unavailable")
    strikes = _delta_strikes(
        latest,
        expiry,
        now=now,
        policy=session_policy,
        providers=providers,
    )
    if strikes is None:
        return _unavailable_map("iron_condor_delta_quotes_unavailable", expiry=expiry, spot=spot)
    put_long_k, put_short_k, call_short_k, call_long_k = strikes
    legs = _session_option_legs(
        latest,
        expiry,
        (
            (put_long_k, "P"),
            (put_short_k, "P"),
            (call_short_k, "C"),
            (call_long_k, "C"),
        ),
        now=now,
        policy=session_policy,
        providers=providers,
    )
    if len(legs) != 4:
        return _unavailable_map(
            "iron_condor_four_leg_quote_unavailable",
            expiry=expiry,
            spot=spot,
            strikes=list(strikes),
        )
    put_long, put_short, call_short, call_long = legs
    quote = conservative_iron_condor_bbo(
        put_long,
        put_short,
        call_short,
        call_long,
        now=now,
        max_quote_age_seconds=session_policy.quote_max_age_seconds,
        max_source_skew_seconds=session_policy.quote_max_skew_seconds,
    )
    economics: dict[str, Any] = {}
    if quote.get("status") == "ready":
        try:
            economics = iron_condor_economics(
                put_long=put_long_k,
                put_short=put_short_k,
                call_short=call_short_k,
                call_long=call_long_k,
                net_credit=float(quote["credit"]),
            )
        except ValueError:
            economics = {}
    inside = put_short_k < spot < call_short_k
    return {
        "status": "ready" if quote.get("status") == "ready" and economics else "unavailable",
        "reason": None if quote.get("status") == "ready" and economics else "iron_condor_credit_unavailable",
        "setup_kind": IRON_CONDOR_DELTA,
        "strategy_type": IRON_CONDOR_TYPE,
        "short_abs_delta": SHORT_ABS_DELTA,
        "long_abs_delta": LONG_ABS_DELTA,
        "expiry": expiry,
        "spot": spot,
        "strikes": [put_long_k, put_short_k, call_short_k, call_long_k],
        "put_long": put_long,
        "put_short": put_short,
        "call_short": call_short,
        "call_long": call_long,
        "legs": legs,
        "quote": quote,
        "economics": economics,
        "spot_inside_shorts": inside,
        "provider": quote.get("provider"),
    }


def enumerate_iron_condor_candidates(
    payload: Mapping[str, Any],
    facts: Mapping[str, Any],
    latest: LatestState,
    *,
    now: datetime,
    policy: StrategyPolicy,
) -> list[dict[str, Any]]:
    structure = build_iron_condor_map(
        payload, facts, latest, now=now, policy=policy
    )
    if structure.get("status") != "ready":
        return []
    now = _utc(now)
    session_policy, _, _ = _session_quote_policy(now, policy)
    if session_policy is None:
        return []
    legs = list(structure.get("legs") or ())
    quote = _map(structure.get("quote"))
    economics = _map(structure.get("economics"))
    put_long, put_short, call_short, call_long = (
        _map(structure.get("put_long")),
        _map(structure.get("put_short")),
        _map(structure.get("call_short")),
        _map(structure.get("call_long")),
    )
    strikes = [float(value) for value in structure.get("strikes") or ()]
    expiry = str(structure.get("expiry") or "")
    candidate_id = _candidate_id(
        facts.get("session_date"),
        IRON_CONDOR_TYPE,
        expiry,
        strikes,
        "IC",
    )
    identity = (
        facts.get("session_date"),
        candidate_id,
        *(leg.get("contract_id") for leg in legs),
    )
    quote_valid = _quote_valid_until(legs, now=now, policy=session_policy)
    credit = _number(quote.get("credit"))
    loss = _number(economics.get("max_loss_points"))
    gain = _number(economics.get("max_gain_points"))
    score = 0.0
    if credit is not None and loss is not None and loss > 0 and gain is not None:
        spread = abs(float(quote.get("ask") or 0.0) - float(quote.get("bid") or 0.0))
        score = round(gain / loss - 0.05 * spread / loss, 4)
    return [
        {
            "candidate_id": candidate_id,
            "strategy_type": IRON_CONDOR_TYPE,
            "setup_kind": IRON_CONDOR_DELTA,
            "setup_state": "ENTRY_WINDOW_OPEN",
            "direction": "NEUTRAL",
            "thesis_direction": "NEUTRAL",
            "payoff_shape": "RANGE",
            "manual_authority_eligible": True,
            "opportunity_id": f"strategy-opportunity:{_hash(identity)[:24]}",
            "target_spx": (strikes[1] + strikes[2]) / 2.0 if len(strikes) == 4 else None,
            "invalidation_spx": strikes[1:3] if len(strikes) == 4 else None,
            "right": "IC",
            "put_long": dict(put_long),
            "put_short": dict(put_short),
            "call_short": dict(call_short),
            "call_long": dict(call_long),
            "legs": [dict(leg) for leg in legs],
            "quote": dict(quote),
            "economics": dict(economics),
            "selection_score": score,
            "spot_inside_shorts": structure.get("spot_inside_shorts"),
            "short_abs_delta": SHORT_ABS_DELTA,
            "long_abs_delta": LONG_ABS_DELTA,
            "quote_valid_until": quote_valid.isoformat() if quote_valid else now.isoformat(),
            "opportunity_valid_until": (
                now + timedelta(seconds=session_policy.opportunity_ttl_seconds)
            ).isoformat(),
            "source": f"gth_{quote.get('provider')}_iron_condor"
            if DEFAULT_MARKET_CALENDAR.is_spx_gth_open(now)
            else f"rth_{quote.get('provider')}_iron_condor",
            "geometry_source": "delta_25_5_iron_condor",
            "automatic_ordering": False,
            "manual_action_only": True,
        }
    ]


def _delta_strikes(
    latest: LatestState,
    expiry: str,
    *,
    now: datetime,
    policy: StrategyPolicy,
    providers: Sequence[Provider],
) -> tuple[float, float, float, float] | None:
    put_long = nearest_abs_delta_strike(
        latest,
        expiry,
        "P",
        target_abs_delta=LONG_ABS_DELTA,
        now=now,
        policy=policy,
        providers=providers,
        max_distance=LONG_DELTA_TOLERANCE,
    )
    put_short = nearest_abs_delta_strike(
        latest,
        expiry,
        "P",
        target_abs_delta=SHORT_ABS_DELTA,
        now=now,
        policy=policy,
        providers=providers,
        max_distance=SHORT_DELTA_TOLERANCE,
    )
    call_short = nearest_abs_delta_strike(
        latest,
        expiry,
        "C",
        target_abs_delta=SHORT_ABS_DELTA,
        now=now,
        policy=policy,
        providers=providers,
        max_distance=SHORT_DELTA_TOLERANCE,
    )
    call_long = nearest_abs_delta_strike(
        latest,
        expiry,
        "C",
        target_abs_delta=LONG_ABS_DELTA,
        now=now,
        policy=policy,
        providers=providers,
        max_distance=LONG_DELTA_TOLERANCE,
    )
    if None in (put_long, put_short, call_short, call_long):
        return None
    if not put_long < put_short < call_short < call_long:
        return None
    return put_long, put_short, call_short, call_long


def _session_quote_policy(
    now: datetime, policy: StrategyPolicy
) -> tuple[StrategyPolicy | None, tuple[Provider, ...], str]:
    if DEFAULT_MARKET_CALENDAR.is_spx_gth_open(now):
        return _gth_quote_policy(policy), (Provider.IBKR, Provider.SCHWAB), ""
    if DEFAULT_MARKET_CALENDAR.is_rth_open(now):
        return policy, (Provider.SCHWAB, Provider.IBKR), ""
    return None, (), "session_not_open_for_spxw_strategy"


def _unavailable_map(
    reason: str,
    *,
    expiry: str | None = None,
    spot: float | None = None,
    strikes: list[float] | None = None,
) -> dict[str, Any]:
    return {
        "status": "unavailable",
        "reason": reason,
        "setup_kind": IRON_CONDOR_DELTA,
        "strategy_type": IRON_CONDOR_TYPE,
        "short_abs_delta": SHORT_ABS_DELTA,
        "long_abs_delta": LONG_ABS_DELTA,
        "expiry": expiry,
        "spot": spot,
        "strikes": list(strikes or ()),
        "quote": {"status": "unavailable", "reasons": [reason]},
        "economics": {},
        "spot_inside_shorts": None,
    }


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("strategy decision time must be timezone-aware")
    return value.astimezone(timezone.utc)
