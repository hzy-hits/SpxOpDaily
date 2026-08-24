"""Always-on 5–20Δ short-leg iron condor map with a 10-point defined-risk wing."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from spx_spark.analytics.options.strategy_payoff import (
    conservative_iron_condor_bbo,
    iron_condor_economics,
)
from spx_spark.analytics.options.surface_attribution import attribute_candidate_surface
from spx_spark.application.order_map.candidate_factory import (
    _candidate_id,
    _gth_quote_policy,
    _hash,
    _map,
    _number,
    _quote_valid_until,
    _round_to_strike,
    _session_option_legs,
    nearest_abs_delta_strike,
)
from spx_spark.application.order_map.strategy_regime import StrategyPolicy
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import Provider
from spx_spark.storage import LatestState

IRON_CONDOR_DELTA = "IRON_CONDOR_DELTA"
IRON_CONDOR_TYPE = "IRON_CONDOR"
PREFERRED_SHORT_DELTAS: tuple[float, ...] = (0.20, 0.15, 0.10, 0.05)
SHORT_DELTA_MIN = 0.05
SHORT_DELTA_MAX = 0.20
SHORT_DELTA_TOLERANCE = 0.05
WING_WIDTH = 10.0
MIN_CREDIT_FRACTION = 0.20
MAX_CREDIT_FRACTION = 0.55
HUMAN_SHORT_DELTA = 0.20
HUMAN_ENTRY_START_ET = time(10, 0)
HUMAN_ENTRY_END_ET = time(11, 30)
HUMAN_MAX_RISK_DOLLARS = 1_000.0
HUMAN_TAKE_PROFIT_BUYBACK_FRACTION = 0.50
HUMAN_STOP_BUYBACK_MULTIPLE = 3.0
HUMAN_HARD_EXIT_ET = "15:45"
HUMAN_EVIDENCE_CONTRACT_HASH = (
    "sha256:18fc09f37e797a435c62584e224853378651d58ffbfbc9d595d940e0b95997f3"
)
NEW_YORK = ZoneInfo("America/New_York")


def build_iron_condor_map(
    payload: Mapping[str, Any],
    facts: Mapping[str, Any],
    latest: LatestState,
    *,
    now: datetime,
    policy: StrategyPolicy,
) -> dict[str, Any]:
    """Return the current 5–20Δ short / 10-wide iron condor, even when not tradable."""

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
    variants = [
        row
        for delta in _short_deltas(policy)
        if (
            row := _structure_for_short_delta(
                latest,
                expiry,
                spot=spot,
                short_abs_delta=delta,
                now=now,
                session_policy=session_policy,
                providers=providers,
            )
        )
        is not None
        and row.get("status") == "ready"
    ]
    if not variants:
        return _unavailable_map(
            "iron_condor_delta_quotes_unavailable",
            expiry=expiry,
            spot=spot,
        )
    ranked_variants: list[dict[str, Any]] = []
    for variant in variants:
        row = _with_surface_score(
            variant,
            facts,
            now=now,
            policy=policy,
        )
        ranked_variants.append(row)
    ranked_variants.sort(
        key=lambda row: (
            float(row.get("selection_score") or 0.0),
            float(row.get("short_abs_delta") or 0.0),
        ),
        reverse=True,
    )
    primary = dict(ranked_variants[0])
    primary["variants"] = [
        {
            "short_abs_delta": row.get("short_abs_delta"),
            "strikes": row.get("strikes"),
            "quote": row.get("quote"),
            "economics": row.get("economics"),
            "selection_score_base": row.get("selection_score_base"),
            "surface_decision_modifier": row.get("surface_decision_modifier"),
            "selection_score": row.get("selection_score"),
        }
        for row in ranked_variants
    ]
    return primary


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
    session_policy, providers, _ = _session_quote_policy(now, policy)
    if session_policy is None:
        return []
    session_mode = "gth" if DEFAULT_MARKET_CALENDAR.is_spx_gth_open(now) else "rth"
    human_window_open = session_mode == "rth" and _human_entry_window_open(now)
    if human_window_open:
        expiry = str(structure.get("expiry") or "")
        spot = _number(structure.get("spot"))
        human_structure = (
            _structure_for_short_delta(
                latest,
                expiry,
                spot=spot,
                short_abs_delta=HUMAN_SHORT_DELTA,
                now=now,
                session_policy=session_policy,
                providers=providers,
            )
            if expiry and spot is not None
            else None
        )
        if human_structure is not None and human_structure.get("status") == "ready":
            structure = _with_surface_score(
                human_structure,
                facts,
                now=now,
                policy=policy,
            )
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
    score = float(structure.get("selection_score") or 0.0)
    return [
        {
            "candidate_id": candidate_id,
            "strategy_type": IRON_CONDOR_TYPE,
            "setup_kind": IRON_CONDOR_DELTA,
            "setup_state": "ENTRY_WINDOW_OPEN",
            "direction": "NEUTRAL",
            "thesis_direction": "NEUTRAL",
            "payoff_shape": "RANGE",
            "manual_authority_eligible": human_window_open,
            "opportunity_id": f"strategy-opportunity:{_hash(identity)[:24]}",
            "target_spx": (strikes[1] + strikes[2]) / 2.0 if len(strikes) == 4 else None,
            "invalidation_spx": strikes[1:3] if len(strikes) == 4 else None,
            "right": "IC",
            "strikes": strikes,
            "put_long": dict(put_long),
            "put_short": dict(put_short),
            "call_short": dict(call_short),
            "call_long": dict(call_long),
            "legs": [dict(leg) for leg in legs],
            "quote": dict(quote),
            "economics": dict(economics),
            "selection_score": score,
            "selection_score_base": structure.get("selection_score_base"),
            "surface_decision_modifier": structure.get("surface_decision_modifier"),
            "surface_attribution": dict(_map(structure.get("surface_attribution"))),
            "spot_inside_shorts": structure.get("spot_inside_shorts"),
            "short_abs_delta": structure.get("short_abs_delta"),
            "wing_width": WING_WIDTH,
            "quote_valid_until": quote_valid.isoformat() if quote_valid else now.isoformat(),
            "opportunity_valid_until": (
                now + timedelta(seconds=session_policy.opportunity_ttl_seconds)
            ).isoformat(),
            "source": f"gth_{quote.get('provider')}_iron_condor"
            if session_mode == "gth"
            else f"rth_{quote.get('provider')}_iron_condor",
            "session_mode": session_mode,
            "geometry_source": (
                "rth_20delta_fixed10_iron_condor"
                if human_window_open
                else "delta_5_20_ten_wide_iron_condor_map"
            ),
            "authorization_policy": policy.policy_version,
            "evidence_status": "forward_unvalidated_user_override",
            "evidence_contract_hash": HUMAN_EVIDENCE_CONTRACT_HASH,
            "management_policy_version": (
                "management_policy.iron_condor.tp50_sl200_hold1545.v2"
            ),
            "management_plan": {
                "entry": "net_credit_limit",
                "take_profit_buyback_fraction": HUMAN_TAKE_PROFIT_BUYBACK_FRACTION,
                "stop_buyback_multiple": HUMAN_STOP_BUYBACK_MULTIPLE,
                "stop_loss_return_on_credit": 2.0,
                "hard_exit_et": HUMAN_HARD_EXIT_ET,
                "management_quote_max_age_seconds": 30.0,
                "management_quote_max_skew_seconds": 30.0,
            },
            "production_evidence": {
                "contract": "rth_20delta_fixed10_daily_first_credit20.v2",
                "opportunity_sessions": 12,
                "sessions": 9,
                "resolved_trades": 9,
                "wins": 8,
                "mean_net_pnl_dollars": 73.88,
                "mean_net_pnl_per_opportunity_dollars": 55.41,
                "opportunity_mean_bootstrap_95pct_ci_dollars": [18.88, 85.74],
                "limitations": [
                    "small_session_sample",
                    "one_minute_stop_sampling",
                    "not_fill_probability",
                ],
            },
            "automatic_ordering": False,
            "manual_action_only": True,
        }
    ]


def _structure_selection_score(structure: Mapping[str, Any]) -> float:
    quote = _map(structure.get("quote"))
    economics = _map(structure.get("economics"))
    loss = _number(economics.get("max_loss_points"))
    gain = _number(economics.get("max_gain_points"))
    bid = _number(quote.get("bid"))
    ask = _number(quote.get("ask"))
    if loss is None or loss <= 0 or gain is None or bid is None or ask is None:
        return 0.0
    return gain / loss - 0.05 * abs(ask - bid) / loss


def _with_surface_score(
    structure: Mapping[str, Any],
    facts: Mapping[str, Any],
    *,
    now: datetime,
    policy: StrategyPolicy,
) -> dict[str, Any]:
    row = dict(structure)
    base = _structure_selection_score(row)
    attribution = attribute_candidate_surface(
        row,
        facts,
        now=now,
        bump_vol_points=policy.surface_bump_vol_points,
        modifier_cap=policy.surface_risk_modifier_cap,
    )
    modifier = min(float(attribution.get("decision_modifier") or 0.0), 0.0)
    row.update(
        {
            "selection_score_base": round(base, 4),
            "surface_decision_modifier": round(modifier, 4),
            "surface_attribution": attribution,
            "selection_score": round(base + modifier, 4),
        }
    )
    return row


def _human_entry_window_open(now: datetime) -> bool:
    local = _utc(now).astimezone(NEW_YORK).time().replace(tzinfo=None)
    return HUMAN_ENTRY_START_ET <= local <= HUMAN_ENTRY_END_ET


def _short_deltas(policy: StrategyPolicy) -> tuple[float, ...]:
    configured = tuple(policy.iron_condor_short_deltas or PREFERRED_SHORT_DELTAS)
    return configured or PREFERRED_SHORT_DELTAS


def _structure_for_short_delta(
    latest: LatestState,
    expiry: str,
    *,
    spot: float,
    short_abs_delta: float,
    now: datetime,
    session_policy: StrategyPolicy,
    providers: Sequence[Provider],
) -> dict[str, Any] | None:
    width = float(session_policy.iron_condor_wing_width or WING_WIDTH)
    strikes = _ten_wide_from_short_delta(
        latest,
        expiry,
        short_abs_delta=short_abs_delta,
        width=width,
        now=now,
        policy=session_policy,
        providers=providers,
    )
    if strikes is None:
        return None
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
        return None
    put_long, put_short, call_short, call_long = legs
    put_delta = abs(_number(put_short.get("delta")) or 99.0)
    call_delta = abs(_number(call_short.get("delta")) or 99.0)
    if not (
        SHORT_DELTA_MIN <= put_delta <= SHORT_DELTA_MAX
        and SHORT_DELTA_MIN <= call_delta <= SHORT_DELTA_MAX
    ):
        return None
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
            return None
    if quote.get("status") != "ready" or not economics:
        return None
    inside = put_short_k < spot < call_short_k
    return {
        "status": "ready",
        "reason": None,
        "setup_kind": IRON_CONDOR_DELTA,
        "strategy_type": IRON_CONDOR_TYPE,
        "short_abs_delta": short_abs_delta,
        "wing_width": width,
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


def _ten_wide_from_short_delta(
    latest: LatestState,
    expiry: str,
    *,
    short_abs_delta: float,
    width: float,
    now: datetime,
    policy: StrategyPolicy,
    providers: Sequence[Provider],
) -> tuple[float, float, float, float] | None:
    put_short = nearest_abs_delta_strike(
        latest,
        expiry,
        "P",
        target_abs_delta=short_abs_delta,
        now=now,
        policy=policy,
        providers=providers,
        max_distance=SHORT_DELTA_TOLERANCE,
        min_abs_delta=SHORT_DELTA_MIN,
        max_abs_delta=min(short_abs_delta, SHORT_DELTA_MAX),
    )
    call_short = nearest_abs_delta_strike(
        latest,
        expiry,
        "C",
        target_abs_delta=short_abs_delta,
        now=now,
        policy=policy,
        providers=providers,
        max_distance=SHORT_DELTA_TOLERANCE,
        min_abs_delta=SHORT_DELTA_MIN,
        max_abs_delta=min(short_abs_delta, SHORT_DELTA_MAX),
    )
    if put_short is None or call_short is None:
        return None
    put_long = _round_to_strike(put_short - width)
    call_long = _round_to_strike(call_short + width)
    if put_long is None or call_long is None:
        return None
    if not put_long < put_short < call_short < call_long:
        return None
    if abs((put_short - put_long) - width) > 0.01 or abs((call_long - call_short) - width) > 0.01:
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
        "short_abs_delta": None,
        "wing_width": WING_WIDTH,
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
