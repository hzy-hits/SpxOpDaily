"""Always-on 5–20Δ short-leg iron condor map with a 10-point defined-risk wing."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from spx_spark.analytics.options.strategy_payoff import (
    IRON_CONDOR_MANAGEMENT_POLICY,
    RTH_IRON_CONDOR_MANAGEMENT_POLICY,
    conservative_iron_condor_bbo,
    iron_condor_economics,
)
from spx_spark.analytics.options.surface_attribution import attribute_candidate_surface
from spx_spark.analytics.options.quote_policy import option_field_observed_at
from spx_spark.application.market_features.session_quote_selection import provider_quote
from spx_spark.application.order_map.candidate_factory import (
    _candidate_id,
    _gth_quote_policy,
    _hash,
    _map,
    _number,
    _quote_valid_until,
    _round_to_strike,
    _session_option_legs,
    _time,
    nearest_abs_delta_strike,
)
from spx_spark.application.order_map.gth_iron_condor import (
    GTH_EVIDENCE_CONTRACT_HASH,
    GTH_MAX_EXACT_QUOTE_AGE_SECONDS,
    GTH_MAX_EXACT_QUOTE_SKEW_SECONDS,
    gth_iron_condor_gate_failures,
    gth_iron_condor_transition,
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
MIN_CREDIT_FRACTION = 0.25
TRANSITION_MIN_CREDIT_FRACTION = 0.23
TRANSITION_MIN_SIDE_CREDIT_SHARE = 0.25
MAX_CREDIT_FRACTION = 0.55
HUMAN_MAX_ATM_IV = 0.2374713681
HUMAN_MAX_SMILE_RICHNESS = 0.0313827831
HUMAN_SHORT_DELTA = 0.20
HUMAN_ENTRY_START_ET = time(10, 0)
HUMAN_ENTRY_END_ET = time(11, 0)
HUMAN_MAX_RISK_DOLLARS = 1_000.0
HUMAN_TAKE_PROFIT_BUYBACK_FRACTION = 0.50
HUMAN_STOP_BUYBACK_MULTIPLE = 3.0
HUMAN_HARD_EXIT_ET = "15:45"
HUMAN_SESSION_STATE_KEY = "iron_condor_session_state"
GAMMA_RISK_VERSION = "iron_condor_gamma_risk.v1"
GAMMA_RISK_LOW_GCR10 = 0.10
GAMMA_RISK_NORMAL_GCR10 = 0.20
GAMMA_RISK_HOT_GCR10 = 0.30
RESEARCH_SHORT_DELTA = 0.175
RESEARCH_MIN_CREDIT_FRACTION = 0.20
RESEARCH_MAX_CREDIT_FRACTION = 0.23
RESEARCH_MIN_SIDE_CREDIT_SHARE = 0.25
RESEARCH_EVIDENCE_VERSION = "iron_condor_17_5d_fixed10_credit20_23_shadow.v1"
HUMAN_EVIDENCE_CONTRACT_HASH = (
    "sha256:2a8a220ed3dee489ccb2373954ade3cdf2a5390f46ee3e9e46d6871299e2e680"
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
    session_mode = (
        "gth" if DEFAULT_MARKET_CALENDAR.is_spx_gth_open(now) else "rth"
    )
    primary["session_mode"] = session_mode
    if session_mode == "gth":
        primary["gth_transition"] = gth_iron_condor_transition(facts, now=now)
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
    research = (
        _structure_for_short_delta(
            latest,
            expiry,
            spot=spot,
            short_abs_delta=RESEARCH_SHORT_DELTA,
            now=now,
            session_policy=session_policy,
            providers=providers,
        )
        if session_mode == "rth"
        else None
    )
    observation: dict[str, Any] = {
        "version": RESEARCH_EVIDENCE_VERSION,
        "decision_effect": "record_only",
        "manual_authority_eligible": False,
        "automatic_ordering": False,
        "target_short_abs_delta": RESEARCH_SHORT_DELTA,
        "wing_width": WING_WIDTH,
        "credit_fraction_band": [
            RESEARCH_MIN_CREDIT_FRACTION,
            RESEARCH_MAX_CREDIT_FRACTION,
        ],
    }
    if research is None:
        observation.update(
            status="unavailable",
            reason=(
                "iron_condor_research_quotes_unavailable"
                if session_mode == "rth"
                else "iron_condor_research_rth_only"
            ),
        )
    else:
        research_quote = _map(research.get("quote"))
        research_economics = _map(research.get("economics"))
        put_long, put_short, call_short, call_long = (
            _map(research.get(name))
            for name in ("put_long", "put_short", "call_short", "call_long")
        )
        research_credit = _number(research_quote.get("credit"))
        research_credit_fraction = _number(
            research_economics.get("credit_fraction_of_width")
        )
        side_credits = (
            (_number(put_short.get("bid")) or 0.0)
            - (_number(put_long.get("ask")) or 0.0),
            (_number(call_short.get("bid")) or 0.0)
            - (_number(call_long.get("ask")) or 0.0),
        )
        research_side_share = (
            min(side_credits) / research_credit
            if research_credit is not None and research_credit > 0
            else None
        )
        qualified = bool(
            research_credit_fraction is not None
            and RESEARCH_MIN_CREDIT_FRACTION
            <= research_credit_fraction
            <= RESEARCH_MAX_CREDIT_FRACTION
            and research_side_share is not None
            and research_side_share >= RESEARCH_MIN_SIDE_CREDIT_SHARE
        )
        observation.update(
            status="qualified" if qualified else "outside_observation_band",
            put_short_abs_delta=round(abs(_number(put_short.get("delta")) or 0.0), 8),
            call_short_abs_delta=round(abs(_number(call_short.get("delta")) or 0.0), 8),
            wing_width=research.get("wing_width"),
            strikes=list(research.get("strikes") or ()),
            quote=dict(research_quote),
            economics=dict(research_economics),
            minimum_side_credit_share=(
                round(research_side_share, 8)
                if research_side_share is not None
                else None
            ),
        )
    primary["research_observations"] = [observation]
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
    gth_transition = _map(structure.get("gth_transition"))
    human_window_open = bool(
        (session_mode == "rth" and _human_entry_window_open(now))
        or (session_mode == "gth" and gth_transition.get("status") == "qualified")
    )
    candidate_quote_policy = session_policy
    if human_window_open:
        expiry = str(structure.get("expiry") or "")
        spot = _number(structure.get("spot"))
        candidate_quote_policy = (
            replace(
                policy,
                quote_max_age_seconds=GTH_MAX_EXACT_QUOTE_AGE_SECONDS,
                quote_max_skew_seconds=GTH_MAX_EXACT_QUOTE_SKEW_SECONDS,
            )
            if session_mode == "gth"
            else session_policy
        )
        human_structure = (
            _structure_for_short_delta(
                latest,
                expiry,
                spot=spot,
                short_abs_delta=HUMAN_SHORT_DELTA,
                now=now,
                session_policy=candidate_quote_policy,
                providers=(Provider.IBKR,) if session_mode == "gth" else (Provider.SCHWAB,),
            )
            if expiry and spot is not None
            else None
        )
        if human_structure is None or human_structure.get("status") != "ready":
            return []
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
    spot = _number(structure.get("spot"))
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
    quote_valid = _quote_valid_until(legs, now=now, policy=candidate_quote_policy)
    score = float(structure.get("selection_score") or 0.0)
    human_surface_gate = human_iron_condor_surface_gate(facts)
    put_short_delta = abs(_number(put_short.get("delta")) or 0.0)
    call_short_delta = abs(_number(call_short.get("delta")) or 0.0)
    put_short_distance = spot - strikes[1] if len(strikes) == 4 and spot is not None else None
    call_short_distance = strikes[2] - spot if len(strikes) == 4 and spot is not None else None
    gamma_values = [_number(leg.get("gamma")) for leg in legs]
    gamma_times = [_time(leg.get("greeks_observed_at")) for leg in legs]
    credit = _number(quote.get("credit"))
    put_side_credit = (
        (_number(put_short.get("bid")) or 0.0)
        - (_number(put_long.get("ask")) or 0.0)
    )
    call_side_credit = (
        (_number(call_short.get("bid")) or 0.0)
        - (_number(call_long.get("ask")) or 0.0)
    )
    minimum_side_credit_share = (
        min(put_side_credit, call_side_credit) / credit
        if credit is not None and credit > 0
        else None
    )
    if (
        len(gamma_values) == 4
        and all(value is not None for value in gamma_values)
        and all(observed_at is not None for observed_at in gamma_times)
        and all(
            0.0
            <= (now - observed_at).total_seconds()
            <= candidate_quote_policy.quote_max_age_seconds
            for observed_at in gamma_times
            if observed_at is not None
        )
        and credit is not None
        and credit > 0
    ):
        put_long_gamma, put_short_gamma, call_short_gamma, call_long_gamma = (
            float(value) for value in gamma_values if value is not None
        )
        net_gamma = put_long_gamma - put_short_gamma - call_short_gamma + call_long_gamma
        greek_ages = [
            (now - observed_at).total_seconds()
            for observed_at in gamma_times
            if observed_at is not None
        ]
        greek_skew = (
            max(observed_at for observed_at in gamma_times if observed_at is not None)
            - min(observed_at for observed_at in gamma_times if observed_at is not None)
        ).total_seconds()
        gcr10 = 0.5 * abs(net_gamma) * 10.0**2 / credit
        gamma_state = (
            "LOW"
            if gcr10 <= GAMMA_RISK_LOW_GCR10
            else "NORMAL"
            if gcr10 <= GAMMA_RISK_NORMAL_GCR10
            else "HOT"
            if gcr10 <= GAMMA_RISK_HOT_GCR10
            else "HIGH"
        )
        gamma_risk = {
            "status": "ready",
            "version": GAMMA_RISK_VERSION,
            "decision_effect": (
                "gth_iron_condor_gate" if session_mode == "gth" else "explanation_only"
            ),
            "state": gamma_state,
            "net_gamma_per_spx_point": round(net_gamma, 8),
            "delta_shock_10_trader_delta": round(abs(net_gamma) * 10.0 * 100.0, 4),
            "gamma_loss_10_points": round(0.5 * abs(net_gamma) * 10.0**2, 6),
            "gcr10": round(gcr10, 8),
            "gcr20": round(0.5 * abs(net_gamma) * 20.0**2 / credit, 8),
            "nearest_short_abs_delta": round(max(put_short_delta, call_short_delta), 8),
            **(
                {
                    "greeks_max_age_seconds": round(max(greek_ages), 3),
                    "greeks_source_skew_seconds": round(greek_skew, 3),
                }
                if session_mode == "gth"
                else {}
            ),
            "entry_gate_applied": session_mode == "gth",
        }
    else:
        gamma_risk = {
            "status": "unavailable",
            "version": GAMMA_RISK_VERSION,
            "decision_effect": (
                "gth_iron_condor_gate" if session_mode == "gth" else "explanation_only"
            ),
            "state": "UNAVAILABLE",
            "entry_gate_applied": session_mode == "gth",
            "reason": "iron_condor_leg_gamma_missing_or_stale",
        }
    return [
        {
            "candidate_id": candidate_id,
            "strategy_type": IRON_CONDOR_TYPE,
            "setup_kind": IRON_CONDOR_DELTA,
            "setup_state": (
                "GTH_EXPANSION_TO_CONTRACTION"
                if session_mode == "gth" and human_window_open
                else "ENTRY_WINDOW_OPEN"
            ),
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
            "human_surface_gate": human_surface_gate,
            "spot_inside_shorts": structure.get("spot_inside_shorts"),
            "spot": spot,
            "short_abs_delta": structure.get("short_abs_delta"),
            "put_short_abs_delta": round(put_short_delta, 8),
            "call_short_abs_delta": round(call_short_delta, 8),
            "put_short_distance_points": (
                round(put_short_distance, 4) if put_short_distance is not None else None
            ),
            "call_short_distance_points": (
                round(call_short_distance, 4) if call_short_distance is not None else None
            ),
            "put_side_credit": round(put_side_credit, 4),
            "call_side_credit": round(call_side_credit, 4),
            "minimum_side_credit_share": (
                round(minimum_side_credit_share, 8)
                if minimum_side_credit_share is not None
                else None
            ),
            "gamma_risk": gamma_risk,
            "gth_transition": gth_transition,
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
                "gth_20delta_fixed10_transition_iron_condor"
                if session_mode == "gth" and human_window_open
                else "rth_20delta_fixed10_iron_condor"
                if session_mode == "rth" and human_window_open
                else "delta_5_20_ten_wide_iron_condor_map"
            ),
            "authorization_policy": policy.policy_version,
            "evidence_status": "forward_unvalidated_user_override",
            "evidence_contract_hash": (
                GTH_EVIDENCE_CONTRACT_HASH
                if session_mode == "gth"
                else HUMAN_EVIDENCE_CONTRACT_HASH
            ),
            "management_policy_version": (
                IRON_CONDOR_MANAGEMENT_POLICY.policy_version
                if session_mode == "gth"
                else RTH_IRON_CONDOR_MANAGEMENT_POLICY.policy_version
            ),
            "management_plan": {
                "entry": "net_credit_limit",
                "take_profit_buyback_fraction": HUMAN_TAKE_PROFIT_BUYBACK_FRACTION,
                "stop_buyback_multiple": HUMAN_STOP_BUYBACK_MULTIPLE,
                "stop_loss_return_on_credit": 2.0,
                "hard_exit_et": (
                    IRON_CONDOR_MANAGEMENT_POLICY.hard_exit_et
                    if session_mode == "gth"
                    else HUMAN_HARD_EXIT_ET
                ),
                "management_quote_max_age_seconds": 30.0,
                "management_quote_max_skew_seconds": 30.0,
            },
            "production_evidence": (
                {
                    "contract": "gth_20delta_fixed10_expansion_to_contraction_ibkr_gcr20_credit25_balanced_tp50_sl200_clear1230_quote30_skew10.v2",
                    "status": "forward_unvalidated_user_override",
                    "limitations": [
                        "atm_straddle_is_short_gamma_pressure_proxy_not_dealer_inventory",
                        "gth_exact_bbo_fill_probability_not_modeled",
                        "overnight_gap_tail_remains",
                    ],
                }
                if session_mode == "gth"
                else {
                    "contract": "rth_20delta_fixed10_daily_first_credit25_or_expansion_to_contraction_credit23_balanced_sides_schwab_only_1000_1100_locked_surface_advisory.v6",
                    "transition_replay_sessions": 36,
                    "transition_resolved_trades": 18,
                    "transition_wins": 16,
                    "transition_mean_net_pnl_dollars": 60.00,
                    "transition_minimum_net_pnl_dollars": -605.56,
                    "limitations": [
                        "same_sample_policy_search",
                        "production_v51_environment_not_fully_reconstructable",
                        "credit23_boundary_uses_displayed_bbo",
                        "one_minute_stop_sampling",
                        "not_fill_probability",
                    ],
                }
            ),
            "automatic_ordering": False,
            "manual_action_only": True,
        }
    ]


def human_iron_condor_surface_gate(facts: Mapping[str, Any]) -> dict[str, Any]:
    """Return ATM/smile context for explanation, never entry authorization."""

    volatility = _map(facts.get("volatility"))
    atm_iv = _number(volatility.get("atm_iv_0dte"))
    put_skew = _number(volatility.get("put_skew_25d_0dte"))
    call_skew = _number(volatility.get("call_skew_25d_0dte"))
    if atm_iv is None or put_skew is None or call_skew is None:
        return {
            "status": "unavailable",
            "passed": False,
            "blocking": False,
            "decision_effect": "explanation_only",
            "atm_iv_0dte": atm_iv,
            "smile_richness": None,
            "max_atm_iv": HUMAN_MAX_ATM_IV,
            "max_smile_richness": HUMAN_MAX_SMILE_RICHNESS,
            "reasons": ["iron_condor_surface_gate_unavailable"],
        }
    smile_richness = 0.5 * (put_skew + call_skew)
    reasons = []
    if atm_iv > HUMAN_MAX_ATM_IV:
        reasons.append("iron_condor_atm_iv_high")
    if smile_richness > HUMAN_MAX_SMILE_RICHNESS:
        reasons.append("iron_condor_smile_richness_high")
    return {
        "status": "ready",
        "passed": not reasons,
        "blocking": False,
        "decision_effect": "explanation_only",
        "atm_iv_0dte": round(atm_iv, 10),
        "smile_richness": round(smile_richness, 10),
        "max_atm_iv": HUMAN_MAX_ATM_IV,
        "max_smile_richness": HUMAN_MAX_SMILE_RICHNESS,
        "reasons": reasons,
    }


def iron_condor_session_state(
    payload: Mapping[str, Any],
    facts: Mapping[str, Any],
    candidates: Sequence[Mapping[str, Any]],
    *,
    now: datetime,
) -> dict[str, Any]:
    """Freeze the first qualifying candidate independently for RTH and GTH."""

    session_date = str(facts.get("session_date") or "")
    session_mode = str(_map(facts.get("session")).get("mode") or "").lower()
    previous = _map(payload.get("previous_strategy_decision"))
    previous_facts = _map(previous.get("market_facts"))
    previous_state = _map(previous_facts.get(HUMAN_SESSION_STATE_KEY))
    if (
        str(previous.get("session_date") or previous_facts.get("session_date") or "")
        == session_date
        and str(previous_state.get("session_mode") or "rth").lower() == session_mode
        and previous_state.get("status") == "eligible"
    ):
        return {**previous_state, "carried_forward": True}

    current_state = _map(facts.get(HUMAN_SESSION_STATE_KEY))
    if (
        current_state.get("status") == "eligible"
        and str(current_state.get("session_mode") or "rth").lower() == session_mode
    ):
        return dict(current_state)

    base = {
        "status": "waiting",
        "session_date": session_date,
        "session_mode": session_mode,
        "contract": (
            "gth_first_expansion_to_contraction_credit25_gcr20_quote30_skew10_candidate_lock"
            if session_mode == "gth"
            else "rth_daily_first_credit25_or_transition_credit23_candidate_lock_surface_advisory"
        ),
        "carried_forward": False,
    }
    if session_mode not in {"rth", "gth"}:
        return base
    for candidate in candidates:
        if not _qualifies_for_human_candidate_lock(candidate, facts):
            continue
        surface_gate = _map(candidate.get("human_surface_gate"))
        return {
            **base,
            "status": "eligible",
            "attempted_at": _utc(now).isoformat(),
            "candidate_id": candidate.get("candidate_id"),
            "strikes": list(candidate.get("strikes") or ()),
            "surface_gate": dict(surface_gate),
            "reasons": [],
        }
    return base


def human_iron_condor_entry_contract(
    candidate: Mapping[str, Any], facts: Mapping[str, Any]
) -> dict[str, Any]:
    if str(candidate.get("session_mode") or "").lower() == "gth":
        transition = _map(candidate.get("gth_transition"))
        return {
            "minimum_credit_fraction": MIN_CREDIT_FRACTION,
            "minimum_side_credit_share": TRANSITION_MIN_SIDE_CREDIT_SHARE,
            "environment_state": transition.get("status"),
            "transition_contract": transition.get("status") == "qualified",
        }
    environment = _map(facts.get("rth_environment"))
    transition = environment.get("state") == "EXPANSION_TO_CONTRACTION"
    return {
        "minimum_credit_fraction": (
            TRANSITION_MIN_CREDIT_FRACTION if transition else MIN_CREDIT_FRACTION
        ),
        "minimum_side_credit_share": (
            TRANSITION_MIN_SIDE_CREDIT_SHARE if transition else None
        ),
        "environment_state": environment.get("state"),
        "transition_contract": transition,
    }


def _qualifies_for_human_candidate_lock(
    candidate: Mapping[str, Any], facts: Mapping[str, Any]
) -> bool:
    if candidate.get("manual_authority_eligible") is not True:
        return False
    if abs((_number(candidate.get("short_abs_delta")) or 0.0) - HUMAN_SHORT_DELTA) > 1e-9:
        return False
    economics = _map(candidate.get("economics"))
    credit_fraction = _number(economics.get("credit_fraction_of_width"))
    loss = _number(economics.get("max_loss_points"))
    entry_contract = human_iron_condor_entry_contract(candidate, facts)
    minimum_credit = float(entry_contract["minimum_credit_fraction"])
    minimum_side_share = _number(entry_contract.get("minimum_side_credit_share"))
    actual_side_share = _number(candidate.get("minimum_side_credit_share"))
    session_mode = str(candidate.get("session_mode") or "").lower()
    expected_hash = (
        GTH_EVIDENCE_CONTRACT_HASH
        if session_mode == "gth"
        else HUMAN_EVIDENCE_CONTRACT_HASH
    )
    gth_gate_ready = not gth_iron_condor_gate_failures(candidate, session_mode)
    return bool(
        credit_fraction is not None
        and minimum_credit - 1e-9 <= credit_fraction <= MAX_CREDIT_FRACTION
        and (
            minimum_side_share is None
            or (
                actual_side_share is not None
                and actual_side_share + 1e-9 >= minimum_side_share
            )
        )
        and loss is not None
        and 0.0 < loss * 100.0 <= HUMAN_MAX_RISK_DOLLARS
        and candidate.get("spot_inside_shorts") is True
        and candidate.get("evidence_contract_hash") == expected_hash
        and gth_gate_ready
    )


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
    provider = Provider(str(put_long.get("provider")))
    enriched_legs: list[dict[str, Any]] = []
    for leg in legs:
        row = dict(leg)
        live_quote = provider_quote(
            latest,
            str(leg.get("contract_id") or ""),
            provider=provider,
            now=now,
        )
        if live_quote is not None and live_quote.greeks is not None:
            row["gamma"] = live_quote.greeks.gamma
            row["greeks_observed_at"] = option_field_observed_at(
                live_quote,
                field="greeks",
            ).isoformat()
        enriched_legs.append(row)
    put_long, put_short, call_short, call_long = enriched_legs
    legs = enriched_legs
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
        max_greeks_age_seconds=policy.quote_max_age_seconds,
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
        max_greeks_age_seconds=policy.quote_max_age_seconds,
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
