"""Deterministic regime dimensions for strategy selection."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

from spx_spark.analytics.options.strategy_payoff import (
    CLOSE_CONVERGENCE_BUTTERFLY_MANAGEMENT_POLICY,
    DEFAULT_MANAGEMENT_POLICY,
    PIN_BUTTERFLY_MANAGEMENT_POLICY,
    ManagementPolicy,
)

MARK_HORIZONS_MINUTES: tuple[int, ...] = (1, 2, 3, 4, 5, 7, 10, 15, 20)

__all__ = (
    "CLOSE_CONVERGENCE_BUTTERFLY_MANAGEMENT_POLICY",
    "DEFAULT_MANAGEMENT_POLICY",
    "DEFAULT_STRATEGY_POLICY",
    "MARK_HORIZONS_MINUTES",
    "ManagementPolicy",
    "PIN_BUTTERFLY_MANAGEMENT_POLICY",
    "StrategyPolicy",
    "assess_rth_environment",
    "assess_regime",
    "butterfly_entry_clock_open",
    "butterfly_max_entry_minutes",
    "five_wide_look_mass_ready",
    "hmm_owns_trend_direction",
    "look_mass_ready",
    "pin_blocks_directional_spreads",
    "pin_look_trade_widths",
    "pin_look_window",
    "pin_stable_center",
    "pin_stable_next_step_text",
    "pin_stable_watch_phase",
    "pin_trade_center",
    "pin_watch_center",
)


@dataclass(frozen=True, slots=True)
class StrategyPolicy:
    policy_version: str = "strategy_policy.bootstrap.v63"
    trend_score: float = 6.0
    trend_efficiency: float = 0.45
    trend_max_vwap_crosses: float = 2.0
    trend_min_breadth: float = 0.55
    balanced_max_score: float = 3.0
    balanced_max_efficiency: float = 0.30
    balanced_min_vwap_crosses: float = 2.0
    quote_max_age_seconds: float = 15.0
    quote_max_skew_seconds: float = 2.0
    gth_quote_max_age_seconds: float = 60.0
    gth_quote_max_skew_seconds: float = 60.0
    gth_widths: tuple[float, ...] = (5.0, 10.0, 15.0, 20.0, 30.0, 40.0, 50.0)
    gth_long_offsets: tuple[float, ...] = (-5.0, 0.0, 5.0)
    gth_delta_targets: tuple[float, ...] = (0.20, 0.15, 0.10, 0.05)
    iron_condor_short_deltas: tuple[float, ...] = (0.20, 0.15, 0.10, 0.05)
    iron_condor_wing_width: float = 10.0
    surface_bump_vol_points: float = 1.0
    surface_risk_modifier_cap: float = 0.05
    rth_vix1d_expansion_15m_pct: float = 0.02
    rth_atm_iv_expansion_5m: float = 0.01
    rth_atm_iv_expansion_15m: float = 0.015
    rth_straddle_reexpansion_15m: float = -0.02
    rth_breadth_balance_min: float = 0.35
    rth_breadth_balance_max: float = 0.65
    rth_expansion_transition_max_age_seconds: float = 1_200.0
    rth_short_rate_proxy_fast_15m_pct: float = 0.0002
    rth_long_rate_proxy_fast_15m_pct: float = 0.0008
    rth_credit_stress_15m_pct: float = -0.001
    rth_dollar_confirmation_15m_pct: float = 0.001
    rth_oil_shock_15m_pct: float = 0.004
    opportunity_ttl_seconds: float = 300.0
    min_target_room_ratio: float = 1.5
    failed_break_min_target_room_ratio: float = 1.8
    max_debit_fraction: float = 0.45
    gth_max_debit_fraction: float = 0.45
    gth_max_risk_usd: float = 1000.0
    failed_break_max_debit_fraction: float = 0.40
    min_stop_atr: float = 0.25
    max_stop_atr: float = 1.0
    late_chase_distance_atr: float = 1.0
    late_chase_impulse_atr: float = 1.0
    es_momentum_min_return_1m: float = 0.35
    es_momentum_min_return_5m: float = 1.0
    es_momentum_max_return_5m_atr: float = 1.50
    es_momentum_opposite_15m_min_atr: float = 0.50
    es_momentum_fresh_break_max_age_seconds: float = 900.0
    es_momentum_extended_distance_atr: float = 2.0
    es_momentum_extended_impulse_atr: float = 2.0
    es_momentum_max_progress: float = 0.50
    es_momentum_add_min_return_5m_atr: float = 0.50
    forward_path_veto_min_sessions: int = 20
    forward_path_veto_min_loss_probability: float = 0.70
    forward_path_veto_max_objective_dollars: float = 0.0
    forward_path_veto_max_p90_net_pnl: float = 0.0
    wall_hazard_min_side_probability: float = 0.17
    wall_hazard_min_execution_ev_points: float = 0.0
    pin_thresholds: tuple[float, ...] = (0.25, 2.5, 5.0, 5.0, 8.0, 0.35, 0.55)
    pin_stable_max_minutes_to_close: float = 300.0
    pin_stable_enter_min_excursions: int = 2
    pin_stable_hold_min_excursions: int = 1
    pin_look_min_excursions: int = 1
    butterfly_look_clock_widths: tuple[float, ...] = (10.0, 15.0, 20.0, 50.0)
    pin_look_min_mass_fraction: float = 0.50
    pin_q_mode_hold_max_distance_points: float = 5.0
    pin_center_hold_max_distance_points: float = 10.0
    pin_center_switch_min_score_margin: float = 0.05
    pin_center_min_confirmation_snapshots: int = 3
    pin_center_min_dwell_seconds: float = 600.0
    pin_body_max_center_distance_points: float = 5.0
    pin_body_max_spot_distance_points: float = 15.0
    hmm_trend_min_probability: float = 0.55
    hmm_balanced_min_probability: float = 0.50
    hmm_max_age_seconds: float = 90.0
    gth_trend_min_abs_return_points: float = 1.0
    butterfly_max_debit_fraction: float = 0.35
    butterfly_max_risk_usd: float = 1000.0
    close_convergence_widths: tuple[float, ...] = (10.0, 15.0, 20.0)
    close_convergence_max_debit_fraction: float = 0.45
    close_convergence_min_training_sessions: int = 15
    butterfly_minutes_per_width_point: float = 12.0
    butterfly_five_wide_early_slack_minutes: float = 10.0
    # 11:00–13:00 ET look window, expressed as minutes remaining to 16:00.
    butterfly_five_wide_look_max_minutes: float = 300.0
    butterfly_five_wide_look_min_minutes: float = 180.0
    butterfly_unresolved_wall_em_multiple: float = 1.5
    candidate_cooldown_seconds: float = 900.0
    max_cards_per_direction_per_session: int = 2
    # GTH streak hysteresis; reprinting the same winner must not refresh the lock.
    gth_winner_stick_seconds: float = 1800.0
    # RTH accepted-card hold; a later flip still needs cash HMM TREND (v32).
    rth_winner_stick_seconds: float = 900.0
    rth_setup_hold_bars: int = 2
    max_trigger_target_progress: float = 0.60
    failed_break_max_trigger_target_progress: float = 0.50
    level_confirmation_min_target_room_ratio: float = 1.00
    level_confirmation_max_trigger_target_progress: float = 0.80

    def entry_quality_kwargs(self) -> dict[str, float]:
        names = (
            "min_target_room_ratio", "failed_break_min_target_room_ratio",
            "max_debit_fraction", "failed_break_max_debit_fraction",
            "min_stop_atr", "max_stop_atr", "late_chase_distance_atr",
            "late_chase_impulse_atr",
            "failed_break_max_trigger_target_progress",
            "max_trigger_target_progress",
            "es_momentum_max_progress",
            "level_confirmation_min_target_room_ratio",
            "level_confirmation_max_trigger_target_progress",
        )
        return {name: getattr(self, name) for name in names}


DEFAULT_STRATEGY_POLICY = StrategyPolicy()


def butterfly_max_entry_minutes(
    width: float | None, policy: StrategyPolicy = DEFAULT_STRATEGY_POLICY
) -> float | None:
    """Late-window remaining minutes that may still authorize a pin butterfly.

    5-wide adds ``butterfly_five_wide_early_slack_minutes`` (70 at the
    default 12 min/point). Wider tents stay on the raw 12 min/point clock.
    The 11:00–13:00 look window is a separate width-ladder opening, not this cap.
    """

    if width is None or width <= 0:
        return None
    minutes = float(width) * policy.butterfly_minutes_per_width_point
    if width == 5.0:
        minutes += policy.butterfly_five_wide_early_slack_minutes
    return minutes


def butterfly_entry_clock_open(
    width: float | None,
    minutes_to_close: float | None,
    policy: StrategyPolicy = DEFAULT_STRATEGY_POLICY,
) -> bool:
    """True when the pin-fly clock allows this width to be ranked.

    Look-ladder widths (10/15/20/50): 11:00–13:00 ET, or their late clocks.
    A leftover of 90 minutes (about 14:30 ET) stays closed for 5-wide.
    """

    if minutes_to_close is None or width is None or width <= 0:
        return False
    late = butterfly_max_entry_minutes(width, policy)
    if late is not None and minutes_to_close <= late:
        return True
    if width not in policy.butterfly_look_clock_widths:
        return False
    return pin_look_window(minutes_to_close, policy)


def pin_look_window(
    minutes_to_close: float | None,
    policy: StrategyPolicy = DEFAULT_STRATEGY_POLICY,
) -> bool:
    """True during the 11:00–13:00 ET look window."""

    return (
        minutes_to_close is not None
        and policy.butterfly_five_wide_look_min_minutes
        <= float(minutes_to_close)
        <= policy.butterfly_five_wide_look_max_minutes
    )


def pin_look_trade_widths(
    minutes_to_close: float | None,
    center: float | None,
    mass: Mapping[str, Any],
    policy: StrategyPolicy = DEFAULT_STRATEGY_POLICY,
) -> tuple[float, ...]:
    """Widths enumerated for a STABLE_PIN body.

    Look window follows the mass box on the 10/15/20/50 ladder. Late RTH
    keeps the 5/10/15/20 scan; 50 is look-window only.
    """

    if not pin_look_window(minutes_to_close, policy):
        return (5.0, 10.0, 15.0, 20.0)
    if center is None:
        return ()
    return tuple(
        width
        for width in policy.butterfly_look_clock_widths
        if look_mass_ready(mass, center, width, policy)
    )


def look_mass_ready(
    mass: Mapping[str, Any],
    center: float,
    width: float,
    policy: StrategyPolicy = DEFAULT_STRATEGY_POLICY,
) -> bool:
    """True when local mass is piled inside [K−W, K+W]."""

    if width <= 0:
        return False
    total = 0.0
    near = 0.0
    for key, value in mass.items():
        weight = _number(value)
        if weight is None:
            continue
        try:
            strike = float(key)
        except (TypeError, ValueError):
            continue
        total += weight
        if abs(strike - center) <= width:
            near += weight
    return total > 0 and near / total >= policy.pin_look_min_mass_fraction


def five_wide_look_mass_ready(
    mass: Mapping[str, Any],
    center: float,
    policy: StrategyPolicy = DEFAULT_STRATEGY_POLICY,
) -> bool:
    """True when local 5-point mass is piled inside [K−5, K+5]."""

    return look_mass_ready(mass, center, 5.0, policy)


def pin_stable_center(regime: Mapping[str, Any] | None) -> float | None:
    """Best PIN_STABLE body, or None when the terminal state is not stable."""

    payload = _map(regime)
    if payload.get("terminal_state") != "PIN_STABLE":
        return None
    return _pin_top_center(payload)


def pin_trade_center(regime: Mapping[str, Any] | None) -> float | None:
    """Confirmed PIN_STABLE body that may enter candidate enumeration."""

    payload = _map(regime)
    pin = _map(payload.get("pin"))
    if (
        payload.get("terminal_state") != "PIN_STABLE"
        or pin.get("center_confirmation_ready") is not True
    ):
        return None
    return _pin_top_center(payload)


def pin_blocks_directional_spreads(regime: Mapping[str, Any] | None) -> bool:
    """True when a forming or stable pin forbids RTH directional debit cards."""

    payload = _map(regime)
    if payload.get("terminal_state") == "PIN_STABLE":
        return True
    return str(_map(payload.get("pin")).get("grade") or "") == "look"


def pin_watch_center(regime: Mapping[str, Any] | None) -> float | None:
    """Observation body for LOOK or TRADE pin. Never a trade authorization."""

    payload = _map(regime)
    grade = str(_map(payload.get("pin")).get("grade") or "")
    if payload.get("terminal_state") != "PIN_STABLE" and grade != "look":
        return None
    return _pin_top_center(payload)


def _pin_top_center(regime: Mapping[str, Any]) -> float | None:
    ranked = _map(regime.get("pin")).get("top_centers") or ()
    if not isinstance(ranked, (list, tuple)) or not ranked:
        return None
    return _number(_map(ranked[0]).get("center"))


def pin_stable_watch_phase(
    minutes_to_close: float | None,
    policy: StrategyPolicy = DEFAULT_STRATEGY_POLICY,
) -> str:
    """Look-window latch, late clock-open, or the 13:00–14:50 wait gap."""

    late = butterfly_max_entry_minutes(5.0, policy)
    if late is not None and minutes_to_close is not None and minutes_to_close <= late:
        return "clock_open"
    if minutes_to_close is not None and (
        policy.butterfly_five_wide_look_min_minutes
        <= minutes_to_close
        <= policy.butterfly_five_wide_look_max_minutes
    ):
        return "look"
    return "wait"


def pin_stable_next_step_text(
    minutes_to_close: float | None,
    policy: StrategyPolicy = DEFAULT_STRATEGY_POLICY,
) -> str:
    phase = pin_stable_watch_phase(minutes_to_close, policy)
    if phase == "look":
        return "11–13 仅评已确认中轴的 10–50 点蝶；提交前刷新三腿报价"
    if phase == "clock_open":
        return "5 点蝶尾盘时钟已开，等待精确三腿报价与赔率"
    late = butterfly_max_entry_minutes(5.0, policy)
    if late is None:
        return "午盘看蝶窗已过，等待 5 点蝶尾盘时钟"
    return f"午盘看蝶窗已过；5 点限价等距收盘 ≤{late:g} 分钟（约 14:50 ET）"


HMM_STATE_DIRECTION = {
    "state_00": "DOWN",
    "state_01": None,
    "state_02": "UP",
}


def hmm_owns_trend_direction(regime: Mapping[str, Any]) -> str | None:
    """Return UP/DOWN when cash HMM owns a TREND path; else None."""

    hmm = _map(regime.get("hmm"))
    if hmm.get("owns_path") is not True:
        return None
    if str(regime.get("path_state") or "") != "TREND":
        return None
    direction = str(regime.get("path_direction") or "").upper()
    return direction if direction in {"UP", "DOWN"} else None


def assess_rth_environment(
    facts: Mapping[str, Any],
    *,
    path_state: str,
    terminal_state: str,
    policy: StrategyPolicy = DEFAULT_STRATEGY_POLICY,
) -> dict[str, Any]:
    """Classify RTH conditions for structure choice, never market direction."""

    if str(_map(facts.get("session")).get("mode") or "").lower() != "rth":
        return {"state": "NOT_APPLICABLE", "status": "not_applicable", "direction_authority": "none"}
    event = _map(facts.get("event"))
    if event.get("entry_allowed") is False or str(event.get("state") or "") in {
        "pre_event",
        "SCHEDULED_EVENT_RISK",
    }:
        return {
            "state": "EVENT_RISK",
            "status": "ready",
            "direction_authority": "none",
            "range_structures_allowed": False,
            "directional_structures_allowed": False,
            "reasons": ["scheduled_event_can_reprice_remaining_distribution"],
        }

    volatility = _map(facts.get("volatility"))
    path = _map(facts.get("path"))
    structure = _map(facts.get("structure"))
    macro = _map(facts.get("macro_context"))
    vix1d = _number(volatility.get("vix1d_return_15m_pct"))
    atm_5m = _number(volatility.get("atm_iv_change_5m"))
    atm_15m = _number(volatility.get("atm_iv_change_15m"))
    straddle_decay = _number(volatility.get("atm_straddle_decay_15m"))
    breadth = _number(path.get("breadth_above_vwap"))
    core = {
        "vix1d_return_15m_pct": vix1d,
        "atm_iv_change_5m": atm_5m,
        "atm_iv_change_15m": atm_15m,
        "atm_straddle_decay_15m": straddle_decay,
        "breadth_above_vwap": breadth,
    }
    missing = [key for key, value in core.items() if value is None]
    if missing:
        breadth_only = missing == ["breadth_above_vwap"]
        return {
            "state": "INSUFFICIENT_DATA",
            "status": "degraded" if breadth_only else "unavailable",
            "direction_authority": "none",
            "range_structures_allowed": False,
            "directional_structures_allowed": breadth_only,
            "missing": missing,
            "reasons": ["rth_breadth_unavailable_directional_advisory" if breadth_only else "rth_environment_core_inputs_unavailable"],
        }

    gamma_state = str(structure.get("gamma_state") or "unknown")
    expansion_signals = {
        "vix1d_expanding": float(vix1d) >= policy.rth_vix1d_expansion_15m_pct,
        "atm_iv_5m_expanding": float(atm_5m) >= policy.rth_atm_iv_expansion_5m,
        "atm_iv_15m_expanding": float(atm_15m) >= policy.rth_atm_iv_expansion_15m,
        "straddle_reexpanding": (
            float(straddle_decay) <= policy.rth_straddle_reexpansion_15m
        ),
        "negative_gamma": gamma_state == "negative_gamma_acceleration",
    }
    contraction_signals = {
        "vix1d_not_expanding": float(vix1d) <= 0.0,
        "atm_iv_5m_not_expanding": float(atm_5m) <= 0.0,
        "atm_iv_15m_not_expanding": float(atm_15m) <= 0.0,
        "straddle_decaying": float(straddle_decay) >= 0.0,
    }
    breadth_balanced = (
        policy.rth_breadth_balance_min
        <= float(breadth)
        <= policy.rth_breadth_balance_max
    )
    breadth_directional = not breadth_balanced
    rate_short = _number(macro.get("short_rate_price_return_15m_pct"))
    rate_long = _number(macro.get("long_rate_price_return_15m_pct"))
    rates_fast = bool(
        rate_short is not None
        and rate_long is not None
        and rate_short * rate_long > 0
        and (
            abs(rate_short) >= policy.rth_short_rate_proxy_fast_15m_pct
            or abs(rate_long) >= policy.rth_long_rate_proxy_fast_15m_pct
        )
    )
    credit = _number(macro.get("credit_hyg_minus_lqd_15m_pct"))
    dollar = _number(macro.get("dollar_uup_return_15m_pct"))
    oil = _number(macro.get("oil_uso_return_15m_pct"))
    confirmations = {
        "rates_etf_price_shock": rates_fast,
        "credit_stress": credit is not None and credit <= policy.rth_credit_stress_15m_pct,
        "dollar_strength": (
            dollar is not None and dollar >= policy.rth_dollar_confirmation_15m_pct
        ),
        "oil_shock": oil is not None and abs(oil) >= policy.rth_oil_shock_15m_pct,
    }
    expansion_count = sum(expansion_signals.values())
    expansion_confirmed = expansion_count >= 2 or (
        expansion_count >= 1
        and (
            path_state == "TREND"
            or breadth_directional
            or any(confirmations.values())
        )
    )
    contraction_confirmed = (
        sum(contraction_signals.values()) >= 3
        and breadth_balanced
        and (path_state == "BALANCED" or terminal_state == "PIN_STABLE")
        and gamma_state != "negative_gamma_acceleration"
    )
    decision_at = _time(facts.get("decision_at"))
    previous_environment = _map(facts.get("previous_rth_environment"))
    previous_observed_at = _time(previous_environment.get("observed_at"))
    last_expansion_at = _time(previous_environment.get("last_expansion_at"))
    if (
        last_expansion_at is None
        and previous_environment.get("state") == "RISK_EXPANSION"
    ):
        last_expansion_at = previous_observed_at
    if expansion_confirmed:
        last_expansion_at = decision_at
    transition_age_seconds = (
        (decision_at - last_expansion_at).total_seconds()
        if decision_at is not None
        and last_expansion_at is not None
        and last_expansion_at <= decision_at
        else None
    )
    recent_expansion = bool(
        transition_age_seconds is not None
        and transition_age_seconds <= policy.rth_expansion_transition_max_age_seconds
    )
    expansion_to_contraction = contraction_confirmed and recent_expansion
    state = (
        "RISK_EXPANSION"
        if expansion_confirmed
        else "EXPANSION_TO_CONTRACTION"
        if expansion_to_contraction
        else "VOL_CONTRACTION_BALANCE"
        if contraction_confirmed
        else "MIXED_UNCONFIRMED"
    )
    return {
        "state": state,
        "status": "ready",
        "direction_authority": "none",
        "directional_structures_allowed": state == "RISK_EXPANSION",
        "range_structures_allowed": state
        in {"VOL_CONTRACTION_BALANCE", "EXPANSION_TO_CONTRACTION"},
        "observed_at": decision_at.isoformat() if decision_at is not None else None,
        "last_expansion_at": (
            last_expansion_at.isoformat() if last_expansion_at is not None else None
        ),
        "transition_age_seconds": transition_age_seconds,
        "expansion_signals": expansion_signals,
        "contraction_signals": contraction_signals,
        "macro_confirmations": confirmations,
        "gamma_state": gamma_state,
        "evidence": core,
        "macro_proxy_status": macro.get("status") or "unavailable",
        "macro_proxy_semantics": macro.get("semantics"),
        "reasons": [
            "macro_is_filter_not_direction",
            "price_trigger_still_required",
        ],
    }


def assess_regime(
    facts: Mapping[str, Any], policy: StrategyPolicy = DEFAULT_STRATEGY_POLICY
) -> dict[str, Any]:
    path, event = _map(facts.get("path")), _map(facts.get("event"))
    score, efficiency = _number(path.get("direction_score")), _number(path.get("efficiency_ratio_30m"))
    crosses, breadth = _number(path.get("vwap_crosses_30m")), _number(path.get("breadth_above_vwap"))
    slope, price_side = _number(path.get("vwap_slope")), str(path.get("price_vs_vwap") or "").lower()
    if not price_side:
        distance = _number(path.get("distance_to_vwap_points"))
        price_side = "above" if distance is not None and distance > 0 else "below" if distance is not None and distance < 0 else ""
    direction = "UP" if score is not None and score > 0 else "DOWN" if score is not None and score < 0 else None
    inputs = (score, efficiency, crosses, breadth, slope)
    contradictions: list[str] = []
    cross = _map(facts.get("cross_index"))
    source = str(cross.get("source") or "")
    session_mode = str(_map(facts.get("session")).get("mode") or "")
    if source == "globex_index":
        distance = _number(path.get("distance_to_vwap_points"))
        if distance is not None:
            price_side = (
                "above" if distance > 0 else "below" if distance < 0 else ""
            )
    hmm_cross = _hmm_cross_map(facts, policy)
    if hmm_cross is None:
        cross_state, cross_direction = None, None
        hmm_payload = {**_hmm_unused_payload(facts), "owns_path": False}
    else:
        cross_state, cross_direction, _cross_reasons, hmm_payload = hmm_cross
        hmm_payload = {**hmm_payload, "owns_path": False}
    hmm_owns_path = hmm_cross is not None and source == "cash_index"
    if hmm_owns_path:
        state, direction, reasons, _ = hmm_cross
        hmm_payload = {**hmm_payload, "owns_path": True}
        if (
            state == "TREND"
            and direction in {"UP", "DOWN"}
            and price_side
            and ("above" if direction == "UP" else "below") not in price_side
        ):
            state = "TRANSITION"
            contradictions.append("price_vwap_direction_conflict")
            reasons = [*reasons, "hmm_price_vwap_contradiction"]
        confidence = round(float(hmm_payload["max_state_probability"]), 2)
    else:
        if hmm_cross is not None:
            hmm_payload = {**hmm_payload, "reason": "hmm_cross_state_only_not_path"}
        state, direction, reasons, contradictions, confidence = _coordinate_path(
            facts,
            policy,
            inputs=inputs,
            price_side=price_side,
            direction=direction,
            use_es_path=source == "globex_index" or session_mode == "gth",
        )
    event_state = {
        "pre_event": "SCHEDULED_EVENT_RISK", "post_event": "POST_EVENT_DISCOVERY",
        "normal": "NORMAL",
    }.get(str(event.get("state") or "unavailable"), "UNCERTAIN")
    pin = _pin_assessment(facts, policy)
    environment = assess_rth_environment(
        facts,
        path_state=state,
        terminal_state=str(pin["terminal_state"]),
        policy=policy,
    )
    coordinate = {
        "cash_index": "index:SPX",
        "globex_index": "future:ES",
    }.get(source) or cross.get("anchor")
    return {
        "schema_version": "regime_assessment.v1", "policy_version": policy.policy_version,
        "path_state": state, "path_direction": direction, "terminal_state": pin["terminal_state"],
        "event_state": event_state, "entry_state": "INSUFFICIENT_DATA",
        "cross_state": cross_state, "cross_direction": cross_direction,
        "coordinate": coordinate,
        "confidence": confidence,
        "reasons": reasons, "contradictions": contradictions, "pin": pin,
        "hmm": hmm_payload,
        "rth_environment": environment,
    }


def _coordinate_path(
    facts: Mapping[str, Any],
    policy: StrategyPolicy,
    *,
    inputs: tuple[Any, ...],
    price_side: str,
    direction: str | None,
    use_es_path: bool,
) -> tuple[str, str | None, list[str], list[str], float]:
    if use_es_path:
        return _es_coordinate_path(_map(facts.get("path")), policy, price_side=price_side)
    score, efficiency, crosses, breadth, slope = inputs
    contradictions: list[str] = []
    capabilities = _map(facts.get("capabilities"))
    path_capability = _map(capabilities.get("path"))
    path_capability_ready = (
        path_capability.get("ready") is True
        if path_capability
        else _map(facts.get("quality")).get("status") == "ready"
    )
    confidence = round(sum(value is not None for value in inputs) / 5, 2)
    if not path_capability_ready:
        return "UNCERTAIN", direction, ["strategy_facts_degraded"], contradictions, confidence
    if None in inputs:
        return "UNCERTAIN", direction, ["path_inputs_unavailable"], contradictions, confidence
    trend = bool(
        abs(float(score)) >= policy.trend_score
        and float(efficiency) >= policy.trend_efficiency
        and float(crosses) <= policy.trend_max_vwap_crosses
        and ((float(score) > 0 and float(breadth) >= policy.trend_min_breadth and float(slope) > 0)
             or (float(score) < 0 and float(breadth) <= 1 - policy.trend_min_breadth and float(slope) < 0))
    )
    if trend and price_side and ("above" if float(score) > 0 else "below") not in price_side:
        trend = False
        contradictions.append("price_vwap_direction_conflict")
    balanced = bool(
        abs(score) <= policy.balanced_max_score
        and efficiency < policy.balanced_max_efficiency
        and crosses >= policy.balanced_min_vwap_crosses
    )
    if trend:
        return "TREND", direction, ["direction_score_confirmed", "path_efficiency_confirmed"], contradictions, 1.0
    if balanced:
        return "BALANCED", None, ["low_path_efficiency", "multiple_vwap_crosses"], contradictions, confidence
    return "TRANSITION", direction, ["path_inputs_not_aligned"], contradictions, confidence


def _es_coordinate_path(
    path: Mapping[str, Any],
    policy: StrategyPolicy,
    *,
    price_side: str,
) -> tuple[str, str | None, list[str], list[str], float]:
    ret5 = _number(path.get("return_5m_points"))
    ret15 = _first_number(path.get("impulse_15m_points"), path.get("return_15m_points"))
    ret1 = _number(path.get("return_1m_points"))
    returns = [value for value in (ret5, ret15, ret1) if value is not None]
    contradictions: list[str] = []
    if not returns:
        return "UNCERTAIN", None, ["es_path_returns_unavailable"], contradictions, 0.0
    threshold = policy.gth_trend_min_abs_return_points
    signed = next((value for value in returns if abs(value) >= threshold), returns[0])
    direction = "UP" if signed > 0 else "DOWN" if signed < 0 else None
    if direction is None:
        return "TRANSITION", None, ["es_path_flat"], contradictions, 0.4
    vwap_conflict = bool(
        price_side and ("above" if direction == "UP" else "below") not in price_side
    )
    if vwap_conflict:
        contradictions.append("price_vwap_direction_conflict")
    efficiency = _number(path.get("efficiency_ratio_30m"))
    trend = bool(
        efficiency is not None
        and float(efficiency) >= policy.trend_efficiency
        and abs(float(signed)) >= threshold
        and not vwap_conflict
    )
    if trend:
        return "TREND", direction, ["es_path_return_confirmed", "path_efficiency_confirmed"], contradictions, 0.7
    if vwap_conflict:
        return "TRANSITION", direction, ["es_price_vwap_contradiction"], contradictions, 0.5
    return "TRANSITION", direction, ["es_path_not_aligned"], contradictions, 0.5


def _hmm_cross_map(
    facts: Mapping[str, Any], policy: StrategyPolicy
) -> tuple[str, str | None, list[str], dict[str, Any]] | None:
    hmm = _map(facts.get("hmm"))
    cross = _map(facts.get("cross_index"))
    source = str(cross.get("source") or "")
    posterior = _map(hmm.get("posterior"))
    probabilities = {
        state: _number(posterior.get(state)) for state in HMM_STATE_DIRECTION
    }
    if (
        hmm.get("status") != "available"
        or cross.get("status") != "ready"
        or cross.get("session_open") is not True
        or source not in {"cash_index", "globex_index"}
        or any(value is None for value in probabilities.values())
    ):
        return None
    resolved = {state: float(value) for state, value in probabilities.items() if value is not None}
    dominant = max(resolved, key=resolved.__getitem__)
    max_probability = resolved[dominant]
    direction = HMM_STATE_DIRECTION[dominant]
    payload = {
        "used": True,
        "status": "available",
        "source": source,
        "anchor": cross.get("anchor"),
        "dominant_state": dominant,
        "max_state_probability": round(max_probability, 4),
        "posterior": {state: round(value, 4) for state, value in resolved.items()},
        "reason": None,
    }
    if dominant == "state_01" and max_probability >= policy.hmm_balanced_min_probability:
        return "BALANCED", None, ["hmm_index_balanced"], payload
    if direction in {"UP", "DOWN"} and max_probability >= policy.hmm_trend_min_probability:
        return (
            "TREND",
            direction,
            ["hmm_index_trend", f"hmm_dominant:{dominant}"],
            payload,
        )
    return "TRANSITION", direction, ["hmm_index_mixed_posterior"], payload


def _hmm_unused_payload(facts: Mapping[str, Any]) -> dict[str, Any]:
    hmm = _map(facts.get("hmm"))
    cross = _map(facts.get("cross_index"))
    reason = str(hmm.get("reason") or "")
    if hmm.get("status") != "available":
        reason = reason or "hmm_unavailable"
    elif cross.get("status") != "ready" or cross.get("session_open") is not True:
        reason = reason or "hmm_index_basket_not_ready"
    else:
        reason = reason or "hmm_index_not_used"
    return {
        "used": False,
        "status": hmm.get("status") or "unavailable",
        "source": cross.get("source"),
        "reason": reason,
    }


def _pin_assessment(facts: Mapping[str, Any], policy: StrategyPolicy) -> dict[str, Any]:
    path, vc, structure = _map(facts.get("path")), _map(facts.get("value_center")), _map(facts.get("structure"))
    vol, mass = _map(facts.get("volatility")), _map(structure.get("q_local_mass_5pt"))
    er, vc15, vc30, vc60 = (_number(path.get("efficiency_ratio_30m")), *(_number(vc.get(f"spx_{w}")) for w in ("15m", "30m", "60m")))
    decay = _number(vol.get("atm_straddle_decay_15m"))
    closes = [float(value) for value in path.get("pin_path_spx") or () if isinstance(value, int | float)]
    breadth = _number(path.get("breadth_above_vwap"))
    vix = _number(vol.get("vix_return_15m_pct"))
    q_mode, q_mode_source = _pin_q_mode(mass, _map(facts.get("pin_latch")), policy)
    required = (er, vc15, vc30, vc60, q_mode, decay, breadth, vix)
    if None in required or len(closes) < 4 or not mass:
        return {"terminal_state": "UNCERTAIN", "reason": "pin_inputs_unavailable", "top_centers": []}
    shock_state = str(_map(facts.get("shock")).get("state") or "NONE")
    if shock_state in {"ACTIVE", "POST_SHOCK_DISCOVERY"}:
        return {
            "terminal_state": "NONE",
            "reason": f"shock_{shock_state.lower()}",
            "depin_risk": 1.0,
            "top_centers": [],
        }
    centers = [float(key) for key in mass if str(key).replace(".", "", 1).isdigit()]
    returns = {center: _excursion_returns(closes, center) for center in centers}
    drift30, drift60 = float(vc15) - float(vc30), float(vc15) - float(vc60)
    extreme = abs(closes[-1] - closes[-4]) >= 5 and closes[-1] in {min(closes[-4:]), max(closes[-4:])}
    depin = min(1.0, 0.25 * max(abs(drift30) / 5, abs(drift60) / 8)
                + 0.20 * min(float(er) / 0.4, 1) + 0.20 * (abs(float(breadth) - 0.5) * 2 if breadth is not None else 0)
                + 0.15 * min(max(float(vix), 0) / 0.01, 1) + 0.10 * extreme
                + 0.10 * min(max(-float(decay), 0) / 0.05, 1))
    refs = [_number(structure.get(key)) for key in ("zero_gamma", "put_wall", "call_wall")]
    flip = structure.get("flip_zone")
    if isinstance(flip, (list, tuple)) and len(flip) >= 2:
        refs.append(sum(map(float, flip[:2])) / 2)
    gamma = min((value for value in refs if value is not None), key=lambda value: abs(value - float(q_mode)), default=None)
    max_mass = max(float(value) for value in mass.values()) or 1.0
    ranked = sorted(({
        "center": center,
        "score": round(0.25 * math.exp(-min((abs(center - value) for value in refs if value is not None), default=30) / 5)
                       + 0.25 * (0.5 * math.exp(-abs(center - float(vc30)) / 5) + 0.5 * math.exp(-abs(center - float(vc60)) / 7.5))
                       + 0.20 * float(mass[f"{center:g}"]) / max_mass
                       + 0.15 * (0, 0.4, 0.7, 1)[min(returns[center], 3)] + 0.10 * (float(decay) > 0)
                       - 0.25 * min(max(abs(drift30) / 5, abs(drift60) / 8), 1) - 0.20 * depin, 4),
        "excursion_returns": returns[center],
    } for center in centers), key=lambda row: row["score"], reverse=True)
    latch = _map(facts.get("pin_latch"))
    raw_leader = _number(_map(ranked[0]).get("center")) if ranked else None
    center_source = "current_score"
    previous_center = _number(latch.get("center"))
    if ranked and previous_center is not None:
        previous_row = next(
            (
                row
                for row in ranked
                if abs(float(row["center"]) - previous_center) <= 0.01
            ),
            None,
        )
        leader = ranked[0]
        if (
            previous_row is not None
            and previous_row is not leader
            and abs(float(leader["center"]) - previous_center)
            <= policy.pin_center_hold_max_distance_points
            and float(leader["score"]) - float(previous_row["score"])
            < policy.pin_center_switch_min_score_margin
        ):
            ranked = [previous_row, *(row for row in ranked if row is not previous_row)]
            center_source = "previous_center_score_margin_hold"
    er_max, drift30_max, drift60_max, migrate30, migrate60, stable_risk, block_risk = policy.pin_thresholds
    migrating = abs(drift30) > migrate30 or abs(drift60) > migrate60 or float(er) > 0.40 or extreme
    aligned = gamma is not None and max(float(q_mode), float(vc30), gamma) - min(float(q_mode), float(vc30), gamma) <= 5
    minutes_to_close = facts.get("minutes_to_close")
    excursions = max(returns.values(), default=0)
    held = _pin_stable_hold(facts, returns, policy)
    stable = (
        minutes_to_close is not None
        and int(minutes_to_close) <= policy.pin_stable_max_minutes_to_close
        and float(er) < er_max and abs(drift30) <= drift30_max and abs(drift60) <= drift60_max
        and (
            excursions >= policy.pin_stable_enter_min_excursions
            or (held and excursions >= policy.pin_stable_hold_min_excursions)
        )
        and float(vix) <= 0.01 and not extreme and aligned
        and float(decay) > 0 and depin < stable_risk
    )
    gth = str(_map(facts.get("session")).get("mode") or "").strip().lower() == "gth"
    in_look_window = pin_look_window(
        float(minutes_to_close) if isinstance(minutes_to_close, int | float) else None,
        policy,
    )
    look = (
        not gth
        and not migrating
        and depin < block_risk
        and excursions >= policy.pin_look_min_excursions
        and in_look_window
        and q_mode is not None
    )
    if migrating or depin >= block_risk:
        terminal, grade = "PIN_MIGRATING", "migrating"
    elif stable:
        terminal, grade = "PIN_STABLE", "stable"
    elif look:
        terminal, grade = "NONE", "look"
    else:
        terminal, grade = "NONE", "none"
    selected_center = _number(_map(ranked[0]).get("center")) if ranked else None
    decision_at = _time(facts.get("decision_at"))
    same_center = (
        selected_center is not None
        and previous_center is not None
        and abs(selected_center - previous_center) <= 0.01
    )
    if same_center:
        previous_count = int(_number(latch.get("center_confirmation_count")) or 1)
        confirmation_count = previous_count + 1
        first_seen_at = (
            _time(latch.get("center_first_seen_at"))
            or _time(latch.get("decision_at"))
            or decision_at
        )
    else:
        confirmation_count = 1 if selected_center is not None else 0
        first_seen_at = decision_at
    confirmation_age = (
        max((decision_at - first_seen_at).total_seconds(), 0.0)
        if decision_at is not None and first_seen_at is not None
        else 0.0
    )
    confirmation_ready = (
        terminal == "PIN_STABLE"
        and confirmation_count >= policy.pin_center_min_confirmation_snapshots
        and confirmation_age >= policy.pin_center_min_dwell_seconds
    )
    confirmation_reason = (
        "pin_not_stable"
        if terminal != "PIN_STABLE"
        else "center_snapshot_confirmation_pending"
        if confirmation_count < policy.pin_center_min_confirmation_snapshots
        else "center_dwell_confirmation_pending"
        if confirmation_age < policy.pin_center_min_dwell_seconds
        else "confirmed"
    )
    return {
        "terminal_state": terminal,
        "depin_risk": round(depin, 4),
        "drift_30m": round(drift30, 2),
        "drift_60m": round(drift60, 2),
        "recent_extreme_acceptance": extreme,
        "q_mode": q_mode,
        "q_mode_source": q_mode_source,
        "grade": grade,
        "center": selected_center,
        "raw_leader_center": raw_leader,
        "center_source": center_source,
        "center_confirmation_count": confirmation_count,
        "center_confirmation_required": policy.pin_center_min_confirmation_snapshots,
        "center_first_seen_at": first_seen_at.isoformat() if first_seen_at else None,
        "center_confirmation_age_seconds": round(confirmation_age, 3),
        "center_confirmation_min_seconds": policy.pin_center_min_dwell_seconds,
        "center_confirmation_ready": confirmation_ready,
        "center_confirmation_reason": confirmation_reason,
        "excursion_held": bool(held and stable and excursions < policy.pin_stable_enter_min_excursions),
        "top_centers": ranked[:3],
    }


def _pin_q_mode(
    mass: Mapping[str, Any],
    latch: Mapping[str, Any],
    policy: StrategyPolicy,
) -> tuple[float | None, str]:
    """PIN alignment uses the local 5-point mass peak, with a one-bin hold."""

    ranked = _mass_ranked_centers(mass)
    local = ranked[0] if ranked else None
    if local is None:
        return None, "missing"
    if latch.get("terminal_state") != "PIN_STABLE":
        return local, "local_mass"
    previous = _number(latch.get("q_mode"))
    if (
        previous is not None
        and abs(previous - local) <= policy.pin_q_mode_hold_max_distance_points
        and any(abs(center - previous) <= 0.01 for center in ranked[:2])
        and abs(previous - local) > 0.01
    ):
        return previous, "local_mass_held"
    return local, "local_mass"


def _mass_ranked_centers(mass: Mapping[str, Any]) -> list[float]:
    ranked: list[tuple[float, float]] = []
    for key, value in mass.items():
        try:
            center = float(key)
        except (TypeError, ValueError):
            continue
        weight = _number(value)
        if weight is None:
            continue
        ranked.append((center, weight))
    ranked.sort(key=lambda item: item[1], reverse=True)
    return [center for center, _weight in ranked]


def _pin_stable_hold(
    facts: Mapping[str, Any],
    returns: Mapping[float, int],
    policy: StrategyPolicy,
) -> bool:
    latch = _map(facts.get("pin_latch"))
    if latch.get("terminal_state") != "PIN_STABLE":
        return False
    if str(latch.get("session_date") or "") != str(facts.get("session_date") or ""):
        return False
    center = _number(latch.get("center"))
    if center is None:
        return False
    reach = policy.pin_body_max_center_distance_points
    return any(
        abs(candidate - center) <= reach and count >= policy.pin_stable_hold_min_excursions
        for candidate, count in returns.items()
    )


def _excursion_returns(values: list[float], center: float) -> int:
    away, count = False, 0
    for value in values:
        if abs(value - center) >= 5:
            away = True
        elif away and abs(value - center) <= 2.5:
            count += 1
            away = False
    return count


def _map(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _number(value: object) -> float | None:
    return float(value) if isinstance(value, int | float) else None


def _time(value: object) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else None


def _first_number(*values: object) -> float | None:
    for value in values:
        number = _number(value)
        if number is not None:
            return number
    return None
