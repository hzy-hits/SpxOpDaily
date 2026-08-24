"""Deterministic payoff and conservative BBO math for strategy candidates."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

NEW_YORK = ZoneInfo("America/New_York")

RISK_OBJECTIVE_VERSION = "risk_adjusted_cvar.v1"
RISK_OBJECTIVE_FORMULA = "E[PnL] - 0.50*CVaR10 - 1.00*quote_width - 0.25*uncertainty"
RISK_OBJECTIVE_TAIL_PROBABILITY = 0.10
RISK_OBJECTIVE_TAIL_LAMBDA = 0.50
RISK_OBJECTIVE_SLIPPAGE_MU = 1.00
RISK_OBJECTIVE_UNCERTAINTY_NU = 0.25
RISK_OBJECTIVE_MIN_SESSIONS = 20


@dataclass(frozen=True, slots=True)
class ManagementPolicy:
    """Real exit rules for candidate evaluation (v3); not settlement binary payoff.

    Default v2 keeps the 50% premium stop, trail, and 15:45 ET hard close.
    The v1 20-minute debit time stop is off (``time_stop_minutes`` is None).
    ``premium_stop_fraction`` / ``time_stop_minutes`` of None or <= 0 disable
    that exit. Changing these constants requires a version bump.
    """

    policy_version: str = "management_policy.v2"
    entry_basis: str = "conservative_combo_ask"
    valuation_basis: str = "conservative_combo_bid"
    profit_arm_return_on_debit: float = 0.50
    trail_after_arm_fraction: float = 0.75
    trail_floor_is_entry_debit: bool = True
    premium_stop_fraction: float | None = 0.50
    time_stop_minutes: int | None = None
    hard_exit_et: str = "15:45"
    fees_per_leg_per_side: float = 1.32
    entry_side: str = "debit"
    credit_take_profit_fraction: float | None = None
    credit_stop_loss_multiple: float | None = None


DEFAULT_MANAGEMENT_POLICY = ManagementPolicy()
# Legacy GTH clearing overlay remains map-only and ends at 12:30 ET.
IRON_CONDOR_MANAGEMENT_POLICY = ManagementPolicy(
    policy_version="management_policy.iron_condor.clear_1230.v1",
    time_stop_minutes=24 * 60,
    hard_exit_et="12:30",
)
# The user-authorized RTH contract buys the package back at half the entry
# credit, stops when buyback liability reaches 3x entry credit (a 200% pre-fee
# loss on collected credit), and otherwise exits at 15:45 ET. Credit marks are
# signed position liquidation values: 2x entry credit minus buyback liability.
RTH_IRON_CONDOR_MANAGEMENT_POLICY = ManagementPolicy(
    policy_version="management_policy.iron_condor.tp50_sl200_hold1545.v2",
    premium_stop_fraction=None,
    time_stop_minutes=None,
    hard_exit_et="15:45",
    entry_side="credit",
    credit_take_profit_fraction=0.50,
    credit_stop_loss_multiple=2.00,
)
# RTH STABLE_PIN butterflies need the pin to complete. Keep +50% trail and
# 15:45 ET hard close; do not flatten on the debit 20-minute / 50% stops.
PIN_BUTTERFLY_MANAGEMENT_POLICY = ManagementPolicy(
    policy_version="management_policy.pin_butterfly.hold_1545.v1",
    premium_stop_fraction=None,
    time_stop_minutes=None,
    hard_exit_et="15:45",
)
# The 60-minute physical-close lane was validated against exact 15:55 ET
# conservative combo bids. Earlier exits materially changed the result, so it
# has its own frozen hold policy and no intrahour premium/trail stop.
CLOSE_CONVERGENCE_BUTTERFLY_MANAGEMENT_POLICY = ManagementPolicy(
    policy_version="management_policy.close_convergence.hold_1555.v1",
    profit_arm_return_on_debit=10.0,
    premium_stop_fraction=None,
    time_stop_minutes=None,
    hard_exit_et="15:55",
)


@dataclass(frozen=True, slots=True)
class PolicyMark:
    at: datetime
    combo_bid: float


@dataclass(frozen=True, slots=True)
class PolicyLabel:
    tp_armed: bool
    tp_before_stop: bool
    time_to_arm_seconds: float | None
    mfe_points: float
    mae_points: float
    policy_pnl_points: float
    exit_reason: str
    exit_at: datetime | None
    exit_bid: float | None
    quote_gap_seconds_max: float
    policy_version: str
    fees_points: float



def conservative_vertical_bbo(
    long_leg: Mapping[str, Any], short_leg: Mapping[str, Any], *, now: datetime,
    max_quote_age_seconds: float = 15.0, max_source_skew_seconds: float = 2.0,
) -> dict[str, Any]:
    return _combo_bbo((long_leg, short_leg), (1, -1), now, max_quote_age_seconds,
                      max_source_skew_seconds, "spread_leg", "vertical")


def conservative_butterfly_bbo(
    lower: Mapping[str, Any], body: Mapping[str, Any], upper: Mapping[str, Any], *, now: datetime,
    max_quote_age_seconds: float = 15.0, max_source_skew_seconds: float = 2.0,
) -> dict[str, Any]:
    return _combo_bbo((lower, body, upper), (1, -2, 1), now, max_quote_age_seconds,
                      max_source_skew_seconds, "butterfly_leg", "butterfly")


def conservative_iron_condor_bbo(
    put_long: Mapping[str, Any],
    put_short: Mapping[str, Any],
    call_short: Mapping[str, Any],
    call_long: Mapping[str, Any],
    *,
    now: datetime,
    max_quote_age_seconds: float = 15.0,
    max_source_skew_seconds: float = 2.0,
) -> dict[str, Any]:
    """Conservative credit: sell shorts at bid, buy longs at ask."""

    legs = (put_long, put_short, call_short, call_long)
    integrity = _combo_quote_integrity(
        legs, now, max_quote_age_seconds, max_source_skew_seconds, "iron_condor_leg"
    )
    if integrity.get("status") != "ready":
        return integrity
    bids = [float(_nonnegative(leg.get("bid")) or 0.0) for leg in legs]
    asks = [float(_positive(leg.get("ask")) or 0.0) for leg in legs]
    conservative = bids[1] + bids[2] - asks[0] - asks[3]
    optimistic = max(asks[1] + asks[2] - bids[0] - bids[3], conservative)
    if conservative <= 0:
        return {"status": "unavailable", "reasons": ["synthetic_iron_condor_credit_invalid"]}
    observed = integrity["times"]
    return {
        "status": "ready",
        "bid": round(conservative, 4),
        "ask": round(optimistic, 4),
        "credit": round(conservative, 4),
        "side": "credit",
        "provider": integrity["provider"],
        "source_times": [value.isoformat() for value in observed],
        "source_skew_seconds": round((max(observed) - min(observed)).total_seconds(), 3),
        "max_quote_age_seconds": round(
            max((_utc(now) - value).total_seconds() for value in observed), 3
        ),
        "reasons": [],
    }


def _combo_quote_integrity(
    legs: tuple[Mapping[str, Any], ...],
    now: datetime,
    max_age: float,
    max_skew: float,
    prefix: str,
) -> dict[str, Any]:
    bids, asks = [_nonnegative(leg.get("bid")) for leg in legs], [_positive(leg.get("ask")) for leg in legs]
    times, providers, reasons = [_time(leg.get("source_at")) for leg in legs], {str(leg.get("provider") or "") for leg in legs}, []
    if len(providers) != 1 or "" in providers:
        reasons.append(f"{prefix}_provider_mismatch")
    if any(value is None for value in (*bids, *asks)):
        reasons.append(f"{prefix}_nbbo_invalid")
    if any(value is None for value in times):
        reasons.append(f"{prefix}_source_time_missing")
    else:
        observed = [value for value in times if value]
        ages = [(_utc(now) - value).total_seconds() for value in observed]
        if any(age < 0 for age in ages):
            reasons.append(f"{prefix}_quote_from_future")
        if any(age > max_age for age in ages):
            reasons.append(f"{prefix}_quote_stale")
        if (max(observed) - min(observed)).total_seconds() > max_skew:
            reasons.append(f"{prefix}_time_skew_exceeded")
    if reasons:
        return {"status": "unavailable", "reasons": reasons}
    observed = [value for value in times if value]
    return {
        "status": "ready",
        "times": observed,
        "provider": next(iter(providers)),
        "reasons": [],
    }


def _combo_bbo(
    legs: tuple[Mapping[str, Any], ...], quantities: tuple[int, ...], now: datetime,
    max_age: float, max_skew: float, prefix: str, name: str,
) -> dict[str, Any]:
    integrity = _combo_quote_integrity(legs, now, max_age, max_skew, prefix)
    if integrity.get("status") != "ready":
        return {"status": "unavailable", "reasons": list(integrity.get("reasons") or ())}
    bids = [_nonnegative(leg.get("bid")) for leg in legs]
    asks = [_positive(leg.get("ask")) for leg in legs]
    net_ask = sum(q * float(ask if q > 0 else bid) for q, bid, ask in zip(quantities, bids, asks))
    net_bid = max(sum(q * float(bid if q > 0 else ask) for q, bid, ask in zip(quantities, bids, asks)), 0.0)
    if net_ask <= 0 or net_bid > net_ask:
        return {"status": "unavailable", "reasons": [f"synthetic_{name}_bbo_invalid"]}
    observed = integrity["times"]
    return {"status": "ready", "bid": round(net_bid, 4), "ask": round(net_ask, 4),
            "provider": integrity["provider"], "source_times": [value.isoformat() for value in observed],
            "source_skew_seconds": round((max(observed) - min(observed)).total_seconds(), 3),
            "max_quote_age_seconds": round(max((_utc(now) - value).total_seconds() for value in observed), 3),
            "reasons": []}


def vertical_economics(
    *, long_strike: float, short_strike: float, net_debit: float, right: str
) -> dict[str, float]:
    direction = 1.0 if right.upper() == "C" else -1.0 if right.upper() == "P" else 0.0
    width = (short_strike - long_strike) * direction
    if direction == 0 or width <= 0:
        raise ValueError("vertical strikes must define a debit spread in right direction")
    if not 0 < net_debit < width:
        raise ValueError("vertical debit must be positive and below width")
    return {
        "width_points": width, "max_loss_points": net_debit,
        "max_gain_points": width - net_debit,
        "breakeven_spx": long_strike + direction * net_debit,
        "debit_fraction_of_width": net_debit / width,
    }


def vertical_payoff(
    settlement: float, *, long_strike: float, short_strike: float, net_debit: float, right: str
) -> float:
    if right.upper() == "C":
        intrinsic = max(settlement - long_strike, 0.0) - max(settlement - short_strike, 0.0)
    elif right.upper() == "P":
        intrinsic = max(long_strike - settlement, 0.0) - max(short_strike - settlement, 0.0)
    else:
        raise ValueError("right must be C or P")
    return intrinsic - net_debit


def butterfly_economics(*, center: float, width: float, net_debit: float) -> dict[str, float]:
    if width <= 0 or not 0 < net_debit < width:
        raise ValueError("butterfly debit must be positive and below wing width")
    return {"width_points": width, "max_loss_points": net_debit,
            "max_gain_points": width - net_debit, "breakeven_low": center - width + net_debit,
            "breakeven_high": center + width - net_debit,
            "debit_fraction_of_width": net_debit / width}


def iron_condor_economics(
    *,
    put_long: float,
    put_short: float,
    call_short: float,
    call_long: float,
    net_credit: float,
) -> dict[str, float]:
    put_width = put_short - put_long
    call_width = call_long - call_short
    if put_width <= 0 or call_width <= 0 or put_short >= call_short:
        raise ValueError("iron condor strikes must be put_long < put_short < call_short < call_long")
    width = max(put_width, call_width)
    if not 0 < net_credit < width:
        raise ValueError("iron condor credit must be positive and below the wider wing")
    return {
        "put_width_points": put_width,
        "call_width_points": call_width,
        "width_points": width,
        "max_gain_points": net_credit,
        "max_loss_points": width - net_credit,
        "breakeven_low": put_short - net_credit,
        "breakeven_high": call_short + net_credit,
        "credit_fraction_of_width": net_credit / width,
    }


def iron_condor_payoff(
    settlement: float,
    *,
    put_long: float,
    put_short: float,
    call_short: float,
    call_long: float,
    net_credit: float,
) -> float:
    """Expiry payoff for one defined-risk short iron condor."""

    if not put_long < put_short < call_short < call_long:
        raise ValueError("iron condor strikes must be strictly ordered")
    intrinsic = (
        max(put_short - settlement, 0.0)
        - max(put_long - settlement, 0.0)
        + max(settlement - call_short, 0.0)
        - max(settlement - call_long, 0.0)
    )
    return net_credit - intrinsic


def butterfly_payoff(settlement: float, *, center: float, width: float, net_debit: float) -> float:
    return max(width - abs(settlement - center), 0.0) - net_debit


def risk_adjusted_cvar_objective(
    pnl_points: Sequence[float],
    *,
    max_loss_points: float,
    quote_width_points: float,
    session_count: int,
    minimum_sessions: int = RISK_OBJECTIVE_MIN_SESSIONS,
) -> dict[str, Any]:
    """Return the frozen one-structure shadow objective from causal PnL paths.

    Strike selection remains discrete.  For one already enumerated structure,
    its non-negative size has a linear objective and ``NO_TRADE`` is the zero-
    size solution.  The result is advisory only and cannot authorize an order.
    """

    values = [float(value) for value in pnl_points]
    if not values or any(not math.isfinite(value) for value in values):
        raise ValueError("risk objective requires finite PnL paths")
    if not math.isfinite(max_loss_points) or max_loss_points <= 0:
        raise ValueError("risk objective max loss must be positive")
    if not math.isfinite(quote_width_points) or quote_width_points < 0:
        raise ValueError("risk objective quote width must be non-negative")
    if type(session_count) is not int or session_count < 0:
        raise ValueError("risk objective session count must be a non-negative integer")
    if type(minimum_sessions) is not int or minimum_sessions <= 0:
        raise ValueError("risk objective minimum sessions must be positive")

    expected = sum(values) / len(values)
    tail_count = max(1, math.ceil(len(values) * RISK_OBJECTIVE_TAIL_PROBABILITY))
    worst_tail = sorted(values)[:tail_count]
    cvar_loss = max(-(sum(worst_tail) / len(worst_tail)), 0.0)
    losses = [-value for value in values if value < 0.0]
    loss_probability = len(losses) / len(values)
    conditional_loss = sum(losses) / len(losses) if losses else 0.0
    uncertainty_fraction = max(1.0 - min(session_count / minimum_sessions, 1.0), 0.0)
    uncertainty = max_loss_points * uncertainty_fraction
    tail_penalty = RISK_OBJECTIVE_TAIL_LAMBDA * cvar_loss
    slippage_penalty = RISK_OBJECTIVE_SLIPPAGE_MU * quote_width_points
    uncertainty_penalty = RISK_OBJECTIVE_UNCERTAINTY_NU * uncertainty
    objective = expected - tail_penalty - slippage_penalty - uncertainty_penalty
    return {
        "version": RISK_OBJECTIVE_VERSION,
        "formula": RISK_OBJECTIVE_FORMULA,
        "authority": "advisory_only",
        "evidence_status": "research_unvalidated",
        "automatic_ordering": False,
        "tail_probability": RISK_OBJECTIVE_TAIL_PROBABILITY,
        "tail_lambda": RISK_OBJECTIVE_TAIL_LAMBDA,
        "slippage_mu": RISK_OBJECTIVE_SLIPPAGE_MU,
        "uncertainty_nu": RISK_OBJECTIVE_UNCERTAINTY_NU,
        "expected_pnl_points": round(expected, 6),
        "cvar10_loss_points": round(cvar_loss, 6),
        "quote_width_points": round(quote_width_points, 6),
        "model_uncertainty_points": round(uncertainty, 6),
        "tail_penalty_points": round(tail_penalty, 6),
        "slippage_penalty_points": round(slippage_penalty, 6),
        "uncertainty_penalty_points": round(uncertainty_penalty, 6),
        "objective_points": round(objective, 6),
        "objective_dollars": round(objective * 100.0, 2),
        "loss_probability": round(loss_probability, 6),
        "conditional_loss_points": round(conditional_loss, 6),
        "n_paths": len(values),
        "n_sessions": session_count,
        "minimum_sessions": minimum_sessions,
        "shadow_choice": "STRUCTURE" if objective > 0.0 else "NO_TRADE",
    }


def vertical_entry_quality(
    *, spot: float, atr: float, target: float, stop: float, trigger: float | None,
    direction: str, setup_kind: str, distance_to_vwap_points: float | None,
    impulse_15m_points: float | None, debit_fraction: float,
    thresholds: Mapping[str, float],
) -> tuple[dict[str, float], list[str]]:
    if atr <= 0:
        return {}, ["entry_quality_atr_or_geometry_unavailable"]
    if (direction == "UP" and not stop < spot < target) or (direction == "DOWN" and not target < spot < stop):
        return {}, ["entry_geometry_not_ordered"]
    target_distance, stop_distance = abs(target - spot), abs(spot - stop)
    ratio = target_distance / stop_distance if stop_distance else 0.0
    distance_atr = abs(distance_to_vwap_points or 0.0) / atr
    impulse_atr, stop_atr = abs(impulse_15m_points or 0.0) / atr, stop_distance / atr
    progress = max(0.0, (spot - trigger) / (target - trigger)) if trigger is not None and target != trigger else 0.0
    failed_break = setup_kind == "FAILED_BREAK_RECLAIM"
    short_cycle = setup_kind == "ES_VOLUME_MOMENTUM"
    min_ratio = thresholds["failed_break_min_target_room_ratio" if failed_break else "min_target_room_ratio"]
    max_debit = thresholds["failed_break_max_debit_fraction" if failed_break else "max_debit_fraction"]
    if short_cycle:
        max_progress = thresholds["es_momentum_max_progress"]
    elif failed_break:
        max_progress = thresholds["failed_break_max_trigger_target_progress"]
    else:
        max_progress = thresholds["max_trigger_target_progress"]
    result = {
        "distance_to_vwap_atr": round(distance_atr, 4), "impulse_15m_atr": round(impulse_atr, 4),
        "target_room_points": round(target_distance, 4), "stop_distance_points": round(stop_distance, 4),
        "target_room_ratio": round(ratio, 4), "debit_fraction_of_width": round(debit_fraction, 4),
        "stop_distance_atr": round(stop_atr, 4), "trigger_target_progress": round(progress, 4),
    }
    # First-impulse shorts die if VWAP distance + 15m impulse count as chase.
    # Short-cycle cards use 5m ATR exhaustion in setup facts instead.
    impulse_chase = (not short_cycle) and (
        distance_atr > thresholds["late_chase_distance_atr"]
        and impulse_atr > thresholds["late_chase_impulse_atr"]
    )
    late = (
        impulse_chase
        or ratio < min_ratio
        or debit_fraction > max_debit
        or progress >= max_progress
    )
    if late:
        return result, ["direction_valid_but_entry_too_late"]
    if not thresholds["min_stop_atr"] <= stop_atr <= thresholds["max_stop_atr"]:
        return result, ["stop_distance_outside_atr_band"]
    return result, []


def vertical_width_path_reasons(
    *,
    long_strike: float,
    short_strike: float,
    right: str,
    target: float | None,
    remaining_expected_move: float | None,
) -> list[str]:
    """Reject debit verticals whose width is a directional lever past the thesis.

    v2 §9.5: short strike must not go beyond the target, and the wing must fit
    inside the remaining 0DTE expected move. Missing EM fails closed.
    """

    code = right.upper()
    reasons: list[str] = []
    if target is None:
        reasons.append("vertical_target_or_invalidation_unavailable")
    elif code == "C":
        if short_strike > target:
            reasons.append("vertical_short_beyond_target")
    elif code == "P":
        if short_strike < target:
            reasons.append("vertical_short_beyond_target")
    else:
        reasons.append("vertical_right_invalid")
    width = abs(short_strike - long_strike)
    if remaining_expected_move is None or remaining_expected_move <= 0:
        reasons.append("vertical_remaining_move_unavailable")
    elif width > remaining_expected_move:
        reasons.append("vertical_width_exceeds_remaining_move")
    return reasons


def debit_vertical_reach_reasons(
    *,
    spot: float,
    long_strike: float,
    short_strike: float,
    right: str,
    remaining_expected_move: float | None,
) -> list[str]:
    """Reject debit verticals the remaining expected move cannot reach.

    GTH 5–20Δ anchors can sit tens of points beyond the remaining 0DTE move.
    Width fitting inside EM is not enough if the long strike itself is unreachable.
    """

    reasons: list[str] = []
    if remaining_expected_move is None or remaining_expected_move <= 0:
        return ["vertical_remaining_move_unavailable"]
    width = abs(short_strike - long_strike)
    if width > remaining_expected_move:
        reasons.append("vertical_width_exceeds_remaining_move")
    code = right.upper()
    if code == "C":
        distance = long_strike - spot
    elif code == "P":
        distance = spot - long_strike
    else:
        return ["vertical_right_invalid"]
    if distance > remaining_expected_move:
        reasons.append("debit_long_beyond_remaining_move")
    return reasons


def _positive(value: object) -> float | None:
    return float(value) if isinstance(value, int | float) and value > 0 else None


def _nonnegative(value: object) -> float | None:
    return float(value) if isinstance(value, int | float) and value >= 0 else None


def _time(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("quote evaluation time must be timezone-aware")
    return value.astimezone(timezone.utc)


def simulate_management_policy(
    marks: Sequence[PolicyMark | Mapping[str, Any]],
    *,
    entry_ask: float,
    leg_count: int,
    entry_at: datetime,
    policy: ManagementPolicy = DEFAULT_MANAGEMENT_POLICY,
    session_date: date | None = None,
) -> PolicyLabel:
    """Replay conservative combo-bid marks under the frozen management policy.

    Fees are charged in dollars and converted to index points at $100/point.
    """

    if entry_ask <= 0:
        raise ValueError("entry_ask must be positive")
    if leg_count <= 0:
        raise ValueError("leg_count must be positive")
    start = _utc(entry_at)
    rows = [_coerce_mark(item) for item in marks]
    rows = sorted(
        (
            row
            for row in rows
            if row.at >= start
            and (policy.entry_side == "credit" or row.combo_bid >= 0)
        ),
        key=lambda row: row.at,
    )
    fees_dollars = policy.fees_per_leg_per_side * float(leg_count) * 2.0
    fees_points = fees_dollars / 100.0
    arm_level = entry_ask * (1.0 + policy.profit_arm_return_on_debit)
    stop_level = (
        entry_ask * policy.premium_stop_fraction
        if policy.premium_stop_fraction is not None and policy.premium_stop_fraction > 0
        else None
    )
    hard_exit = _hard_exit_at(start, policy.hard_exit_et, session_date=session_date)
    time_stop_at = (
        start + timedelta(minutes=policy.time_stop_minutes)
        if policy.time_stop_minutes is not None and policy.time_stop_minutes > 0
        else None
    )

    peak = entry_ask
    armed = False
    time_to_arm: float | None = None
    mfe = 0.0
    mae = 0.0
    gap_max = 0.0
    previous_at = start
    exit_at: datetime | None = None
    exit_bid: float | None = None
    exit_reason = "marks_exhausted"

    if policy.entry_side == "credit":
        take_profit = (
            entry_ask * (1.0 + policy.credit_take_profit_fraction)
            if policy.credit_take_profit_fraction is not None
            and policy.credit_take_profit_fraction > 0
            else None
        )
        stop_loss = (
            entry_ask * (1.0 - policy.credit_stop_loss_multiple)
            if policy.credit_stop_loss_multiple is not None
            and policy.credit_stop_loss_multiple > 0
            else None
        )
        for mark in rows:
            gap_max = max(gap_max, (mark.at - previous_at).total_seconds())
            previous_at = mark.at
            pnl = mark.combo_bid - entry_ask
            mfe = max(mfe, pnl)
            mae = min(mae, pnl)
            if mark.at >= hard_exit:
                exit_at, exit_bid, exit_reason = mark.at, mark.combo_bid, "hard_close"
                break
            if take_profit is not None and mark.combo_bid >= take_profit:
                armed = True
                time_to_arm = (mark.at - start).total_seconds()
                exit_at, exit_bid, exit_reason = mark.at, mark.combo_bid, "profit_take"
                break
            if stop_loss is not None and mark.combo_bid <= stop_loss:
                exit_at, exit_bid, exit_reason = mark.at, mark.combo_bid, "stop_loss"
                break
        else:
            if rows:
                exit_at, exit_bid = rows[-1].at, rows[-1].combo_bid
        policy_pnl = (
            exit_bid - entry_ask - fees_points
            if exit_bid is not None
            else -entry_ask - fees_points
        )
        return PolicyLabel(
            tp_armed=armed,
            tp_before_stop=exit_reason == "profit_take",
            time_to_arm_seconds=time_to_arm,
            mfe_points=round(mfe, 6),
            mae_points=round(mae, 6),
            policy_pnl_points=round(policy_pnl, 6),
            exit_reason=exit_reason,
            exit_at=exit_at,
            exit_bid=round(exit_bid, 6) if exit_bid is not None else None,
            quote_gap_seconds_max=round(gap_max, 3),
            policy_version=policy.policy_version,
            fees_points=round(fees_points, 6),
        )

    for mark in rows:
        gap_max = max(gap_max, (mark.at - previous_at).total_seconds())
        previous_at = mark.at
        pnl = mark.combo_bid - entry_ask
        mfe = max(mfe, pnl)
        mae = min(mae, pnl)

        if mark.at >= hard_exit:
            exit_at, exit_bid, exit_reason = mark.at, mark.combo_bid, "hard_close"
            break
        if time_stop_at is not None and mark.at >= time_stop_at:
            exit_at, exit_bid, exit_reason = mark.at, mark.combo_bid, "time_stop"
            break
        if stop_level is not None and mark.combo_bid <= stop_level:
            exit_at, exit_bid, exit_reason = mark.at, mark.combo_bid, "premium_stop"
            break
        if not armed and mark.combo_bid >= arm_level:
            armed = True
            time_to_arm = (mark.at - start).total_seconds()
            peak = mark.combo_bid
            continue
        if armed:
            peak = max(peak, mark.combo_bid)
            trail = peak * policy.trail_after_arm_fraction
            if policy.trail_floor_is_entry_debit:
                trail = max(trail, entry_ask)
            if mark.combo_bid < trail:
                exit_at, exit_bid, exit_reason = mark.at, mark.combo_bid, "trail"
                break
    else:
        if rows:
            exit_at, exit_bid = rows[-1].at, rows[-1].combo_bid
            gap_max = max(gap_max, 0.0)

    policy_pnl = (exit_bid - entry_ask - fees_points) if exit_bid is not None else -entry_ask - fees_points
    return PolicyLabel(
        tp_armed=armed,
        tp_before_stop=armed and exit_reason in {"trail", "hard_close", "time_stop", "marks_exhausted"},
        time_to_arm_seconds=time_to_arm,
        mfe_points=round(mfe, 6),
        mae_points=round(mae, 6),
        policy_pnl_points=round(policy_pnl, 6),
        exit_reason=exit_reason,
        exit_at=exit_at,
        exit_bid=round(exit_bid, 6) if exit_bid is not None else None,
        quote_gap_seconds_max=round(gap_max, 3),
        policy_version=policy.policy_version,
        fees_points=round(fees_points, 6),
    )


def management_policy_for_candidate(candidate: Mapping[str, Any] | None) -> ManagementPolicy:
    """Select the frozen exit policy for one structure. Verticals use v2."""

    row = candidate or {}
    if str(row.get("setup_kind") or "") == "CLOSE_CONVERGENCE_60M":
        return CLOSE_CONVERGENCE_BUTTERFLY_MANAGEMENT_POLICY
    if str(row.get("setup_kind") or "") == "STABLE_PIN":
        return PIN_BUTTERFLY_MANAGEMENT_POLICY
    if str(row.get("strategy_type") or "").upper() == "IRON_CONDOR":
        if str(row.get("session_mode") or "").lower() == "rth":
            return RTH_IRON_CONDOR_MANAGEMENT_POLICY
        return IRON_CONDOR_MANAGEMENT_POLICY
    return DEFAULT_MANAGEMENT_POLICY


def policy_mark_horizon_end(
    entry_at: datetime,
    policy: ManagementPolicy = DEFAULT_MANAGEMENT_POLICY,
    *,
    session_date: date | None = None,
    lookforward_minutes: int | None = None,
) -> datetime:
    """Last instant the policy may still hold: time-stop or the hard close."""

    start = _utc(entry_at)
    hard = _hard_exit_at(start, policy.hard_exit_et, session_date=session_date)
    end = hard
    if policy.time_stop_minutes is not None and policy.time_stop_minutes > 0:
        end = min(end, start + timedelta(minutes=policy.time_stop_minutes))
    if lookforward_minutes is not None:
        end = min(end, start + timedelta(minutes=lookforward_minutes))
    return end


def _coerce_mark(value: PolicyMark | Mapping[str, Any]) -> PolicyMark:
    if isinstance(value, PolicyMark):
        if value.at.tzinfo is None:
            raise ValueError("policy mark time must be timezone-aware")
        return PolicyMark(at=_utc(value.at), combo_bid=float(value.combo_bid))
    at = value.get("at")
    bid = value.get("combo_bid")
    if not isinstance(at, datetime) or at.tzinfo is None:
        raise ValueError("policy mark requires timezone-aware at")
    if not isinstance(bid, (int, float)):
        raise ValueError("policy mark requires numeric combo_bid")
    return PolicyMark(at=_utc(at), combo_bid=float(bid))


def _hard_exit_at(
    entry_at: datetime, hard_exit_et: str, *, session_date: date | None
) -> datetime:
    hour_text, minute_text = hard_exit_et.split(":", 1)
    day = session_date or entry_at.astimezone(NEW_YORK).date()
    local = datetime.combine(
        day, time(hour=int(hour_text), minute=int(minute_text)), tzinfo=NEW_YORK
    )
    return local.astimezone(timezone.utc)
