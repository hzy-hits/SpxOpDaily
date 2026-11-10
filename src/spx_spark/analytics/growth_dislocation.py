"""Pure scoring and state logic for Growth Dislocation LEAPS discovery."""

from __future__ import annotations

import math
import statistics
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from spx_spark.settings.growth_dislocation import GrowthDislocationSettings


POLICY_VERSION = "growth_dislocation_leaps.v14"


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def price_location_52w(last: float, low_52w: float, high_52w: float) -> float | None:
    if high_52w <= low_52w:
        return None
    return (last - low_52w) / (high_52w - low_52w)


def market_cap_score(market_cap: float, policy: GrowthDislocationSettings) -> float:
    """Size preference for history coverage and opportunity ranking, not a gate."""
    return 100.0 * market_cap / (market_cap + policy.min_market_cap)


def price_features(closes: Sequence[float]) -> dict[str, Any] | None:
    """Completed-close features; breakout evidence never uses the next bar early.

    v14: MA20 rises over five sessions, MA50 is non-declining, close > MA50.
    Break the preceding 20 closes, then hold that FIXED level on the next
    completed session. Evidence survives only while structure and level hold.
    All thresholds are initial, forward-unvalidated policy, not fitted alpha.
    """

    if len(closes) < 61 or any(not math.isfinite(value) or value <= 0 for value in closes):
        return None
    clean = [float(value) for value in closes]
    rsi_series = _rsi_wilder(clean, period=14)
    recent_rsi = [value for value in rsi_series[-20:] if value is not None]
    if not recent_rsi or rsi_series[-1] is None:
        return None
    returns = [math.log(current / prior) for prior, current in zip(clean, clean[1:])]
    rv20 = statistics.stdev(returns[-20:]) * math.sqrt(252.0) if len(returns) >= 20 else None
    low_20d = min(clean[-20:])
    breakout_index = confirmed_index = None
    breakout_level = 0.0
    repair_origin_observed = False
    for index in range(54, len(clean)):
        ma20 = statistics.fmean(clean[index - 19:index + 1])
        ma50 = statistics.fmean(clean[index - 49:index + 1])
        slope20 = ma20 / statistics.fmean(clean[index - 24:index - 4]) - 1.0
        slope50 = ma50 / statistics.fmean(clean[index - 54:index - 4]) - 1.0
        repaired = clean[index] > ma50 and slope20 > 0.0 and slope50 >= 0.0
        if not repaired or clean[index] < breakout_level:
            repair_origin_observed = True
            breakout_index = confirmed_index = None
            breakout_level = 0.0
        if repaired and breakout_index is not None and index == breakout_index + 1:
            confirmed_index = index
        if repaired and repair_origin_observed and breakout_index is None and clean[index] > max(clean[index - 20:index]):
            breakout_level = max(clean[index - 20:index])
            breakout_index = index
    return {
        "daily_close": clean[-1],
        "ma20": ma20,
        "ma50": ma50,
        "ma20_slope_5d": slope20,
        "ma50_slope_5d": slope50,
        "breakout_level": breakout_level or None,
        "breakout_index": breakout_index,
        "confirmation_index": confirmed_index,
        "confirmation_age_sessions": len(clean) - 1 - confirmed_index if confirmed_index is not None else None,
        "return_20d": clean[-1] / clean[-21] - 1.0,
        "return_60d": clean[-1] / clean[-61] - 1.0,
        "rsi14": float(rsi_series[-1]),
        "rsi14_min_20d": min(recent_rsi),
        "return_5d": clean[-1] / clean[-6] - 1.0,
        "return_10d": clean[-1] / clean[-11] - 1.0,
        "ma5": statistics.fmean(clean[-5:]),
        "ma10": statistics.fmean(clean[-10:]),
        "low_20d": low_20d,
        "distance_from_20d_low": clean[-1] / low_20d - 1.0,
        "realized_vol_20d": float(rv20) if rv20 is not None else 0.0,
    }


def select_target_leaps(
    contracts: Sequence[Any],
    policy: GrowthDislocationSettings,
    *,
    spot: float | None = None,
    rejection_counts: dict[str, int] | None = None,
) -> Any | None:
    eligible: list[Any] = []
    def reject(reason: str) -> None:
        if rejection_counts is not None:
            rejection_counts[reason] = rejection_counts.get(reason, 0) + 1

    for contract in contracts:
        if not policy.min_leaps_dte <= int(contract.dte) <= policy.max_leaps_dte:
            reject("dte_out_of_range")
            continue
        if contract.delta is None:
            reject("delta_missing")
            continue
        if not (
            policy.target_delta_min <= float(contract.delta) <= policy.target_delta_max
        ):
            reject("delta_out_of_range")
            continue
        spread = spread_mid_ratio(contract.bid, contract.ask)
        if spread is None:
            reject("bid_ask_invalid")
            continue
        if spread > policy.max_leaps_spread_mid:
            reject("spread_above_limit")
            continue
        if (
            contract.volatility is None
            or not math.isfinite(float(contract.volatility))
            or float(contract.volatility) <= 0.0
        ):
            reject("iv_missing")
            continue
        if contract.open_interest is None:
            reject("open_interest_missing")
            continue
        if int(contract.open_interest) < policy.min_target_leaps_open_interest:
            reject("open_interest_below_limit")
            continue
        if spot is not None:
            value_ratio = extrinsic_value_ratio(
                spot=spot,
                strike=float(contract.strike),
                bid=float(contract.bid),
                ask=float(contract.ask),
            )
            if value_ratio is None:
                reject("extrinsic_invalid")
                continue
            if value_ratio > policy.max_extrinsic_value_ratio:
                reject("extrinsic_above_limit")
                continue
        eligible.append(contract)
    if not eligible:
        return None

    target_delta = (policy.target_delta_min + policy.target_delta_max) / 2.0

    def selection_key(contract: Any) -> tuple[int, float, float, int, int, str]:
        spread = spread_mid_ratio(float(contract.bid), float(contract.ask))
        assert spread is not None
        preferred = int(int(contract.dte) < policy.preferred_leaps_dte)
        return (
            preferred,
            abs(float(contract.delta) - target_delta),
            spread,
            -int(contract.open_interest),
            -int(contract.dte),
            str(contract.symbol),
        )

    return min(eligible, key=selection_key)


def spread_mid_ratio(bid: float | None, ask: float | None) -> float | None:
    if bid is None or ask is None or bid < 0.0 or ask <= bid:
        return None
    mid = (bid + ask) / 2.0
    return (ask - bid) / mid if mid > 0.0 else None


def extrinsic_value_ratio(
    *,
    spot: float,
    strike: float,
    bid: float,
    ask: float,
) -> float | None:
    """Return a call's mid-price time value as a fraction of spot."""

    if spot <= 0.0 or strike <= 0.0 or bid < 0.0 or ask <= bid:
        return None
    mid = (bid + ask) / 2.0
    intrinsic = max(spot - strike, 0.0)
    if mid < intrinsic:
        return None
    return (mid - intrinsic) / spot


def score_candidate(
    data: Mapping[str, Any],
    policy: GrowthDislocationSettings,
) -> dict[str, Any] | None:
    required = (
        "daily_close", "ma20", "ma50", "ma20_slope_5d", "ma50_slope_5d",
        "breakout_level", "confirmation_index",
        "return_20d", "return_60d", "sector_return_20d", "sector_return_60d",
        "market_return_20d", "market_return_60d",
        "last",
        "spread_mid",
        "leaps_dte",
        "leaps_delta",
        "leaps_bid",
        "leaps_ask",
        "leaps_strike",
        "target_leaps_oi",
        "current_iv",
        "realized_vol_20d",
        "ivp_52w",
        "price_location_52w",
        "market_cap",
        "dividend_yield",
    )
    if any(data.get(key) is None or not math.isfinite(float(data[key])) for key in required):
        return None
    if candidate_state(data, spot=float(data["last"])) != "TRIGGER":
        return None
    if float(data["market_cap"]) <= 0.0 or float(data["dividend_yield"]) < 0.0:
        return None
    if not 0.0 <= float(data["ivp_52w"]) <= 1.0:
        return None
    spread = float(data["spread_mid"])
    if spread > policy.max_leaps_spread_mid:
        return None
    if not policy.min_leaps_dte <= int(data["leaps_dte"]) <= policy.max_leaps_dte:
        return None
    if not policy.target_delta_min <= float(data["leaps_delta"]) <= policy.target_delta_max:
        return None
    current_iv = float(data["current_iv"])
    realized_vol_20d = float(data["realized_vol_20d"])
    if (
        current_iv <= 0.0
        or realized_vol_20d <= 0.0
    ):
        return None
    if int(data["target_leaps_oi"]) < policy.min_target_leaps_open_interest:
        return None
    time_value_ratio = extrinsic_value_ratio(
        spot=float(data["last"]),
        strike=float(data["leaps_strike"]),
        bid=float(data["leaps_bid"]),
        ask=float(data["leaps_ask"]),
    )
    if time_value_ratio is None or time_value_ratio > policy.max_extrinsic_value_ratio:
        return None

    ivrv_score = iv_rv_score(current_iv, realized_vol_20d, policy)
    excess = sum(
        weight * (float(data[f"return_{window}d"]) - float(data[f"{benchmark}_return_{window}d"]))
        for window, weight in ((20, 0.3), (60, 0.2))
        for benchmark in ("sector", "market")
    )
    rs_score = clamp(50.0 + 250.0 * excess, 0.0, 100.0)
    cap_score = market_cap_score(float(data["market_cap"]), policy)
    ivp_52w_score = 100.0 / (1.0 + float(data["ivp_52w"]) / policy.max_ivp_52w)
    # Saturating slopes reward repair, not unlimited distance above an average.
    trend_score = 50.0 + 25.0 * clamp(float(data["ma20_slope_5d"]) / 0.03, 0.0, 1.0) + 25.0 * clamp(float(data["ma50_slope_5d"]) / 0.02, 0.0, 1.0)
    support = max(float(data["breakout_level"]), float(data["ma50"]))
    support_distance = max(0.0, float(data["last"]) / support - 1.0)
    entry_score = 60.0 / (1.0 + support_distance / 0.10) + 40.0 / (1.0 + realized_vol_20d / 0.40)
    absolute_iv_score = 100.0 / (1.0 + current_iv / policy.max_current_leaps_iv)
    vol_cost_score = 0.5 * ivp_52w_score + 0.25 * absolute_iv_score + 0.25 * ivrv_score
    liquidity_score = 100.0 / (1.0 + spread / 0.04)
    size_liquidity_score = 0.5 * cap_score + 0.5 * liquidity_score
    final_score = (
        0.35 * trend_score + 0.20 * rs_score + 0.20 * entry_score
        + 0.15 * vol_cost_score + 0.10 * size_liquidity_score
    )
    return {
        "ivrv_score": ivrv_score,
        "rs_score": rs_score,
        "trend_score": trend_score,
        "entry_score": entry_score,
        "size_liquidity_score": size_liquidity_score,
        "support_level": support,
        "distance_from_support": support_distance,
        "final_score": final_score,
        "market_cap_score": cap_score,
        "ivp_52w_score": ivp_52w_score,
        "vol_cost_score": vol_cost_score,
        "absolute_iv_score": absolute_iv_score,
        "priority_score": final_score,
        "extrinsic_value_ratio": time_value_ratio,
        "state": "TRIGGER",
        "rs_20d_sector": float(data["return_20d"]) - float(data["sector_return_20d"]),
        "rs_60d_market": float(data["return_60d"]) - float(data["market_return_60d"]),
    }


def apply_crowding(
    candidates: Sequence[dict[str, Any]],
    policy: GrowthDislocationSettings,
    *,
    sort_key: Callable[[Mapping[str, Any]], tuple[Any, ...]] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    ordered = sorted(candidates, key=sort_key or candidate_sort_key)
    top: list[dict[str, Any]] = []
    reserve: list[dict[str, Any]] = []
    counts: dict[str, int] = {}
    for candidate in ordered:
        group = str(candidate.get("crowding_group") or "Unknown")
        if (
            len(top) < policy.top_count
            and counts.get(group, 0) < policy.max_names_per_crowding_group
        ):
            top.append(candidate)
            counts[group] = counts.get(group, 0) + 1
        else:
            reserve.append(candidate)
    return top, reserve


def candidate_sort_key(candidate: Mapping[str, Any]) -> tuple[float, float, str]:
    """Use the same score ordering for the scanner, Core Pool and notifications."""

    return (
        -float(candidate.get("final_score") or 0.0),
        -float(candidate.get("market_cap") or 0.0),
        str(candidate.get("symbol") or ""),
    )


def priority_sort_key(candidate: Mapping[str, Any]) -> tuple[float, float, float, float, str]:
    """Use the same complete opportunity score for notifications."""

    priority_score = candidate.get("priority_score")
    return (
        1.0 if priority_score is None else 0.0,
        -float(priority_score or 0.0),
        -float(candidate.get("final_score") or 0.0),
        -float(candidate.get("market_cap") or 0.0),
        str(candidate.get("symbol") or ""),
    )


def iv_rv_score(
    current_iv: float,
    realized_vol_20d: float,
    policy: GrowthDislocationSettings,
) -> float:
    """Score option IV relative to recent realized volatility without gating it."""

    ratio = current_iv / realized_vol_20d
    return 100.0 * clamp(
        (policy.iv_rv_zero_score_ratio - ratio)
        / (policy.iv_rv_zero_score_ratio - policy.iv_rv_full_score_ratio),
        0.0,
        1.0,
    )


def candidate_state(data: Mapping[str, Any], *, spot: float | None = None) -> str:
    """Price confirmation is independent of score; live prices can only veto."""
    required = ("daily_close", "ma50", "ma20_slope_5d", "ma50_slope_5d")
    if any(data.get(key) is None or not math.isfinite(float(data[key])) for key in required):
        return "STALE"
    if not (
        float(data["daily_close"]) > float(data["ma50"])
        and float(data["ma20_slope_5d"]) > 0.0
        and float(data["ma50_slope_5d"]) >= 0.0
    ):
        return "WATCH"
    level = float(data.get("breakout_level") or 0.0)
    if data.get("confirmation_index") is None or not math.isfinite(level) or level <= 0.0 or float(data["daily_close"]) < level:
        return "ARMED"
    if spot is not None and (not math.isfinite(spot) or spot < level or spot <= float(data["ma50"])):
        return "ARMED"
    return "TRIGGER"


def _rsi_wilder(closes: Sequence[float], *, period: int) -> list[float | None]:
    values: list[float | None] = [None] * len(closes)
    changes = [current - prior for prior, current in zip(closes, closes[1:])]
    if len(changes) < period:
        return values
    gains = [max(change, 0.0) for change in changes]
    losses = [max(-change, 0.0) for change in changes]
    average_gain = statistics.fmean(gains[:period])
    average_loss = statistics.fmean(losses[:period])
    values[period] = _rsi(average_gain, average_loss)
    for index in range(period, len(changes)):
        average_gain = (average_gain * (period - 1) + gains[index]) / period
        average_loss = (average_loss * (period - 1) + losses[index]) / period
        values[index + 1] = _rsi(average_gain, average_loss)
    return values


def _rsi(average_gain: float, average_loss: float) -> float:
    if average_loss == 0.0:
        return 100.0 if average_gain > 0.0 else 50.0
    return 100.0 - 100.0 / (1.0 + average_gain / average_loss)


def new_core_member(row: Mapping[str, Any], *, at: str, reason: str) -> dict[str, Any]:
    return {
        "symbol": str(row["symbol"]),
        "lifecycle_status": "CORE_ACTIVE",
        "pause_reason": None,
        "entered_at": at,
        "entry_reason": reason,
        "last_eligible_at": at,
        "last_complete_observed_at": at,
        "snapshot": dict(row),
    }


def core_display_row(
    *,
    member: Mapping[str, Any],
    stored: Mapping[str, Any],
    current: Mapping[str, Any] | None,
    warming: Mapping[str, Any] | None,
    observation: Mapping[str, Any] | None,
) -> dict[str, Any]:
    row = dict(current or stored)
    if current is not None:
        today_state = str(current.get("state") or "WATCH")
    elif observation and observation.get("trend_evaluated") and observation.get("today_state") in {"WATCH", "ARMED"}:
        # Another symbol's partial data can freeze membership, not the known
        # price verdict. The stored score/contract remain historical.
        today_state = str(observation["today_state"])
    else:
        # A retained non-candidate has unknown eligibility, never a current buy
        # opportunity. Confirmed failures have already left the dynamic pool.
        today_state = "STALE"
    row.update(
        {
            "core_status": member.get("lifecycle_status", "CORE_PAUSED"),
            "today_state": today_state,
            "pause_reason": member.get("pause_reason"),
            "entry_reason": member.get("entry_reason"),
            "entered_at": member.get("entered_at"),
            "snapshot_status": "current" if current is not None else "historical",
            "snapshot_as_of_et": row.get("as_of_et"),
            # Keep the last eligible snapshot for audit, but do not let its
            # fields or clocks stand in for a missing current observation.
            "current_observation": dict(observation or {}),
        }
    )
    if current is None:
        row["detail_refreshed_this_run"] = False
        row["data_quality"] = "historical"
    if current is None and observation is not None:
        for key in (
            "price_location_52w",
            "market_cap",
            "dividend_yield",
            "ivp_13w",
            "ivp_26w",
            "ivp_52w",
        ):
            if observation.get(key) is not None:
                row[key] = observation[key]
    if warming is not None:
        row["data_quality_reasons"] = warming.get("data_quality_reasons", [])
    return row
