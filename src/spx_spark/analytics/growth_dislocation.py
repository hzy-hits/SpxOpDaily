"""Pure scoring and state logic for Growth Dislocation LEAPS discovery."""

from __future__ import annotations

import math
import statistics
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from spx_spark.settings.growth_dislocation import GrowthDislocationSettings


POLICY_VERSION = "growth_dislocation_leaps.v11"
IV_SCORE_CHEAP_CUTOFF = 0.10


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def price_location_52w(last: float, low_52w: float, high_52w: float) -> float | None:
    if high_52w <= low_52w:
        return None
    return (last - low_52w) / (high_52w - low_52w)


def price_features(closes: Sequence[float]) -> dict[str, float] | None:
    """Build causal daily features from closes ending at the decision value."""

    clean = [float(value) for value in closes if math.isfinite(value) and value > 0.0]
    if len(clean) < 35:
        return None
    rsi_series = _rsi_wilder(clean, period=14)
    recent_rsi = [value for value in rsi_series[-20:] if value is not None]
    if not recent_rsi or rsi_series[-1] is None:
        return None
    returns = [math.log(current / prior) for prior, current in zip(clean, clean[1:])]
    rv20 = statistics.stdev(returns[-20:]) * math.sqrt(252.0) if len(returns) >= 20 else None
    low_20d = min(clean[-20:])
    return {
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
) -> Any | None:
    eligible: list[Any] = []
    for contract in contracts:
        if not policy.min_leaps_dte <= int(contract.dte) <= policy.max_leaps_dte:
            continue
        if contract.delta is None or not (
            policy.target_delta_min <= float(contract.delta) <= policy.target_delta_max
        ):
            continue
        spread = spread_mid_ratio(contract.bid, contract.ask)
        if spread is None or spread > policy.max_leaps_spread_mid:
            continue
        if contract.volatility is None or not (
            0.0 < float(contract.volatility) <= policy.max_current_leaps_iv
        ):
            continue
        if int(contract.open_interest) < policy.min_target_leaps_open_interest:
            continue
        if spot is not None:
            value_ratio = extrinsic_value_ratio(
                spot=spot,
                strike=float(contract.strike),
                bid=float(contract.bid),
                ask=float(contract.ask),
            )
            if value_ratio is None or value_ratio > policy.max_extrinsic_value_ratio:
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
        "rsi14",
        "rsi14_min_20d",
        "return_5d",
        "return_10d",
        "sector_return_5d",
        "sector_return_10d",
        "ma10",
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
        "ivp_13w",
        "ivp_26w",
        "ivp_52w",
    )
    if any(data.get(key) is None for key in required):
        return None
    if (
        float(data["ivp_13w"]) > policy.max_ivp_13w
        or float(data["ivp_26w"]) > policy.max_ivp_26w
        or float(data["ivp_52w"]) > policy.max_ivp_52w
    ):
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
        or current_iv > policy.max_current_leaps_iv
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

    iv_score = iv_cheapness_score(
        float(data["ivp_13w"]),
        float(data["ivp_26w"]),
    )
    ivrv_score = iv_rv_score(current_iv, realized_vol_20d, policy)
    rsi_score = rsi_recovery_score(
        float(data["rsi14"]),
        float(data["rsi14_min_20d"]),
        policy,
    )
    rs_score = relative_strength_score(
        float(data["return_5d"]),
        float(data["return_10d"]),
        float(data["sector_return_5d"]),
        float(data["sector_return_10d"]),
    )
    final_score = 0.40 * iv_score + 0.10 * ivrv_score + 0.30 * rsi_score + 0.20 * rs_score
    price_dislocation_score: float | None = None
    ivp_52w_score: float | None = None
    priority_score: float | None = None
    if data.get("price_location_52w") is not None and data.get("ivp_52w") is not None:
        price_dislocation_score = 100.0 * clamp(
            1.0 - float(data["price_location_52w"]) / policy.max_price_location_52w,
            0.0,
            1.0,
        )
        ivp_52w_score = 100.0 * clamp(
            1.0 - float(data["ivp_52w"]) / policy.max_ivp_52w,
            0.0,
            1.0,
        )
        priority_score = 0.50 * price_dislocation_score + 0.50 * ivp_52w_score
    rs5_sector = float(data["return_5d"]) - float(data["sector_return_5d"])
    state = candidate_state(
        close=float(data["last"]),
        ma10=float(data["ma10"]),
        rs5_sector=rs5_sector,
    )
    return {
        "iv_score": iv_score,
        "ivrv_score": ivrv_score,
        "rs_score": rs_score,
        "rsi_score": rsi_score,
        "final_score": final_score,
        "price_dislocation_score": price_dislocation_score,
        "ivp_52w_score": ivp_52w_score,
        "priority_score": priority_score,
        "extrinsic_value_ratio": time_value_ratio,
        "state": state,
        "rs_5d_sector": rs5_sector,
        "rs_10d_sector": float(data["return_10d"]) - float(data["sector_return_10d"]),
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
    """Prefer larger eligible issuers, then use the V1 signal score as a tiebreaker."""

    return (
        -float(candidate.get("market_cap") or 0.0),
        -float(candidate.get("final_score") or 0.0),
        str(candidate.get("symbol") or ""),
    )


def priority_sort_key(candidate: Mapping[str, Any]) -> tuple[float, float, float, float, str]:
    """Prefer complete 52-week dislocation and IVP priority for notifications."""

    priority_score = candidate.get("priority_score")
    return (
        1.0 if priority_score is None else 0.0,
        -float(priority_score or 0.0),
        -float(candidate.get("final_score") or 0.0),
        -float(candidate.get("market_cap") or 0.0),
        str(candidate.get("symbol") or ""),
    )


def iv_cheapness_score(
    ivp_13w: float,
    ivp_26w: float,
) -> float:
    weighted_ivp = 0.60 * ivp_13w + 0.40 * ivp_26w
    return 100.0 * max(0.0, 1.0 - weighted_ivp / IV_SCORE_CHEAP_CUTOFF)


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


def rsi_recovery_score(
    rsi_now: float,
    rsi_min_20d: float,
    policy: GrowthDislocationSettings,
) -> float:
    if rsi_min_20d >= policy.rsi_oversold_threshold:
        return 30.0
    if rsi_now < policy.rsi_oversold_threshold:
        lower_anchor = policy.rsi_oversold_threshold - 10.0
        progress = clamp(
            (rsi_now - lower_anchor) / (policy.rsi_oversold_threshold - lower_anchor),
            0.0,
            1.0,
        )
        return 20.0 + 40.0 * progress
    if rsi_now < policy.rsi_recovery_min:
        progress = (rsi_now - policy.rsi_oversold_threshold) / (
            policy.rsi_recovery_min - policy.rsi_oversold_threshold
        )
        return 60.0 + 20.0 * progress
    if rsi_now < policy.rsi_recovery_optimal_low:
        progress = (rsi_now - policy.rsi_recovery_min) / (
            policy.rsi_recovery_optimal_low - policy.rsi_recovery_min
        )
        return 80.0 + 20.0 * progress
    if rsi_now <= policy.rsi_recovery_optimal_high:
        return 100.0
    upper_anchor = policy.rsi_recovery_optimal_high + 20.0
    progress = clamp(
        (rsi_now - policy.rsi_recovery_optimal_high)
        / (upper_anchor - policy.rsi_recovery_optimal_high),
        0.0,
        1.0,
    )
    return 100.0 - 80.0 * progress


def relative_strength_score(
    stock_ret_5d: float,
    stock_ret_10d: float,
    sector_ret_5d: float,
    sector_ret_10d: float,
) -> float:
    rs_sector = 0.6 * (stock_ret_5d - sector_ret_5d) + 0.4 * (stock_ret_10d - sector_ret_10d)
    return clamp(50.0 + 400.0 * rs_sector, 0.0, 100.0)


def candidate_state(
    *,
    close: float,
    ma10: float,
    rs5_sector: float,
) -> str:
    if rs5_sector > 0.0 and close > ma10:
        return "TRIGGER"
    if rs5_sector > 0.0 or close > ma10:
        return "ARMED"
    return "WATCH"


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
