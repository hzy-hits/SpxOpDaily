"""Pure scoring and state logic for Growth Dislocation LEAPS discovery."""

from __future__ import annotations

import math
import statistics
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from spx_spark.settings.growth_dislocation import GrowthDislocationSettings


POLICY_VERSION = "growth_dislocation_leaps.v8"
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
) -> Any | None:
    eligible = [
        contract
        for contract in contracts
        if policy.min_leaps_dte <= int(contract.dte) <= policy.max_leaps_dte
        and contract.delta is not None
        and policy.target_delta_min <= float(contract.delta) <= policy.target_delta_max
        and contract.bid is not None
        and contract.ask is not None
        and float(contract.ask) > float(contract.bid) >= 0.0
    ]
    if not eligible:
        return None

    def selection_key(contract: Any) -> tuple[float, int, float, int, str]:
        spread = spread_mid_ratio(float(contract.bid), float(contract.ask))
        assert spread is not None
        preferred = int(int(contract.dte) < policy.preferred_leaps_dte)
        return (
            spread,
            preferred,
            abs(float(contract.delta) - 0.70),
            -int(contract.open_interest),
            str(contract.symbol),
        )

    return min(eligible, key=selection_key)


def spread_mid_ratio(bid: float | None, ask: float | None) -> float | None:
    if bid is None or ask is None or bid < 0.0 or ask <= bid:
        return None
    mid = (bid + ask) / 2.0
    return (ask - bid) / mid if mid > 0.0 else None


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
        "max_option_dte",
        "ivp_13w",
        "ivp_26w",
    )
    if any(data.get(key) is None for key in required):
        return None
    if float(data["ivp_13w"]) > policy.max_ivp_13w or float(data["ivp_26w"]) > policy.max_ivp_26w:
        return None
    spread = float(data["spread_mid"])
    if spread > policy.max_leaps_spread_mid:
        return None
    if int(data["max_option_dte"]) < policy.min_leaps_dte:
        return None

    iv_score = iv_cheapness_score(
        float(data["ivp_13w"]),
        float(data["ivp_26w"]),
    )
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
    final_score = 0.50 * iv_score + 0.30 * rsi_score + 0.20 * rs_score
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
            1.0
            - float(data["ivp_52w"])
            / max(policy.max_ivp_13w, policy.max_ivp_26w),
            0.0,
            1.0,
        )
        priority_score = 0.50 * price_dislocation_score + 0.50 * ivp_52w_score
    rs5_sector = float(data["return_5d"]) - float(data["sector_return_5d"])
    state = candidate_state(
        rsi_now=float(data["rsi14"]),
        rsi_min_20d=float(data["rsi14_min_20d"]),
        close=float(data["last"]),
        ma10=float(data["ma10"]),
        rs5_sector=rs5_sector,
        policy=policy,
    )
    return {
        "iv_score": iv_score,
        "rs_score": rs_score,
        "rsi_score": rsi_score,
        "final_score": final_score,
        "price_dislocation_score": price_dislocation_score,
        "ivp_52w_score": ivp_52w_score,
        "priority_score": priority_score,
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
    rsi_now: float,
    rsi_min_20d: float,
    close: float,
    ma10: float,
    rs5_sector: float,
    policy: GrowthDislocationSettings,
) -> str:
    was_oversold = rsi_min_20d < policy.rsi_oversold_threshold
    if was_oversold and rsi_now >= policy.rsi_recovery_min and rs5_sector > 0.0 and close > ma10:
        return "TRIGGER"
    if was_oversold and rsi_now > policy.rsi_oversold_threshold:
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
