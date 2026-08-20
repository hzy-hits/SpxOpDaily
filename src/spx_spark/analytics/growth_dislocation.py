"""Pure scoring and state logic for Growth Dislocation LEAPS discovery."""

from __future__ import annotations

import math
import statistics
from collections.abc import Mapping, Sequence
from typing import Any

from spx_spark.settings.growth_dislocation import GrowthDislocationSettings


POLICY_VERSION = "growth_dislocation_leaps.v4"

GROWTH_TYPE_BONUS = {
    "HighGrowth": 100.0,
    "QualityGrowth": 85.0,
    "ThemeOptionality": 80.0,
    "Cyclical": 40.0,
    "Value/Other": 20.0,
    "Unclassified": 50.0,
}


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

    def selection_key(contract: Any) -> tuple[int, float, float, int, str]:
        preferred = int(int(contract.dte) < policy.preferred_leaps_dte)
        target_dte = (policy.preferred_leaps_dte + policy.max_leaps_dte) / 2.0
        return (
            preferred,
            abs(float(contract.delta) - 0.70),
            abs(int(contract.dte) - target_dte),
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
        "market_return_5d",
        "market_return_10d",
        "sector_return_5d",
        "sector_return_10d",
        "ma5",
        "ma10",
        "low_20d",
        "last",
        "market_cap",
        "spread_mid",
        "target_leaps_oi",
        "max_option_dte",
        "ivp_13w",
        "ivp_26w",
    )
    if any(data.get(key) is None for key in required):
        return None
    if float(data["ivp_13w"]) > policy.max_ivp_13w or float(data["ivp_26w"]) > policy.max_ivp_26w:
        return None
    spread = float(data["spread_mid"])
    if spread > policy.hard_max_leaps_spread_mid:
        return None

    iv_score = iv_cheapness_score(
        float(data["ivp_13w"]),
        float(data["ivp_26w"]),
        data.get("ivp_52w"),
        policy,
    )
    rsi_score = rsi_recovery_score(
        float(data["rsi14"]),
        float(data["rsi14_min_20d"]),
        policy,
    )
    price_score = price_recovery_score(
        float(data["last"]),
        float(data["low_20d"]),
        float(data["return_5d"]),
        float(data["ma5"]),
        float(data["ma10"]),
    )
    rs_score = relative_strength_score(
        float(data["return_5d"]),
        float(data["return_10d"]),
        float(data["market_return_5d"]),
        float(data["market_return_10d"]),
        float(data["sector_return_5d"]),
        float(data["sector_return_10d"]),
    )
    liquidity_score = option_liquidity_score(
        spread,
        int(data["target_leaps_oi"]),
        data.get("underlying_avg_option_volume"),
        int(data["max_option_dte"]),
        policy,
    )
    market_cap_score = market_cap_quality_score(float(data["market_cap"]))
    growth_value = _optional_number(data.get("growth_score"))
    convexity_value = _optional_number(data.get("convexity_score"))
    growth_score = clamp(growth_value if growth_value is not None else 50.0, 0.0, 100.0)
    convexity_score = clamp(
        convexity_value if convexity_value is not None else 50.0, 0.0, 100.0
    )
    final_score = (
        0.20 * iv_score
        + 0.18 * rs_score
        + 0.12 * rsi_score
        + 0.10 * price_score
        + 0.15 * liquidity_score
        + 0.08 * market_cap_score
        + 0.10 * growth_score
        + 0.07 * convexity_score
    )
    rs5_market = float(data["return_5d"]) - float(data["market_return_5d"])
    rs5_sector = float(data["return_5d"]) - float(data["sector_return_5d"])
    state = candidate_state(
        rsi_now=float(data["rsi14"]),
        rsi_min_20d=float(data["rsi14_min_20d"]),
        ma5=float(data["ma5"]),
        ma10=float(data["ma10"]),
        rs5_market=rs5_market,
        rs5_sector=rs5_sector,
        distance_from_20d_low=float(data["last"]) / float(data["low_20d"]) - 1.0,
        policy=policy,
    )
    return {
        "iv_score": iv_score,
        "rs_score": rs_score,
        "rsi_score": rsi_score,
        "price_recovery_score": price_score,
        "liquidity_score": liquidity_score,
        "market_cap_score": market_cap_score,
        "growth_score": growth_score,
        "convexity_score": convexity_score,
        "final_score": final_score,
        "state": state,
        "rs_5d_market": rs5_market,
        "rs_10d_market": float(data["return_10d"]) - float(data["market_return_10d"]),
        "rs_5d_sector": rs5_sector,
        "rs_10d_sector": float(data["return_10d"]) - float(data["sector_return_10d"]),
    }


def apply_crowding(
    candidates: Sequence[dict[str, Any]],
    policy: GrowthDislocationSettings,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    ordered = sorted(candidates, key=lambda row: float(row["final_score"]), reverse=True)
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


def iv_cheapness_score(
    ivp_13w: float,
    ivp_26w: float,
    ivp_52w: Any,
    policy: GrowthDislocationSettings,
) -> float:
    """Score IBKR-history IV percentiles; 52-week history is bonus-only."""

    score_13w = 100.0 * max(0.0, 1.0 - ivp_13w / policy.max_ivp_13w)
    score_26w = 100.0 * max(0.0, 1.0 - ivp_26w / policy.max_ivp_26w)
    score = 0.6 * score_13w + 0.4 * score_26w
    if ivp_52w is not None:
        percentile_52w = float(ivp_52w)
        score += 8.0 if percentile_52w <= 0.05 else 4.0 if percentile_52w <= 0.10 else 0.0
    return min(score, 100.0)


def rsi_recovery_score(
    rsi_now: float,
    rsi_min_20d: float,
    policy: GrowthDislocationSettings,
) -> float:
    if rsi_min_20d < policy.rsi_oversold_threshold:
        if policy.rsi_recovery_optimal_low <= rsi_now <= policy.rsi_recovery_optimal_high:
            return 100.0
        if policy.rsi_recovery_min <= rsi_now < policy.rsi_recovery_optimal_low:
            return 85.0
        if policy.rsi_oversold_threshold <= rsi_now < policy.rsi_recovery_min:
            return 65.0
        if rsi_now < policy.rsi_oversold_threshold:
            return 30.0
        if rsi_now <= 65.0:
            return 60.0
        return 30.0
    if policy.rsi_recovery_optimal_low <= rsi_now <= policy.rsi_recovery_optimal_high:
        return 55.0
    if policy.rsi_recovery_min <= rsi_now < policy.rsi_recovery_optimal_low:
        return 45.0
    return 25.0 if rsi_now < policy.rsi_recovery_min else 35.0


def price_recovery_score(
    last: float, low_20d: float, return_5d: float, ma5: float, ma10: float
) -> float:
    recovery = last / low_20d - 1.0
    if 0.03 <= recovery <= 0.12:
        base = 100.0
    elif 0.0 <= recovery < 0.03:
        base = 55.0
    elif 0.12 < recovery <= 0.20:
        base = 75.0
    elif recovery > 0.20:
        base = 40.0
    else:
        base = 10.0
    return min(base + (10.0 if return_5d > 0.0 else 0.0) + (10.0 if ma5 > ma10 else 0.0), 100.0)


def relative_strength_score(
    stock_ret_5d: float,
    stock_ret_10d: float,
    market_ret_5d: float,
    market_ret_10d: float,
    sector_ret_5d: float,
    sector_ret_10d: float,
) -> float:
    rs_market = 0.6 * (stock_ret_5d - market_ret_5d) + 0.4 * (stock_ret_10d - market_ret_10d)
    rs_sector = 0.6 * (stock_ret_5d - sector_ret_5d) + 0.4 * (stock_ret_10d - sector_ret_10d)
    return clamp(50.0 + 0.5 * (rs_market + rs_sector) * 500.0, 0.0, 100.0)


def option_liquidity_score(
    spread_mid: float,
    open_interest: int,
    underlying_avg_option_volume: Any,
    max_dte: int,
    policy: GrowthDislocationSettings,
) -> float:
    if spread_mid <= 0.03:
        spread_score = 100.0
    elif spread_mid <= 0.05:
        spread_score = 90.0
    elif spread_mid <= 0.08:
        spread_score = 75.0
    elif spread_mid <= policy.max_leaps_spread_mid:
        spread_score = 60.0
    elif spread_mid <= policy.hard_max_leaps_spread_mid:
        spread_score = 35.0
    else:
        spread_score = 0.0
    oi_score = (
        100.0
        if open_interest >= 2000
        else 85.0
        if open_interest >= 1000
        else 70.0
        if open_interest >= 500
        else 55.0
        if open_interest >= 300
        else 35.0
        if open_interest >= 100
        else 15.0
    )
    volume = float(underlying_avg_option_volume or 0.0)
    volume_score = (
        100.0
        if volume >= 50_000
        else 85.0
        if volume >= 20_000
        else 70.0
        if volume >= 10_000
        else 50.0
        if volume >= 5_000
        else 25.0
    )
    dte_score = 100.0 if max_dte >= 720 else 90.0 if max_dte >= 540 else 70.0
    return 0.40 * spread_score + 0.20 * oi_score + 0.20 * volume_score + 0.20 * dte_score


def market_cap_quality_score(market_cap: float) -> float:
    return (
        100.0
        if market_cap >= 100e9
        else 95.0
        if market_cap >= 30e9
        else 80.0
        if market_cap >= 10e9
        else 60.0
        if market_cap >= 5e9
        else 40.0
        if market_cap >= 3e9
        else 0.0
    )


def growth_convexity_profile(
    data: Mapping[str, Any],
    *,
    growth_theme: str | None,
    drawdown_from_52w_high: float,
) -> dict[str, Any]:
    """Build source-aware soft growth and rerating scores without hard rejection."""

    revenue_growth = _optional_number(data.get("revenue_growth_yoy"))
    forward_growth = _optional_number(data.get("forward_revenue_growth"))
    margin_change = _optional_number(data.get("operating_margin_change"))
    fcf_growth = _optional_number(data.get("fcf_growth_yoy"))
    fcf_positive = data.get("fcf_positive") if isinstance(data.get("fcf_positive"), bool) else None
    roi = _optional_number(data.get("return_on_investment"))
    cash_flow_proxy = data.get("cash_flow_positive_proxy") is True

    growth_quality = 0.0
    coverage = 0.0
    if revenue_growth is not None:
        coverage += 35.0
        growth_quality += _revenue_growth_points(revenue_growth)
    if forward_growth is not None:
        coverage += 25.0
        growth_quality += _forward_growth_points(forward_growth)
    if margin_change is not None:
        coverage += 20.0
        growth_quality += _margin_trend_points(margin_change)
    if fcf_growth is not None or fcf_positive is not None:
        coverage += 20.0
        growth_quality += _fcf_trend_points(fcf_growth, fcf_positive)

    types: list[str] = []
    classification_notes: list[str] = []
    if revenue_growth is not None and revenue_growth >= 0.15:
        if forward_growth is None or forward_growth >= 0.10:
            types.append("HighGrowth")
            if forward_growth is None:
                classification_notes.append("high_growth_reported_revenue_only")
    if (
        revenue_growth is not None
        and revenue_growth >= 0.08
        and roi is not None
        and roi >= 0.10
        and cash_flow_proxy
    ):
        types.append("QualityGrowth")
        classification_notes.append("positive_cash_flow_uses_pcf_proxy")
    theme = str(growth_theme or "").strip().lower() or None
    if theme is not None:
        types.append("ThemeOptionality")
    if not types:
        types.append("Value/Other" if coverage > 0.0 else "Unclassified")
    type_bonus = max(GROWTH_TYPE_BONUS[label] for label in types)
    growth_score = (
        50.0
        if coverage == 0.0 and types == ["Unclassified"]
        else min(growth_quality + 0.15 * type_bonus, 100.0)
    )

    margin_recovery = _margin_recovery_potential(data, margin_change)
    balance_score = _balance_sheet_quality(data)
    convexity = _drawdown_convexity_points(drawdown_from_52w_high)
    if "HighGrowth" in types or "ThemeOptionality" in types:
        convexity += 25.0
    convexity += 20.0 if margin_recovery == "HIGH" else 10.0 if margin_recovery == "MEDIUM" else 0.0
    has_catalyst = data.get("has_12m_catalyst")
    if has_catalyst is True:
        convexity += 20.0
    if balance_score is not None and balance_score >= 70.0:
        convexity += 10.0
    convexity_coverage = 50.0
    if margin_recovery != "UNAVAILABLE":
        convexity_coverage += 20.0
    if has_catalyst is not None:
        convexity_coverage += 20.0
    if balance_score is not None:
        convexity_coverage += 10.0

    notes = []
    if forward_growth is None:
        notes.append("forward_revenue_growth_unavailable")
    if fcf_growth is None:
        notes.append("fcf_growth_yoy_unavailable")
    if has_catalyst is None:
        notes.append("catalyst_unavailable")
    return {
        **{key: data.get(key) for key in _FUNDAMENTAL_OUTPUT_KEYS},
        "growth_theme": theme,
        "growth_type": "/".join(types),
        "growth_types": types,
        "growth_classification_notes": classification_notes,
        "growth_quality_score": min(growth_quality, 100.0),
        "growth_quality_coverage": coverage / 100.0,
        "growth_type_bonus": type_bonus,
        "growth_score": growth_score,
        "drawdown_from_52w_high": drawdown_from_52w_high,
        "margin_recovery_potential": margin_recovery,
        "has_12m_catalyst": has_catalyst,
        "balance_sheet_quality": balance_score,
        "convexity_score": min(convexity, 100.0),
        "convexity_coverage": convexity_coverage / 100.0,
        "fundamental_data_notes": notes,
    }


_FUNDAMENTAL_OUTPUT_KEYS = (
    "revenue_growth_yoy",
    "revenue_growth_ttm",
    "forward_revenue_growth",
    "operating_margin_mrq",
    "operating_margin_ttm",
    "operating_margin_change",
    "fcf_growth_yoy",
    "fcf_positive",
    "return_on_investment",
    "cash_flow_positive_proxy",
    "beta",
    "current_ratio",
    "total_debt_to_equity",
    "fundamental_source",
)


def _revenue_growth_points(value: float) -> float:
    if value >= 0.25:
        return 35.0
    if value >= 0.15:
        return 30.0
    if value >= 0.08:
        return 22.0
    return 10.0 if value >= 0.0 else 0.0


def _forward_growth_points(value: float) -> float:
    if value >= 0.20:
        return 25.0
    if value >= 0.10:
        return 18.0
    return 10.0 if value >= 0.05 else 0.0


def _margin_trend_points(value: float) -> float:
    if value > 0.03:
        return 20.0
    if value > 0.0:
        return 15.0
    return 8.0 if value > -0.03 else 0.0


def _fcf_trend_points(value: float | None, positive: bool | None) -> float:
    if value is not None:
        return 20.0 if value > 0.20 else 12.0 if value > 0.0 else 6.0 if positive else 0.0
    return 6.0 if positive else 0.0


def _drawdown_convexity_points(value: float) -> float:
    if value <= -0.60:
        return 25.0
    if value <= -0.40:
        return 20.0
    return 12.0 if value <= -0.25 else 0.0


def _margin_recovery_potential(data: Mapping[str, Any], change: float | None) -> str:
    margin = _optional_number(data.get("operating_margin_mrq"))
    if margin is None or change is None:
        return "UNAVAILABLE"
    if margin < 0.0 and change > 0.03:
        return "HIGH"
    if margin < 0.0 and change > 0.0:
        return "MEDIUM"
    return "LOW"


def _balance_sheet_quality(data: Mapping[str, Any]) -> float | None:
    current = _optional_number(data.get("current_ratio"))
    debt = _optional_number(data.get("total_debt_to_equity"))
    if current is None or debt is None:
        return None
    if current >= 1.5 and debt <= 0.5:
        return 100.0
    if current >= 1.0 and debt <= 1.0:
        return 70.0
    if current >= 0.75 and debt <= 2.0:
        return 40.0
    return 20.0


def _optional_number(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def candidate_state(
    *,
    rsi_now: float,
    rsi_min_20d: float,
    ma5: float,
    ma10: float,
    rs5_market: float,
    rs5_sector: float,
    distance_from_20d_low: float,
    policy: GrowthDislocationSettings,
) -> str:
    passed_recovery = (
        rsi_min_20d < policy.rsi_oversold_threshold and rsi_now > policy.rsi_oversold_threshold
    )
    confirmations = sum(
        (
            rsi_now >= policy.rsi_recovery_min,
            ma5 > ma10,
            rs5_market > 0.0,
            rs5_sector > 0.0,
            0.03 <= distance_from_20d_low <= 0.15,
        )
    )
    if passed_recovery and confirmations >= 3:
        return "TRIGGER"
    if passed_recovery or confirmations >= 2:
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
