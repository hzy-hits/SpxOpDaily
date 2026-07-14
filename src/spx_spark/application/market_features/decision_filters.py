"""Deterministic regime and false-breakout filters for order decisions."""

from __future__ import annotations

import math
from enum import StrEnum
from typing import Any

from spx_spark.application.market_features.models import (
    FrameQuality,
    MinuteMarketFrame,
    OptionStructureFrame,
)
from spx_spark.settings.market_features import MarketFeatureSettings


class RegimeMode(StrEnum):
    UNAVAILABLE = "unavailable"
    TRANSITION = "transition"
    TRENDING = "trending"
    MEAN_REVERTING = "mean_reverting"


class BreakoutVerdict(StrEnum):
    NOT_APPLICABLE = "not_applicable"
    UNAVAILABLE = "unavailable"
    BLOCKED = "blocked"
    PENDING = "pending"
    SUPPORTED = "supported"


_BREAKOUT_PHASES = frozenset({"break_pending", "accepted", "retest", "confirmed"})
_PHASE_IMPULSE_POINTS = {
    "break_pending": 3.0,
    "accepted": 6.0,
    "retest": 8.0,
    "confirmed": 10.0,
}


def build_regime_decision(
    market: MinuteMarketFrame,
    options: OptionStructureFrame,
    *,
    trend: dict[str, Any],
    level_decision: dict[str, Any],
    policy: MarketFeatureSettings,
) -> dict[str, Any]:
    if market.quality is FrameQuality.UNAVAILABLE:
        return {
            "mode": RegimeMode.UNAVAILABLE.value,
            "direction": "none",
            "trend_score": 0.0,
            "mean_reversion_score": 0.0,
            "evidence": [],
            "invalidations": ["es_path_unavailable"],
        }

    direction = _market_direction(market, trend)
    direction_sign = 1 if direction == "up" else -1 if direction == "down" else 0
    trend_score, trend_evidence = _trend_score(market, direction_sign, policy)
    reversion_score, reversion_evidence = _mean_reversion_score(
        market,
        options,
        level_decision,
        policy,
    )

    if (
        direction_sign
        and trend_score >= policy.trend_min_score
        and trend_score - reversion_score >= policy.regime_score_margin
    ):
        mode = RegimeMode.TRENDING
    elif (
        reversion_score >= policy.mean_reversion_min_score
        and reversion_score - trend_score >= policy.regime_score_margin
    ):
        mode = RegimeMode.MEAN_REVERTING
        direction = "none"
    else:
        mode = RegimeMode.TRANSITION

    return {
        "mode": mode.value,
        "direction": direction,
        "volatility_regime": _volatility_regime(market),
        "trend_score": round(trend_score, 1),
        "mean_reversion_score": round(reversion_score, 1),
        "evidence": [*trend_evidence, *reversion_evidence],
        "invalidations": [],
    }


def build_breakout_filter(
    market: MinuteMarketFrame,
    options: OptionStructureFrame,
    *,
    level_decision: dict[str, Any],
    regime_decision: dict[str, Any],
    policy: MarketFeatureSettings,
) -> dict[str, Any]:
    phase = str(level_decision.get("phase") or "far")
    thesis = str(level_decision.get("thesis") or "none")
    event_id = level_decision.get("event_id")
    direction_sign = _breakout_direction(level_decision)
    base = {
        "event_id": event_id,
        "phase": phase,
        "thesis": thesis,
        "direction": "up" if direction_sign > 0 else "down" if direction_sign < 0 else "none",
        "actionable": False,
    }
    if thesis != "breakout" or phase not in _BREAKOUT_PHASES or direction_sign == 0:
        return {
            **base,
            "verdict": BreakoutVerdict.NOT_APPLICABLE.value,
            "impulse_score": 0.0,
            "barrier_score": 0.0,
            "evidence": [],
            "invalidations": [],
        }
    if market.quality is FrameQuality.UNAVAILABLE or options.quality is FrameQuality.UNAVAILABLE:
        return {
            **base,
            "verdict": BreakoutVerdict.UNAVAILABLE.value,
            "impulse_score": 0.0,
            "barrier_score": 0.0,
            "evidence": [],
            "invalidations": ["market_or_option_frame_unavailable"],
        }

    impulse, impulse_evidence = _breakout_impulse(
        market,
        options,
        level_decision,
        direction_sign,
        regime_decision,
        policy,
    )
    barrier, barrier_metrics, barrier_evidence = _breakout_barrier(
        market,
        options,
        level_decision,
        direction_sign,
        regime_decision,
        policy,
    )
    margin = impulse - barrier
    opposite_regime = _regime_direction_relation(regime_decision, direction_sign) < 0
    if opposite_regime:
        verdict = BreakoutVerdict.BLOCKED
    elif (
        barrier - impulse >= policy.breakout_score_margin
        or (barrier >= 60 and impulse < policy.breakout_min_impulse_score)
    ):
        verdict = BreakoutVerdict.BLOCKED
    elif (
        phase in {"accepted", "retest", "confirmed"}
        and impulse >= policy.breakout_min_impulse_score
        and margin >= policy.breakout_score_margin
    ):
        verdict = BreakoutVerdict.SUPPORTED
    else:
        verdict = BreakoutVerdict.PENDING

    exposure_quality = str(options.exposure.get("quality") or "unavailable")
    invalidations = [] if exposure_quality == "ok" else ["dex_exposure_degraded"]
    if opposite_regime:
        invalidations.append("regime_direction_opposes_breakout")
    return {
        **base,
        "verdict": verdict.value,
        "actionable": verdict is BreakoutVerdict.SUPPORTED and phase == "confirmed",
        "impulse_score": round(impulse, 1),
        "barrier_score": round(barrier, 1),
        "score_margin": round(margin, 1),
        **barrier_metrics,
        "evidence": [*impulse_evidence, *barrier_evidence],
        "invalidations": invalidations,
        "proxy_disclaimer": "house_structure_proxy_not_dealer_position",
    }


def _trend_score(
    market: MinuteMarketFrame,
    direction: int,
    policy: MarketFeatureSettings,
) -> tuple[float, list[str]]:
    es = market.es
    score = 0.0
    evidence: list[str] = []
    return_15 = _number(es.get("return_15m_points"))
    return_60 = _number(es.get("return_60m_points"))
    if direction and _aligned(return_15, direction) and _aligned(return_60, direction):
        score += 25
        evidence.append("es_15m_60m_aligned")
    efficiency = _number(es.get("trend_efficiency_60m"))
    if (
        direction
        and efficiency is not None
        and efficiency >= policy.trend_efficiency_high
        and _aligned(return_60, direction)
    ):
        score += 20
        evidence.append("trend_efficiency_high")
    if direction and _aligned(es.get("vwap_distance_points"), direction):
        score += 10
        evidence.append("price_on_trend_side_of_vwap")
    if direction and _aligned(es.get("vwap_slope_15m_points"), direction):
        score += 10
        evidence.append("vwap_slope_aligned")
    if _price_volume_direction_relation(market, direction) > 0:
        score += 15
        evidence.append("price_volume_aligned")
    if _cross_asset_direction_relation(market, direction) > 0:
        score += 10
        evidence.append("es_spy_confirmed")
    if direction and _volatility_confirms(market, direction):
        score += 10
        evidence.append("vix_vvix_confirm_trend")
    swing_key = "higher_low_60m" if direction > 0 else "lower_high_60m"
    if direction and es.get(swing_key) is True:
        score += 10
        evidence.append(swing_key)
    return min(score, 100.0), evidence


def _mean_reversion_score(
    market: MinuteMarketFrame,
    options: OptionStructureFrame,
    level_decision: dict[str, Any],
    policy: MarketFeatureSettings,
) -> tuple[float, list[str]]:
    es = market.es
    volume = market.volume
    cross = market.cross_asset
    score = 0.0
    evidence: list[str] = []
    efficiency = _number(es.get("trend_efficiency_60m"))
    if efficiency is not None and efficiency <= policy.trend_efficiency_low:
        score += 25
        evidence.append("trend_efficiency_low")
    slope = _number(es.get("vwap_slope_15m_points"))
    if slope is not None and abs(slope) <= policy.flat_vwap_slope_points:
        score += 15
        evidence.append("vwap_flat")
    return_15 = _number(es.get("return_15m_points"))
    return_60 = _number(es.get("return_60m_points"))
    if return_15 is not None and return_60 is not None and return_15 * return_60 < 0:
        score += 15
        evidence.append("es_horizons_conflict")
    if volume.get("price_volume_alignment_5m") == "price_volume_divergent":
        score += 15
        evidence.append("price_volume_divergent")
    if cross.get("es_spy_direction_confirmation_15m") == "divergent":
        score += 10
        evidence.append("es_spy_divergent")
    if market.volatility.get("vix_vvix_direction_confirmation_15m") == "divergent":
        score += 10
        evidence.append("vix_vvix_divergent")
    phase = str(level_decision.get("phase") or "far")
    thesis = str(level_decision.get("thesis") or "none")
    if thesis == "fade" and phase in {"reject_pending", "rejected", "retest", "confirmed"}:
        score += 15
        evidence.append("level_rejection_path")
    structure = options.structure
    spot = _number(structure.get("underlier"))
    put_wall = _number(structure.get("put_wall"))
    call_wall = _number(structure.get("call_wall"))
    if spot is not None and put_wall is not None and call_wall is not None:
        if min(put_wall, call_wall) <= spot <= max(put_wall, call_wall):
            score += 5
            evidence.append("price_between_primary_walls")
    return score, evidence


def _breakout_impulse(
    market: MinuteMarketFrame,
    options: OptionStructureFrame,
    level_decision: dict[str, Any],
    direction: int,
    regime_decision: dict[str, Any],
    policy: MarketFeatureSettings,
) -> tuple[float, list[str]]:
    es = market.es
    score = 0.0
    evidence: list[str] = []
    returns = (
        _number(es.get("return_15m_points")),
        _number(es.get("return_60m_points")),
    )
    aligned_count = sum(_aligned(value, direction) for value in returns)
    score += 20.0 if aligned_count == 2 else 10.0 if aligned_count == 1 else 0.0
    if aligned_count:
        evidence.append(f"es_horizons_aligned_{aligned_count}")
    efficiency = _number(es.get("trend_efficiency_60m"))
    if (
        efficiency is not None
        and efficiency >= policy.trend_efficiency_high
        and _aligned(es.get("return_60m_points"), direction)
    ):
        score += 15
        evidence.append("trend_efficiency_supports_breakout")
    if _aligned(es.get("vwap_distance_points"), direction):
        score += 10
        evidence.append("price_vwap_supports_breakout")
    if _aligned(es.get("vwap_slope_15m_points"), direction):
        score += 10
        evidence.append("vwap_slope_supports_breakout")
    if _price_volume_direction_relation(market, direction) > 0:
        score += 15
        evidence.append("price_volume_supports_breakout")
    if _cross_asset_direction_relation(market, direction) > 0:
        score += 10
        evidence.append("es_spy_supports_breakout")
    if _regime_direction_relation(regime_decision, direction) > 0:
        score += 10
        evidence.append("trending_regime_supports_breakout")
    if _volatility_confirms(market, direction):
        score += 10
        evidence.append("vix_vvix_support_breakout")
        ratio = _number(market.volatility.get("vix1d_vix_ratio"))
        if ratio is not None and ratio >= 1.0:
            score += 5
            evidence.append("short_volatility_stressed")
    phase = str(level_decision.get("phase") or "far")
    score += _PHASE_IMPULSE_POINTS.get(phase, 0.0)
    oi_dex, volume_dex = _dex_ratios(options)
    dex_support = sum(
        ratio is not None and ratio * direction > 0.10 for ratio in (oi_dex, volume_dex)
    )
    score += 10.0 if dex_support == 2 else 5.0 if dex_support == 1 else 0.0
    if dex_support:
        evidence.append(f"dex_proxy_supports_breakout_{dex_support}")
    return min(score, 100.0), evidence


def _breakout_barrier(
    market: MinuteMarketFrame,
    options: OptionStructureFrame,
    level_decision: dict[str, Any],
    direction: int,
    regime_decision: dict[str, Any],
    policy: MarketFeatureSettings,
) -> tuple[float, dict[str, Any], list[str]]:
    structure = options.structure
    concentration = options.concentration
    level = _number(level_decision.get("level"))
    walls = structure.get("call_walls" if direction > 0 else "put_walls")
    walls = [row for row in walls or [] if isinstance(row, dict)]
    abs_gex = _number(structure.get("abs_gex"))
    local_gex = 0.0
    next_distances: list[float] = []
    if level is not None:
        for wall in walls:
            strike = _number(wall.get("strike"))
            gex = _number(wall.get("gex"))
            if strike is None or gex is None:
                continue
            outward = direction * (strike - level)
            if 0 <= outward <= policy.breakout_local_gex_band_points:
                local_gex += abs(gex)
            if outward > 0:
                next_distances.append(outward)
    local_share = local_gex / abs_gex if abs_gex and abs_gex > 0 else None
    next_distance = min(next_distances) if next_distances else None
    top_share = _number(concentration.get("gamma_top_share"))
    score = 0.0
    evidence: list[str] = []
    if local_share is not None:
        score += min(local_share / 0.35, 1.0) * 35.0
        if local_share >= 0.15:
            evidence.append("dense_local_gex_ahead")
    if next_distance is not None and next_distance <= policy.breakout_near_wall_points:
        score += (1.0 - next_distance / policy.breakout_near_wall_points) * 20.0
        evidence.append("next_wall_nearby")
    if top_share is not None:
        score += min(top_share, 1.0) * 15.0
        if top_share >= 0.60:
            evidence.append("gex_concentration_high")
    oi_dex, volume_dex = _dex_ratios(options)
    dex_divergent = (
        oi_dex is not None
        and volume_dex is not None
        and abs(oi_dex) >= 0.05
        and abs(volume_dex) >= 0.05
        and oi_dex * volume_dex < 0
    )
    if dex_divergent:
        score += 15
        evidence.append("oi_volume_dex_divergent")
    elif volume_dex is not None and volume_dex * direction < -0.10:
        score += 12
        evidence.append("volume_dex_opposes_breakout")
    gex_divergence = _number(options.exposure.get("gex_weighting_divergence"))
    if gex_divergence is not None and abs(gex_divergence) >= 0.25:
        score += 10
        evidence.append("oi_volume_gex_divergent")
    if regime_decision.get("mode") == RegimeMode.MEAN_REVERTING.value:
        score += 10
        evidence.append("mean_reversion_regime")
    regime_relation = _regime_direction_relation(regime_decision, direction)
    if regime_relation < 0:
        score += 25
        evidence.append("trending_regime_opposes_breakout")
    efficiency = _number(market.es.get("trend_efficiency_60m"))
    if (
        efficiency is not None
        and efficiency >= policy.trend_efficiency_high
        and _aligned(market.es.get("return_60m_points"), -direction)
    ):
        score += 15
        evidence.append("trend_efficiency_opposes_breakout")
    if _price_volume_direction_relation(market, direction) < 0:
        score += 15
        evidence.append("price_volume_opposes_breakout")
    if _cross_asset_direction_relation(market, direction) < 0:
        score += 10
        evidence.append("es_spy_opposes_breakout")
    if _volatility_opposes(market, direction):
        score += 10
        evidence.append("vix_vvix_oppose_breakout")
    metrics = {
        "local_abs_gex_share": local_share,
        "next_wall_distance_points": next_distance,
        "gamma_top_share": top_share,
        "oi_net_dex_ratio_proxy": oi_dex,
        "volume_net_dex_ratio_proxy": volume_dex,
        "oi_volume_dex_divergent": dex_divergent,
        "vix1d_vix_ratio": _number(market.volatility.get("vix1d_vix_ratio")),
        "vix_return_15m_pct": _number(market.volatility.get("vix_return_15m_pct")),
        "vix_vvix_confirmation": market.volatility.get(
            "vix_vvix_direction_confirmation_15m"
        ),
    }
    return min(score, 100.0), metrics, evidence


def _dex_ratios(options: OptionStructureFrame) -> tuple[float | None, float | None]:
    oi = options.exposure.get("oi_weighted")
    volume = options.exposure.get("volume_weighted")
    return (
        _number(oi.get("net_dex_ratio_proxy")) if isinstance(oi, dict) else None,
        _number(volume.get("net_dex_ratio_proxy")) if isinstance(volume, dict) else None,
    )


def _market_direction(market: MinuteMarketFrame, trend: dict[str, Any]) -> str:
    short = _number(market.es.get("return_15m_points"))
    medium = _number(market.es.get("return_60m_points"))
    if short is not None and medium is not None and short * medium > 0:
        return "up" if short > 0 else "down"
    regime = str(trend.get("regime") or "neutral")
    return "up" if regime == "bullish" else "down" if regime == "bearish" else "none"


def _volatility_regime(market: MinuteMarketFrame) -> str:
    ratio = _number(market.volatility.get("vix1d_vix_ratio"))
    if ratio is None:
        return "unknown"
    return "stressed" if ratio >= 1.0 else "normal" if ratio >= 0.8 else "quiet"


def _volatility_confirms(market: MinuteMarketFrame, direction: int) -> bool:
    vix_return = _number(market.volatility.get("vix_return_15m_pct"))
    confirmation = market.volatility.get("vix_vvix_direction_confirmation_15m")
    return bool(
        vix_return is not None
        and confirmation == "confirmed"
        and vix_return * direction < 0
    )


def _volatility_opposes(market: MinuteMarketFrame, direction: int) -> bool:
    vix_return = _number(market.volatility.get("vix_return_15m_pct"))
    confirmation = market.volatility.get("vix_vvix_direction_confirmation_15m")
    return bool(
        vix_return is not None
        and confirmation == "confirmed"
        and vix_return * direction > 0
    )


def _breakout_direction(level_decision: dict[str, Any]) -> int:
    direction = str(level_decision.get("direction") or "")
    if direction in {"up", "down"}:
        return 1 if direction == "up" else -1
    outside = _number(level_decision.get("outside_direction"))
    if outside:
        return 1 if outside > 0 else -1
    kind = str(level_decision.get("level_kind") or "")
    return -1 if kind in {"put_wall", "flip_low"} else 1 if kind in {"flip_high", "call_wall"} else 0


def _price_volume_direction_relation(market: MinuteMarketFrame, direction: int) -> int:
    if market.volume.get("price_volume_alignment_5m") != "price_volume_aligned":
        return 0
    return _direction_relation(market.es.get("return_5m_points"), direction)


def _cross_asset_direction_relation(market: MinuteMarketFrame, direction: int) -> int:
    if market.cross_asset.get("es_spy_direction_confirmation_15m") != "confirmed":
        return 0
    return _direction_relation(market.es.get("return_15m_points"), direction)


def _regime_direction_relation(regime: dict[str, Any], direction: int) -> int:
    if regime.get("mode") != RegimeMode.TRENDING.value:
        return 0
    regime_direction = str(regime.get("direction") or "none")
    regime_sign = (
        1 if regime_direction == "up" else -1 if regime_direction == "down" else 0
    )
    return regime_sign * direction


def _direction_relation(value: object, direction: int) -> int:
    parsed = _number(value)
    if parsed is None or math.isclose(parsed, 0.0) or direction == 0:
        return 0
    return 1 if parsed * direction > 0 else -1


def _aligned(value: object, direction: int) -> bool:
    parsed = _number(value)
    return parsed is not None and parsed * direction > 0


def _number(value: object) -> float | None:
    if not isinstance(value, int | float):
        return None
    parsed = float(value)
    return parsed if math.isfinite(parsed) else None
