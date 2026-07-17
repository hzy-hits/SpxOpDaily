"""Option-structure and hot-contract L1 feature calculations."""

from __future__ import annotations

import math
import statistics
from collections import Counter, defaultdict
from dataclasses import asdict
from datetime import datetime, timedelta
from typing import Any

from spx_spark.analytics.options.models import ExpiryOptionsMap, OptionsMap
from spx_spark.application.market_features.exposure_strikes import key_strike_features
from spx_spark.application.market_features.market import quote_source_at
from spx_spark.application.market_features.models import (
    FrameQuality,
    L1MicrostructureFrame,
    OptionStructureFrame,
)
from spx_spark.features.exposure_map import ExposureMap, ExpiryExposure
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import (
    InstrumentType,
    MarketDataQuality,
    OptionRight,
    Provider,
    Quote,
    as_utc,
)
from spx_spark.settings.market_features import MarketFeatureSettings
from spx_spark.storage import LatestState, select_best_quotes


def build_option_structure_frame(
    state: LatestState,
    options_map: OptionsMap,
    *,
    now: datetime,
    history: list[dict[str, Any]],
    previous_contracts: dict[str, Any],
    policy: MarketFeatureSettings,
    exposure_map: ExposureMap | None = None,
    last_usable_frame: dict[str, Any] | None = None,
) -> tuple[OptionStructureFrame, dict[str, Any]]:
    now = as_utc(now)
    front = options_map.expiries[0] if options_map.expiries else None
    next_expiry = options_map.expiries[1] if len(options_map.expiries) > 1 else None
    l1, current_contracts = build_l1_microstructure(
        state,
        front=front,
        now=now,
        previous_contracts=previous_contracts,
        history=history,
        policy=policy,
    )
    quality = FrameQuality.READY if front and front.option_count > 0 else FrameQuality.UNAVAILABLE
    if front and (front.coverage.live <= 0 or front.coverage.with_mid <= 0):
        quality = FrameQuality.DEGRADED
    structure = structure_features(front, history=history, underlier=options_map.underlier.price)
    frozen_expiry: str | None = None
    if front is None:
        frozen_expiry, structure = frozen_structure_for_session(
            last_usable_frame,
            now=now,
        )
    volatility = option_volatility_features(front, next_expiry, history=history, now=now)
    concentration = concentration_features(state, front)
    density = density_features(front, history=history, now=now)
    exposure = exposure_features(
        _expiry_exposure(exposure_map, front.expiry if front else None),
        underlier=options_map.underlier.price,
    )
    effective_expiry = front.expiry if front else frozen_expiry
    frame_id = f"options:{effective_expiry or 'none'}:{now.strftime('%Y%m%dT%H%M')}"
    frame = OptionStructureFrame(
        schema_version=1,
        frame_id=frame_id,
        as_of=now,
        quality=quality,
        front_expiry=effective_expiry,
        next_expiry=next_expiry.expiry if next_expiry else None,
        structure=structure,
        volatility=volatility,
        concentration=concentration,
        density=density,
        l1=l1,
        diagnostics={
            "warnings": list(options_map.warnings),
            "underlier": options_map.underlier.price,
            "underlier_source": options_map.underlier.source,
        },
        exposure=exposure,
    )
    return frame, current_contracts


def frozen_structure_for_session(
    frame: dict[str, Any] | None,
    *,
    now: datetime,
) -> tuple[str | None, dict[str, Any]]:
    """Retain same-session OI/GEX locations without treating old quotes as live."""

    expected_expiry = DEFAULT_MARKET_CALENDAR.research_expiry(now).strftime("%Y%m%d")
    if not isinstance(frame, dict) or str(frame.get("front_expiry") or "") != expected_expiry:
        return None, _empty_structure()
    prior = frame.get("structure")
    if not isinstance(prior, dict) or not _structure_has_levels(prior):
        return None, _empty_structure()
    frozen = {
        key: prior.get(key)
        for key in (
            "put_wall",
            "call_wall",
            "zero_gamma",
            "flip_zone",
            "gamma_state",
            "gex_quality",
            "net_gex",
            "abs_gex",
            "net_gamma_ratio",
            "max_pain",
            "call_walls",
            "put_walls",
        )
    }
    frozen.update(
        {
            "underlier": None,
            "distance_to_put_wall": None,
            "distance_to_call_wall": None,
            "distance_to_zero_gamma": None,
            "put_wall_migration_points": None,
            "call_wall_migration_points": None,
            "zero_gamma_migration_points": None,
            "frozen": True,
            "source": "frozen_last_usable_option_frame",
            "frozen_as_of": frame.get("as_of"),
        }
    )
    return expected_expiry, frozen


def option_frame_has_usable_live_structure(frame: OptionStructureFrame) -> bool:
    return bool(
        frame.front_expiry
        and frame.structure.get("frozen") is not True
        and _structure_has_levels(frame.structure)
        and frame.l1.contract_count > 0
        and frame.l1.diagnostics.get("fresh_candidate_count", 0) > 0
    )


def _structure_has_levels(structure: dict[str, Any]) -> bool:
    return any(
        _number(structure.get(key)) is not None for key in ("put_wall", "call_wall", "zero_gamma")
    )


def _expiry_exposure(
    exposure_map: ExposureMap | None,
    expiry: str | None,
) -> ExpiryExposure | None:
    if exposure_map is None or expiry is None:
        return None
    return next((item for item in exposure_map.expiries if item.expiry == expiry), None)


def exposure_features(
    exposure: ExpiryExposure | None,
    *,
    underlier: float | None = None,
) -> dict[str, Any]:
    if exposure is None:
        return {
            "quality": "unavailable",
            "oi_quality": "missing",
            "oi_weighted": {},
            "volume_weighted": {},
            "key_strikes": [],
            "warnings": ["exposure_map_unavailable"],
        }
    return {
        "quality": exposure.quality,
        "oi_quality": exposure.oi_quality,
        "snapshot_age_seconds": exposure.snapshot_age_seconds,
        "delta_coverage_ratio": exposure.delta_coverage_ratio,
        "iv_coverage_ratio": exposure.iv_coverage_ratio,
        "oi_weighted": asdict(exposure.oi_weighted),
        "volume_weighted": asdict(exposure.volume_weighted),
        "key_strikes": key_strike_features(exposure, underlier=underlier),
        "gex_weighting_divergence": exposure.gex_weighting_divergence,
        "sign_convention": exposure.sign_convention,
        "dealer_position_sign": exposure.dealer_position_sign,
        "warnings": list(exposure.warnings),
    }


def structure_features(
    front: ExpiryOptionsMap | None,
    *,
    history: list[dict[str, Any]],
    underlier: float | None,
) -> dict[str, Any]:
    if front is None:
        return _empty_structure()
    prior_frame = history[-1] if history and isinstance(history[-1], dict) else {}
    prior = prior_frame.get("structure") if prior_frame.get("front_expiry") == front.expiry else {}
    prior = prior if isinstance(prior, dict) else {}
    max_pain = front.max_pain.to_dict() if front.max_pain else None
    return {
        "underlier": underlier,
        "put_wall": front.put_wall,
        "call_wall": front.call_wall,
        "zero_gamma": front.zero_gamma,
        "flip_zone": list(front.gamma_flip_zone) if front.gamma_flip_zone else None,
        "distance_to_put_wall": _difference(underlier, front.put_wall),
        "distance_to_call_wall": _difference(underlier, front.call_wall),
        "distance_to_zero_gamma": _difference(underlier, front.zero_gamma),
        "put_wall_migration_points": _difference(front.put_wall, _number(prior.get("put_wall"))),
        "call_wall_migration_points": _difference(front.call_wall, _number(prior.get("call_wall"))),
        "zero_gamma_migration_points": _difference(
            front.zero_gamma, _number(prior.get("zero_gamma"))
        ),
        "gamma_state": front.gamma_state,
        "gex_quality": front.gex_quality,
        "net_gex": front.net_gex,
        "abs_gex": front.abs_gex,
        "net_gamma_ratio": front.net_gamma_ratio,
        "max_pain": max_pain,
        "call_walls": [wall.to_dict() for wall in front.call_walls],
        "put_walls": [wall.to_dict() for wall in front.put_walls],
    }


def option_volatility_features(
    front: ExpiryOptionsMap | None,
    next_expiry: ExpiryOptionsMap | None,
    *,
    history: list[dict[str, Any]],
    now: datetime,
) -> dict[str, Any]:
    if front is None:
        return {
            "atm_iv_0dte": None,
            "atm_iv_1dte": None,
            "atm_iv_change_5m": None,
            "atm_iv_change_15m": None,
            "atm_iv_change_60m": None,
            "term_gap": None,
        }
    current_iv = front.atm_iv
    result = {
        "atm_iv_0dte": current_iv,
        "atm_iv_1dte": next_expiry.atm_iv if next_expiry else None,
        "put_skew_25d_0dte": front.put_skew_25d,
        "call_skew_25d_0dte": front.call_skew_25d,
        "put_skew_25d_1dte": next_expiry.put_skew_25d if next_expiry else None,
        "expected_move_points_0dte": front.expected_move_points,
        "term_gap": _difference(current_iv, next_expiry.atm_iv if next_expiry else None),
    }
    for minutes in (5, 15, 60):
        prior = _history_value(
            history,
            now=now,
            minutes=minutes,
            section="volatility",
            key="atm_iv_0dte",
        )
        result[f"atm_iv_change_{minutes}m"] = _difference(current_iv, prior)
    return result


def concentration_features(state: LatestState, front: ExpiryOptionsMap | None) -> dict[str, Any]:
    if front is None:
        return {
            "strike_volume_top5_share": None,
            "strike_volume_hhi": None,
            "gamma_top_share": None,
        }
    contract_volume: dict[str, tuple[float, float]] = {}
    for quote in state.best_quotes:
        if (
            quote.instrument.instrument_type is InstrumentType.OPTION
            and quote.instrument.expiry == front.expiry
            and quote.instrument.strike is not None
            and quote.volume is not None
            and quote.volume >= 0
        ):
            key = quote.instrument.canonical_id
            current = contract_volume.get(key)
            value = float(quote.volume)
            if current is None or value > current[1]:
                contract_volume[key] = (float(quote.instrument.strike), value)
    strike_volume: dict[float, float] = defaultdict(float)
    for strike, volume in contract_volume.values():
        strike_volume[strike] += volume
    total_volume = sum(strike_volume.values())
    shares = [value / total_volume for value in strike_volume.values()] if total_volume else []
    top5 = sum(sorted(shares, reverse=True)[:5]) if shares else None
    hhi = sum(share * share for share in shares) if shares else None
    top_gex = sum(abs(item.abs_gex) for item in front.top_gex_strikes)
    gamma_share = top_gex / front.abs_gex if front.abs_gex and front.abs_gex > 0 else None
    return {
        "strike_volume_top5_share": top5,
        "strike_volume_hhi": hhi,
        "gamma_top_share": min(gamma_share, 1.0) if gamma_share is not None else None,
        "strike_count_with_volume": len(strike_volume),
        "total_contract_volume": total_volume,
    }


def density_features(
    front: ExpiryOptionsMap | None,
    *,
    history: list[dict[str, Any]],
    now: datetime,
) -> dict[str, Any]:
    if front is None or front.rn_density is None:
        return {
            "quality": "unavailable",
            "median": None,
            "p10": None,
            "p90": None,
            "prob_below_put_wall": None,
            "prob_above_call_wall": None,
        }
    result = front.rn_density.to_dict()
    for key in (
        "median",
        "p10",
        "p90",
        "prob_below_put_wall",
        "prob_above_call_wall",
    ):
        current = _number(result.get(key))
        for minutes in (5, 15, 60):
            prior = _history_value(
                history,
                now=now,
                minutes=minutes,
                section="density",
                key=key,
            )
            result[f"{key}_change_{minutes}m"] = _difference(current, prior)
    return result


def build_l1_microstructure(
    state: LatestState,
    *,
    front: ExpiryOptionsMap | None,
    now: datetime,
    previous_contracts: dict[str, Any],
    history: list[dict[str, Any]],
    policy: MarketFeatureSettings,
) -> tuple[L1MicrostructureFrame, dict[str, Any]]:
    if front is None:
        return _empty_l1(), {}
    candidates = _fresh_front_quotes(state, expiry=front.expiry, now=now, policy=policy)
    selected = _select_hot_quotes(
        candidates,
        underlier=front.atm_strike,
        limit=policy.hot_option_limit,
    )
    current_contracts = {
        quote.instrument.canonical_id: _contract_sample(quote) for quote in selected
    }
    spreads = [quote.spread_bps for quote in selected if quote.spread_bps is not None]
    imbalances = [
        imbalance(quote.bid_size, quote.ask_size)
        for quote in selected
        if imbalance(quote.bid_size, quote.ask_size) is not None
    ]
    two_sided = [quote for quote in selected if quote.mid is not None]
    fresh_ratio = len(selected) / max(min(policy.hot_option_limit, len(candidates)), 1)
    changed = 0
    common = 0
    mid_velocities: list[float] = []
    call_rises: list[bool] = []
    put_rises: list[bool] = []
    for quote in selected:
        key = quote.instrument.canonical_id
        previous = previous_contracts.get(key)
        if not isinstance(previous, dict):
            continue
        common += 1
        current_source = quote_source_at(quote).isoformat()
        if current_source != previous.get("source_at"):
            changed += 1
        prior_mid = _number(previous.get("mid"))
        prior_at = _parse_at(previous.get("observed_at"))
        if quote.mid is None or prior_mid is None or prior_at is None:
            continue
        minutes = max((now - prior_at).total_seconds() / 60.0, 1 / 60)
        velocity = (quote.mid - prior_mid) / minutes
        mid_velocities.append(velocity)
        if quote.instrument.right is OptionRight.CALL:
            call_rises.append(velocity > 0)
        elif quote.instrument.right is OptionRight.PUT:
            put_rises.append(velocity > 0)
    divergences = provider_mid_divergences(candidates, policy=policy)
    spread_p50 = _percentile(spreads, 0.50)
    spread_p90 = _percentile(spreads, 0.90)
    historical_spreads = [
        _number(item.get("l1", {}).get("metrics", {}).get("spread_p50_bps"))
        for item in history
        if isinstance(item, dict) and isinstance(item.get("l1"), dict)
    ]
    historical_spreads = [value for value in historical_spreads if value is not None]
    spread_percentile = (
        sum(value <= spread_p50 for value in historical_spreads) / len(historical_spreads)
        if spread_p50 is not None and historical_spreads
        else None
    )
    two_sided_ratio = len(two_sided) / len(selected) if selected else 0.0
    coverage_score = 100.0 * (0.65 * two_sided_ratio + 0.35 * fresh_ratio)
    p50_component = max(
        0.0,
        1.0 - (spread_p50 or 10_000.0) / policy.l1_spread_p50_limit_bps,
    )
    p90_component = max(
        0.0,
        1.0 - (spread_p90 or 10_000.0) / policy.l1_spread_p90_limit_bps,
    )
    execution_score = 100.0 * (0.60 * p50_component + 0.40 * p90_component)
    liquidity_score = round(0.45 * coverage_score + 0.55 * execution_score, 1) if selected else None
    quality = (
        FrameQuality.READY
        if selected
        and two_sided_ratio >= 0.75
        and liquidity_score is not None
        and liquidity_score >= policy.min_l1_liquidity_score
        else FrameQuality.DEGRADED
        if selected
        else FrameQuality.UNAVAILABLE
    )
    provider_counts = Counter(quote.provider.value for quote in selected)
    frame = L1MicrostructureFrame(
        quality=quality,
        expiry=front.expiry,
        contract_count=len(selected),
        metrics={
            "spread_p50_bps": spread_p50,
            "spread_p90_bps": spread_p90,
            "spread_history_percentile": spread_percentile,
            "size_imbalance_median": statistics.median(imbalances) if imbalances else None,
            "mid_velocity_median_per_minute": (
                statistics.median(mid_velocities) if mid_velocities else None
            ),
            "quote_update_ratio": changed / common if common else None,
            "call_mid_rising_ratio": sum(call_rises) / len(call_rises) if call_rises else None,
            "put_mid_rising_ratio": sum(put_rises) / len(put_rises) if put_rises else None,
            "cross_provider_mid_difference_p50": _percentile(divergences, 0.50),
            "cross_provider_mid_difference_max": max(divergences) if divergences else None,
            "two_sided_ratio": two_sided_ratio,
            "coverage_score": round(coverage_score, 1) if selected else None,
            "execution_score": round(execution_score, 1) if selected else None,
            "liquidity_score": liquidity_score,
        },
        diagnostics={
            "fresh_candidate_count": len(candidates),
            "common_contract_count": common,
            "provider_pair_count": len(divergences),
            "selected_provider_counts": dict(sorted(provider_counts.items())),
            "reason": None if selected else "no_fresh_option_quotes",
        },
    )
    return frame, current_contracts


def provider_mid_divergences(quotes: list[Quote], *, policy: MarketFeatureSettings) -> list[float]:
    grouped: dict[str, dict[Provider, Quote]] = defaultdict(dict)
    for quote in quotes:
        if quote.provider in {Provider.SCHWAB, Provider.IBKR}:
            current = grouped[quote.instrument.canonical_id].get(quote.provider)
            if current is None or quote_source_at(quote) > quote_source_at(current):
                grouped[quote.instrument.canonical_id][quote.provider] = quote
    differences: list[float] = []
    for providers in grouped.values():
        schwab, ibkr = providers.get(Provider.SCHWAB), providers.get(Provider.IBKR)
        if not schwab or not ibkr or schwab.mid is None or ibkr.mid is None:
            continue
        if abs((quote_source_at(schwab) - quote_source_at(ibkr)).total_seconds()) > (
            policy.provider_sync_tolerance_seconds
        ):
            continue
        differences.append(abs(schwab.mid - ibkr.mid))
    return differences


def merge_option_history(
    history: list[dict[str, Any]],
    frame: OptionStructureFrame,
    *,
    policy: MarketFeatureSettings,
) -> list[dict[str, Any]]:
    payload = frame.to_dict()
    retained = [item for item in history if isinstance(item, dict)]
    if retained and str(retained[-1].get("as_of", ""))[:16] == frame.as_of.isoformat()[:16]:
        retained[-1] = payload
    else:
        retained.append(payload)
    cutoff = frame.as_of - timedelta(minutes=policy.option_history_minutes)
    return [item for item in retained if (_parse_at(item.get("as_of")) or cutoff) >= cutoff]


def _fresh_front_quotes(
    state: LatestState,
    *,
    expiry: str,
    now: datetime,
    policy: MarketFeatureSettings,
) -> list[Quote]:
    candidates = [
        quote
        for quote in state.quotes
        if quote.instrument.instrument_type is InstrumentType.OPTION
        and quote.instrument.expiry == expiry
    ]
    return [
        quote
        for quote in select_best_quotes(candidates, as_of=now)
        if quote.quality is MarketDataQuality.LIVE
        and quote.mid is not None
        and (now - quote_source_at(quote)).total_seconds() <= policy.max_quote_age_seconds
    ]


def _select_hot_quotes(quotes: list[Quote], *, underlier: float | None, limit: int) -> list[Quote]:
    by_contract: dict[str, Quote] = {}
    for quote in quotes:
        key = quote.instrument.canonical_id
        current = by_contract.get(key)
        if current is None or quote_source_at(quote) > quote_source_at(current):
            by_contract[key] = quote
    center = underlier or 0.0
    return sorted(
        by_contract.values(),
        key=lambda quote: (
            abs((quote.instrument.strike or center) - center),
            quote.instrument.right.value if quote.instrument.right else "",
        ),
    )[:limit]


def _contract_sample(quote: Quote) -> dict[str, Any]:
    return {
        "observed_at": quote.received_at.isoformat(),
        "source_at": quote_source_at(quote).isoformat(),
        "provider": quote.provider.value,
        "mid": quote.mid,
        "bid": quote.bid,
        "ask": quote.ask,
        "implied_vol": quote.greeks.implied_vol if quote.greeks else None,
    }


def imbalance(bid_size: float | None, ask_size: float | None) -> float | None:
    if bid_size is None or ask_size is None or bid_size < 0 or ask_size < 0:
        return None
    total = bid_size + ask_size
    return (bid_size - ask_size) / total if total > 0 else None


def _history_value(
    history: list[dict[str, Any]],
    *,
    now: datetime,
    minutes: int,
    section: str,
    key: str,
) -> float | None:
    target = as_utc(now) - timedelta(minutes=minutes)
    candidates = [
        item for item in history if (_parse_at(item.get("as_of")) or as_utc(now)) <= target
    ]
    if not candidates:
        return None
    selected = max(candidates, key=lambda item: _parse_at(item.get("as_of")) or target)
    values = selected.get(section)
    return _number(values.get(key)) if isinstance(values, dict) else None


def _empty_structure() -> dict[str, Any]:
    return {
        "underlier": None,
        "put_wall": None,
        "call_wall": None,
        "zero_gamma": None,
        "flip_zone": None,
        "distance_to_put_wall": None,
        "distance_to_call_wall": None,
        "distance_to_zero_gamma": None,
        "max_pain": None,
        "net_gex": None,
        "abs_gex": None,
        "net_gamma_ratio": None,
    }


def _empty_l1() -> L1MicrostructureFrame:
    return L1MicrostructureFrame(
        quality=FrameQuality.UNAVAILABLE,
        expiry=None,
        contract_count=0,
        metrics={},
        diagnostics={"reason": "missing_front_expiry"},
    )


def _difference(first: float | None, second: float | None) -> float | None:
    return first - second if first is not None and second is not None else None


def _percentile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = (len(ordered) - 1) * quantile
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (index - lower)


def _parse_at(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return as_utc(datetime.fromisoformat(value))
    except ValueError:
        return None


def _number(value: object) -> float | None:
    return float(value) if isinstance(value, int | float) and math.isfinite(value) else None
