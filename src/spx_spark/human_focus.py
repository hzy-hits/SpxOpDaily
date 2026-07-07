from __future__ import annotations

from typing import Any

from spx_spark.config import env_csv
from spx_spark.iv_surface import IvSurfaceSnapshot
from spx_spark.marketdata import MarketDataQuality, Quote
from spx_spark.options_map import ExpiryOptionsMap, OptionsMap
from spx_spark.storage import LatestState
from spx_spark.strategy.micopedia import MicopediaInputs, build_micopedia_signal


BAD_QUALITIES = {
    MarketDataQuality.MISSING,
    MarketDataQuality.ERROR,
    MarketDataQuality.STALE,
    MarketDataQuality.UNKNOWN,
}


def quote_summary(state: LatestState, instrument_id: str) -> dict[str, object]:
    quote = state.best_quote(instrument_id)
    if quote is None:
        return {
            "instrument_id": instrument_id,
            "quality": MarketDataQuality.MISSING.value,
            "price": None,
            "move_bps": None,
            "age_ms": None,
        }
    price = quote.effective_price
    return {
        "instrument_id": instrument_id,
        "quality": quote.quality.value,
        "price": price,
        "move_bps": move_bps(quote),
        "age_ms": quote.quote_age_ms(state.as_of),
    }


def move_bps(quote: Quote) -> float | None:
    price = quote.effective_price
    close = quote.close
    if price is None or close is None or close <= 0:
        return None
    return (price / close - 1.0) * 10_000.0


def expiry_options_summary(expiry: ExpiryOptionsMap) -> dict[str, object]:
    return {
        "expiry": expiry.expiry,
        "option_count": expiry.option_count,
        "atm_strike": expiry.atm_strike,
        "atm_straddle_mid": expiry.atm_straddle_mid,
        "expected_move_points": expiry.expected_move_points,
        "expected_move_pct": expiry.expected_move_pct,
        "atm_iv": expiry.atm_iv,
        "put_skew_ratio": expiry.put_skew_ratio,
        "call_skew_ratio": expiry.call_skew_ratio,
        "put_skew_25d": expiry.put_skew_25d,
        "call_skew_25d": expiry.call_skew_25d,
        "skew_method": expiry.skew_method,
        "gamma_state": expiry.gamma_state,
        "zero_gamma": expiry.zero_gamma,
        "zero_gamma_distance_points": expiry.zero_gamma_distance_points,
        "put_wall": expiry.put_wall,
        "call_wall": expiry.call_wall,
        "wall_ladder": {
            "method": expiry.wall_method,
            "call_walls": [
                {"strike": wall.strike, "oi": wall.open_interest, "gex": wall.gex}
                for wall in expiry.call_walls
            ],
            "put_walls": [
                {"strike": wall.strike, "oi": wall.open_interest, "gex": wall.gex}
                for wall in expiry.put_walls
            ],
        },
        "nearest_wall": expiry.nearest_wall,
        "nearest_wall_distance_points": expiry.nearest_wall_distance_points,
        "net_gamma_ratio": expiry.net_gamma_ratio,
        "gex_quality": expiry.gex_quality,
        "coverage": {
            "total": expiry.coverage.total,
            "live": expiry.coverage.live,
            "stale": expiry.coverage.stale,
            "delayed": expiry.coverage.delayed,
            "unknown_age": expiry.coverage.unknown_age,
            "max_age_ms": expiry.coverage.max_age_ms,
            "with_iv": expiry.coverage.with_iv,
            "with_gamma": expiry.coverage.with_gamma,
            "with_open_interest": expiry.coverage.with_open_interest,
            "avg_spread_bps": expiry.coverage.avg_spread_bps,
        },
        "warnings": expiry.warnings,
        "level_probabilities": [lp.to_dict() for lp in expiry.level_probabilities],
        "gamma_profile": {
            "zero_gamma": expiry.zero_gamma,
            "flip_zone": list(expiry.gamma_flip_zone) if expiry.gamma_flip_zone else None,
            "net_gamma_ratio": expiry.net_gamma_ratio,
            "gex_weighting": expiry.gex_weighting,
            "zero_gamma_method": expiry.zero_gamma_method,
            "top_strikes": [
                {
                    "strike": row.strike,
                    "net_gex": row.net_gex,
                    "call_oi": row.call_open_interest,
                    "put_oi": row.put_open_interest,
                }
                for row in expiry.top_gex_strikes[:6]
            ],
        },
    }


def surface_expiry_summary(expiry: dict[str, Any], history_by_expiry: dict[str, dict[str, Any]]) -> dict[str, object]:
    expiry_id = str(expiry.get("expiry") or "")
    return {
        "expiry": expiry_id,
        "atm_iv": expiry.get("atm_iv"),
        "atm_straddle_mid": expiry.get("atm_straddle_mid"),
        "expected_move_points": expiry.get("expected_move_points"),
        "expected_move_pct": expiry.get("expected_move_pct"),
        "put_skew_ratio": expiry.get("put_skew_ratio"),
        "call_skew_ratio": expiry.get("call_skew_ratio"),
        "surface_fit_quality": expiry.get("surface_fit_quality"),
        "gamma_state": expiry.get("gamma_state"),
        "zero_gamma": expiry.get("zero_gamma"),
        "put_wall": expiry.get("put_wall"),
        "call_wall": expiry.get("call_wall"),
        "option_count": expiry.get("option_count"),
        "iv_coverage_ratio": expiry.get("iv_coverage_ratio"),
        "gamma_coverage_ratio": expiry.get("gamma_coverage_ratio"),
        "avg_spread_bps": expiry.get("avg_spread_bps"),
        "history_1h": history_by_expiry.get(expiry_id),
    }


def gamma_state_for_micopedia(options_map: OptionsMap) -> str:
    if not options_map.expiries:
        return "unknown"
    raw = options_map.expiries[0].gamma_state
    if raw == "positive_gamma_pin":
        return "pin"
    if raw == "zero_gamma_transition":
        return "transition"
    if raw == "negative_gamma_acceleration":
        return "negative"
    return "unknown"


def time_phase_from_window(window: dict[str, object]) -> str:
    name = str(window.get("name") or "").lower()
    if "premarket" in name:
        return "premarket"
    if "open" in name:
        return "open"
    if "close" in name:
        return "late"
    if "weekend" in name or "closed" in name:
        return "closed"
    return "unknown"


def micopedia_context(
    state: LatestState,
    *,
    options_map: OptionsMap,
    window: dict[str, object],
) -> dict[str, object]:
    front = options_map.expiries[0] if options_map.expiries else None
    key_levels = [
        value
        for value in (
            front.put_wall if front else None,
            front.call_wall if front else None,
            front.zero_gamma if front else None,
            front.nearest_wall if front else None,
        )
        if value is not None
    ]
    es_quote = state.best_quote("future:ES")
    has_es_data = es_quote is not None and es_quote.quality not in BAD_QUALITIES
    inputs = MicopediaInputs(
        created_at=state.as_of,
        underlier_price=options_map.underlier.price,
        vix1d=effective_price(state, "index:VIX1D"),
        vix=effective_price(state, "index:VIX"),
        skew_index=effective_price(state, "index:SKEW"),
        put_skew_ratio=(front.put_skew_ratio if front else None),
        gamma_state=gamma_state_for_micopedia(options_map),
        directional_bias="neutral_unclear",
        time_phase=time_phase_from_window(window),
        event_tags=tuple(env_csv("MICOPEDIA_EVENT_TAGS", "")),
        key_levels=tuple(key_levels),
        has_option_chain=bool(options_map.expiries),
        has_es_data=has_es_data,
    )
    signal = build_micopedia_signal(inputs)
    return {
        "source": signal.source,
        "regime": signal.regime,
        "directional_bias": signal.directional_bias,
        "confidence": signal.confidence,
        "dip_context": signal.dip_context,
        "vix_ratio": inputs.vix_ratio,
        "event_tags": list(inputs.event_tags),
        "suggested_sampling_mode": signal.suggested_sampling_mode,
        "candidate_expression": signal.candidate_expression,
        "map_focus": (
            "SPX price map: level reaction, opening range, prior high/low, and VWAP.",
            "SPXW option map: ATM straddle, call wall, put wall, zero-gamma zone, and max-payoff risk.",
            "SPXW IV surface: ATM IV, 0DTE/next-expiry gap, skew, curvature, and IV-crush risk.",
            "ES confirmation: use ES only to validate SPX timing and liquidity.",
        ),
        "trigger_watchlist": signal.trigger_watchlist[:4],
        "risk_policy": signal.risk_policy[:4],
        "invalidation_checks": (
            "Reject the thesis if SPX accepts on the wrong side of the mapped wall or key level.",
            "Reject the thesis if SPXW IV/skew behavior contradicts the expected range or crush scenario.",
            "Reject the thesis if ES does not confirm the SPX timing read.",
        ),
        "next_checks": (
            "Check SPX location versus nearest SPXW wall and zero-gamma zone.",
            "Check whether 0DTE ATM IV, skew, and straddle changed meaningfully over the last hour.",
            "Check ES confirmation before waking the human.",
        ),
    }


def effective_price(state: LatestState, instrument_id: str) -> float | None:
    quote = state.best_quote(instrument_id)
    return quote.effective_price if quote else None


def human_data_warnings(
    state: LatestState,
    *,
    options_map: OptionsMap,
    iv_surface: IvSurfaceSnapshot | None,
) -> tuple[str, ...]:
    warnings: list[str] = []
    spx = state.best_quote("index:SPX")
    es = state.best_quote("future:ES")
    if spx is None or spx.quality in BAD_QUALITIES:
        warnings.append("SPX quote is missing or degraded.")
    if es is None or es.quality in BAD_QUALITIES:
        warnings.append("ES quote is missing or degraded.")
    if not options_map.expiries:
        warnings.append("SPXW option map is missing.")
    if iv_surface is None:
        warnings.append("SPXW IV surface is missing.")
    elif any(expiry.surface_fit_quality != "raw_grid" for expiry in iv_surface.expiries[:2]):
        warnings.append("SPXW IV surface quality is degraded.")
    return tuple(dict.fromkeys(warnings))


def build_human_focus_context(
    state: LatestState,
    *,
    options_map: OptionsMap,
    iv_surface: IvSurfaceSnapshot | None,
    iv_surface_history_1h: dict[str, Any] | None,
    window: dict[str, object],
) -> dict[str, object]:
    history_by_expiry = {
        str(item.get("expiry") or ""): item
        for item in (iv_surface_history_1h or {}).get("expiries", [])
        if isinstance(item, dict)
    }
    surface_payload = iv_surface.to_dict() if iv_surface is not None else None
    surface_expiries = surface_payload.get("expiries", []) if isinstance(surface_payload, dict) else []
    return {
        "visible_scope": ("SPX", "SPXW", "ES", "VIX", "VIX1D", "VIX9D", "VIX3M", "VVIX", "SKEW"),
        "prices": {
            "spx": quote_summary(state, "index:SPX"),
            "es": quote_summary(state, "future:ES"),
        },
        "vol_context": {
            "vix": quote_summary(state, "index:VIX"),
            "vix1d": quote_summary(state, "index:VIX1D"),
            "vix9d": quote_summary(state, "index:VIX9D"),
            "vix3m": quote_summary(state, "index:VIX3M"),
            "vvix": quote_summary(state, "index:VVIX"),
            "skew": quote_summary(state, "index:SKEW"),
        },
        "spxw_options": {
            "underlier_price": options_map.underlier.price,
            "expiries": [expiry_options_summary(expiry) for expiry in options_map.expiries[:2]],
            "warnings": options_map.warnings,
            "wall_confluence": (
                options_map.spy_confluence.to_dict() if options_map.spy_confluence else None
            ),
        },
        "spxw_iv_surface": {
            "front_expiry": iv_surface.front_expiry if iv_surface else None,
            "next_expiry": iv_surface.next_expiry if iv_surface else None,
            "front_vs_next_atm_iv_gap": iv_surface.front_vs_next_atm_iv_gap if iv_surface else None,
            "history_1h": iv_surface_history_1h,
            "expiries": [
                surface_expiry_summary(expiry, history_by_expiry)
                for expiry in surface_expiries[:2]
                if isinstance(expiry, dict)
            ],
            "warnings": iv_surface.warnings if iv_surface else (),
        },
        "micopedia": micopedia_context(state, options_map=options_map, window=window),
        "data_warnings": human_data_warnings(state, options_map=options_map, iv_surface=iv_surface),
    }
