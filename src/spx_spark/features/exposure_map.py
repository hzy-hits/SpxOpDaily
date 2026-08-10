from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import datetime
from typing import Any

from spx_spark.analytics.greeks.black_scholes import bs_delta, bs_gamma
from spx_spark.analytics.greeks.higher_order import (
    bs_charm_per_minute,
    bs_vanna_per_vol_point,
)
from spx_spark.analytics.options.chain import (
    chain_implied_spot,
    is_spxw_option,
    median_strike_step,
    pair_by_strike,
)
from spx_spark.analytics.options.constants import UNDERLIER_MISMATCH_SOURCES
from spx_spark.analytics.options.exposure import (
    build_gex_by_strike,
    build_wall_ladder,
    gex_weight,
    interpolate_zero,
    nearest_zero,
    signed_gex,
    zero_gamma_bracket,
    zero_gamma_spot_scan,
)
from spx_spark.analytics.options.exposure_types import StrikeGex, WallLevel
from spx_spark.analytics.options.models import UnderlierReference
from spx_spark.analytics.options.quote_policy import (
    option_analytical_lane,
    option_analytical_max_age_seconds,
    option_analytical_iv_allowed,
    option_analytical_pricing_allowed,
    option_field_age_seconds,
    option_field_live_entitlement,
    option_field_live_entitlement_source,
)
from spx_spark.analytics.options.pricing import (
    finite_float,
    option_iv,
    time_to_expiry_years,
    usable_delta,
)
from spx_spark.analytics.options.quality import option_gamma_structural
from spx_spark.features.exposure_freshness import (
    determine_iv_source as _determine_iv_source,
    determine_oi_quality as _determine_oi_quality,
    early_session as _early_session,
    freshness_summary as _freshness_summary,
    snapshot_age_seconds as _snapshot_age_seconds,
    tau_is_floored as _tau_is_floored,
)
from spx_spark.features.exposure_schema import (
    DEALER_POSITION_SIGN,
    DIRECTION,
    METHOD,
    MODEL,
    PROXY_DISCLAIMER,
    SIGN_CONVENTION,
    ExpiryExposure,
    ExposureAggregates,
    ExposureInputRow,
    ExposureMap,
    StrikeExposure,
    StrikeExposureValues,
    WallSet,
    exposure_map_to_dict,
    net_dex_proxy_by_expiry,
    persist_exposure_map,
)
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import Quote
from spx_spark.settings import settings_value
from spx_spark.storage import LatestState, configured_quote_use_decision

# Re-export shared GEX types/helpers for existing import paths.
__all__ = (
    "StrikeGex",
    "WallLevel",
    "build_gex_by_strike",
    "build_wall_ladder",
    "gex_weight",
    "interpolate_zero",
    "nearest_zero",
    "signed_gex",
    "zero_gamma_bracket",
    "METHOD",
    "PROXY_DISCLAIMER",
    "exposure_map_to_dict",
    "net_dex_proxy_by_expiry",
    "persist_exposure_map",
)


def _leg_weight(row: ExposureInputRow, weighting: str) -> float | None:
    if weighting == "oi_weighted":
        weight = row.open_interest
    elif weighting == "volume_weighted":
        weight = row.volume
    elif weighting == "oi_plus_volume":
        weight = row.open_interest + row.volume
    else:
        raise ValueError(f"unsupported weighting: {weighting}")
    if weight <= 0:
        return None
    return weight


def _leg_gex(row: ExposureInputRow, *, spot: float, weighting: str) -> float | None:
    if not row.analytical_allowed:
        return None
    weight = _leg_weight(row, weighting)
    if weight is None or row.gamma is None:
        return None
    sign = 1.0 if row.right == "C" else -1.0
    return sign * row.gamma * weight * 100.0 * spot * spot * 0.01


def _leg_dex(row: ExposureInputRow, *, spot: float, weighting: str) -> float | None:
    if not row.analytical_allowed:
        return None
    weight = _leg_weight(row, weighting)
    if weight is None or row.delta is None:
        return None
    return row.delta * weight * 100.0 * spot * 0.01


def _leg_vex(
    row: ExposureInputRow, *, spot: float, weighting: str, tau_years: float
) -> float | None:
    if not row.analytical_allowed:
        return None
    weight = _leg_weight(row, weighting)
    if weight is None or row.iv is None:
        return None
    vanna = bs_vanna_per_vol_point(spot, row.strike, row.iv, tau_years)
    if vanna is None:
        return None
    sign = 1.0 if row.right == "C" else -1.0
    return sign * vanna * weight * 100.0 * spot * 0.01


def _leg_cex(
    row: ExposureInputRow,
    *,
    spot: float,
    weighting: str,
    tau_years: float,
    tau_floored: bool,
) -> float | None:
    if not row.analytical_allowed:
        return None
    if tau_floored:
        return None
    weight = _leg_weight(row, weighting)
    if weight is None or row.iv is None:
        return None
    charm = bs_charm_per_minute(spot, row.strike, row.iv, tau_years)
    if charm is None:
        return None
    sign = 1.0 if row.right == "C" else -1.0
    return sign * charm * weight * 100.0 * spot * 0.01


def strike_exposure_values(
    rows: tuple[ExposureInputRow, ...],
    *,
    spot: float,
    tau_years: float,
    weighting: str,
    tau_floored: bool = False,
) -> StrikeExposureValues:
    call_gex = put_gex = None
    dex_values: list[float] = []
    vex_total = 0.0
    vex_count = 0
    cex_total = 0.0
    cex_count = 0

    for row in rows:
        gex = _leg_gex(row, spot=spot, weighting=weighting)
        dex = _leg_dex(row, spot=spot, weighting=weighting)
        vex = _leg_vex(row, spot=spot, weighting=weighting, tau_years=tau_years)
        cex = _leg_cex(
            row, spot=spot, weighting=weighting, tau_years=tau_years, tau_floored=tau_floored
        )
        if row.right == "C":
            call_gex = gex
        else:
            put_gex = gex
        if dex is not None:
            dex_values.append(dex)
        if vex is not None:
            vex_total += vex
            vex_count += 1
        if cex is not None:
            cex_total += cex
            cex_count += 1

    call_value = call_gex or 0.0
    put_value = put_gex or 0.0
    has_gex = call_gex is not None or put_gex is not None
    net_gex = (call_value + put_value) if has_gex else None
    abs_gex = (abs(call_value) + abs(put_value)) if has_gex else None

    return StrikeExposureValues(
        call_gex=call_gex,
        put_gex=put_gex,
        net_gex=net_gex,
        abs_gex=abs_gex,
        net_dex_proxy=sum(dex_values) if dex_values else None,
        vex_proxy=vex_total if vex_count else None,
        cex_proxy=cex_total if cex_count else None,
        abs_dex_proxy=sum(abs(value) for value in dex_values) if dex_values else None,
    )


def exposure_input_row_from_quote(quote: Quote, *, as_of: datetime) -> ExposureInputRow | None:
    if not is_spxw_option(quote):
        return None
    instrument = quote.instrument
    strike = finite_float(instrument.strike)
    right = instrument.right
    expiry = instrument.expiry
    if strike is None or strike <= 0 or right is None or not expiry:
        return None
    raw = quote.raw if isinstance(quote.raw, dict) else {}
    age_ms = quote.quote_age_ms(as_of)
    configured_decision = configured_quote_use_decision(quote, as_of=as_of)
    analytical_only = raw.get("analytical_only") is True
    analytical_reason = (
        str(raw["analytical_rejection_reason"]) if raw.get("analytical_rejection_reason") else None
    )
    greek_rejections = raw.get("greeks_rejection_reasons")
    if analytical_reason is None and isinstance(greek_rejections, list) and greek_rejections:
        analytical_reason = str(greek_rejections[0])
    pricing_lane = option_analytical_lane(quote, field="pricing")
    greeks_lane = option_analytical_lane(quote, field="greeks")
    core_max_age = float(settings_value("market_data.latest_stale_after_seconds"))
    rotation_max_age = float(settings_value("market_data.rotation_stale_after_seconds"))
    analytical_max_age = option_analytical_max_age_seconds(
        quote,
        core_max_age_seconds=core_max_age,
        rotation_max_age_seconds=rotation_max_age,
        field="greeks",
    )
    pricing_analytical_allowed = (
        option_analytical_pricing_allowed(quote)
        if analytical_only
        else (
            configured_decision.pricing_allowed
            and option_field_live_entitlement(quote, field="pricing")
        )
    )
    greeks_analytical_allowed = (
        option_analytical_iv_allowed(quote)
        if analytical_only
        else (
            quote.greeks is not None
            and option_field_live_entitlement(quote, field="greeks")
        )
    )
    if analytical_reason is None and not pricing_analytical_allowed:
        if not option_field_live_entitlement(quote, field="pricing"):
            source = option_field_live_entitlement_source(quote, field="pricing")
            analytical_reason = (
                f"pricing_field_not_live:{source or 'contract_rejected'}"
            )
        elif not analytical_only:
            analytical_reason = configured_decision.reason
    if analytical_reason is None and not greeks_analytical_allowed:
        if quote.greeks is None:
            analytical_reason = "greeks_missing"
        elif not option_field_live_entitlement(quote, field="greeks"):
            source = option_field_live_entitlement_source(quote, field="greeks")
            analytical_reason = (
                f"greeks_field_not_live:{source or 'contract_rejected'}"
            )
    analytical_allowed = pricing_analytical_allowed and greeks_analytical_allowed
    if not analytical_allowed and analytical_reason is None:
        analytical_reason = (
            configured_decision.reason
            if not pricing_analytical_allowed
            else "greeks_missing_or_rejected"
        )
    iv = option_iv(quote) if analytical_allowed else None
    delta = usable_delta(quote) if analytical_allowed else None
    gamma = option_gamma_structural(quote, as_of=as_of) if analytical_allowed else None
    return ExposureInputRow(
        contract_id=instrument.canonical_id,
        expiry=expiry,
        strike=strike,
        right=right.value,
        provider=quote.provider.value,
        quality=quote.quality.value,
        bid=quote.bid,
        ask=quote.ask,
        mid=quote.mid,
        iv=iv,
        delta=delta,
        gamma=gamma,
        open_interest=finite_float(quote.open_interest) or 0.0,
        volume=finite_float(quote.volume) or 0.0,
        quote_age_seconds=age_ms / 1000.0 if age_ms is not None else None,
        observation_age_seconds=option_field_age_seconds(
            quote,
            as_of=as_of,
            field="pricing",
        ),
        structure_age_seconds=option_field_age_seconds(
            quote,
            as_of=as_of,
            field="greeks",
        ),
        pricing_provider=str(raw.get("pricing_provider") or quote.provider.value),
        greeks_provider=(str(raw["greeks_provider"]) if raw.get("greeks_provider") else None),
        open_interest_provider=str(raw.get("open_interest_provider") or quote.provider.value),
        pricing_lane=pricing_lane,
        greeks_lane=greeks_lane,
        open_interest_lane=option_analytical_lane(quote, field="open_interest"),
        open_interest_observation_age_seconds=option_field_age_seconds(
            quote,
            as_of=as_of,
            field="open_interest",
        ),
        analytical_max_age_seconds=analytical_max_age,
        pricing_allowed=(configured_decision.pricing_allowed if not analytical_only else False),
        analytical_allowed=analytical_allowed,
        analytical_reason=analytical_reason,
        delta_source="vendor" if delta is not None else "missing",
        gamma_source="vendor" if gamma is not None else "missing",
    )


def _sum_optional(values: list[float | None]) -> float | None:
    cleaned = [value for value in values if value is not None]
    if not cleaned:
        return None
    return sum(cleaned)


def _ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator <= 0:
        return None
    return numerator / denominator


def _aggregate_exposure(
    strike_values: tuple[StrikeExposureValues, ...],
    *,
    include_dagex: bool,
    call_put_dex: tuple[list[float | None], list[float | None]] | None = None,
) -> ExposureAggregates:
    net_gex = _sum_optional([row.net_gex for row in strike_values])
    abs_gex = _sum_optional([row.abs_gex for row in strike_values])
    net_dex = _sum_optional([row.net_dex_proxy for row in strike_values])
    dex_denominator = None
    if call_put_dex is not None:
        call_dex, put_dex = call_put_dex
        call_sum = _sum_optional(call_dex)
        put_sum = _sum_optional(put_dex)
        if call_sum is not None or put_sum is not None:
            dex_denominator = abs(call_sum or 0.0) + abs(put_sum or 0.0)
    return ExposureAggregates(
        net_gex=net_gex,
        abs_gex=abs_gex,
        net_gamma_ratio=_ratio(net_gex, abs_gex),
        net_dex_proxy=net_dex,
        net_dex_ratio_proxy=_ratio(net_dex, dex_denominator),
        dagex_proxy=net_gex if include_dagex else None,
        vex_proxy=_sum_optional([row.vex_proxy for row in strike_values]),
        cex_proxy=_sum_optional([row.cex_proxy for row in strike_values]),
        abs_dex_proxy=dex_denominator,
    )


def _with_model_greeks(
    row: ExposureInputRow,
    *,
    spot: float,
    tau_years: float,
) -> ExposureInputRow:
    if not row.analytical_allowed or row.iv is None or spot <= 0 or tau_years <= 0:
        return row
    delta = row.delta
    delta_source = row.delta_source
    if delta is None or not -1.0 <= delta <= 1.0:
        delta = bs_delta(spot, row.strike, row.iv, tau_years, row.right)
        delta_source = "bs_from_observed_iv"
    gamma = row.gamma
    gamma_source = row.gamma_source
    if gamma is None or gamma < 0:
        gamma = bs_gamma(spot, row.strike, row.iv, tau_years)
        gamma_source = "bs_from_observed_iv"
    return replace(
        row,
        delta=delta,
        gamma=gamma,
        delta_source=delta_source,
        gamma_source=gamma_source,
    )


def _build_strike_exposure(
    strike: float,
    rows: tuple[ExposureInputRow, ...],
    *,
    spot: float,
    tau_years: float,
    tau_floored: bool,
    iv_missing: bool,
) -> StrikeExposure:
    call_row = next((row for row in rows if row.right == "C"), None)
    put_row = next((row for row in rows if row.right == "P"), None)
    call_analytical = call_row if call_row is not None and call_row.analytical_allowed else None
    put_analytical = put_row if put_row is not None and put_row.analytical_allowed else None
    call_iv = None if iv_missing else (call_analytical.iv if call_analytical else None)
    put_iv = None if iv_missing else (put_analytical.iv if put_analytical else None)
    call_vanna = (
        None
        if iv_missing or call_analytical is None or call_iv is None
        else bs_vanna_per_vol_point(spot, strike, call_iv, tau_years)
    )
    put_vanna = (
        None
        if iv_missing or put_analytical is None or put_iv is None
        else bs_vanna_per_vol_point(spot, strike, put_iv, tau_years)
    )
    call_charm = (
        None
        if iv_missing or call_analytical is None or call_iv is None
        else bs_charm_per_minute(spot, strike, call_iv, tau_years)
    )
    put_charm = (
        None
        if iv_missing or put_analytical is None or put_iv is None
        else bs_charm_per_minute(spot, strike, put_iv, tau_years)
    )

    def metadata(row: ExposureInputRow | None) -> dict[str, Any]:
        if row is None:
            return {"available": False}
        return {
            "available": True,
            "pricing_provider": row.pricing_provider,
            "greeks_provider": row.greeks_provider,
            "open_interest_provider": row.open_interest_provider,
            "pricing_lane": row.pricing_lane,
            "greeks_lane": row.greeks_lane,
            "open_interest_lane": row.open_interest_lane,
            "source_age_seconds": row.quote_age_seconds,
            "observed_age_seconds": row.observation_age_seconds,
            "structure_age_seconds": row.structure_age_seconds,
            "open_interest_observation_age_seconds": (row.open_interest_observation_age_seconds),
            "analytical_max_age_seconds": row.analytical_max_age_seconds,
            "pricing_allowed": row.pricing_allowed,
            "analytical_allowed": row.analytical_allowed,
            "analytical_reason": row.analytical_reason,
            "delta_source": row.delta_source,
            "gamma_source": row.gamma_source,
            "nbbo_interpolated": False,
        }

    return StrikeExposure(
        strike=strike,
        call_open_interest=call_row.open_interest if call_row else 0.0,
        put_open_interest=put_row.open_interest if put_row else 0.0,
        call_volume=call_row.volume if call_row else 0.0,
        put_volume=put_row.volume if put_row else 0.0,
        call_iv=call_iv,
        put_iv=put_iv,
        call_delta=call_analytical.delta if call_analytical else None,
        put_delta=put_analytical.delta if put_analytical else None,
        call_gamma=call_analytical.gamma if call_analytical else None,
        put_gamma=put_analytical.gamma if put_analytical else None,
        call_vanna_per_vol_point=call_vanna,
        put_vanna_per_vol_point=put_vanna,
        call_charm_per_minute=call_charm,
        put_charm_per_minute=put_charm,
        oi_weighted=strike_exposure_values(
            rows, spot=spot, tau_years=tau_years, weighting="oi_weighted", tau_floored=tau_floored
        ),
        volume_weighted=strike_exposure_values(
            rows,
            spot=spot,
            tau_years=tau_years,
            weighting="volume_weighted",
            tau_floored=tau_floored,
        ),
        leg_metadata={
            "call": metadata(call_row),
            "put": metadata(put_row),
        },
    )


def _nullify_oi_weighted(strike: StrikeExposure) -> StrikeExposure:
    null_values = StrikeExposureValues(
        call_gex=None,
        put_gex=None,
        net_gex=None,
        abs_gex=None,
        net_dex_proxy=None,
        vex_proxy=None,
        cex_proxy=None,
    )
    return StrikeExposure(
        strike=strike.strike,
        call_open_interest=strike.call_open_interest,
        put_open_interest=strike.put_open_interest,
        call_volume=strike.call_volume,
        put_volume=strike.put_volume,
        call_iv=strike.call_iv,
        put_iv=strike.put_iv,
        call_delta=strike.call_delta,
        put_delta=strike.put_delta,
        call_gamma=strike.call_gamma,
        put_gamma=strike.put_gamma,
        call_vanna_per_vol_point=strike.call_vanna_per_vol_point,
        put_vanna_per_vol_point=strike.put_vanna_per_vol_point,
        call_charm_per_minute=strike.call_charm_per_minute,
        put_charm_per_minute=strike.put_charm_per_minute,
        oi_weighted=null_values,
        volume_weighted=strike.volume_weighted,
        leg_metadata=strike.leg_metadata,
    )


def _nullify_vanna_family(strike: StrikeExposure) -> StrikeExposure:
    def _strip(values: StrikeExposureValues) -> StrikeExposureValues:
        return StrikeExposureValues(
            call_gex=values.call_gex,
            put_gex=values.put_gex,
            net_gex=values.net_gex,
            abs_gex=values.abs_gex,
            net_dex_proxy=values.net_dex_proxy,
            vex_proxy=None,
            cex_proxy=None,
            abs_dex_proxy=values.abs_dex_proxy,
        )

    return StrikeExposure(
        strike=strike.strike,
        call_open_interest=strike.call_open_interest,
        put_open_interest=strike.put_open_interest,
        call_volume=strike.call_volume,
        put_volume=strike.put_volume,
        call_iv=strike.call_iv,
        put_iv=strike.put_iv,
        call_delta=strike.call_delta,
        put_delta=strike.put_delta,
        call_gamma=strike.call_gamma,
        put_gamma=strike.put_gamma,
        call_vanna_per_vol_point=None,
        put_vanna_per_vol_point=None,
        call_charm_per_minute=None,
        put_charm_per_minute=None,
        oi_weighted=_strip(strike.oi_weighted),
        volume_weighted=_strip(strike.volume_weighted),
        leg_metadata=strike.leg_metadata,
    )


def _nullify_all(strike: StrikeExposure) -> StrikeExposure:
    null_values = StrikeExposureValues(
        call_gex=None,
        put_gex=None,
        net_gex=None,
        abs_gex=None,
        net_dex_proxy=None,
        vex_proxy=None,
        cex_proxy=None,
    )
    return StrikeExposure(
        strike=strike.strike,
        call_open_interest=strike.call_open_interest,
        put_open_interest=strike.put_open_interest,
        call_volume=strike.call_volume,
        put_volume=strike.put_volume,
        call_iv=None,
        put_iv=None,
        call_delta=None,
        put_delta=None,
        call_gamma=None,
        put_gamma=None,
        call_vanna_per_vol_point=None,
        put_vanna_per_vol_point=None,
        call_charm_per_minute=None,
        put_charm_per_minute=None,
        oi_weighted=null_values,
        volume_weighted=null_values,
        leg_metadata=strike.leg_metadata,
    )


def _build_expiry_exposure(
    expiry: str,
    quotes: list[Quote],
    *,
    spot: float | None,
    as_of: datetime,
) -> ExpiryExposure:
    rows = tuple(
        row
        for quote in quotes
        if (row := exposure_input_row_from_quote(quote, as_of=as_of)) is not None
    )
    tau_years = time_to_expiry_years(expiry, as_of=as_of)
    if spot is not None and spot > 0:
        rows = tuple(_with_model_greeks(row, spot=spot, tau_years=tau_years) for row in rows)
    warnings: list[str] = []
    oi_quality = _determine_oi_quality(rows)
    iv_source = _determine_iv_source(rows)
    snapshot_age = _snapshot_age_seconds(rows)
    freshness = _freshness_summary(rows)
    delta_coverage = (
        sum(
            1
            for row in rows
            if row.analytical_allowed and row.delta is not None and row.delta_source == "vendor"
        )
        / len(rows)
        if rows
        else 0.0
    )
    iv_coverage = (
        sum(1 for row in rows if row.analytical_allowed and row.iv is not None) / len(rows)
        if rows
        else 0.0
    )
    tau_floored = _tau_is_floored(expiry, as_of)
    if tau_floored:
        for row in rows:
            warnings.append(f"tau_floored:{row.contract_id}")

    if _early_session(as_of):
        warnings.append("early_session_low_volume")

    if oi_quality == "schwab_unverified":
        warnings.append("schwab_oi_unverified")
    for reason, count in freshness.get("rejection_counts", {}).items():
        warnings.append(f"analytical_leg_rejected:{reason}:{count}")

    quality = "ok"
    unavailable = not any(row.analytical_allowed for row in rows)
    oi_weighted_disabled = oi_quality in {"stale_or_zero", "missing", "unverified_provider"}
    if unavailable:
        quality = "unavailable"
    elif oi_weighted_disabled:
        # Docs: only missing/stale OI nullifies oi_weighted. Schwab OI stays
        # numeric with schwab_oi_unverified warning and downstream confidence caps.
        quality = "no_open_interest"

    iv_missing = iv_source == "missing"

    by_strike: dict[float, tuple[ExposureInputRow, ...]] = defaultdict(tuple)
    for row in rows:
        by_strike[row.strike] = by_strike[row.strike] + (row,)

    strike_rows: list[StrikeExposure] = []
    for strike in sorted(by_strike):
        strike_rows.append(
            _build_strike_exposure(
                strike,
                by_strike[strike],
                spot=spot or 0.0,
                tau_years=tau_years,
                tau_floored=tau_floored,
                iv_missing=iv_missing,
            )
        )

    if unavailable:
        strike_rows = [_nullify_all(strike) for strike in strike_rows]
    else:
        if oi_weighted_disabled:
            strike_rows = [_nullify_oi_weighted(strike) for strike in strike_rows]
        if iv_missing:
            strike_rows = [_nullify_vanna_family(strike) for strike in strike_rows]

    oi_values = tuple(strike.oi_weighted for strike in strike_rows)
    vol_values = tuple(strike.volume_weighted for strike in strike_rows)

    call_dex_oi: list[float | None] = []
    put_dex_oi: list[float | None] = []
    call_dex_vol: list[float | None] = []
    put_dex_vol: list[float | None] = []
    for strike in strike_rows:
        call_row = next((row for row in by_strike[strike.strike] if row.right == "C"), None)
        put_row = next((row for row in by_strike[strike.strike] if row.right == "P"), None)
        if spot is not None and call_row is not None:
            call_dex_oi.append(_leg_dex(call_row, spot=spot, weighting="oi_weighted"))
            call_dex_vol.append(_leg_dex(call_row, spot=spot, weighting="volume_weighted"))
        else:
            call_dex_oi.append(None)
            call_dex_vol.append(None)
        if spot is not None and put_row is not None:
            put_dex_oi.append(_leg_dex(put_row, spot=spot, weighting="oi_weighted"))
            put_dex_vol.append(_leg_dex(put_row, spot=spot, weighting="volume_weighted"))
        else:
            put_dex_oi.append(None)
            put_dex_vol.append(None)

    oi_weighted = _aggregate_exposure(
        oi_values, include_dagex=False, call_put_dex=(call_dex_oi, put_dex_oi)
    )
    volume_weighted = _aggregate_exposure(
        vol_values, include_dagex=True, call_put_dex=(call_dex_vol, put_dex_vol)
    )

    if unavailable:
        null_agg = ExposureAggregates(
            net_gex=None,
            abs_gex=None,
            net_gamma_ratio=None,
            net_dex_proxy=None,
            net_dex_ratio_proxy=None,
            dagex_proxy=None,
            vex_proxy=None,
            cex_proxy=None,
        )
        oi_weighted = null_agg
        volume_weighted = null_agg
    elif delta_coverage < 0.5:
        warnings.append("low_delta_coverage")
        oi_weighted = ExposureAggregates(
            net_gex=oi_weighted.net_gex,
            abs_gex=oi_weighted.abs_gex,
            net_gamma_ratio=oi_weighted.net_gamma_ratio,
            net_dex_proxy=None,
            net_dex_ratio_proxy=None,
            dagex_proxy=None,
            vex_proxy=oi_weighted.vex_proxy,
            cex_proxy=oi_weighted.cex_proxy,
        )
        volume_weighted = ExposureAggregates(
            net_gex=volume_weighted.net_gex,
            abs_gex=volume_weighted.abs_gex,
            net_gamma_ratio=volume_weighted.net_gamma_ratio,
            net_dex_proxy=None,
            net_dex_ratio_proxy=None,
            dagex_proxy=volume_weighted.dagex_proxy,
            vex_proxy=volume_weighted.vex_proxy,
            cex_proxy=volume_weighted.cex_proxy,
        )

    divergence = None
    if oi_weighted.net_gamma_ratio is not None and volume_weighted.net_gamma_ratio is not None:
        divergence = volume_weighted.net_gamma_ratio - oi_weighted.net_gamma_ratio

    wall_method = "unavailable"
    call_walls: tuple[WallLevel, ...] = ()
    put_walls: tuple[WallLevel, ...] = ()
    pin_candidate: float | None = None
    zero_gamma: float | None = None
    gamma_flip_zone: tuple[float, float] | None = None
    zero_gamma_method = "strike_profile_fallback_no_flip"

    if spot is not None and not unavailable:
        gex_rows = [
            StrikeGex(
                strike=strike.strike,
                call_gex=strike.oi_weighted.call_gex or 0.0,
                put_gex=strike.oi_weighted.put_gex or 0.0,
                net_gex=strike.oi_weighted.net_gex or 0.0,
                abs_gex=strike.oi_weighted.abs_gex or 0.0,
                call_open_interest=strike.call_open_interest,
                put_open_interest=strike.put_open_interest,
                call_volume=strike.call_volume,
                put_volume=strike.put_volume,
            )
            for strike in strike_rows
            if strike.oi_weighted.call_gex is not None or strike.oi_weighted.put_gex is not None
        ]
        if gex_rows:
            wall_method = "oi_gex"
        elif strike_rows:
            volume_rows = [
                StrikeGex(
                    strike=strike.strike,
                    call_gex=strike.volume_weighted.call_gex or 0.0,
                    put_gex=strike.volume_weighted.put_gex or 0.0,
                    net_gex=strike.volume_weighted.net_gex or 0.0,
                    abs_gex=strike.volume_weighted.abs_gex or 0.0,
                    call_open_interest=strike.call_open_interest,
                    put_open_interest=strike.put_open_interest,
                    call_volume=strike.call_volume,
                    put_volume=strike.put_volume,
                )
                for strike in strike_rows
                if strike.volume_weighted.call_gex is not None
                or strike.volume_weighted.put_gex is not None
            ]
            if volume_rows:
                wall_method = "volume_fallback"
                gex_rows = volume_rows
        if gex_rows:
            strike_step = median_strike_step([row.strike for row in gex_rows])
            call_walls, put_walls = build_wall_ladder(
                gex_rows, underlier=spot, strike_step=strike_step
            )
            pin_max = float(settings_value("steven.pin_max_distance_points"))
            candidates = [
                strike
                for strike in strike_rows
                if strike.call_open_interest > 0
                and strike.put_open_interest > 0
                and strike.oi_weighted.net_gex is not None
                and abs(strike.strike - spot) <= pin_max
            ]
            if candidates:
                pin_candidate = max(
                    candidates,
                    key=lambda strike: abs(strike.oi_weighted.net_gex or 0.0),
                ).strike

        accepted_contract_ids = {row.contract_id for row in rows if row.analytical_allowed}
        pairs = pair_by_strike(
            [quote for quote in quotes if quote.instrument.canonical_id in accepted_contract_ids]
        )
        if oi_weighted_disabled:
            zero_gamma = None
            gamma_flip_zone = None
            zero_gamma_method = f"unavailable_{oi_quality}"
        else:
            zg_scan, flip_scan, scan_method = zero_gamma_spot_scan(
                pairs,
                underlier=spot,
                expiry=expiry,
                as_of=as_of,
                intraday=False,
            )
            if scan_method == "expiry_elapsed":
                zero_gamma = None
                gamma_flip_zone = None
                zero_gamma_method = scan_method
            elif zg_scan is not None:
                zero_gamma = zg_scan
                gamma_flip_zone = flip_scan
                zero_gamma_method = scan_method
            elif gex_rows:
                zero_gamma = nearest_zero(gex_rows, spot)
                gamma_flip_zone = zero_gamma_bracket(gex_rows, spot)
                zero_gamma_method = f"strike_profile_fallback_{scan_method}"

    return ExpiryExposure(
        expiry=expiry,
        row_count=len(rows),
        strike_count=len(strike_rows),
        quality=quality,
        oi_quality=oi_quality,
        iv_source=iv_source,
        snapshot_age_seconds=snapshot_age,
        delta_coverage_ratio=delta_coverage,
        iv_coverage_ratio=iv_coverage,
        strikes=tuple(strike_rows),
        oi_weighted=oi_weighted,
        volume_weighted=volume_weighted,
        gex_weighting_divergence=divergence,
        walls=WallSet(
            call_walls=call_walls,
            put_walls=put_walls,
            wall_method=wall_method,
            pin_candidate=pin_candidate,
        ),
        zero_gamma=zero_gamma,
        gamma_flip_zone=gamma_flip_zone,
        zero_gamma_method=zero_gamma_method,
        sign_convention=SIGN_CONVENTION,
        dealer_position_sign=DEALER_POSITION_SIGN,
        direction=DIRECTION,
        model=MODEL,
        warnings=tuple(dict.fromkeys(warnings)),
        freshness=freshness,
    )


def build_exposure_map(
    state: LatestState,
    *,
    grouped_quotes: Mapping[str, Sequence[Quote]] | None = None,
) -> ExposureMap:
    from spx_spark.options_map import group_spxw_option_quotes, select_underlier

    underlier = select_underlier(state)
    all_grouped = (
        grouped_quotes
        if grouped_quotes is not None
        else group_spxw_option_quotes(state)
    )
    active_expiries = {
        expiry.strftime("%Y%m%d")
        for expiry in DEFAULT_MARKET_CALENDAR.research_expiries(state.as_of)
    }
    grouped = {
        expiry: quotes for expiry, quotes in all_grouped.items() if expiry in active_expiries
    }

    warnings: list[str] = []
    underlier_mismatch = (
        underlier.source is not None and underlier.source in UNDERLIER_MISMATCH_SOURCES
    )
    if (underlier.price is None or underlier_mismatch) and grouped:
        front_expiry = sorted(grouped)[0]
        implied = chain_implied_spot(pair_by_strike(grouped[front_expiry]))
        reference = underlier.price
        implied_plausible = implied is not None and (
            reference is None or abs(implied / reference - 1.0) <= 0.02
        )
        if implied_plausible:
            underlier = UnderlierReference(price=implied, source="chain_implied")
            underlier_mismatch = False
    if underlier.price is None:
        warnings.append("missing SPX underlier reference")
    elif underlier_mismatch:
        warnings.append(f"underlier_mismatch: using {underlier.source} price for SPX strikes")

    expiries = tuple(
        _build_expiry_exposure(
            expiry,
            quotes,
            spot=underlier.price,
            as_of=state.as_of,
        )
        for expiry, quotes in sorted(grouped.items())
    )
    return ExposureMap(
        created_at=datetime.now(tz=state.as_of.tzinfo),
        as_of=state.as_of,
        underlier=underlier,
        expiries=expiries,
        warnings=tuple(dict.fromkeys(warnings)),
    )
