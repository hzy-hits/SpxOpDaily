from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

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
from spx_spark.analytics.options.pricing import (
    finite_float,
    option_iv,
    time_to_expiry_years,
    usable_delta,
)
from spx_spark.analytics.options.quality import option_gamma_structural
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR, ET
from spx_spark.marketdata import Quote
from spx_spark.settings import settings_value
from spx_spark.state_io import atomic_write_json_secure
from spx_spark.storage import LatestState, configured_quote_use_decision

SIGN_CONVENTION = "calls_positive_puts_negative"
DEALER_POSITION_SIGN = "unknown"
DIRECTION = "unknown"
MODEL = "bs_r0_q0"
METHOD = "call_positive_put_negative_oi_proxy_not_dealer_position"
PROXY_DISCLAIMER = (
    "all *_proxy metrics are house-defined; not comparable to any vendor metric of similar name"
)

_MIN_TIME_TO_EXPIRY_YEARS = 15.0 / (60.0 * 24.0 * 365.0)

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
)

@dataclass(frozen=True)
class ExposureInputRow:
    contract_id: str
    expiry: str
    strike: float
    right: str
    provider: str
    quality: str
    bid: float | None
    ask: float | None
    mid: float | None
    iv: float | None
    delta: float | None
    gamma: float | None
    open_interest: float
    volume: float
    quote_age_seconds: float | None
    pricing_allowed: bool


@dataclass(frozen=True)
class StrikeExposureValues:
    call_gex: float | None
    put_gex: float | None
    net_gex: float | None
    abs_gex: float | None
    net_dex_proxy: float | None
    vex_proxy: float | None
    cex_proxy: float | None
    abs_dex_proxy: float | None = None


@dataclass(frozen=True)
class StrikeExposure:
    strike: float
    call_open_interest: float
    put_open_interest: float
    call_volume: float
    put_volume: float
    call_iv: float | None
    put_iv: float | None
    call_delta: float | None
    put_delta: float | None
    call_gamma: float | None
    put_gamma: float | None
    call_vanna_per_vol_point: float | None
    put_vanna_per_vol_point: float | None
    call_charm_per_minute: float | None
    put_charm_per_minute: float | None
    oi_weighted: StrikeExposureValues
    volume_weighted: StrikeExposureValues


@dataclass(frozen=True)
class ExposureAggregates:
    net_gex: float | None
    abs_gex: float | None
    net_gamma_ratio: float | None
    net_dex_proxy: float | None
    net_dex_ratio_proxy: float | None
    dagex_proxy: float | None
    vex_proxy: float | None
    cex_proxy: float | None
    abs_dex_proxy: float | None = None


@dataclass(frozen=True)
class WallSet:
    call_walls: tuple[WallLevel, ...]
    put_walls: tuple[WallLevel, ...]
    wall_method: str
    pin_candidate: float | None


@dataclass(frozen=True)
class ExpiryExposure:
    expiry: str
    row_count: int
    strike_count: int
    quality: str
    oi_quality: str
    iv_source: str
    snapshot_age_seconds: float | None
    delta_coverage_ratio: float
    iv_coverage_ratio: float
    strikes: tuple[StrikeExposure, ...]
    oi_weighted: ExposureAggregates
    volume_weighted: ExposureAggregates
    gex_weighting_divergence: float | None
    walls: WallSet
    zero_gamma: float | None
    gamma_flip_zone: tuple[float, float] | None
    zero_gamma_method: str
    sign_convention: str
    dealer_position_sign: str
    direction: str
    model: str
    warnings: tuple[str, ...]


@dataclass(frozen=True)
class ExposureMap:
    created_at: datetime
    as_of: datetime
    underlier: Any
    expiries: tuple[ExpiryExposure, ...]
    warnings: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return exposure_map_to_dict(self)


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
    if not row.pricing_allowed:
        return None
    weight = _leg_weight(row, weighting)
    if weight is None or row.gamma is None:
        return None
    sign = 1.0 if row.right == "C" else -1.0
    return sign * row.gamma * weight * 100.0 * spot * spot * 0.01


def _leg_dex(row: ExposureInputRow, *, spot: float, weighting: str) -> float | None:
    if not row.pricing_allowed:
        return None
    weight = _leg_weight(row, weighting)
    if weight is None or row.delta is None:
        return None
    return row.delta * weight * 100.0 * spot * 0.01


def _leg_vex(
    row: ExposureInputRow, *, spot: float, weighting: str, tau_years: float
) -> float | None:
    if not row.pricing_allowed:
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
    if not row.pricing_allowed:
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
    age_ms = quote.quote_age_ms(as_of)
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
        iv=option_iv(quote),
        delta=usable_delta(quote),
        gamma=option_gamma_structural(quote, as_of=as_of),
        open_interest=finite_float(quote.open_interest) or 0.0,
        volume=finite_float(quote.volume) or 0.0,
        quote_age_seconds=age_ms / 1000.0 if age_ms is not None else None,
        pricing_allowed=configured_quote_use_decision(quote, as_of=as_of).pricing_allowed,
    )


def _tau_is_floored(expiry: str, as_of: datetime) -> bool:
    expiry_date = datetime.strptime(expiry, "%Y%m%d").date()
    session = DEFAULT_MARKET_CALENDAR.session(expiry_date)
    if session is None:
        return True
    delta_seconds = (session.close_at - as_of.astimezone(session.close_at.tzinfo)).total_seconds()
    if delta_seconds <= 0:
        return True
    years = delta_seconds / (365.0 * 24.0 * 3600.0)
    return years < _MIN_TIME_TO_EXPIRY_YEARS


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


def _determine_oi_quality(rows: tuple[ExposureInputRow, ...]) -> str:
    if not rows:
        return "missing"
    positive = [row for row in rows if row.open_interest > 0]
    if not positive:
        return "stale_or_zero"
    providers = Counter(row.provider for row in positive)
    dominant = providers.most_common(1)[0][0]
    if dominant == "schwab":
        return "schwab_unverified"
    return "ibkr_ok"


def _determine_iv_source(rows: tuple[ExposureInputRow, ...]) -> str:
    if not rows:
        return "missing"
    with_iv = [row for row in rows if row.iv is not None]
    if len(with_iv) / len(rows) < 0.5:
        return "missing"
    providers = Counter(row.provider for row in with_iv)
    if len(providers) > 1:
        return "mixed"
    dominant = providers.most_common(1)[0][0]
    if dominant == "schwab":
        return "vendor_schwab"
    return "vendor_ibkr"


def _snapshot_age_seconds(rows: tuple[ExposureInputRow, ...]) -> float | None:
    ages = [
        row.quote_age_seconds
        for row in rows
        if row.pricing_allowed and row.quote_age_seconds is not None
    ]
    if not ages:
        return None
    return max(ages)


def _early_session(as_of: datetime) -> bool:
    session = DEFAULT_MARKET_CALENDAR.session(as_of.astimezone(ET).date())
    if session is None:
        return False
    elapsed = (as_of.astimezone(ET) - session.open_at).total_seconds()
    return 0 <= elapsed <= 30 * 60


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
    call_iv = None if iv_missing else (call_row.iv if call_row else None)
    put_iv = None if iv_missing else (put_row.iv if put_row else None)
    call_vanna = (
        None
        if iv_missing or call_row is None or call_iv is None
        else bs_vanna_per_vol_point(spot, strike, call_iv, tau_years)
    )
    put_vanna = (
        None
        if iv_missing or put_row is None or put_iv is None
        else bs_vanna_per_vol_point(spot, strike, put_iv, tau_years)
    )
    call_charm = (
        None
        if iv_missing or call_row is None or call_iv is None
        else bs_charm_per_minute(spot, strike, call_iv, tau_years)
    )
    put_charm = (
        None
        if iv_missing or put_row is None or put_iv is None
        else bs_charm_per_minute(spot, strike, put_iv, tau_years)
    )
    return StrikeExposure(
        strike=strike,
        call_open_interest=call_row.open_interest if call_row else 0.0,
        put_open_interest=put_row.open_interest if put_row else 0.0,
        call_volume=call_row.volume if call_row else 0.0,
        put_volume=put_row.volume if put_row else 0.0,
        call_iv=call_iv,
        put_iv=put_iv,
        call_delta=call_row.delta if call_row else None,
        put_delta=put_row.delta if put_row else None,
        call_gamma=call_row.gamma if call_row else None,
        put_gamma=put_row.gamma if put_row else None,
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
    warnings: list[str] = []
    oi_quality = _determine_oi_quality(rows)
    iv_source = _determine_iv_source(rows)
    snapshot_age = _snapshot_age_seconds(rows)
    delta_coverage = (
        sum(1 for row in rows if row.pricing_allowed and row.delta is not None) / len(rows)
        if rows
        else 0.0
    )
    iv_coverage = (
        sum(1 for row in rows if row.pricing_allowed and row.iv is not None) / len(rows)
        if rows
        else 0.0
    )
    tau_years = time_to_expiry_years(expiry, as_of=as_of)
    tau_floored = _tau_is_floored(expiry, as_of)
    if tau_floored:
        for row in rows:
            warnings.append(f"tau_floored:{row.contract_id}")

    if _early_session(as_of):
        warnings.append("early_session_low_volume")

    if oi_quality == "schwab_unverified":
        warnings.append("schwab_oi_unverified")

    quality = "ok"
    unavailable = not any(row.pricing_allowed for row in rows)
    if unavailable:
        quality = "unavailable"
    elif oi_quality in {"stale_or_zero", "missing"}:
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
        if oi_quality in {"stale_or_zero", "missing"}:
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
    if (
        oi_weighted.net_gamma_ratio is not None
        and volume_weighted.net_gamma_ratio is not None
    ):
        divergence = volume_weighted.net_gamma_ratio - oi_weighted.net_gamma_ratio

    wall_method = "oi_gex"
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
        if not gex_rows and strike_rows:
            wall_method = "volume_fallback"
            gex_rows = [
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

        pairs = pair_by_strike(quotes)
        zg_scan, flip_scan, scan_method = zero_gamma_spot_scan(
            pairs,
            underlier=spot,
            expiry=expiry,
            as_of=as_of,
            intraday=False,
        )
        if zg_scan is not None:
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
    )


def build_exposure_map(state: LatestState) -> ExposureMap:
    from spx_spark.options_map import group_spxw_option_quotes, select_underlier

    underlier = select_underlier(state)
    all_grouped = group_spxw_option_quotes(state)
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
        warnings.append(
            f"underlier_mismatch: using {underlier.source} price for SPX strikes"
        )

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


def exposure_map_to_dict(exposure: ExposureMap) -> dict[str, Any]:
    def _values_dict(values: StrikeExposureValues) -> dict[str, Any]:
        return asdict(values)

    def _aggregates_dict(aggregates: ExposureAggregates) -> dict[str, Any]:
        payload = asdict(aggregates)
        return payload

    expiries_payload = []
    for expiry in exposure.expiries:
        strikes_payload = []
        for strike in expiry.strikes:
            strikes_payload.append(
                {
                    "strike": strike.strike,
                    "call_open_interest": strike.call_open_interest,
                    "put_open_interest": strike.put_open_interest,
                    "call_volume": strike.call_volume,
                    "put_volume": strike.put_volume,
                    "call_iv": strike.call_iv,
                    "put_iv": strike.put_iv,
                    "call_delta": strike.call_delta,
                    "put_delta": strike.put_delta,
                    "call_gamma": strike.call_gamma,
                    "put_gamma": strike.put_gamma,
                    "call_vanna_per_vol_point": strike.call_vanna_per_vol_point,
                    "put_vanna_per_vol_point": strike.put_vanna_per_vol_point,
                    "call_charm_per_minute": strike.call_charm_per_minute,
                    "put_charm_per_minute": strike.put_charm_per_minute,
                    "oi_weighted": _values_dict(strike.oi_weighted),
                    "volume_weighted": _values_dict(strike.volume_weighted),
                }
            )
        expiries_payload.append(
            {
                "expiry": expiry.expiry,
                "row_count": expiry.row_count,
                "strike_count": expiry.strike_count,
                "quality": expiry.quality,
                "oi_quality": expiry.oi_quality,
                "iv_source": expiry.iv_source,
                "snapshot_age_seconds": expiry.snapshot_age_seconds,
                "delta_coverage_ratio": expiry.delta_coverage_ratio,
                "iv_coverage_ratio": expiry.iv_coverage_ratio,
                "strikes": strikes_payload,
                "oi_weighted": _aggregates_dict(expiry.oi_weighted),
                "volume_weighted": _aggregates_dict(expiry.volume_weighted),
                "gex_weighting_divergence": expiry.gex_weighting_divergence,
                "walls": {
                    "call_walls": [wall.to_dict() for wall in expiry.walls.call_walls],
                    "put_walls": [wall.to_dict() for wall in expiry.walls.put_walls],
                    "wall_method": expiry.walls.wall_method,
                    "pin_candidate": expiry.walls.pin_candidate,
                },
                "zero_gamma": expiry.zero_gamma,
                "gamma_flip_zone": expiry.gamma_flip_zone,
                "zero_gamma_method": expiry.zero_gamma_method,
                "sign_convention": SIGN_CONVENTION,
                "dealer_position_sign": DEALER_POSITION_SIGN,
                "direction": DIRECTION,
                "model": MODEL,
                "method": METHOD,
                "proxy_disclaimer": PROXY_DISCLAIMER,
                "warnings": list(expiry.warnings),
            }
        )
    return {
        "created_at": exposure.created_at.isoformat(),
        "as_of": exposure.as_of.isoformat(),
        "underlier": asdict(exposure.underlier),
        "expiries": expiries_payload,
        "warnings": list(exposure.warnings),
    }


def net_dex_proxy_by_expiry(
    exposure: ExposureMap, *, weighting: str
) -> dict[str, float | None]:
    if weighting not in {"oi_weighted", "volume_weighted"}:
        raise ValueError(f"unsupported weighting: {weighting}")
    result: dict[str, float | None] = {}
    for expiry in exposure.expiries:
        aggregates = (
            expiry.oi_weighted if weighting == "oi_weighted" else expiry.volume_weighted
        )
        result[expiry.expiry] = aggregates.net_dex_proxy
    return result


def persist_exposure_map(exposure: ExposureMap, data_root: Path | str) -> Path:
    """Atomically write exposure_map.json under {data_root}/latest/."""
    path = Path(data_root) / "latest" / "exposure_map.json"
    atomic_write_json_secure(path, exposure_map_to_dict(exposure))
    return path
