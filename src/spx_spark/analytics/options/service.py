"""Compose per-expiry and multi-expiry options analytics."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from datetime import datetime

from spx_spark.analytics.options.chain import median_strike_step, pair_by_strike
from spx_spark.analytics.options.density import build_rn_density
from spx_spark.analytics.options.exposure import (
    build_gex_by_strike,
    build_wall_ladder,
    nearest_zero,
    zero_gamma_bracket,
    zero_gamma_spot_scan,
)
from spx_spark.analytics.options.exposure_types import StrikeGex, WallLevel
from spx_spark.analytics.options.levels import classify_gamma_state
from spx_spark.analytics.options.max_pain import build_max_pain
from spx_spark.analytics.options.models import (
    ExpiryOptionsMap,
    LevelProbability,
)
from spx_spark.analytics.options.pricing import (
    finite_float,
    interpolated_atm_iv,
    option_iv,
    option_mid,
    time_to_expiry_years,
    weighted_mean,
    wing_iv_at_delta,
)
from spx_spark.analytics.options.probability import probability_for_level
from spx_spark.analytics.options.quote_policy import option_analytical_pricing_allowed
from spx_spark.analytics.options.quality import build_coverage
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import OptionRight, Quote


MIN_OI_CONTRACT_COVERAGE_RATIO = 0.80
MIN_OI_DISTINCT_STRIKES = 3


def _open_interest_provider(quote: Quote) -> str:
    raw = quote.raw if isinstance(quote.raw, Mapping) else {}
    return str(raw.get("open_interest_provider") or quote.provider.value).lower()


def _verified_oi_pairs(
    pairs: dict[float, dict[OptionRight, Quote]],
) -> dict[float, dict[OptionRight, Quote]]:
    """Retain prices/volume while stripping every non-IBKR OI observation."""

    return {
        strike: {
            right: (
                quote
                if _open_interest_provider(quote) == "ibkr"
                else replace(quote, open_interest=None)
            )
            for right, quote in pair.items()
        }
        for strike, pair in pairs.items()
    }


def oi_wall_coverage(
    pairs: dict[float, dict[OptionRight, Quote]],
    *,
    underlier: float,
) -> tuple[bool, str, float]:
    """Require broad, two-wing, provider-verified OI before publishing walls."""

    legs = [
        quote
        for pair in pairs.values()
        for quote in pair.values()
    ]
    if not legs:
        return False, "oi_coverage_missing", 0.0

    def trusted_positive(quote: Quote) -> bool:
        if finite_float(quote.open_interest) is None or float(quote.open_interest) <= 0:
            return False
        return _open_interest_provider(quote) == "ibkr"

    trusted = [quote for quote in legs if trusted_positive(quote)]
    ratio = len(trusted) / len(legs)
    if ratio < MIN_OI_CONTRACT_COVERAGE_RATIO:
        return False, "oi_contract_coverage_below_threshold", ratio
    if len({float(quote.instrument.strike or 0.0) for quote in trusted}) < MIN_OI_DISTINCT_STRIKES:
        return False, "oi_strike_coverage_below_threshold", ratio
    put_wing = any(
        quote.instrument.right is OptionRight.PUT
        and float(quote.instrument.strike or 0.0) <= underlier
        for quote in trusted
    )
    call_wing = any(
        quote.instrument.right is OptionRight.CALL
        and float(quote.instrument.strike or 0.0) >= underlier
        for quote in trusted
    )
    if not put_wing or not call_wing:
        return False, "oi_two_wing_coverage_missing", ratio
    return True, "verified_two_wing_coverage", ratio


def build_expiry_map(
    expiry: str,
    quotes: list[Quote],
    underlier: float | None,
    *,
    as_of: datetime,
    underlier_mismatch: bool = False,
) -> ExpiryOptionsMap:
    coverage_quotes = quotes
    quotes = [
        quote
        for quote in quotes
        if not (
            isinstance(quote.raw, Mapping)
            and quote.raw.get("analytical_only") is True
        )
        or option_analytical_pricing_allowed(quote)
    ]
    coverage = build_coverage(coverage_quotes, as_of=as_of)
    pairs = pair_by_strike(quotes)
    strikes = sorted(pairs)
    warnings: list[str] = []
    atm_strike = (
        min(strikes, key=lambda strike: abs(strike - underlier)) if strikes and underlier else None
    )
    atm_call = pairs.get(atm_strike, {}).get(OptionRight.CALL) if atm_strike is not None else None
    atm_put = pairs.get(atm_strike, {}).get(OptionRight.PUT) if atm_strike is not None else None
    atm_call_mid = option_mid(atm_call)
    atm_put_mid = option_mid(atm_put)
    straddle = (
        atm_call_mid + atm_put_mid if atm_call_mid is not None and atm_put_mid is not None else None
    )
    atm_iv = interpolated_atm_iv(pairs, underlier)

    put_iv_items: list[tuple[float, float]] = []
    call_iv_items: list[tuple[float, float]] = []
    if underlier is not None:
        for quote in quotes:
            strike = finite_float(quote.instrument.strike)
            right = quote.instrument.right
            iv = option_iv(quote)
            if strike is None or right is None or iv is None:
                continue
            weight = max(
                finite_float(quote.open_interest) or finite_float(quote.volume) or 1.0, 1.0
            )
            moneyness = strike / underlier
            if right == OptionRight.PUT and 0.97 <= moneyness <= 0.995:
                put_iv_items.append((iv, weight))
            if right == OptionRight.CALL and 1.005 <= moneyness <= 1.03:
                call_iv_items.append((iv, weight))
    put_wing_iv = weighted_mean(put_iv_items)
    call_wing_iv = weighted_mean(call_iv_items)

    put_quotes = [quote for quote in quotes if quote.instrument.right == OptionRight.PUT]
    call_quotes = [quote for quote in quotes if quote.instrument.right == OptionRight.CALL]
    put_iv_25 = wing_iv_at_delta(put_quotes)
    call_iv_25 = wing_iv_at_delta(call_quotes)
    if put_iv_25 is not None and call_iv_25 is not None:
        skew_method = "delta_25"
        put_skew_25d = put_iv_25 - atm_iv if atm_iv is not None else None
        call_skew_25d = call_iv_25 - atm_iv if atm_iv is not None else None
    else:
        skew_method = "moneyness_fallback"
        put_skew_25d = (
            put_wing_iv - atm_iv if put_wing_iv is not None and atm_iv is not None else None
        )
        call_skew_25d = (
            call_wing_iv - atm_iv if call_wing_iv is not None and atm_iv is not None else None
        )

    intraday = expiry == DEFAULT_MARKET_CALENDAR.research_expiry(as_of).strftime("%Y%m%d")
    oi_usable, oi_coverage_reason, oi_coverage_ratio = (
        oi_wall_coverage(pairs, underlier=underlier)
        if underlier
        else (False, "underlier_unavailable", 0.0)
    )
    verified_oi_pairs = _verified_oi_pairs(pairs)
    if oi_usable:
        gex_weighting = "oi_plus_volume" if intraday else "oi"
    else:
        gex_weighting = "volume" if intraday else "unavailable"
    gex_rows = (
        build_gex_by_strike(
            verified_oi_pairs if oi_usable else pairs,
            underlier=underlier,
            as_of=as_of,
            intraday=intraday,
            include_open_interest=oi_usable,
        )
        if underlier and (oi_usable or intraday)
        else []
    )
    net_gex = sum(row.net_gex for row in gex_rows) if gex_rows else None
    abs_gex = sum(row.abs_gex for row in gex_rows) if gex_rows else None
    net_gamma_ratio = net_gex / abs_gex if net_gex is not None and abs_gex and abs_gex > 0 else None
    zg_method = "strike_profile_fallback_no_flip"
    zero = None
    gamma_flip_zone = None
    if underlier and oi_usable:
        zg_scan, flip_scan, scan_method = zero_gamma_spot_scan(
            verified_oi_pairs,
            underlier=underlier,
            expiry=expiry,
            as_of=as_of,
            intraday=intraday,
        )
        if scan_method == "expiry_elapsed":
            zero = None
            gamma_flip_zone = None
            zg_method = scan_method
        elif zg_scan is not None:
            zero = zg_scan
            gamma_flip_zone = flip_scan
            zg_method = scan_method
        else:
            zero = nearest_zero(gex_rows, underlier)
            gamma_flip_zone = zero_gamma_bracket(gex_rows, underlier)
            zg_method = f"strike_profile_fallback_{scan_method}"
    elif underlier:
        zg_method = f"unavailable_{oi_coverage_reason}"
    else:
        zero = None
        gamma_flip_zone = None
    zero_distance = zero - underlier if zero is not None and underlier is not None else None

    # Walls come from OI-weighted GEX: OI is positioning (where hedging flow
    # will actually defend), while intraday volume piles up at whatever ATM
    # strikes price already visited and makes walls chase the tape. When OI is
    # entirely missing (e.g. GTH before the OCC update), fall back to the
    # intraday-weighted rows so the map is not empty.
    strike_step = median_strike_step(strikes)
    wall_rows: list[StrikeGex] = []
    wall_method = "unavailable"
    oi_rows = (
        build_gex_by_strike(
            verified_oi_pairs,
            underlier=underlier,
            as_of=as_of,
            intraday=False,
        )
        if underlier and oi_usable
        else []
    )
    if oi_rows:
        wall_rows = oi_rows
        wall_method = "oi_gex"
    elif intraday and underlier:
        wall_rows = build_gex_by_strike(
            pairs,
            underlier=underlier,
            as_of=as_of,
            intraday=True,
            include_open_interest=False,
        )
        wall_method = "volume_fallback" if wall_rows else "unavailable"
    call_walls: tuple[WallLevel, ...] = ()
    put_walls: tuple[WallLevel, ...] = ()
    if underlier and wall_rows:
        call_walls, put_walls = build_wall_ladder(
            wall_rows,
            underlier=underlier,
            strike_step=strike_step,
        )
    call_wall = call_walls[0].strike if call_walls else None
    put_wall = put_walls[0].strike if put_walls else None
    walls = [wall for wall in (call_wall, put_wall) if wall is not None]
    nearest_wall_value = (
        min(walls, key=lambda wall: abs(wall - underlier)) if walls and underlier else None
    )
    nearest_wall_distance = (
        nearest_wall_value - underlier if nearest_wall_value is not None and underlier else None
    )
    gex_quality = "open_interest_gex" if oi_rows else "no_open_interest_gex"

    if underlier is None:
        warnings.append("missing underlier reference; ATM, surface, and GEX map are degraded")
    if not quotes:
        warnings.append("missing option quotes")
    if coverage.with_iv < max(1, coverage.total // 2):
        warnings.append("low IV coverage")
    if coverage.with_gamma < max(1, coverage.total // 2):
        warnings.append("low gamma coverage")
    if coverage.with_open_interest == 0:
        warnings.append("missing open interest; call/put wall and GEX are unavailable")
    elif not oi_usable:
        warnings.append(
            f"open interest wall coverage rejected:{oi_coverage_reason}:"
            f"{oi_coverage_ratio:.3f}"
        )
    if underlier_mismatch:
        warnings.append("underlier mismatch; wall distance and gamma alerts suppressed")

    # Industry expected-move convention: 0.85 x ATM straddle. An ATM straddle
    # is ~0.798x of a true 1-sigma move (1-sigma ~ 1.25x straddle), so
    # 0.85 x straddle ~ 0.68 sigma — roughly a 50% interval, ~32% tighter
    # than a real 1-sigma move. Downstream coefficients (e.g. the order-map
    # touch-time 0.6) are calibrated against this EM unit; do not rescale.
    expected_move = straddle * 0.85 if straddle is not None else None
    expected_move_pct = (
        expected_move / underlier if expected_move is not None and underlier else None
    )
    gamma_state = classify_gamma_state(
        net_gamma_ratio=net_gamma_ratio,
        zero_gamma_distance_points=zero_distance,
        underlier=underlier,
        gex_quality=gex_quality,
        underlier_mismatch=underlier_mismatch,
    )
    if underlier_mismatch:
        nearest_wall_value = None
        nearest_wall_distance = None

    rn_density = (
        build_rn_density(
            pairs,
            underlier=underlier,
            put_wall=put_wall,
            call_wall=call_wall,
            expected_move_points=expected_move,
        )
        if underlier
        else None
    )
    max_pain = build_max_pain(verified_oi_pairs, underlier=underlier) if oi_usable else None

    level_probabilities: list[LevelProbability] = []
    if underlier is not None:
        tau_years = time_to_expiry_years(expiry, as_of=as_of)
        for level_name, level_value in (
            ("put_wall", put_wall),
            ("zero_gamma", zero),
            ("call_wall", call_wall),
        ):
            if level_value is None:
                continue
            (
                prob_close,
                prob_touch,
                source_strike,
                source_delta,
                probability_method,
            ) = probability_for_level(
                level_value,
                underlier=underlier,
                pairs=pairs,
                strike_step=strike_step,
                tau_years=tau_years,
            )
            level_probabilities.append(
                LevelProbability(
                    level_name=level_name,
                    level=level_value,
                    prob_close_beyond=prob_close,
                    prob_touch=prob_touch,
                    source_strike=source_strike,
                    source_delta=source_delta,
                    probability_method=probability_method.value,
                )
            )

    return ExpiryOptionsMap(
        expiry=expiry,
        option_count=len(coverage_quotes),
        strike_count=len(strikes),
        atm_strike=atm_strike,
        atm_call_mid=atm_call_mid,
        atm_put_mid=atm_put_mid,
        atm_straddle_mid=straddle,
        expected_move_points=expected_move,
        expected_move_pct=expected_move_pct,
        atm_iv=atm_iv,
        put_wing_iv=put_wing_iv,
        call_wing_iv=call_wing_iv,
        put_skew_ratio=put_wing_iv / atm_iv if put_wing_iv is not None and atm_iv else None,
        call_skew_ratio=call_wing_iv / atm_iv if call_wing_iv is not None and atm_iv else None,
        net_gex=net_gex,
        abs_gex=abs_gex,
        net_gamma_ratio=net_gamma_ratio,
        zero_gamma=zero,
        zero_gamma_distance_points=zero_distance,
        call_wall=call_wall,
        put_wall=put_wall,
        nearest_wall=nearest_wall_value,
        nearest_wall_distance_points=nearest_wall_distance,
        gamma_state=gamma_state,
        gex_quality=gex_quality,
        coverage=coverage,
        top_gex_strikes=tuple(sorted(gex_rows, key=lambda row: row.abs_gex, reverse=True)[:10]),
        warnings=tuple(dict.fromkeys(warnings)),
        level_probabilities=tuple(level_probabilities),
        gamma_flip_zone=gamma_flip_zone,
        gex_weighting=gex_weighting,
        zero_gamma_method=zg_method,
        put_skew_25d=put_skew_25d,
        call_skew_25d=call_skew_25d,
        skew_method=skew_method,
        call_walls=call_walls,
        put_walls=put_walls,
        wall_method=wall_method,
        rn_density=rn_density,
        max_pain=max_pain,
    )
