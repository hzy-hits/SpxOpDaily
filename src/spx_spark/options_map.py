from __future__ import annotations

import argparse
import json
import math
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

from spx_spark.config import StorageSettings
from spx_spark.marketdata import InstrumentType, MarketDataQuality, OptionRight, Quote
from spx_spark.storage import LatestState, LatestStateStore


UNDERLIER_CANDIDATES = (
    ("index:SPX", 1.0),
    ("future:ES", 1.0),
    ("future:MES", 1.0),
    ("equity:SPY", 10.0),
    ("crypto_perp:xyz:SP500", 1.0),
)

BAD_QUALITIES = {
    MarketDataQuality.MISSING,
    MarketDataQuality.ERROR,
    MarketDataQuality.STALE,
    MarketDataQuality.UNKNOWN,
    MarketDataQuality.DELAYED,
    MarketDataQuality.DELAYED_FROZEN,
}


@dataclass(frozen=True)
class UnderlierReference:
    price: float | None
    source: str | None


@dataclass(frozen=True)
class OptionCoverage:
    total: int
    live: int
    stale: int
    delayed: int
    unknown_age: int
    max_age_ms: float | None
    with_bid_ask: int
    with_mid: int
    with_iv: int
    with_delta: int
    with_gamma: int
    with_theta: int
    with_vega: int
    with_open_interest: int
    avg_spread_bps: float | None


@dataclass(frozen=True)
class StrikeGex:
    strike: float
    call_gex: float
    put_gex: float
    net_gex: float
    abs_gex: float
    call_open_interest: float
    put_open_interest: float


@dataclass(frozen=True)
class ExpiryOptionsMap:
    expiry: str
    option_count: int
    strike_count: int
    atm_strike: float | None
    atm_call_mid: float | None
    atm_put_mid: float | None
    atm_straddle_mid: float | None
    expected_move_points: float | None
    expected_move_pct: float | None
    atm_iv: float | None
    put_wing_iv: float | None
    call_wing_iv: float | None
    put_skew_ratio: float | None
    call_skew_ratio: float | None
    net_gex: float | None
    abs_gex: float | None
    net_gamma_ratio: float | None
    zero_gamma: float | None
    zero_gamma_distance_points: float | None
    call_wall: float | None
    put_wall: float | None
    nearest_wall: float | None
    nearest_wall_distance_points: float | None
    gamma_state: str
    gex_quality: str
    coverage: OptionCoverage
    top_gex_strikes: tuple[StrikeGex, ...]
    warnings: tuple[str, ...]


@dataclass(frozen=True)
class OptionsMap:
    created_at: datetime
    as_of: datetime
    underlier: UnderlierReference
    expiries: tuple[ExpiryOptionsMap, ...]
    warnings: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["created_at"] = self.created_at.isoformat()
        payload["as_of"] = self.as_of.isoformat()
        return payload


def finite_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(out):
        return None
    return out


def select_underlier(state: LatestState) -> UnderlierReference:
    for instrument_id, multiplier in UNDERLIER_CANDIDATES:
        quote = state.best_quote(instrument_id)
        if quote is None or quote.quality in BAD_QUALITIES:
            continue
        price = quote.effective_price
        if price is not None and price > 0:
            return UnderlierReference(price=price * multiplier, source=instrument_id)
    return UnderlierReference(price=None, source=None)


def is_spxw_option(quote: Quote) -> bool:
    instrument = quote.instrument
    if instrument.instrument_type != InstrumentType.OPTION:
        return False
    if (instrument.underlier or instrument.symbol).upper() != "SPX":
        return False
    trading_class = (instrument.trading_class or instrument.provider_symbol or "").upper()
    return trading_class.startswith("SPXW") or quote.instrument.canonical_id.startswith("option:SPX:SPXW:")


def option_mid(quote: Quote | None) -> float | None:
    if quote is None or quote.quality in BAD_QUALITIES:
        return None
    return quote.mid or quote.effective_price


def option_iv(quote: Quote | None) -> float | None:
    if quote is None or quote.quality in BAD_QUALITIES or quote.greeks is None:
        return None
    value = finite_float(quote.greeks.implied_vol)
    return value if value is not None and value > 0 else None


def option_gamma(quote: Quote) -> float | None:
    if quote.quality in BAD_QUALITIES or quote.greeks is None:
        return None
    value = finite_float(quote.greeks.gamma)
    return value if value is not None and value > 0 else None


def weighted_mean(items: list[tuple[float, float]]) -> float | None:
    cleaned = [(value, max(weight, 0.0)) for value, weight in items if value > 0 and weight >= 0]
    denom = sum(weight for _value, weight in cleaned)
    if denom <= 0:
        return None
    return sum(value * weight for value, weight in cleaned) / denom


def pair_by_strike(quotes: list[Quote]) -> dict[float, dict[OptionRight, Quote]]:
    pairs: dict[float, dict[OptionRight, Quote]] = defaultdict(dict)
    for quote in quotes:
        strike = finite_float(quote.instrument.strike)
        right = quote.instrument.right
        if strike is None or strike <= 0 or right is None:
            continue
        pairs[strike][right] = quote
    return pairs


def build_coverage(quotes: list[Quote], *, as_of: datetime) -> OptionCoverage:
    quality_counts = Counter(quote.quality for quote in quotes)
    spreads = [quote.spread_bps for quote in quotes if quote.spread_bps is not None]
    ages = [quote.quote_age_ms(as_of) for quote in quotes]
    known_ages = [age for age in ages if age is not None]
    return OptionCoverage(
        total=len(quotes),
        live=quality_counts[MarketDataQuality.LIVE],
        stale=quality_counts[MarketDataQuality.STALE],
        delayed=quality_counts[MarketDataQuality.DELAYED] + quality_counts[MarketDataQuality.DELAYED_FROZEN],
        unknown_age=sum(1 for age in ages if age is None),
        max_age_ms=max(known_ages) if known_ages else None,
        with_bid_ask=sum(1 for quote in quotes if quote.mid is not None),
        with_mid=sum(1 for quote in quotes if option_mid(quote) is not None),
        with_iv=sum(1 for quote in quotes if option_iv(quote) is not None),
        with_delta=sum(1 for quote in quotes if quote.greeks is not None and quote.greeks.delta is not None),
        with_gamma=sum(1 for quote in quotes if option_gamma(quote) is not None),
        with_theta=sum(1 for quote in quotes if quote.greeks is not None and quote.greeks.theta is not None),
        with_vega=sum(1 for quote in quotes if quote.greeks is not None and quote.greeks.vega is not None),
        with_open_interest=sum(
            1 for quote in quotes if quote.open_interest is not None and quote.open_interest > 0
        ),
        avg_spread_bps=sum(spreads) / len(spreads) if spreads else None,
    )


def interpolate_zero(left: StrikeGex, right: StrikeGex) -> float | None:
    denom = right.net_gex - left.net_gex
    if abs(denom) <= 1e-12:
        return None
    weight = -left.net_gex / denom
    if weight < 0 or weight > 1:
        return None
    return left.strike + weight * (right.strike - left.strike)


def build_gex_by_strike(
    pairs: dict[float, dict[OptionRight, Quote]],
    *,
    underlier: float,
) -> list[StrikeGex]:
    rows: list[StrikeGex] = []
    for strike, pair in sorted(pairs.items()):
        call = pair.get(OptionRight.CALL)
        put = pair.get(OptionRight.PUT)
        call_gex = signed_gex(call, sign=1.0, underlier=underlier) if call is not None else None
        put_gex = signed_gex(put, sign=-1.0, underlier=underlier) if put is not None else None
        if call_gex is None and put_gex is None:
            continue
        call_value = call_gex or 0.0
        put_value = put_gex or 0.0
        rows.append(
            StrikeGex(
                strike=strike,
                call_gex=call_value,
                put_gex=put_value,
                net_gex=call_value + put_value,
                abs_gex=abs(call_value) + abs(put_value),
                call_open_interest=finite_float(call.open_interest) if call else 0.0,
                put_open_interest=finite_float(put.open_interest) if put else 0.0,
            )
        )
    return rows


def signed_gex(quote: Quote, *, sign: float, underlier: float) -> float | None:
    gamma = option_gamma(quote)
    open_interest = finite_float(quote.open_interest)
    if gamma is None or open_interest is None or open_interest <= 0:
        return None
    return sign * gamma * open_interest * 100.0 * underlier * underlier * 0.01


def nearest_zero(gex_rows: list[StrikeGex], underlier: float) -> float | None:
    if not gex_rows:
        return None
    zeros: list[float] = []
    for left, right in zip(gex_rows, gex_rows[1:]):
        if abs(left.net_gex) <= 1e-12:
            zeros.append(left.strike)
        elif left.net_gex * right.net_gex < 0:
            zero = interpolate_zero(left, right)
            if zero is not None:
                zeros.append(zero)
    if abs(gex_rows[-1].net_gex) <= 1e-12:
        zeros.append(gex_rows[-1].strike)
    if not zeros:
        return None
    return min(zeros, key=lambda value: abs(value - underlier))


def classify_gamma_state(
    *,
    net_gamma_ratio: float | None,
    zero_gamma_distance_points: float | None,
    underlier: float | None,
    gex_quality: str,
    underlier_mismatch: bool = False,
) -> str:
    if underlier_mismatch:
        return "unknown_underlier_mismatch"
    if gex_quality == "no_open_interest_gex":
        return "unknown_no_open_interest"
    if net_gamma_ratio is None:
        return "unknown"
    if underlier and zero_gamma_distance_points is not None:
        if abs(zero_gamma_distance_points) / underlier <= 0.005:
            return "zero_gamma_transition"
    if net_gamma_ratio >= 0.15:
        return "positive_gamma_pin"
    if net_gamma_ratio <= -0.15:
        return "negative_gamma_acceleration"
    return "mixed_gamma"


def build_expiry_map(
    expiry: str,
    quotes: list[Quote],
    underlier: float | None,
    *,
    as_of: datetime,
    underlier_mismatch: bool = False,
) -> ExpiryOptionsMap:
    coverage = build_coverage(quotes, as_of=as_of)
    pairs = pair_by_strike(quotes)
    strikes = sorted(pairs)
    warnings: list[str] = []
    atm_strike = min(strikes, key=lambda strike: abs(strike - underlier)) if strikes and underlier else None
    atm_call = pairs.get(atm_strike, {}).get(OptionRight.CALL) if atm_strike is not None else None
    atm_put = pairs.get(atm_strike, {}).get(OptionRight.PUT) if atm_strike is not None else None
    atm_call_mid = option_mid(atm_call)
    atm_put_mid = option_mid(atm_put)
    straddle = (
        atm_call_mid + atm_put_mid
        if atm_call_mid is not None and atm_put_mid is not None
        else None
    )
    atm_ivs = [iv for iv in (option_iv(atm_call), option_iv(atm_put)) if iv is not None]
    atm_iv = sum(atm_ivs) / len(atm_ivs) if atm_ivs else None

    put_iv_items: list[tuple[float, float]] = []
    call_iv_items: list[tuple[float, float]] = []
    if underlier is not None:
        for quote in quotes:
            strike = finite_float(quote.instrument.strike)
            right = quote.instrument.right
            iv = option_iv(quote)
            if strike is None or right is None or iv is None:
                continue
            weight = max(finite_float(quote.open_interest) or finite_float(quote.volume) or 1.0, 1.0)
            moneyness = strike / underlier
            if right == OptionRight.PUT and 0.97 <= moneyness <= 0.995:
                put_iv_items.append((iv, weight))
            if right == OptionRight.CALL and 1.005 <= moneyness <= 1.03:
                call_iv_items.append((iv, weight))
    put_wing_iv = weighted_mean(put_iv_items)
    call_wing_iv = weighted_mean(call_iv_items)

    gex_rows = build_gex_by_strike(pairs, underlier=underlier) if underlier else []
    net_gex = sum(row.net_gex for row in gex_rows) if gex_rows else None
    abs_gex = sum(row.abs_gex for row in gex_rows) if gex_rows else None
    net_gamma_ratio = net_gex / abs_gex if net_gex is not None and abs_gex and abs_gex > 0 else None
    zero = nearest_zero(gex_rows, underlier) if underlier else None
    zero_distance = zero - underlier if zero is not None and underlier is not None else None
    call_wall_row = max(gex_rows, key=lambda row: row.call_gex) if gex_rows else None
    put_wall_row = min(gex_rows, key=lambda row: row.put_gex) if gex_rows else None
    call_wall = call_wall_row.strike if call_wall_row and call_wall_row.call_gex > 0 else None
    put_wall = put_wall_row.strike if put_wall_row and put_wall_row.put_gex < 0 else None
    walls = [wall for wall in (call_wall, put_wall) if wall is not None]
    nearest_wall_value = min(walls, key=lambda wall: abs(wall - underlier)) if walls and underlier else None
    nearest_wall_distance = nearest_wall_value - underlier if nearest_wall_value is not None and underlier else None
    gex_quality = "open_interest_gex" if gex_rows else "no_open_interest_gex"

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
    if underlier_mismatch:
        warnings.append("underlier mismatch; wall distance and gamma alerts suppressed")

    expected_move_pct = straddle / underlier if straddle is not None and underlier else None
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
    return ExpiryOptionsMap(
        expiry=expiry,
        option_count=len(quotes),
        strike_count=len(strikes),
        atm_strike=atm_strike,
        atm_call_mid=atm_call_mid,
        atm_put_mid=atm_put_mid,
        atm_straddle_mid=straddle,
        expected_move_points=straddle,
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
    )


def build_options_map(state: LatestState) -> OptionsMap:
    underlier = select_underlier(state)
    grouped: dict[str, list[Quote]] = defaultdict(list)
    for quote in state.best_quotes:
        if not is_spxw_option(quote):
            continue
        expiry = quote.instrument.expiry or "unknown"
        grouped[expiry].append(quote)

    warnings: list[str] = []
    underlier_mismatch = underlier.source is not None and underlier.source != "index:SPX"
    if underlier.price is None:
        warnings.append("missing SPX underlier reference")
    elif underlier_mismatch:
        warnings.append(
            f"underlier_mismatch: using {underlier.source} price for SPX strikes; wall/gamma alerts suppressed"
        )
    if not grouped:
        warnings.append("missing SPXW option quotes")

    expiries = tuple(
        build_expiry_map(
            expiry,
            quotes,
            underlier.price,
            as_of=state.as_of,
            underlier_mismatch=underlier_mismatch,
        )
        for expiry, quotes in sorted(grouped.items())
    )
    return OptionsMap(
        created_at=datetime.now(tz=state.as_of.tzinfo),
        as_of=state.as_of,
        underlier=underlier,
        expiries=expiries,
        warnings=tuple(dict.fromkeys(warnings)),
    )


def format_number(value: float | None, digits: int = 2) -> str:
    if value is None:
        return "-"
    return f"{value:.{digits}f}"


def print_options_map(options_map: OptionsMap) -> None:
    print(f"Options map as of: {options_map.as_of.isoformat()}")
    print(f"Underlier: {format_number(options_map.underlier.price)} source={options_map.underlier.source or '-'}")
    if options_map.warnings:
        print("Warnings:")
        for warning in options_map.warnings:
            print(f"- {warning}")
    if not options_map.expiries:
        return
    print("\nExpiry map:")
    headers = [
        "expiry",
        "state",
        "opts",
        "atm",
        "straddle",
        "atm_iv",
        "put_skew",
        "call_skew",
        "zero_g",
        "put_wall",
        "call_wall",
    ]
    rows: list[list[str]] = []
    for item in options_map.expiries:
        rows.append(
            [
                item.expiry,
                item.gamma_state,
                str(item.option_count),
                format_number(item.atm_strike, 0),
                format_number(item.atm_straddle_mid),
                format_number(item.atm_iv, 4),
                format_number(item.put_skew_ratio, 3),
                format_number(item.call_skew_ratio, 3),
                format_number(item.zero_gamma, 0),
                format_number(item.put_wall, 0),
                format_number(item.call_wall, 0),
            ]
        )
    widths = [
        max(len(headers[index]), *(len(row[index]) for row in rows)) for index in range(len(headers))
    ]
    print(" | ".join(header.ljust(widths[index]) for index, header in enumerate(headers)))
    print("-+-".join("-" * width for width in widths))
    for row in rows:
        print(" | ".join(value.ljust(widths[index]) for index, value in enumerate(row)))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the current SPXW options map.")
    parser.add_argument("--json", action="store_true", help="Print JSON.")
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    state = LatestStateStore(StorageSettings.from_env()).load()
    options_map = build_options_map(state)
    if args.json:
        print(json.dumps(options_map.to_dict(), indent=2, sort_keys=True))
    else:
        print_options_map(options_map)
    return 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
