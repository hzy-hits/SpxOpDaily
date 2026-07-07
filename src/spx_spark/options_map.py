from __future__ import annotations

import argparse
import json
import math
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, time as dt_time
from typing import Any

from spx_spark.config import StorageSettings, default_spxw_expiry, NY_TZ
from spx_spark.marketdata import InstrumentType, MarketDataQuality, OptionRight, Provider, ProviderStatus, Quote
from spx_spark.storage import LatestState, LatestStateStore


UNDERLIER_CANDIDATES = (
    ("index:SPX", 1.0),
    ("future:ES", 1.0),
    ("future:MES", 1.0),
    ("equity:SPY", 10.0),
)

UNDERLIER_MISMATCH_SOURCES = frozenset(
    {
        "future:ES",
        "future:MES",
        "equity:SPY",
    }
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
class LevelProbability:
    level_name: str
    level: float
    prob_close_beyond: float | None
    prob_touch: float | None
    source_strike: float | None
    source_delta: float | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class WallConfluence:
    spy_underlier: float | None
    spy_front_expiry: str | None
    spy_call_wall_spx: float | None
    spy_put_wall_spx: float | None
    call_wall_confluent: bool | None
    put_wall_confluent: bool | None
    tolerance_points: float
    spy_option_count: int
    quality: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


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
    level_probabilities: tuple[LevelProbability, ...] = ()
    gamma_flip_zone: tuple[float, float] | None = None
    gex_weighting: str = "oi"
    zero_gamma_method: str = "strike_profile_fallback_no_flip"
    put_skew_25d: float | None = None
    call_skew_25d: float | None = None
    skew_method: str = "moneyness_fallback"


@dataclass(frozen=True)
class OptionsMap:
    created_at: datetime
    as_of: datetime
    underlier: UnderlierReference
    expiries: tuple[ExpiryOptionsMap, ...]
    warnings: tuple[str, ...]
    spy_confluence: WallConfluence | None = None

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


def ibkr_provider_unavailable(state: LatestState) -> bool:
    for provider_state in state.provider_states:
        if provider_state.provider != Provider.IBKR:
            continue
        if provider_state.status == ProviderStatus.UNAVAILABLE:
            return True
        if provider_state.status == ProviderStatus.DEGRADED and provider_state.connected is not True:
            return True
    return False


def select_underlier(state: LatestState) -> UnderlierReference:
    for instrument_id, multiplier in UNDERLIER_CANDIDATES:
        quote = state.best_quote(instrument_id)
        if quote is None or quote.quality in BAD_QUALITIES:
            continue
        price = quote.effective_price
        if price is not None and price > 0:
            return UnderlierReference(price=price * multiplier, source=instrument_id)
    return UnderlierReference(price=None, source=None)


def chain_implied_spot(pairs: dict[float, dict[OptionRight, Quote]]) -> float | None:
    """SPX spot implied by put-call parity at the synthetic ATM strike.

    S ~= K + C(K) - P(K) at the strike where |C - P| is smallest (r~=0 for
    0DTE/1DTE). This is the option market's own SPX-scale reference, so it
    avoids the ES/SPY basis that otherwise forces gamma/wall suppression
    outside SPX cash hours.
    """
    best: tuple[float, float, float, float] | None = None
    for strike, sides in pairs.items():
        call_mid = option_mid(sides.get(OptionRight.CALL))
        put_mid = option_mid(sides.get(OptionRight.PUT))
        if call_mid is None or put_mid is None:
            continue
        diff = abs(call_mid - put_mid)
        if best is None or diff < best[0]:
            best = (diff, strike, call_mid, put_mid)
    if best is None:
        return None
    _, strike, call_mid, put_mid = best
    return strike + call_mid - put_mid


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


def usable_delta(quote: Quote | None) -> float | None:
    if quote is None or quote.quality in BAD_QUALITIES or quote.greeks is None:
        return None
    value = finite_float(quote.greeks.delta)
    if value is None or not math.isfinite(value):
        return None
    return value


def median_strike_step(strikes: list[float]) -> float:
    if len(strikes) < 2:
        return 5.0
    diffs = [strikes[index + 1] - strikes[index] for index in range(len(strikes) - 1)]
    diffs.sort()
    mid = len(diffs) // 2
    if len(diffs) % 2:
        return diffs[mid]
    return (diffs[mid - 1] + diffs[mid]) / 2.0


def probability_for_level(
    level: float,
    *,
    underlier: float,
    pairs: dict[float, dict[OptionRight, Quote]],
    strike_step: float,
) -> tuple[float | None, float | None, float | None, float | None]:
    right = OptionRight.CALL if level >= underlier else OptionRight.PUT
    candidates: list[tuple[float, float, float]] = []
    for strike, pair in pairs.items():
        delta = usable_delta(pair.get(right))
        if delta is None:
            continue
        candidates.append((strike, abs(strike - level), delta))
    if not candidates:
        return (None, None, None, None)
    source_strike, distance, source_delta = min(candidates, key=lambda item: item[1])
    if distance > 2 * strike_step:
        return (None, None, None, None)
    prob_close_beyond = max(0.0, min(1.0, source_delta if right == OptionRight.CALL else abs(source_delta)))
    prob_touch = min(1.0, 2 * prob_close_beyond)
    return (prob_close_beyond, prob_touch, source_strike, source_delta)


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
    intraday: bool = False,
) -> list[StrikeGex]:
    rows: list[StrikeGex] = []
    for strike, pair in sorted(pairs.items()):
        call = pair.get(OptionRight.CALL)
        put = pair.get(OptionRight.PUT)
        call_gex = (
            signed_gex(call, sign=1.0, underlier=underlier, intraday=intraday) if call is not None else None
        )
        put_gex = (
            signed_gex(put, sign=-1.0, underlier=underlier, intraday=intraday) if put is not None else None
        )
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
                call_open_interest=(finite_float(call.open_interest) or 0.0) if call else 0.0,
                put_open_interest=(finite_float(put.open_interest) or 0.0) if put else 0.0,
            )
        )
    return rows


def gex_weight(quote: Quote, *, intraday: bool) -> float | None:
    """非 0DTE: OI(现行为)。0DTE(intraday=True): OI + volume。

    OI/volume 缺失按 0;两者都缺或 <=0 返回 None。volume 近似当日新开仓
    (也含平仓,是有意的粗近似)。
    """
    open_interest = finite_float(quote.open_interest) or 0.0
    volume = finite_float(quote.volume) or 0.0
    if intraday:
        weight = open_interest + volume
    else:
        weight = open_interest
    if weight <= 0:
        return None
    return weight


def signed_gex(quote: Quote, *, sign: float, underlier: float, intraday: bool = False) -> float | None:
    gamma = option_gamma(quote)
    weight = gex_weight(quote, intraday=intraday)
    if gamma is None or weight is None:
        return None
    return sign * gamma * weight * 100.0 * underlier * underlier * 0.01


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


def zero_gamma_bracket(gex_rows: list[StrikeGex], underlier: float) -> tuple[float, float] | None:
    if not gex_rows:
        return None
    brackets: list[tuple[float, float, float]] = []
    for left, right in zip(gex_rows, gex_rows[1:]):
        if abs(left.net_gex) <= 1e-12:
            brackets.append((left.strike, left.strike, abs(left.strike - underlier)))
        elif left.net_gex * right.net_gex < 0:
            zero = interpolate_zero(left, right)
            if zero is not None:
                brackets.append((left.strike, right.strike, abs(zero - underlier)))
    if abs(gex_rows[-1].net_gex) <= 1e-12:
        last = gex_rows[-1].strike
        brackets.append((last, last, abs(last - underlier)))
    if not brackets:
        return None
    left_strike, right_strike, _distance = min(brackets, key=lambda item: item[2])
    return (left_strike, right_strike)


def bs_gamma(spot: float, strike: float, iv: float, t_years: float) -> float | None:
    """Black-Scholes gamma,r=q=0."""
    if spot <= 0 or strike <= 0 or iv <= 0 or t_years <= 0:
        return None
    sqrt_t = math.sqrt(t_years)
    d1 = (math.log(spot / strike) + 0.5 * iv * iv * t_years) / (iv * sqrt_t)
    phi = math.exp(-0.5 * d1 * d1) / math.sqrt(2.0 * math.pi)
    return phi / (spot * iv * sqrt_t)


_MIN_TIME_TO_EXPIRY_YEARS = 15.0 / (60.0 * 24.0 * 365.0)


def time_to_expiry_years(expiry: str, *, as_of: datetime) -> float:
    """Years from as_of to expiry at 16:00 ET (365-day year), floored at 15 minutes."""
    expiry_date = datetime.strptime(expiry, "%Y%m%d").date()
    expiry_dt = datetime.combine(expiry_date, dt_time(16, 0), tzinfo=NY_TZ)
    as_of_ny = as_of.astimezone(NY_TZ)
    delta_seconds = (expiry_dt - as_of_ny).total_seconds()
    if delta_seconds <= 0:
        return _MIN_TIME_TO_EXPIRY_YEARS
    years = delta_seconds / (365.0 * 24.0 * 3600.0)
    return max(years, _MIN_TIME_TO_EXPIRY_YEARS)


def zero_gamma_spot_scan(
    pairs: dict[float, dict[OptionRight, Quote]],
    *,
    underlier: float,
    expiry: str,
    as_of: datetime,
    intraday: bool,
) -> tuple[float | None, tuple[float, float] | None, str]:
    """Return (zero_gamma, flip_zone, method) via spot scan with BS gamma revaluation."""
    contracts: list[tuple[float, float, float, float]] = []
    total_legs = 0
    for strike, pair in pairs.items():
        for right, sign in ((OptionRight.CALL, 1.0), (OptionRight.PUT, -1.0)):
            quote = pair.get(right)
            if quote is None:
                continue
            total_legs += 1
            iv = option_iv(quote)
            weight = gex_weight(quote, intraday=intraday)
            if iv is None or weight is None:
                continue
            contracts.append((strike, sign, iv, weight))

    if total_legs == 0 or len(contracts) / total_legs < 0.6 or len(contracts) < 4:
        return (None, None, "insufficient_iv")

    strikes = sorted(pairs)
    step = min(median_strike_step(strikes), 5.0)
    t_years = time_to_expiry_years(expiry, as_of=as_of)

    def net_gex_at(spot: float) -> float:
        total = 0.0
        for strike, sign, iv, weight in contracts:
            gamma = bs_gamma(spot, strike, iv, t_years)
            if gamma is None:
                continue
            total += sign * weight * gamma * 100.0 * spot * spot * 0.01
        return total

    grid: list[float] = []
    spot = strikes[0]
    while spot <= strikes[-1] + 1e-9:
        grid.append(spot)
        spot += step

    values = [net_gex_at(s) for s in grid]
    roots: list[tuple[float, tuple[float, float]]] = []
    for index in range(len(grid) - 1):
        left_s, right_s = grid[index], grid[index + 1]
        left_v, right_v = values[index], values[index + 1]
        if abs(left_v) <= 1e-12:
            roots.append((left_s, (left_s, right_s)))
        elif left_v * right_v < 0:
            weight = -left_v / (right_v - left_v)
            root = left_s + weight * (right_s - left_s)
            roots.append((root, (left_s, right_s)))

    if abs(values[-1]) <= 1e-12:
        roots.append((grid[-1], (grid[-1], grid[-1])))

    if not roots:
        return (None, None, "no_flip")

    zero_gamma, flip_zone = min(roots, key=lambda item: abs(item[0] - underlier))
    return (zero_gamma, flip_zone, "spot_scan")


def interpolated_atm_iv(
    pairs: dict[float, dict[OptionRight, Quote]],
    underlier: float | None,
) -> float | None:
    """Linearly interpolate ATM IV on each side; average call and put."""
    if underlier is None:
        return None

    def side_iv(right: OptionRight) -> float | None:
        strikes_with_iv: list[tuple[float, float]] = []
        for strike, pair in pairs.items():
            iv = option_iv(pair.get(right))
            if iv is not None:
                strikes_with_iv.append((strike, iv))
        if not strikes_with_iv:
            return None
        below = [(strike, iv) for strike, iv in strikes_with_iv if strike <= underlier]
        above = [(strike, iv) for strike, iv in strikes_with_iv if strike >= underlier]
        if below and above:
            strike_low, iv_low = max(below, key=lambda item: item[0])
            strike_high, iv_high = min(above, key=lambda item: item[0])
            if strike_high == strike_low:
                return iv_low
            weight = (underlier - strike_low) / (strike_high - strike_low)
            return iv_low + weight * (iv_high - iv_low)
        nearest_strike, nearest_iv = min(strikes_with_iv, key=lambda item: abs(item[0] - underlier))
        return nearest_iv

    ivs = [iv for iv in (side_iv(OptionRight.CALL), side_iv(OptionRight.PUT)) if iv is not None]
    if not ivs:
        return None
    return sum(ivs) / len(ivs)


def wing_iv_at_delta(quotes_one_side: list[Quote], target_abs_delta: float = 0.25) -> float | None:
    """Return IV of the quote whose |delta| is closest to target, if within 0.15."""
    candidates: list[tuple[float, float]] = []
    for quote in quotes_one_side:
        delta = usable_delta(quote)
        iv = option_iv(quote)
        if delta is None or iv is None:
            continue
        distance = abs(abs(delta) - target_abs_delta)
        candidates.append((distance, iv))
    if not candidates:
        return None
    distance, iv = min(candidates, key=lambda item: item[0])
    if distance > 0.15:
        return None
    return iv


def is_spy_option(quote: Quote) -> bool:
    instrument = quote.instrument
    if instrument.instrument_type != InstrumentType.OPTION:
        return False
    return (instrument.underlier or instrument.symbol).upper() == "SPY"


def build_spy_confluence(
    state: LatestState,
    front_spxw: ExpiryOptionsMap | None,
) -> WallConfluence:
    spy_quotes = [quote for quote in state.best_quotes if is_spy_option(quote)]
    if not spy_quotes:
        return WallConfluence(
            spy_underlier=None,
            spy_front_expiry=None,
            spy_call_wall_spx=None,
            spy_put_wall_spx=None,
            call_wall_confluent=None,
            put_wall_confluent=None,
            tolerance_points=10.0,
            spy_option_count=0,
            quality="missing_spy_chain",
        )

    spy_underlier_quote = state.best_quote("equity:SPY")
    spy_underlier = (
        spy_underlier_quote.effective_price
        if spy_underlier_quote is not None
        else None
    )
    if spy_underlier is None or spy_underlier <= 0:
        return WallConfluence(
            spy_underlier=None,
            spy_front_expiry=None,
            spy_call_wall_spx=None,
            spy_put_wall_spx=None,
            call_wall_confluent=None,
            put_wall_confluent=None,
            tolerance_points=10.0,
            spy_option_count=len(spy_quotes),
            quality="missing_spy_underlier",
        )

    front_expiry = min(
        (quote.instrument.expiry or "unknown" for quote in spy_quotes),
        default=None,
    )
    front_quotes = [
        quote
        for quote in spy_quotes
        if (quote.instrument.expiry or "unknown") == front_expiry
    ]
    pairs = pair_by_strike(front_quotes)
    gex_rows = build_gex_by_strike(pairs, underlier=spy_underlier)
    call_wall_row = max(gex_rows, key=lambda row: row.call_gex) if gex_rows else None
    put_wall_row = min(gex_rows, key=lambda row: row.put_gex) if gex_rows else None
    spy_call_wall = (
        call_wall_row.strike if call_wall_row and call_wall_row.call_gex > 0 else None
    )
    spy_put_wall = put_wall_row.strike if put_wall_row and put_wall_row.put_gex < 0 else None
    spy_call_wall_spx = spy_call_wall * 10.0 if spy_call_wall is not None else None
    spy_put_wall_spx = spy_put_wall * 10.0 if spy_put_wall is not None else None

    spx_quote = state.best_quote("index:SPX")
    spxw_underlier = spx_quote.effective_price if spx_quote is not None else None
    tolerance_reference = spxw_underlier if spxw_underlier is not None else spy_underlier * 10.0
    tolerance = max(10.0, tolerance_reference * 0.0015)

    call_wall_confluent: bool | None = None
    put_wall_confluent: bool | None = None
    if front_spxw is not None:
        if spy_call_wall_spx is not None and front_spxw.call_wall is not None:
            call_wall_confluent = abs(spy_call_wall_spx - front_spxw.call_wall) <= tolerance
        if spy_put_wall_spx is not None and front_spxw.put_wall is not None:
            put_wall_confluent = abs(spy_put_wall_spx - front_spxw.put_wall) <= tolerance

    return WallConfluence(
        spy_underlier=spy_underlier,
        spy_front_expiry=front_expiry,
        spy_call_wall_spx=spy_call_wall_spx,
        spy_put_wall_spx=spy_put_wall_spx,
        call_wall_confluent=call_wall_confluent,
        put_wall_confluent=put_wall_confluent,
        tolerance_points=tolerance,
        spy_option_count=len(spy_quotes),
        quality="ok",
    )


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
            weight = max(finite_float(quote.open_interest) or finite_float(quote.volume) or 1.0, 1.0)
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
        put_skew_25d = put_wing_iv - atm_iv if put_wing_iv is not None and atm_iv is not None else None
        call_skew_25d = call_wing_iv - atm_iv if call_wing_iv is not None and atm_iv is not None else None

    intraday = expiry == default_spxw_expiry()
    gex_weighting = "oi_plus_volume" if intraday else "oi"
    gex_rows = (
        build_gex_by_strike(pairs, underlier=underlier, intraday=intraday) if underlier else []
    )
    net_gex = sum(row.net_gex for row in gex_rows) if gex_rows else None
    abs_gex = sum(row.abs_gex for row in gex_rows) if gex_rows else None
    net_gamma_ratio = net_gex / abs_gex if net_gex is not None and abs_gex and abs_gex > 0 else None
    zg_method = "strike_profile_fallback_no_flip"
    zero = None
    gamma_flip_zone = None
    if underlier:
        zg_scan, flip_scan, scan_method = zero_gamma_spot_scan(
            pairs,
            underlier=underlier,
            expiry=expiry,
            as_of=as_of,
            intraday=intraday,
        )
        if zg_scan is not None:
            zero = zg_scan
            gamma_flip_zone = flip_scan
            zg_method = scan_method
        else:
            zero = nearest_zero(gex_rows, underlier)
            gamma_flip_zone = zero_gamma_bracket(gex_rows, underlier)
            zg_method = f"strike_profile_fallback_{scan_method}"
    else:
        zero = None
        gamma_flip_zone = None
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

    # ATM straddle ≈ 1.25σ; industry 1σ approximation ≈ 0.85×straddle.
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

    strike_step = median_strike_step(strikes)
    level_probabilities: list[LevelProbability] = []
    if underlier is not None:
        for level_name, level_value in (
            ("put_wall", put_wall),
            ("zero_gamma", zero),
            ("call_wall", call_wall),
        ):
            if level_value is None:
                continue
            prob_close, prob_touch, source_strike, source_delta = probability_for_level(
                level_value,
                underlier=underlier,
                pairs=pairs,
                strike_step=strike_step,
            )
            level_probabilities.append(
                LevelProbability(
                    level_name=level_name,
                    level=level_value,
                    prob_close_beyond=prob_close,
                    prob_touch=prob_touch,
                    source_strike=source_strike,
                    source_delta=source_delta,
                )
            )

    return ExpiryOptionsMap(
        expiry=expiry,
        option_count=len(quotes),
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
    )


def group_spxw_option_quotes(state: LatestState) -> dict[str, list[Quote]]:
    ibkr_down = ibkr_provider_unavailable(state)
    use_ibkr_only = not ibkr_down and any(
        is_spxw_option(quote) and quote.provider == Provider.IBKR for quote in state.best_quotes
    )
    grouped: dict[str, list[Quote]] = defaultdict(list)
    for quote in state.best_quotes:
        if not is_spxw_option(quote):
            continue
        if quote.provider == Provider.IBKR and ibkr_down:
            continue
        if use_ibkr_only and quote.provider != Provider.IBKR:
            continue
        expiry = quote.instrument.expiry or "unknown"
        grouped[expiry].append(quote)
    return grouped


def build_options_map(state: LatestState) -> OptionsMap:
    underlier = select_underlier(state)
    grouped = group_spxw_option_quotes(state)

    warnings: list[str] = []
    underlier_mismatch = (
        underlier.source is not None and underlier.source in UNDERLIER_MISMATCH_SOURCES
    )
    if (underlier.price is None or underlier_mismatch) and grouped:
        # Outside SPX cash hours the reference degrades to ES/SPY, whose basis
        # forced gamma/wall suppression. Put-call parity on the front expiry
        # gives an SPX-consistent spot, so gamma/GEX stay live around the clock.
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
            f"underlier_mismatch: using {underlier.source} price for SPX strikes; wall/gamma alerts suppressed"
        )
    if not grouped:
        warnings.append("missing SPXW option quotes")
    if ibkr_provider_unavailable(state):
        warnings.append("IBKR feed unavailable; stale SPXW option quotes suppressed")

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
    front_spxw = expiries[0] if expiries else None
    spy_confluence = build_spy_confluence(state, front_spxw)
    return OptionsMap(
        created_at=datetime.now(tz=state.as_of.tzinfo),
        as_of=state.as_of,
        underlier=underlier,
        expiries=expiries,
        warnings=tuple(dict.fromkeys(warnings)),
        spy_confluence=spy_confluence,
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
