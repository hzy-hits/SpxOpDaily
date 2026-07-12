"""GEX/DEX strike profiles, wall ladder helpers, and zero-gamma spot scan."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from spx_spark.analytics.options.chain import median_strike_step
from spx_spark.analytics.options.exposure_types import StrikeGex, WallLevel
from spx_spark.analytics.options.pricing import (
    bs_gamma,
    finite_float,
    option_iv,
    time_to_expiry_years,
)
from spx_spark.analytics.options.quality import option_gamma_structural
from spx_spark.marketdata import OptionRight, Quote

WALL_LADDER_DEPTH = 4


def interpolate_zero(left: StrikeGex, right: StrikeGex) -> float | None:
    denom = right.net_gex - left.net_gex
    if abs(denom) <= 1e-12:
        return None
    weight = -left.net_gex / denom
    if weight < 0 or weight > 1:
        return None
    return left.strike + weight * (right.strike - left.strike)


def gex_weight(quote: Quote, *, intraday: bool) -> float | None:
    open_interest = finite_float(quote.open_interest) or 0.0
    volume = finite_float(quote.volume) or 0.0
    if intraday:
        weight = open_interest + volume
    else:
        weight = open_interest
    if weight <= 0:
        return None
    return weight


def signed_gex(
    quote: Quote, *, sign: float, underlier: float, intraday: bool = False
) -> float | None:
    gamma = option_gamma_structural(quote)
    weight = gex_weight(quote, intraday=intraday)
    if gamma is None or weight is None:
        return None
    return sign * gamma * weight * 100.0 * underlier * underlier * 0.01


def build_gex_by_strike(
    pairs: dict[float, dict[Any, Quote]],
    *,
    underlier: float,
    intraday: bool = False,
) -> list[StrikeGex]:
    rows: list[StrikeGex] = []
    for strike, pair in sorted(pairs.items()):
        call = pair.get(OptionRight.CALL)
        put = pair.get(OptionRight.PUT)
        call_gex = (
            signed_gex(call, sign=1.0, underlier=underlier, intraday=intraday)
            if call is not None
            else None
        )
        put_gex = (
            signed_gex(put, sign=-1.0, underlier=underlier, intraday=intraday)
            if put is not None
            else None
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
                call_volume=(finite_float(call.volume) or 0.0) if call else 0.0,
                put_volume=(finite_float(put.volume) or 0.0) if put else 0.0,
            )
        )
    return rows


def build_wall_ladder(
    gex_rows: list[StrikeGex],
    *,
    underlier: float,
    strike_step: float,
    depth: int = WALL_LADDER_DEPTH,
) -> tuple[tuple[WallLevel, ...], tuple[WallLevel, ...]]:
    tolerance = strike_step / 2.0
    call_rows = [
        row for row in gex_rows if row.call_gex > 0 and row.strike >= underlier - tolerance
    ]
    put_rows = [row for row in gex_rows if row.put_gex < 0 and row.strike <= underlier + tolerance]
    call_rows.sort(key=lambda row: -row.call_gex)
    put_rows.sort(key=lambda row: row.put_gex)
    call_walls = tuple(
        WallLevel(
            strike=row.strike,
            side="call",
            gex=row.call_gex,
            open_interest=row.call_open_interest,
            volume=row.call_volume,
            distance_points=row.strike - underlier,
        )
        for row in call_rows[:depth]
    )
    put_walls = tuple(
        WallLevel(
            strike=row.strike,
            side="put",
            gex=row.put_gex,
            open_interest=row.put_open_interest,
            volume=row.put_volume,
            distance_points=row.strike - underlier,
        )
        for row in put_rows[:depth]
    )
    return call_walls, put_walls


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
