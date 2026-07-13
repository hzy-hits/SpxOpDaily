"""Open-interest settlement pain and single-side concentration levels."""

from __future__ import annotations

from collections.abc import Mapping

from spx_spark.analytics.options.models import MaxPain
from spx_spark.analytics.options.pricing import finite_float
from spx_spark.marketdata import OptionRight, Quote


def build_max_pain(
    pairs: Mapping[float, Mapping[OptionRight, Quote]],
    *,
    underlier: float | None = None,
) -> MaxPain | None:
    """Compute combined OI max pain plus call/put OI peak strikes.

    The settlement payout is expressed in index points times contracts. The
    standard contract multiplier is deliberately omitted because it is a
    constant and does not change the minimizing strike.
    """

    rows: list[tuple[float, float, float]] = []
    for raw_strike, sides in pairs.items():
        strike = finite_float(raw_strike)
        if strike is None:
            continue
        call = sides.get(OptionRight.CALL)
        put = sides.get(OptionRight.PUT)
        call_oi = max(finite_float(call.open_interest) or 0.0, 0.0) if call else 0.0
        put_oi = max(finite_float(put.open_interest) or 0.0, 0.0) if put else 0.0
        if call_oi > 0 or put_oi > 0:
            rows.append((strike, call_oi, put_oi))

    if not rows:
        return None

    rows.sort(key=lambda row: row[0])
    call_peak = max(rows, key=lambda row: (row[1], -row[0]))
    put_peak = max(rows, key=lambda row: (row[2], -row[0]))

    def payout(settlement: float) -> float:
        return sum(
            call_oi * max(settlement - strike, 0.0)
            + put_oi * max(strike - settlement, 0.0)
            for strike, call_oi, put_oi in rows
        )

    center = finite_float(underlier)
    candidates = [(payout(strike), strike) for strike, _, _ in rows]
    _, settlement = min(
        candidates,
        key=lambda item: (
            item[0],
            abs(item[1] - center) if center is not None else item[1],
        ),
    )
    payout_points = payout(settlement)
    strike_range = (rows[0][0], rows[-1][0])
    quality = "ok" if len(rows) >= 40 else "partial_window"
    return MaxPain(
        settlement_strike=settlement,
        payout_points=payout_points,
        call_oi_peak_strike=call_peak[0],
        call_oi_peak=call_peak[1],
        put_oi_peak_strike=put_peak[0],
        put_oi_peak=put_peak[2],
        call_open_interest=sum(row[1] for row in rows),
        put_open_interest=sum(row[2] for row in rows),
        oi_strike_count=len(rows),
        strike_range=strike_range,
        quality=quality,
    )
