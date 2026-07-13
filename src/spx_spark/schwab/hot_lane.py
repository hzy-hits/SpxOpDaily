"""Select concrete Schwab front-expiry option symbols for batched quotes."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime

from spx_spark.marketdata import OptionRight, Quote, as_utc


@dataclass(frozen=True)
class HotLanePlan:
    expiry: str
    reference_spot: float
    symbols: tuple[str, ...]
    pair_count: int


def option_symbol_budget(
    *,
    context_symbol_count: int,
    max_batch_size: int = 500,
    reserve: int = 10,
) -> int:
    if context_symbol_count < 0 or reserve < 0:
        raise ValueError("symbol counts cannot be negative")
    return max(max_batch_size - context_symbol_count - reserve, 0)


def hot_plan_is_fresh(
    *,
    hot_expiry: str | None,
    expected_expiry: str,
    planned_at: datetime | None,
    now: datetime,
    max_age_seconds: float,
) -> bool:
    if hot_expiry != expected_expiry or planned_at is None or max_age_seconds <= 0:
        return False
    age_seconds = (as_utc(now) - as_utc(planned_at)).total_seconds()
    return 0 <= age_seconds <= max_age_seconds


def select_hot_lane(
    quotes: tuple[Quote, ...],
    *,
    expiry: str,
    spot: float,
    symbol_budget: int,
) -> HotLanePlan:
    pairs: dict[float, dict[OptionRight, str]] = defaultdict(dict)
    for quote in quotes:
        instrument = quote.instrument
        if instrument.expiry != expiry or instrument.strike is None or instrument.right is None:
            continue
        symbol = quote.provider_symbol or instrument.provider_symbol
        if symbol:
            pairs[float(instrument.strike)][instrument.right] = symbol
    complete = [
        (strike, sides[OptionRight.CALL], sides[OptionRight.PUT])
        for strike, sides in pairs.items()
        if OptionRight.CALL in sides and OptionRight.PUT in sides
    ]
    complete.sort(key=lambda item: (abs(item[0] - spot), item[0]))
    max_pairs = max(symbol_budget // 2, 0)
    selected = complete[:max_pairs]
    symbols = tuple(symbol for _strike, call, put in selected for symbol in (call, put))
    return HotLanePlan(
        expiry=expiry,
        reference_spot=spot,
        symbols=symbols,
        pair_count=len(selected),
    )
