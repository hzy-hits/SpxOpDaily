"""Level close/touch probability from delta anchors."""

from __future__ import annotations

from spx_spark.analytics.options.pricing import usable_delta
from spx_spark.marketdata import OptionRight, Quote


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
    prob_close_beyond = max(
        0.0, min(1.0, source_delta if right == OptionRight.CALL else abs(source_delta))
    )
    prob_touch = min(1.0, 2 * prob_close_beyond)
    return (prob_close_beyond, prob_touch, source_strike, source_delta)
