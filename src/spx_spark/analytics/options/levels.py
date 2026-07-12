"""Gamma regime classification and SPY wall confluence."""

from __future__ import annotations

from collections.abc import Sequence

from spx_spark.analytics.options.chain import is_spy_option, pair_by_strike
from spx_spark.analytics.options.exposure import build_gex_by_strike
from spx_spark.analytics.options.models import ExpiryOptionsMap, WallConfluence
from spx_spark.marketdata import Quote


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


def build_spy_confluence(
    quotes: Sequence[Quote],
    front_spxw: ExpiryOptionsMap | None,
    *,
    spy_underlier: float | None = None,
    spx_underlier: float | None = None,
) -> WallConfluence:
    """SPY vs SPXW wall confluence from quote sequences (no storage coupling)."""

    spy_quotes = [quote for quote in quotes if is_spy_option(quote)]
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
        quote for quote in spy_quotes if (quote.instrument.expiry or "unknown") == front_expiry
    ]
    pairs = pair_by_strike(front_quotes)
    gex_rows = build_gex_by_strike(pairs, underlier=spy_underlier)
    call_wall_row = max(gex_rows, key=lambda row: row.call_gex) if gex_rows else None
    put_wall_row = min(gex_rows, key=lambda row: row.put_gex) if gex_rows else None
    spy_call_wall = call_wall_row.strike if call_wall_row and call_wall_row.call_gex > 0 else None
    spy_put_wall = put_wall_row.strike if put_wall_row and put_wall_row.put_gex < 0 else None
    spy_call_wall_spx = spy_call_wall * 10.0 if spy_call_wall is not None else None
    spy_put_wall_spx = spy_put_wall * 10.0 if spy_put_wall is not None else None

    tolerance_reference = spx_underlier if spx_underlier is not None else spy_underlier * 10.0
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
