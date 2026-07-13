"""Composable completeness evaluators for post-close review inputs."""

from __future__ import annotations

import math
from datetime import datetime

from spx_spark.iv_surface import IvSurfaceExpiry, IvSurfaceSnapshot
from spx_spark.market_calendar import ET, MarketSession
from spx_spark.marketdata import InstrumentType, MarketDataQuality, Quote
from spx_spark.post_close_review import (
    ReviewCompletenessCheck,
    ReviewCompletenessPolicy,
    _five_minute_bucket_count,
    _gap_minutes,
    _inside_session,
    _ratio_check,
    _usable_quote,
    finite_price,
)


def evaluate_review_completeness(
    *,
    session: MarketSession,
    spx_quotes: list[Quote],
    es_quotes: list[Quote],
    quotes: tuple[Quote, ...],
    snapshots: tuple[IvSurfaceSnapshot, ...],
    policy: ReviewCompletenessPolicy,
) -> tuple[ReviewCompletenessCheck, ...]:
    return (
        *evaluate_index_completeness(session, spx_quotes, "SPX", policy),
        *evaluate_index_completeness(session, es_quotes, "ES", policy),
        *evaluate_option_completeness(session, quotes, policy),
        *evaluate_surface_completeness(session, snapshots, policy),
    )


def evaluate_index_completeness(
    session: MarketSession,
    series: list[Quote],
    label: str,
    policy: ReviewCompletenessPolicy,
) -> tuple[ReviewCompletenessCheck, ...]:
    price_rows = [quote for quote in series if finite_price(quote) is not None]
    usable = [quote for quote in price_rows if _usable_quote(quote)]
    usable_times = sorted(quote.received_at for quote in usable)
    bucket_count = _five_minute_bucket_count(usable_times, session)
    live_rows = sum(
        quote.quality == MarketDataQuality.LIVE and finite_price(quote) is not None
        for quote in series
    )
    first_at = usable_times[0].astimezone(ET) if usable_times else None
    last_at = usable_times[-1].astimezone(ET) if usable_times else None
    return (
        _ratio_check(
            name=f"{label.lower()}_five_minute_bucket_coverage",
            numerator=bucket_count,
            denominator=session.expected_five_minute_buckets,
            threshold=policy.min_index_bucket_ratio,
            label=f"{label} five-minute buckets",
        ),
        edge_gap_check(
            name=f"{label.lower()}_first_observation_gap_minutes",
            observed_at=first_at,
            edge_at=session.open_at,
            session=session,
            threshold=policy.max_edge_gap_minutes,
            label=f"{label} first usable observation gap",
            missing_reason=f"{label} has no usable live/frozen observation",
        ),
        edge_gap_check(
            name=f"{label.lower()}_last_observation_gap_minutes",
            observed_at=last_at,
            edge_at=session.close_at,
            session=session,
            threshold=policy.max_edge_gap_minutes,
            label=f"{label} last usable observation gap",
            missing_reason=f"{label} has no usable live/frozen observation",
        ),
        _ratio_check(
            name=f"{label.lower()}_live_ratio",
            numerator=live_rows,
            denominator=len(series),
            threshold=policy.min_index_live_ratio,
            label=f"{label} live observations",
        ),
    )


def evaluate_option_completeness(
    session: MarketSession,
    quotes: tuple[Quote, ...],
    policy: ReviewCompletenessPolicy,
) -> tuple[ReviewCompletenessCheck, ...]:
    expiry = session.trading_date.strftime("%Y%m%d")
    rows = [
        quote
        for quote in quotes
        if quote.instrument.instrument_type == InstrumentType.OPTION
        and quote.instrument.expiry == expiry
        and _inside_session(quote.received_at, session)
    ]
    usable = [quote for quote in rows if _usable_quote(quote)]
    contracts = {quote.instrument.canonical_id for quote in usable}
    strikes = {
        float(quote.instrument.strike) for quote in usable if quote.instrument.strike is not None
    }
    rights = {
        quote.instrument.right.value for quote in usable if quote.instrument.right is not None
    }
    iv_rows = sum(
        quote.greeks is not None
        and quote.greeks.implied_vol is not None
        and math.isfinite(quote.greeks.implied_vol)
        and quote.greeks.implied_vol > 0
        for quote in usable
    )
    last_at = max((quote.received_at for quote in usable), default=None)
    metrics = (
        count_check(
            "front_option_unique_contracts",
            len(contracts),
            policy.min_front_option_contracts,
            "unique front-expiry contracts",
        ),
        count_check(
            "front_option_unique_strikes",
            len(strikes),
            policy.min_front_option_strikes,
            "unique front-expiry strikes",
        ),
        count_check(
            "front_option_strike_span",
            max(strikes) - min(strikes) if strikes else 0.0,
            policy.min_front_option_strike_span,
            "front-expiry strike span",
        ),
    )
    return (
        *metrics,
        ReviewCompletenessCheck(
            name="front_option_call_put_coverage",
            measured=tuple(sorted(rights)),
            threshold=("C", "P"),
            passed={"C", "P"}.issubset(rights),
            reason=(
                f"front-expiry rights present: {','.join(sorted(rights)) or 'none'}; required C and P"
            ),
        ),
        _ratio_check(
            name="front_option_usable_ratio",
            numerator=len(usable),
            denominator=len(rows),
            threshold=policy.min_option_usable_ratio,
            label="usable live/frozen front-expiry option rows",
        ),
        _ratio_check(
            name="front_option_iv_coverage_ratio",
            numerator=iv_rows,
            denominator=len(rows),
            threshold=policy.min_option_iv_ratio,
            label="front-expiry option rows with usable IV",
        ),
        edge_gap_check(
            name="front_option_last_observation_gap_minutes",
            observed_at=last_at.astimezone(ET) if last_at else None,
            edge_at=session.close_at,
            session=session,
            threshold=policy.max_edge_gap_minutes,
            label="front-expiry last usable option gap",
            missing_reason="front expiry has no usable option observation",
        ),
    )


def evaluate_surface_completeness(
    session: MarketSession,
    snapshots: tuple[IvSurfaceSnapshot, ...],
    policy: ReviewCompletenessPolicy,
) -> tuple[ReviewCompletenessCheck, ...]:
    expiry = session.trading_date.strftime("%Y%m%d")
    surfaces: list[tuple[datetime, IvSurfaceExpiry]] = []
    for snapshot in snapshots:
        if _inside_session(snapshot.as_of, session):
            row = next((item for item in snapshot.expiries if item.expiry == expiry), None)
            if row is not None:
                surfaces.append((snapshot.as_of, row))
    surfaces.sort(key=lambda item: item[0])
    latest = surfaces[-1][1] if surfaces else None
    last_at = surfaces[-1][0].astimezone(ET) if surfaces else None
    bucket_count = _five_minute_bucket_count([stamp for stamp, _ in surfaces], session)
    return (
        _ratio_check(
            name="front_iv_surface_five_minute_bucket_coverage",
            numerator=bucket_count,
            denominator=session.expected_five_minute_buckets,
            threshold=policy.min_surface_bucket_ratio,
            label="front-expiry IV surface five-minute buckets",
        ),
        edge_gap_check(
            name="front_iv_surface_last_observation_gap_minutes",
            observed_at=last_at,
            edge_at=session.close_at,
            session=session,
            threshold=policy.max_edge_gap_minutes,
            label="front-expiry IV surface last gap",
            missing_reason="front expiry has no IV surface observation",
        ),
        ratio_value_check(
            "latest_front_iv_coverage_ratio",
            latest.iv_coverage_ratio if latest else None,
            policy.min_surface_iv_ratio,
            "latest front-expiry surface IV coverage",
        ),
        ratio_value_check(
            "latest_front_gamma_coverage_ratio",
            latest.gamma_coverage_ratio if latest else None,
            policy.min_surface_gamma_ratio,
            "latest front-expiry surface gamma coverage",
        ),
    )


def edge_gap_check(
    *,
    name: str,
    observed_at: datetime | None,
    edge_at: datetime,
    session: MarketSession,
    threshold: float,
    label: str,
    missing_reason: str,
) -> ReviewCompletenessCheck:
    gap = (
        _gap_minutes(edge_at, observed_at)
        if edge_at <= (observed_at or edge_at)
        else _gap_minutes(observed_at, edge_at)
    )
    passed = (
        gap is not None
        and observed_at is not None
        and session.open_at <= observed_at <= session.close_at
        and gap <= threshold
    )
    return ReviewCompletenessCheck(
        name=name,
        measured=round(gap, 6) if gap is not None else None,
        threshold=threshold,
        passed=passed,
        reason=f"{label}: {gap:.1f} minutes; required <= {threshold:g}"
        if gap is not None
        else missing_reason,
    )


def count_check(
    name: str, measured: float, threshold: float, label: str
) -> ReviewCompletenessCheck:
    return ReviewCompletenessCheck(
        name=name,
        measured=measured,
        threshold=threshold,
        passed=measured >= threshold,
        reason=f"{label}: {measured:g}; required >= {threshold:g}",
    )


def ratio_value_check(
    name: str,
    measured: float | None,
    threshold: float,
    label: str,
) -> ReviewCompletenessCheck:
    passed = measured is not None and math.isfinite(measured) and measured >= threshold
    return ReviewCompletenessCheck(
        name=name,
        measured=round(measured, 6) if measured is not None else None,
        threshold=threshold,
        passed=passed,
        reason=(
            f"{label}: {measured:.1%}; required >= {threshold:.1%}"
            if measured is not None
            else f"{label}: missing; required >= {threshold:.1%}"
        ),
    )
