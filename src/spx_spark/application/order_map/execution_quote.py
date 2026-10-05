"""Execution-grade option quote gates for conditional repricing."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import StrEnum
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from spx_spark.analytics.options.pricing import usable_delta
from spx_spark.application.market_features.market import quote_source_at
from spx_spark.application.order_map.strategy_regime import StrategyPolicy
from spx_spark.marketdata import MarketDataQuality, Provider, Quote
from spx_spark.settings.order_map import DEFAULT_ORDER_MAP_POLICY, OrderMapPolicy
from spx_spark.storage import LatestState, configured_quote_use_decision


MAX_FUTURE_TIMESTAMP_SKEW_SECONDS = 5.0


class ExecutionQuoteStatus(StrEnum):
    EXECUTABLE = "executable"
    RANGE_ONLY = "range_only"


@dataclass(frozen=True)
class ExecutionQuoteGate:
    status: ExecutionQuoteStatus
    reasons: tuple[str, ...]
    mid: float | None
    bid: float | None
    ask: float | None
    spread_points: float | None
    spread_bps: float | None
    spread_percentile: float | None
    transport_age_seconds: float | None
    source_age_seconds: float | None
    provider_mid_divergence_bps: float | None
    providers: tuple[str, ...]
    excluded_providers: tuple[str, ...]

    @property
    def executable(self) -> bool:
        return self.status is ExecutionQuoteStatus.EXECUTABLE

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["status"] = self.status.value
        return payload


def evaluate_execution_quote(
    quote: Quote,
    all_quotes: Iterable[Quote],
    *,
    as_of: datetime,
    policy: OrderMapPolicy = DEFAULT_ORDER_MAP_POLICY,
) -> ExecutionQuoteGate:
    """Fail closed when the market mid is unsuitable as a model anchor."""

    reasons: list[str] = []
    decision = configured_quote_use_decision(quote, as_of=as_of)
    if not decision.pricing_allowed:
        reasons.append(f"quote_not_actionable:{decision.reason}")
    if quote.bid is None or quote.ask is None or quote.mid is None:
        reasons.append("not_two_sided")

    spread_points = quote.spread
    spread_bps = quote.spread_bps
    if spread_points is None or spread_points > policy.execution_max_spread_points:
        reasons.append("spread_points_exceeded")
    if spread_bps is None or spread_bps > policy.execution_max_spread_bps:
        reasons.append("spread_bps_exceeded")

    comparable = [
        item.spread_bps
        for item in all_quotes
        if item.instrument.expiry == quote.instrument.expiry
        and item.instrument.right == quote.instrument.right
        and item.spread_bps is not None
    ]
    spread_percentile = _percentile_rank(spread_bps, comparable)
    if (
        spread_percentile is not None
        and len(comparable) >= 5
        and spread_percentile > policy.execution_max_spread_percentile
    ):
        reasons.append("spread_percentile_exceeded")

    transport_at = quote.last_update_at or quote.received_at
    source_at = quote.quote_time
    transport_age = _age_seconds(as_of, transport_at)
    source_age = _age_seconds(as_of, source_at)
    if transport_age is None or transport_age > policy.execution_max_quote_age_seconds:
        reasons.append("transport_quote_stale")
    elif transport_age < -MAX_FUTURE_TIMESTAMP_SKEW_SECONDS:
        reasons.append("execution_quote_transport_timestamp_in_future")
    if source_age is None or source_age > policy.execution_max_source_age_seconds:
        reasons.append("source_quote_stale_or_unverified")
    elif source_age < -MAX_FUTURE_TIMESTAMP_SKEW_SECONDS:
        reasons.append("execution_quote_source_timestamp_in_future")

    provider_mids: dict[str, float] = {}
    excluded_providers: list[str] = []
    for item in all_quotes:
        if item.instrument.canonical_id != quote.instrument.canonical_id or item.mid is None:
            continue
        exclusion = _provider_quote_exclusion(
            item,
            reference=quote,
            as_of=as_of,
            policy=policy,
        )
        if exclusion is not None:
            excluded_providers.append(f"{item.provider.value}:{exclusion}")
            continue
        provider_mids[item.provider.value] = item.mid
    divergence = _mid_divergence_bps(tuple(provider_mids.values()))
    if divergence is not None and divergence > policy.execution_max_provider_mid_divergence_bps:
        reasons.append("provider_mid_divergence_exceeded")

    unique_reasons = tuple(dict.fromkeys(reasons))
    return ExecutionQuoteGate(
        status=(
            ExecutionQuoteStatus.RANGE_ONLY if unique_reasons else ExecutionQuoteStatus.EXECUTABLE
        ),
        reasons=unique_reasons,
        mid=quote.mid,
        bid=quote.bid,
        ask=quote.ask,
        spread_points=spread_points,
        spread_bps=spread_bps,
        spread_percentile=spread_percentile,
        transport_age_seconds=transport_age,
        source_age_seconds=source_age,
        provider_mid_divergence_bps=divergence,
        providers=tuple(sorted(provider_mids)),
        excluded_providers=tuple(sorted(set(excluded_providers))),
    )


def _age_seconds(as_of: datetime, value: datetime | None) -> float | None:
    if value is None:
        return None
    now = _utc(as_of)
    return (now - _utc(value)).total_seconds()


def _provider_quote_exclusion(
    quote: Quote,
    *,
    reference: Quote,
    as_of: datetime,
    policy: OrderMapPolicy,
) -> str | None:
    """Reject stale or differently anchored providers from cross-source checks."""

    if not configured_quote_use_decision(quote, as_of=as_of).pricing_allowed:
        return "quote_not_actionable"
    source_at = quote.quote_time
    source_age = _age_seconds(as_of, source_at)
    if source_age is None or source_age > policy.execution_max_source_age_seconds:
        return "source_stale_or_unverified"
    if source_age < -MAX_FUTURE_TIMESTAMP_SKEW_SECONDS:
        return "source_timestamp_in_future"
    reference_underlier = _greeks_underlier(reference)
    quote_underlier = _greeks_underlier(quote)
    if (
        reference_underlier is not None
        and quote_underlier is not None
        and abs(reference_underlier - quote_underlier)
        > policy.execution_max_provider_underlier_divergence_points
    ):
        return "model_underlier_divergence"
    return None


def _greeks_underlier(quote: Quote) -> float | None:
    value = quote.greeks.underlier_price if quote.greeks is not None else None
    return float(value) if value is not None and value > 0 else None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _percentile_rank(value: float | None, samples: list[float]) -> float | None:
    if value is None or not samples:
        return None
    return sum(sample <= value for sample in samples) / len(samples)


def _mid_divergence_bps(values: tuple[float, ...]) -> float | None:
    if len(values) < 2:
        return None
    low = min(values)
    high = max(values)
    center = (low + high) / 2.0
    return (high - low) / center * 10_000.0 if center > 0 else None


def nearest_abs_delta_strike(
    latest: LatestState,
    expiry: str,
    right: str,
    *,
    target_abs_delta: float,
    now: datetime,
    policy: StrategyPolicy,
    providers: Sequence[Provider],
    max_distance: float = 0.08,
    min_abs_delta: float | None = None,
    max_abs_delta: float | None = None,
    max_greeks_age_seconds: float | None = None,
    diagnostics: dict[str, Any] | None = None,
) -> float | None:
    """Return the strike whose |delta| is closest to target among fresh quotes.

    When ``max_abs_delta`` is set, richer strikes above that cap are ignored so
    a 20Δ target means 20Δ or the next strike below it, never 21–25Δ.
    """

    wanted = str(right or "").upper()
    floor = 0.0 if min_abs_delta is None else float(min_abs_delta)
    ceiling = None if max_abs_delta is None else float(max_abs_delta)
    counts = dict(contracts=0, fresh_bbo=0, missing_delta=0, stale_delta=0, usable_delta=0)
    if diagnostics is not None:
        diagnostics.update(counts, selected_strike=None, providers=[p.value for p in providers])
    for provider in providers:
        best_strike: float | None = None
        best_distance: float | None = None
        for quote in latest.quotes:
            instrument = quote.instrument
            if (
                quote.provider is not provider
                or instrument.expiry != expiry
                or str(getattr(instrument.right, "value", instrument.right) or "").upper() != wanted
            ):
                continue
            counts["contracts"] += 1
            source_at = quote_source_at(quote)
            if source_at is None:
                continue
            age = (now - source_at).total_seconds()
            if age < 0.0 or age > policy.quote_max_age_seconds:
                continue
            fresh_bbo = (quote.quality is MarketDataQuality.LIVE and quote.bid is not None
                         and quote.ask is not None and 0 <= quote.bid <= quote.ask)
            if fresh_bbo:
                counts["fresh_bbo"] += 1
            delta = usable_delta(quote)
            if delta is None:
                if fresh_bbo and (quote.greeks is None or quote.greeks.delta is None):
                    counts["missing_delta"] += 1
                continue
            if max_greeks_age_seconds is not None:
                raw = quote.raw if isinstance(quote.raw, Mapping) else {}
                raw_at = raw.get("greeks_observed_at")
                try:
                    greeks_at = raw_at if isinstance(raw_at, datetime) else datetime.fromisoformat(str(raw_at).replace("Z", "+00:00"))
                    greeks_at = greeks_at if greeks_at.tzinfo else source_at
                except ValueError:
                    greeks_at = source_at
                greeks_provider = str(raw.get("greeks_provider") or provider.value)
                greeks_age = (now - greeks_at).total_seconds()
                if (
                    greeks_provider != provider.value
                    or greeks_age < 0.0
                    or greeks_age > max_greeks_age_seconds
                ):
                    counts["stale_delta"] += 1
                    continue
            counts["usable_delta"] += 1
            abs_delta = abs(delta)
            if abs_delta < floor:
                continue
            if ceiling is not None and abs_delta - ceiling > 1e-9:
                continue
            distance = abs(abs_delta - target_abs_delta)
            if best_distance is None or distance < best_distance:
                best_distance = distance
                best_strike = round(float(instrument.strike) / 5.0) * 5.0 if instrument.strike is not None else None
        if best_strike is not None and best_distance is not None and best_distance <= max_distance:
            if diagnostics is not None:
                diagnostics.update(counts, selected_strike=best_strike)
            return best_strike
    if diagnostics is not None:
        diagnostics.update(counts)
    return None
