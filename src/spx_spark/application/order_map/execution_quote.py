"""Execution-grade option quote gates for conditional repricing."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import StrEnum
from typing import Iterable

from spx_spark.marketdata import Quote
from spx_spark.settings.order_map import DEFAULT_ORDER_MAP_POLICY, OrderMapPolicy
from spx_spark.storage import configured_quote_use_decision


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
    source_at = quote.quote_time or quote.trade_time
    transport_age = _age_seconds(as_of, transport_at)
    source_age = _age_seconds(as_of, source_at)
    if transport_age is None or transport_age > policy.execution_max_quote_age_seconds:
        reasons.append("transport_quote_stale")
    if source_age is None or source_age > policy.execution_max_source_age_seconds:
        reasons.append("source_quote_stale_or_unverified")

    provider_mids = {
        item.provider.value: item.mid
        for item in all_quotes
        if item.instrument.canonical_id == quote.instrument.canonical_id
        and item.mid is not None
        and configured_quote_use_decision(item, as_of=as_of).pricing_allowed
    }
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
    )


def _age_seconds(as_of: datetime, value: datetime | None) -> float | None:
    if value is None:
        return None
    now = _utc(as_of)
    return max((now - _utc(value)).total_seconds(), 0.0)


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
