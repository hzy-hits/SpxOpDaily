"""Single stateful executor for the Schwab hot/context quote lane."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable

from spx_spark.config import SchwabSettings, StorageSettings
from spx_spark.schwab.collector_state import CollectorBudgetState
from spx_spark.schwab.market_data_plan import cadence_seconds, request_is_due
from spx_spark.schwab.quota_machine import lane_allowed
from spx_spark.schwab.request_models import CollectionProfile, QuotaMode, SchwabLane


class QuoteLaneOutcome(str, Enum):
    NOT_READY = "not_ready"
    SUCCESS = "success"
    PARTIAL = "partial"


@dataclass(frozen=True)
class QuoteLaneResult:
    outcome: QuoteLaneOutcome
    attempted: bool = False
    request_count: int = 0
    quote_counts: dict[str, int] = field(default_factory=dict)
    errors: tuple[str, ...] = ()


def collect_quote_lane(
    *,
    client: Any,
    quote_symbols: list[str],
    budget_state: CollectorBudgetState,
    profile: CollectionProfile,
    quota_mode: QuotaMode,
    now: datetime,
    request_ceiling: int,
    requests_used: int,
    settings: SchwabSettings,
    typed_settings: Any,
    storage_settings: StorageSettings,
    require_hot_plan: bool,
    already_attempted: bool,
    collect_batches: Callable[..., Any],
    persist: Callable[..., Any],
) -> QuoteLaneResult:
    quote_key = "quotes:hot_context"
    cadence = cadence_seconds(
        SchwabLane.HOT_AND_CONTEXT_QUOTES,
        profile=profile,
        policy=typed_settings.cadence,
    )
    ready = (
        not already_attempted
        and (bool(budget_state.hot_symbols) or not require_hot_plan)
        and request_is_due(
            budget_state.chain_last_fetched_at.get(quote_key),
            now=now,
            cadence=cadence,
        )
        and lane_allowed(quota_mode, priority=1)
    )
    if not ready:
        return QuoteLaneResult(QuoteLaneOutcome.NOT_READY)

    priority_symbols = list(dict.fromkeys(quote_symbols))
    priority_set = set(priority_symbols)
    hot_symbols = [
        symbol
        for symbol in dict.fromkeys(budget_state.hot_symbols)
        if symbol not in priority_set
    ]
    symbols = priority_symbols + hot_symbols
    count, counts, errors, complete = collect_batches(
        client,
        symbols,
        settings=settings,
        storage_settings=storage_settings,
        received_at=now,
        batch_size=typed_settings.capacity.operational_quote_batch_size,
        priority_symbol_count=len(priority_symbols),
        available_requests=max(request_ceiling - requests_used, 0),
        hot_lane=bool(budget_state.hot_symbols),
        persist_snapshot=persist,
    )
    if complete:
        budget_state.chain_last_fetched_at[quote_key] = now
    return QuoteLaneResult(
        QuoteLaneOutcome.SUCCESS if complete else QuoteLaneOutcome.PARTIAL,
        attempted=True,
        request_count=count,
        quote_counts=counts,
        errors=tuple(errors),
    )
