"""Execute planned Schwab chain lanes and reduce their results."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Callable

from spx_spark.config import SchwabSettings, StorageSettings
from spx_spark.schwab.chain_lane import (
    LaneDisposition,
    LaneOutcome,
    execute_chain_lane,
    plan_chain_lanes,
)
from spx_spark.schwab.collector_io import chain_spot
from spx_spark.schwab.collector_state import CollectorBudgetState
from spx_spark.schwab.front_discovery import build_front_discovery
from spx_spark.schwab.request_models import CollectionProfile, QuotaMode, SchwabLane


@dataclass
class ChainCycleResult:
    request_count: int = 0
    quote_counts: dict[str, int] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    chains_fetched: list[str] = field(default_factory=list)
    chains_skipped: list[str] = field(default_factory=list)
    lanes_fetched: list[str] = field(default_factory=list)
    lanes_skipped: list[str] = field(default_factory=list)
    chain_as_of: dict[str, str | None] = field(default_factory=dict)
    coverage: dict[str, dict[str, Any]] = field(default_factory=dict)


def collect_chain_cycle(
    *,
    client: Any,
    chain_symbols: list[str],
    quote_symbols: list[str],
    current_expiry: date,
    next_expiry: date,
    now: datetime,
    profile: CollectionProfile,
    quota_mode: QuotaMode,
    budget_state: CollectorBudgetState,
    settings: SchwabSettings,
    typed_settings: Any,
    storage_settings: StorageSettings,
    available_requests: int,
    fetch: Callable[..., Any],
    persist: Callable[..., Any],
) -> ChainCycleResult:
    result = ChainCycleResult()
    plans = plan_chain_lanes(
        chain_symbols=chain_symbols,
        current_expiry=current_expiry,
        next_expiry=next_expiry,
        now=now,
        profile=profile,
        quota_mode=quota_mode,
        budget_state=budget_state,
        settings=settings,
        typed_settings=typed_settings,
        available_requests=available_requests,
    )
    result.chain_as_of = {
        plan.canonical: plan.last_fetched_at.isoformat() if plan.last_fetched_at else None
        for plan in plans
        if plan.updates_canonical_clock
    }
    for plan in plans:
        lane_result = execute_chain_lane(
            plan,
            client=client,
            settings=settings,
            now=now,
            fetch=fetch,
        )
        if lane_result.outcome is LaneOutcome.SKIPPED:
            _record_skipped(result, plan)
            continue
        if lane_result.outcome is LaneOutcome.FAILED:
            result.errors.append(lane_result.error or f"{plan.lane_key}: failed")
            continue

        snapshot = lane_result.snapshot
        if snapshot is None:
            result.errors.append(f"{plan.lane_key}: missing_snapshot")
            continue
        persist(snapshot, storage_settings)
        result.request_count += 1
        result.lanes_fetched.append(plan.lane_key)
        result.quote_counts[plan.canonical if plan.updates_canonical_clock else plan.lane_key] = (
            snapshot.quote_count
        )
        budget_state.chain_last_fetched_at[plan.lane_key] = now
        if plan.updates_canonical_clock:
            budget_state.chain_last_fetched_at[plan.canonical] = now
            result.chains_fetched.append(plan.canonical)
            result.chain_as_of[plan.canonical] = now.isoformat()
        if plan.lane is SchwabLane.FRONT_CHAIN:
            _apply_front_discovery(
                result,
                payload=lane_result.payload,
                quotes=snapshot.quotes,
                plan=plan,
                quote_symbols=quote_symbols,
                expiry=current_expiry.strftime("%Y%m%d"),
                now=now,
                budget_state=budget_state,
                typed_settings=typed_settings,
            )
    return result


def _record_skipped(result: ChainCycleResult, plan: Any) -> None:
    result.lanes_skipped.append(plan.lane_key)
    if plan.updates_canonical_clock:
        result.chains_skipped.append(plan.canonical)
    if plan.disposition is LaneDisposition.BUDGET_BLOCKED and plan.updates_canonical_clock:
        result.errors.append(f"{plan.lane_key}: planned_request_ceiling")


def _apply_front_discovery(
    result: ChainCycleResult,
    *,
    payload: Any,
    quotes: tuple[Any, ...],
    plan: Any,
    quote_symbols: list[str],
    expiry: str,
    now: datetime,
    budget_state: CollectorBudgetState,
    typed_settings: Any,
) -> None:
    spot = chain_spot(payload, quotes) or budget_state.last_spot
    if spot is None:
        return
    if (
        budget_state.last_spot is not None
        and abs(spot - budget_state.last_spot) >= typed_settings.hot_lane.recenter_drift_points
    ):
        budget_state.burst_until = now + timedelta(seconds=60)
    budget_state.last_spot = spot
    discovery = build_front_discovery(
        quotes,
        spot=spot,
        expiry=expiry,
        requested_strike_count=plan.strike_count,
        context_symbol_count=len(quote_symbols),
        typed_settings=typed_settings,
    )
    budget_state.strike_counts[plan.lane_key] = discovery.next_strike_count
    budget_state.hot_symbols = list(discovery.hot_plan.symbols)
    budget_state.hot_expiry = discovery.hot_plan.expiry
    budget_state.hot_reference_spot = discovery.hot_plan.reference_spot
    result.coverage[plan.lane_key] = discovery.coverage_summary
