"""Pure chain-lane planning and transport execution contracts."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import Any, Callable
from urllib.error import HTTPError, URLError

from spx_spark.config import SchwabSettings
from spx_spark.provider_adapter import ProviderSnapshot
from spx_spark.schwab.adapter import snapshot_from_chain_payload
from spx_spark.schwab.collector_state import CollectorBudgetState
from spx_spark.schwab.market_data_plan import cadence_seconds, request_is_due
from spx_spark.schwab.quota_machine import lane_allowed
from spx_spark.schwab.request_models import CollectionProfile, QuotaMode, SchwabLane
from spx_spark.schwab.symbols import canonical_underlier_for_schwab, option_chain_strike_count_for


class LaneDisposition(str, Enum):
    READY = "ready"
    NOT_DUE = "not_due"
    BUDGET_BLOCKED = "budget_blocked"
    QUOTA_BLOCKED = "quota_blocked"


class LaneOutcome(str, Enum):
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass(frozen=True)
class ChainLanePlan:
    lane: SchwabLane
    lane_key: str
    symbol: str
    canonical: str
    expiry: date
    strike_count: int
    priority: int
    disposition: LaneDisposition
    last_fetched_at: datetime | None
    updates_canonical_clock: bool


@dataclass(frozen=True)
class ChainLaneResult:
    plan: ChainLanePlan
    outcome: LaneOutcome
    payload: Any | None = None
    snapshot: ProviderSnapshot | None = None
    error: str | None = None


def plan_chain_lanes(
    *,
    chain_symbols: list[str],
    current_expiry: date,
    next_expiry: date,
    now: datetime,
    profile: CollectionProfile,
    quota_mode: QuotaMode,
    budget_state: CollectorBudgetState,
    settings: SchwabSettings,
    typed_settings: Any,
    available_requests: int,
) -> tuple[ChainLanePlan, ...]:
    candidates = [_front_candidate(symbol, current_expiry) for symbol in chain_symbols]
    if any(candidate[2] == "SPX" for candidate in candidates):
        candidates.append((SchwabLane.NEXT_CHAIN, "SPX", "SPX", next_expiry, 2, False))

    plans: list[ChainLanePlan] = []
    remaining = max(available_requests, 0)
    for lane, symbol, canonical, expiry, priority, updates_canonical in candidates:
        lane_key = f"{canonical}:{'next' if lane is SchwabLane.NEXT_CHAIN else 'front'}"
        last_fetched = budget_state.chain_last_fetched_at.get(lane_key)
        if last_fetched is None and updates_canonical:
            last_fetched = budget_state.chain_last_fetched_at.get(canonical)
        cadence = cadence_seconds(
            lane,
            profile=profile,
            policy=typed_settings.cadence,
            underlier=canonical,
        )
        disposition = _lane_disposition(
            due=request_is_due(last_fetched, now=now, cadence=cadence),
            quota_allowed=lane_allowed(quota_mode, priority=priority),
            has_budget=remaining > 0,
        )
        if disposition is LaneDisposition.READY:
            remaining -= 1
        strike_count = _strike_count(
            lane=lane,
            lane_key=lane_key,
            canonical=canonical,
            budget_state=budget_state,
            settings=settings,
            typed_settings=typed_settings,
        )
        plans.append(
            ChainLanePlan(
                lane=lane,
                lane_key=lane_key,
                symbol=symbol,
                canonical=canonical,
                expiry=expiry,
                strike_count=strike_count,
                priority=priority,
                disposition=disposition,
                last_fetched_at=last_fetched,
                updates_canonical_clock=updates_canonical,
            )
        )
    return tuple(plans)


def execute_chain_lane(
    plan: ChainLanePlan,
    *,
    client: Any,
    settings: SchwabSettings,
    now: datetime,
    fetch: Callable[..., Any],
) -> ChainLaneResult:
    if plan.disposition is not LaneDisposition.READY:
        return ChainLaneResult(plan=plan, outcome=LaneOutcome.SKIPPED)
    try:
        payload = fetch(
            client,
            plan.symbol,
            settings,
            now=now,
            strike_count=plan.strike_count,
            expiry=plan.expiry,
        )
        snapshot = snapshot_from_chain_payload(
            payload,
            underlier=plan.canonical,
            received_at=now,
        )
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, ValueError) as exc:
        return ChainLaneResult(
            plan=plan,
            outcome=LaneOutcome.FAILED,
            error=f"{plan.lane_key}: {exc}",
        )
    return ChainLaneResult(
        plan=plan,
        outcome=LaneOutcome.SUCCESS,
        payload=payload,
        snapshot=snapshot,
    )


def _front_candidate(
    symbol: str,
    expiry: date,
) -> tuple[SchwabLane, str, str, date, int, bool]:
    canonical = canonical_underlier_for_schwab(symbol)
    lane = SchwabLane.FRONT_CHAIN if canonical == "SPX" else SchwabLane.CONFIRMATION_CHAIN
    priority = 0 if canonical == "SPX" else 3 if canonical in {"SPY", "XSP"} else 4
    return lane, symbol, canonical, expiry, priority, True


def _lane_disposition(*, due: bool, quota_allowed: bool, has_budget: bool) -> LaneDisposition:
    if not due:
        return LaneDisposition.NOT_DUE
    if not has_budget:
        return LaneDisposition.BUDGET_BLOCKED
    if not quota_allowed:
        return LaneDisposition.QUOTA_BLOCKED
    return LaneDisposition.READY


def _strike_count(
    *,
    lane: SchwabLane,
    lane_key: str,
    canonical: str,
    budget_state: CollectorBudgetState,
    settings: SchwabSettings,
    typed_settings: Any,
) -> int:
    if lane is SchwabLane.NEXT_CHAIN:
        return 60
    if canonical == "SPX":
        return budget_state.strike_counts.get(
            lane_key,
            typed_settings.wide_chain.strike_count_candidates[0],
        )
    return option_chain_strike_count_for(canonical, settings.option_chain_strike_count)
