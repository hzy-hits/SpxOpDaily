"""Batch replay of the frozen management policy across historical paths.

The joint/clearing path distributions replay thousands of paths with one shared
mark timetable. Doing that with the scalar policy in Python dominated the
decision cycle, so the vectorised kernel is used when every path exits; the
scalar loop remains the reference and fallback for incomplete paths.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime

import numpy as np

from spx_spark.analytics.options.strategy_payoff import (
    ManagementPolicy,
    PolicyLabel,
    PolicyMark,
    simulate_management_policy,
    simulate_management_policy_batch,
)


@dataclass
class PathReplay:
    pnls: list[float] = field(default_factory=list)
    holds: list[float] = field(default_factory=list)
    invalidation_hits: int = 0
    tp_before_stop: int = 0
    exit_reasons: Counter[str] = field(default_factory=Counter)
    failure_reason: str | None = None


def replay_paths(
    liquidation_values: np.ndarray,
    spots: np.ndarray,
    mark_times: list[datetime],
    *,
    entry_price: float,
    contract_count: int,
    entry_at: datetime,
    policy: ManagementPolicy,
    session_date: date | None = None,
    max_quote_gap_seconds: float | None = None,
    invalidation: Callable[[tuple[float, ...]], bool] | None = None,
) -> PathReplay:
    """Replay one mark timetable across paths, falling back to the scalar loop.

    ``invalidation`` is the optional per-path predicate used for diagnostics;
    it never changes the exit decision.
    """

    outcome = PathReplay()
    try:
        labels = simulate_management_policy_batch(
            liquidation_values,
            mark_times,
            entry_price=entry_price,
            contract_count=contract_count,
            entry_at=entry_at,
            policy=policy,
            session_date=session_date,
            max_quote_gap_seconds=max_quote_gap_seconds,
        )
    except (TypeError, ValueError):
        labels = None
    if labels is not None and all(label.policy_pnl_points is not None for label in labels):
        for index, label in enumerate(labels):
            _record(outcome, label, index, spots, invalidation, entry_at)
        return outcome
    for index in range(len(liquidation_values)):
        marks = [
            PolicyMark(at=at, liquidation_value=float(value))
            for at, value in zip(mark_times, liquidation_values[index], strict=True)
        ]
        label = simulate_management_policy(
            marks,
            entry_price=entry_price,
            contract_count=contract_count,
            entry_at=entry_at,
            policy=policy,
            session_date=session_date,
            max_quote_gap_seconds=max_quote_gap_seconds,
        )
        _record(outcome, label, index, spots, invalidation, entry_at)
        if label.policy_pnl_points is None:
            outcome.failure_reason = label.exit_reason
            return outcome
    return outcome


def _record(
    outcome: PathReplay,
    label: PolicyLabel,
    index: int,
    spots: np.ndarray,
    invalidation: Callable[[tuple[float, ...]], bool] | None,
    entry_at: datetime,
) -> None:
    if invalidation is not None:
        projected = tuple(float(value) for value in spots[index])
        if invalidation(projected):
            outcome.invalidation_hits += 1
    if label.policy_pnl_points is not None:
        outcome.pnls.append(float(label.policy_pnl_points))
    if label.exit_at is not None:
        outcome.holds.append((label.exit_at - entry_at).total_seconds() / 60.0)
    if label.tp_before_stop:
        outcome.tp_before_stop += 1
    outcome.exit_reasons[label.exit_reason] += 1


def simulate_paths(
    liquidation_values: np.ndarray,
    spots: np.ndarray,
    mark_times: list[datetime],
    *,
    entry_price: float,
    contract_count: int,
    entry_at: datetime,
    policy: ManagementPolicy,
    session_date: date | None = None,
    max_quote_gap_seconds: float | None = None,
    invalidation: Callable[[tuple[float, ...]], bool] | None = None,
) -> dict[str, object]:
    """Return the counter dict consumed by the joint path distribution."""

    outcome = replay_paths(
        liquidation_values,
        spots,
        mark_times,
        entry_price=entry_price,
        contract_count=contract_count,
        entry_at=entry_at,
        policy=policy,
        session_date=session_date,
        max_quote_gap_seconds=max_quote_gap_seconds,
        invalidation=invalidation,
    )
    counters = {
        "invalidation": outcome.invalidation_hits,
        "tp": outcome.tp_before_stop,
        "premium_stop": 0,
        "stop_loss": 0,
        "hard_close": 0,
        "time_stop": 0,
    }
    if outcome.failure_reason is not None:
        return {
            "pnls": [],
            "holds": [],
            "counters": counters,
            "reason": outcome.failure_reason,
        }
    for reason, count in outcome.exit_reasons.items():
        if reason in counters:
            counters[reason] = count
    return {"pnls": outcome.pnls, "holds": outcome.holds, "counters": counters}
