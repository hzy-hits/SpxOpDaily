"""Persistent runtime estimate of IBKR concurrent market-data capacity."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


CAPACITY_STATE_NAME = "ibkr_market_data_capacity.json"
TICKER_LIMIT_ERROR_CODES = frozenset({101})
TICKER_LIMIT_MESSAGE_MARKERS = (
    "max number of tickers",
    "maximum number of market data",
    "market data lines",
)


@dataclass
class CapacityEstimate:
    configured_capacity: int
    estimated_capacity: int
    observed_lower_bound: int = 0
    active_high_watermark: int = 0
    consecutive_full_successes: int = 0
    last_rejection_at: str | None = None
    last_updated_at: str | None = None

    @classmethod
    def initial(cls, configured_capacity: int) -> "CapacityEstimate":
        capacity = max(int(configured_capacity), 1)
        return cls(configured_capacity=capacity, estimated_capacity=capacity)


class MarketDataCapacityTracker:
    def __init__(self, path: Path, *, configured_capacity: int) -> None:
        self.path = path
        self.state = load_capacity_estimate(path, configured_capacity=configured_capacity)

    @property
    def effective_capacity(self) -> int:
        return min(self.state.estimated_capacity, self.state.configured_capacity)

    def observe_success(self, *, active_lines: int, now: datetime | None = None) -> None:
        stamp = now or datetime.now(tz=timezone.utc)
        active = max(int(active_lines), 0)
        state = self.state
        previous_estimate = state.estimated_capacity
        state.observed_lower_bound = max(state.observed_lower_bound, active)
        state.active_high_watermark = max(state.active_high_watermark, active)
        if active >= state.estimated_capacity:
            state.consecutive_full_successes += 1
        else:
            state.consecutive_full_successes = 0
        if (
            state.consecutive_full_successes >= 10
            and state.estimated_capacity < state.configured_capacity
        ):
            state.estimated_capacity += 1
            state.consecutive_full_successes = 0
        if state.estimated_capacity != previous_estimate:
            state.last_updated_at = stamp.isoformat()
            save_capacity_estimate(self.path, state)

    def observe_error(
        self,
        *,
        error_code: int,
        message: str,
        active_lines: int,
        now: datetime | None = None,
    ) -> bool:
        if not is_ticker_limit_error(error_code, message):
            return False
        stamp = now or datetime.now(tz=timezone.utc)
        active = max(int(active_lines), 1)
        state = self.state
        state.observed_lower_bound = max(state.observed_lower_bound, active)
        state.active_high_watermark = max(state.active_high_watermark, active)
        state.estimated_capacity = min(
            state.estimated_capacity,
            max(active, state.observed_lower_bound),
        )
        state.consecutive_full_successes = 0
        state.last_rejection_at = stamp.isoformat()
        state.last_updated_at = stamp.isoformat()
        save_capacity_estimate(self.path, state)
        return True


def is_ticker_limit_error(error_code: int, message: str) -> bool:
    lowered = message.lower()
    return error_code in TICKER_LIMIT_ERROR_CODES or any(
        marker in lowered for marker in TICKER_LIMIT_MESSAGE_MARKERS
    )


def active_market_data_lines(owner: Any) -> int:
    labels: set[str] = set()
    for attribute in (
        "base_subs",
        "hot_subs",
        "rotation_subs",
        "spy_subs",
        "slow_active_subs",
    ):
        subscriptions = getattr(owner, attribute, {})
        if isinstance(subscriptions, dict):
            labels.update(str(label) for label in subscriptions)
    return len(labels)


def load_capacity_estimate(path: Path, *, configured_capacity: int) -> CapacityEstimate:
    initial = CapacityEstimate.initial(configured_capacity)
    if not path.is_file():
        return initial
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return initial
    if not isinstance(payload, dict):
        return initial
    estimated = _positive_int(payload.get("estimated_capacity")) or initial.estimated_capacity
    return CapacityEstimate(
        configured_capacity=initial.configured_capacity,
        estimated_capacity=min(estimated, initial.configured_capacity),
        observed_lower_bound=max(int(payload.get("observed_lower_bound", 0)), 0),
        active_high_watermark=max(int(payload.get("active_high_watermark", 0)), 0),
        consecutive_full_successes=max(int(payload.get("consecutive_full_successes", 0)), 0),
        last_rejection_at=_optional_text(payload.get("last_rejection_at")),
        last_updated_at=_optional_text(payload.get("last_updated_at")),
    )


def save_capacity_estimate(path: Path, state: CapacityEstimate) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    temporary.write_text(json.dumps(asdict(state), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def _positive_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _optional_text(value: Any) -> str | None:
    return str(value) if value else None
