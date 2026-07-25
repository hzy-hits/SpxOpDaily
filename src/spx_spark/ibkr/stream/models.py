"""Typed models and timing constants for the IBKR stream runtime."""

from __future__ import annotations

import random
import time
from math import ceil, log2
from dataclasses import dataclass
from enum import Enum

from spx_spark.config import IbkrSettings
from spx_spark.sampling import OptionContractSpec

MAX_TRACKED_ERRORS = 200
SUBSCRIPTION_CONFIRM_SECONDS = 0.5
SUBSCRIPTION_REJECTION_CODES = frozenset({100, 101, 200, 354, 420, 10197})
OPTION_ROTATION_RETRY_SECONDS = 30.0
QUALIFICATION_TIMEOUT_SECONDS = 5.0
HOT_FLUSH_LIFECYCLE_BUDGET_SECONDS = 6.0
HOT_FLUSH_SLEEP_MAX_SECONDS = 5.0
OPTION_CACHE_TTL_SECONDS = 900.0


class StreamAction(str, Enum):
    CONTINUE = "continue"
    RECONNECT = "reconnect"
    CONFLICT_WAIT = "conflict_wait"
    POLICY_BLOCKED = "policy_blocked"
    GATEWAY_RESTART = "gateway_restart"


@dataclass
class CompetingSessionCircuit:
    """Deterministic exponential cooldown for IBKR error 10197.

    The circuit never preempts another IBKR session.  It only controls when
    this collector may make its next ordinary, read-only market-data attempt.
    A genuinely healthy data flush closes the circuit; merely reconnecting the
    socket does not.
    """

    min_seconds: float
    max_seconds: float
    failures: int = 0
    retry_not_before: float = 0.0

    def __post_init__(self) -> None:
        if self.min_seconds <= 0:
            raise ValueError("competing-session cooldown minimum must be positive")
        if self.max_seconds < self.min_seconds:
            raise ValueError("competing-session cooldown maximum cannot be below minimum")

    def open(self, *, now_monotonic: float) -> float:
        max_doublings = max(ceil(log2(self.max_seconds / self.min_seconds)), 0)
        exponent = min(self.failures, max_doublings)
        delay = min(self.min_seconds * (2**exponent), self.max_seconds)
        self.failures += 1
        self.retry_not_before = now_monotonic + delay
        return delay

    def remaining_seconds(self, *, now_monotonic: float) -> float:
        return max(self.retry_not_before - now_monotonic, 0.0)

    def state(self, *, now_monotonic: float) -> str:
        if self.failures == 0:
            return "closed"
        if self.remaining_seconds(now_monotonic=now_monotonic) > 0:
            return "open"
        return "half_open"

    def close(self) -> None:
        self.failures = 0
        self.retry_not_before = 0.0


@dataclass
class ReconnectPolicy:
    min_seconds: float
    max_seconds: float
    attempt: int = 0

    def next_delay(self) -> float:
        base = min(self.min_seconds * (2**self.attempt), self.max_seconds)
        self.attempt += 1
        # Jitter avoids synchronized reconnect storms after a shared disconnect;
        # cap again so jittered values never exceed max_seconds.
        return min(reconnect_jitter(base), self.max_seconds)

    def reset(self) -> None:
        self.attempt = 0


def reconnect_jitter(seconds: float) -> float:
    """Scale a backoff delay by a random factor in [0.5, 1.5)."""

    return seconds * random.uniform(0.5, 1.5)


def lifecycle_has_qualification_budget(
    started_at: float,
    *,
    now_monotonic: float | None = None,
) -> bool:
    """Keep lifecycle work bounded so persisted hot rows stay <=12s apart."""

    now = time.monotonic() if now_monotonic is None else now_monotonic
    remaining = HOT_FLUSH_LIFECYCLE_BUDGET_SECONDS - max(now - started_at, 0.0)
    return remaining >= QUALIFICATION_TIMEOUT_SECONDS + SUBSCRIPTION_CONFIRM_SECONDS


def effective_hot_flush_sleep_seconds(configured_seconds: float) -> float:
    """Honor faster flush settings while enforcing the reliability ceiling."""

    return min(max(float(configured_seconds), 0.0), HOT_FLUSH_SLEEP_MAX_SECONDS)


@dataclass(frozen=True)
class OptionSubscriptionPlan:
    """Line-budgeted view of a sampling plan.

    `hot` stays subscribed for the lifetime of the plan; `rotations` are
    swapped in one slice at a time, each slice fitting the leftover budget.
    """

    atm_strike: int
    expiry: str
    hot: tuple[OptionContractSpec, ...]
    rotations: tuple[tuple[OptionContractSpec, ...], ...]

    @property
    def rotation_count(self) -> int:
        return len(self.rotations)


def replace_client_id(settings: IbkrSettings, client_id: int) -> IbkrSettings:
    from dataclasses import asdict

    payload = asdict(settings)
    payload["client_id"] = client_id
    return IbkrSettings(**payload)
