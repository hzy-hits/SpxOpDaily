"""Deterministic Schwab-primary / IBKR-fallback market-data controller."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from spx_spark.marketdata import as_utc, parse_timestamp


class FailoverMode(str, Enum):
    SCHWAB_PRIMARY = "schwab_primary"
    RECOVERY_PENDING = "recovery_pending"
    IBKR_FALLBACK = "ibkr_fallback"
    BOTH_UNAVAILABLE = "both_unavailable"


@dataclass(frozen=True)
class FailoverObservation:
    observed_at: datetime
    schwab_healthy: bool
    ibkr_healthy: bool
    schwab_reason: str | None = None
    ibkr_reason: str | None = None


@dataclass(frozen=True)
class FailoverTransition:
    transition_id: str
    sequence: int
    previous_mode: FailoverMode
    mode: FailoverMode
    occurred_at: datetime
    reason: str

    def to_dict(self) -> dict[str, object]:
        return {
            "transition_id": self.transition_id,
            "sequence": self.sequence,
            "previous_mode": self.previous_mode.value,
            "mode": self.mode.value,
            "occurred_at": as_utc(self.occurred_at).isoformat(),
            "reason": self.reason,
        }


@dataclass(frozen=True)
class FailoverState:
    mode: FailoverMode
    updated_at: datetime
    sequence: int = 0
    schwab_unhealthy_streak: int = 0
    schwab_recovery_streak: int = 0
    ibkr_unhealthy_streak: int = 0
    last_schwab_reason: str | None = None
    last_ibkr_reason: str | None = None
    transition: FailoverTransition | None = None

    @classmethod
    def initial(cls, *, now: datetime | None = None) -> "FailoverState":
        return cls(
            mode=FailoverMode.SCHWAB_PRIMARY,
            updated_at=as_utc(now or datetime.now(tz=timezone.utc)),
        )

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "FailoverState":
        transition_raw = raw.get("transition")
        transition = None
        if isinstance(transition_raw, dict):
            occurred_at = parse_timestamp(transition_raw.get("occurred_at"))
            if occurred_at is not None:
                transition = FailoverTransition(
                    transition_id=str(transition_raw.get("transition_id") or ""),
                    sequence=int(transition_raw.get("sequence") or 0),
                    previous_mode=FailoverMode(str(transition_raw["previous_mode"])),
                    mode=FailoverMode(str(transition_raw["mode"])),
                    occurred_at=occurred_at,
                    reason=str(transition_raw.get("reason") or ""),
                )
        updated_at = parse_timestamp(raw.get("updated_at"))
        if updated_at is None:
            raise ValueError("provider failover state has no valid updated_at")
        return cls(
            mode=FailoverMode(str(raw.get("mode") or FailoverMode.SCHWAB_PRIMARY.value)),
            updated_at=updated_at,
            sequence=int(raw.get("sequence") or 0),
            schwab_unhealthy_streak=int(raw.get("schwab_unhealthy_streak") or 0),
            schwab_recovery_streak=int(raw.get("schwab_recovery_streak") or 0),
            ibkr_unhealthy_streak=int(raw.get("ibkr_unhealthy_streak") or 0),
            last_schwab_reason=_optional_text(raw.get("last_schwab_reason")),
            last_ibkr_reason=_optional_text(raw.get("last_ibkr_reason")),
            transition=transition,
        )

    @property
    def ibkr_market_data_required(self) -> bool:
        return self.mode in {
            FailoverMode.RECOVERY_PENDING,
            FailoverMode.IBKR_FALLBACK,
            FailoverMode.BOTH_UNAVAILABLE,
        }

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["mode"] = self.mode.value
        payload["updated_at"] = as_utc(self.updated_at).isoformat()
        payload["ibkr_market_data_required"] = self.ibkr_market_data_required
        payload["transition"] = self.transition.to_dict() if self.transition is not None else None
        return payload


@dataclass(frozen=True)
class FailoverThresholds:
    schwab_unhealthy_observations: int
    schwab_recovery_observations: int
    ibkr_unhealthy_observations: int

    def __post_init__(self) -> None:
        for name, value in asdict(self).items():
            if value <= 0:
                raise ValueError(f"{name} must be positive")


def advance_failover(
    state: FailoverState,
    observation: FailoverObservation,
    thresholds: FailoverThresholds,
) -> FailoverState:
    now = as_utc(observation.observed_at)
    schwab_unhealthy = 0 if observation.schwab_healthy else state.schwab_unhealthy_streak + 1
    schwab_recovery = state.schwab_recovery_streak + 1 if observation.schwab_healthy else 0
    ibkr_unhealthy = (
        0
        if state.mode == FailoverMode.SCHWAB_PRIMARY or observation.ibkr_healthy
        else state.ibkr_unhealthy_streak + 1
    )
    next_mode = state.mode
    reason = "health observation"

    if state.mode == FailoverMode.SCHWAB_PRIMARY:
        if schwab_unhealthy >= thresholds.schwab_unhealthy_observations:
            if observation.ibkr_healthy:
                next_mode = FailoverMode.IBKR_FALLBACK
                reason = "Schwab unhealthy; IBKR fallback is ready"
            else:
                next_mode = FailoverMode.RECOVERY_PENDING
                ibkr_unhealthy = 0
                reason = "Schwab unhealthy; requesting IBKR fallback"
    elif state.mode == FailoverMode.RECOVERY_PENDING:
        if observation.ibkr_healthy and not observation.schwab_healthy:
            next_mode = FailoverMode.IBKR_FALLBACK
            reason = "IBKR fallback became ready"
        elif observation.schwab_healthy and (
            schwab_recovery >= thresholds.schwab_recovery_observations
        ):
            next_mode = FailoverMode.SCHWAB_PRIMARY
            reason = "Schwab recovered before fallback activation"
        elif (
            not observation.schwab_healthy
            and ibkr_unhealthy >= thresholds.ibkr_unhealthy_observations
        ):
            next_mode = FailoverMode.BOTH_UNAVAILABLE
            reason = "Schwab and IBKR are unavailable"
    elif state.mode == FailoverMode.IBKR_FALLBACK:
        if observation.schwab_healthy and (
            schwab_recovery >= thresholds.schwab_recovery_observations
        ):
            next_mode = FailoverMode.SCHWAB_PRIMARY
            reason = "Schwab recovery confirmed"
        elif (
            not observation.schwab_healthy
            and ibkr_unhealthy >= thresholds.ibkr_unhealthy_observations
        ):
            next_mode = FailoverMode.BOTH_UNAVAILABLE
            reason = "IBKR fallback failed while Schwab remained unavailable"
    elif state.mode == FailoverMode.BOTH_UNAVAILABLE:
        if observation.schwab_healthy and (
            schwab_recovery >= thresholds.schwab_recovery_observations
        ):
            next_mode = FailoverMode.SCHWAB_PRIMARY
            reason = "Schwab recovery confirmed"
        elif observation.ibkr_healthy and not observation.schwab_healthy:
            next_mode = FailoverMode.IBKR_FALLBACK
            reason = "IBKR fallback recovered"

    transition = state.transition
    sequence = state.sequence
    if next_mode != state.mode:
        sequence += 1
        occurred_at = as_utc(now)
        transition = FailoverTransition(
            transition_id=(
                "provider-failover:"
                f"{occurred_at.strftime('%Y%m%dT%H%M%S%fZ')}:"
                f"{sequence}:{next_mode.value}"
            ),
            sequence=sequence,
            previous_mode=state.mode,
            mode=next_mode,
            occurred_at=occurred_at,
            reason=reason,
        )
        if next_mode == FailoverMode.SCHWAB_PRIMARY:
            ibkr_unhealthy = 0

    return FailoverState(
        mode=next_mode,
        updated_at=now,
        sequence=sequence,
        schwab_unhealthy_streak=schwab_unhealthy,
        schwab_recovery_streak=schwab_recovery,
        ibkr_unhealthy_streak=ibkr_unhealthy,
        last_schwab_reason=observation.schwab_reason,
        last_ibkr_reason=observation.ibkr_reason,
        transition=transition,
    )


def control_requires_ibkr_market_data(
    raw: dict[str, Any],
    *,
    now: datetime,
    max_age_seconds: float,
) -> bool:
    if raw.get("monitoring_active") is not True:
        return False
    if raw.get("ibkr_market_data_required") is not True:
        return False
    updated_at = parse_timestamp(raw.get("updated_at"))
    if updated_at is None:
        return False
    age_seconds = (as_utc(now) - updated_at).total_seconds()
    return 0 <= age_seconds <= max_age_seconds


def control_allows_new_entries(
    raw: dict[str, Any],
    *,
    now: datetime,
    max_age_seconds: float,
) -> bool:
    """Fail closed unless a fresh RTH control state explicitly permits entries."""

    if raw.get("monitoring_active") is not True:
        return False
    if raw.get("new_entries_allowed") is not True:
        return False
    if raw.get("mode") not in {
        FailoverMode.SCHWAB_PRIMARY.value,
        FailoverMode.IBKR_FALLBACK.value,
    }:
        return False
    updated_at = parse_timestamp(raw.get("updated_at"))
    if updated_at is None:
        return False
    age_seconds = (as_utc(now) - updated_at).total_seconds()
    return 0 <= age_seconds <= max_age_seconds


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
