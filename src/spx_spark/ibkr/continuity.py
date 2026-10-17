"""IBKR data-plane continuity state and append-only gap ledger.

The stream health projection answers "is the data plane healthy now". This
module answers "did it break, for how long, and what proved recovery", which is
what a GTH candidate needs after an outage. It is derived only from the health
projection; it never fabricates ticks or quotes.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from spx_spark.state_io import append_jsonl_secure, atomic_write_json_secure, read_json_object

LANE = "ibkr_option"
HEALTHY = "healthy"
SUSPECT = "suspect"
GAP = "gap"
RECOVERING = "recovering"


@dataclass(frozen=True)
class ContinuityPolicy:
    suspect_after_seconds: float = 20.0
    rearm_samples: int = 3
    invalidate_after_gap_seconds: float = 60.0


@dataclass(frozen=True)
class ContinuityState:
    state: str
    gap_start: datetime | None
    last_good_at: datetime | None
    healthy_samples: int
    updated_at: datetime

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "lane": LANE,
            "state": self.state,
            "gap_start": self.gap_start.isoformat() if self.gap_start else None,
            "last_good_at": self.last_good_at.isoformat() if self.last_good_at else None,
            "healthy_samples": self.healthy_samples,
            "updated_at": self.updated_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "ContinuityState | None":
        state = str(payload.get("state") or "")
        if state not in {HEALTHY, SUSPECT, GAP, RECOVERING}:
            return None
        return cls(
            state=state,
            gap_start=_time(payload.get("gap_start")),
            last_good_at=_time(payload.get("last_good_at")),
            healthy_samples=max(int(payload.get("healthy_samples") or 0), 0),
            updated_at=_time(payload.get("updated_at")) or datetime.now(tz=timezone.utc),
        )


def health_status(
    health: Mapping[str, Any],
    *,
    now: datetime,
    policy: ContinuityPolicy = ContinuityPolicy(),
) -> tuple[str, str, datetime | None]:
    """Return (kind, reason, observed_at) for one health projection."""

    observed = _time(health.get("observed_at"))
    if observed is None:
        return "unhealthy", "health_missing", None
    age = (now - observed).total_seconds()
    max_age = _number(health.get("max_age_seconds")) or 90.0
    if age > max_age:
        return "unhealthy", "health_stale", observed
    if bool(health.get("policy_blocked")):
        return "unhealthy", "policy_blocked", observed
    if str(health.get("circuit_state") or "unknown") != "closed":
        return "unhealthy", "circuit_open", observed
    if _number(health.get("conflict_count")) not in {None, 0.0}:
        return "unhealthy", "competing_session", observed
    if health.get("connected") is not True:
        return "unhealthy", "disconnected", observed
    if health.get("data_plane_healthy") is not True:
        return "unhealthy", "data_plane_unhealthy", observed
    if age > policy.suspect_after_seconds:
        return "suspect", "health_aging", observed
    return "healthy", "ok", observed


def advance_continuity(
    previous: Mapping[str, Any] | None,
    health: Mapping[str, Any],
    *,
    now: datetime,
    policy: ContinuityPolicy = ContinuityPolicy(),
) -> tuple[ContinuityState, dict[str, Any] | None]:
    """Advance one health sample; return the state and one ledger event."""

    now = _utc(now)
    prior = ContinuityState.from_dict(previous or {})
    if prior is None:
        prior = ContinuityState(HEALTHY, None, None, 0, now)
    kind, reason, observed = health_status(health, now=now, policy=policy)
    if kind == "healthy":
        if prior.state in {GAP, RECOVERING}:
            samples = prior.healthy_samples + 1
            gap_start = prior.gap_start or now
            state = ContinuityState(
                HEALTHY if samples >= policy.rearm_samples else RECOVERING,
                gap_start if samples < policy.rearm_samples else None,
                observed or now,
                samples,
                now,
            )
            event = _event(
                "gap_end" if state.state == HEALTHY else "recovering",
                state,
                now,
                reason,
                gap_start=gap_start,
                gap_end=now if state.state == HEALTHY else None,
                recovery_samples=samples,
            )
            return state, event
        return ContinuityState(HEALTHY, None, observed or now, 0, now), None
    if kind == "suspect":
        return ContinuityState(SUSPECT, prior.gap_start, prior.last_good_at, 0, now), None
    gap_start = prior.gap_start or now
    state = ContinuityState(GAP, gap_start, prior.last_good_at, 0, now)
    if prior.state == GAP:
        return state, None
    return state, _event("gap_start", state, now, reason, gap_start=gap_start)


def record_continuity(
    data_root: str | Path,
    health: Mapping[str, Any],
    *,
    now: datetime,
    policy: ContinuityPolicy = ContinuityPolicy(),
) -> ContinuityState:
    """Persist state on change and append one ledger row per transition."""

    root = Path(data_root)
    state_path = root / "latest" / "ibkr_continuity.json"
    previous = read_json_object(state_path)
    previous_state = ContinuityState.from_dict(previous)
    state, event = advance_continuity(previous, health, now=now, policy=policy)
    changed = previous_state is None or (
        state.state,
        state.gap_start,
        state.healthy_samples,
    ) != (
        previous_state.state,
        previous_state.gap_start,
        previous_state.healthy_samples,
    )
    if changed:
        atomic_write_json_secure(state_path, state.to_dict())
    if event is not None:
        append_jsonl_secure(root / "audit" / "ibkr_continuity.jsonl", event)
    return state


def _event(
    event: str,
    state: ContinuityState,
    now: datetime,
    reason: str,
    *,
    gap_start: datetime | None = None,
    gap_end: datetime | None = None,
    recovery_samples: int | None = None,
) -> dict[str, Any]:
    gap_seconds = (
        (gap_end - gap_start).total_seconds()
        if gap_start is not None and gap_end is not None
        else None
    )
    return {
        "schema_version": 1,
        "lane": LANE,
        "event": event,
        "state": state.state,
        "observed_at": now.isoformat(),
        "reason": reason,
        "gap_start": gap_start.isoformat() if gap_start else None,
        "gap_end": gap_end.isoformat() if gap_end else None,
        "gap_seconds": round(gap_seconds, 3) if gap_seconds is not None else None,
        "recovery_samples": recovery_samples,
    }


def _time(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return _utc(parsed)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)
