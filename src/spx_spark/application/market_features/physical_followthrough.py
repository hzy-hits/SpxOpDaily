"""Causal empirical baseline for confirmed-level directional follow-through.

The first production experiment intentionally stays simple: completed level
events from prior trading dates feed a transparent Beta-Binomial estimate.  It
is a physical, uncalibrated baseline rather than a fill or net-PnL model.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Mapping


MODEL_VERSION = "physical_followthrough_beta_binomial.v1"
FEATURE_SET_VERSION = "confirmed_level_directional_return.v1"
CALIBRATION_VERSION = "uncalibrated_beta_posterior_normal_interval.v1"


@dataclass(frozen=True, slots=True)
class PhysicalFollowThroughEstimate:
    status: str
    probability: float | None
    interval_low: float | None
    interval_high: float | None
    sample_count: int
    success_count: int
    session_count: int
    horizon_seconds: int
    trained_through_date: date | None
    cohort: str
    reason_codes: tuple[str, ...]
    model_version: str = MODEL_VERSION
    feature_set_version: str = FEATURE_SET_VERSION
    calibration_version: str = CALIBRATION_VERSION

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "probability": self.probability,
            "interval_low": self.interval_low,
            "interval_high": self.interval_high,
            "sample_count": self.sample_count,
            "success_count": self.success_count,
            "session_count": self.session_count,
            "horizon_seconds": self.horizon_seconds,
            "trained_through_date": (
                self.trained_through_date.isoformat() if self.trained_through_date else None
            ),
            "cohort": self.cohort,
            "reason_codes": list(self.reason_codes),
            "model_version": self.model_version,
            "feature_set_version": self.feature_set_version,
            "calibration_version": self.calibration_version,
            "event_definition": "directional_terminal_return_bps_above_zero",
            "distribution": "physical",
            "evidence_status": "research_unvalidated",
        }


@dataclass(frozen=True, slots=True)
class _Outcome:
    event_id: str
    partition_date: date
    completed_at: datetime
    direction: str
    thesis: str
    level_kind: str
    directional_return_bps: float


def estimate_physical_followthrough(
    features_root: Path,
    *,
    now: datetime,
    trading_date: date,
    horizon_seconds: int,
    window_days: int,
    minimum_samples: int,
    prior_alpha: float,
    prior_beta: float,
    direction: str | None = None,
    thesis: str | None = None,
) -> PhysicalFollowThroughEstimate:
    """Estimate prior-day follow-through without leaking the current session."""

    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("physical follow-through now must be timezone-aware")
    if horizon_seconds <= 0 or window_days <= 0 or minimum_samples <= 0:
        raise ValueError("physical follow-through settings must be positive")
    if prior_alpha <= 0 or prior_beta <= 0:
        raise ValueError("physical follow-through Beta prior must be positive")
    if direction not in {None, "up", "down"}:
        raise ValueError("physical follow-through direction must be up/down/None")

    earliest = trading_date - timedelta(days=window_days)
    rows = tuple(
        _iter_outcomes(
            Path(features_root) / "level_decision_outcomes",
            horizon_seconds=horizon_seconds,
            earliest=earliest,
            latest_exclusive=trading_date,
            available_at=now.astimezone(timezone.utc),
        )
    )
    matching = tuple(
        row
        for row in rows
        if (direction is None or row.direction == direction)
        and (not thesis or row.thesis == thesis)
    )
    if len(matching) >= minimum_samples:
        selected = matching
        cohort = "direction_thesis"
        used_global_baseline = False
    else:
        selected = rows
        cohort = "all_confirmed_events"
        used_global_baseline = True

    if not selected:
        return PhysicalFollowThroughEstimate(
            status="unavailable",
            probability=None,
            interval_low=None,
            interval_high=None,
            sample_count=0,
            success_count=0,
            session_count=0,
            horizon_seconds=horizon_seconds,
            trained_through_date=None,
            cohort=cohort,
            reason_codes=("physical_outcomes_unavailable",),
        )

    successes = sum(row.directional_return_bps > 0.0 for row in selected)
    alpha = prior_alpha + successes
    beta = prior_beta + len(selected) - successes
    probability = alpha / (alpha + beta)
    variance = alpha * beta / ((alpha + beta) ** 2 * (alpha + beta + 1.0))
    radius = 1.959963984540054 * math.sqrt(variance)
    interval_low = max(0.0, probability - radius)
    interval_high = min(1.0, probability + radius)
    sessions = {row.partition_date for row in selected}
    status = "estimated_uncalibrated" if len(selected) >= minimum_samples else "insufficient_sample"
    reasons: list[str] = ["research_unvalidated", "not_fill_probability"]
    if status == "insufficient_sample":
        reasons.append("physical_sample_below_minimum")
    if matching and used_global_baseline and (direction or thesis):
        reasons.append("requested_cohort_below_minimum_using_global_baseline")
    return PhysicalFollowThroughEstimate(
        status=status,
        probability=round(probability, 6),
        interval_low=round(interval_low, 6),
        interval_high=round(interval_high, 6),
        sample_count=len(selected),
        success_count=successes,
        session_count=len(sessions),
        horizon_seconds=horizon_seconds,
        trained_through_date=max(sessions),
        cohort=cohort,
        reason_codes=tuple(sorted(set(reasons))),
    )


def _iter_outcomes(
    root: Path,
    *,
    horizon_seconds: int,
    earliest: date,
    latest_exclusive: date,
    available_at: datetime,
) -> Iterable[_Outcome]:
    seen: set[str] = set()
    for path in sorted(root.glob("date=*/outcomes.jsonl")):
        partition = _partition_date(path)
        if partition is None or partition < earliest or partition >= latest_exclusive:
            continue
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except OSError:
            continue
        for line in lines:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(row, Mapping):
                continue
            event_id = str(row.get("event_id") or "").strip()
            if (
                not event_id
                or event_id in seen
                or row.get("status") != "complete"
                or row.get("horizon_seconds") != horizon_seconds
            ):
                continue
            completed_at = _timestamp(row.get("completed_at"))
            value = _finite(row.get("return_bps"))
            direction = str(row.get("direction") or "")
            if completed_at is None or completed_at > available_at or value is None:
                continue
            if direction not in {"up", "down"}:
                continue
            seen.add(event_id)
            yield _Outcome(
                event_id=event_id,
                partition_date=partition,
                completed_at=completed_at,
                direction=direction,
                thesis=str(row.get("thesis") or "none"),
                level_kind=str(row.get("level_kind") or "unknown"),
                directional_return_bps=value if direction == "up" else -value,
            )


def _partition_date(path: Path) -> date | None:
    try:
        return date.fromisoformat(path.parent.name.removeprefix("date="))
    except ValueError:
        return None


def _timestamp(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


def _finite(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    parsed = float(value)
    return parsed if math.isfinite(parsed) else None


__all__ = [
    "CALIBRATION_VERSION",
    "FEATURE_SET_VERSION",
    "MODEL_VERSION",
    "PhysicalFollowThroughEstimate",
    "estimate_physical_followthrough",
]
