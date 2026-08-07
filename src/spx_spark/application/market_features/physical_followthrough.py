"""Causal nearest-neighbour baseline for confirmed-level follow-through."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Mapping
from zoneinfo import ZoneInfo

import numpy as np
from scipy.stats import beta as beta_distribution
from sklearn.neighbors import NearestNeighbors
from sklearn.preprocessing import StandardScaler


MODEL_VERSION = "physical_followthrough_nearest_neighbor.v2"
FEATURE_SET_VERSION = "direction_thesis_level_time_bucket.v2"
CALIBRATION_VERSION = "uncalibrated_weighted_beta_interval.v2"
NEW_YORK = ZoneInfo("America/New_York")


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
    effective_sample_count: float = 0.0
    historical_sessions: tuple[str, ...] = ()
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
            "n_raw": self.sample_count,
            "n_effective": self.effective_sample_count,
            "historical_sessions": list(self.historical_sessions),
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
    level_kind: str | None = None,
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
    if not rows:
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
            cohort="nearest_neighbors",
            reason_codes=("physical_outcomes_unavailable",),
        )

    query = _features(direction or "unknown", thesis or "none", level_kind or "unknown", now)
    matrix = np.asarray(
        [_features(row.direction, row.thesis, row.level_kind, row.completed_at) for row in rows]
    )
    scaler = StandardScaler().fit(matrix)
    neighbours = NearestNeighbors(n_neighbors=min(len(rows), 30)).fit(
        scaler.transform(matrix)
    )
    distances, indices = neighbours.kneighbors(scaler.transform([query]))
    selected = tuple(rows[int(index)] for index in indices[0])
    weights = np.exp(-(distances[0] - distances[0].min()))
    effective = float(weights.sum() ** 2 / np.square(weights).sum())
    successes = int(sum(row.directional_return_bps > 0.0 for row in selected))
    weighted_successes = sum(
        float(weight) for row, weight in zip(selected, weights, strict=True)
        if row.directional_return_bps > 0.0
    )
    alpha = prior_alpha + weighted_successes
    beta = prior_beta + float(weights.sum()) - weighted_successes
    probability = alpha / (alpha + beta)
    interval_low, interval_high = beta_distribution.ppf((0.025, 0.975), alpha, beta)
    session_dates = {row.partition_date for row in selected}
    sessions = tuple(sorted(value.isoformat() for value in session_dates))
    status = "estimated_uncalibrated" if effective >= minimum_samples else "insufficient_sample"
    reasons = ["research_unvalidated", "not_fill_probability", "nearest_neighbor_sparse_shrinkage_input"]
    if status == "insufficient_sample":
        reasons.append("physical_sample_below_minimum")
    return PhysicalFollowThroughEstimate(
        status=status,
        probability=round(probability, 6),
        interval_low=round(interval_low, 6),
        interval_high=round(interval_high, 6),
        sample_count=len(selected),
        success_count=successes,
        session_count=len(sessions),
        horizon_seconds=horizon_seconds,
        trained_through_date=max(session_dates),
        cohort="nearest_neighbors",
        reason_codes=tuple(sorted(set(reasons))),
        effective_sample_count=round(effective, 6),
        historical_sessions=sessions,
    )


def _features(direction: str, thesis: str, level_kind: str, observed_at: datetime) -> list[float]:
    local = observed_at.astimezone(NEW_YORK)
    minute = local.hour * 60 + local.minute
    buckets = (585, 660, 750, 840, 900, 945)
    bucket = next((index for index, edge in enumerate(buckets[1:]) if minute < edge), 4)
    return [
        1.0 if direction == "up" else -1.0 if direction == "down" else 0.0,
        1.0 if thesis == "breakout" else -1.0 if thesis == "fade" else 0.0,
        1.0 if "call" in level_kind else -1.0 if "put" in level_kind else 0.0,
        float(bucket),
    ]


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
