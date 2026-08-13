"""Causal nearest-neighbour baseline for confirmed-level follow-through."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
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
PIN_CLOCK_WINDOW_MINUTES = 30


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
class PhysicalSpotPath:
    """One completed prior-session 1-minute price window in SPX points."""

    session_date: date
    start_minute: int
    prices: tuple[float, ...]
    same_clock: bool


RTH_OPEN_MINUTE = 9 * 60 + 30
IRON_CONDOR_CLEAR_MINUTE = 12 * 60 + 30


@dataclass(frozen=True, slots=True)
class ClearingSpotPath:
    """One prior session from RTH open (or current clock) to the 12:30 ET clear."""

    session_date: date
    overnight_gap: float
    start_minute: int
    prices: tuple[float, ...]


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


def estimate_physical_terminal_range(
    features_root: Path,
    *,
    now: datetime,
    trading_date: date,
    horizon_seconds: int,
    window_days: int,
    minimum_samples: int,
    prior_alpha: float,
    prior_beta: float,
    current_spot: float,
    lower_level: float,
    upper_level: float,
) -> PhysicalFollowThroughEstimate:
    """Estimate a causal same-clock terminal-range probability for a pin candidate.

    Each completed prior session contributes total weight one, so dense intraminute
    snapshots cannot masquerade as independent market days.
    """

    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("physical terminal-range now must be timezone-aware")
    if horizon_seconds <= 0 or horizon_seconds % 60:
        raise ValueError("physical terminal-range horizon must be whole positive minutes")
    if window_days <= 0 or minimum_samples <= 0:
        raise ValueError("physical terminal-range settings must be positive")
    if prior_alpha <= 0 or prior_beta <= 0:
        raise ValueError("physical terminal-range Beta prior must be positive")
    if not all(math.isfinite(value) and value > 0 for value in (current_spot, lower_level, upper_level)):
        raise ValueError("physical terminal-range levels must be finite and positive")
    if lower_level >= upper_level:
        raise ValueError("physical terminal-range levels must be ordered")

    local_now = now.astimezone(NEW_YORK)
    query_minute = local_now.hour * 60 + local_now.minute
    horizon_minutes = horizon_seconds // 60
    earliest = trading_date - timedelta(days=window_days)
    session_rates: list[float] = []
    raw_samples = raw_successes = 0
    sessions: list[str] = []
    root = Path(features_root) / "spx_standardized_samples"
    for path in sorted(root.glob("date=*/events.jsonl")):
        partition = _partition_date(path)
        if partition is None or partition < earliest or partition >= trading_date:
            continue
        try:
            stat = path.stat()
        except OSError:
            continue
        prices = dict(_load_standardized_session(str(path), stat.st_mtime_ns, stat.st_size))
        outcomes = []
        for minute, start in prices.items():
            if abs(minute - query_minute) > PIN_CLOCK_WINDOW_MINUTES:
                continue
            terminal = prices.get(minute + horizon_minutes)
            if terminal is None:
                continue
            projected = current_spot + terminal - start
            outcomes.append(lower_level < projected < upper_level)
        if not outcomes:
            continue
        raw_samples += len(outcomes)
        successes = sum(outcomes)
        raw_successes += successes
        session_rates.append(successes / len(outcomes))
        sessions.append(partition.isoformat())

    if not session_rates:
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
            cohort="same_clock_terminal_range",
            reason_codes=("physical_terminal_range_samples_unavailable",),
        )

    weighted_successes = sum(session_rates)
    effective = float(len(session_rates))
    alpha = prior_alpha + weighted_successes
    beta = prior_beta + effective - weighted_successes
    probability = alpha / (alpha + beta)
    interval_low, interval_high = beta_distribution.ppf((0.025, 0.975), alpha, beta)
    status = "estimated_uncalibrated" if effective >= minimum_samples else "insufficient_sample"
    reasons = [
        "not_fill_probability",
        "physical_terminal_range_same_clock_bootstrap",
        "research_unvalidated",
        "session_cluster_weighted",
    ]
    if status == "insufficient_sample":
        reasons.append("physical_sample_below_minimum")
    return PhysicalFollowThroughEstimate(
        status=status,
        probability=round(probability, 6),
        interval_low=round(float(interval_low), 6),
        interval_high=round(float(interval_high), 6),
        sample_count=raw_samples,
        success_count=raw_successes,
        session_count=len(sessions),
        horizon_seconds=horizon_seconds,
        trained_through_date=max(date.fromisoformat(value) for value in sessions),
        cohort="same_clock_terminal_range",
        reason_codes=tuple(sorted(reasons)),
        effective_sample_count=effective,
        historical_sessions=tuple(sessions),
        model_version="physical_terminal_range_bootstrap.v1",
        feature_set_version="rth_same_clock_return_window.v1",
        calibration_version="uncalibrated_session_weighted_beta.v1",
    )


def load_physical_spot_paths(
    features_root: Path,
    *,
    now: datetime,
    trading_date: date,
    window_days: int,
    horizon_minutes: int,
    clock_window_minutes: int = PIN_CLOCK_WINDOW_MINUTES,
    minimum_same_clock: int = 30,
    max_paths: int = 4000,
) -> tuple[tuple[PhysicalSpotPath, ...], str]:
    """Return causal 1-minute SPX windows from completed prior sessions.

    Same-clock paths (within ``clock_window_minutes`` of the decision minute)
    are preferred. If that cohort is too small — typical in GTH, where cash
    SPX samples only cover RTH — fall back to every contiguous window so the
    physical move library still has thousands of shapes. The current session
    is excluded.
    """

    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("physical spot-path now must be timezone-aware")
    if horizon_minutes <= 0 or window_days <= 0 or max_paths <= 0:
        raise ValueError("physical spot-path settings must be positive")

    query_minute = _new_york_minute(now)
    earliest = trading_date - timedelta(days=window_days)
    same_clock: list[PhysicalSpotPath] = []
    all_paths: list[PhysicalSpotPath] = []
    root = Path(features_root) / "spx_standardized_samples"
    for path in sorted(root.glob("date=*/events.jsonl")):
        partition = _partition_date(path)
        if partition is None or partition < earliest or partition >= trading_date:
            continue
        try:
            stat = path.stat()
        except OSError:
            continue
        prices = dict(_load_standardized_session(str(path), stat.st_mtime_ns, stat.st_size))
        if not prices:
            continue
        for start in prices:
            window = tuple(prices.get(start + offset) for offset in range(horizon_minutes + 1))
            if any(value is None for value in window):
                continue
            aligned = abs(start - query_minute) <= clock_window_minutes
            row = PhysicalSpotPath(
                session_date=partition,
                start_minute=start,
                prices=tuple(float(value) for value in window if value is not None),
                same_clock=aligned,
            )
            if len(row.prices) != horizon_minutes + 1:
                continue
            all_paths.append(row)
            if aligned:
                same_clock.append(row)

    if len(same_clock) >= minimum_same_clock:
        return _cap_paths(same_clock, max_paths), "same_clock"
    if all_paths:
        return _cap_paths(all_paths, max_paths), "session_shape_fallback"
    return (), "unavailable"


def load_iron_condor_clearing_paths(
    features_root: Path,
    *,
    now: datetime,
    trading_date: date,
    window_days: int,
    open_minute: int = RTH_OPEN_MINUTE,
    clear_minute: int = IRON_CONDOR_CLEAR_MINUTE,
) -> tuple[tuple[ClearingSpotPath, ...], str]:
    """Return one overnight-gap + RTH-to-12:30 path per completed session.

    GTH iron condors are not a 20-minute product. Each historical day contributes
    one path: prior close → RTH open gap, then 1-minute prints through 12:30 ET.
    During RTH before the clearing window, the overnight gap is omitted and the
    path starts at the current New York minute.
    """

    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("iron-condor clearing now must be timezone-aware")
    if window_days <= 0:
        raise ValueError("iron-condor clearing window_days must be positive")
    if not open_minute < clear_minute:
        raise ValueError("iron-condor clearing clock must be after the RTH open")

    query_minute = _new_york_minute(now)
    if query_minute >= clear_minute and open_minute <= query_minute <= 16 * 60:
        return (), "past_clearing_window"
    earliest = trading_date - timedelta(days=window_days)
    sessions: list[tuple[date, dict[int, float]]] = []
    root = Path(features_root) / "spx_standardized_samples"
    for path in sorted(root.glob("date=*/events.jsonl")):
        partition = _partition_date(path)
        if partition is None or partition < earliest or partition >= trading_date:
            continue
        try:
            stat = path.stat()
        except OSError:
            continue
        loaded = dict(_load_standardized_session(str(path), stat.st_mtime_ns, stat.st_size))
        if loaded:
            sessions.append((partition, loaded))
    if not sessions:
        return (), "unavailable"

    in_rth_to_clear = open_minute <= query_minute < clear_minute
    rows: list[ClearingSpotPath] = []
    for index, (partition, prices) in enumerate(sessions):
        if in_rth_to_clear:
            start = query_minute
            gap = 0.0
        else:
            start = open_minute
            if index == 0:
                continue
            prior = sessions[index - 1][1]
            prior_close = prior.get(max(prior))
            open_price = prices.get(open_minute)
            if prior_close is None or open_price is None:
                continue
            gap = open_price - prior_close
        window = tuple(
            prices.get(start + offset) for offset in range(clear_minute - start + 1)
        )
        if any(value is None for value in window):
            continue
        rows.append(
            ClearingSpotPath(
                session_date=partition,
                overnight_gap=float(gap),
                start_minute=start,
                prices=tuple(float(value) for value in window if value is not None),
            )
        )
    if not rows:
        return (), "unavailable"
    mode = "rth_to_clear" if in_rth_to_clear else "overnight_gap_and_rth_to_clear"
    return tuple(rows), mode


def _cap_paths(rows: list[PhysicalSpotPath], max_paths: int) -> tuple[PhysicalSpotPath, ...]:
    if len(rows) <= max_paths:
        return tuple(rows)
    step = len(rows) / float(max_paths)
    return tuple(rows[min(len(rows) - 1, int(index * step))] for index in range(max_paths))


def _new_york_minute(value: datetime) -> int:
    local = value.astimezone(NEW_YORK)
    return local.hour * 60 + local.minute


@lru_cache(maxsize=64)
def _load_standardized_session(
    path_text: str, _mtime_ns: int, _size: int
) -> tuple[tuple[int, float], ...]:
    """Return the last eligible SPX observation for each New York minute."""

    prices: dict[int, float] = {}
    try:
        lines = Path(path_text).read_text(encoding="utf-8").splitlines()
    except OSError:
        return ()
    for line in lines:
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(row, Mapping) or row.get("status") != "selected":
            continue
        selected = row.get("selected")
        if not isinstance(selected, Mapping):
            continue
        price = _finite(selected.get("price"))
        minute = _timestamp(row.get("minute"))
        if price is None or minute is None:
            continue
        local = minute.astimezone(NEW_YORK)
        prices[local.hour * 60 + local.minute] = price
    return tuple(sorted(prices.items()))


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
    "ClearingSpotPath",
    "PhysicalSpotPath",
    "estimate_physical_followthrough",
    "estimate_physical_terminal_range",
    "load_iron_condor_clearing_paths",
    "load_physical_spot_paths",
]
