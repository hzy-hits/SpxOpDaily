"""Session-health coverage used by the forward strategy readiness gate."""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR, ET


GTH_OPEN_ET = time(20, 15)
GTH_CLOSE_ET = time(9, 25)


def measure_session_completeness(
    features_root: Path,
    *,
    cutoff_at: datetime,
    minimum_coverage: float = 0.90,
) -> list[dict[str, object]]:
    """Measure GTH and RTH minute coverage without reading ``quality_ok``.

    The GTH window for trading date D is 20:15 ET on D-1 through 09:25 ET on
    D. RTH uses the canonical calendar, including scheduled early closes.
    Both intervals are half-open and each distinct wall-clock minute counts at
    most once, so faster duplicate health samples cannot inflate coverage.
    """

    if not 0 < minimum_coverage <= 1:
        raise ValueError("minimum coverage must be in (0, 1]")
    cutoff = _utc(cutoff_at)
    grouped: dict[date, list[datetime]] = defaultdict(list)
    detector_grouped: dict[date, list[datetime]] = defaultdict(list)
    root = Path(features_root).expanduser().resolve()
    pattern = root / "level_decision_health/date=*"
    for partition in sorted(root.glob(str(pattern.relative_to(root)))):
        for path in sorted(partition.glob("*.jsonl")):
            for payload in _read_json_objects(path):
                at = _parse_time(payload.get("at"))
                session_day = _parse_date(payload.get("session_date"))
                if at is None or session_day is None or at >= cutoff:
                    continue
                grouped[session_day].append(at)

    detector_pattern = root / "gth_detector_health/date=*"
    for partition in sorted(root.glob(str(detector_pattern.relative_to(root)))):
        for path in sorted(partition.glob("*.jsonl")):
            for payload in _read_json_objects(path):
                at = _parse_time(payload.get("at"))
                session_day = _parse_date(payload.get("session_date"))
                if at is None or session_day is None or at >= cutoff:
                    continue
                detector_grouped[session_day].append(at)
    detector_started = min(detector_grouped, default=None)
    detector_started_at = min(
        (sample for samples in detector_grouped.values() for sample in samples),
        default=None,
    )

    rows: list[dict[str, object]] = []
    for session_day, samples in sorted(grouped.items()):
        session = DEFAULT_MARKET_CALENDAR.session(session_day)
        if session is None or session.close_at.astimezone(timezone.utc) > cutoff:
            continue
        gth_start = datetime.combine(
            session_day - timedelta(days=1),
            GTH_OPEN_ET,
            tzinfo=ET,
        )
        gth_end = datetime.combine(session_day, GTH_CLOSE_ET, tzinfo=ET)
        gth = _window_coverage(samples, gth_start, gth_end)
        rth = _window_coverage(samples, session.open_at, session.close_at)
        detector_required = detector_started is not None and session_day >= detector_started
        detector_gth = (
            _window_coverage(detector_grouped.get(session_day, ()), gth_start, gth_end)
            if detector_required
            else None
        )
        reasons = []
        if gth["coverage_ratio"] < minimum_coverage:
            reasons.append("gth_minute_coverage_below_90_percent")
        if rth["coverage_ratio"] < minimum_coverage:
            reasons.append("rth_minute_coverage_below_90_percent")
        if detector_gth is not None and detector_gth["coverage_ratio"] < minimum_coverage:
            reasons.append("gth_detector_health_coverage_below_90_percent")
        rth_complete = rth["coverage_ratio"] >= minimum_coverage
        gth_complete = bool(
            gth["coverage_ratio"] >= minimum_coverage
            and (detector_gth is None or detector_gth["coverage_ratio"] >= minimum_coverage)
        )
        rows.append(
            {
                "session_date": session_day.isoformat(),
                "rth_complete": rth_complete,
                "gth_complete": gth_complete,
                "complete": not reasons,
                "gth": gth,
                "rth": rth,
                "gth_detector_health": detector_gth,
                "gth_detector_health_required": detector_required,
                "gth_detector_health_started_session": (
                    detector_started.isoformat() if detector_started is not None else None
                ),
                "gth_detector_health_started_at": (
                    detector_started_at.isoformat() if detector_started_at is not None else None
                ),
                "reasons": reasons,
            }
        )
    return rows


def _window_coverage(
    samples: Sequence[datetime],
    start: datetime,
    end: datetime,
) -> dict[str, object]:
    start_utc = start.astimezone(timezone.utc)
    end_utc = end.astimezone(timezone.utc)
    expected = int((end_utc - start_utc).total_seconds() // 60)
    minutes = {
        sample.astimezone(timezone.utc).replace(second=0, microsecond=0)
        for sample in samples
        if start_utc <= sample.astimezone(timezone.utc) < end_utc
    }
    ratio = min(len(minutes) / expected, 1.0) if expected else 0.0
    return {
        "observed_minutes": len(minutes),
        "expected_minutes": expected,
        "coverage_ratio": round(ratio, 6),
    }


def _read_json_objects(path: Path) -> list[Mapping[str, object]]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []
    rows: list[Mapping[str, object]] = []
    for line in lines:
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _parse_time(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


def _parse_date(value: object) -> date | None:
    if not isinstance(value, str):
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("readiness timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)
