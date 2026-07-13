"""Empirical first-touch calibration from completed pricing outcomes."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable, Mapping


@dataclass(frozen=True)
class TouchTimeEstimate:
    calibrated: bool
    source: str
    sample_count: int
    session_count: int
    early_fraction: float | None
    base_fraction: float | None
    late_fraction: float | None
    cohort: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def estimate_touch_time(
    data_root: str,
    *,
    distance_over_em: float | None,
    session_bucket: str,
    volatility_regime: str,
    trend_regime: str,
    min_sessions: int = 5,
    min_samples: int = 20,
) -> TouchTimeEstimate:
    rows = list(_outcomes(data_root))
    distance_bucket = _distance_bucket(distance_over_em)
    cohort = {
        "distance_bucket": distance_bucket,
        "session_bucket": session_bucket,
        "volatility_regime": volatility_regime,
        "trend_regime": trend_regime,
    }
    matched = [
        row
        for row in rows
        if _distance_bucket(_number(row.get("distance_over_em"))) == distance_bucket
        and row.get("session_bucket") == session_bucket
        and row.get("volatility_regime") == volatility_regime
        and row.get("trend_regime") == trend_regime
        and _number(row.get("actual_touch_fraction")) is not None
    ]
    sessions = {str(row.get("session_date")) for row in matched if row.get("session_date")}
    if len(matched) < min_samples or len(sessions) < min_sessions:
        return TouchTimeEstimate(
            calibrated=False,
            source="collecting_outcomes",
            sample_count=len(matched),
            session_count=len(sessions),
            early_fraction=None,
            base_fraction=None,
            late_fraction=None,
            cohort=cohort,
        )
    values = sorted(min(max(float(row["actual_touch_fraction"]), 0.01), 0.95) for row in matched)
    return TouchTimeEstimate(
        calibrated=True,
        source="empirical_first_touch_cohort",
        sample_count=len(values),
        session_count=len(sessions),
        early_fraction=_quantile(values, 0.25),
        base_fraction=_quantile(values, 0.50),
        late_fraction=_quantile(values, 0.75),
        cohort=cohort,
    )


def _outcomes(data_root: str) -> Iterable[Mapping[str, object]]:
    root = Path(data_root) / "features" / "pricing_outcomes"
    for path in sorted(root.glob("date=*/outcomes.jsonl")):
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except OSError:
            continue
        for line in lines:
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                yield payload


def _distance_bucket(value: float | None) -> str:
    if value is None:
        return "unknown"
    if value < 0.25:
        return "lt_0.25"
    if value < 0.50:
        return "0.25_0.50"
    if value < 1.0:
        return "0.50_1.00"
    return "gte_1.00"


def _quantile(values: list[float], fraction: float) -> float:
    index = (len(values) - 1) * fraction
    lower = int(index)
    upper = min(lower + 1, len(values) - 1)
    weight = index - lower
    return values[lower] * (1.0 - weight) + values[upper] * weight


def _number(value: object) -> float | None:
    if not isinstance(value, int | float):
        return None
    parsed = float(value)
    return parsed if parsed == parsed else None
