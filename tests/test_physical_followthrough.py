from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from spx_spark.application.market_features.physical_followthrough import (
    estimate_physical_followthrough,
)


NOW = datetime(2026, 8, 5, 14, 0, tzinfo=timezone.utc)


def _write(root: Path, day: str, rows: list[dict[str, object]]) -> None:
    path = root / "level_decision_outcomes" / f"date={day}" / "outcomes.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def _row(
    event: str,
    value: float,
    *,
    direction: str = "up",
    thesis: str = "breakout",
    horizon: int = 300,
    status: str = "complete",
) -> dict[str, object]:
    return {
        "event_id": event,
        "status": status,
        "horizon_seconds": horizon,
        "return_bps": value,
        "direction": direction,
        "thesis": thesis,
        "level_kind": "call_wall",
        "completed_at": "2026-08-04T15:00:00+00:00",
    }


def test_estimate_is_directional_deduplicated_and_prior_day_only(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "2026-08-04",
        [
            _row("up-win", 4.0),
            _row("up-loss", -2.0),
            _row("down-win", -3.0, direction="down"),
            _row("duplicate", 5.0),
            _row("duplicate", -5.0),
            _row("wrong-horizon", 5.0, horizon=60),
            _row("incomplete", 5.0, status="incomplete"),
        ],
    )
    _write(tmp_path, "2026-08-05", [_row("same-day-future-label", 10.0)])

    estimate = estimate_physical_followthrough(
        tmp_path,
        now=NOW,
        trading_date=date(2026, 8, 5),
        horizon_seconds=300,
        window_days=35,
        minimum_samples=3,
        prior_alpha=1.0,
        prior_beta=1.0,
        direction="up",
        thesis="breakout",
    )

    assert estimate.status == "estimated_uncalibrated"
    # The v2 nearest-neighbour model keeps the opposite-direction observation
    # with a lower similarity weight instead of hard-filtering it away.
    assert estimate.sample_count == 4
    assert estimate.success_count == 3
    assert estimate.probability == pytest.approx(0.620798)
    assert estimate.trained_through_date == date(2026, 8, 4)
    assert estimate.session_count == 1


def test_sparse_requested_cohort_stays_an_explicit_nearest_neighbor_estimate(
    tmp_path: Path,
) -> None:
    _write(
        tmp_path,
        "2026-08-04",
        [
            _row("up", 2.0),
            _row("down", -2.0, direction="down", thesis="fade"),
        ],
    )

    estimate = estimate_physical_followthrough(
        tmp_path,
        now=NOW,
        trading_date=date(2026, 8, 5),
        horizon_seconds=300,
        window_days=35,
        minimum_samples=3,
        prior_alpha=1.0,
        prior_beta=1.0,
        direction="up",
        thesis="breakout",
    )

    assert estimate.status == "insufficient_sample"
    assert estimate.cohort == "nearest_neighbors"
    assert estimate.sample_count == 2
    assert "nearest_neighbor_sparse_shrinkage_input" in estimate.reason_codes
    assert "physical_sample_below_minimum" in estimate.reason_codes


def test_missing_history_is_an_explicit_unavailable_result(tmp_path: Path) -> None:
    estimate = estimate_physical_followthrough(
        tmp_path,
        now=NOW,
        trading_date=date(2026, 8, 5),
        horizon_seconds=300,
        window_days=35,
        minimum_samples=30,
        prior_alpha=1.0,
        prior_beta=1.0,
    )

    assert estimate.status == "unavailable"
    assert estimate.probability is None
    assert estimate.reason_codes == ("physical_outcomes_unavailable",)
