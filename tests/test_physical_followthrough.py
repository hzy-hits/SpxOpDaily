from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest
import numpy as np

from spx_spark.application.market_features import physical_close_convergence
from spx_spark.application.market_features.physical_close_convergence import (
    estimate_physical_close_convergence,
)
from spx_spark.application.market_features.physical_followthrough import (
    estimate_physical_followthrough,
    estimate_physical_terminal_range,
)


NOW = datetime(2026, 8, 5, 14, 0, tzinfo=timezone.utc)


def test_close_convergence_clock_fails_closed_before_quote_lake_scan(
    tmp_path: Path,
) -> None:
    estimate = estimate_physical_close_convergence(
        tmp_path,
        now=datetime(2026, 8, 6, 18, 59, 59, tzinfo=timezone.utc),
        trading_date=date(2026, 8, 6),
    )

    assert estimate.status == "unavailable"
    assert estimate.reason_codes == ("close_convergence_clock_closed",)
    assert estimate.training_sessions == 0


def test_close_convergence_uses_prior_sessions_and_frozen_1500_prefix(
    monkeypatch,
    tmp_path: Path,
) -> None:
    history = (
        "2026-07-13",
        "2026-07-14",
        "2026-07-15",
        "2026-07-16",
        "2026-07-17",
        "2026-07-20",
        "2026-07-21",
        "2026-07-22",
        "2026-07-23",
        "2026-07-24",
        "2026-07-27",
        "2026-07-28",
        "2026-07-29",
        "2026-07-30",
        "2026-07-31",
        "2026-08-03",
        "2026-08-04",
        "2026-08-05",
    )
    quote_root = tmp_path / "lake" / "quotes" / "schema=v1"
    for day in history:
        (quote_root / f"date={day}" / "provider=schwab").mkdir(parents=True)
    calls: list[tuple[date, datetime, bool]] = []

    def load_path(
        _connection,
        *,
        quote_root: Path,
        session_date: date,
        available_at: datetime,
        complete: bool,
        allow_partial: bool = False,
    ):
        del quote_root, allow_partial
        calls.append((session_date, available_at, complete))
        position = (
            history.index(session_date.isoformat())
            if session_date.isoformat() in history
            else len(history)
        )
        minute = np.arange(390, dtype=float)
        spx = (
            7600.0
            + position * 1.5
            + minute * (0.018 + position * 0.0002)
            + np.sin(minute / 19.0 + position * 0.2) * 1.8
        )
        es = spx + 25.0 + np.cos(minute / 23.0 + position * 0.1)
        return physical_close_convergence._CloseSessionPath(
            session_date=session_date,
            epoch_seconds=np.arange(390, dtype=np.int64),
            spx=spx,
            es=es,
            spx_coverage=1.0,
            es_coverage=1.0,
        )

    monkeypatch.setattr(
        physical_close_convergence,
        "_load_close_session_path",
        load_path,
    )
    estimate = estimate_physical_close_convergence(
        tmp_path,
        now=datetime(2026, 8, 6, 19, 0, 30, tzinfo=timezone.utc),
        trading_date=date(2026, 8, 6),
    )

    assert estimate.status == "ready"
    assert estimate.training_sessions == len(history)
    assert estimate.trained_through_date == date(2026, 8, 5)
    assert len(estimate.settlement_quantiles) == 51
    assert estimate.q10 < estimate.q50 < estimate.q90
    assert estimate.center is not None and estimate.center % 5.0 == 0.0
    assert all(session_date < date(2026, 8, 6) for session_date, _, _ in calls[:-1])
    assert calls[-1] == (
        date(2026, 8, 6),
        physical_close_convergence.DEFAULT_MARKET_CALENDAR.session(
            date(2026, 8, 6)
        ).close_at
        - timedelta(minutes=60),
        False,
    )


def test_close_convergence_merges_live_state_after_last_compacted_minute(
    tmp_path: Path,
) -> None:
    session_date = date(2026, 8, 6)
    session = physical_close_convergence.DEFAULT_MARKET_CALENDAR.session(session_date)
    assert session is not None
    timeline = np.arange(
        int(session.open_at.timestamp()) + 60,
        int(session.close_at.timestamp()) + 1,
        60,
        dtype=np.int64,
    )
    decision_index = len(timeline) - 61
    spx = np.full(len(timeline), np.nan)
    es = np.full(len(timeline), np.nan)
    spx[:250] = 7700.0 + np.arange(250) * 0.02
    es[:250] = 7725.0 + np.arange(250) * 0.02
    base = physical_close_convergence._CloseSessionPath(
        session_date=session_date,
        epoch_seconds=timeline,
        spx=spx,
        es=es,
        spx_coverage=250 / (decision_index + 1),
        es_coverage=250 / (decision_index + 1),
    )
    samples = []
    for position in range(250, decision_index + 1):
        observed = datetime.fromtimestamp(int(timeline[position]) - 1, timezone.utc)
        samples.append(
            {
                "at": observed.isoformat(),
                "instruments": {
                    "index:SPX": {
                        "provider": "schwab",
                        "quality": "live",
                        "price": 7705.0 + position * 0.02,
                        "source_at": observed.isoformat(),
                    }
                },
                "es_by_provider": {
                    "schwab": {
                        "provider": "schwab",
                        "quality": "live",
                        "price": 7730.0 + position * 0.02,
                        "source_at": observed.isoformat(),
                    }
                },
            }
        )
    state_path = tmp_path / "latest" / "market_feature_state.json"
    state_path.parent.mkdir(parents=True)
    state_path.write_text(json.dumps({"market_samples": samples}), encoding="utf-8")

    merged = physical_close_convergence._merge_close_current_state(
        base,
        data_root=tmp_path,
        trading_date=session_date,
        available_at=session.close_at - timedelta(minutes=60),
    )

    assert merged is not None
    assert merged.spx_coverage == 1.0
    assert merged.es_coverage == 1.0
    assert merged.spx[decision_index] == pytest.approx(7705.0 + decision_index * 0.02)


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


def test_terminal_range_bootstrap_is_prior_day_and_session_weighted(tmp_path: Path) -> None:
    for day, terminal in (("2026-08-03", 101.0), ("2026-08-04", 110.0)):
        path = tmp_path / "spx_standardized_samples" / f"date={day}" / "events.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        rows = [
            {
                "status": "selected",
                "minute": f"{day}T14:00:00+00:00",
                "observed_at": f"{day}T14:00:00+00:00",
                "selected": {"price": 100.0},
            },
            {
                "status": "selected",
                "minute": f"{day}T14:05:00+00:00",
                "observed_at": f"{day}T14:05:00+00:00",
                "selected": {"price": terminal},
            },
        ]
        path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")

    estimate = estimate_physical_terminal_range(
        tmp_path,
        now=NOW,
        trading_date=date(2026, 8, 5),
        horizon_seconds=300,
        window_days=35,
        minimum_samples=30,
        prior_alpha=1.0,
        prior_beta=1.0,
        current_spot=100.0,
        lower_level=99.0,
        upper_level=105.0,
    )

    assert estimate.status == "insufficient_sample"
    assert estimate.sample_count == 2
    assert estimate.success_count == 1
    assert estimate.session_count == 2
    assert estimate.effective_sample_count == 2.0
    assert estimate.probability == pytest.approx(0.5)
    assert estimate.historical_sessions == ("2026-08-03", "2026-08-04")
