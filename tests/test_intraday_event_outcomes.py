from __future__ import annotations

import json
import stat
from datetime import datetime, timedelta, timezone

import pytest

from spx_spark.intraday_event_outcomes import (
    IntradayEventOutcomeSettings,
    IntradayEventOutcomeTracker,
    IntradayOutcomeStoreError,
    SynchronizedSPXSample,
)


UTC = timezone.utc


def settings(tmp_path) -> IntradayEventOutcomeSettings:
    return IntradayEventOutcomeSettings(
        state_path=str(tmp_path / "latest" / "outcome-state.json"),
        results_path=str(tmp_path / "analysis" / "outcomes.jsonl"),
    )


def sample(at: datetime, spx: float, *, es_lag_seconds: float = 0.0) -> SynchronizedSPXSample:
    return SynchronizedSPXSample(
        spx=spx,
        spx_source_at=at,
        es_source_at=at - timedelta(seconds=es_lag_seconds),
    )


def jsonl(path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_tracks_shock_and_reclaim_with_5_15_30_minute_spx_outcomes(tmp_path) -> None:
    cfg = settings(tmp_path)
    tracker = IntradayEventOutcomeTracker(cfg)
    start = datetime(2026, 7, 10, 14, 32, tzinfo=UTC)

    shock_id = tracker.observe_event(
        event_id="spx_shock:20260710:down:1432",
        phase="shock",
        direction="down",
        sample=sample(start, 7500.0),
    )
    reclaim_id = tracker.observe_event(
        event_id="spx_shock:20260710:down:1432",
        phase="reclaim",
        direction="down",
        sample=sample(start + timedelta(minutes=2), 7480.0),
    )
    assert shock_id != reclaim_id

    points = (
        (3, 7470.0),
        (5, 7515.0),
        (7, 7520.0),
        (15, 7490.0),
        (20, 7530.0),
        (30, 7475.0),
        (32, 7500.0),
    )
    emitted: list[dict[str, object]] = []
    for minutes, price in points:
        emitted.extend(tracker.observe_sample(sample(start + timedelta(minutes=minutes), price)))

    assert len(emitted) == 6
    records = jsonl(tmp_path / "analysis" / "outcomes.jsonl")
    assert {(row["phase"], row["horizon_minutes"]) for row in records} == {
        ("shock", 5),
        ("shock", 15),
        ("shock", 30),
        ("reclaim", 5),
        ("reclaim", 15),
        ("reclaim", 30),
    }

    shock_15 = next(
        row for row in records if row["phase"] == "shock" and row["horizon_minutes"] == 15
    )
    assert shock_15["return_bps"] == pytest.approx(-13.3333333)
    assert shock_15["hypothesis_direction"] == "down"
    assert shock_15["mfe_bps"] == pytest.approx(40.0)
    assert shock_15["mae_bps"] == pytest.approx(-26.6666667)
    assert shock_15["path_high_return_bps"] == pytest.approx(26.6666667)
    assert shock_15["path_low_return_bps"] == pytest.approx(-40.0)

    reclaim_5 = next(
        row for row in records if row["phase"] == "reclaim" and row["horizon_minutes"] == 5
    )
    assert reclaim_5["return_bps"] == pytest.approx(53.4759358)
    assert reclaim_5["hypothesis_direction"] == "up"
    assert reclaim_5["mfe_bps"] == pytest.approx(53.4759358)
    assert reclaim_5["mae_bps"] == pytest.approx(-13.3689840)


def test_state_survives_restart_and_duplicate_inputs_are_idempotent(tmp_path) -> None:
    cfg = settings(tmp_path)
    start = datetime(2026, 7, 10, 15, 0, tzinfo=UTC)
    event_id = "spx_shock:20260710:up:1500"
    first = IntradayEventOutcomeTracker(cfg)
    observation_id = first.observe_event(
        event_id=event_id,
        phase="shock",
        direction="up",
        sample=sample(start, 7500.0),
    )
    first.observe_sample(sample(start + timedelta(minutes=2), 7510.0))

    restarted = IntradayEventOutcomeTracker(cfg)
    assert (
        restarted.observe_event(
            event_id=event_id,
            phase="shock",
            direction="up",
            sample=sample(start + timedelta(minutes=1), 7999.0),
        )
        == observation_id
    )
    at_five = sample(start + timedelta(minutes=5), 7520.0)
    first_emit = restarted.observe_sample(at_five)
    assert len(first_emit) == 1
    assert restarted.observe_sample(at_five) == ()
    assert restarted.flush_pending_results() == ()

    records = jsonl(tmp_path / "analysis" / "outcomes.jsonl")
    assert len(records) == 1
    assert records[0]["start_spx"] == 7500.0
    assert records[0]["sample_count"] == 2


def test_strategy_event_preserves_opaque_decision_link_and_call_direction(tmp_path) -> None:
    tracker = IntradayEventOutcomeTracker(settings(tmp_path))
    start = datetime(2026, 7, 10, 14, 0, tzinfo=UTC)
    tracker.observe_event(
        event_id="raw-strategy-event",
        phase="strategy",
        direction="up",
        sample=sample(start, 7500.0),
        event_key="evt_opaque",
        decision_id="dec_opaque",
    )

    records = tracker.observe_sample(sample(start + timedelta(minutes=5), 7515.0))

    assert len(records) == 1
    assert records[0]["event_key"] == "evt_opaque"
    assert records[0]["decision_id"] == "dec_opaque"
    assert records[0]["hypothesis_direction"] == "up"
    assert records[0]["return_bps"] == pytest.approx(20.0)


def test_journal_dedupes_crash_window_before_state_marks_emitted(tmp_path) -> None:
    cfg = settings(tmp_path)
    tracker = IntradayEventOutcomeTracker(cfg)
    start = datetime(2026, 7, 10, 14, 0, tzinfo=UTC)
    tracker.observe_event(
        event_id="safe-event",
        phase="shock",
        direction="down",
        sample=sample(start, 7500.0),
    )
    tracker.observe_sample(sample(start + timedelta(minutes=5), 7510.0))

    state_path = tmp_path / "latest" / "outcome-state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    observation = next(iter(state["observations"].values()))
    observation["horizons"]["5"]["emitted"] = False
    state_path.write_text(json.dumps(state), encoding="utf-8")

    restarted = IntradayEventOutcomeTracker(cfg)
    assert restarted.flush_pending_results() == ()
    assert len(jsonl(tmp_path / "analysis" / "outcomes.jsonl")) == 1
    repaired = json.loads(state_path.read_text(encoding="utf-8"))
    repaired_observation = next(iter(repaired["observations"].values()))
    assert repaired_observation["horizons"]["5"]["emitted"] is True


def test_completed_observations_are_pruned_after_configured_retention(tmp_path) -> None:
    base = settings(tmp_path)
    cfg = IntradayEventOutcomeSettings(
        state_path=base.state_path,
        results_path=base.results_path,
        completed_retention_seconds=60,
    )
    tracker = IntradayEventOutcomeTracker(cfg)
    start = datetime(2026, 7, 10, 14, 0, tzinfo=UTC)
    event_id = "spx_shock:20260710:down:1400"
    tracker.observe_event(
        event_id=event_id,
        phase="shock",
        direction="down",
        sample=sample(start, 7500.0),
    )
    for minutes, price in ((5, 7490.0), (15, 7510.0), (30, 7520.0)):
        tracker.observe_sample(sample(start + timedelta(minutes=minutes), price))

    state_path = tmp_path / "latest" / "outcome-state.json"
    before = json.loads(state_path.read_text(encoding="utf-8"))
    assert len(before["observations"]) == 1
    observation = next(iter(before["observations"].values()))
    assert len(observation["samples"]) == 4
    assert all(metric["emitted"] for metric in observation["horizons"].values())

    tracker.observe_sample(sample(start + timedelta(minutes=30, seconds=59), 7521.0))
    retained = json.loads(state_path.read_text(encoding="utf-8"))
    assert len(retained["observations"]) == 1

    tracker.observe_sample(sample(start + timedelta(minutes=31), 7522.0))
    pruned = json.loads(state_path.read_text(encoding="utf-8"))
    assert pruned["observations"] == {}
    results_path = tmp_path / "analysis" / "outcomes.jsonl"
    assert len(jsonl(results_path)) == 3

    # Re-observing the same event after state retention expires can do work again,
    # but the append-only journal still de-duplicates every horizon record.
    tracker.observe_event(
        event_id=event_id,
        phase="shock",
        direction="down",
        sample=sample(start + timedelta(hours=1), 7525.0),
    )
    for minutes, price in ((5, 7530.0), (15, 7540.0), (30, 7550.0)):
        tracker.observe_sample(sample(start + timedelta(hours=1, minutes=minutes), price))
    assert len(jsonl(results_path)) == 3


def test_files_are_owner_only_and_raw_identifiers_never_persist(tmp_path) -> None:
    cfg = settings(tmp_path)
    tracker = IntradayEventOutcomeTracker(cfg)
    start = datetime(2026, 7, 10, 14, 0, tzinfo=UTC)
    sensitive_id = "acct-U123456-token=do-not-store@example.invalid"
    opaque_id = tracker.observe_event(
        event_id=sensitive_id,
        phase="shock",
        direction="down",
        sample=sample(start, 7500.0),
    )
    tracker.observe_sample(sample(start + timedelta(minutes=5), 7510.0))

    state_path = tmp_path / "latest" / "outcome-state.json"
    results_path = tmp_path / "analysis" / "outcomes.jsonl"
    combined = state_path.read_text(encoding="utf-8") + results_path.read_text(encoding="utf-8")
    assert sensitive_id not in combined
    assert "U123456" not in combined
    assert "token=" not in combined
    assert "@example.invalid" not in combined
    assert opaque_id in combined
    assert stat.S_IMODE(state_path.stat().st_mode) == 0o600
    assert stat.S_IMODE(results_path.stat().st_mode) == 0o600
    assert stat.S_IMODE((tmp_path / "latest" / "outcome-state.json.lock").stat().st_mode) == 0o600
    assert stat.S_IMODE((tmp_path / "analysis" / "outcomes.jsonl.lock").stat().st_mode) == 0o600


def test_requires_synchronized_finite_samples(tmp_path) -> None:
    tracker = IntradayEventOutcomeTracker(settings(tmp_path))
    start = datetime(2026, 7, 10, 14, 0, tzinfo=UTC)
    with pytest.raises(ValueError, match="not synchronized"):
        tracker.observe_event(
            event_id="event",
            phase="shock",
            direction="down",
            sample=sample(start, 7500.0, es_lag_seconds=6.0),
        )
    with pytest.raises(ValueError, match="positive finite"):
        tracker.observe_event(
            event_id="event",
            phase="shock",
            direction="down",
            sample=sample(start, float("nan")),
        )


def test_missing_sample_near_horizon_is_explicitly_incomplete(tmp_path) -> None:
    tracker = IntradayEventOutcomeTracker(settings(tmp_path))
    start = datetime(2026, 7, 10, 14, 0, tzinfo=UTC)
    tracker.observe_event(
        event_id="event",
        phase="reclaim",
        direction="down",
        sample=sample(start, 7500.0),
    )
    records = tracker.observe_sample(sample(start + timedelta(minutes=6), 7510.0))
    assert len(records) == 1
    assert records[0]["horizon_minutes"] == 5
    assert records[0]["status"] == "incomplete"
    assert records[0]["reason"] == "no_synchronized_sample_near_target"
    assert records[0]["return_bps"] is None


def test_horizon_uses_nearest_sample_without_future_path_leakage(tmp_path) -> None:
    tracker = IntradayEventOutcomeTracker(settings(tmp_path))
    start = datetime(2026, 7, 10, 14, 0, tzinfo=UTC)
    tracker.observe_event(
        event_id="event",
        phase="shock",
        direction="up",
        sample=sample(start, 7500.0),
    )
    tracker.observe_sample(sample(start + timedelta(minutes=4, seconds=50), 7510.0))
    records = tracker.observe_sample(sample(start + timedelta(minutes=5, seconds=20), 7600.0))

    assert len(records) == 1
    assert records[0]["sample_at"] == (start + timedelta(minutes=4, seconds=50)).isoformat()
    assert records[0]["return_bps"] == pytest.approx(13.3333333)
    assert records[0]["mfe_bps"] == pytest.approx(13.3333333)
    assert records[0]["sample_count"] == 1


def test_corrupt_state_is_not_silently_overwritten(tmp_path) -> None:
    cfg = settings(tmp_path)
    state_path = tmp_path / "latest" / "outcome-state.json"
    state_path.parent.mkdir(parents=True)
    original = b"{not-json"
    state_path.write_bytes(original)
    tracker = IntradayEventOutcomeTracker(cfg)
    start = datetime(2026, 7, 10, 14, 0, tzinfo=UTC)

    with pytest.raises(IntradayOutcomeStoreError, match="unreadable"):
        tracker.observe_event(
            event_id="event",
            phase="shock",
            direction="down",
            sample=sample(start, 7500.0),
        )
    assert state_path.read_bytes() == original


def test_result_template_partitions_by_new_york_trading_date(tmp_path) -> None:
    cfg = IntradayEventOutcomeSettings(
        state_path=str(tmp_path / "latest" / "state.json"),
        results_path=str(
            tmp_path
            / "features"
            / "intraday_event_outcomes"
            / "date={trading_date}"
            / "outcomes.jsonl"
        ),
    )
    tracker = IntradayEventOutcomeTracker(cfg)
    start = datetime(2026, 7, 11, 0, 30, tzinfo=UTC)  # July 10 in New York.
    tracker.observe_event(
        event_id="partitioned-event",
        phase="shock",
        direction="up",
        sample=sample(start, 7500.0),
    )
    tracker.observe_sample(sample(start + timedelta(minutes=5), 7510.0))

    path = tmp_path / "features" / "intraday_event_outcomes" / "date=2026-07-10" / "outcomes.jsonl"
    assert path.exists()
    assert len(jsonl(path)) == 1
