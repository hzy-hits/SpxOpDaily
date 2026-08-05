from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from spx_spark.application.market_features.models import (
    FrameQuality,
    L1MicrostructureFrame,
    OptionStructureFrame,
)
from spx_spark.application.market_features.physical_followthrough import (
    PhysicalFollowThroughEstimate,
)
from spx_spark.application.market_features.strategy_distribution_forecast import (
    Q_METHOD_VERSION,
    build_strategy_distribution_forecast,
    clear_strategy_distribution_forecast_cache,
    latest_strategy_distribution_forecast_path,
    process_strategy_distribution_forecast,
    strategy_distribution_forecast_audit_path,
)
from spx_spark.marketdata import InstrumentId, MarketDataQuality, Provider, Quote
from spx_spark.settings.strategy_distribution import StrategyDistributionSettings
from spx_spark.storage import LatestState


NOW = datetime(2026, 8, 5, 14, 0, tzinfo=timezone.utc)
TRADING_DATE = date(2026, 8, 5)


@pytest.fixture(autouse=True)
def _clear_physical_cache() -> None:
    clear_strategy_distribution_forecast_cache()


def _settings(**overrides: object) -> StrategyDistributionSettings:
    values: dict[str, object] = {
        "enabled": True,
        "horizon_seconds": 300,
        "window_days": 35,
        "refresh_seconds": 60.0,
        "projection_ttl_seconds": 90.0,
        "append_interval_seconds": 60.0,
        "minimum_physical_samples": 1,
        "beta_prior_alpha": 1.0,
        "beta_prior_beta": 1.0,
    }
    values.update(overrides)
    return StrategyDistributionSettings(**values)  # type: ignore[arg-type]


def _state(at: datetime, *, spot: float | None = 7760.0) -> LatestState:
    if spot is None:
        return LatestState(created_at=at, as_of=at, quotes=(), best_quotes=())
    quote = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.SCHWAB,
        received_at=at,
        quality=MarketDataQuality.LIVE,
        mark=spot,
        quote_time=at,
    )
    return LatestState(
        created_at=at,
        as_of=at,
        quotes=(quote,),
        best_quotes=(quote,),
    )


def _option_frame(
    at: datetime,
    *,
    atm_iv: float | None = 0.20,
    quality: FrameQuality = FrameQuality.READY,
) -> OptionStructureFrame:
    return OptionStructureFrame(
        schema_version=1,
        frame_id=f"option-frame:{at.isoformat()}",
        as_of=at,
        quality=quality,
        front_expiry="20260805",
        next_expiry="20260806",
        structure={},
        volatility={"atm_iv_0dte": atm_iv},
        concentration={},
        density={},
        l1=L1MicrostructureFrame(
            quality=quality,
            expiry="20260805",
            contract_count=40,
            metrics={},
            diagnostics={},
        ),
        diagnostics={},
    )


def _decision(
    at: datetime,
    *,
    direction: str | None = "up",
    event_id: str = "level:7760:breakout",
) -> dict[str, object]:
    return {
        "event_id": event_id,
        "phase": "confirmed",
        "direction": direction,
        "thesis": "breakout",
        "updated_at": at.isoformat(),
        "expires_at": (at + timedelta(hours=1)).isoformat(),
    }


def _estimate(
    *,
    probability: float = 0.60,
    trained_through_date: date | None = date(2026, 8, 4),
) -> PhysicalFollowThroughEstimate:
    return PhysicalFollowThroughEstimate(
        status="estimated_uncalibrated",
        probability=probability,
        interval_low=0.42,
        interval_high=0.75,
        sample_count=40,
        success_count=24,
        session_count=5,
        horizon_seconds=300,
        trained_through_date=trained_through_date,
        cohort="direction_thesis",
        reason_codes=("not_fill_probability", "research_unvalidated"),
    )


def _write_outcomes(root: Path, day: str, rows: list[dict[str, object]]) -> None:
    path = root / "features" / "level_decision_outcomes" / f"date={day}" / "outcomes.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )


def _outcome(event_id: str, value: float, *, day: str) -> dict[str, object]:
    return {
        "event_id": event_id,
        "status": "complete",
        "horizon_seconds": 300,
        "return_bps": value,
        "direction": "up",
        "thesis": "breakout",
        "level_kind": "call_wall",
        "completed_at": f"{day}T19:00:00+00:00",
    }


def _lines(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_producer_compares_same_event_and_publishes_formal_no_trade(
    tmp_path: Path,
) -> None:
    _write_outcomes(
        tmp_path,
        "2026-08-04",
        [
            _outcome("prior-win", 4.0, day="2026-08-04"),
            _outcome("prior-loss", -2.0, day="2026-08-04"),
        ],
    )

    payload = process_strategy_distribution_forecast(
        data_root=tmp_path,
        action_state=_state(NOW),
        option_frame=_option_frame(NOW),
        raw_level_decision=_decision(NOW),
        now=NOW,
        settings=_settings(),
        trading_date=TRADING_DATE,
    )

    assert payload["schema_version"] == "strategy_distribution_forecast.v1"
    assert payload["q_event"]["event"] == payload["p_event"]["event"]  # type: ignore[index]
    event = payload["q_event"]["event"]  # type: ignore[index]
    assert event["kind"] == "terminal_above"  # type: ignore[index]
    assert event["lower_level"] == 7760.0  # type: ignore[index]
    assert payload["q_event"]["method_version"] == Q_METHOD_VERSION  # type: ignore[index]
    assert payload["q_event"]["quality"] == "degraded"  # type: ignore[index]
    assert payload["p_event"]["sample_count"] == 2  # type: ignore[index]
    assert payload["p_event"]["session_count"] == 1  # type: ignore[index]
    assert payload["p_event"]["trained_through_date"] == "2026-08-04"  # type: ignore[index]
    assert payload["p_event"]["interval_low"] is not None  # type: ignore[index]
    assert payload["p_event"]["interval_high"] is not None  # type: ignore[index]
    assert payload["strategy_candidates"] == []
    assert payload["shadow_decision"]["action"] == "no_trade"  # type: ignore[index]
    assert payload["action_authority"] == "none"
    assert payload["automatic_ordering"] is False

    latest = json.loads(
        latest_strategy_distribution_forecast_path(tmp_path).read_text(encoding="utf-8")
    )
    assert latest == payload
    audit = _lines(strategy_distribution_forecast_audit_path(tmp_path, TRADING_DATE))
    assert audit == [payload]


def test_down_direction_defines_the_matching_terminal_below_event(tmp_path: Path) -> None:
    def estimator(*_args: object, **_kwargs: object) -> PhysicalFollowThroughEstimate:
        return _estimate()

    payload = process_strategy_distribution_forecast(
        data_root=tmp_path,
        action_state=_state(NOW),
        option_frame=_option_frame(NOW),
        raw_level_decision=_decision(NOW, direction="down"),
        now=NOW,
        settings=_settings(),
        trading_date=TRADING_DATE,
        physical_estimator=estimator,
    )

    assert payload["q_event"]["event"] == payload["p_event"]["event"]  # type: ignore[index]
    event = payload["q_event"]["event"]  # type: ignore[index]
    assert event["kind"] == "terminal_below"  # type: ignore[index]
    assert event["upper_level"] == 7760.0  # type: ignore[index]


def test_unconfirmed_phase_cannot_borrow_the_confirmed_event_baseline(tmp_path: Path) -> None:
    calls = 0

    def estimator(*_args: object, **_kwargs: object) -> PhysicalFollowThroughEstimate:
        nonlocal calls
        calls += 1
        raise AssertionError("unconfirmed phase must not run the physical estimator")

    decision = _decision(NOW)
    decision["phase"] = "testing"
    payload = process_strategy_distribution_forecast(
        data_root=tmp_path,
        action_state=_state(NOW),
        option_frame=_option_frame(NOW),
        raw_level_decision=decision,
        now=NOW,
        settings=_settings(),
        trading_date=TRADING_DATE,
        physical_estimator=estimator,
    )

    assert calls == 0
    assert payload["q_event"]["event"] is None  # type: ignore[index]
    assert payload["p_event"]["event"] is None  # type: ignore[index]
    assert "level_phase_not_confirmed" in payload["quality_reason_codes"]  # type: ignore[operator]


@pytest.mark.parametrize(
    ("direction", "spot", "reason"),
    [
        (None, 7760.0, "level_direction_unavailable"),
        ("up", None, "action_spx_quote_unavailable"),
    ],
)
def test_missing_direction_or_action_spot_is_a_formal_no_trade_without_fake_probability(
    tmp_path: Path,
    direction: str | None,
    spot: float | None,
    reason: str,
) -> None:
    calls = 0

    def estimator(*_args: object, **_kwargs: object) -> PhysicalFollowThroughEstimate:
        nonlocal calls
        calls += 1
        raise AssertionError("physical estimator must not run without a directional event")

    payload = process_strategy_distribution_forecast(
        data_root=tmp_path,
        action_state=_state(NOW, spot=spot),
        option_frame=_option_frame(NOW),
        raw_level_decision=_decision(NOW, direction=direction),
        now=NOW,
        settings=_settings(),
        trading_date=TRADING_DATE,
        physical_estimator=estimator,
    )

    assert calls == 0
    assert payload["quality"] == "unavailable"
    assert payload["q_event"]["event"] is None  # type: ignore[index]
    assert payload["p_event"]["event"] is None  # type: ignore[index]
    assert payload["q_event"]["probability"] is None  # type: ignore[index]
    assert payload["p_event"]["probability"] is None  # type: ignore[index]
    assert payload["shadow_decision"]["action"] == "no_trade"  # type: ignore[index]
    assert reason in payload["quality_reason_codes"]  # type: ignore[operator]


def test_physical_estimate_excludes_current_trading_date(tmp_path: Path) -> None:
    _write_outcomes(
        tmp_path,
        "2026-08-04",
        [_outcome("prior-loss", -3.0, day="2026-08-04")],
    )
    _write_outcomes(
        tmp_path,
        "2026-08-05",
        [_outcome("same-day-future-win", 100.0, day="2026-08-05")],
    )

    payload = process_strategy_distribution_forecast(
        data_root=tmp_path,
        action_state=_state(NOW),
        option_frame=_option_frame(NOW),
        raw_level_decision=_decision(NOW),
        now=NOW,
        settings=_settings(),
        trading_date=TRADING_DATE,
    )

    physical = payload["p_event"]
    assert physical["sample_count"] == 1  # type: ignore[index]
    assert physical["probability"] == pytest.approx(1 / 3)  # type: ignore[index]
    assert physical["trained_through_date"] == "2026-08-04"  # type: ignore[index]


def test_audit_append_is_semantic_change_or_cadence_but_latest_always_refreshes(
    tmp_path: Path,
) -> None:
    def estimator(*_args: object, **_kwargs: object) -> PhysicalFollowThroughEstimate:
        return _estimate()

    state = _state(NOW)
    frame = _option_frame(NOW)
    settings = _settings()
    first = process_strategy_distribution_forecast(
        data_root=tmp_path,
        action_state=state,
        option_frame=frame,
        raw_level_decision=_decision(NOW),
        now=NOW,
        settings=settings,
        trading_date=TRADING_DATE,
        physical_estimator=estimator,
    )
    second = process_strategy_distribution_forecast(
        data_root=tmp_path,
        action_state=state,
        option_frame=frame,
        raw_level_decision=_decision(NOW),
        now=NOW + timedelta(seconds=10),
        settings=settings,
        trading_date=TRADING_DATE,
        physical_estimator=estimator,
    )
    third = process_strategy_distribution_forecast(
        data_root=tmp_path,
        action_state=state,
        option_frame=frame,
        raw_level_decision=_decision(NOW, direction="down"),
        now=NOW + timedelta(seconds=20),
        settings=settings,
        trading_date=TRADING_DATE,
        physical_estimator=estimator,
    )
    fourth = process_strategy_distribution_forecast(
        data_root=tmp_path,
        action_state=state,
        option_frame=frame,
        raw_level_decision=_decision(NOW, direction="down"),
        now=NOW + timedelta(seconds=81),
        settings=settings,
        trading_date=TRADING_DATE,
        physical_estimator=estimator,
    )

    audit = _lines(strategy_distribution_forecast_audit_path(tmp_path, TRADING_DATE))
    assert [row["document_id"] for row in audit] == [
        first["document_id"],
        third["document_id"],
        fourth["document_id"],
    ]
    assert first["q_event"]["event"]["event_id"] != second["q_event"]["event"]["event_id"]  # type: ignore[index]
    assert second["document_id"] not in {row["document_id"] for row in audit}
    latest = json.loads(
        latest_strategy_distribution_forecast_path(tmp_path).read_text(encoding="utf-8")
    )
    assert latest["document_id"] == fourth["document_id"]
    assert datetime.fromisoformat(latest["valid_until"]) - datetime.fromisoformat(
        latest["available_at"]
    ) == timedelta(seconds=90)


def test_physical_baseline_refreshes_on_configured_cadence(tmp_path: Path) -> None:
    calls = 0

    def estimator(*_args: object, **_kwargs: object) -> PhysicalFollowThroughEstimate:
        nonlocal calls
        calls += 1
        return _estimate(probability=0.55 + calls / 100)

    settings = _settings(refresh_seconds=60.0)
    probabilities: list[float | None] = []
    for offset in (0, 30, 61):
        at = NOW + timedelta(seconds=offset)
        document = build_strategy_distribution_forecast(
            data_root=tmp_path,
            action_state=_state(at),
            option_frame=_option_frame(at),
            raw_level_decision=_decision(NOW),
            now=at,
            trading_date=TRADING_DATE,
            settings=settings,
            physical_estimator=estimator,
        )
        probabilities.append(document.p_event.probability)

    assert calls == 2
    assert probabilities == pytest.approx([0.56, 0.56, 0.57])


def test_missing_atm_iv_keeps_same_event_and_physical_baseline_but_blocks_action(
    tmp_path: Path,
) -> None:
    def estimator(*_args: object, **_kwargs: object) -> PhysicalFollowThroughEstimate:
        return _estimate()

    payload = process_strategy_distribution_forecast(
        data_root=tmp_path,
        action_state=_state(NOW),
        option_frame=_option_frame(NOW, atm_iv=None),
        raw_level_decision=_decision(NOW),
        now=NOW,
        settings=_settings(),
        trading_date=TRADING_DATE,
        physical_estimator=estimator,
    )

    assert payload["q_event"]["event"] == payload["p_event"]["event"]  # type: ignore[index]
    assert payload["q_event"]["status"] == "unavailable"  # type: ignore[index]
    assert payload["p_event"]["status"] == "available"  # type: ignore[index]
    assert payload["shadow_decision"]["action"] == "no_trade"  # type: ignore[index]
    assert payload["strategy_candidates"] == []


def test_non_prior_physical_training_date_is_rejected(tmp_path: Path) -> None:
    def estimator(*_args: object, **_kwargs: object) -> PhysicalFollowThroughEstimate:
        return _estimate(trained_through_date=TRADING_DATE)

    payload = process_strategy_distribution_forecast(
        data_root=tmp_path,
        action_state=_state(NOW),
        option_frame=_option_frame(NOW),
        raw_level_decision=_decision(NOW),
        now=NOW,
        settings=_settings(),
        trading_date=TRADING_DATE,
        physical_estimator=estimator,
    )

    assert payload["p_event"]["status"] == "unavailable"  # type: ignore[index]
    assert payload["p_event"]["trained_through_date"] is None  # type: ignore[index]
    assert "physical_training_date_not_prior" in payload["p_event"]["reason_codes"]  # type: ignore[index]
