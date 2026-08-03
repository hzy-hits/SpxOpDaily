from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path

import pytest

from spx_spark.application.runtime.market_regime_signal import (
    MODEL_VERSION,
    SignalPaths,
    produce_once,
)


UTC = timezone.utc
NOW = datetime(2026, 8, 3, 5, 4, 41, tzinfo=UTC)


def _write(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _seed(paths: SignalPaths) -> None:
    _write(
        paths.market,
        {
            "schema_version": 1,
            "frame_id": "market:2026-08-03:20260803T0503",
            "session_id": "2026-08-03",
            "as_of": "2026-08-03T05:03:55.790671+00:00",
            "quality": "ready",
            "es": {
                "price": 7564.125,
                "return_15m_points": -1.125,
                "return_60m_points": 2.25,
                "vwap_distance_points": 9.619047784221948,
                "vwap_slope_15m_points": -0.09854569406434166,
                "trend_efficiency_60m": 0.13580246913580246,
            },
            "cross_asset": {},
        },
    )
    _write(
        paths.options,
        {
            "schema_version": 1,
            "frame_id": "options:20260803:20260803T0504",
            "as_of": "2026-08-03T05:04:00.794527+00:00",
            "quality": "ready",
            "front_expiry": "20260803",
            "structure": {"underlier": 7535.1},
            "volatility": {"expected_move_points_0dte": 28.815},
            "density": {
                "quality": "ok",
                "p10": 7507.4,
                "median": 7543.1,
                "p90": 7566.9,
            },
        },
    )
    _write(
        paths.latest_state,
        {
            "as_of": "2026-08-03T05:04:40.774367+00:00",
            "best_quotes": [
                {
                    "instrument": {"canonical_id": "future:ES:20260918"},
                    "provider": "ibkr",
                    "quality": "live",
                    "mid": 7564.125,
                    "close": 7519.25,
                    "quote_time": "2026-08-03T05:04:40.118908+00:00",
                }
            ],
        },
    )
    _write(
        paths.prior_rth_context,
        {
            "as_of": "2026-08-03T05:04:05.799451+00:00",
            "for_trading_date": "2026-08-03",
            "close": 7489.72,
        },
    )


def _forecasts(payload: dict[str, object]) -> dict[str, dict[str, object]]:
    return {
        str(row["forecast_kind"]): row
        for row in payload["range_forecasts"]  # type: ignore[union-attr]
    }


def test_live_shape_advances_deterministic_forward_filter_and_writes_rust_wire(
    tmp_path: Path,
) -> None:
    paths = SignalPaths.from_data_root(tmp_path)
    _seed(paths)

    first = produce_once(paths=paths, now=NOW, max_input_age_seconds=90.0)
    regime = first["market_regime"]
    assert isinstance(regime, dict)
    posterior = {row["state_id"]: row["probability"] for row in regime["posterior"]}
    assert posterior == pytest.approx(
        {"state_00": 0.1598330608, "state_01": 0.6136792225, "state_02": 0.2264877167}
    )
    assert regime["posterior_entropy"] == pytest.approx(0.9290719259)
    assert regime["observation_count"] == 1
    assert math.fsum(posterior.values()) == pytest.approx(1.0)

    forecasts = _forecasts(first)
    assert forecasts["projected_open"]["median"] == pytest.approx(7534.595)
    assert forecasts["projected_open"]["distribution"] == "experimental_heuristic"
    assert (
        forecasts["risk_neutral_close"]["lower"],
        forecasts["risk_neutral_close"]["median"],
        forecasts["risk_neutral_close"]["upper"],
    ) == (7507.4, 7543.1, 7566.9)
    shift = 0.25 * 28.815 * (posterior["state_02"] - posterior["state_00"])
    assert forecasts["hmm_adjusted_close"]["median"] == pytest.approx(7543.1 + shift)
    assert forecasts["hmm_adjusted_close"]["distribution"] == "experimental_heuristic"
    assert first["schema_version"] == "experimental_research_signals.v1"
    assert first["automatic_ordering"] is False
    assert set(first) == {
        "schema_version",
        "document_id",
        "generated_at",
        "market_regime",
        "range_forecasts",
        "automatic_ordering",
    }
    assert paths.output.stat().st_mode & 0o777 == 0o600

    inode = paths.output.stat().st_ino
    assert produce_once(paths=paths, now=NOW, max_input_age_seconds=90.0) == first
    assert paths.output.stat().st_ino == inode
    state = json.loads(paths.state.read_text(encoding="utf-8"))
    assert state["online_state"]["observation_count"] == 1
    assert "online_state" not in first

    market = json.loads(paths.market.read_text(encoding="utf-8"))
    market["frame_id"] = "market:2026-08-03:20260803T0504"
    market["as_of"] = "2026-08-03T05:04:45+00:00"
    market["es"]["return_15m_points"] = 8.0
    market["es"]["return_60m_points"] = 12.0
    _write(paths.market, market)
    second = produce_once(
        paths=paths,
        now=datetime(2026, 8, 3, 5, 4, 50, tzinfo=UTC),
        max_input_age_seconds=90.0,
    )
    assert second["market_regime"]["observation_count"] == 2
    assert json.loads(paths.state.read_text(encoding="utf-8"))["online_state"][
        "observation_count"
    ] == 2


def test_missing_range_inputs_are_omitted_without_blocking_regime(tmp_path: Path) -> None:
    paths = SignalPaths.from_data_root(tmp_path)
    _seed(paths)
    _write(paths.options, {})

    payload = produce_once(paths=paths, now=NOW, max_input_age_seconds=90.0)

    assert payload["market_regime"] is not None
    assert payload["range_forecasts"] == []


def test_missing_front_expiry_never_claims_a_risk_neutral_close(tmp_path: Path) -> None:
    paths = SignalPaths.from_data_root(tmp_path)
    _seed(paths)
    options = json.loads(paths.options.read_text(encoding="utf-8"))
    options.pop("front_expiry")
    _write(paths.options, options)

    payload = produce_once(paths=paths, now=NOW, max_input_age_seconds=90.0)

    assert set(_forecasts(payload)) == {"projected_open"}


def test_stale_es_quote_omits_projected_open_but_keeps_close_context(tmp_path: Path) -> None:
    paths = SignalPaths.from_data_root(tmp_path)
    _seed(paths)
    latest = json.loads(paths.latest_state.read_text(encoding="utf-8"))
    latest["best_quotes"][0]["quote_time"] = "2026-08-03T04:00:00+00:00"
    _write(paths.latest_state, latest)

    payload = produce_once(paths=paths, now=NOW, max_input_age_seconds=90.0)

    assert set(_forecasts(payload)) == {"risk_neutral_close", "hmm_adjusted_close"}


def test_corrupt_persisted_posterior_reinitializes_instead_of_crashing(tmp_path: Path) -> None:
    paths = SignalPaths.from_data_root(tmp_path)
    _seed(paths)
    _write(
        paths.state,
        {
            "schema_version": "experimental_research_signals.state.v1",
            "online_state": {
                "model_version": MODEL_VERSION,
                "session_id": "2026-08-03",
                "posterior": [float("nan"), -1.0, 2.0],
            },
        },
    )

    payload = produce_once(paths=paths, now=NOW, max_input_age_seconds=90.0)

    regime = payload["market_regime"]
    assert isinstance(regime, dict)
    assert regime["observation_count"] == 1
