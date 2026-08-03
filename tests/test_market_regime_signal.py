from __future__ import annotations

import copy
import json
import math
from datetime import datetime, timezone
from pathlib import Path

import pytest

from spx_spark.application.runtime.market_regime_signal import (
    MODEL_VERSION,
    SignalPaths,
    build_signal,
    produce_once,
)


UTC = timezone.utc
PREMARKET_NOW = datetime(2026, 8, 3, 5, 4, 41, tzinfo=UTC)
RTH_NOW = datetime(2026, 8, 3, 15, 4, 41, tzinfo=UTC)
INDEX_PRICES = {
    "index:SPX": (6_300.0, 6_285.0),
    "index:NDX": (23_000.0, 22_940.0),
    "index:DJI": (44_000.0, 43_950.0),
    "index:RUT": (2_250.0, 2_240.0),
}


def _write(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _read(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _prior_rth() -> dict[str, object]:
    returns = {
        "index:SPX": 18.0,
        "index:NDX": 31.0,
        "index:DJI": 9.0,
        "index:RUT": 24.0,
    }
    return {
        "schema_version": "prior_rth_context.v2",
        "status": "ready",
        "as_of": "2026-08-01T00:01:00+00:00",
        "for_trading_date": "2026-08-03",
        "session_date": "2026-07-31",
        "close": 7_489.72,
        "cross_index": {
            "status": "ready",
            "return_bps": returns,
            "relative_to_spx_return_bps": {
                instrument: value - returns["index:SPX"] for instrument, value in returns.items()
            },
            "return_dispersion_bps": 8.2,
            "breadth": {
                "available_count": 4,
                "up_count": 4,
                "down_count": 0,
                "flat_count": 0,
            },
            "reason_codes": [],
        },
        "reasons": [],
    }


def _options(*, as_of: str) -> dict[str, object]:
    return {
        "schema_version": 1,
        "frame_id": f"options:20260803:{as_of}",
        "as_of": as_of,
        "quality": "ready",
        "front_expiry": "20260803",
        "structure": {"underlier": 7_535.1},
        "volatility": {"expected_move_points_0dte": 28.815},
        "density": {
            "quality": "ok",
            "p10": 7_507.4,
            "median": 7_543.1,
            "p90": 7_566.9,
        },
    }


def _premarket_market() -> dict[str, object]:
    return {
        "schema_version": 1,
        "frame_id": "market:2026-08-03:20260803T0503",
        "session_id": "2026-08-03",
        "as_of": "2026-08-03T05:03:55.790671+00:00",
        "quality": "ready",
        "es": {
            "price": 7_564.125,
            "return_15m_points": -1.125,
            "return_60m_points": 2.25,
            "vwap_distance_points": 9.619047784221948,
            "vwap_slope_15m_points": -0.09854569406434166,
            "trend_efficiency_60m": 0.13580246913580246,
        },
        "cross_asset": {},
    }


def _cash_index_payload() -> dict[str, object]:
    source_at = "2026-08-03T15:04:39+00:00"
    return {
        "status": "ready",
        "cash_session_open": True,
        "observations": {
            instrument: {
                "status": "available",
                "price": price,
                "reference_close": reference,
                "price_kind": "last",
                "provider": "schwab",
                "quality": "live",
                "source_at": source_at,
                "missing_reason": None,
            }
            for instrument, (price, reference) in INDEX_PRICES.items()
        },
        "missing_instruments": [],
        "source_skew_limit_seconds": 5.0,
        "relative_to_spx_15m_bps": {
            "index:SPX": 0.0,
            "index:NDX": 8.0,
            "index:DJI": -3.0,
            "index:RUT": 5.0,
        },
        "dispersion_15m_bps": 12.0,
        "breadth_15m": {"up_count": 3, "down_count": 1, "flat_count": 0},
        "reason_codes": [],
    }


def _rth_market() -> dict[str, object]:
    market = _premarket_market()
    market.update(
        {
            "frame_id": "market:2026-08-03:20260803T1504",
            "as_of": "2026-08-03T15:04:40+00:00",
            "cross_asset": {"cash_index": _cash_index_payload()},
        }
    )
    return market


def _seed_premarket(paths: SignalPaths) -> None:
    _write(paths.market, _premarket_market())
    _write(paths.options, _options(as_of="2026-08-03T05:04:00.794527+00:00"))
    _write(
        paths.latest_state,
        {
            "as_of": "2026-08-03T05:04:40.774367+00:00",
            "best_quotes": [
                {
                    "instrument": {"canonical_id": "future:ES:20260918"},
                    "provider": "ibkr",
                    "quality": "live",
                    "mid": 7_564.125,
                    "close": 7_519.25,
                    "quote_time": "2026-08-03T05:04:40.118908+00:00",
                }
            ],
        },
    )
    _write(paths.prior_rth_context, _prior_rth())


def _seed_rth(paths: SignalPaths) -> None:
    _write(paths.market, _rth_market())
    _write(paths.options, _options(as_of="2026-08-03T15:04:40+00:00"))
    _write(paths.latest_state, {"as_of": "2026-08-03T15:04:40+00:00", "best_quotes": []})
    _write(paths.prior_rth_context, _prior_rth())
    _write(
        paths.spx_minutes,
        {
            "as_of": "2026-08-03T15:04:00+00:00",
            "rows": [
                {
                    "session_date": "2026-08-03",
                    "minute": minute,
                    "selected": {"price": price},
                }
                for minute, price in (
                    ("2026-08-03T14:30:00+00:00", 6_300.0),
                    ("2026-08-03T15:00:00+00:00", 6_312.0),
                    ("2026-08-03T15:04:00+00:00", 6_307.0),
                )
            ],
        },
    )


def _forecasts(payload: dict[str, object]) -> dict[str, dict[str, object]]:
    rows = payload["forecasts"]
    assert isinstance(rows, list)
    return {str(row["target"]): row for row in rows}


def _posterior(regime: dict[str, object]) -> dict[str, float]:
    rows = regime["posterior"]
    assert isinstance(rows, list)
    return {str(row["state_id"]): float(row["probability"]) for row in rows}


def _build_from_paths(
    paths: SignalPaths,
    *,
    now: datetime,
    market: dict[str, object] | None = None,
    prior: dict[str, object] | None = None,
) -> dict[str, object]:
    return build_signal(
        market=market or _read(paths.market),
        options=_read(paths.options),
        spx_minutes=_read(paths.spx_minutes) if paths.spx_minutes.exists() else {},
        latest_state=_read(paths.latest_state),
        prior_rth_context=prior or _read(paths.prior_rth_context),
        previous={},
        now=now,
        max_input_age_seconds=90.0,
    )


def test_v2_document_is_advisory_explicit_and_same_frame_does_not_repeat(
    tmp_path: Path,
) -> None:
    paths = SignalPaths.from_data_root(tmp_path)
    _seed_premarket(paths)

    first = produce_once(paths=paths, now=PREMARKET_NOW, max_input_age_seconds=90.0)

    assert first["schema_version"] == "research_context.v2"
    assert set(first) == {
        "schema_version",
        "document_id",
        "generated_at",
        "evidence_status",
        "use_scope",
        "action_authority",
        "automatic_ordering",
        "cross_index_frame",
        "prior_rth_context",
        "regime",
        "regime_reason_codes",
        "forecasts",
        "close_location",
    }
    assert first["evidence_status"] == "bootstrap_unvalidated"
    assert first["use_scope"] == "advisory"
    assert first["action_authority"] == "none"
    assert first["automatic_ordering"] is False
    regime = first["regime"]
    assert isinstance(regime, dict)
    assert regime["inference"] == "filtered"
    assert regime["parameter_mode"] == "fixed_bootstrap"
    assert regime["update_index"] == 1
    posterior = _posterior(regime)
    assert math.fsum(posterior.values()) == pytest.approx(1.0)
    assert first["cross_index_frame"]["missing_instruments"] == [
        "index:SPX",
        "index:NDX",
        "index:DJI",
        "index:RUT",
    ]
    assert first["prior_rth_context"]["status"] == "ready"

    forecasts = _forecasts(first)
    assert list(forecasts) == ["rth_close", "session_high", "session_low"]
    assert forecasts["rth_close"]["status"] == "available"
    assert forecasts["rth_close"]["distribution"] == "experimental_heuristic"
    assert "risk_neutral_source_not_physical" in forecasts["rth_close"]["reason_codes"]
    assert forecasts["session_high"]["status"] == "unavailable"
    assert forecasts["session_low"]["status"] == "unavailable"
    assert first["close_location"]["status"] == "unavailable"
    assert paths.output.stat().st_mode & 0o777 == 0o600

    inode = paths.output.stat().st_ino
    assert produce_once(paths=paths, now=PREMARKET_NOW, max_input_age_seconds=90.0) == first
    assert paths.output.stat().st_ino == inode

    options = _read(paths.options)
    options["density"]["median"] = 7_543.2
    _write(paths.options, options)
    same_frame = produce_once(
        paths=paths,
        now=datetime(2026, 8, 3, 5, 4, 43, tzinfo=UTC),
        max_input_age_seconds=90.0,
    )
    assert same_frame["regime"]["update_index"] == 1
    assert _posterior(same_frame["regime"]) == pytest.approx(posterior)
    assert (
        _forecasts(same_frame)["rth_close"]["quantiles"]["p50"]
        != (forecasts["rth_close"]["quantiles"]["p50"])
    )

    market = _read(paths.market)
    market["frame_id"] = "market:2026-08-03:20260803T0504"
    market["as_of"] = "2026-08-03T05:04:45+00:00"
    market["es"]["return_15m_points"] = 8.0
    market["es"]["return_60m_points"] = 12.0
    _write(paths.market, market)
    next_frame = produce_once(
        paths=paths,
        now=datetime(2026, 8, 3, 5, 4, 50, tzinfo=UTC),
        max_input_age_seconds=90.0,
    )
    assert next_frame["regime"]["update_index"] == 2
    assert _read(paths.state)["online_state"]["observation_count"] == 2


def test_rth_cross_index_prior_context_and_intraday_ranges_feed_realtime_output(
    tmp_path: Path,
) -> None:
    paths = SignalPaths.from_data_root(tmp_path)
    _seed_rth(paths)

    payload = produce_once(paths=paths, now=RTH_NOW, max_input_age_seconds=90.0)

    frame = payload["cross_index_frame"]
    assert frame["status"] == "ready"
    assert frame["missing_instruments"] == []
    assert [row["instrument"] for row in frame["observations"]] == list(INDEX_PRICES)
    forecasts = _forecasts(payload)
    for target in ("rth_close", "session_high", "session_low"):
        forecast = forecasts[target]
        assert forecast["status"] == "available"
        assert forecast["distribution"] == "experimental_heuristic"
        quantiles = forecast["quantiles"]
        assert quantiles["p10"] < quantiles["p50"] < quantiles["p90"]
    location = payload["close_location"]
    assert location["status"] == "available"
    assert location["bucket_definition"] == ("thirds_of_projected_session_low_to_high_range")
    assert math.fsum(location["probabilities"].values()) == pytest.approx(1.0)
    assert location["distribution"] == "experimental_heuristic"

    internal = _build_from_paths(paths, now=RTH_NOW)
    observation = internal["regime"]["observation"]
    assert observation["components"]["cash_index"]["status"] == "available"
    assert observation["components"]["prior_rth"]["status"] == "available"
    base_score = observation["direction_score"]

    weaker_cash = copy.deepcopy(_read(paths.market))
    cash = weaker_cash["cross_asset"]["cash_index"]
    cash["relative_to_spx_15m_bps"].update(
        {"index:NDX": -30.0, "index:DJI": -25.0, "index:RUT": -35.0}
    )
    cash["dispersion_15m_bps"] = 40.0
    cash["breadth_15m"] = {"up_count": 0, "down_count": 4, "flat_count": 0}
    cash_score = _build_from_paths(paths, now=RTH_NOW, market=weaker_cash)["regime"]["observation"][
        "direction_score"
    ]
    assert cash_score != pytest.approx(base_score)

    weaker_prior = copy.deepcopy(_read(paths.prior_rth_context))
    weaker_prior["cross_index"]["return_bps"] = {instrument: -80.0 for instrument in INDEX_PRICES}
    weaker_prior["cross_index"]["breadth"] = {
        "available_count": 4,
        "up_count": 0,
        "down_count": 4,
        "flat_count": 0,
    }
    prior_score = _build_from_paths(paths, now=RTH_NOW, prior=weaker_prior)["regime"][
        "observation"
    ]["direction_score"]
    assert prior_score != pytest.approx(base_score)


def test_missing_cash_index_is_explicit_and_degrades_without_blocking_advisory_regime(
    tmp_path: Path,
) -> None:
    paths = SignalPaths.from_data_root(tmp_path)
    _seed_rth(paths)
    market = _read(paths.market)
    cash = market["cross_asset"]["cash_index"]
    cash["status"] = "degraded"
    cash["missing_instruments"] = ["index:RUT"]
    cash["observations"]["index:RUT"] = {
        "status": "missing",
        "missing_reason": "fresh_quote_unavailable",
    }
    cash["relative_to_spx_15m_bps"] = {}
    cash["dispersion_15m_bps"] = None
    cash["breadth_15m"] = None
    cash["reason_codes"] = ["cash_index_missing:index:RUT"]
    _write(paths.market, market)

    payload = produce_once(paths=paths, now=RTH_NOW, max_input_age_seconds=90.0)

    frame = payload["cross_index_frame"]
    assert frame["status"] == "degraded"
    assert frame["missing_instruments"] == ["index:RUT"]
    assert frame["observations"][-1]["missing_reason"] == "fresh_quote_unavailable"
    assert payload["regime"] is not None
    assert "cash_index_component_unavailable" in payload["regime_reason_codes"]
    assert payload["action_authority"] == "none"


def test_incomplete_prior_rth_four_index_context_is_explicitly_degraded(
    tmp_path: Path,
) -> None:
    paths = SignalPaths.from_data_root(tmp_path)
    _seed_rth(paths)
    prior = _read(paths.prior_rth_context)
    prior["cross_index"]["return_bps"]["index:RUT"] = None
    _write(paths.prior_rth_context, prior)

    payload = produce_once(paths=paths, now=RTH_NOW, max_input_age_seconds=90.0)

    assert payload["prior_rth_context"]["status"] == "partial"
    assert "prior_rth_returns_incomplete" in payload["prior_rth_context"]["reason_codes"]
    assert "prior_rth_component_unavailable" in payload["regime_reason_codes"]
    assert payload["regime"] is not None


def test_missing_inputs_return_complete_fail_closed_v2_shape(tmp_path: Path) -> None:
    paths = SignalPaths.from_data_root(tmp_path)
    for path in (
        paths.market,
        paths.options,
        paths.spx_minutes,
        paths.latest_state,
        paths.prior_rth_context,
    ):
        _write(path, {})

    payload = produce_once(
        paths=paths,
        now=PREMARKET_NOW,
        max_input_age_seconds=90.0,
    )

    assert payload["schema_version"] == "research_context.v2"
    assert payload["regime"] is None
    assert payload["regime_reason_codes"]
    assert payload["prior_rth_context"]["status"] == "unavailable"
    assert all(row["status"] == "unavailable" for row in payload["forecasts"])
    assert all(row["reason_codes"] for row in payload["forecasts"])
    assert payload["close_location"]["status"] == "unavailable"
    assert payload["automatic_ordering"] is False


def test_missing_front_expiry_keeps_explicit_unavailable_close(tmp_path: Path) -> None:
    paths = SignalPaths.from_data_root(tmp_path)
    _seed_premarket(paths)
    options = _read(paths.options)
    options.pop("front_expiry")
    _write(paths.options, options)

    payload = produce_once(
        paths=paths,
        now=PREMARKET_NOW,
        max_input_age_seconds=90.0,
    )

    close = _forecasts(payload)["rth_close"]
    assert close["status"] == "unavailable"
    assert close["reason_codes"] == ["front_expiry_mismatch"]
    assert close["distribution"] is None
    assert close["quantiles"] is None
    assert _forecasts(payload)["session_high"]["reason_codes"] == [
        "same_day_expected_move_expiry_mismatch"
    ]


def test_missing_expected_move_or_stale_spx_path_degrades_intraday_extremes(
    tmp_path: Path,
) -> None:
    paths = SignalPaths.from_data_root(tmp_path)
    _seed_rth(paths)
    options = _read(paths.options)
    options["volatility"] = {}
    _write(paths.options, options)

    missing_em = produce_once(paths=paths, now=RTH_NOW, max_input_age_seconds=90.0)
    forecasts = _forecasts(missing_em)
    assert forecasts["session_high"]["reason_codes"] == ["same_day_expected_move_unavailable"]
    assert forecasts["session_low"]["reason_codes"] == ["same_day_expected_move_unavailable"]
    assert missing_em["close_location"]["status"] == "unavailable"

    _seed_rth(paths)
    minutes = _read(paths.spx_minutes)
    minutes["rows"] = minutes["rows"][:1]
    _write(paths.spx_minutes, minutes)
    stale = produce_once(paths=paths, now=RTH_NOW, max_input_age_seconds=90.0)
    assert _forecasts(stale)["session_high"]["reason_codes"] == [
        "fresh_spx_intraday_path_unavailable"
    ]


def test_corrupt_persisted_posterior_reinitializes_instead_of_crashing(
    tmp_path: Path,
) -> None:
    paths = SignalPaths.from_data_root(tmp_path)
    _seed_premarket(paths)
    _write(
        paths.state,
        {
            "schema_version": "research_context.state.v2",
            "online_state": {
                "model_version": MODEL_VERSION,
                "session_id": "2026-08-03",
                "posterior": [float("nan"), -1.0, 2.0],
            },
        },
    )

    payload = produce_once(
        paths=paths,
        now=PREMARKET_NOW,
        max_input_age_seconds=90.0,
    )

    assert payload["regime"]["update_index"] == 1
    assert math.fsum(_posterior(payload["regime"]).values()) == pytest.approx(1.0)
