from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from spx_spark.application.market_features.market_state_5m import (
    TREND_UP,
    score_market_state_5m,
)
from spx_spark.application.market_features.market_state_5m_inputs import (
    SECTOR_INSTRUMENTS,
    _atr_5m,
    _bar_vwaps,
    _es_vwap_series,
    _moving_average_diagnostics,
    _provider_vwap_series,
    build_market_state_5m_inputs,
    project_spx_equivalent_moving_averages,
    update_same_time_range_baselines,
)
from spx_spark.application.market_features.moving_average_context import (
    _ma_regime,
    rth_atr_5m,
)
from spx_spark.market_calendar import ET


DAY = datetime(2026, 7, 24, 10, 0, tzinfo=ET)


def bar(
    start: datetime,
    *,
    open_: float,
    high: float,
    low: float,
    close: float,
    segment: str,
    quality: str = "ok",
    contract_identity: str | None = "ES:202609",
) -> dict[str, object]:
    return {
        "bar_start": start.isoformat(),
        "bar_end": (start + timedelta(minutes=5)).isoformat(),
        "interval_seconds": 300,
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "sample_count": 60,
        "max_sample_gap_seconds": 5.0,
        "quality": quality,
        "gap_before": False,
        "provider": "schwab",
        "contract_identity": contract_identity,
        "segment": segment,
        "trading_date_et": start.date().isoformat(),
    }


def trending_bars() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    premarket = DAY.replace(hour=8, minute=15)
    price = 7380.0
    for index in range(15):
        start = premarket + timedelta(minutes=5 * index)
        rows.append(
            bar(
                start,
                open_=price,
                high=price + 2.0,
                low=price - 1.0,
                close=price + 1.0,
                segment="us_premarket",
            )
        )
        price += 1.0
    rth = DAY.replace(hour=9, minute=30)
    for index in range(6):
        start = rth + timedelta(minutes=5 * index)
        base = 7400.0 + index * 5.0
        rows.append(
            bar(
                start,
                open_=base,
                high=base + 5.0,
                low=base - 1.0,
                close=base + 4.0,
                segment="rth",
            )
        )
    return rows


def market_samples(
    *,
    include_sectors: bool = True,
    es_provider: str = "schwab",
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    start = DAY.replace(hour=9, minute=30)
    for index in range(31):
        at = start + timedelta(minutes=index)
        es_price = 7400.0 + index
        instruments: dict[str, object] = {
            "future:ES": {
                "price": es_price,
                "volume": 100_000.0 + index * 100.0,
                "provider": es_provider,
                "source_at": at.isoformat(),
                "quality": "live",
            }
        }
        if include_sectors:
            for sector_index, instrument_id in enumerate(SECTOR_INSTRUMENTS):
                instruments[instrument_id] = {
                    "price": 100.0 + sector_index + index * 0.1,
                    "volume": 10_000.0 + index * 50.0,
                    "provider": "schwab",
                    "source_at": at.isoformat(),
                    "quality": "live",
                }
        rows.append(
            {
                "at": at.isoformat(),
                "session_id": "2026-07-24",
                "segment": "rth",
                "instruments": instruments,
                "es_by_provider": {es_provider: instruments["future:ES"]},
            }
        )
    return rows


def baselines(value: float = 31.0, count: int = 20) -> dict[str, object]:
    return {
        "schema_version": "market_state_5m_range_baselines.v1",
        "slots": {
            "10:00": [
                {
                    "trading_date_et": f"2026-06-{index + 1:02d}",
                    "range_points": value,
                }
                for index in range(count)
            ]
        },
    }


def rth_history(
    closes: list[float],
    *,
    contract_identity: str = "ES:202609",
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    close_index = 0
    for day in (22, 23, 24):
        session_start = DAY.replace(day=day, hour=9, minute=30)
        for bar_index in range(78):
            if close_index >= len(closes):
                return rows
            close = closes[close_index]
            rows.append(
                bar(
                    session_start + timedelta(minutes=5 * bar_index),
                    open_=close - 0.25,
                    high=close + 1.0,
                    low=close - 1.0,
                    close=close,
                    segment="rth",
                    contract_identity=contract_identity,
                )
            )
            close_index += 1
    if close_index != len(closes):
        raise AssertionError("test history exceeds three RTH sessions")
    return rows


def test_derives_all_eight_inputs_and_scores_clean_trend_up() -> None:
    derived = build_market_state_5m_inputs(
        bars=trending_bars(),
        market_samples=market_samples(),
        range_baselines=baselines(),
        now=DAY,
    )

    assert derived["schema_version"] == "market_state_5m_inputs.v2"
    assert derived["status"] == "ready"
    assert derived["available_count"] == 8
    values = derived["values"]
    assert values["price_vs_vwap"] == "ABOVE_CONFIRMED"
    assert values["vwap_slope"] > 0.30
    assert values["opening_range_state"] == "ABOVE_ORH_CONFIRMED"
    assert values["market_structure"] == "HH_HL"
    assert values["efficiency_ratio"] == pytest.approx(1.0)
    assert values["vwap_cross_count"] == 0
    assert values["same_time_range_ratio"] == pytest.approx(1.0)
    assert values["breadth_above_vwap"] == pytest.approx(1.0)

    scored = score_market_state_5m(now=DAY, **values)
    assert scored["D"] == 10
    assert scored["state"] == TREND_UP
    assert scored["action_authority"] == "none"
    assert scored["actionable"] is False


def test_rth_sma20_sma50_and_spx_basis_projection_are_read_only() -> None:
    start = DAY.replace(hour=9, minute=30)
    rows = [
        bar(
            start + timedelta(minutes=5 * index),
            open_=7400.0 + index,
            high=7401.0 + index,
            low=7399.0 + index,
            close=7400.0 + index,
            segment="rth",
        )
        for index in range(50)
    ]

    moving = _moving_average_diagnostics(rows)
    projected = project_spx_equivalent_moving_averages(
        moving,
        es_spx_basis_points=34.15,
        basis_contract_identity="ES:202609",
    )

    assert moving["status"] == "partial"
    assert moving["price"] == 7449.0
    assert moving["sma20"] == 7439.5
    assert moving["sma50"] == 7424.5
    assert moving["sma200"] is None
    assert moving["relation"] == "bullish_stack"
    assert moving["contract_identity"] == "ES:202609"
    assert moving["action_authority"] == "none"
    assert projected["spx_equivalent_sma20"] == 7405.35
    assert projected["spx_equivalent_sma50"] == 7390.35
    assert projected["basis_contract_identity_matches_sma"] is True
    assert projected["spx_projection_near_line"] is False
    assert projected["spx_projection_near_line_tolerance_points"] == 4.25
    assert projected["projection_method"] == (
        "es_sma_minus_synchronized_current_basis_not_cash_spx_sma"
    )


def test_rth_atr14_ignores_real_gth_path_and_overnight_gap() -> None:
    prior_start = DAY.replace(day=23, hour=15, minute=30)
    prior_rth = [
        bar(
            prior_start + timedelta(minutes=5 * index),
            open_=100.0,
            high=101.0,
            low=99.0,
            close=100.0,
            segment="rth",
        )
        for index in range(6)
    ]
    gth_start = DAY.replace(day=23, hour=16, minute=0)
    gth = [
        bar(
            gth_start + timedelta(minutes=5 * index),
            open_=5000.0,
            high=5100.0,
            low=4900.0,
            close=5000.0,
            segment="globex",
        )
        for index in range(12)
    ]
    current_start = DAY.replace(hour=9, minute=30)
    current_rth = [
        bar(
            current_start + timedelta(minutes=5 * index),
            open_=1000.0,
            high=1002.0,
            low=998.0,
            close=1000.0,
            segment="rth",
        )
        for index in range(8)
    ]
    current_rth[0]["gap_before"] = True
    rth_only = [*prior_rth, *current_rth]
    with_real_session_path = [*prior_rth, *gth, *current_rth]

    shared, diagnostics = rth_atr_5m(with_real_session_path)
    production, production_diagnostics = _atr_5m(with_real_session_path)
    without_gth, _ = rth_atr_5m(rth_only)

    assert shared == pytest.approx((6 * 2.0 + 8 * 4.0) / 14)
    assert production == pytest.approx(shared)
    assert without_gth == pytest.approx(shared)
    assert diagnostics["periods_used"] == 14
    assert diagnostics["session_count"] == 2
    assert diagnostics["overnight_gap_included"] is False
    assert production_diagnostics["method"] == diagnostics["method"]


def test_rth_sma_fails_closed_without_contract_identity() -> None:
    start = DAY.replace(hour=9, minute=30)
    rows = [
        bar(
            start + timedelta(minutes=5 * index),
            open_=7400.0 + index,
            high=7401.0 + index,
            low=7399.0 + index,
            close=7400.0 + index,
            segment="rth",
            contract_identity=None,
        )
        for index in range(50)
    ]

    moving = _moving_average_diagnostics(rows)

    assert moving["status"] == "warming"
    assert moving["sma20"] is None
    assert moving["sma50"] is None
    assert "es_contract_identity_unavailable" in moving["reasons"]


def test_spx_ma_projection_fails_closed_on_contract_mismatch() -> None:
    projected = project_spx_equivalent_moving_averages(
        {
            "sma20": 7439.5,
            "sma50": 7424.5,
            "distance_to_sma20_points": 2.0,
            "contract_identity": "ES:202609",
        },
        es_spx_basis_points=34.15,
        basis_contract_identity="ES:202612",
    )

    assert projected["basis_contract_identity_matches_sma"] is False
    assert projected["spx_equivalent_sma20"] is None
    assert projected["spx_equivalent_sma50"] is None
    assert projected["spx_projection_near_line"] is False
    assert projected["projection_method"] == ("unavailable_basis_contract_identity_mismatch")


def test_rth_sma_rejects_truncated_cross_session_boundary() -> None:
    prior_start = DAY.replace(day=23, hour=15, minute=5)
    current_start = DAY.replace(hour=9, minute=30)
    rows = [
        bar(
            prior_start + timedelta(minutes=5 * index),
            open_=7350.0 + index,
            high=7351.0 + index,
            low=7349.0 + index,
            close=7350.0 + index,
            segment="rth",
        )
        for index in range(10)
    ]
    rows.extend(
        bar(
            current_start + timedelta(minutes=5 * index),
            open_=7400.0 + index,
            high=7401.0 + index,
            low=7399.0 + index,
            close=7400.0 + index,
            segment="rth",
        )
        for index in range(40)
    )
    rows[10]["gap_before"] = True

    moving = _moving_average_diagnostics(rows)

    assert moving["status"] == "partial"
    assert moving["sma20"] == 7429.5
    assert moving["sma50"] is None
    assert "sma50_rth_session_boundary_gap" in moving["reasons"]


def test_rth_sma_accepts_complete_adjacent_session_boundary() -> None:
    prior_start = DAY.replace(day=23, hour=15, minute=10)
    current_start = DAY.replace(hour=9, minute=30)
    rows = [
        bar(
            prior_start + timedelta(minutes=5 * index),
            open_=7350.0 + index,
            high=7351.0 + index,
            low=7349.0 + index,
            close=7350.0 + index,
            segment="rth",
        )
        for index in range(10)
    ]
    rows.extend(
        bar(
            current_start + timedelta(minutes=5 * index),
            open_=7400.0 + index,
            high=7401.0 + index,
            low=7399.0 + index,
            close=7400.0 + index,
            segment="rth",
        )
        for index in range(40)
    )
    rows[10]["gap_before"] = True

    moving = _moving_average_diagnostics(rows)

    assert moving["status"] == "partial"
    assert moving["sma20"] == 7429.5
    assert moving["sma50"] == 7406.5


def test_ma50_ma200_aligned_regime_uses_closed_rth_bars_and_atr_units() -> None:
    rows = rth_history([7400.0 + index * 0.2 for index in range(220)])

    moving = _moving_average_diagnostics(rows, atr_5m=10.0)

    assert moving["status"] == "ready"
    assert moving["price"] == 7443.8
    assert moving["sma50"] == 7438.9
    assert moving["sma200"] == 7423.9
    assert moving["distance_to_sma50_atr"] == pytest.approx(0.49)
    assert moving["distance_to_sma200_atr"] == pytest.approx(1.99)
    assert moving["ma50_ma200_spread_points"] == pytest.approx(15.0)
    assert moving["ma50_ma200_spread_atr"] == pytest.approx(1.5)
    assert moving["ma50_slope_3_atr"] == pytest.approx(0.06)
    assert moving["ma50_slope_6_atr"] == pytest.approx(0.12)
    assert moving["ma200_slope_3_atr"] == pytest.approx(0.06)
    assert moving["ma200_slope_6_atr"] == pytest.approx(0.12)
    assert moving["spread_change_3_atr"] == pytest.approx(0.0)
    assert moving["regime_state"] == "TREND_ALIGNED"
    assert moving["regime_direction"] == "up"
    assert moving["same_direction_convexity"] == "confluence_only"
    assert moving["action_authority"] == "none"


def test_ma50_ma200_extended_regime_does_not_authorize_chasing() -> None:
    rows = rth_history([7400.0 + index for index in range(220)])

    moving = _moving_average_diagnostics(rows, atr_5m=10.0)

    assert moving["distance_to_sma50_atr"] == pytest.approx(2.45)
    assert moving["distance_to_sma200_atr"] == pytest.approx(9.95)
    assert moving["regime_state"] == "TREND_EXTENDED"
    assert moving["regime_direction"] == "up"
    assert moving["same_direction_convexity"] == "do_not_chase"


@pytest.mark.parametrize(
    (
        "inputs",
        "expected_state",
        "expected_direction",
        "expected_convexity",
    ),
    [
        (
            {
                "distance50_atr": 0.5,
                "distance200_atr": -1.0,
                "spread_atr": -0.5,
                "ma50_slope_3_atr": 0.1,
                "ma50_slope_6_atr": 0.05,
                "ma200_slope_3_atr": -0.03,
                "ma200_slope_6_atr": -0.05,
                "spread_change_3_atr": 0.08,
            },
            "REGIME_TRANSITION",
            "up",
            "wait_for_wall_confirmation",
        ),
        (
            {
                "distance50_atr": 0.5,
                "distance200_atr": 0.8,
                "spread_atr": 0.4,
                "ma50_slope_3_atr": -0.1,
                "ma50_slope_6_atr": 0.05,
                "ma200_slope_3_atr": 0.03,
                "ma200_slope_6_atr": -0.05,
                "spread_change_3_atr": -0.08,
            },
            "MIXED",
            None,
            "wait_for_wall_confirmation",
        ),
        (
            {
                "distance50_atr": -0.5,
                "distance200_atr": -1.5,
                "spread_atr": -0.8,
                "ma50_slope_3_atr": -0.08,
                "ma50_slope_6_atr": -0.12,
                "ma200_slope_3_atr": -0.03,
                "ma200_slope_6_atr": -0.06,
                "spread_change_3_atr": -0.02,
            },
            "TREND_ALIGNED",
            "down",
            "confluence_only",
        ),
        (
            {
                "distance50_atr": -2.5,
                "distance200_atr": -3.5,
                "spread_atr": -0.8,
                "ma50_slope_3_atr": -0.08,
                "ma50_slope_6_atr": -0.12,
                "ma200_slope_3_atr": -0.03,
                "ma200_slope_6_atr": -0.06,
                "spread_change_3_atr": -0.02,
            },
            "TREND_EXTENDED",
            "down",
            "do_not_chase",
        ),
    ],
    ids=("transition_up", "mixed", "aligned_down", "extended_down"),
)
def test_ma_regime_direct_classification_covers_all_directional_branches(
    inputs: dict[str, float],
    expected_state: str,
    expected_direction: str | None,
    expected_convexity: str,
) -> None:
    assert _ma_regime(**inputs) == (
        expected_state,
        expected_direction,
        expected_convexity,
    )


def test_ma50_ma200_cross_age_persistence_and_fresh_boundary() -> None:
    closes = [7400.0] * 200 + [7400.0 + 5.0 * index for index in range(1, 8)]
    fresh = _moving_average_diagnostics(rth_history(closes), atr_5m=10.0)

    assert fresh["cross_direction"] == "golden"
    assert fresh["bars_since_cross"] == 6
    assert fresh["cross_persistent_2_bars"] is True
    assert fresh["cross_fresh"] is True
    assert fresh["regime_state"] == "TREND_EXTENDED"

    stale = _moving_average_diagnostics(
        rth_history([*closes, 7440.0]),
        atr_5m=10.0,
    )
    assert stale["bars_since_cross"] == 7
    assert stale["cross_persistent_2_bars"] is True
    assert stale["cross_fresh"] is False


def test_ma50_ma200_warms_and_fails_closed_on_gap_or_contract_roll() -> None:
    warming = _moving_average_diagnostics(
        rth_history([7400.0 + index * 0.1 for index in range(199)]),
        atr_5m=8.0,
    )
    assert warming["sma200"] is None
    assert warming["regime_state"] is None
    assert warming["same_direction_convexity"] is None

    rows = rth_history([7400.0 + index * 0.1 for index in range(210)])
    rows[-100]["gap_before"] = True
    gapped = _moving_average_diagnostics(rows, atr_5m=8.0)
    assert gapped["sma200"] is None
    assert gapped["regime_state"] is None
    assert "sma200_rth_gap" in gapped["reasons"]

    rolled_rows = rth_history(
        [7300.0 + index * 0.1 for index in range(210)],
        contract_identity="ES:202609",
    )
    for row in rolled_rows[-50:]:
        row["contract_identity"] = "ES:202612"
    rolled = _moving_average_diagnostics(rolled_rows, atr_5m=8.0)
    assert rolled["contract_identity"] == "ES:202612"
    assert rolled["rth_bar_count"] == 50
    assert rolled["sma200"] is None
    assert rolled["regime_state"] is None


def test_spx_projection_includes_sma200_and_near_line_detection() -> None:
    projected = project_spx_equivalent_moving_averages(
        {
            "sma20": 7440.0,
            "sma50": 7425.0,
            "sma200": 7400.0,
            "distance_to_sma20_points": 10.0,
            "distance_to_sma50_points": 8.0,
            "distance_to_sma200_points": 4.0,
            "contract_identity": "ES:202609",
        },
        es_spx_basis_points=34.15,
        basis_contract_identity="ES:202609",
    )

    assert projected["spx_equivalent_sma200"] == 7365.85
    assert projected["spx_projection_near_line"] is True


def test_missing_breadth_and_range_history_are_not_filled_as_neutral() -> None:
    derived = build_market_state_5m_inputs(
        bars=trending_bars(),
        market_samples=market_samples(include_sectors=False),
        range_baselines=baselines(count=9),
        now=DAY,
    )

    assert derived["status"] == "incomplete"
    assert "breadth_above_vwap" in derived["missing"]
    assert "same_time_range_ratio" in derived["missing"]
    assert derived["diagnostics"]["same_time_range"]["status"] == "warming"
    assert derived["diagnostics"]["breadth"]["usable_count"] == 0


def test_provider_that_joins_late_cannot_fake_session_vwap() -> None:
    samples = market_samples()
    for row in samples[16:]:
        quote = dict(row["instruments"]["future:ES"])
        quote["provider"] = "ibkr"
        row["es_by_provider"]["ibkr"] = quote

    derived = build_market_state_5m_inputs(
        bars=trending_bars(),
        market_samples=samples,
        range_baselines=baselines(),
        now=DAY,
    )

    assert derived["diagnostics"]["vwap"]["provider"] == "schwab"
    assert derived["diagnostics"]["vwap"]["first_at"].startswith("2026-07-24T09:30")


def test_fresh_provider_beats_a_denser_but_stale_provider() -> None:
    start = DAY.replace(hour=9, minute=30)
    samples: list[dict[str, object]] = []
    for index in range(31):
        at = start + timedelta(minutes=index)
        providers: dict[str, object] = {}
        if index <= 20:
            providers["schwab"] = {
                "price": 7400.0 + index,
                "volume": 100_000.0 + index * 100.0,
                "source_at": at.isoformat(),
            }
        if index % 2 == 0:
            providers["ibkr"] = {
                "price": 7400.0 + index,
                "volume": 100_000.0 + index * 100.0,
                "source_at": at.isoformat(),
            }
        samples.append(
            {
                "at": at.isoformat(),
                "segment": "rth",
                "es_by_provider": providers,
            }
        )

    series, diagnostics = _es_vwap_series(
        samples,
        trading_date=DAY.date(),
        now=DAY,
    )

    assert series
    assert diagnostics["provider"] == "ibkr"
    assert datetime.fromisoformat(str(diagnostics["last_at"])) == DAY


def test_opening_range_rejects_intrabar_spike_that_closes_inside() -> None:
    bars = trending_bars()
    rth = [row for row in bars if row["segment"] == "rth"]
    rth[-2]["high"] = 7500.0
    rth[-2]["close"] = 7410.0
    rth[-1]["close"] = 7412.0

    derived = build_market_state_5m_inputs(
        bars=bars,
        market_samples=market_samples(),
        range_baselines=baselines(),
        now=DAY,
    )

    assert derived["values"]["opening_range_state"] == "INSIDE"


def test_same_time_baseline_updates_current_session_once_per_slot() -> None:
    first = update_same_time_range_baselines(
        baselines(),
        bars=trending_bars(),
        now=DAY,
    )
    second = update_same_time_range_baselines(
        first,
        bars=trending_bars(),
        now=DAY,
    )

    rows = second["slots"]["10:00"]
    assert len(rows) == 21
    current = [row for row in rows if row["trading_date_et"] == "2026-07-24"]
    assert len(current) == 1
    assert current[0]["source"] == "live_es_5m_ohlc"
    assert current[0]["dip_atr_30m"] == pytest.approx(0.166667)
    assert current[0]["rally_atr_30m"] == pytest.approx(5.0)
    assert second["rolling_path_window_minutes"] == 30


def test_rolling_path_percentiles_emit_low_confidence_at_five_prior_sessions() -> None:
    history = [
        {
            "trading_date_et": f"2026-06-{index + 1:02d}",
            "range_points": 31.0,
            "dip_atr_30m": 0.1,
            "rally_atr_30m": 1.0,
        }
        for index in range(5)
    ]

    derived = build_market_state_5m_inputs(
        bars=trending_bars(),
        market_samples=market_samples(),
        range_baselines={
            "schema_version": "market_state_5m_range_baselines.v1",
            "slots": {"10:00": history},
        },
        now=DAY,
    )

    rolling = derived["diagnostics"]["rolling_path_percentiles"]
    assert rolling["status"] == "provisional"
    assert rolling["confidence"] == "low"
    assert rolling["sample_count"] == 5
    assert rolling["minimum_sessions"] == 5
    assert rolling["target_sessions"] == 20
    assert rolling["dip"]["raw_percentile"] == pytest.approx(0.916667)
    assert rolling["dip"]["shrunk_percentile"] == pytest.approx(0.604167)
    assert rolling["rally"]["raw_percentile"] == pytest.approx(0.916667)
    assert rolling["rally"]["shrunk_percentile"] == pytest.approx(0.604167)
    assert rolling["probability_semantics"] == "historical_rank_not_forward_probability"
    assert rolling["action_authority"] == "none"


def test_rolling_path_percentiles_are_medium_and_exclude_current_or_future_dates() -> None:
    prior = [
        {
            "trading_date_et": f"2026-06-{index + 1:02d}",
            "range_points": 31.0,
            "dip_atr_30m": 0.0,
            "rally_atr_30m": 0.0,
        }
        for index in range(10)
    ]
    non_causal = [
        {
            "trading_date_et": trading_date,
            "range_points": 1.0,
            "dip_atr_30m": 999.0,
            "rally_atr_30m": 999.0,
        }
        for trading_date in ("2026-07-24", "2026-07-25")
    ]

    derived = build_market_state_5m_inputs(
        bars=trending_bars(),
        market_samples=market_samples(),
        range_baselines={
            "schema_version": "market_state_5m_range_baselines.v1",
            "slots": {"10:00": [*prior, *non_causal]},
        },
        now=DAY,
    )

    rolling = derived["diagnostics"]["rolling_path_percentiles"]
    assert rolling["status"] == "provisional"
    assert rolling["confidence"] == "medium"
    assert rolling["sample_count"] == 10
    assert rolling["dip"]["sample_count"] == 10
    assert rolling["rally"]["sample_count"] == 10
    assert rolling["dip"]["raw_percentile"] == pytest.approx(0.954545)
    assert rolling["rally"]["raw_percentile"] == pytest.approx(0.954545)
    assert rolling["dip"]["shrunk_percentile"] == pytest.approx(0.727273)
    assert rolling["rally"]["shrunk_percentile"] == pytest.approx(0.727273)


def test_rolling_path_percentiles_count_each_prior_session_once() -> None:
    rows = []
    for index in range(5):
        row = {
            "trading_date_et": f"2026-06-{index + 1:02d}",
            "range_points": 31.0,
            "dip_atr_30m": 0.1 + index,
            "rally_atr_30m": 1.0 + index,
        }
        rows.extend([row, {**row}])

    derived = build_market_state_5m_inputs(
        bars=trending_bars(),
        market_samples=market_samples(),
        range_baselines={
            "schema_version": "market_state_5m_range_baselines.v1",
            "slots": {"10:00": rows},
        },
        now=DAY,
    )

    rolling = derived["diagnostics"]["rolling_path_percentiles"]
    assert rolling["sample_count"] == 5
    assert rolling["dip"]["sample_count"] == 5
    assert rolling["rally"]["sample_count"] == 5


def test_rolling_path_recovers_after_an_earlier_rth_gap() -> None:
    rows = trending_bars()
    rth_start = DAY.replace(hour=9, minute=30)
    price = 7430.0
    for index in range(6, 13):
        start = rth_start + timedelta(minutes=5 * index)
        rows.append(
            bar(
                start,
                open_=price,
                high=price + 3.0,
                low=price - 1.0,
                close=price + 2.0,
                segment="rth",
            )
        )
        price += 2.0
    rows = [
        row
        for row in rows
        if row["bar_start"] != (rth_start + timedelta(minutes=10)).isoformat()
    ]
    now = DAY.replace(hour=10, minute=35)
    history = [
        {
            "trading_date_et": f"2026-06-{index + 1:02d}",
            "dip_atr_30m": 0.2,
            "rally_atr_30m": 1.0,
        }
        for index in range(5)
    ]
    prior = {
        "schema_version": "market_state_5m_range_baselines.v1",
        "slots": {"10:35": history},
    }

    derived = build_market_state_5m_inputs(
        bars=rows,
        market_samples=[],
        range_baselines=prior,
        now=now,
    )
    rolling = derived["diagnostics"]["rolling_path_percentiles"]

    assert derived["values"]["same_time_range_ratio"] is None
    assert rolling["status"] == "provisional"
    assert rolling["slot_et"] == "10:35"
    assert rolling["sample_count"] == 5

    updated = update_same_time_range_baselines(prior, bars=rows, now=now)
    current = [
        row
        for row in updated["slots"]["10:35"]
        if row["trading_date_et"] == "2026-07-24"
    ]
    assert len(current) == 1
    assert "range_points" not in current[0]
    assert current[0]["dip_atr_30m"] >= 0
    assert current[0]["rally_atr_30m"] >= 0


def test_rolling_path_keeps_mild_partial_bar_as_low_confidence_shadow_only() -> None:
    rows = trending_bars()
    rows[-1].update(
        {
            "quality": "partial",
            "sample_count": 47,
            "max_sample_gap_seconds": 31.915,
            "leading_edge_gap_seconds": 1.875,
            "trailing_edge_gap_seconds": 2.625,
            "contract_identity_ambiguous": False,
        }
    )
    history = [
        {
            "trading_date_et": f"2026-06-{index + 1:02d}",
            "dip_atr_30m": 0.1,
            "rally_atr_30m": 1.0,
        }
        for index in range(13)
    ]

    derived = build_market_state_5m_inputs(
        bars=rows,
        market_samples=market_samples(),
        range_baselines={
            "schema_version": "market_state_5m_range_baselines.v1",
            "slots": {"10:00": history},
        },
        now=DAY,
    )
    rolling = derived["diagnostics"]["rolling_path_percentiles"]

    assert derived["values"]["price_vs_vwap"] is None
    assert derived["values"]["market_structure"] is None
    assert derived["values"]["efficiency_ratio"] is None
    assert derived["values"]["same_time_range_ratio"] is None
    assert rolling["status"] == "provisional"
    assert rolling["slot_et"] == "10:00"
    assert rolling["sample_count"] == 13
    assert rolling["historical_sample_confidence"] == "medium"
    assert rolling["confidence"] == "low"
    assert rolling["confidence_cap_reason"] == "mild_partial_bar_observed_shadow_only"
    assert rolling["input_quality"] == "degraded"
    assert rolling["partial_bar_count"] == 1
    assert rolling["atr_source"] == "degraded_observed_rth_true_range"
    assert rolling["degraded_bar_policy"]["missing_prices_filled"] is False
    assert rolling["action_authority"] == "none"


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("sample_count", 29),
        ("max_sample_gap_seconds", -0.001),
        ("max_sample_gap_seconds", 45.001),
        ("leading_edge_gap_seconds", 30.001),
        ("trailing_edge_gap_seconds", 30.001),
    ),
)
def test_rolling_path_rejects_materially_incomplete_partial_bar(
    field: str,
    value: float,
) -> None:
    rows = trending_bars()
    rows[-1].update(
        {
            "quality": "partial",
            "sample_count": 47,
            "max_sample_gap_seconds": 31.915,
            "leading_edge_gap_seconds": 1.875,
            "trailing_edge_gap_seconds": 2.625,
            "contract_identity_ambiguous": False,
            field: value,
        }
    )

    derived = build_market_state_5m_inputs(
        bars=rows,
        market_samples=market_samples(),
        range_baselines=baselines(),
        now=DAY,
    )
    rolling = derived["diagnostics"]["rolling_path_percentiles"]

    assert rolling["status"] == "warming"
    assert rolling["sample_count"] == 0
    assert rolling["action_authority"] == "none"


def test_rolling_path_rejects_non_five_minute_bar_contract() -> None:
    rows = trending_bars()
    end = datetime.fromisoformat(str(rows[-1]["bar_end"]))
    rows[-1]["bar_end"] = (end + timedelta(seconds=1)).isoformat()

    derived = build_market_state_5m_inputs(
        bars=rows,
        market_samples=market_samples(),
        range_baselines=baselines(),
        now=end + timedelta(seconds=1),
    )
    rolling = derived["diagnostics"]["rolling_path_percentiles"]

    assert rolling["status"] == "warming"
    assert rolling["reason"] == "rolling_path_requires_six_contiguous_observed_bars"
    assert rolling["action_authority"] == "none"


def test_rolling_path_rejects_second_partial_or_contract_change() -> None:
    mild_partial = {
        "quality": "partial",
        "sample_count": 47,
        "max_sample_gap_seconds": 31.915,
        "leading_edge_gap_seconds": 4.291387,
        "trailing_edge_gap_seconds": 3.25,
        "contract_identity_ambiguous": False,
    }
    for mutation in ("second_partial", "contract_change"):
        rows = trending_bars()
        rows[-1].update(mild_partial)
        if mutation == "second_partial":
            rows[-2].update(mild_partial)
        else:
            rows[-1]["contract_identity"] = "ES:202612"

        derived = build_market_state_5m_inputs(
            bars=rows,
            market_samples=market_samples(),
            range_baselines=baselines(),
            now=DAY,
        )
        rolling = derived["diagnostics"]["rolling_path_percentiles"]

        assert rolling["status"] == "warming"
        assert rolling["sample_count"] == 0
        assert rolling["action_authority"] == "none"


def test_rolling_path_reproduces_july_29_boundary_partial_without_polluting_baseline() -> None:
    starts = [
        datetime(2026, 7, 29, 9, 45, tzinfo=ET),
        datetime(2026, 7, 29, 9, 50, tzinfo=ET),
        datetime(2026, 7, 29, 9, 55, tzinfo=ET),
        datetime(2026, 7, 29, 10, 0, tzinfo=ET),
        datetime(2026, 7, 29, 10, 5, tzinfo=ET),
        datetime(2026, 7, 29, 10, 10, tzinfo=ET),
    ]
    prices = [
        (7455.375, 7458.125, 7445.875, 7448.125),
        (7448.125, 7448.125, 7430.625, 7434.125),
        (7431.375, 7432.375, 7422.375, 7430.375),
        (7431.125, 7436.125, 7429.875, 7433.375),
        (7434.125, 7438.375, 7423.625, 7425.125),
        (7421.875, 7425.875, 7407.125, 7409.375),
    ]
    rows = [
        bar(
            start,
            open_=open_,
            high=high,
            low=low,
            close=close,
            segment="rth",
        )
        for start, (open_, high, low, close) in zip(starts, prices, strict=True)
    ]
    rows[-1].update(
        {
            "quality": "partial",
            "sample_count": 47,
            "max_sample_gap_seconds": 31.915,
            "leading_edge_gap_seconds": 4.291387,
            "trailing_edge_gap_seconds": 3.25,
            "contract_identity_ambiguous": False,
        }
    )
    history = [
        {
            "trading_date_et": f"2026-06-{index + 1:02d}",
            "dip_atr_30m": 0.1,
            "rally_atr_30m": 1.0,
        }
        for index in range(13)
    ]
    baseline = {
        "schema_version": "market_state_5m_range_baselines.v1",
        "slots": {"10:15": history},
    }
    now = datetime(2026, 7, 29, 10, 15, tzinfo=ET)

    derived = build_market_state_5m_inputs(
        bars=rows,
        market_samples=[],
        range_baselines=baseline,
        now=now,
    )
    rolling = derived["diagnostics"]["rolling_path_percentiles"]

    assert derived["values"]["efficiency_ratio"] is None
    assert derived["values"]["market_structure"] is None
    assert rolling["slot_et"] == "10:15"
    assert rolling["latest_bar_end"] == now.isoformat()
    assert rolling["close"] == 7409.375
    assert rolling["rolling_high"] == 7458.125
    assert rolling["rolling_low"] == 7407.125
    assert rolling["dip_points"] == 48.75
    assert rolling["rally_points"] == 2.25
    assert rolling["atr_5m"] == pytest.approx(13.541667)
    assert rolling["input_quality"] == "degraded"
    assert rolling["confidence"] == "low"
    assert rolling["sample_count"] == 13
    assert rolling["action_authority"] == "none"

    updated = update_same_time_range_baselines(baseline, bars=rows, now=now)
    assert updated == baseline


def test_future_dated_quotes_and_range_rows_cannot_leak_into_state() -> None:
    samples = market_samples()
    for row in samples:
        future_at = DAY + timedelta(days=1)
        for quote in row["instruments"].values():
            quote["source_at"] = future_at.isoformat()
        row["es_by_provider"]["schwab"]["source_at"] = future_at.isoformat()
    future_rows = [
        {
            "trading_date_et": f"2026-08-{index + 1:02d}",
            "range_points": 1.0,
        }
        for index in range(20)
    ]
    prior_rows = [
        {
            "trading_date_et": f"2026-06-{index + 1:02d}",
            "range_points": 31.0,
        }
        for index in range(9)
    ]

    derived = build_market_state_5m_inputs(
        bars=trending_bars(),
        market_samples=samples,
        range_baselines={
            "schema_version": "market_state_5m_range_baselines.v1",
            "slots": {"10:00": [*prior_rows, *future_rows]},
        },
        now=DAY,
    )

    assert derived["values"]["price_vs_vwap"] is None
    assert derived["values"]["breadth_above_vwap"] is None
    assert derived["values"]["same_time_range_ratio"] is None
    assert derived["diagnostics"]["same_time_range"]["sample_count"] == 9


def test_quote_exactly_at_bar_end_belongs_to_next_bar() -> None:
    target = trending_bars()[-1]
    end = datetime.fromisoformat(str(target["bar_end"]))
    mapped = _bar_vwaps(
        [target],
        [
            (end - timedelta(seconds=1), 7400.0),
            (end, 9999.0),
        ],
    )

    assert mapped[str(target["bar_start"])] == 7400.0


def test_missing_middle_bar_is_not_compressed_into_a_thirty_minute_window() -> None:
    bars = [
        row
        for row in trending_bars()
        if not (
            row["segment"] == "rth"
            and datetime.fromisoformat(str(row["bar_start"])).time()
            == datetime.strptime("09:45", "%H:%M").time()
        )
    ]

    derived = build_market_state_5m_inputs(
        bars=bars,
        market_samples=market_samples(),
        range_baselines=baselines(),
        now=DAY,
    )

    assert derived["values"]["market_structure"] is None
    assert derived["values"]["efficiency_ratio"] is None
    assert derived["values"]["vwap_cross_count"] is None
    assert derived["values"]["same_time_range_ratio"] is None
    assert (
        derived["diagnostics"]["same_time_range"]["reason"] == "rth_bars_not_continuous_from_open"
    )


def test_sector_breadth_rejects_cross_section_timestamp_skew() -> None:
    samples = market_samples()
    for instrument_id in SECTOR_INSTRUMENTS[:8]:
        samples[-1]["instruments"][instrument_id]["source_at"] = (
            DAY - timedelta(seconds=50)
        ).isoformat()

    derived = build_market_state_5m_inputs(
        bars=trending_bars(),
        market_samples=samples,
        range_baselines=baselines(),
        now=DAY,
    )

    assert derived["values"]["breadth_above_vwap"] is None
    assert derived["diagnostics"]["breadth"]["reason"] == "sector_cross_section_timestamp_skew"
    assert derived["diagnostics"]["breadth"]["cross_section_skew_seconds"] == 50.0


def test_vwap_accepts_two_minute_snapshot_gap_with_source_jitter() -> None:
    start = DAY.replace(hour=9, minute=30)
    points = [
        (start, 10_000.0, 100.0),
        (start + timedelta(seconds=60), 10_100.0, 101.0),
        (start + timedelta(seconds=187.8), 10_300.0, 103.0),
    ]

    series, diagnostics = _provider_vwap_series(
        points,
        trading_date=DAY.date(),
    )

    assert len(series) == 2
    assert diagnostics["max_gap_seconds"] == pytest.approx(127.8)


def test_vwap_sampling_gap_is_unavailable_then_recovers_from_fresh_delta() -> None:
    start = DAY.replace(hour=9, minute=30)
    before_gap = start + timedelta(minutes=1)
    gap_at = start + timedelta(minutes=4)
    recovered_at = start + timedelta(minutes=5)
    points = [
        (start, 10_000.0, 100.0),
        (before_gap, 11_000.0, 101.0),
        (gap_at, 11_100.0, 104.0),
        (recovered_at, 11_200.0, 105.0),
    ]

    series, diagnostics = _provider_vwap_series(
        points,
        trading_date=DAY.date(),
    )
    gap_bar_vwap = _bar_vwaps(
        [
            {
                "bar_start": (gap_at - timedelta(minutes=5)).isoformat(),
                "bar_end": (gap_at + timedelta(seconds=1)).isoformat(),
            }
        ],
        series,
    )
    recovered_bar_vwap = _bar_vwaps(
        [
            {
                "bar_start": gap_at.isoformat(),
                "bar_end": (recovered_at + timedelta(seconds=1)).isoformat(),
            }
        ],
        series,
    )

    assert [at for at, _ in series] == [before_gap, recovered_at]
    assert gap_bar_vwap == {}
    assert recovered_bar_vwap[gap_at.isoformat()] == pytest.approx(101.363636)
    assert diagnostics["gap_count"] == 1
    assert diagnostics["reset_count"] == 0
    assert diagnostics["observed_volume"] == pytest.approx(1_100.0)
    assert diagnostics["skipped_cross_gap_volume"] == pytest.approx(100.0)
    assert diagnostics["observed_volume_ratio"] == pytest.approx(11 / 12)
    assert diagnostics["minimum_observed_volume_ratio"] == 0.80
    assert diagnostics["partial_observed_volume"] is True
    assert diagnostics["volume_coverage"] == "partial_observed_deltas"


def test_vwap_sampling_gap_stays_unavailable_below_observed_volume_threshold() -> None:
    start = DAY.replace(hour=9, minute=30)
    before_gap = start + timedelta(minutes=1)
    gap_at = start + timedelta(minutes=4)
    after_gap_one = start + timedelta(minutes=5)
    after_gap_two = start + timedelta(minutes=6)
    points = [
        (start, 10_000.0, 100.0),
        (before_gap, 10_100.0, 101.0),
        (gap_at, 11_000.0, 104.0),
        (after_gap_one, 11_100.0, 105.0),
        (after_gap_two, 11_200.0, 106.0),
    ]

    series, diagnostics = _provider_vwap_series(
        points,
        trading_date=DAY.date(),
    )
    post_gap_bar_vwap = _bar_vwaps(
        [
            {
                "bar_start": gap_at.isoformat(),
                "bar_end": (after_gap_two + timedelta(seconds=1)).isoformat(),
            }
        ],
        series,
    )

    assert [at for at, _ in series] == [before_gap]
    assert post_gap_bar_vwap == {}
    assert diagnostics["observed_volume"] == pytest.approx(300.0)
    assert diagnostics["skipped_cross_gap_volume"] == pytest.approx(900.0)
    assert diagnostics["observed_volume_ratio"] == pytest.approx(0.25)
    assert diagnostics["minimum_observed_volume_ratio"] == 0.80


def test_vwap_volume_reset_skips_reset_delta_then_recovers() -> None:
    start = DAY.replace(hour=9, minute=30)
    before_reset = start + timedelta(minutes=1)
    reset_at = start + timedelta(minutes=2)
    recovered_at = start + timedelta(minutes=3)
    points = [
        (start, 10_000.0, 100.0),
        (before_reset, 10_100.0, 101.0),
        (reset_at, 5.0, 102.0),
        (recovered_at, 105.0, 103.0),
    ]

    series, diagnostics = _provider_vwap_series(
        points,
        trading_date=DAY.date(),
    )

    assert [at for at, _ in series] == [before_reset, recovered_at]
    assert series[-1][1] == pytest.approx(102.0)
    assert diagnostics["gap_count"] == 0
    assert diagnostics["reset_count"] == 1
    assert diagnostics["observed_volume"] == pytest.approx(200.0)
    assert diagnostics["skipped_cross_gap_volume"] == 0.0
    assert diagnostics["observed_volume_ratio"] == 1.0
    assert diagnostics["minimum_observed_volume_ratio"] == 0.80
    assert diagnostics["partial_observed_volume"] is True
    assert diagnostics["volume_coverage"] == "partial_observed_deltas"
