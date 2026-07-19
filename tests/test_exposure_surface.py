from __future__ import annotations

import json
import math
from datetime import datetime, timedelta, timezone

import pytest

import spx_spark.features.exposure_surface as exposure_surface_module
from spx_spark.analytics.greeks.black_scholes import bs_gamma
from spx_spark.features.exposure_surface import (
    SCALAR_CALCULATION_ENGINE,
    SurfaceContract,
    SurfaceGridConfig,
    VECTORIZED_CALCULATION_ENGINE,
    build_exposure_surface,
)


AS_OF = datetime(2026, 7, 20, 18, 0, tzinfo=timezone.utc)
EXPIRY_CLOSE = AS_OF + timedelta(hours=2)
EXPIRY = "20260720"


def contract(
    strike: float,
    right: str,
    *,
    iv: float | None = 0.20,
    oi: float | None = 100.0,
    volume: float | None = 50.0,
    expiry: str = EXPIRY,
) -> SurfaceContract:
    return SurfaceContract(
        expiry=expiry,
        strike=strike,
        right=right,
        iv=iv,
        open_interest=oi,
        volume=volume,
    )


def crossing_contracts() -> tuple[SurfaceContract, ...]:
    return (
        contract(95.0, "C", oi=100.0, volume=1000.0),
        contract(95.0, "P", oi=1000.0, volume=100.0),
        contract(105.0, "C", oi=1000.0, volume=100.0),
        contract(105.0, "P", oi=100.0, volume=1000.0),
    )


def test_vectorized_surface_metrics_preserve_exact_call_put_cancellation() -> None:
    rows: list[SurfaceContract] = []
    for right in ("C", "P"):
        for index in range(64):
            rows.append(
                contract(
                    80.0 + index * 40.0 / 63.0,
                    right,
                    iv=0.05 + (index % 17) * 0.10,
                    oi=float(10 ** (index % 7)),
                    volume=float(10 ** (index % 7)),
                )
            )
    prepared, coverages, _qualities, _warnings = exposure_surface_module._prepared_contracts(
        tuple(rows),
        expiry=EXPIRY,
        config=SurfaceGridConfig(min_usable_contracts=1, min_coverage_ratio=0.0),
    )
    kwargs = {
        "spots": (95.0, 100.0, 105.0),
        "tau_seconds": 1_800.0,
        "coverages": coverages,
        "reference_spot": 100.0,
        "reference_tau_seconds": 1_800.0,
    }

    scalar, _ = exposure_surface_module._surface_metrics_scalar(
        prepared,
        reference_cache={},
        **kwargs,
    )
    vectorized, _ = exposure_surface_module._surface_metrics_vectorized(
        prepared,
        reference_cache={},
        **kwargs,
    )

    for weighting in ("oi_weighted", "volume_weighted"):
        for metric in ("signed_gamma", "charm", "vanna"):
            expected = scalar[weighting].to_dict()[metric]
            assert expected == (0.0, 0.0, 0.0)
            assert vectorized[weighting].to_dict()[metric] == expected


def test_vectorized_stable_sum_preserves_small_real_imbalance() -> None:
    rows = (
        contract(100.0, "C", oi=1_000_000.0, volume=1_000_000.0),
        contract(100.0, "P", oi=999_999.999999, volume=999_999.999999),
    )
    prepared, coverages, _qualities, _warnings = exposure_surface_module._prepared_contracts(
        rows,
        expiry=EXPIRY,
        config=SurfaceGridConfig(min_usable_contracts=1, min_coverage_ratio=0.0),
    )
    vectorized, failed = exposure_surface_module._surface_metrics_vectorized(
        prepared,
        spots=(100.0,),
        tau_seconds=1_800.0,
        coverages=coverages,
        reference_spot=100.0,
        reference_tau_seconds=1_800.0,
        reference_cache={},
    )

    assert failed == {"oi_weighted": False, "volume_weighted": False}
    for weighting in ("oi_weighted", "volume_weighted"):
        for metric in ("signed_gamma", "charm", "vanna"):
            value = vectorized[weighting].to_dict()[metric][0]
            assert value is not None and value != 0.0


def test_builds_rectangular_surface_with_zero_ridge_and_global_extrema() -> None:
    surface = build_exposure_surface(
        crossing_contracts(),
        spot=100.0,
        as_of=AS_OF,
        expiry_close=EXPIRY_CLOSE,
        spot_points=(90.0, 95.0, 100.0, 105.0, 110.0),
        time_offsets_minutes=(0.0, 30.0),
    )

    assert surface.schema_version == "spxw_exposure_surface.v1"
    assert surface.sign_convention == "calls_positive_puts_negative"
    assert surface.dealer_position_sign == "unknown"
    assert surface.metric_units["signed_gamma"] == (
        "proxy_delta_dollars_per_1pct_underlier_move"
    )
    assert "not_buy_sell_flow" in surface.weighting_semantics["volume_weighted"]
    assert surface.strike_ladder_basis == (
        "observed_contract_strike_revalued_at_reference_spot_minutes_forward_0"
    )
    assert surface.spot_grid == (90.0, 95.0, 100.0, 105.0, 110.0)
    assert len(surface.time_slices) == 2
    first = surface.time_slices[0]
    oi = first.weightings["oi_weighted"]
    assert oi.quality == "ok"
    assert all(len(series) == 5 for series in oi.metrics.to_dict().values())
    assert all(value is not None and value >= 0 for value in oi.metrics.gross_gamma)
    assert oi.metrics.signed_gamma[1] is not None and oi.metrics.signed_gamma[1] < 0
    assert oi.metrics.signed_gamma[3] is not None and oi.metrics.signed_gamma[3] > 0
    assert oi.zero_ridge_spot is not None
    assert 95.0 < oi.zero_ridge_spot < 105.0
    assert oi.positive_peak is not None and oi.positive_peak.value > 0
    assert oi.negative_trough is not None and oi.negative_trough.value < 0
    assert oi.coverage.usable_contracts == 4
    assert oi.coverage.ratio == 1.0

    assert tuple(row.strike for row in surface.strike_ladder) == (95.0, 105.0)
    low_strike = surface.strike_ladder[0]
    assert low_strike.call is not None and low_strike.call.open_interest == 100.0
    assert low_strike.put is not None and low_strike.put.open_interest == 1000.0
    ladder_oi = low_strike.weightings["oi_weighted"]
    assert ladder_oi.quality == "ok"
    assert ladder_oi.metrics.signed_gamma is not None
    assert ladder_oi.metrics.signed_gamma < 0
    assert ladder_oi.metrics.gross_gamma is not None
    assert ladder_oi.metrics.gross_gamma >= 0
    assert ladder_oi.metrics.charm is not None
    assert ladder_oi.metrics.vanna is not None

    payload = surface.to_dict()
    json.dumps(payload, allow_nan=False)


def test_oi_and_volume_weightings_are_independent() -> None:
    surface = build_exposure_surface(
        crossing_contracts(),
        spot=100.0,
        as_of=AS_OF,
        expiry_close=EXPIRY_CLOSE,
        spot_points=(95.0, 100.0, 105.0),
        time_offsets_minutes=(0.0,),
    )
    row = surface.time_slices[0]
    oi = row.weightings["oi_weighted"].metrics.signed_gamma
    volume = row.weightings["volume_weighted"].metrics.signed_gamma

    assert oi[0] is not None and volume[0] is not None
    assert oi[0] < 0 < volume[0]
    assert oi[-1] is not None and volume[-1] is not None
    assert volume[-1] < 0 < oi[-1]


def test_vectorized_kernel_matches_scalar_surface_and_reference_ladder() -> None:
    kwargs = {
        "spot": 100.0,
        "as_of": AS_OF,
        "expiry_close": EXPIRY_CLOSE,
        "spot_points": (90.0, 95.0, 100.0, 105.0, 110.0),
        "time_offsets_minutes": (0.0, 15.0, 30.0, 60.0),
    }
    scalar = build_exposure_surface(
        crossing_contracts(),
        **kwargs,
        _calculation_engine=SCALAR_CALCULATION_ENGINE,
    )
    vectorized = build_exposure_surface(
        crossing_contracts(),
        **kwargs,
        _calculation_engine=VECTORIZED_CALCULATION_ENGINE,
    )

    assert vectorized.quality == scalar.quality
    assert vectorized.warnings == scalar.warnings
    assert len(vectorized.time_slices) == len(scalar.time_slices)
    for actual_slice, expected_slice in zip(
        vectorized.time_slices,
        scalar.time_slices,
        strict=True,
    ):
        assert actual_slice.minutes_forward == expected_slice.minutes_forward
        assert actual_slice.tau_seconds == expected_slice.tau_seconds
        assert actual_slice.quality == expected_slice.quality
        assert actual_slice.warnings == expected_slice.warnings
        for weighting in ("oi_weighted", "volume_weighted"):
            actual = actual_slice.weightings[weighting]
            expected = expected_slice.weightings[weighting]
            assert actual.coverage == expected.coverage
            assert actual.quality == expected.quality
            assert actual.warnings == expected.warnings
            for metric, actual_values in actual.metrics.to_dict().items():
                assert actual_values == pytest.approx(
                    expected.metrics.to_dict()[metric],
                    rel=2e-13,
                    abs=1e-9,
                )
            assert actual.zero_ridge_spot == pytest.approx(
                expected.zero_ridge_spot,
                rel=2e-13,
                abs=1e-10,
            )

    assert tuple(row.strike for row in vectorized.strike_ladder) == tuple(
        row.strike for row in scalar.strike_ladder
    )
    for actual_row, expected_row in zip(
        vectorized.strike_ladder,
        scalar.strike_ladder,
        strict=True,
    ):
        assert actual_row.call == expected_row.call
        assert actual_row.put == expected_row.put
        assert actual_row.quality == expected_row.quality
        assert actual_row.warnings == expected_row.warnings
        for weighting in ("oi_weighted", "volume_weighted"):
            actual = actual_row.weightings[weighting]
            expected = expected_row.weightings[weighting]
            assert actual.quality == expected.quality
            assert actual.warnings == expected.warnings
            assert actual.metrics.signed_gamma == pytest.approx(
                expected.metrics.signed_gamma,
                rel=2e-13,
                abs=1e-9,
            )
            assert actual.metrics.gross_gamma == pytest.approx(
                expected.metrics.gross_gamma,
                rel=2e-13,
                abs=1e-9,
            )
            assert actual.metrics.charm == pytest.approx(
                expected.metrics.charm,
                rel=2e-13,
                abs=1e-12,
            )
            assert actual.metrics.vanna == pytest.approx(
                expected.metrics.vanna,
                rel=2e-13,
                abs=1e-12,
            )


def test_vectorized_kernel_preserves_zero_missing_and_independent_weightings() -> None:
    rows = (
        contract(95.0, "C", oi=0.0, volume=25.0),
        contract(105.0, "P", oi=0.0, volume=None),
    )
    surface = build_exposure_surface(
        rows,
        spot=100.0,
        as_of=AS_OF,
        expiry_close=EXPIRY_CLOSE,
        spot_points=(95.0, 100.0, 105.0),
        time_offsets_minutes=(0.0,),
        config=SurfaceGridConfig(min_usable_contracts=1),
        _calculation_engine=VECTORIZED_CALCULATION_ENGINE,
    )

    oi = surface.time_slices[0].weightings["oi_weighted"]
    volume = surface.time_slices[0].weightings["volume_weighted"]
    assert oi.coverage.usable_contracts == 2
    assert oi.metrics.signed_gamma == (0.0, 0.0, 0.0)
    assert oi.metrics.gross_gamma == (0.0, 0.0, 0.0)
    assert volume.coverage.usable_contracts == 1
    assert all(value is not None and value > 0.0 for value in volume.metrics.signed_gamma)
    assert all(value is not None and value > 0.0 for value in volume.metrics.gross_gamma)


def test_vectorized_kernel_fails_closed_on_nonfinite_exposure_math() -> None:
    surface = build_exposure_surface(
        (contract(5e-324, "C", iv=10.0, oi=10_000_000.0, volume=0.0),),
        spot=1e308,
        as_of=AS_OF,
        expiry_close=EXPIRY_CLOSE,
        spot_points=(1e308,),
        time_offsets_minutes=(0.0,),
        config=SurfaceGridConfig(min_usable_contracts=1, min_coverage_ratio=0.0),
        _calculation_engine=VECTORIZED_CALCULATION_ENGINE,
    )

    oi = surface.time_slices[0].weightings["oi_weighted"]
    assert oi.quality == "unavailable"
    assert oi.metrics.signed_gamma == (None,)
    assert "calculation_failed" in oi.warnings
    volume = surface.time_slices[0].weightings["volume_weighted"]
    assert volume.quality == "ok"
    assert volume.metrics.signed_gamma == (0.0,)
    assert surface.strike_ladder[0].weightings["oi_weighted"].quality == "unavailable"
    volume_ladder = surface.strike_ladder[0].weightings["volume_weighted"]
    assert volume_ladder.quality == "degraded"
    assert volume_ladder.metrics.signed_gamma == 0.0
    json.dumps(surface.to_dict(), allow_nan=False)


def test_vectorized_reference_cell_avoids_scalar_recalculation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unexpected_scalar(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("reference strike ladder recalculated scalar Greeks")

    monkeypatch.setattr(
        exposure_surface_module,
        "_contract_exposure_bases",
        unexpected_scalar,
    )
    surface = build_exposure_surface(
        crossing_contracts(),
        spot=100.0,
        as_of=AS_OF,
        expiry_close=EXPIRY_CLOSE,
        spot_points=(95.0, 100.0, 105.0),
        time_offsets_minutes=(0.0, 30.0),
        _calculation_engine=VECTORIZED_CALCULATION_ENGINE,
    )

    assert surface.quality == "ok"
    assert all(row.quality == "ok" for row in surface.strike_ladder)


def test_gamma_units_and_call_put_sign_convention_match_existing_kernel() -> None:
    contracts = (
        contract(100.0, "C", oi=200.0, volume=0.0),
        contract(100.0, "P", oi=50.0, volume=0.0),
    )
    surface = build_exposure_surface(
        contracts,
        spot=100.0,
        as_of=AS_OF,
        expiry_close=EXPIRY_CLOSE,
        spot_points=(100.0,),
        time_offsets_minutes=(0.0,),
        config=SurfaceGridConfig(min_usable_contracts=1),
    )
    values = surface.time_slices[0].weightings["oi_weighted"].metrics
    tau_years = (EXPIRY_CLOSE - AS_OF).total_seconds() / (365.0 * 24.0 * 3600.0)
    per_contract = bs_gamma(100.0, 100.0, 0.20, tau_years) * 100.0 * 100.0**2 * 0.01

    assert values.signed_gamma[0] == pytest.approx(per_contract * (200.0 - 50.0))
    assert values.gross_gamma[0] == pytest.approx(per_contract * (200.0 + 50.0))


def test_near_expiry_and_future_near_expiry_slices_fail_closed() -> None:
    near = build_exposure_surface(
        crossing_contracts(),
        spot=100.0,
        as_of=AS_OF,
        expiry_close=AS_OF + timedelta(minutes=4),
        spot_points=(95.0, 100.0, 105.0),
        time_offsets_minutes=(0.0,),
    )
    assert near.quality == "unavailable"
    assert "near_expiry_under_min_tau" in near.warnings
    assert near.time_slices[0].quality == "unavailable"
    assert near.time_slices[0].weightings["oi_weighted"].metrics.signed_gamma == (
        None,
        None,
        None,
    )

    partial = build_exposure_surface(
        crossing_contracts(),
        spot=100.0,
        as_of=AS_OF,
        expiry_close=AS_OF + timedelta(minutes=20),
        spot_points=(95.0, 100.0, 105.0),
        time_offsets_minutes=(0.0, 16.0),
    )
    assert partial.time_slices[0].quality == "ok"
    assert partial.time_slices[1].quality == "unavailable"
    assert "near_expiry_under_min_tau" in partial.time_slices[1].warnings


def test_invalid_contracts_degrade_coverage_without_serializing_nan() -> None:
    rows = (
        contract(95.0, "P", oi=1000.0),
        contract(105.0, "C", oi=1000.0),
        contract(100.0, "C", iv=math.nan, oi=1000.0),
        contract(100.0, "X", oi=1000.0),
    )
    surface = build_exposure_surface(
        rows,
        spot=100.0,
        as_of=AS_OF,
        expiry_close=EXPIRY_CLOSE,
        spot_points=(95.0, 100.0, 105.0),
        time_offsets_minutes=(0.0,),
    )
    oi = surface.time_slices[0].weightings["oi_weighted"]
    assert oi.coverage.total_contracts == 4
    assert oi.coverage.usable_contracts == 2
    assert oi.coverage.ratio == 0.5
    assert oi.quality == "degraded"
    assert "low_contract_coverage" in surface.time_slices[0].warnings
    assert "low_contract_coverage" in surface.warnings
    json.dumps(surface.to_dict(), allow_nan=False)


def test_zero_weights_are_valid_zero_surfaces_but_missing_weights_are_unavailable() -> None:
    zero_rows = tuple(
        contract(strike, right, oi=0.0, volume=0.0)
        for strike in (95.0, 105.0)
        for right in ("C", "P")
    )
    zero_surface = build_exposure_surface(
        zero_rows,
        spot=100.0,
        as_of=AS_OF,
        expiry_close=EXPIRY_CLOSE,
        spot_points=(95.0, 100.0, 105.0),
        time_offsets_minutes=(0.0,),
    )

    assert zero_surface.quality == "ok"
    for weighting in ("oi_weighted", "volume_weighted"):
        row = zero_surface.time_slices[0].weightings[weighting]
        assert row.quality == "ok"
        assert row.coverage.usable_contracts == 4
        assert row.metrics.signed_gamma == (0.0, 0.0, 0.0)
        assert row.metrics.gross_gamma == (0.0, 0.0, 0.0)
        assert all(
            strike.weightings[weighting].metrics.signed_gamma == 0.0
            for strike in zero_surface.strike_ladder
        )

    missing_rows = tuple(
        contract(strike, right, oi=None, volume=None)
        for strike in (95.0, 105.0)
        for right in ("C", "P")
    )
    missing_surface = build_exposure_surface(
        missing_rows,
        spot=100.0,
        as_of=AS_OF,
        expiry_close=EXPIRY_CLOSE,
        spot_points=(100.0,),
        time_offsets_minutes=(0.0,),
    )

    assert missing_surface.quality == "unavailable"
    assert "no_usable_contracts" in missing_surface.time_slices[0].warnings
    assert "no_usable_contracts" in missing_surface.warnings
    missing_oi = missing_surface.time_slices[0].weightings["oi_weighted"]
    assert missing_oi.metrics.signed_gamma == (None,)


def test_greeks_are_evaluated_once_per_active_contract_cell_and_reused_by_ladder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    counts = {"gamma": 0, "charm": 0, "vanna": 0}
    original_gamma = exposure_surface_module.bs_gamma
    original_charm = exposure_surface_module.bs_charm_per_minute
    original_vanna = exposure_surface_module.bs_vanna_per_vol_point

    def counted_gamma(*args: float) -> float:
        counts["gamma"] += 1
        return original_gamma(*args)

    def counted_charm(*args: float) -> float | None:
        counts["charm"] += 1
        return original_charm(*args)

    def counted_vanna(*args: float) -> float | None:
        counts["vanna"] += 1
        return original_vanna(*args)

    monkeypatch.setattr(exposure_surface_module, "bs_gamma", counted_gamma)
    monkeypatch.setattr(exposure_surface_module, "bs_charm_per_minute", counted_charm)
    monkeypatch.setattr(exposure_surface_module, "bs_vanna_per_vol_point", counted_vanna)

    build_exposure_surface(
        crossing_contracts(),
        spot=100.0,
        as_of=AS_OF,
        expiry_close=EXPIRY_CLOSE,
        spot_points=(95.0, 100.0, 105.0),
        time_offsets_minutes=(0.0, 30.0),
    )

    expected_contract_cells = 4 * 3 * 2
    assert counts == {
        "gamma": expected_contract_cells,
        "charm": expected_contract_cells,
        "vanna": expected_contract_cells,
    }


def test_realistic_input_bounds_fail_closed_without_nan_or_exceptions() -> None:
    rows = (
        contract(95.0, "C", iv=0.20, oi=100.0, volume=100.0),
        contract(95.0, "P", iv=0.20, oi=100.0, volume=100.0),
        contract(105.0, "C", iv=5e-324, oi=100.0, volume=100.0),
        contract(105.0, "P", iv=0.20, oi=1e308, volume=1e308),
    )
    surface = build_exposure_surface(
        rows,
        spot=100.0,
        as_of=AS_OF,
        expiry_close=EXPIRY_CLOSE,
        spot_points=(100.0,),
        time_offsets_minutes=(0.0,),
    )

    assert surface.quality == "degraded"
    oi = surface.time_slices[0].weightings["oi_weighted"]
    assert oi.quality == "degraded"
    assert oi.coverage.usable_contracts == 2
    assert "low_contract_coverage" in oi.warnings
    json.dumps(surface.to_dict(), allow_nan=False)


def test_kernel_failure_marks_metrics_and_all_summary_levels_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def broken_gamma(*_args: float) -> float:
        raise ValueError("broken kernel")

    monkeypatch.setattr(exposure_surface_module, "bs_gamma", broken_gamma)
    surface = build_exposure_surface(
        crossing_contracts(),
        spot=100.0,
        as_of=AS_OF,
        expiry_close=EXPIRY_CLOSE,
        spot_points=(100.0,),
        time_offsets_minutes=(0.0,),
    )

    assert surface.quality == "unavailable"
    assert "calculation_failed" in surface.warnings
    assert "calculation_failed" in surface.time_slices[0].warnings
    assert surface.time_slices[0].weightings["oi_weighted"].metrics.signed_gamma == (
        None,
    )
    assert all(row.quality == "unavailable" for row in surface.strike_ladder)


@pytest.mark.parametrize(
    ("spot_points", "time_offsets", "config", "message"),
    (
        (
            tuple(float(value) for value in range(10)),
            (0.0,),
            SurfaceGridConfig(max_spot_points=9),
            "spot grid exceeds",
        ),
        (
            (95.0, 100.0),
            tuple(float(value) for value in range(5)),
            SurfaceGridConfig(max_time_points=4),
            "time grid exceeds",
        ),
        (
            (95.0, 100.0, 105.0),
            (0.0, 5.0),
            SurfaceGridConfig(max_cells=5),
            "surface grid exceeds",
        ),
        (
            (100.0,),
            (0.0,),
            SurfaceGridConfig(max_contract_cell_evaluations=11),
            "surface Greek evaluation count exceeds",
        ),
    ),
)
def test_grid_limits_are_enforced(
    spot_points: tuple[float, ...],
    time_offsets: tuple[float, ...],
    config: SurfaceGridConfig,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        build_exposure_surface(
            crossing_contracts(),
            spot=100.0,
            as_of=AS_OF,
            expiry_close=EXPIRY_CLOSE,
            spot_points=spot_points,
            time_offsets_minutes=time_offsets,
            config=config,
        )


def test_mixed_expiries_and_naive_clocks_fail_closed_or_reject() -> None:
    mixed = (*crossing_contracts(), contract(100.0, "C", expiry="20260721"))
    surface = build_exposure_surface(
        mixed,
        spot=100.0,
        as_of=AS_OF,
        expiry_close=EXPIRY_CLOSE,
        spot_points=(100.0,),
        time_offsets_minutes=(0.0,),
    )
    assert surface.quality == "unavailable"
    assert "mixed_expiry_contracts" in surface.warnings

    mismatch = build_exposure_surface(
        crossing_contracts(),
        spot=100.0,
        as_of=AS_OF,
        expiry_close=EXPIRY_CLOSE + timedelta(days=1),
        spot_points=(100.0,),
        time_offsets_minutes=(0.0,),
    )
    assert mismatch.quality == "unavailable"
    assert "expiry_close_mismatch" in mismatch.warnings
    assert mismatch.strike_ladder
    assert all(row.quality == "unavailable" for row in mismatch.strike_ladder)

    with pytest.raises(ValueError, match="timezone-aware"):
        build_exposure_surface(
            crossing_contracts(),
            spot=100.0,
            as_of=AS_OF.replace(tzinfo=None),
            expiry_close=EXPIRY_CLOSE,
        )
