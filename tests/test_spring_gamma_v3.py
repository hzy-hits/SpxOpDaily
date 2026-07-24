from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any

import pytest

from spx_spark.application.market_features.spring_gamma_v3 import (
    CALIBRATION_STATUS,
    MODEL_VERSION,
    SCHEMA_VERSION,
    build_spring_gamma_v3_shadow,
)


NOW = datetime(2026, 7, 24, 14, 0, tzinfo=timezone.utc)
EXPIRY = "20260724"


def _settings(**overrides: object) -> SimpleNamespace:
    values: dict[str, object] = {
        "enabled": True,
        "report_enabled": True,
        "prediction_interval_seconds": 900,
        "horizons_minutes": (15, 30, 60),
        "rth_greek_max_age_seconds": 20.0,
        "rth_iv_max_age_seconds": 20.0,
        "gth_greek_max_age_seconds": 90.0,
        "gth_iv_max_age_seconds": 90.0,
        "min_pair_ratio": 0.80,
        "min_iv_coverage": 0.80,
        "min_delta_coverage": 0.80,
        "min_oi_coverage": 0.80,
        "min_paired_strikes": 3,
        "min_probability": 0.60,
        "min_margin": 0.10,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _inputs(
    *,
    at: datetime = NOW - timedelta(seconds=1),
    segment: str = "rth",
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    market = {
        "frame_id": "market:test",
        "session_id": "2026-07-24",
        "as_of": at.isoformat(),
        "quality": "ready",
        "diagnostics": {"segment": segment},
        "es": {
            "price": 6030.0,
            "source_at": at.isoformat(),
            "observed_at": at.isoformat(),
            "return_5m_points": 4.0,
            "return_15m_points": 9.0,
            "return_60m_points": 24.0,
            "return_180m_points": 31.0,
            "vwap_distance_points": 8.0,
            "vwap_slope_15m_points": 1.5,
            "trend_efficiency_60m": 0.72,
        },
    }
    option = {
        "frame_id": "options:test",
        "as_of": at.isoformat(),
        "quality": "ready",
        "front_expiry": EXPIRY,
        "structure": {"frozen": False},
    }
    greek = {
        "status": "ok",
        "as_of": at.isoformat(),
        "expiry": EXPIRY,
        "model": {"name": "finite_difference_bs", "spot": 6000.0},
        "coverage": {"usable_ratio": 1.0, "oi_ratio": 1.0},
        "aggregate": {
            "as_of": at.isoformat(),
            "expiry": EXPIRY,
            "quality": "ok",
            "iv_coverage_ratio": 1.0,
            "oi_coverage_ratio": 1.0,
            "gross_gamma_abs": 1_000_000.0,
            "gross_charm_5m_abs": 1_000_000.0,
            "gross_vanna_1vol_abs": 500_000.0,
        },
    }
    strikes = []
    for strike in (5970.0, 5980.0, 5990.0, 6000.0, 6010.0, 6020.0, 6030.0):
        strikes.append(
            {
                "strike": strike,
                "call_open_interest": 100.0,
                "put_open_interest": 110.0,
                "call_iv": 0.18,
                "put_iv": 0.19,
                "call_delta": 0.52,
                "put_delta": -0.48,
                "call_gamma": 0.012,
                "put_gamma": 0.013,
                "call_vanna_per_vol_point": 0.002,
                "put_vanna_per_vol_point": -0.002,
                "call_charm_per_minute": -0.0002,
                "put_charm_per_minute": 0.0002,
            }
        )
    exposure = {
        "as_of": at.isoformat(),
        "underlier": {"price": 6000.0, "source": "SPX"},
        "expiries": [
            {
                "expiry": EXPIRY,
                "quality": "ok",
                "oi_quality": "ibkr_ok",
                "snapshot_age_seconds": 1.0,
                "iv_coverage_ratio": 1.0,
                "delta_coverage_ratio": 1.0,
                "strikes": strikes,
                "oi_weighted": {
                    "net_gamma_ratio": 0.20,
                    "vex_proxy": 12.0,
                    "cex_proxy": -4.0,
                },
                "sign_convention": "calls_positive_puts_negative",
            }
        ],
    }
    return market, option, greek, exposure


def _build(
    inputs: tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]],
    **kwargs: object,
) -> dict[str, Any]:
    market, option, greek, exposure = inputs
    return build_spring_gamma_v3_shadow(
        market_frame=market,
        option_frame=option,
        greek_reference=greek,
        exposure_map=exposure,
        now=NOW,
        expected_expiry=EXPIRY,
        settings=kwargs.pop("settings", _settings()),
        **kwargs,
    )


def test_ready_trend_is_continuous_es_only_and_permanently_non_actionable() -> None:
    inputs = _inputs()
    result = _build(inputs)

    assert result["schema_version"] == SCHEMA_VERSION
    assert result["model_version"] == MODEL_VERSION
    assert result["prediction_id"].startswith("spring-gamma-v3:2026-07-24:")
    assert len(result["input_fingerprint"]) == 64
    assert result["status"] == "ready"
    assert result["opportunity"] == "trend_continuation"
    assert result["direction"]["decision"] == "up"
    assert result["direction"]["calibration_status"] == CALIBRATION_STATUS
    assert result["calibration_status"] == CALIBRATION_STATUS
    assert result["direction_authority"] == "none"
    assert result["action_authority"] == "none"
    assert result["actionable"] is False
    assert result["automatic_ordering"] is False
    assert set(result["direction"]["scores"]) == {"15m", "30m", "60m"}
    assert all(
        -1.0 < value < 1.0 for value in result["direction"]["scores"].values() if value is not None
    )
    assert result["direction"]["score_method"]["30m"].startswith("linear_interpolation")
    assert "return_30m" not in result["direction"]["feature_weights"]

    changed = deepcopy(inputs)
    changed[2]["aggregate"]["gross_gamma_abs"] *= 3.0
    changed[2]["aggregate"]["gross_charm_5m_abs"] *= 3.0
    changed[2]["aggregate"]["gross_vanna_1vol_abs"] *= 3.0
    changed[3]["expiries"][0]["oi_weighted"]["net_gamma_ratio"] = -0.90
    for row in changed[3]["expiries"][0]["strikes"]:
        row["call_vanna_per_vol_point"] *= -20.0
        row["put_charm_per_minute"] *= -20.0
    changed_result = _build(changed)

    assert changed_result["direction"] == result["direction"]
    assert changed_result["opportunity"] == result["opportunity"]
    assert changed_result["risk"]["net_gamma_ratio_proxy"] == -0.90


def test_charm_vanna_only_reduce_confidence_and_never_flip_direction() -> None:
    low_risk = _build(_inputs())
    high_inputs = deepcopy(_inputs())
    high_inputs[2]["aggregate"]["gross_charm_5m_abs"] = 100_000_000.0
    high_inputs[2]["aggregate"]["gross_vanna_1vol_abs"] = 100_000_000.0
    high_risk = _build(high_inputs)

    assert high_risk["risk"]["bounded_penalty"] > low_risk["risk"]["bounded_penalty"]
    assert abs(high_risk["direction"]["confidence_score"]) < abs(
        low_risk["direction"]["confidence_score"]
    )
    assert high_risk["direction"]["diagnostic_es_direction"] == "up"
    assert high_risk["risk"]["direction_sign_effect"] == "none"
    assert high_risk["risk"]["direction_score_adjustment"] == 0.0


def test_spring_reversion_requires_es_reversal_and_whitelisted_fade_path() -> None:
    inputs = _inputs()
    inputs[0]["es"].update(
        {
            "return_5m_points": 6.0,
            "return_15m_points": 8.0,
            "return_60m_points": -12.0,
            "return_180m_points": -20.0,
            "vwap_distance_points": -2.0,
            "vwap_slope_15m_points": 0.5,
        }
    )
    level = {
        "phase": "rejected",
        "thesis": "fade",
        "level_kind": "put_wall",
        "level": 5980.0,
        "spot": 5988.0,
        "quality_ok": True,
        "direction": "down",
        "event_id": "must-not-enter-model",
    }
    result = _build(inputs, level_decision=level)

    assert result["status"] == "ready"
    assert result["opportunity"] == "spring_reversion"
    assert result["direction"]["decision"] == "up"
    assert result["level_gate"]["distance"] == 8.0
    assert "direction" not in result["level_gate"]
    assert "event_id" not in result["level_gate"]

    opposite_claim = _build(inputs, level_decision={**level, "direction": "up"})
    assert opposite_claim["direction"] == result["direction"]
    assert opposite_claim["input_fingerprint"] == result["input_fingerprint"]

    no_fade = _build(inputs, level_decision={**level, "thesis": "breakout"})
    assert no_fade["status"] == "abstain"
    assert no_fade["opportunity"] == "transition"
    assert "spring_reversion_path_unconfirmed" in no_fade["abstain_reasons"]


@pytest.mark.parametrize(
    ("mutation", "reason"),
    [
        (
            lambda values: values[3]["expiries"][0]["strikes"][0].update(call_iv=None),
            "iv_coverage_insufficient",
        ),
        (
            lambda values: values[3]["expiries"][0]["strikes"][0].update(put_delta=None),
            "delta_coverage_insufficient",
        ),
        (
            lambda values: values[3]["expiries"][0]["strikes"][0].update(call_gamma=None),
            "greek_coverage_insufficient",
        ),
        (
            lambda values: values[3]["expiries"][0]["strikes"][0].update(
                put_vanna_per_vol_point=None
            ),
            "greek_coverage_insufficient",
        ),
        (
            lambda values: values[3]["expiries"][0]["strikes"][0].update(
                call_charm_per_minute=None
            ),
            "greek_coverage_insufficient",
        ),
        (
            lambda values: values[2]["aggregate"].update(gross_charm_5m_abs=None),
            "greek_charm_missing",
        ),
    ],
)
def test_missing_iv_delta_or_greeks_fail_closed(
    mutation: Any,
    reason: str,
) -> None:
    inputs = _inputs()
    mutation(inputs)
    strict = _settings(
        min_pair_ratio=1.0,
        min_iv_coverage=1.0,
        min_delta_coverage=1.0,
    )
    result = _build(inputs, settings=strict)

    assert result["status"] == "abstain"
    assert result["direction"]["decision"] == "abstain"
    assert reason in result["abstain_reasons"]
    assert result["direction"]["diagnostic_es_direction"] == "up"
    assert max(result["direction"]["p_up"], result["direction"]["p_down"]) < 0.60


def test_zero_oi_leg_is_not_mislabeled_as_a_missing_call_put_pair() -> None:
    inputs = _inputs()
    inputs[3]["expiries"][0]["strikes"][0]["call_open_interest"] = 0.0
    result = _build(inputs)

    assert result["status"] == "ready"
    assert result["quality"]["coverage"]["paired_strikes"] == 7
    assert result["quality"]["coverage"]["complete_pair_ratio"] == 1.0
    assert result["quality"]["coverage"]["nonzero_oi_leg_ratio"] < 1.0
    assert "oi_coverage_ratio" not in result["quality"]["coverage"]


def test_zero_charm_and_vanna_are_valid_observations_not_missing_values() -> None:
    inputs = _inputs()
    inputs[2]["aggregate"]["gross_charm_5m_abs"] = 0.0
    inputs[2]["aggregate"]["gross_vanna_1vol_abs"] = 0.0
    result = _build(inputs)

    assert result["status"] == "ready"
    assert result["risk"]["charm_equiv_5m"] == 0.0
    assert result["risk"]["vanna_equiv_1vol"] == 0.0
    assert result["risk"]["bounded_penalty"] == 0.0


def test_one_sided_wing_abstains_even_when_global_ratio_passes() -> None:
    inputs = _inputs()
    for row in inputs[3]["expiries"][0]["strikes"]:
        if row["strike"] < 6000.0:
            row["call_iv"] = None
            row["put_iv"] = None
    lenient = _settings(
        min_pair_ratio=0.50,
        min_iv_coverage=0.50,
        min_delta_coverage=0.50,
        min_paired_strikes=3,
    )
    result = _build(inputs, settings=lenient)
    coverage = result["quality"]["coverage"]

    assert coverage["complete_pair_ratio"] >= 0.50
    assert coverage["core_complete_pair_ratio"] >= 0.50
    assert coverage["left_wing_paired_strikes"] == 0
    assert result["status"] == "abstain"
    assert "left_wing_unpaired" in result["abstain_reasons"]


def test_rth_and_gth_use_independent_freshness_profiles() -> None:
    old = NOW - timedelta(seconds=30)
    rth_inputs = _inputs(at=old, segment="rth")
    gth_inputs = _inputs(at=old, segment="europe")

    rth = _build(rth_inputs)
    gth = _build(gth_inputs)

    assert rth["session"] == "rth"
    assert rth["status"] == "abstain"
    assert "market_frame_stale" in rth["abstain_reasons"]
    assert "greek_reference_stale" in rth["abstain_reasons"]
    assert "iv_surface_stale" in rth["abstain_reasons"]
    assert rth["direction"]["diagnostic_es_direction"] == "up"
    assert max(rth["direction"]["p_up"], rth["direction"]["p_down"]) < 0.60

    assert gth["session"] == "gth"
    assert gth["status"] == "ready"
    assert gth["quality"]["policy"]["greek_max_age_seconds"] == 90.0
    assert gth["risk"]["prior"] == "fixed_weak_gth"
    assert gth["risk"]["bounded_penalty"] < rth["risk"]["bounded_penalty"]


@pytest.mark.parametrize(
    ("mutation", "reason"),
    [
        (
            lambda values: values[1]["structure"].update(frozen=True),
            "option_structure_frozen",
        ),
        (
            lambda values: values[1].update(front_expiry="20260727"),
            "option_exact_expiry_mismatch",
        ),
        (
            lambda values: values[2].update(expiry="20260727"),
            "greek_exact_expiry_mismatch",
        ),
        (
            lambda values: values[3].update(expiries=[]),
            "exposure_exact_expiry_unavailable",
        ),
        (
            lambda values: values[3]["expiries"][0]["oi_weighted"].update(net_gamma_ratio=None),
            "net_gamma_ratio_unavailable",
        ),
    ],
)
def test_frozen_or_non_exact_expiry_inputs_fail_closed(
    mutation: Any,
    reason: str,
) -> None:
    inputs = _inputs()
    mutation(inputs)
    result = _build(inputs)

    assert result["status"] == "abstain"
    assert result["opportunity"] == "abstain"
    assert reason in result["abstain_reasons"]


def test_missing_es_does_not_default_to_down() -> None:
    inputs = _inputs()
    inputs[0]["es"]["return_15m_points"] = None
    result = _build(inputs)

    assert result["status"] == "abstain"
    assert result["direction"]["decision"] == "abstain"
    assert result["direction"]["scores"]["15m"] is None
    assert "es_direction_inputs_incomplete" in result["abstain_reasons"]


def test_disabled_shadow_is_explicit_and_never_actionable() -> None:
    result = _build(_inputs(), settings=_settings(enabled=False))

    assert result["status"] == "disabled"
    assert result["direction"]["decision"] == "abstain"
    assert result["actionable"] is False
    assert result["automatic_ordering"] is False
    assert "shadow_disabled" in result["abstain_reasons"]


def test_nonfinite_inputs_abstain_without_breaking_fingerprint_serialization() -> None:
    inputs = _inputs()
    inputs[0]["es"]["return_15m_points"] = float("nan")
    inputs[2]["aggregate"]["gross_gamma_abs"] = float("inf")
    inputs[3]["expiries"][0]["strikes"][0]["put_gamma"] = float("nan")

    result = _build(inputs, settings=_settings(min_pair_ratio=1.0))

    json.dumps(result, allow_nan=False)
    assert len(result["input_fingerprint"]) == 64
    assert result["status"] == "abstain"
    assert result["direction"]["scores"]["15m"] is None
    assert "greek_gamma_missing" in result["abstain_reasons"]
    assert "greek_coverage_insufficient" in result["abstain_reasons"]
