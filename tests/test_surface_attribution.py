from __future__ import annotations

from datetime import datetime, timezone

import pytest

from spx_spark.analytics.options.surface_attribution import attribute_candidate_surface


NOW = datetime(2026, 8, 7, 15, 0, tzinfo=timezone.utc)


def _candidate(*, right: str = "C", missing_iv: bool = False) -> dict[str, object]:
    strikes = (100.0, 110.0) if right == "C" else (100.0, 90.0)
    return {
        "strategy_type": f"{'CALL' if right == 'C' else 'PUT'}_DEBIT_VERTICAL",
        "expiry": "20260810",
        "long": {
            "strike": strikes[0],
            "right": right,
            "implied_vol": None if missing_iv else 0.20,
        },
        "short": {"strike": strikes[1], "right": right, "implied_vol": 0.22},
        "economics": {"width_points": 10.0, "max_loss_points": 3.0},
    }


def _facts() -> dict[str, object]:
    return {
        "spot": {"spx": 100.0},
        "volatility": {
            "atm_iv_0dte": 0.20,
            "put_skew_25d_0dte": 0.04,
            "call_skew_25d_0dte": 0.01,
            "atm_iv_minus_es_realized_vol": 0.03,
            "expected_move_points": 20.0,
        },
    }


def test_surface_attribution_returns_entry_frozen_loadings_and_negative_only_modifier() -> None:
    result = attribute_candidate_surface(
        _candidate(),
        _facts(),
        now=NOW,
        bump_vol_points=1.0,
        modifier_cap=0.05,
    )

    assert result["status"] == "ready"
    assert result["authority"] == "structure_risk_only"
    assert result["automatic_ordering"] is False
    assert result["coordinate"] == "entry_frozen_strike"
    assert result["surface_context"]["put_skew_25d_0dte"] == 0.04
    assert result["surface_exposure"]["atm_beta_points"] != 0.0
    assert result["surface_exposure"]["call_skew_beta_points"] != 0.0
    assert result["surface_exposure"]["put_skew_beta_points"] == pytest.approx(0.0)
    assert -0.05 <= result["decision_modifier"] <= 0.0


def test_surface_attribution_missing_iv_is_visible_and_has_zero_decision_effect() -> None:
    result = attribute_candidate_surface(
        _candidate(missing_iv=True),
        _facts(),
        now=NOW,
        bump_vol_points=1.0,
        modifier_cap=0.05,
    )

    assert result["status"] == "unavailable"
    assert result["decision_modifier"] == 0.0
    assert result["reason_codes"] == ["surface_leg_iv_unavailable"]


def test_surface_modifier_is_capped_and_does_not_become_a_positive_prior() -> None:
    candidate = _candidate()
    candidate["economics"] = {"width_points": 10.0, "max_loss_points": 0.01}
    result = attribute_candidate_surface(
        candidate,
        _facts(),
        now=NOW,
        bump_vol_points=1.0,
        modifier_cap=0.02,
    )

    assert result["decision_modifier"] == -0.02
