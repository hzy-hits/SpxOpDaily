from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

from spx_spark.application.order_map.strategy_edge_model import (
    FEATURE_NAMES,
    FEATURE_VERSION,
    SCHEMA_VERSION,
    apply_strategy_edge_authority,
    candidate_edge_features,
)


NOW = datetime(2026, 8, 18, 15, 0, tzinfo=timezone.utc)


def _candidate(*, setup_kind: str = "MODEL_VERTICAL") -> dict[str, object]:
    return {
        "candidate_id": "put-7700-7695",
        "strategy_type": "PUT_DEBIT_VERTICAL",
        "setup_kind": setup_kind,
        "direction": "DOWN",
        "target_spx": 7695.0,
        "invalidation_spx": 7712.0,
        "right": "P",
        "long": {
            "strike": 7700.0,
            "right": "P",
            "delta": -0.45,
            "implied_vol": 0.15,
        },
        "short": {
            "strike": 7695.0,
            "right": "P",
            "delta": -0.35,
            "implied_vol": 0.155,
        },
        "quote": {"status": "ready", "bid": 1.8, "ask": 2.0},
        "economics": {
            "width_points": 5.0,
            "max_loss_points": 2.0,
            "max_gain_points": 3.0,
            "breakeven_spx": 7698.0,
            "debit_fraction_of_width": 0.4,
        },
        "edge": {"edge_status": "research_unvalidated"},
    }


def _facts() -> dict[str, object]:
    return {
        "minutes_to_close": 300.0,
        "session": {"mode": "rth"},
        "spot": {"spx": 7706.0},
        "path": {
            "atr_5m": 5.0,
            "return_1m_points": -1.0,
            "return_5m_points": -5.0,
            "impulse_15m_points": -9.0,
            "return_60m_points": -24.0,
            "distance_to_vwap_points": -12.0,
            "efficiency_ratio_30m": 0.55,
            "vwap_crosses_30m": 1.0,
            "vwap_slope": -0.5,
            "breadth_above_vwap": 0.30,
            "direction_score": -8.0,
        },
        "volatility": {
            "expected_move_points": 30.0,
            "atm_iv_0dte": 0.16,
            "atm_iv_change_5m": 0.01,
            "atm_iv_change_15m": 0.02,
            "vix_return_15m_pct": 0.01,
        },
        "structure": {
            "put_wall": 7695.0,
            "call_wall": 7730.0,
            "zero_gamma": 7710.0,
            "flip_zone": [7700.0, 7705.0],
        },
        "shock": {"state": "NONE"},
    }


def _regime() -> dict[str, object]:
    return {
        "path_state": "TREND",
        "path_direction": "DOWN",
        "event_state": "NORMAL",
        "terminal_state": "NONE",
    }


def _write_artifact(
    root: Path,
    *,
    promoted: bool = True,
    expected: float = 0.50,
    residual_q10: float = -0.10,
    p_profit_logit: float = 1.0,
    p_stop_logit: float = -2.0,
) -> None:
    model = {
        "model_version": "entry_edge_rth_vertical_v1",
        "promoted": promoted,
        "promotion": {"promoted": promoted},
        "thresholds": {
            "min_expected_pnl_points": 0.25,
            "min_expected_pnl_lcb_points": 0.10,
            "min_p_profit": 0.58,
            "max_p_stop_first_5m": 0.30,
            "min_return_on_risk": 0.08,
        },
        "residual_q10_points": residual_q10,
        "feature_mean": [0.0] * len(FEATURE_NAMES),
        "feature_scale": [1.0] * len(FEATURE_NAMES),
        "pnl": {"intercept": expected, "coef": [0.0] * len(FEATURE_NAMES)},
        "profit": {
            "intercept": p_profit_logit,
            "coef": [0.0] * len(FEATURE_NAMES),
        },
        "stop_first_5m": {
            "intercept": p_stop_logit,
            "coef": [0.0] * len(FEATURE_NAMES),
        },
        "max_abs_z": 100.0,
        "trained_through": "2026-08-17",
        "holdout_metrics": {"profit_factor": 1.4},
    }
    artifact = {
        "schema_version": SCHEMA_VERSION,
        "feature_version": FEATURE_VERSION,
        "artifact_version": "entry_edge.v1:test",
        "generated_at": NOW.isoformat(),
        "valid_days": 14,
        "feature_names": list(FEATURE_NAMES),
        "models": {"rth|vertical": model},
    }
    path = root / "research" / "strategy_edge_model.v1.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(artifact), encoding="utf-8")


def test_feature_vector_is_complete_and_directional() -> None:
    features = candidate_edge_features(_candidate(), _facts(), _regime(), now=NOW)

    assert tuple(features) == FEATURE_NAMES
    assert features["return_5m_atr_directional"] == 1.0
    assert features["path_aligned"] == 1.0
    assert features["target_distance_atr"] > 0.0
    assert features["stop_distance_atr"] > 0.0


def test_promoted_positive_edge_is_the_only_pass(tmp_path: Path) -> None:
    _write_artifact(tmp_path)

    result = apply_strategy_edge_authority(
        [_candidate()], _facts(), _regime(), data_root=tmp_path, now=NOW
    )

    assert len(result.passed) == 1
    assert result.rejected == []
    edge = result.passed[0]["edge"]["strategy_edge"]
    assert edge["expected_pnl_points"] == 0.5
    assert edge["expected_pnl_lcb_points"] == 0.4
    assert edge["model_coverage"] == "in_domain"
    assert result.passed[0]["edge"]["edge_status"] == "promoted_model_pass"


def test_missing_or_unpromoted_artifact_fails_closed(tmp_path: Path) -> None:
    missing = apply_strategy_edge_authority(
        [_candidate()], _facts(), _regime(), data_root=tmp_path, now=NOW
    )
    assert missing.passed == []
    assert "strategy_edge_model_artifact_missing" in missing.rejected[0][
        "rejection_reasons"
    ]

    _write_artifact(tmp_path, promoted=False)
    unpromoted = apply_strategy_edge_authority(
        [_candidate()], _facts(), _regime(), data_root=tmp_path, now=NOW
    )
    assert unpromoted.passed == []
    assert "strategy_edge_model_not_promoted" in unpromoted.rejected[0][
        "rejection_reasons"
    ]


def test_missing_artifact_allows_authorized_es_momentum_fallback(tmp_path: Path) -> None:
    candidate = _candidate(setup_kind="ES_VOLUME_MOMENTUM")
    regime = {**_regime(), "policy_version": "strategy_policy.bootstrap.v57"}

    result = apply_strategy_edge_authority(
        [candidate], _facts(), regime, data_root=tmp_path, now=NOW
    )

    assert result.rejected == []
    assert result.passed[0]["edge"]["edge_status"] == (
        "explicit_manual_policy_unvalidated"
    )
    assert result.passed[0]["edge"]["strategy_edge"]["fallback_reason"] == (
        "strategy_edge_model_artifact_missing"
    )


def test_es_momentum_fallback_requires_frozen_policy_contract(tmp_path: Path) -> None:
    candidate = _candidate(setup_kind="ES_VOLUME_MOMENTUM")
    regime = {**_regime(), "policy_version": "strategy_policy.bootstrap.v56"}

    result = apply_strategy_edge_authority(
        [candidate], _facts(), regime, data_root=tmp_path, now=NOW
    )

    assert result.passed == []
    assert "es_volume_momentum_policy_authority_invalid" in result.rejected[0][
        "rejection_reasons"
    ]


def test_nonpositive_lower_bound_is_rejected(tmp_path: Path) -> None:
    _write_artifact(tmp_path, expected=0.20, residual_q10=-0.30)

    result = apply_strategy_edge_authority(
        [_candidate()], _facts(), _regime(), data_root=tmp_path, now=NOW
    )

    assert result.passed == []
    assert "strategy_edge_lower_bound_below_threshold" in result.rejected[0][
        "rejection_reasons"
    ]


def test_pure_fixture_without_data_root_preserves_legacy_candidate() -> None:
    result = apply_strategy_edge_authority(
        [_candidate()], _facts(), _regime(), data_root=None, now=NOW
    )

    assert len(result.passed) == 1
    assert result.rejected == []
    assert "strategy_edge" not in result.passed[0]["edge"]
