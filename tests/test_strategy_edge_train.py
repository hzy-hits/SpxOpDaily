from __future__ import annotations

from datetime import datetime, timedelta, timezone

from spx_spark.application.order_map.strategy_edge_model import FEATURE_NAMES
from spx_spark.data_platform.research.strategy_edge_train import train_edge_artifact


def _rows() -> list[dict[str, object]]:
    start = datetime(2026, 6, 20, 14, 0, tzinfo=timezone.utc)
    rows: list[dict[str, object]] = []
    for session_index in range(24):
        session = (start + timedelta(days=session_index)).date().isoformat()
        for candidate_index in range(4):
            features = {name: 0.0 for name in FEATURE_NAMES}
            features["return_5m_atr_directional"] = candidate_index / 10.0
            pnl = 0.35 + candidate_index / 20.0
            rows.append(
                {
                    "session_date": session,
                    "decision_at": (
                        start
                        + timedelta(days=session_index, minutes=candidate_index)
                    ).isoformat(),
                    "model_key": "rth|vertical",
                    "features": features,
                    "policy_pnl_points": pnl,
                    "profit": 1,
                    "stop_first_5m": 0,
                    "max_loss_points": 1.0,
                }
            )
    return rows


def test_walk_forward_artifact_can_promote_a_stable_synthetic_edge() -> None:
    artifact, report = train_edge_artifact(
        _rows(),
        generated_at=datetime(2026, 8, 18, tzinfo=timezone.utc),
        holdout_sessions=4,
        min_train_sessions=5,
        thresholds={
            "min_expected_pnl_points": -10.0,
            "min_expected_pnl_lcb_points": -10.0,
            "min_p_profit": 0.0,
            "max_p_stop_first_5m": 1.0,
            "min_return_on_risk": -10.0,
        },
        promotion_gates={
            "min_oof_trades": 1,
            "min_holdout_trades": 1,
            "min_profit_factor": 0.0,
            "min_average_pnl_points": 0.0,
            "min_positive_session_ratio": 0.0,
            "max_drawdown_r": 99.0,
            "max_top_session_profit_concentration": 1.0,
        },
    )

    model = artifact["models"]["rth|vertical"]
    assert model["promoted"] is True
    assert model["oof_metrics"]["net_pnl_points"] > 0
    assert model["holdout_metrics"]["net_pnl_points"] > 0
    assert len(model["feature_mean"]) == len(FEATURE_NAMES)
    assert report["promoted_models"] == ["rth|vertical"]
