from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from spx_spark.application.order_map import strategy_select
from spx_spark.application.order_map.strategy_edge_model import (
    EdgeAuthorityResult,
    apply_strategy_edge_authority,
)
from spx_spark.application.order_map.strategy_ranker import RankResult


NOW = datetime(2026, 8, 18, 15, 0, tzinfo=timezone.utc)


def test_v40_preaverage_manual_authority_is_explicitly_unvalidated(
    tmp_path: Path,
) -> None:
    candidate = {
        "setup_kind": "PREAVERAGE15_PULLBACK",
        "authorization_policy": "strategy_policy.bootstrap.v40",
        "evidence_contract_hash": (
            "sha256:fc276ff1d44bf4a150ff18889c445a6eaa68b12131b93b4c191765617fc1fb27"
        ),
        "evidence_status": "forward_unvalidated_user_override",
        "selection_score": 1.0,
    }

    result = apply_strategy_edge_authority(
        [candidate],
        {"session": {"mode": "rth"}},
        {},
        data_root=tmp_path,
        now=NOW,
    )

    assert result.rejected == []
    assert result.passed[0]["edge"]["edge_status"] == ("explicit_manual_policy_unvalidated")
    assert result.passed[0]["edge"]["strategy_edge"]["evidence_status"] == (
        "forward_unvalidated_user_override"
    )


def test_v44_close_convergence_manual_authority_is_explicitly_unvalidated(
    tmp_path: Path,
) -> None:
    candidate = {
        "setup_kind": "CLOSE_CONVERGENCE_60M",
        "authorization_policy": "strategy_policy.bootstrap.v57",
        "evidence_contract_hash": (
            "sha256:095333c301d7317da804792c243002c4dd36116e982970ee391b1c4dbd926732"
        ),
        "evidence_status": "forward_unvalidated_user_override",
        "close_convergence": {"status": "ready", "training_sessions": 28},
        "convergence_risk": {"objective_points": -0.1, "n_paths": 51},
        "selection_score": -0.1,
    }

    result = apply_strategy_edge_authority(
        [candidate],
        {"session": {"mode": "rth"}},
        {},
        data_root=tmp_path,
        now=NOW,
    )

    assert result.rejected == []
    assert result.passed[0]["edge"]["edge_status"] == ("explicit_manual_policy_unvalidated")
    assert result.passed[0]["edge"]["strategy_edge"]["convergence_risk"] == {
        "objective_points": -0.1,
        "n_paths": 51,
    }


def _gth_minute_candidate(**overrides: object) -> dict[str, object]:
    return {
        "candidate_id": "gth-call-7700-7710",
        "strategy_type": "CALL_DEBIT_VERTICAL",
        "setup_kind": "TREND_PULLBACK",
        "source": "gth_level_manual_candidate",
        "direction": "UP",
        "economics": {
            "width_points": 10.0,
            "max_loss_points": 4.0,
            "max_gain_points": 6.0,
            "debit_fraction_of_width": 0.40,
        },
        "selection_score": 1.0,
        **overrides,
    }


def test_v48_gth_confirmed_source_uses_minute_gate_without_model(tmp_path: Path) -> None:
    result = apply_strategy_edge_authority(
        [_gth_minute_candidate()],
        {
            "session": {"mode": "gth"},
            "path": {
                "return_1m_points": 0.25,
                "return_5m_points": -1.5,
                "atr_5m": 4.0,
            },
        },
        {"path_state": "TRANSITION", "path_direction": "DOWN"},
        data_root=tmp_path,
        now=NOW,
    )

    assert result.rejected == []
    candidate = result.passed[0]
    assert candidate["authorization_policy"] == "strategy_policy.bootstrap.v48"
    assert candidate["edge"]["edge_status"] == "explicit_manual_policy_unvalidated"
    assert candidate["edge"]["strategy_edge"]["gate_kind"] == "gth_minute_confirmation"


def test_v48_gth_minute_gate_rejects_opposing_1m_or_excess_risk(tmp_path: Path) -> None:
    result = apply_strategy_edge_authority(
        [_gth_minute_candidate(economics={
            "width_points": 30.0,
            "max_loss_points": 12.0,
            "max_gain_points": 18.0,
            "debit_fraction_of_width": 0.40,
        })],
        {
            "session": {"mode": "gth"},
            "path": {
                "return_1m_points": -0.25,
                "return_5m_points": 0.5,
                "atr_5m": 4.0,
            },
        },
        {},
        data_root=tmp_path,
        now=NOW,
    )

    assert result.passed == []
    assert "gth_1m_direction_not_confirmed" in result.rejected[0]["rejection_reasons"]
    assert "gth_minute_defined_risk_above_max" in result.rejected[0]["rejection_reasons"]


def test_v48_does_not_authorize_gth_width_scan(tmp_path: Path) -> None:
    result = apply_strategy_edge_authority(
        [
            _gth_minute_candidate(
                setup_kind="GTH_WIDTH_SCAN",
                source="gth_ibkr_width_enumeration",
            )
        ],
        {
            "session": {"mode": "gth"},
            "path": {
                "return_1m_points": 1.0,
                "return_5m_points": 2.0,
                "atr_5m": 4.0,
            },
        },
        {},
        data_root=tmp_path,
        now=NOW,
    )

    assert result.passed == []
    assert "strategy_edge_model_artifact_missing" in result.rejected[0]["rejection_reasons"]


def test_model_rejection_prevents_manual_authority(monkeypatch) -> None:
    facts = {
        "decision_at": NOW.isoformat(),
        "available_at": NOW.isoformat(),
        "session_date": "2026-08-18",
        "session": {"mode": "rth"},
        "minutes_to_close": 300.0,
        "capabilities": {
            "global": {
                "reasons": [],
                "session_legal": True,
                "coordinate_ready": True,
                "market_frame_ready": True,
            },
            "path": {"ready": True},
        },
        "path": {},
        "probability": {},
        "structure": {"strike_differential_context": {}},
        "rth_setups": [],
    }
    regime = {
        "path_state": "TREND",
        "path_direction": "DOWN",
        "terminal_state": "NONE",
        "entry_state": "INSUFFICIENT_DATA",
    }
    candidate = {
        "candidate_id": "candidate",
        "strategy_type": "PUT_DEBIT_VERTICAL",
        "setup_kind": "ES_VOLUME_MOMENTUM",
        "direction": "DOWN",
        "opportunity_id": "opportunity",
        "quote": {"status": "ready", "bid": 1.0, "ask": 1.2},
        "economics": {"max_loss_points": 1.2, "max_gain_points": 3.8},
        "long": {"strike": 7700.0, "source_at": NOW.isoformat()},
        "short": {"strike": 7695.0, "source_at": NOW.isoformat()},
        "failed_gates": [
            {
                "gate": "strategy_edge_lower_bound_below_threshold",
                "actual": -0.1,
                "threshold": 0.1,
            }
        ],
        "rejection_reasons": ["strategy_edge_lower_bound_below_threshold"],
    }

    monkeypatch.setattr(strategy_select, "build_market_fact_pack", lambda *_: facts)
    monkeypatch.setattr(strategy_select, "assess_regime", lambda *_: regime)
    monkeypatch.setattr(
        strategy_select,
        "_rth_committed_direction",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        strategy_select,
        "build_iron_condor_map",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        strategy_select,
        "enumerate_event_settlement_candidates",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        strategy_select,
        "enumerate_candidates",
        lambda *_args, **_kwargs: [candidate],
    )
    monkeypatch.setattr(
        strategy_select,
        "enumerate_iron_condor_candidates",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        strategy_select,
        "rank_candidates",
        lambda *_args, **_kwargs: RankResult(
            passed=[candidate],
            near_misses=[],
            gate_audit=[],
        ),
    )
    monkeypatch.setattr(
        strategy_select,
        "apply_strategy_edge_authority",
        lambda *_args, **_kwargs: EdgeAuthorityResult(
            passed=[],
            rejected=[candidate],
        ),
    )
    monkeypatch.setattr(
        strategy_select,
        "_attach_iron_condor_only_paths",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        strategy_select,
        "_base_decision",
        lambda *_args, **_kwargs: {
            "schema_version": "strategy_decision.v2",
            "decision_at": NOW.isoformat(),
            "available_at": NOW.isoformat(),
        },
    )

    decision = strategy_select.build_strategy_decision(
        {},
        object(),
        NOW,
        data_root="/production-data",
    )

    assert decision["decision_type"] == "NO_TRADE"
    assert decision["action_authority"] == "none"
    assert decision["why_not"]["primary_blocker"] == ("strategy_edge_lower_bound_below_threshold")
