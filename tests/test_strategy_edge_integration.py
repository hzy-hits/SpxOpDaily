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
    assert result.passed[0]["edge"]["edge_status"] == (
        "explicit_manual_policy_unvalidated"
    )
    assert result.passed[0]["edge"]["strategy_edge"]["evidence_status"] == (
        "forward_unvalidated_user_override"
    )


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
    assert decision["why_not"]["primary_blocker"] == (
        "strategy_edge_lower_bound_below_threshold"
    )
