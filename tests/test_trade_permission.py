"""Trade permission governor above Butterfly / Debit Vertical selection."""

from __future__ import annotations

from datetime import datetime, timezone

from spx_spark.application.order_map.strategy_regime import (
    BUTTERFLY_TYPES,
    DEBIT_VERTICAL_TYPES,
    DEFAULT_STRATEGY_POLICY,
    StrategyPolicy,
    assess_trade_permission,
)
from spx_spark.application.order_map.strategy_ranker import _hard_gate_candidate


def _facts(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "decision_at": "2026-08-10T17:00:00+00:00",  # 13:00 ET
        "available_at": "2026-08-10T17:00:00+00:00",
        "quality": {"status": "ready"},
        "session": {"mode": "rth"},
        "capabilities": {
            "global": {
                "session_legal": True,
                "coordinate_ready": True,
                "market_frame_ready": True,
            }
        },
        "shock": {"state": "NONE"},
        "volatility": {"vix": 16.0, "vix1d": 15.0, "vix_return_15m_pct": 0.0},
    }
    base.update(overrides)
    return base


def _regime(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "path_state": "BALANCED",
        "path_direction": None,
        "terminal_state": "PIN_STABLE",
        "event_state": "NORMAL",
        "contradictions": [],
    }
    base.update(overrides)
    return base


def test_policy_bootstrap_v5_risk_caps() -> None:
    policy = DEFAULT_STRATEGY_POLICY
    assert policy.policy_version == "strategy_policy.bootstrap.v5"
    assert policy.butterfly_max_risk_usd == 250.0
    assert policy.vertical_max_risk_usd == 250.0
    assert policy.entry_cutoff_et == "14:15"
    assert policy.max_entries_per_day == 2
    assert policy.max_open_positions == 1


def test_shock_and_event_force_no_trade() -> None:
    permission = assess_trade_permission(
        _facts(shock={"state": "ACTIVE"}),
        _regime(),
    )
    assert permission["state"] == "NO_TRADE"
    assert "shock_active" in permission["hard_reasons"]

    permission = assess_trade_permission(
        _facts(),
        _regime(event_state="POST_EVENT_DISCOVERY"),
    )
    assert permission["state"] == "NO_TRADE"
    assert "event_post_event_discovery" in permission["hard_reasons"]


def test_entry_cutoff_and_transition_force_no_trade() -> None:
    permission = assess_trade_permission(
        _facts(decision_at="2026-08-10T18:20:00+00:00"),  # 14:20 ET
        _regime(path_state="TREND", path_direction="UP", terminal_state="NONE"),
    )
    assert permission["state"] == "NO_TRADE"
    assert "entry_cutoff_reached" in permission["hard_reasons"]

    permission = assess_trade_permission(
        _facts(decision_at="2026-08-10T19:40:00+00:00"),  # 15:40 ET
        _regime(),
    )
    assert permission["state"] == "NO_TRADE"
    assert "pin_entry_cutoff_reached" in permission["hard_reasons"]

    permission = assess_trade_permission(
        _facts(),
        _regime(path_state="TRANSITION", terminal_state="NONE"),
    )
    assert permission["state"] == "NO_TRADE"
    assert "path_state_no_trade" in permission["hard_reasons"]


def test_gth_session_skips_rth_entry_cutoff() -> None:
    permission = assess_trade_permission(
        _facts(
            decision_at="2026-08-10T18:20:00+00:00",  # 14:20 ET
            session={"mode": "gth"},
        ),
        _regime(path_state="TREND", path_direction="UP", terminal_state="NONE"),
    )
    assert permission["state"] == "ALLOW_DIRECTIONAL"
    assert "entry_cutoff_reached" not in permission["hard_reasons"]


def test_allow_pin_and_directional_type_sets() -> None:
    pin = assess_trade_permission(_facts(), _regime())
    assert pin["state"] == "ALLOW_PIN"
    assert tuple(pin["allowed_strategy_types"]) == BUTTERFLY_TYPES

    directional = assess_trade_permission(
        _facts(),
        _regime(path_state="TREND", path_direction="UP", terminal_state="NONE"),
    )
    assert directional["state"] == "ALLOW_DIRECTIONAL"
    assert tuple(directional["allowed_strategy_types"]) == DEBIT_VERTICAL_TYPES


def test_high_vix_is_defense_no_trade_not_farther_otm_selling() -> None:
    permission = assess_trade_permission(
        _facts(volatility={"vix": 32.0, "vix1d": 28.0}),
        _regime(),
    )
    assert permission["state"] == "NO_TRADE"
    assert "high_vix_defense_no_trade" in permission["hard_reasons"]


def test_absolute_risk_gates_block_above_250() -> None:
    now = datetime(2026, 8, 10, 17, 0, tzinfo=timezone.utc)
    policy = StrategyPolicy()
    vertical = {
        "strategy_type": "CALL_DEBIT_VERTICAL",
        "direction": "UP",
        "setup_kind": "TREND_PULLBACK",
        "trigger_level": 5000.0,
        "target_spx": 5020.0,
        "invalidation_spx": 4985.0,
        "quote": {"status": "ready"},
        "quote_valid_until": "2026-08-10T17:05:00+00:00",
        "opportunity_valid_until": "2026-08-10T17:05:00+00:00",
        "automatic_ordering": False,
        "manual_action_only": True,
        "long": {"strike": 5000},
        "short": {"strike": 5020},
        "economics": {
            "debit_fraction_of_width": 0.3,
            "max_loss_points": 3.0,  # $300
        },
    }
    facts = {
        "spot": {"spx": 5005.0},
        "path": {
            "atr_5m": 4.0,
            "distance_to_vwap_points": 1.0,
            "impulse_15m_points": 2.0,
        },
    }
    gates = _hard_gate_candidate(
        vertical,
        facts,
        {
            "trade_permission": {
                "allowed_strategy_types": list(DEBIT_VERTICAL_TYPES),
            }
        },
        now=now,
        policy=policy,
    )
    assert any(gate["gate"] == "vertical_risk_budget" for gate in gates)

    butterfly = {
        "strategy_type": "CALL_BUTTERFLY",
        "center": 5000.0,
        "width": 10.0,
        "quote": {"status": "ready"},
        "quote_valid_until": "2026-08-10T17:05:00+00:00",
        "opportunity_valid_until": "2026-08-10T17:05:00+00:00",
        "automatic_ordering": False,
        "manual_action_only": True,
        "legs": [{"strike": 4990}, {"strike": 5000}, {"strike": 5010}],
        "economics": {
            "max_loss_points": 3.5,
            "max_gain_points": 6.5,
            "width_points": 10.0,
        },
        "pin": {"depin_risk": 0.1, "recent_extreme_acceptance": False},
    }
    butterfly_facts = {
        "spot": {"spx": 5000.0},
        "path": {"breadth_above_vwap": 0.5},
        "value_center": {"spx_30m": 5000.0},
        "structure": {"q_mode": 5000.0},
        "volatility": {"vix_return_15m_pct": 0.0},
        "shock": {"state": "NONE"},
    }
    butterfly_gates = _hard_gate_candidate(
        butterfly,
        butterfly_facts,
        {
            "terminal_state": "PIN_STABLE",
            "pin": {"depin_risk": 0.1, "recent_extreme_acceptance": False},
            "trade_permission": {"allowed_strategy_types": list(BUTTERFLY_TYPES)},
        },
        now=now,
        policy=policy,
    )
    assert any(gate["gate"] == "butterfly_risk_budget" for gate in butterfly_gates)
