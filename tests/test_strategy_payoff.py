from copy import deepcopy
from datetime import datetime, timedelta, timezone

import pytest
from hypothesis import given
from hypothesis import strategies as st

from spx_spark.analytics.options.strategy_payoff import (
    conservative_vertical_bbo,
    vertical_economics,
    vertical_payoff,
)
from spx_spark.application.order_map.strategy_select import build_strategy_decision
from spx_spark.data_platform.research.strategy_decision_replay import (
    build_vertical_replay_report,
    classify_gth_vertical_record,
)
from spx_spark.storage import LatestState


@given(
    width=st.floats(min_value=1.0, max_value=100.0, allow_nan=False, allow_infinity=False),
    debit_fraction=st.floats(
        min_value=0.01, max_value=0.99, allow_nan=False, allow_infinity=False
    ),
    long_strike=st.floats(
        min_value=100.0, max_value=10_000.0, allow_nan=False, allow_infinity=False
    ),
)
def test_vertical_risk_is_bounded_and_sums_to_width(
    width: float, debit_fraction: float, long_strike: float
) -> None:
    debit = width * debit_fraction
    economics = vertical_economics(
        long_strike=long_strike,
        short_strike=long_strike + width,
        net_debit=debit,
        right="C",
    )

    assert economics["max_loss_points"] + economics["max_gain_points"] == pytest.approx(width)
    assert vertical_payoff(
        economics["breakeven_spx"],
        long_strike=long_strike,
        short_strike=long_strike + width,
        net_debit=debit,
        right="C",
    ) == pytest.approx(0.0, abs=1e-8)
    assert vertical_payoff(
        long_strike - width,
        long_strike=long_strike,
        short_strike=long_strike + width,
        net_debit=debit,
        right="C",
    ) == pytest.approx(-debit)

    put = vertical_economics(
        long_strike=long_strike + width,
        short_strike=long_strike,
        net_debit=debit,
        right="P",
    )
    assert put["max_loss_points"] + put["max_gain_points"] == pytest.approx(width)
    assert vertical_payoff(
        put["breakeven_spx"],
        long_strike=long_strike + width,
        short_strike=long_strike,
        net_debit=debit,
        right="P",
    ) == pytest.approx(0.0, abs=1e-8)


def test_conservative_vertical_bbo_uses_ask_minus_bid_and_rejects_stale_quotes() -> None:
    now = datetime(2026, 8, 7, 14, 0, tzinfo=timezone.utc)
    long_leg = {
        "bid": 10.0,
        "ask": 10.2,
        "provider": "schwab",
        "source_at": (now - timedelta(seconds=2)).isoformat(),
    }
    short_leg = {
        "bid": 4.0,
        "ask": 4.2,
        "provider": "schwab",
        "source_at": (now - timedelta(seconds=1)).isoformat(),
    }

    bbo = conservative_vertical_bbo(long_leg, short_leg, now=now)
    assert bbo["ask"] == pytest.approx(6.2)
    assert bbo["bid"] == pytest.approx(5.8)

    long_leg["source_at"] = (now - timedelta(seconds=16)).isoformat()
    assert conservative_vertical_bbo(long_leg, short_leg, now=now) == {
        "status": "unavailable",
        "reasons": ["spread_leg_quote_stale", "spread_leg_time_skew_exceeded"],
    }


def test_conservative_vertical_bbo_accepts_zero_bid_without_using_mid() -> None:
    now = datetime(2026, 8, 7, 14, 0, tzinfo=timezone.utc)
    long_leg = {
        "bid": 0.0,
        "ask": 0.2,
        "provider": "schwab",
        "source_at": now.isoformat(),
    }
    short_leg = {
        "bid": 0.0,
        "ask": 0.1,
        "provider": "schwab",
        "source_at": now.isoformat(),
    }

    assert conservative_vertical_bbo(long_leg, short_leg, now=now)["ask"] == 0.2


def test_rth_vertical_is_manual_candidate_but_late_chase_is_no_trade() -> None:
    now = datetime(2026, 8, 7, 15, 0, tzinfo=timezone.utc)
    payload = _decision_payload(now)

    decision = build_strategy_decision(payload, _state(now), now)

    assert decision["decision_type"] == "CALL_DEBIT_VERTICAL"
    assert decision["candidate"]["setup_kind"] == "TREND_PULLBACK"
    assert decision["execution"]["action"] == "MANUAL_LIMIT"
    assert decision["execution"]["limit"] == pytest.approx(3.0)
    assert decision["automatic_ordering"] is False
    assert datetime.fromisoformat(decision["candidate"]["opportunity_valid_until"]) == (
        now + timedelta(minutes=5)
    )

    late = deepcopy(payload)
    late["minute_market_frame"]["es"]["vwap_distance_points"] = 12.0
    late["minute_market_frame"]["es"]["return_15m_points"] = 11.0
    rejected = build_strategy_decision(late, _state(now), now)

    assert rejected["decision_type"] == "NO_TRADE"
    assert rejected["regime"]["entry_state"] == "LATE_CHASE"
    assert "direction_valid_but_entry_too_late" in rejected["why_not"]["reasons"]


def test_gth_level_path_can_authorize_manual_candidate_but_trend_background_cannot() -> None:
    now = datetime(2026, 8, 7, 3, 0, tzinfo=timezone.utc)
    payload = _decision_payload(now)
    payload["gth_level_manual_candidate"] = _gth_candidate(now, "upper_acceptance_call")
    payload.pop("call_skew_spread_shadow")

    decision = build_strategy_decision(payload, _state(now), now)

    assert decision["decision_type"] == "CALL_DEBIT_VERTICAL"
    assert decision["candidate"]["source"] == "gth_level_manual_candidate"
    assert decision["action_authority"] == "manual"

    payload["gth_level_manual_candidate"] = _gth_candidate(now, "trend_transition_call")
    rejected = build_strategy_decision(payload, _state(now), now)

    assert rejected["decision_type"] == "NO_TRADE"
    assert "trend_background_cannot_authorize_entry" in rejected["why_not"]["reasons"]


def test_vertical_replay_is_causal_and_compares_four_slippage_levels() -> None:
    at = "2026-08-06T10:52:25+00:00"
    decisions = [
        {
            "opportunity_id": "late",
            "session_date": "2026-08-05",
            "decision_at": at,
            "available_at": "2026-08-06T10:52:24+00:00",
            "new_action": "NO_TRADE",
            "new_reason": "direction_valid_but_entry_too_late",
            "manual_candidate_complete": True,
            "automatic_ordering": False,
        },
        {
            "opportunity_id": "good",
            "session_date": "2026-08-06",
            "decision_at": at,
            "available_at": at,
            "new_action": "TRADE",
            "new_reason": "vertical_entry_quality_passed",
            "manual_candidate_complete": True,
            "automatic_ordering": False,
        },
    ]
    opportunities = [
        _opportunity("late", (-80.0, -100.0, -120.0, -160.0)),
        _opportunity("good", (80.0, 60.0, 40.0, 0.0)),
    ]

    report = build_vertical_replay_report(
        opportunities,
        decisions,
        frozen_cases={"2026-08-05": True, "2026-08-06": True},
        minimum_sessions=2,
    )

    assert report["slippage_grid"] == [0.0, 0.05, 0.1, 0.2]
    assert len(report["legacy_vs_new"]) == 4
    assert report["late_chase_legacy_loss_usd"] == -100.0
    assert report["bootstrap_gate"]["status"] == "pass"


def test_historical_gth_record_uses_same_stop_atr_and_trend_gates() -> None:
    now = datetime(2026, 8, 6, 10, 52, 25, tzinfo=timezone.utc)
    record = {
        **_gth_candidate(now, "upper_acceptance_call"),
        "candidate_id": "gth:7730-call",
        "session_date": "2026-08-06",
        "evaluated_at": now.isoformat(),
        "decision_ask": 15.6,
        "spread_width_points": 40.0,
        "current_parity_spx": 7734.3,
        "trigger_level": 7730.0,
        "target_spx": 7770.0,
        "invalidation_spx": 7722.0,
        "automatic_ordering": False,
    }

    rejected = classify_gth_vertical_record(record, atr_5m=4.27)
    assert rejected["new_action"] == "NO_TRADE"
    assert rejected["new_reason"] == "stop_distance_outside_atr_band"

    record["path_kind"] = "trend_transition_call"
    trend_only = classify_gth_vertical_record(record, atr_5m=20.0)
    assert trend_only["new_action"] == "NO_TRADE"
    assert trend_only["new_reason"] == "trend_background_cannot_authorize_entry"


def _state(now: datetime) -> LatestState:
    observed = now - timedelta(seconds=1)
    return LatestState(created_at=observed, as_of=observed, quotes=(), best_quotes=())


def _decision_payload(now: datetime) -> dict[str, object]:
    observed = now - timedelta(seconds=1)
    long_leg = {
        "contract_id": "option:SPX:SPXW:20260807:7710:C",
        "strike": 7710.0,
        "right": "C",
        "provider": "schwab",
        "bid": 3.8,
        "ask": 4.0,
        "source_at": observed.isoformat(),
    }
    short_leg = {
        "contract_id": "option:SPX:SPXW:20260807:7720:C",
        "strike": 7720.0,
        "right": "C",
        "provider": "schwab",
        "bid": 1.0,
        "ask": 1.2,
        "source_at": observed.isoformat(),
    }
    return {
        "trading_date": "2026-08-07",
        "pricing_allowed": True,
        "underlier": {"price": 7710.0, "source": "index:SPX"},
        "minute_market_frame": {
            "as_of": observed.isoformat(),
            "quality": "ready",
            "es": {
                "price": 7735.0,
                "vwap": 7733.0,
                "vwap_distance_points": 2.0,
                "return_15m_points": 3.0,
                "return_60m_points": 8.0,
                "vwap_slope_15m_points": 0.5,
            },
            "diagnostics": {
                "rth_market_state": {
                    "D": 7.0,
                    "input_lineage": {
                        "values": {
                            "efficiency_ratio": 0.60,
                            "vwap_cross_count": 0,
                            "price_vs_vwap": "above",
                            "breadth_above_vwap": 0.70,
                        },
                        "diagnostics": {"moving_averages": {"atr_5m": 10.0}},
                    },
                }
            },
        },
        "option_structure_frame": {
            "as_of": observed.isoformat(),
            "quality": "ready",
            "l1": {"quality": "ready"},
            "structure": {
                "put_wall": 7680.0,
                "zero_gamma": 7695.0,
                "flip_zone": [7700.0, 7705.0],
                "call_wall": 7730.0,
            },
        },
        "macro_event": {"mode": "normal", "entry_allowed": True},
        "level_decision": {
            "phase": "confirmed",
            "thesis": "breakout",
            "direction": "up",
            "level_kind": "flip_high",
            "level": 7705.0,
            "event_id": "level:7705:up",
        },
        "call_skew_spread_shadow": {
            "status": "candidate",
            "candidate": {"long": long_leg, "short": short_leg},
        },
        "candidates": [],
    }


def _gth_candidate(now: datetime, path_kind: str) -> dict[str, object]:
    observed = now - timedelta(seconds=1)
    long_id = "option:SPX:SPXW:20260807:7710:C"
    short_id = "option:SPX:SPXW:20260807:7720:C"
    return {
        "status": "manual_ready",
        "direction": "up",
        "path_kind": path_kind,
        "manual_action_eligible": True,
        "execution_eligible": False,
        "trigger_level": 7705.0,
        "current_parity_spx": 7710.0,
        "target_spx": 7730.0,
        "invalidation_spx": 7705.0,
        "valid_until": (now + timedelta(minutes=5)).isoformat(),
        "long_contract_id": long_id,
        "short_contract_id": short_id,
        "block_reasons": [],
        "exact_spread_snapshot": {
            "long": {
                "provider": "ibkr",
                "bid": 3.8,
                "ask": 4.0,
                "source_at": observed.isoformat(),
            },
            "short": {
                "provider": "ibkr",
                "bid": 1.0,
                "ask": 1.2,
                "source_at": observed.isoformat(),
            },
        },
    }


def _opportunity(opportunity_id: str, pnl: tuple[float, float, float, float]) -> dict:
    return {
        "opportunity_id": opportunity_id,
        "latency_sensitivity": [
            {
                "latency_seconds": 0,
                "cost": {
                    "slippage_sensitivity": [
                        {
                            "per_leg_side_slippage_points": slippage,
                            "net_pnl_usd": net_pnl,
                        }
                        for slippage, net_pnl in zip((0.0, 0.05, 0.1, 0.2), pnl)
                    ]
                },
            }
        ],
    }
