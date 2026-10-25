from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from spx_spark.application.order_map import delivery_templates  # noqa: F401
from spx_spark.application.order_map.strategy_regime import (
    DEFAULT_STRATEGY_POLICY,
    pin_stable_watch_phase,
)
from spx_spark.notifier.templates import render_message


def test_pin_stable_watch_v1_matches_golden_output() -> None:
    now = datetime(2026, 8, 14, 18, 38, tzinfo=timezone.utc)
    decision = {
        "decision_id": "strategy:pin-stable-test",
        "session_date": "2026-08-14",
        "decision_at": now.isoformat(),
        "action_authority": "none",
        "decision_type": "NO_TRADE",
        "candidate": None,
        "regime": {
            "terminal_state": "PIN_STABLE",
            "path_state": "BALANCED",
            "pin": {
                "depin_risk": 0.17,
                "center_confirmation_count": 3,
                "center_confirmation_required": 3,
                "center_confirmation_age_seconds": 660.0,
                "center_confirmation_min_seconds": 600.0,
                "center_confirmation_ready": True,
                "top_centers": [{"center": 7785.0, "score": 0.8}],
            },
        },
        "market_facts": {
            "session_date": "2026-08-14",
            "minutes_to_close": 201.0,
            "session": {"mode": "rth", "legal": True},
        },
    }
    phase = pin_stable_watch_phase(201.0, DEFAULT_STRATEGY_POLICY)
    rendered = render_message(
        "pin_stable_watch",
        channel="feishu",
        context={
            "decision": decision,
            "regime": decision["regime"],
            "center": 7785.0,
            "minutes": 201.0,
            "phase": phase,
        },
    )
    golden = (
        Path(__file__).parent / "golden" / "templates" / "pin_stable_watch.v1.txt"
    ).read_text(encoding="utf-8")
    assert rendered.text == golden.rstrip("\n")


def test_trade_ready_v1_matches_golden_output() -> None:
    now = datetime(2026, 8, 7, 15, 0, tzinfo=timezone.utc)
    decision = {
        "decision_id": "strategy:test",
        "decision_at": now.isoformat(),
        "policy_version": "v69",
        "market_facts": {"session": {"mode": "rth"}},
        "regime": {},
    }
    candidate = {
        "strategy_type": "CALL_DEBIT_VERTICAL",
        "direction": "UP",
        "setup_kind": "TREND_PULLBACK",
        "long": {
            "contract_id": "option:SPX:SPXW:20260807:7710:C",
            "strike": 7710.0,
            "right": "C",
            "bid": 3.8,
            "ask": 4.0,
        },
        "short": {
            "contract_id": "option:SPX:SPXW:20260807:7720:C",
            "strike": 7720.0,
            "right": "C",
            "bid": 1.0,
            "ask": 1.2,
        },
        "quote": {"bid": 2.8, "ask": 3.1, "mid": 2.95},
        "economics": {"max_loss_points": 10.0, "max_profit_points": 20.0},
        "opportunity_valid_until": now.replace(minute=5).isoformat(),
        "target_spx": 7730.0,
        "invalidation_spx": 7700.0,
    }
    rendered = render_message(
        "trade_ready",
        channel="feishu",
        context={
            "decision": decision,
            "candidate": candidate,
            "extra": "",
            "quote_status": "ready",
        },
    )
    golden = (
        Path(__file__).parent / "golden" / "templates" / "trade_ready.v1.txt"
    ).read_text(encoding="utf-8")
    assert rendered.text == golden.rstrip("\n")
    assert rendered.sections


@pytest.mark.parametrize("quote_status", ["ready", "refresh_required"])
@pytest.mark.parametrize("setup,kind,direction,target", [
    ("TREND_PULLBACK", "CALL_DEBIT_VERTICAL", "UP", "SPX 7730"),
    ("TREND_PULLBACK", "PUT_DEBIT_VERTICAL", "DOWN", "SPX 7700"),
    ("CLOSE_CONVERGENCE_60M", "CALL_BUTTERFLY", "NEUTRAL", "SPX 7730"),
    ("EVENT_SETTLEMENT_THRESHOLD", "CALL_DEBIT_VERTICAL", "UP", "盈亏平衡 7713"),
])
def test_trade_ready_bark_summary_retains_target(setup, kind, direction, target, quote_status):
    from copy import deepcopy
    from spx_spark.notifier.format_push import bark_lockscreen_summary

    right = "P" if kind == "PUT_DEBIT_VERTICAL" else "C"
    candidate = {
        "setup_kind": setup, "strategy_type": kind, "direction": direction,
        "target_spx": 7700.0 if direction == "DOWN" else 7730.0,
        "center": 7730.0, "width": 10.0, "right": right,
        "long": {"strike": 7720.0 if right == "P" else 7710.0, "right": right},
        "short": {"strike": 7710.0 if right == "P" else 7720.0, "right": right},
        "quote": {"bid": 2.8, "ask": 3.0},
        "economics": {"max_loss_points": 3.0, "breakeven_spx": 7713.0},
        "view": {"threshold_level": 7700.0, "breakeven_gap_points": 13.0},
    }
    original = deepcopy(candidate)
    rendered = render_message("trade_ready", channel="bark", context={
        "decision": {}, "candidate": candidate, "quote_status": quote_status,
    })
    summary = bark_lockscreen_summary(rendered.text, title=rendered.title, kind="trade_intent")
    assert target in summary
    assert "最大亏损 $300" in summary
    assert "买 " in summary and "卖 " in summary
    assert len(summary) <= 280
    assert candidate == original
