from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

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
