from __future__ import annotations

from datetime import datetime, timedelta, timezone

from spx_spark.application.order_map.bias_machine import load_intraday_call_bias


def test_formal_down_signal_maps_to_put_bias_with_upper_invalidation(monkeypatch) -> None:
    now = datetime(2026, 7, 13, 14, 30, tzinfo=timezone.utc)
    monkeypatch.setattr(
        "spx_spark.application.order_map.bias_machine.load_level_decision_shadow",
        lambda _storage: {
            "phase": "confirmed",
            "formal_signal": True,
            "actionable": True,
            "thesis": "breakout",
            "direction": "down",
            "level_kind": "flip_low",
            "level": 7550.0,
            "expiry": "20260713",
            "event_id": "level:one",
            "phase_at": now.isoformat(),
            "expires_at": (now + timedelta(minutes=5)).isoformat(),
        },
    )
    monkeypatch.setattr(
        "spx_spark.application.order_map.bias_machine._breakout_filter_allows",
        lambda _storage, *, event_id: event_id == "level:one",
    )

    bias = load_intraday_call_bias(now=now)

    assert bias is not None
    assert bias["play"] == "level_breakout_put"
    assert bias["invalidation_level"] == 7553.0
    assert bias["actionable"] is True


def test_breakout_filter_blocks_formal_breakout_from_order_map(monkeypatch) -> None:
    now = datetime(2026, 7, 13, 14, 30, tzinfo=timezone.utc)
    monkeypatch.setattr(
        "spx_spark.application.order_map.bias_machine.load_level_decision_shadow",
        lambda _storage: {
            "phase": "confirmed",
            "actionable": True,
            "thesis": "breakout",
            "direction": "up",
            "level": 7550.0,
            "expiry": "20260713",
            "event_id": "level:blocked",
            "expires_at": (now + timedelta(minutes=5)).isoformat(),
        },
    )
    monkeypatch.setattr(
        "spx_spark.application.order_map.bias_machine._breakout_filter_allows",
        lambda _storage, *, event_id: False,
    )

    assert load_intraday_call_bias(now=now) is None


def test_expired_formal_signal_does_not_reach_order_map(monkeypatch) -> None:
    now = datetime(2026, 7, 13, 14, 30, tzinfo=timezone.utc)
    monkeypatch.setattr(
        "spx_spark.application.order_map.bias_machine.load_level_decision_shadow",
        lambda _storage: {
            "phase": "confirmed",
            "actionable": True,
            "thesis": "fade",
            "direction": "up",
            "level": 7550.0,
            "expiry": "20260713",
            "expires_at": (now - timedelta(seconds=1)).isoformat(),
        },
    )

    assert load_intraday_call_bias(now=now) is None
