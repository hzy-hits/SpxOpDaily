"""Signal lifecycle machine tests."""

from __future__ import annotations

import pytest

from spx_spark.application.order_map.models import SignalMode
from spx_spark.application.order_map.signal_machine import (
    advance_signal_mode,
    signal_lifecycle_for_call_bias,
)
from spx_spark.domain.state_machines import SignalMode as DomainSignalMode


def test_signal_happy_path() -> None:
    armed = advance_signal_mode(SignalMode.OBSERVING, "arm")
    triggered = advance_signal_mode(armed.mode, "trigger")
    confirmed = advance_signal_mode(triggered.mode, "confirm")
    assert confirmed.mode is SignalMode.CONFIRMED
    assert SignalMode is DomainSignalMode


@pytest.mark.parametrize(
    ("mode", "event"),
    [
        (SignalMode.OBSERVING, "confirm"),
        (SignalMode.CONFIRMED, "arm"),
        (SignalMode.INVALIDATED, "trigger"),
    ],
)
def test_illegal_signal_transitions_rejected(mode: SignalMode, event: str) -> None:
    with pytest.raises(ValueError, match="illegal|terminal"):
        advance_signal_mode(mode, event)


@pytest.mark.parametrize(
    ("bias", "expected_mode", "expected_reason"),
    [
        (None, SignalMode.OBSERVING, "neutral"),
        ({"status": "neutral"}, SignalMode.OBSERVING, "neutral"),
        ({"status": "watch"}, SignalMode.ARMED, "watch"),
        ({"status": "watch", "armed_at": "2026-07-11T15:00:00+00:00"}, SignalMode.TRIGGERED, "watch_armed"),
        ({"status": "confirmed", "play": "flip_reclaim_call"}, SignalMode.CONFIRMED, "confirmed"),
    ],
)
def test_call_bias_drives_signal_lifecycle(
    bias: dict[str, object] | None,
    expected_mode: SignalMode,
    expected_reason: str,
) -> None:
    transition = signal_lifecycle_for_call_bias(bias)
    assert transition.mode is expected_mode
    assert transition.reason == expected_reason
