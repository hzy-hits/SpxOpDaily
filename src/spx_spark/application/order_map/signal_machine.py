"""Typed signal lifecycle transitions for order-map watches."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from spx_spark.domain.state_machines import SignalMode


@dataclass(frozen=True)
class SignalTransition:
    mode: SignalMode
    reason: str


_ALLOWED: dict[tuple[SignalMode, str], SignalMode] = {
    (SignalMode.OBSERVING, "arm"): SignalMode.ARMED,
    (SignalMode.ARMED, "trigger"): SignalMode.TRIGGERED,
    (SignalMode.TRIGGERED, "confirm"): SignalMode.CONFIRMED,
    (SignalMode.ARMED, "invalidate"): SignalMode.INVALIDATED,
    (SignalMode.TRIGGERED, "invalidate"): SignalMode.INVALIDATED,
    (SignalMode.OBSERVING, "expire"): SignalMode.EXPIRED,
    (SignalMode.ARMED, "expire"): SignalMode.EXPIRED,
    (SignalMode.TRIGGERED, "expire"): SignalMode.EXPIRED,
}


TERMINAL = frozenset({SignalMode.CONFIRMED, SignalMode.INVALIDATED, SignalMode.EXPIRED})


def advance_signal_mode(mode: SignalMode, event: str) -> SignalTransition:
    """Pure transition helper. Illegal moves raise ValueError."""
    if mode in TERMINAL:
        raise ValueError(f"terminal signal mode {mode.value} rejects event {event!r}")
    key = (mode, event)
    if key not in _ALLOWED:
        raise ValueError(f"illegal signal transition {mode.value} + {event!r}")
    return SignalTransition(mode=_ALLOWED[key], reason=event)


def signal_lifecycle_for_call_bias(
    bias: Mapping[str, object] | None,
) -> SignalTransition:
    """Map intraday conditional_call_bias status onto the typed SignalMode path.

    Drive transitions exclusively through ``advance_signal_mode`` so play
    payloads carry a legal lifecycle without changing watch mutation logic.
    """

    if not bias:
        return SignalTransition(mode=SignalMode.OBSERVING, reason="neutral")
    status = str(bias.get("status") or "neutral")
    if status in {"", "neutral"}:
        return SignalTransition(mode=SignalMode.OBSERVING, reason="neutral")
    mode = SignalMode.OBSERVING
    reason = status
    try:
        if status == "watch":
            mode = advance_signal_mode(mode, "arm").mode
            if bias.get("armed_at"):
                mode = advance_signal_mode(mode, "trigger").mode
                reason = "watch_armed"
            else:
                reason = "watch"
        elif status == "confirmed":
            mode = advance_signal_mode(mode, "arm").mode
            mode = advance_signal_mode(mode, "trigger").mode
            mode = advance_signal_mode(mode, "confirm").mode
            reason = "confirmed"
        else:
            return SignalTransition(mode=SignalMode.OBSERVING, reason=f"unknown:{status}")
    except ValueError:
        return SignalTransition(mode=SignalMode.OBSERVING, reason="illegal_fallback")
    return SignalTransition(mode=mode, reason=reason)


def annotate_call_bias_with_signal_mode(
    bias: dict[str, object] | None,
) -> dict[str, object]:
    """Return a bias dict that always includes signal_mode / signal_reason."""

    transition = signal_lifecycle_for_call_bias(bias)
    payload = dict(bias or {})
    payload["signal_mode"] = transition.mode.value
    payload["signal_reason"] = transition.reason
    return payload
