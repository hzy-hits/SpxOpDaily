"""Bounded-context state machine enums (stdlib-only)."""

from __future__ import annotations

from enum import Enum


class ReplanMode(str, Enum):
    STEADY = "steady"
    CANDIDATE_PENDING = "candidate_pending"
    APPLYING = "applying"
    COOLDOWN = "cooldown"
    FAILURE_BACKOFF = "failure_backoff"


class DeliveryMode(str, Enum):
    PENDING = "pending"
    REVIEWING = "reviewing"
    APPROVED = "approved"
    REJECTED = "rejected"
    DELIVERING = "delivering"
    DELIVERED = "delivered"
    RETRY_WAIT = "retry_wait"
    DEAD_LETTER = "dead_letter"


class SignalMode(str, Enum):
    OBSERVING = "observing"
    ARMED = "armed"
    TRIGGERED = "triggered"
    CONFIRMED = "confirmed"
    INVALIDATED = "invalidated"
    EXPIRED = "expired"
