"""Typed policy for the ES Globex trend state machine."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class GlobexTrendSettings:
    enabled: bool = True
    interval_seconds: int = 30
    sample_interval_seconds: int = 60
    short_horizon_minutes: int = 15
    medium_horizon_minutes: int = 60
    long_horizon_minutes: int = 180
    short_move_points: float = 3.5
    medium_move_points: float = 8.0
    long_move_points: float = 15.0
    reversal_points: float = 10.0
    confirmation_observations: int = 2
    max_quote_age_seconds: float = 90.0
    retention_hours: int = 18
    pending_event_ttl_seconds: int = 300
    continuation_step_points: float = 10.0
    continuation_confirmation_observations: int = 2
    continuation_cooldown_seconds: int = 1800
    continuation_max_milestones_per_transition: int = 2
    continuation_session_budget: int = 3

    def __post_init__(self) -> None:
        positive = (
            self.interval_seconds,
            self.sample_interval_seconds,
            self.short_horizon_minutes,
            self.medium_horizon_minutes,
            self.long_horizon_minutes,
            self.short_move_points,
            self.medium_move_points,
            self.long_move_points,
            self.reversal_points,
            self.confirmation_observations,
            self.max_quote_age_seconds,
            self.retention_hours,
            self.pending_event_ttl_seconds,
            self.continuation_step_points,
            self.continuation_confirmation_observations,
            self.continuation_cooldown_seconds,
            self.continuation_max_milestones_per_transition,
            self.continuation_session_budget,
        )
        if any(value <= 0 for value in positive):
            raise ValueError("globex trend settings must be positive")
        if not (
            self.short_horizon_minutes
            < self.medium_horizon_minutes
            < self.long_horizon_minutes
        ):
            raise ValueError("globex trend horizons must be strictly increasing")
