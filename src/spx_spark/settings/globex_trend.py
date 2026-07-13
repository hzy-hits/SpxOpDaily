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
        )
        if any(value <= 0 for value in positive):
            raise ValueError("globex trend settings must be positive")
        if not (
            self.short_horizon_minutes
            < self.medium_horizon_minutes
            < self.long_horizon_minutes
        ):
            raise ValueError("globex trend horizons must be strictly increasing")
