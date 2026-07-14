"""Typed policy for the wall/flip decision machine and its acceptance audit."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LevelDecisionPolicy:
    enabled: bool = True
    notify_transitions: bool = True
    formal_signal_enabled: bool = False
    approach_points: float = 12.0
    test_points: float = 4.0
    break_buffer_points: float = 3.0
    reject_points: float = 6.0
    accept_hold_seconds: float = 20.0
    retest_points: float = 4.0
    confirm_move_points: float = 4.0
    confirm_hold_seconds: float = 10.0
    phase_timeout_seconds: float = 90.0
    event_ttl_seconds: float = 300.0
    data_grace_seconds: float = 30.0
    structure_drift_points: float = 5.0
    es_confirm_ratio: float = 0.25
    terminal_rearm_seconds: float = 30.0
    structure_interval_seconds: int = 900
    structure_required_confirmations: int = 3
    structure_band_half_width_points: float = 5.0
    structure_switch_min_points: float = 10.0
    max_frozen_structure_age_sessions: int = 1
    outcome_horizons_seconds: tuple[int, ...] = (30, 60, 180, 300)
    outcome_sample_tolerance_seconds: float = 20.0
    outcome_no_follow_through_mfe_bps: float = 2.0
    outcome_false_confirmation_mae_bps: float = -5.0
    outcome_follow_through_end_bps: float = 3.0
    outcome_retention_seconds: float = 3600.0
    acceptance_min_events: int = 100
    acceptance_min_sessions: int = 20
    acceptance_min_complete_rth_sessions: int = 5
    acceptance_min_rth_sample_ratio: float = 0.95
    acceptance_max_rth_gap_seconds: float = 45.0
    acceptance_expected_sample_seconds: float = 15.0

    def __post_init__(self) -> None:
        positive = (
            self.approach_points,
            self.test_points,
            self.break_buffer_points,
            self.reject_points,
            self.accept_hold_seconds,
            self.retest_points,
            self.confirm_move_points,
            self.confirm_hold_seconds,
            self.phase_timeout_seconds,
            self.event_ttl_seconds,
            self.data_grace_seconds,
            self.structure_drift_points,
            self.es_confirm_ratio,
            self.terminal_rearm_seconds,
            self.structure_interval_seconds,
            self.structure_required_confirmations,
            self.structure_band_half_width_points,
            self.structure_switch_min_points,
            self.outcome_sample_tolerance_seconds,
            self.outcome_no_follow_through_mfe_bps,
            self.outcome_follow_through_end_bps,
            self.outcome_retention_seconds,
            self.acceptance_max_rth_gap_seconds,
            self.acceptance_expected_sample_seconds,
        )
        if any(value <= 0 for value in positive):
            raise ValueError("level-decision policy thresholds must be positive")
        if self.outcome_false_confirmation_mae_bps >= 0:
            raise ValueError("false-confirmation MAE threshold must be negative")
        if self.max_frozen_structure_age_sessions < 0:
            raise ValueError("frozen-structure session TTL cannot be negative")
        if (
            not self.outcome_horizons_seconds
            or tuple(sorted(set(self.outcome_horizons_seconds)))
            != self.outcome_horizons_seconds
            or any(value <= 0 for value in self.outcome_horizons_seconds)
        ):
            raise ValueError("outcome horizons must be unique, positive, and ascending")
        if min(
            self.acceptance_min_events,
            self.acceptance_min_sessions,
            self.acceptance_min_complete_rth_sessions,
        ) <= 0:
            raise ValueError("level-decision acceptance minimums must be positive")
        if not 0 < self.acceptance_min_rth_sample_ratio <= 1:
            raise ValueError("RTH acceptance sample ratio must be in (0, 1]")
