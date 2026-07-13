"""Schwab settings slice."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class SchwabCapacitySettings:
    nominal_requests_per_minute: int
    planned_requests_per_minute: int
    max_symbols_per_quote_request: int
    operational_quote_batch_size: int = 80

    def __post_init__(self) -> None:
        if self.nominal_requests_per_minute <= 0:
            raise ValueError("Schwab nominal request capacity must be positive")
        if not 0 < self.planned_requests_per_minute < self.nominal_requests_per_minute:
            raise ValueError("Schwab planned capacity must preserve a request reserve")
        if not 1 <= self.max_symbols_per_quote_request <= 500:
            raise ValueError("Schwab quote batch capacity must be between 1 and 500")
        if not 1 <= self.operational_quote_batch_size <= self.max_symbols_per_quote_request:
            raise ValueError("Schwab operational quote batch size exceeds symbol capacity")


@dataclass(frozen=True)
class SchwabCadenceSettings:
    off_hours_quote_seconds: float
    off_hours_front_chain_seconds: float
    off_hours_next_chain_seconds: float
    off_hours_confirmation_chain_seconds: float
    gth_quote_seconds: float
    gth_front_chain_seconds: float
    gth_next_chain_seconds: float
    gth_confirmation_chain_seconds: float
    normal_quote_seconds: float
    normal_front_chain_seconds: float
    active_quote_seconds: float
    active_front_chain_seconds: float
    burst_quote_seconds: float
    burst_front_chain_seconds: float
    next_chain_seconds: float
    spy_xsp_chain_seconds: float
    qqq_iwm_chain_seconds: float

    def __post_init__(self) -> None:
        if any(value <= 0 for value in self.__dict__.values()):
            raise ValueError("Schwab cadences must be positive")


@dataclass(frozen=True)
class SchwabWideChainSettings:
    strike_count_candidates: tuple[int, ...]
    next_expiry_strike_count: int
    min_usable_strikes: int
    min_two_sided_ratio: float
    expected_move_multiple: float
    min_width_points: float
    max_gap_multiple: float

    def __post_init__(self) -> None:
        if tuple(sorted(set(self.strike_count_candidates))) != self.strike_count_candidates:
            raise ValueError("Schwab strike-count candidates must be unique and ascending")
        if (
            not self.strike_count_candidates
            or self.next_expiry_strike_count <= 0
            or self.min_usable_strikes <= 0
        ):
            raise ValueError("Schwab wide-chain counts must be positive")
        if not 0 < self.min_two_sided_ratio <= 1:
            raise ValueError("Schwab two-sided ratio must be in (0, 1]")
        if any(
            value <= 0
            for value in (
                self.expected_move_multiple,
                self.min_width_points,
                self.max_gap_multiple,
            )
        ):
            raise ValueError("Schwab wide-chain thresholds must be positive")


@dataclass(frozen=True)
class SchwabHotLaneSettings:
    minimum_dynamic_symbol_reserve: int
    max_plan_age_seconds: float
    recenter_drift_points: float

    def __post_init__(self) -> None:
        if not 0 <= self.minimum_dynamic_symbol_reserve < 500:
            raise ValueError("Schwab dynamic symbol reserve is invalid")
        if self.max_plan_age_seconds <= 0 or self.recenter_drift_points <= 0:
            raise ValueError("Schwab hot-lane thresholds must be positive")


@dataclass(frozen=True)
class SchwabSettingsSlice:
    streaming_mode: str
    request_budget_warning_per_minute: int
    collection_enabled: bool = True
    service_loop_enabled: bool = False
    collection_interval_seconds: int = 1
    capacity: SchwabCapacitySettings = field(
        default_factory=lambda: SchwabCapacitySettings(120, 84, 500, 80)
    )
    cadence: SchwabCadenceSettings = field(
        default_factory=lambda: SchwabCadenceSettings(
            15, 60, 300, 300, 15, 15, 60, 300, 2, 3, 1.5, 2.5, 1.5, 2, 30, 15, 30
        )
    )
    wide_chain: SchwabWideChainSettings = field(
        default_factory=lambda: SchwabWideChainSettings(
            (80, 100, 120), 40, 40, 0.8, 2.5, 150, 2.0
        )
    )
    hot_lane: SchwabHotLaneSettings = field(
        default_factory=lambda: SchwabHotLaneSettings(10, 30, 10)
    )
