"""Typed advisory contract for strategy-distribution research forecasts.

The contract deliberately separates risk-neutral probability, physical
probability, displayed-quote reach, signed net-PnL estimates, and the shadow
selection.  It cannot authorize an order.  In particular, a displayed quote
reaching a limit is not evidence that an order actually filled.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum


SCHEMA_VERSION = "strategy_distribution_forecast.v1"
VERTICAL_WIDTH_POINTS = 10.0
MAX_STRATEGY_CANDIDATES = 2


class ForecastSession(str, Enum):
    GTH = "gth"
    RTH = "rth"


class ProbabilityMeasure(str, Enum):
    RISK_NEUTRAL = "risk_neutral"
    PHYSICAL = "physical"


class ProbabilityEventKind(str, Enum):
    TERMINAL_ABOVE = "terminal_above"
    TERMINAL_BELOW = "terminal_below"
    TERMINAL_BETWEEN = "terminal_between"
    UPPER_FIRST_TOUCH = "upper_first_touch"
    LOWER_FIRST_TOUCH = "lower_first_touch"


class EstimateStatus(str, Enum):
    AVAILABLE = "available"
    UNAVAILABLE = "unavailable"


class EstimateQuality(str, Enum):
    READY = "ready"
    DEGRADED = "degraded"
    UNAVAILABLE = "unavailable"


class CalibrationStatus(str, Enum):
    UNCALIBRATED = "uncalibrated"
    WALK_FORWARD_CALIBRATED = "walk_forward_calibrated"


class ExecutionSemantics(str, Enum):
    DISPLAYED_QUOTE_REACH_PROXY = "displayed_quote_reach_proxy"


class NetPnlBasis(str, Enum):
    DISPLAYED_QUOTE_REACH_PROXY = "displayed_quote_reach_proxy"


class NetPnlUnit(str, Enum):
    USD_PER_ONE_SPREAD = "usd_per_one_spread"


class CandidateDirection(str, Enum):
    CALL_VERTICAL_10 = "call_vertical_10"
    PUT_VERTICAL_10 = "put_vertical_10"


class ShadowAction(str, Enum):
    NO_TRADE = "no_trade"
    MANUAL_CANDIDATE = "manual_candidate"


class ForecastQuality(str, Enum):
    READY = "ready"
    DEGRADED = "degraded"
    UNAVAILABLE = "unavailable"


class ForecastEvidenceStatus(str, Enum):
    RESEARCH_UNVALIDATED = "research_unvalidated"


class ForecastActionAuthority(str, Enum):
    NONE = "none"


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def _aware(value: datetime, name: str) -> None:
    _require(
        type(value) is datetime and value.tzinfo is not None and value.utcoffset() is not None,
        f"{name} must be timezone-aware",
    )


def _date(value: date, name: str) -> None:
    _require(type(value) is date, f"{name} must be a date")


def _token(value: str, name: str) -> None:
    _require(type(value) is str and bool(value.strip()), f"{name} must be non-empty")


def _optional_token(value: str | None, name: str) -> None:
    if value is not None:
        _token(value, name)


def _finite(value: float, name: str) -> None:
    _require(type(value) is float and math.isfinite(value), f"{name} must be a finite float")


def _positive(value: float, name: str) -> None:
    _finite(value, name)
    _require(value > 0.0, f"{name} must be positive")


def _probability(value: float, name: str) -> None:
    _finite(value, name)
    _require(0.0 <= value <= 1.0, f"{name} must be within [0, 1]")


def _reason_codes(values: tuple[str, ...], name: str) -> None:
    _require(type(values) is tuple, f"{name} must be a tuple")
    _require(
        all(type(value) is str and bool(value.strip()) for value in values),
        f"{name} contain invalid values",
    )
    _require(values == tuple(sorted(set(values))), f"{name} must be unique and sorted")


@dataclass(frozen=True, slots=True)
class ProbabilityEventDefinition:
    event_id: str
    kind: ProbabilityEventKind
    target_at: datetime
    lower_level: float | None = None
    upper_level: float | None = None

    def __post_init__(self) -> None:
        _token(self.event_id, "probability event_id")
        _require(type(self.kind) is ProbabilityEventKind, "probability event kind must be typed")
        _aware(self.target_at, "probability event target_at")
        if self.lower_level is not None:
            _positive(self.lower_level, "probability event lower_level")
        if self.upper_level is not None:
            _positive(self.upper_level, "probability event upper_level")

        if self.kind is ProbabilityEventKind.TERMINAL_ABOVE:
            _require(
                self.lower_level is not None and self.upper_level is None,
                "terminal_above requires only lower_level",
            )
        elif self.kind is ProbabilityEventKind.TERMINAL_BELOW:
            _require(
                self.lower_level is None and self.upper_level is not None,
                "terminal_below requires only upper_level",
            )
        else:
            _require(
                self.lower_level is not None and self.upper_level is not None,
                f"{self.kind.value} requires lower_level and upper_level",
            )
            assert self.lower_level is not None and self.upper_level is not None
            _require(
                self.lower_level < self.upper_level,
                "probability event levels must be strictly ordered",
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "event_id": self.event_id,
            "kind": self.kind.value,
            "target_at": self.target_at.isoformat(),
            "lower_level": self.lower_level,
            "upper_level": self.upper_level,
        }


@dataclass(frozen=True, slots=True)
class ProbabilityEstimate:
    measure: ProbabilityMeasure
    event: ProbabilityEventDefinition | None
    status: EstimateStatus
    quality: EstimateQuality
    probability: float | None
    method_version: str | None
    reason_codes: tuple[str, ...]
    sample_count: int | None = None
    session_count: int | None = None
    interval_low: float | None = None
    interval_high: float | None = None
    trained_through_date: date | None = None
    effective_sample_count: float | None = None
    historical_sessions: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _require(type(self.measure) is ProbabilityMeasure, "probability measure must be typed")
        _require(
            self.event is None or type(self.event) is ProbabilityEventDefinition,
            "probability event must be typed or null",
        )
        _require(type(self.status) is EstimateStatus, "probability status must be typed")
        _require(type(self.quality) is EstimateQuality, "probability quality must be typed")
        _optional_token(self.method_version, "probability method_version")
        _reason_codes(self.reason_codes, "probability reason_codes")
        self._validate_evidence_metadata()

        if self.status is EstimateStatus.UNAVAILABLE:
            _require(
                self.quality is EstimateQuality.UNAVAILABLE,
                "unavailable probability quality must be unavailable",
            )
            _require(self.probability is None, "unavailable probability must be null")
            _require(bool(self.reason_codes), "unavailable probability requires reason_codes")
            return

        _require(
            self.quality in {EstimateQuality.READY, EstimateQuality.DEGRADED},
            "available probability quality cannot be unavailable",
        )
        _require(self.event is not None, "available probability event is missing")
        _require(self.probability is not None, "available probability value is missing")
        assert self.probability is not None
        _probability(self.probability, "probability")
        _token(self.method_version or "", "available probability method_version")
        if self.quality is EstimateQuality.READY:
            _require(
                not self.reason_codes,
                "ready probability cannot contain degradation reasons",
            )
        else:
            _require(bool(self.reason_codes), "degraded probability requires reason_codes")

    def _validate_evidence_metadata(self) -> None:
        for value, name in (
            (self.sample_count, "probability sample_count"),
            (self.session_count, "probability session_count"),
        ):
            _require(
                value is None or (type(value) is int and value >= 0),
                f"{name} must be a non-negative integer or null",
            )
        if self.interval_low is not None:
            _probability(self.interval_low, "probability interval_low")
        if self.interval_high is not None:
            _probability(self.interval_high, "probability interval_high")
        _require(
            (self.interval_low is None) == (self.interval_high is None),
            "probability interval bounds must be present together",
        )
        if self.interval_low is not None and self.interval_high is not None:
            _require(
                self.interval_low <= self.interval_high,
                "probability interval bounds must be ordered",
            )
            if self.probability is not None:
                _require(
                    self.interval_low <= self.probability <= self.interval_high,
                    "probability must lie inside its interval",
                )
        if self.trained_through_date is not None:
            _date(self.trained_through_date, "probability trained_through_date")
        if self.effective_sample_count is not None:
            _finite(self.effective_sample_count, "probability effective_sample_count")
            _require(
                self.effective_sample_count >= 0.0,
                "probability effective_sample_count must be non-negative",
            )
        _reason_codes(self.historical_sessions, "probability historical_sessions")

        if self.measure is ProbabilityMeasure.RISK_NEUTRAL:
            _require(
                self.sample_count is None
                and self.session_count is None
                and self.interval_low is None
                and self.interval_high is None
                and self.trained_through_date is None,
                "risk-neutral probability cannot claim physical sample evidence",
            )
            _require(
                self.effective_sample_count is None and not self.historical_sessions,
                "risk-neutral probability cannot claim physical neighbour evidence",
            )
            return

        _require(
            self.sample_count is not None and self.session_count is not None,
            "physical probability requires sample_count and session_count",
        )
        assert self.sample_count is not None and self.session_count is not None
        _require(
            self.session_count <= self.sample_count,
            "physical probability session_count cannot exceed sample_count",
        )
        if self.status is EstimateStatus.UNAVAILABLE:
            _require(
                self.interval_low is None
                and self.interval_high is None
                and self.trained_through_date is None,
                "unavailable physical probability cannot claim interval or training date",
            )
        else:
            _require(
                self.interval_low is not None and self.interval_high is not None,
                "available physical probability requires interval bounds",
            )
            _require(
                self.sample_count > 0 and self.session_count > 0,
                "available physical probability requires positive sample evidence",
            )
            _require(
                self.trained_through_date is not None,
                "available physical probability requires trained_through_date",
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "measure": self.measure.value,
            "event": self.event.to_dict() if self.event is not None else None,
            "status": self.status.value,
            "quality": self.quality.value,
            "probability": self.probability,
            "method_version": self.method_version,
            "reason_codes": list(self.reason_codes),
            "sample_count": self.sample_count,
            "session_count": self.session_count,
            "interval_low": self.interval_low,
            "interval_high": self.interval_high,
            "trained_through_date": (
                self.trained_through_date.isoformat() if self.trained_through_date else None
            ),
            "n_raw": self.sample_count,
            "n_effective": self.effective_sample_count,
            "historical_sessions": list(self.historical_sessions),
        }


@dataclass(frozen=True, slots=True)
class ExecutionEstimate:
    status: EstimateStatus
    limit_debit_points: float
    wait_horizon_seconds: int
    quote_reach_probability: float | None
    model_version: str | None
    reason_codes: tuple[str, ...]
    execution_semantics: ExecutionSemantics = ExecutionSemantics.DISPLAYED_QUOTE_REACH_PROXY
    actual_fill_probability: float | None = None

    def __post_init__(self) -> None:
        _require(type(self.status) is EstimateStatus, "execution status must be typed")
        _require(
            self.execution_semantics is ExecutionSemantics.DISPLAYED_QUOTE_REACH_PROXY,
            "execution semantics must remain displayed_quote_reach_proxy",
        )
        _positive(self.limit_debit_points, "execution limit_debit_points")
        _require(
            type(self.wait_horizon_seconds) is int and self.wait_horizon_seconds > 0,
            "execution wait_horizon_seconds must be a positive integer",
        )
        _optional_token(self.model_version, "execution model_version")
        _reason_codes(self.reason_codes, "execution reason_codes")
        _require(
            self.actual_fill_probability is None,
            "actual_fill_probability must remain null without order-at-risk evidence",
        )

        if self.status is EstimateStatus.UNAVAILABLE:
            _require(
                self.quote_reach_probability is None,
                "unavailable quote-reach probability must be null",
            )
            _require(bool(self.reason_codes), "unavailable execution requires reason_codes")
            return

        _require(
            self.quote_reach_probability is not None,
            "available quote-reach probability is missing",
        )
        assert self.quote_reach_probability is not None
        _probability(self.quote_reach_probability, "quote_reach_probability")
        _token(self.model_version or "", "available execution model_version")

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status.value,
            "execution_semantics": self.execution_semantics.value,
            "limit_debit_points": self.limit_debit_points,
            "wait_horizon_seconds": self.wait_horizon_seconds,
            "quote_reach_probability": self.quote_reach_probability,
            "actual_fill_probability": self.actual_fill_probability,
            "model_version": self.model_version,
            "reason_codes": list(self.reason_codes),
        }


@dataclass(frozen=True, slots=True)
class NetPnlEstimate:
    status: EstimateStatus
    expected_net_pnl: float | None
    p10_net_pnl: float | None
    p50_net_pnl: float | None
    p90_net_pnl: float | None
    tail_loss_p10: float | None
    model_version: str | None
    reason_codes: tuple[str, ...]
    basis: NetPnlBasis = NetPnlBasis.DISPLAYED_QUOTE_REACH_PROXY
    unit: NetPnlUnit = NetPnlUnit.USD_PER_ONE_SPREAD

    def __post_init__(self) -> None:
        _require(type(self.status) is EstimateStatus, "net-PnL status must be typed")
        _require(
            self.basis is NetPnlBasis.DISPLAYED_QUOTE_REACH_PROXY,
            "net-PnL basis must remain displayed_quote_reach_proxy",
        )
        _require(
            self.unit is NetPnlUnit.USD_PER_ONE_SPREAD,
            "net-PnL unit must be usd_per_one_spread",
        )
        _optional_token(self.model_version, "net-PnL model_version")
        _reason_codes(self.reason_codes, "net-PnL reason_codes")
        values = (
            self.expected_net_pnl,
            self.p10_net_pnl,
            self.p50_net_pnl,
            self.p90_net_pnl,
            self.tail_loss_p10,
        )

        if self.status is EstimateStatus.UNAVAILABLE:
            _require(
                all(value is None for value in values),
                "unavailable net-PnL fields must be null",
            )
            _require(bool(self.reason_codes), "unavailable net-PnL requires reason_codes")
            return

        _require(all(value is not None for value in values), "available net-PnL fields are missing")
        assert self.expected_net_pnl is not None
        assert self.p10_net_pnl is not None
        assert self.p50_net_pnl is not None
        assert self.p90_net_pnl is not None
        assert self.tail_loss_p10 is not None
        _finite(self.expected_net_pnl, "expected_net_pnl")
        _finite(self.p10_net_pnl, "p10_net_pnl")
        _finite(self.p50_net_pnl, "p50_net_pnl")
        _finite(self.p90_net_pnl, "p90_net_pnl")
        _finite(self.tail_loss_p10, "tail_loss_p10")
        _require(
            self.p10_net_pnl <= self.p50_net_pnl <= self.p90_net_pnl,
            "net-PnL quantiles must be nondecreasing",
        )
        _require(self.tail_loss_p10 >= 0.0, "tail_loss_p10 must be non-negative")
        _token(self.model_version or "", "available net-PnL model_version")

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status.value,
            "basis": self.basis.value,
            "unit": self.unit.value,
            "expected_net_pnl": self.expected_net_pnl,
            "p10_net_pnl": self.p10_net_pnl,
            "p50_net_pnl": self.p50_net_pnl,
            "p90_net_pnl": self.p90_net_pnl,
            "tail_loss_p10": self.tail_loss_p10,
            "model_version": self.model_version,
            "reason_codes": list(self.reason_codes),
        }


@dataclass(frozen=True, slots=True)
class CandidateScore:
    status: EstimateStatus
    tail_risk_penalty: float | None
    model_uncertainty_penalty: float | None
    liquidity_risk_penalty: float | None
    total: float | None
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        _require(type(self.status) is EstimateStatus, "candidate score status must be typed")
        _reason_codes(self.reason_codes, "candidate score reason_codes")
        values = (
            self.tail_risk_penalty,
            self.model_uncertainty_penalty,
            self.liquidity_risk_penalty,
            self.total,
        )
        if self.status is EstimateStatus.UNAVAILABLE:
            _require(
                all(value is None for value in values),
                "unavailable candidate score fields must be null",
            )
            _require(bool(self.reason_codes), "unavailable candidate score requires reason_codes")
            return

        _require(
            all(value is not None for value in values),
            "available candidate score fields are missing",
        )
        assert self.tail_risk_penalty is not None
        assert self.model_uncertainty_penalty is not None
        assert self.liquidity_risk_penalty is not None
        assert self.total is not None
        for value, name in (
            (self.tail_risk_penalty, "tail_risk_penalty"),
            (self.model_uncertainty_penalty, "model_uncertainty_penalty"),
            (self.liquidity_risk_penalty, "liquidity_risk_penalty"),
            (self.total, "candidate score total"),
        ):
            _finite(value, name)
        _require(
            self.tail_risk_penalty >= 0.0
            and self.model_uncertainty_penalty >= 0.0
            and self.liquidity_risk_penalty >= 0.0,
            "candidate score penalties must be non-negative",
        )

    def validate_against(self, net_pnl: NetPnlEstimate) -> None:
        _require(
            self.status is EstimateStatus.AVAILABLE,
            "cannot validate an unavailable candidate score",
        )
        _require(
            net_pnl.status is EstimateStatus.AVAILABLE,
            "available candidate score requires available net-PnL",
        )
        assert net_pnl.expected_net_pnl is not None
        assert self.tail_risk_penalty is not None
        assert self.model_uncertainty_penalty is not None
        assert self.liquidity_risk_penalty is not None
        assert self.total is not None
        expected = (
            net_pnl.expected_net_pnl
            - self.tail_risk_penalty
            - self.model_uncertainty_penalty
            - self.liquidity_risk_penalty
        )
        _require(
            math.isclose(self.total, expected, rel_tol=1e-9, abs_tol=1e-6),
            "candidate score total does not reconcile",
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status.value,
            "tail_risk_penalty": self.tail_risk_penalty,
            "model_uncertainty_penalty": self.model_uncertainty_penalty,
            "liquidity_risk_penalty": self.liquidity_risk_penalty,
            "total": self.total,
            "reason_codes": list(self.reason_codes),
        }


@dataclass(frozen=True, slots=True)
class StrategyCandidate:
    candidate_id: str
    probability_event_id: str
    direction: CandidateDirection
    expiry: date
    long_contract_id: str
    short_contract_id: str
    long_strike: float
    short_strike: float
    execution: ExecutionEstimate
    net_pnl: NetPnlEstimate
    score: CandidateScore
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        _token(self.candidate_id, "strategy candidate_id")
        _token(self.probability_event_id, "strategy candidate probability_event_id")
        _require(type(self.direction) is CandidateDirection, "candidate direction must be typed")
        _date(self.expiry, "candidate expiry")
        _token(self.long_contract_id, "candidate long_contract_id")
        _token(self.short_contract_id, "candidate short_contract_id")
        _require(
            self.long_contract_id != self.short_contract_id,
            "candidate exact contract ids must differ",
        )
        _positive(self.long_strike, "candidate long_strike")
        _positive(self.short_strike, "candidate short_strike")
        _require(type(self.execution) is ExecutionEstimate, "candidate execution must be typed")
        _require(type(self.net_pnl) is NetPnlEstimate, "candidate net-PnL must be typed")
        _require(type(self.score) is CandidateScore, "candidate score must be typed")
        _reason_codes(self.reason_codes, "candidate reason_codes")

        if self.direction is CandidateDirection.CALL_VERTICAL_10:
            width = self.short_strike - self.long_strike
        else:
            width = self.long_strike - self.short_strike
        _require(
            math.isclose(width, VERTICAL_WIDTH_POINTS, rel_tol=0.0, abs_tol=1e-9),
            "candidate must be an exact 10-point debit vertical",
        )
        _require(
            self.execution.limit_debit_points < VERTICAL_WIDTH_POINTS,
            "candidate limit debit must be below vertical width",
        )
        if self.net_pnl.status is EstimateStatus.AVAILABLE:
            _require(
                self.execution.status is EstimateStatus.AVAILABLE,
                "available net-PnL requires available execution estimate",
            )
        if self.score.status is EstimateStatus.AVAILABLE:
            self.score.validate_against(self.net_pnl)

    def to_dict(self) -> dict[str, object]:
        return {
            "candidate_id": self.candidate_id,
            "probability_event_id": self.probability_event_id,
            "direction": self.direction.value,
            "expiry": self.expiry.isoformat(),
            "long_contract_id": self.long_contract_id,
            "short_contract_id": self.short_contract_id,
            "long_strike": self.long_strike,
            "short_strike": self.short_strike,
            "execution": self.execution.to_dict(),
            "net_pnl": self.net_pnl.to_dict(),
            "score": self.score.to_dict(),
            "reason_codes": list(self.reason_codes),
        }


@dataclass(frozen=True, slots=True)
class ShadowDecision:
    action: ShadowAction
    selected_candidate_id: str | None
    score_threshold: float
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        _require(type(self.action) is ShadowAction, "shadow action must be typed")
        _optional_token(self.selected_candidate_id, "selected_candidate_id")
        _finite(self.score_threshold, "shadow score_threshold")
        _require(self.score_threshold >= 0.0, "shadow score_threshold must be non-negative")
        _reason_codes(self.reason_codes, "shadow decision reason_codes")
        if self.action is ShadowAction.NO_TRADE:
            _require(
                self.selected_candidate_id is None,
                "NO_TRADE cannot select a candidate",
            )
            _require(bool(self.reason_codes), "NO_TRADE requires reason_codes")
        else:
            _token(self.selected_candidate_id or "", "MANUAL_CANDIDATE selected_candidate_id")
            _require(
                not self.reason_codes,
                "MANUAL_CANDIDATE cannot contain block reasons",
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "action": self.action.value,
            "selected_candidate_id": self.selected_candidate_id,
            "score_threshold": self.score_threshold,
            "reason_codes": list(self.reason_codes),
        }


@dataclass(frozen=True, slots=True)
class StrategyDistributionForecast:
    document_id: str
    source_snapshot_id: str
    trading_date_et: date
    session: ForecastSession
    observed_through: datetime
    available_at: datetime
    valid_until: datetime
    model_version: str
    feature_set_version: str
    calibration_status: CalibrationStatus
    calibration_version: str | None
    policy_version: str
    q_event: ProbabilityEstimate
    p_event: ProbabilityEstimate
    strategy_candidates: tuple[StrategyCandidate, ...]
    shadow_decision: ShadowDecision
    quality: ForecastQuality
    quality_reason_codes: tuple[str, ...]
    schema_version: str = SCHEMA_VERSION
    evidence_status: ForecastEvidenceStatus = ForecastEvidenceStatus.RESEARCH_UNVALIDATED
    action_authority: ForecastActionAuthority = ForecastActionAuthority.NONE
    automatic_ordering: bool = False

    def __post_init__(self) -> None:
        _token(self.document_id, "forecast document_id")
        _token(self.source_snapshot_id, "forecast source_snapshot_id")
        _date(self.trading_date_et, "forecast trading_date_et")
        _require(type(self.session) is ForecastSession, "forecast session must be typed")
        _aware(self.observed_through, "forecast observed_through")
        _aware(self.available_at, "forecast available_at")
        _aware(self.valid_until, "forecast valid_until")
        _require(
            self.observed_through <= self.available_at,
            "forecast observed_through is after available_at",
        )
        _require(
            self.valid_until > self.available_at,
            "forecast valid_until must be after available_at",
        )
        _token(self.model_version, "forecast model_version")
        _token(self.feature_set_version, "forecast feature_set_version")
        _require(
            type(self.calibration_status) is CalibrationStatus,
            "forecast calibration_status must be typed",
        )
        _optional_token(self.calibration_version, "forecast calibration_version")
        if self.calibration_status is CalibrationStatus.UNCALIBRATED:
            _require(
                self.calibration_version is None,
                "uncalibrated forecast cannot claim calibration_version",
            )
        else:
            _token(
                self.calibration_version or "",
                "walk-forward calibrated forecast calibration_version",
            )
        _token(self.policy_version, "forecast policy_version")
        _require(type(self.q_event) is ProbabilityEstimate, "q_event must be typed")
        _require(type(self.p_event) is ProbabilityEstimate, "p_event must be typed")
        _require(
            self.q_event.measure is ProbabilityMeasure.RISK_NEUTRAL,
            "q_event must use risk-neutral probability",
        )
        _require(
            self.p_event.measure is ProbabilityMeasure.PHYSICAL,
            "p_event must use physical probability",
        )
        _require(
            self.q_event.event == self.p_event.event,
            "q_event and p_event must describe the same event",
        )
        if self.q_event.event is not None:
            _require(
                self.q_event.event.target_at > self.observed_through,
                "probability event target must be after observed_through",
            )
        else:
            _require(
                self.q_event.status is EstimateStatus.UNAVAILABLE
                and self.p_event.status is EstimateStatus.UNAVAILABLE,
                "missing probability event requires unavailable q_event and p_event",
            )
        if self.p_event.trained_through_date is not None:
            _require(
                self.p_event.trained_through_date < self.trading_date_et,
                "physical probability training must precede trading_date_et",
            )
        _require(
            type(self.strategy_candidates) is tuple
            and all(type(candidate) is StrategyCandidate for candidate in self.strategy_candidates),
            "strategy_candidates must be a typed tuple",
        )
        _require(
            len(self.strategy_candidates) <= MAX_STRATEGY_CANDIDATES,
            "strategy_candidates exceed the bounded contract",
        )
        candidate_ids = tuple(candidate.candidate_id for candidate in self.strategy_candidates)
        _require(
            candidate_ids == tuple(sorted(set(candidate_ids))),
            "strategy candidate ids must be unique and sorted",
        )
        for candidate in self.strategy_candidates:
            _require(
                self.q_event.event is not None,
                "strategy candidate requires a probability event",
            )
            assert self.q_event.event is not None
            _require(
                candidate.probability_event_id == self.q_event.event.event_id,
                "strategy candidate references a different probability event",
            )
            _require(
                candidate.expiry == self.trading_date_et,
                "0DTE candidate expiry must match trading_date_et",
            )

        _require(type(self.shadow_decision) is ShadowDecision, "shadow_decision must be typed")
        _require(type(self.quality) is ForecastQuality, "forecast quality must be typed")
        _reason_codes(self.quality_reason_codes, "forecast quality_reason_codes")
        if self.quality is ForecastQuality.READY:
            _require(
                not self.quality_reason_codes,
                "ready forecast cannot contain quality reasons",
            )
            _require(
                self.q_event.status is EstimateStatus.AVAILABLE
                and self.q_event.quality is EstimateQuality.READY
                and self.p_event.status is EstimateStatus.AVAILABLE
                and self.p_event.quality is EstimateQuality.READY,
                "ready forecast requires ready q_event and p_event",
            )
        else:
            _require(
                bool(self.quality_reason_codes),
                "non-ready forecast requires quality_reason_codes",
            )

        selected_id = self.shadow_decision.selected_candidate_id
        selected = next(
            (
                candidate
                for candidate in self.strategy_candidates
                if candidate.candidate_id == selected_id
            ),
            None,
        )
        if self.shadow_decision.action is ShadowAction.MANUAL_CANDIDATE:
            _require(
                self.quality is ForecastQuality.READY, "manual candidate requires READY quality"
            )
            _require(
                selected is not None, "selected manual candidate is not in strategy_candidates"
            )
            assert selected is not None
            _require(
                selected.execution.status is EstimateStatus.AVAILABLE
                and selected.net_pnl.status is EstimateStatus.AVAILABLE
                and selected.score.status is EstimateStatus.AVAILABLE,
                "manual candidate requires available execution, net-PnL, and score",
            )
            assert selected.score.total is not None
            _require(
                selected.score.total > self.shadow_decision.score_threshold,
                "manual candidate score must exceed threshold",
            )
        else:
            _require(selected is None, "NO_TRADE cannot resolve a selected candidate")
        if not self.strategy_candidates:
            _require(
                self.shadow_decision.action is ShadowAction.NO_TRADE,
                "empty candidate set requires NO_TRADE",
            )

        _require(self.schema_version == SCHEMA_VERSION, "forecast schema_version drift")
        _require(
            self.evidence_status is ForecastEvidenceStatus.RESEARCH_UNVALIDATED,
            "forecast evidence status drift",
        )
        _require(
            self.action_authority is ForecastActionAuthority.NONE,
            "forecast cannot have action authority",
        )
        _require(self.automatic_ordering is False, "forecast cannot enable automatic ordering")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "document_id": self.document_id,
            "source_snapshot_id": self.source_snapshot_id,
            "trading_date_et": self.trading_date_et.isoformat(),
            "session": self.session.value,
            "observed_through": self.observed_through.isoformat(),
            "available_at": self.available_at.isoformat(),
            "valid_until": self.valid_until.isoformat(),
            "model_version": self.model_version,
            "feature_set_version": self.feature_set_version,
            "calibration_status": self.calibration_status.value,
            "calibration_version": self.calibration_version,
            "policy_version": self.policy_version,
            "evidence_status": self.evidence_status.value,
            "q_event": self.q_event.to_dict(),
            "p_event": self.p_event.to_dict(),
            "strategy_candidates": [candidate.to_dict() for candidate in self.strategy_candidates],
            "shadow_decision": self.shadow_decision.to_dict(),
            "quality": self.quality.value,
            "quality_reason_codes": list(self.quality_reason_codes),
            "action_authority": self.action_authority.value,
            "automatic_ordering": self.automatic_ordering,
        }


__all__ = [
    "MAX_STRATEGY_CANDIDATES",
    "SCHEMA_VERSION",
    "VERTICAL_WIDTH_POINTS",
    "CalibrationStatus",
    "CandidateDirection",
    "CandidateScore",
    "EstimateQuality",
    "EstimateStatus",
    "ExecutionEstimate",
    "ExecutionSemantics",
    "ForecastActionAuthority",
    "ForecastEvidenceStatus",
    "ForecastQuality",
    "ForecastSession",
    "NetPnlBasis",
    "NetPnlEstimate",
    "NetPnlUnit",
    "ProbabilityEstimate",
    "ProbabilityEventDefinition",
    "ProbabilityEventKind",
    "ProbabilityMeasure",
    "ShadowAction",
    "ShadowDecision",
    "StrategyCandidate",
    "StrategyDistributionForecast",
]
