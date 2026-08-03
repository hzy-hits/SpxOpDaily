"""Strict advisory-only contracts for cross-index research context.

The contracts in this module describe observations and experimental model
output.  They deliberately cannot authorize an order, trigger, notification,
or any other execution-side effect.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum


SCHEMA_VERSION = "research_context.v2"


class CashIndex(str, Enum):
    SPX = "index:SPX"
    NDX = "index:NDX"
    DJI = "index:DJI"
    RUT = "index:RUT"


CASH_INDEX_ORDER = (CashIndex.SPX, CashIndex.NDX, CashIndex.DJI, CashIndex.RUT)


class ObservationStatus(str, Enum):
    AVAILABLE = "available"
    MISSING = "missing"


class ResearchDataQuality(str, Enum):
    LIVE = "live"
    DELAYED = "delayed"
    FROZEN = "frozen"
    MISSING = "missing"


class IndexPriceKind(str, Enum):
    LAST = "last"
    MID = "mid"


class ResearchSession(str, Enum):
    RTH = "rth"


class ResearchUseScope(str, Enum):
    ADVISORY = "advisory"


class ResearchEvidenceStatus(str, Enum):
    BOOTSTRAP_UNVALIDATED = "bootstrap_unvalidated"


class ResearchContextStatus(str, Enum):
    READY = "ready"
    PARTIAL = "partial"
    UNAVAILABLE = "unavailable"


class ActionAuthority(str, Enum):
    NONE = "none"


class HMMInference(str, Enum):
    FILTERED = "filtered"


class HMMParameterMode(str, Enum):
    FIXED_BOOTSTRAP = "fixed_bootstrap"


class ForecastStatus(str, Enum):
    AVAILABLE = "available"
    UNAVAILABLE = "unavailable"


class ForecastDistribution(str, Enum):
    RISK_NEUTRAL = "risk_neutral"
    PHYSICAL = "physical"
    EXPERIMENTAL_HEURISTIC = "experimental_heuristic"


class ForecastTarget(str, Enum):
    RTH_CLOSE = "rth_close"
    SESSION_HIGH = "session_high"
    SESSION_LOW = "session_low"


FORECAST_TARGET_ORDER = (
    ForecastTarget.RTH_CLOSE,
    ForecastTarget.SESSION_HIGH,
    ForecastTarget.SESSION_LOW,
)


class CloseLocationBucket(str, Enum):
    LOWER_THIRD = "lower_third"
    MIDDLE_THIRD = "middle_third"
    UPPER_THIRD = "upper_third"


CLOSE_LOCATION_BUCKET_ORDER = (
    CloseLocationBucket.LOWER_THIRD,
    CloseLocationBucket.MIDDLE_THIRD,
    CloseLocationBucket.UPPER_THIRD,
)


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def _aware(value: datetime, name: str) -> None:
    _require(
        type(value) is datetime and value.tzinfo is not None and value.utcoffset() is not None,
        f"{name} must be timezone-aware",
    )


def _token(value: str, name: str) -> None:
    _require(type(value) is str and bool(value.strip()), f"{name} must be non-empty")


def _finite(value: float, name: str) -> None:
    _require(type(value) is float and math.isfinite(value), f"{name} must be finite float")


def _reason_codes(values: tuple[str, ...], name: str) -> None:
    _require(type(values) is tuple, f"{name} must be a tuple")
    _require(
        all(type(value) is str and bool(value.strip()) for value in values),
        f"{name} contain invalid values",
    )
    _require(values == tuple(sorted(set(values))), f"{name} must be unique and sorted")


@dataclass(frozen=True, slots=True)
class IndexObservation:
    instrument: CashIndex
    status: ObservationStatus
    quality: ResearchDataQuality
    available_at: datetime
    lineage_id: str
    price: float | None = None
    reference_close: float | None = None
    price_kind: IndexPriceKind | None = None
    provider: str | None = None
    source_as_of: datetime | None = None
    missing_reason: str | None = None

    def __post_init__(self) -> None:
        _require(type(self.instrument) is CashIndex, "instrument must be typed")
        _require(type(self.status) is ObservationStatus, "observation status must be typed")
        _require(type(self.quality) is ResearchDataQuality, "observation quality must be typed")
        _aware(self.available_at, "observation available_at")
        _token(self.lineage_id, "observation lineage_id")
        if self.status is ObservationStatus.MISSING:
            _require(
                self.quality is ResearchDataQuality.MISSING, "missing observation quality drift"
            )
            _require(
                self.price is None
                and self.reference_close is None
                and self.price_kind is None
                and self.provider is None
                and self.source_as_of is None,
                "missing observation cannot contain market values",
            )
            _token(self.missing_reason or "", "missing observation reason")
            return
        _require(
            self.quality is not ResearchDataQuality.MISSING, "available observation is missing"
        )
        _require(self.missing_reason is None, "available observation cannot have missing_reason")
        _require(type(self.price_kind) is IndexPriceKind, "available price_kind must be typed")
        _token(self.provider or "", "available observation provider")
        _require(self.source_as_of is not None, "available observation source_as_of missing")
        assert self.source_as_of is not None
        _aware(self.source_as_of, "observation source_as_of")
        _require(self.source_as_of <= self.available_at, "source_as_of is after available_at")
        _require(self.price is not None, "available observation price missing")
        assert self.price is not None
        _finite(self.price, "observation price")
        _require(self.price > 0.0, "observation price must be positive")
        if self.reference_close is not None:
            _finite(self.reference_close, "observation reference_close")
            _require(self.reference_close > 0.0, "observation reference_close must be positive")

    @property
    def return_bps(self) -> float | None:
        if self.price is None or self.reference_close is None:
            return None
        return (self.price / self.reference_close - 1.0) * 10_000.0

    def to_dict(self) -> dict[str, object]:
        return {
            "instrument": self.instrument.value,
            "status": self.status.value,
            "quality": self.quality.value,
            "price": self.price,
            "reference_close": self.reference_close,
            "return_bps": self.return_bps,
            "price_kind": self.price_kind.value if self.price_kind is not None else None,
            "provider": self.provider,
            "source_as_of": self.source_as_of.isoformat() if self.source_as_of else None,
            "available_at": self.available_at.isoformat(),
            "lineage_id": self.lineage_id,
            "missing_reason": self.missing_reason,
        }


@dataclass(frozen=True, slots=True)
class CrossIndexFrame:
    frame_id: str
    trading_date_et: date
    observed_through: datetime
    available_at: datetime
    observations: tuple[IndexObservation, ...]
    feature_set_version: str
    source_skew_limit_seconds: float
    session: ResearchSession = ResearchSession.RTH

    def __post_init__(self) -> None:
        _token(self.frame_id, "cross-index frame_id")
        _require(type(self.trading_date_et) is date, "trading_date_et must be a date")
        _aware(self.observed_through, "cross-index observed_through")
        _aware(self.available_at, "cross-index available_at")
        _require(
            self.observed_through <= self.available_at, "frame observed_through is unavailable"
        )
        _token(self.feature_set_version, "cross-index feature_set_version")
        _finite(self.source_skew_limit_seconds, "source_skew_limit_seconds")
        _require(self.source_skew_limit_seconds > 0.0, "source skew limit must be positive")
        _require(self.session is ResearchSession.RTH, "cash-index frame must be RTH")
        _require(
            type(self.observations) is tuple
            and all(type(observation) is IndexObservation for observation in self.observations),
            "cross-index observations must be typed",
        )
        _require(
            tuple(observation.instrument for observation in self.observations) == CASH_INDEX_ORDER,
            "cross-index frame must contain SPX/NDX/DJI/RUT exactly once in canonical order",
        )
        for observation in self.observations:
            _require(
                observation.available_at <= self.available_at,
                f"{observation.instrument.value} was unavailable at frame publication",
            )
            if observation.source_as_of is not None:
                _require(
                    observation.source_as_of <= self.observed_through,
                    f"{observation.instrument.value} source is after observed_through",
                )

    @property
    def missing_instruments(self) -> tuple[str, ...]:
        return tuple(
            observation.instrument.value
            for observation in self.observations
            if observation.status is ObservationStatus.MISSING
        )

    @property
    def source_skew_seconds(self) -> float | None:
        source_times = [
            observation.source_as_of
            for observation in self.observations
            if observation.source_as_of is not None
        ]
        if len(source_times) < 2:
            return None
        return (max(source_times) - min(source_times)).total_seconds()

    @property
    def status(self) -> str:
        skew = self.source_skew_seconds
        ready = (
            not self.missing_instruments
            and all(
                observation.quality is ResearchDataQuality.LIVE for observation in self.observations
            )
            and skew is not None
            and skew <= self.source_skew_limit_seconds
        )
        return "ready" if ready else "degraded"

    def to_dict(self) -> dict[str, object]:
        return {
            "frame_id": self.frame_id,
            "trading_date_et": self.trading_date_et.isoformat(),
            "session": self.session.value,
            "observed_through": self.observed_through.isoformat(),
            "available_at": self.available_at.isoformat(),
            "feature_set_version": self.feature_set_version,
            "status": self.status,
            "missing_instruments": list(self.missing_instruments),
            "source_skew_seconds": self.source_skew_seconds,
            "source_skew_limit_seconds": self.source_skew_limit_seconds,
            "observations": [observation.to_dict() for observation in self.observations],
        }


@dataclass(frozen=True, slots=True)
class PriorRthContextReference:
    context_id: str
    status: ResearchContextStatus
    for_trading_date: date
    session_date: date
    source_as_of: datetime
    available_at: datetime
    return_bps: tuple[tuple[CashIndex, float | None], ...]
    reason_codes: tuple[str, ...]
    schema_version: str = "prior_rth_context.v2"

    def __post_init__(self) -> None:
        _token(self.context_id, "prior-RTH context_id")
        _require(type(self.status) is ResearchContextStatus, "prior-RTH status must be typed")
        _require(type(self.for_trading_date) is date, "prior-RTH for_trading_date invalid")
        _require(type(self.session_date) is date, "prior-RTH session_date invalid")
        _require(
            self.session_date < self.for_trading_date,
            "prior-RTH session must precede the forecast trading date",
        )
        _aware(self.source_as_of, "prior-RTH source_as_of")
        _aware(self.available_at, "prior-RTH available_at")
        _require(self.source_as_of <= self.available_at, "prior-RTH context was unavailable")
        _require(self.schema_version == "prior_rth_context.v2", "prior-RTH schema drift")
        _require(
            type(self.return_bps) is tuple
            and tuple(instrument for instrument, _value in self.return_bps) == CASH_INDEX_ORDER,
            "prior-RTH returns must contain SPX/NDX/DJI/RUT in canonical order",
        )
        for instrument, value in self.return_bps:
            _require(type(instrument) is CashIndex, "prior-RTH instrument must be typed")
            if value is not None:
                _finite(value, f"prior-RTH return {instrument.value}")
        _reason_codes(self.reason_codes, "prior-RTH reason_codes")
        available_return_count = sum(value is not None for _instrument, value in self.return_bps)
        if self.status is ResearchContextStatus.READY:
            _require(
                available_return_count == len(CASH_INDEX_ORDER),
                "ready prior-RTH returns incomplete",
            )
        elif self.status is ResearchContextStatus.PARTIAL:
            _require(
                available_return_count > 0,
                "partial prior-RTH returns invalid",
            )
            _require(bool(self.reason_codes), "partial prior-RTH context requires reasons")
        if self.status is ResearchContextStatus.UNAVAILABLE:
            _require(bool(self.reason_codes), "unavailable prior-RTH context requires reasons")

    def to_dict(self) -> dict[str, object]:
        return {
            "context_id": self.context_id,
            "schema_version": self.schema_version,
            "status": self.status.value,
            "for_trading_date": self.for_trading_date.isoformat(),
            "session_date": self.session_date.isoformat(),
            "source_as_of": self.source_as_of.isoformat(),
            "available_at": self.available_at.isoformat(),
            "return_bps": {instrument.value: value for instrument, value in self.return_bps},
            "reason_codes": list(self.reason_codes),
            "semantics": "observed_prior_rth_cash_index_regime_not_market_maker_behavior",
        }


@dataclass(frozen=True, slots=True)
class FilteredRegimePosterior:
    signal_id: str
    frame_id: str
    model_version: str
    feature_set_version: str
    sequence_id: str
    trading_date_et: date
    observed_through: datetime
    available_at: datetime
    update_index: int
    probabilities: tuple[tuple[str, float], ...]
    inference: HMMInference = HMMInference.FILTERED
    parameter_mode: HMMParameterMode = HMMParameterMode.FIXED_BOOTSTRAP
    evidence_status: ResearchEvidenceStatus = ResearchEvidenceStatus.BOOTSTRAP_UNVALIDATED
    use_scope: ResearchUseScope = ResearchUseScope.ADVISORY
    trained_through_date: date | None = None
    posterior_entropy: float = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (self.signal_id, "regime signal_id"),
            (self.frame_id, "regime frame_id"),
            (self.model_version, "regime model_version"),
            (self.feature_set_version, "regime feature_set_version"),
            (self.sequence_id, "regime sequence_id"),
        ):
            _token(value, name)
        _require(type(self.trading_date_et) is date, "regime trading_date_et must be a date")
        _aware(self.observed_through, "regime observed_through")
        _aware(self.available_at, "regime available_at")
        _require(self.observed_through <= self.available_at, "regime observed after publication")
        _require(type(self.update_index) is int and self.update_index > 0, "update_index invalid")
        _require(self.inference is HMMInference.FILTERED, "HMM inference must remain filtered")
        _require(
            self.parameter_mode is HMMParameterMode.FIXED_BOOTSTRAP,
            "current real-time HMM must remain fixed bootstrap",
        )
        _require(
            self.evidence_status is ResearchEvidenceStatus.BOOTSTRAP_UNVALIDATED,
            "bootstrap HMM evidence status drift",
        )
        _require(self.use_scope is ResearchUseScope.ADVISORY, "HMM must remain advisory")
        _require(
            self.trained_through_date is None, "fixed bootstrap cannot claim a training cutoff"
        )
        _require(type(self.probabilities) is tuple and self.probabilities, "posterior missing")
        _require(
            all(type(item) is tuple and len(item) == 2 for item in self.probabilities),
            "posterior must contain state/probability pairs",
        )
        states = tuple(state for state, _probability in self.probabilities)
        _require(states == tuple(sorted(set(states))), "posterior states must be unique and sorted")
        for state, probability in self.probabilities:
            _token(state, "posterior state")
            _finite(probability, f"posterior probability {state}")
            _require(0.0 <= probability <= 1.0, f"posterior probability invalid for {state}")
        _require(
            math.isclose(
                math.fsum(value for _state, value in self.probabilities), 1.0, abs_tol=1e-9
            ),
            "posterior probabilities must sum to one",
        )
        object.__setattr__(
            self,
            "posterior_entropy",
            -math.fsum(value * math.log(value) for _state, value in self.probabilities if value),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "signal_id": self.signal_id,
            "frame_id": self.frame_id,
            "model_version": self.model_version,
            "feature_set_version": self.feature_set_version,
            "sequence_id": self.sequence_id,
            "trading_date_et": self.trading_date_et.isoformat(),
            "observed_through": self.observed_through.isoformat(),
            "available_at": self.available_at.isoformat(),
            "update_index": self.update_index,
            "inference": self.inference.value,
            "parameter_mode": self.parameter_mode.value,
            "evidence_status": self.evidence_status.value,
            "use_scope": self.use_scope.value,
            "trained_through_date": None,
            "posterior": [
                {"state_id": state, "probability": probability}
                for state, probability in self.probabilities
            ],
            "posterior_entropy": self.posterior_entropy,
        }


@dataclass(frozen=True, slots=True)
class QuantileBand:
    p10: float
    p50: float
    p90: float

    def __post_init__(self) -> None:
        for value, name in ((self.p10, "p10"), (self.p50, "p50"), (self.p90, "p90")):
            _finite(value, name)
            _require(value > 0.0, f"{name} must be positive")
        _require(self.p10 < self.p50 < self.p90, "forecast quantiles must be strictly ordered")

    def to_dict(self) -> dict[str, float]:
        return {"p10": self.p10, "p50": self.p50, "p90": self.p90}


@dataclass(frozen=True, slots=True)
class SpxRangeForecast:
    forecast_id: str
    target: ForecastTarget
    status: ForecastStatus
    observed_through: datetime
    available_at: datetime
    target_at: datetime
    reason_codes: tuple[str, ...]
    distribution: ForecastDistribution | None = None
    quantiles: QuantileBand | None = None
    model_version: str | None = None
    evidence_status: ResearchEvidenceStatus = ResearchEvidenceStatus.BOOTSTRAP_UNVALIDATED
    use_scope: ResearchUseScope = ResearchUseScope.ADVISORY

    def __post_init__(self) -> None:
        _token(self.forecast_id, "forecast_id")
        _require(type(self.target) is ForecastTarget, "forecast target must be typed")
        _require(type(self.status) is ForecastStatus, "forecast status must be typed")
        _aware(self.observed_through, "forecast observed_through")
        _aware(self.available_at, "forecast available_at")
        _aware(self.target_at, "forecast target_at")
        _require(self.observed_through <= self.available_at, "forecast observed after publication")
        _reason_codes(self.reason_codes, "forecast reason_codes")
        _require(
            self.evidence_status is ResearchEvidenceStatus.BOOTSTRAP_UNVALIDATED,
            "forecast evidence status drift",
        )
        _require(self.use_scope is ResearchUseScope.ADVISORY, "forecast must remain advisory")
        if self.status is ForecastStatus.UNAVAILABLE:
            _require(bool(self.reason_codes), "unavailable forecast requires reason_codes")
            _require(
                self.distribution is None and self.quantiles is None and self.model_version is None,
                "unavailable forecast cannot claim a distribution or model output",
            )
            return
        _require(
            self.target_at > self.available_at, "available forecast target must be in the future"
        )
        _require(type(self.distribution) is ForecastDistribution, "forecast distribution missing")
        _require(type(self.quantiles) is QuantileBand, "forecast quantiles missing")
        _token(self.model_version or "", "forecast model_version")

    def to_dict(self) -> dict[str, object]:
        return {
            "forecast_id": self.forecast_id,
            "target": self.target.value,
            "status": self.status.value,
            "observed_through": self.observed_through.isoformat(),
            "available_at": self.available_at.isoformat(),
            "target_at": self.target_at.isoformat(),
            "reason_codes": list(self.reason_codes),
            "distribution": self.distribution.value if self.distribution else None,
            "quantiles": self.quantiles.to_dict() if self.quantiles else None,
            "model_version": self.model_version,
            "evidence_status": self.evidence_status.value,
            "use_scope": self.use_scope.value,
        }


@dataclass(frozen=True, slots=True)
class CloseLocationDistribution:
    status: ForecastStatus
    observed_through: datetime
    available_at: datetime
    target_at: datetime
    reason_codes: tuple[str, ...]
    probabilities: tuple[tuple[CloseLocationBucket, float], ...] = ()
    method_version: str | None = None
    distribution: ForecastDistribution | None = None
    evidence_status: ResearchEvidenceStatus = ResearchEvidenceStatus.BOOTSTRAP_UNVALIDATED
    use_scope: ResearchUseScope = ResearchUseScope.ADVISORY

    def __post_init__(self) -> None:
        _require(type(self.status) is ForecastStatus, "close-location status must be typed")
        _aware(self.observed_through, "close-location observed_through")
        _aware(self.available_at, "close-location available_at")
        _aware(self.target_at, "close-location target_at")
        _require(
            self.observed_through <= self.available_at,
            "close-location observation is after publication",
        )
        _reason_codes(self.reason_codes, "close-location reason_codes")
        _require(
            self.evidence_status is ResearchEvidenceStatus.BOOTSTRAP_UNVALIDATED,
            "close-location evidence status drift",
        )
        _require(self.use_scope is ResearchUseScope.ADVISORY, "close-location must be advisory")
        if self.status is ForecastStatus.UNAVAILABLE:
            _require(bool(self.reason_codes), "unavailable close-location requires reasons")
            _require(
                not self.probabilities
                and self.method_version is None
                and self.distribution is None,
                "unavailable close-location cannot claim probabilities",
            )
            return
        _require(self.target_at > self.available_at, "close-location target must be in the future")
        _require(
            tuple(bucket for bucket, _probability in self.probabilities)
            == CLOSE_LOCATION_BUCKET_ORDER,
            "close-location buckets must be canonical thirds",
        )
        for bucket, probability in self.probabilities:
            _require(type(bucket) is CloseLocationBucket, "close-location bucket must be typed")
            _finite(probability, f"close-location probability {bucket.value}")
            _require(0.0 <= probability <= 1.0, "close-location probability is invalid")
        _require(
            math.isclose(
                math.fsum(probability for _bucket, probability in self.probabilities),
                1.0,
                abs_tol=1e-9,
            ),
            "close-location probabilities must sum to one",
        )
        _token(self.method_version or "", "close-location method_version")
        _require(
            self.distribution is ForecastDistribution.EXPERIMENTAL_HEURISTIC,
            "close-location cannot claim physical probability semantics",
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status.value,
            "observed_through": self.observed_through.isoformat(),
            "available_at": self.available_at.isoformat(),
            "target_at": self.target_at.isoformat(),
            "reason_codes": list(self.reason_codes),
            "probabilities": {
                bucket.value: probability for bucket, probability in self.probabilities
            },
            "method_version": self.method_version,
            "distribution": self.distribution.value if self.distribution else None,
            "bucket_definition": "thirds_of_projected_session_low_to_high_range",
            "evidence_status": self.evidence_status.value,
            "use_scope": self.use_scope.value,
        }


@dataclass(frozen=True, slots=True)
class ResearchContextDocument:
    document_id: str
    generated_at: datetime
    cross_index_frame: CrossIndexFrame
    prior_rth_context: PriorRthContextReference
    regime: FilteredRegimePosterior | None
    regime_reason_codes: tuple[str, ...]
    forecasts: tuple[SpxRangeForecast, ...]
    close_location: CloseLocationDistribution
    schema_version: str = SCHEMA_VERSION
    evidence_status: ResearchEvidenceStatus = ResearchEvidenceStatus.BOOTSTRAP_UNVALIDATED
    use_scope: ResearchUseScope = ResearchUseScope.ADVISORY
    action_authority: ActionAuthority = ActionAuthority.NONE
    automatic_ordering: bool = False

    def __post_init__(self) -> None:
        _token(self.document_id, "research document_id")
        _aware(self.generated_at, "research generated_at")
        _require(
            type(self.cross_index_frame) is CrossIndexFrame,
            "cross_index_frame must be typed",
        )
        _require(
            type(self.prior_rth_context) is PriorRthContextReference,
            "prior_rth_context must be typed",
        )
        _require(
            self.generated_at >= self.cross_index_frame.available_at, "document predates frame"
        )
        _require(
            self.generated_at >= self.prior_rth_context.available_at,
            "document predates prior-RTH context",
        )
        _require(self.schema_version == SCHEMA_VERSION, "research schema_version drift")
        _require(
            self.evidence_status is ResearchEvidenceStatus.BOOTSTRAP_UNVALIDATED,
            "research evidence status drift",
        )
        _require(self.use_scope is ResearchUseScope.ADVISORY, "research use_scope drift")
        _require(self.action_authority is ActionAuthority.NONE, "research has action authority")
        _require(self.automatic_ordering is False, "research cannot enable automatic ordering")
        _reason_codes(self.regime_reason_codes, "regime_reason_codes")
        _require(
            self.regime is None or type(self.regime) is FilteredRegimePosterior,
            "regime must be typed",
        )
        if self.regime is None:
            _require(bool(self.regime_reason_codes), "missing regime requires explicit reasons")
        else:
            _require(
                self.regime.frame_id == self.cross_index_frame.frame_id,
                "regime references a different cross-index frame",
            )
        _require(
            type(self.forecasts) is tuple
            and all(type(forecast) is SpxRangeForecast for forecast in self.forecasts),
            "forecasts must be typed",
        )
        _require(
            type(self.close_location) is CloseLocationDistribution,
            "close_location must be typed",
        )
        _require(
            tuple(forecast.target for forecast in self.forecasts) == FORECAST_TARGET_ORDER,
            "research document must contain close/high/low forecasts in canonical order",
        )
        for item in (*self.forecasts, self.close_location):
            _require(item.available_at <= self.generated_at, "document predates model output")
        _require(
            all(forecast.target_at == self.close_location.target_at for forecast in self.forecasts),
            "close/high/low and close-location targets must align",
        )
        if self.close_location.status is ForecastStatus.AVAILABLE:
            _require(
                all(forecast.status is ForecastStatus.AVAILABLE for forecast in self.forecasts),
                "available close-location requires close/high/low forecasts",
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "document_id": self.document_id,
            "generated_at": self.generated_at.isoformat(),
            "evidence_status": self.evidence_status.value,
            "use_scope": self.use_scope.value,
            "action_authority": self.action_authority.value,
            "automatic_ordering": self.automatic_ordering,
            "cross_index_frame": self.cross_index_frame.to_dict(),
            "prior_rth_context": self.prior_rth_context.to_dict(),
            "regime": self.regime.to_dict() if self.regime else None,
            "regime_reason_codes": list(self.regime_reason_codes),
            "forecasts": [forecast.to_dict() for forecast in self.forecasts],
            "close_location": self.close_location.to_dict(),
        }


__all__ = [
    "ActionAuthority",
    "CASH_INDEX_ORDER",
    "CLOSE_LOCATION_BUCKET_ORDER",
    "CashIndex",
    "CloseLocationBucket",
    "CloseLocationDistribution",
    "CrossIndexFrame",
    "FORECAST_TARGET_ORDER",
    "FilteredRegimePosterior",
    "ForecastDistribution",
    "ForecastStatus",
    "ForecastTarget",
    "HMMInference",
    "HMMParameterMode",
    "IndexObservation",
    "IndexPriceKind",
    "ObservationStatus",
    "QuantileBand",
    "PriorRthContextReference",
    "ResearchContextDocument",
    "ResearchContextStatus",
    "ResearchDataQuality",
    "ResearchEvidenceStatus",
    "ResearchSession",
    "ResearchUseScope",
    "SCHEMA_VERSION",
    "SpxRangeForecast",
]
