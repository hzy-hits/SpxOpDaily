use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, NaiveDate, Utc};
use chrono_tz::America::New_York;
use serde::{Deserialize, Serialize};

use crate::validation::{require_schema, unique_tokens};
use crate::{
    DomainError, MarketSession, NonNegativeF64, PositiveF64, ProbabilityF64,
    RESEARCH_SIGNALS_SCHEMA_VERSION, Token, Validate,
};

const POSTERIOR_SUM_TOLERANCE: f64 = 1e-6;
const ENTROPY_TOLERANCE: f64 = 1e-9;
const RESEARCH_CONTEXT_SCHEMA_VERSION: &str = "research_context.v2";
const PRIOR_RTH_CONTEXT_SCHEMA_VERSION: &str = "prior_rth_context.v2";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResearchUseScope {
    Experimental,
    Advisory,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RegimeInferenceSemantics {
    CausalFiltered,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForecastDistribution {
    RiskNeutral,
    Physical,
    ExperimentalHeuristic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RangeForecastKind {
    ProjectedOpen,
    RiskNeutralClose,
    HmmAdjustedClose,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegimeProbabilityV1 {
    pub state_id: Token,
    pub probability: ProbabilityF64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketRegimeSignalV1 {
    pub signal_id: Token,
    pub feature_set_version: Token,
    pub model_version: Token,
    pub lineage_id: Token,
    pub session: MarketSession,
    pub observed_through: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub valid_until: DateTime<Utc>,
    pub inference_semantics: RegimeInferenceSemantics,
    pub use_scope: ResearchUseScope,
    pub posterior: Vec<RegimeProbabilityV1>,
    pub posterior_entropy: NonNegativeF64,
    pub observation_count: u32,
}

impl Validate for MarketRegimeSignalV1 {
    fn validate(&self) -> Result<(), DomainError> {
        if self.use_scope != ResearchUseScope::Experimental {
            return Err(DomainError::Invalid {
                field: "regime use_scope",
                reason: "legacy research signals must remain experimental",
            });
        }
        if self.observed_through > self.available_at {
            return Err(DomainError::TimeOrder(
                "regime observed_through is after available_at",
            ));
        }
        if self.valid_until <= self.available_at {
            return Err(DomainError::TimeOrder(
                "regime valid_until must be after available_at",
            ));
        }
        if !(2..=16).contains(&self.posterior.len()) {
            return Err(DomainError::Invalid {
                field: "regime posterior",
                reason: "must contain 2..=16 states",
            });
        }
        let state_ids: Vec<Token> = self
            .posterior
            .iter()
            .map(|state| state.state_id.clone())
            .collect();
        unique_tokens(&state_ids, "regime state_id")?;
        let probability_sum: f64 = self
            .posterior
            .iter()
            .map(|state| state.probability.get())
            .sum();
        if (probability_sum - 1.0).abs() > POSTERIOR_SUM_TOLERANCE {
            return Err(DomainError::Invalid {
                field: "regime posterior",
                reason: "probabilities must sum to one",
            });
        }
        let state_count = u32::try_from(self.posterior.len()).expect("posterior is bounded to 16");
        let maximum_entropy = f64::from(state_count).ln();
        if self.posterior_entropy.get() > maximum_entropy + ENTROPY_TOLERANCE {
            return Err(DomainError::Invalid {
                field: "posterior_entropy",
                reason: "cannot exceed ln(state_count)",
            });
        }
        if self.observation_count == 0 {
            return Err(DomainError::Invalid {
                field: "observation_count",
                reason: "must be positive",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RangeForecastV1 {
    pub forecast_id: Token,
    pub forecast_kind: RangeForecastKind,
    pub feature_set_version: Token,
    pub model_version: Token,
    pub lineage_id: Token,
    pub session: MarketSession,
    pub observed_through: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub valid_until: DateTime<Utc>,
    pub target_at: DateTime<Utc>,
    pub distribution: ForecastDistribution,
    pub use_scope: ResearchUseScope,
    pub lower_probability: ProbabilityF64,
    pub lower: PositiveF64,
    pub median: PositiveF64,
    pub upper_probability: ProbabilityF64,
    pub upper: PositiveF64,
}

impl Validate for RangeForecastV1 {
    fn validate(&self) -> Result<(), DomainError> {
        if self.use_scope != ResearchUseScope::Experimental {
            return Err(DomainError::Invalid {
                field: "range use_scope",
                reason: "legacy research signals must remain experimental",
            });
        }
        if self.observed_through > self.available_at {
            return Err(DomainError::TimeOrder(
                "range observed_through is after available_at",
            ));
        }
        if self.valid_until <= self.available_at {
            return Err(DomainError::TimeOrder(
                "range valid_until must be after available_at",
            ));
        }
        if self.valid_until > self.target_at {
            return Err(DomainError::TimeOrder(
                "range valid_until is after target_at",
            ));
        }
        if self.lower_probability.get() <= 0.0
            || self.lower_probability.get() >= 0.5
            || self.upper_probability.get() <= 0.5
            || self.upper_probability.get() >= 1.0
        {
            return Err(DomainError::Invalid {
                field: "range probabilities",
                reason: "must bracket the median strictly within 0..1",
            });
        }
        if self.lower.get() >= self.median.get() || self.median.get() >= self.upper.get() {
            return Err(DomainError::Invalid {
                field: "range levels",
                reason: "must satisfy lower < median < upper",
            });
        }
        let expected_distribution = match self.forecast_kind {
            RangeForecastKind::RiskNeutralClose => ForecastDistribution::RiskNeutral,
            RangeForecastKind::ProjectedOpen | RangeForecastKind::HmmAdjustedClose => {
                ForecastDistribution::ExperimentalHeuristic
            }
        };
        if self.distribution != expected_distribution {
            return Err(DomainError::Invalid {
                field: "range distribution",
                reason: "does not match forecast_kind semantics",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum CashIndex {
    #[serde(rename = "index:SPX")]
    Spx,
    #[serde(rename = "index:NDX")]
    Ndx,
    #[serde(rename = "index:DJI")]
    Dji,
    #[serde(rename = "index:RUT")]
    Rut,
}

const CASH_INDEX_ORDER: [CashIndex; 4] = [
    CashIndex::Spx,
    CashIndex::Ndx,
    CashIndex::Dji,
    CashIndex::Rut,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservationStatus {
    Available,
    Missing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResearchDataQuality {
    Live,
    Delayed,
    Frozen,
    Missing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexPriceKind {
    Last,
    Mid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResearchSession {
    Rth,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CrossIndexFrameStatus {
    Ready,
    Degraded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResearchContextStatus {
    Ready,
    Partial,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResearchEvidenceStatus {
    BootstrapUnvalidated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionAuthority {
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HmmInference {
    Filtered,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HmmParameterMode {
    FixedBootstrap,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForecastStatus {
    Available,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForecastTarget {
    RthClose,
    SessionHigh,
    SessionLow,
}

const FORECAST_TARGET_ORDER: [ForecastTarget; 3] = [
    ForecastTarget::RthClose,
    ForecastTarget::SessionHigh,
    ForecastTarget::SessionLow,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum CloseLocationBucket {
    #[serde(rename = "lower_third")]
    Lower,
    #[serde(rename = "middle_third")]
    Middle,
    #[serde(rename = "upper_third")]
    Upper,
}

const CLOSE_LOCATION_BUCKETS: [CloseLocationBucket; 3] = [
    CloseLocationBucket::Lower,
    CloseLocationBucket::Middle,
    CloseLocationBucket::Upper,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PriorRthSemantics {
    #[serde(rename = "observed_prior_rth_cash_index_regime_not_market_maker_behavior")]
    ObservedCashIndexRegime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CloseLocationBucketDefinition {
    #[serde(rename = "thirds_of_projected_session_low_to_high_range")]
    ProjectedRangeThirds,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexObservationV2 {
    pub instrument: CashIndex,
    pub status: ObservationStatus,
    pub quality: ResearchDataQuality,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub price: Option<PositiveF64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub reference_close: Option<PositiveF64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub return_bps: Option<f64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub price_kind: Option<IndexPriceKind>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub provider: Option<Token>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub source_as_of: Option<DateTime<Utc>>,
    pub available_at: DateTime<Utc>,
    pub lineage_id: Token,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub missing_reason: Option<Token>,
}

impl Validate for IndexObservationV2 {
    fn validate(&self) -> Result<(), DomainError> {
        if self.return_bps.is_some_and(|value| !value.is_finite()) {
            return Err(DomainError::Invalid {
                field: "index return_bps",
                reason: "must be finite when present",
            });
        }
        match self.status {
            ObservationStatus::Missing => {
                if self.quality != ResearchDataQuality::Missing
                    || self.price.is_some()
                    || self.reference_close.is_some()
                    || self.return_bps.is_some()
                    || self.price_kind.is_some()
                    || self.provider.is_some()
                    || self.source_as_of.is_some()
                    || self.missing_reason.is_none()
                {
                    return Err(DomainError::Invalid {
                        field: "missing index observation",
                        reason: "must contain only explicit missing evidence",
                    });
                }
            }
            ObservationStatus::Available => {
                if self.quality == ResearchDataQuality::Missing
                    || self.price.is_none()
                    || self.price_kind.is_none()
                    || self.provider.is_none()
                    || self.source_as_of.is_none()
                    || self.missing_reason.is_some()
                {
                    return Err(DomainError::Invalid {
                        field: "available index observation",
                        reason: "must contain typed market evidence",
                    });
                }
                if self
                    .source_as_of
                    .is_some_and(|source_as_of| source_as_of > self.available_at)
                {
                    return Err(DomainError::TimeOrder(
                        "index source_as_of is after available_at",
                    ));
                }
                if self.reference_close.is_some() != self.return_bps.is_some() {
                    return Err(DomainError::Invalid {
                        field: "index return_bps",
                        reason: "must accompany reference_close exactly",
                    });
                }
                if let (Some(price), Some(reference_close), Some(return_bps)) =
                    (self.price, self.reference_close, self.return_bps)
                {
                    let expected = (price.get() / reference_close.get() - 1.0) * 10_000.0;
                    if (expected - return_bps).abs() > 1e-6 {
                        return Err(DomainError::Invalid {
                            field: "index return_bps",
                            reason: "does not match price and reference_close",
                        });
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CrossIndexFrameV2 {
    pub frame_id: Token,
    pub trading_date_et: NaiveDate,
    pub session: ResearchSession,
    pub observed_through: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub feature_set_version: Token,
    pub status: CrossIndexFrameStatus,
    pub missing_instruments: Vec<CashIndex>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub source_skew_seconds: Option<NonNegativeF64>,
    pub source_skew_limit_seconds: PositiveF64,
    pub observations: Vec<IndexObservationV2>,
}

impl Validate for CrossIndexFrameV2 {
    fn validate(&self) -> Result<(), DomainError> {
        if self.observed_through > self.available_at {
            return Err(DomainError::TimeOrder(
                "cross-index observed_through is after available_at",
            ));
        }
        let instruments: Vec<CashIndex> = self
            .observations
            .iter()
            .map(|observation| observation.instrument)
            .collect();
        if instruments != CASH_INDEX_ORDER {
            return Err(DomainError::Invalid {
                field: "cross-index observations",
                reason: "must contain SPX, NDX, DJI, and RUT in canonical order",
            });
        }
        for observation in &self.observations {
            observation.validate()?;
            if observation.available_at > self.available_at {
                return Err(DomainError::TimeOrder(
                    "index observation is after frame available_at",
                ));
            }
            if observation
                .source_as_of
                .is_some_and(|source_as_of| source_as_of > self.observed_through)
            {
                return Err(DomainError::TimeOrder(
                    "index source is after frame observed_through",
                ));
            }
        }
        let expected_missing: Vec<CashIndex> = self
            .observations
            .iter()
            .filter(|observation| observation.status == ObservationStatus::Missing)
            .map(|observation| observation.instrument)
            .collect();
        if self.missing_instruments != expected_missing {
            return Err(DomainError::Invalid {
                field: "missing_instruments",
                reason: "does not match the observation statuses",
            });
        }
        let source_times: Vec<DateTime<Utc>> = self
            .observations
            .iter()
            .filter_map(|observation| observation.source_as_of)
            .collect();
        let expected_skew = if source_times.len() < 2 {
            None
        } else {
            let earliest = source_times
                .iter()
                .min()
                .expect("at least two source times");
            let latest = source_times
                .iter()
                .max()
                .expect("at least two source times");
            Some(
                (*latest - *earliest)
                    .to_std()
                    .map_err(|_| DomainError::Invalid {
                        field: "source_skew_seconds",
                        reason: "source time difference is negative",
                    })?
                    .as_secs_f64(),
            )
        };
        if match (self.source_skew_seconds, expected_skew) {
            (Some(actual), Some(expected)) => (actual.get() - expected).abs() > 1e-6,
            (None, None) => false,
            _ => true,
        } {
            return Err(DomainError::Invalid {
                field: "source_skew_seconds",
                reason: "does not match observation source timestamps",
            });
        }
        let expected_ready = expected_missing.is_empty()
            && self
                .observations
                .iter()
                .all(|observation| observation.quality == ResearchDataQuality::Live)
            && self
                .source_skew_seconds
                .is_some_and(|skew| skew.get() <= self.source_skew_limit_seconds.get());
        if (self.status == CrossIndexFrameStatus::Ready) != expected_ready {
            return Err(DomainError::Invalid {
                field: "cross-index status",
                reason: "does not match source completeness, quality, and skew",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PriorRthContextV2 {
    pub context_id: Token,
    pub schema_version: String,
    pub status: ResearchContextStatus,
    pub for_trading_date: NaiveDate,
    pub session_date: NaiveDate,
    pub source_as_of: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub return_bps: BTreeMap<CashIndex, Option<f64>>,
    pub reason_codes: Vec<Token>,
    pub semantics: PriorRthSemantics,
}

impl Validate for PriorRthContextV2 {
    fn validate(&self) -> Result<(), DomainError> {
        require_schema(
            &self.schema_version,
            PRIOR_RTH_CONTEXT_SCHEMA_VERSION,
            "prior RTH research context",
        )?;
        if self.session_date >= self.for_trading_date {
            return Err(DomainError::TimeOrder(
                "prior RTH session must precede the forecast trading date",
            ));
        }
        if self.source_as_of > self.available_at {
            return Err(DomainError::TimeOrder(
                "prior RTH source_as_of is after available_at",
            ));
        }
        if self.return_bps.keys().copied().collect::<BTreeSet<_>>()
            != CASH_INDEX_ORDER.into_iter().collect()
            || self
                .return_bps
                .values()
                .flatten()
                .any(|value| !value.is_finite())
        {
            return Err(DomainError::Invalid {
                field: "prior RTH returns",
                reason: "must contain finite-or-missing SPX, NDX, DJI, and RUT values",
            });
        }
        validate_reason_codes(&self.reason_codes, "prior RTH reason_codes")?;
        let available = self
            .return_bps
            .values()
            .filter(|value| value.is_some())
            .count();
        match self.status {
            ResearchContextStatus::Ready if available != CASH_INDEX_ORDER.len() => {
                return Err(DomainError::Invalid {
                    field: "prior RTH status",
                    reason: "ready context requires all four returns",
                });
            }
            ResearchContextStatus::Partial if available == 0 || self.reason_codes.is_empty() => {
                return Err(DomainError::Invalid {
                    field: "prior RTH status",
                    reason: "partial context requires at least one return and explicit reasons",
                });
            }
            ResearchContextStatus::Unavailable if self.reason_codes.is_empty() => {
                return Err(DomainError::Invalid {
                    field: "prior RTH status",
                    reason: "unavailable context requires explicit reasons",
                });
            }
            _ => {}
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FilteredRegimePosteriorV2 {
    pub signal_id: Token,
    pub frame_id: Token,
    pub model_version: Token,
    pub feature_set_version: Token,
    pub sequence_id: Token,
    pub trading_date_et: NaiveDate,
    pub observed_through: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub update_index: u32,
    pub inference: HmmInference,
    pub parameter_mode: HmmParameterMode,
    pub evidence_status: ResearchEvidenceStatus,
    pub use_scope: ResearchUseScope,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub trained_through_date: Option<NaiveDate>,
    pub posterior: Vec<RegimeProbabilityV1>,
    pub posterior_entropy: NonNegativeF64,
}

impl Validate for FilteredRegimePosteriorV2 {
    fn validate(&self) -> Result<(), DomainError> {
        if self.observed_through > self.available_at {
            return Err(DomainError::TimeOrder(
                "filtered regime observed_through is after available_at",
            ));
        }
        if self.update_index == 0
            || self.inference != HmmInference::Filtered
            || self.parameter_mode != HmmParameterMode::FixedBootstrap
            || self.evidence_status != ResearchEvidenceStatus::BootstrapUnvalidated
            || self.use_scope != ResearchUseScope::Advisory
            || self.trained_through_date.is_some()
        {
            return Err(DomainError::Invalid {
                field: "filtered regime semantics",
                reason: "must remain filtered, fixed-bootstrap, unvalidated, and advisory",
            });
        }
        if !(2..=16).contains(&self.posterior.len()) {
            return Err(DomainError::Invalid {
                field: "filtered regime posterior",
                reason: "must contain 2..=16 states",
            });
        }
        let state_ids: Vec<Token> = self
            .posterior
            .iter()
            .map(|state| state.state_id.clone())
            .collect();
        unique_tokens(&state_ids, "filtered regime state_id")?;
        if state_ids.windows(2).any(|pair| pair[0] > pair[1]) {
            return Err(DomainError::Invalid {
                field: "filtered regime posterior",
                reason: "states must be sorted",
            });
        }
        let probability_sum: f64 = self
            .posterior
            .iter()
            .map(|state| state.probability.get())
            .sum();
        if (probability_sum - 1.0).abs() > 1e-9 {
            return Err(DomainError::Invalid {
                field: "filtered regime posterior",
                reason: "probabilities must sum to one",
            });
        }
        let expected_entropy = -self
            .posterior
            .iter()
            .map(|state| state.probability.get())
            .filter(|probability| *probability > 0.0)
            .map(|probability| probability * probability.ln())
            .sum::<f64>();
        if (self.posterior_entropy.get() - expected_entropy).abs() > 1e-9 {
            return Err(DomainError::Invalid {
                field: "posterior_entropy",
                reason: "does not match the filtered posterior",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuantileBandV2 {
    pub p10: PositiveF64,
    pub p50: PositiveF64,
    pub p90: PositiveF64,
}

impl Validate for QuantileBandV2 {
    fn validate(&self) -> Result<(), DomainError> {
        if self.p10.get() >= self.p50.get() || self.p50.get() >= self.p90.get() {
            return Err(DomainError::Invalid {
                field: "forecast quantiles",
                reason: "must satisfy p10 < p50 < p90",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpxRangeForecastV2 {
    pub forecast_id: Token,
    pub target: ForecastTarget,
    pub status: ForecastStatus,
    pub observed_through: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub target_at: DateTime<Utc>,
    pub reason_codes: Vec<Token>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub distribution: Option<ForecastDistribution>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub quantiles: Option<QuantileBandV2>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub model_version: Option<Token>,
    pub evidence_status: ResearchEvidenceStatus,
    pub use_scope: ResearchUseScope,
}

impl Validate for SpxRangeForecastV2 {
    fn validate(&self) -> Result<(), DomainError> {
        if self.observed_through > self.available_at {
            return Err(DomainError::TimeOrder(
                "range forecast observed_through is after available_at",
            ));
        }
        validate_reason_codes(&self.reason_codes, "range forecast reason_codes")?;
        if self.evidence_status != ResearchEvidenceStatus::BootstrapUnvalidated
            || self.use_scope != ResearchUseScope::Advisory
        {
            return Err(DomainError::Invalid {
                field: "range forecast semantics",
                reason: "must remain unvalidated and advisory",
            });
        }
        match self.status {
            ForecastStatus::Unavailable => {
                if self.reason_codes.is_empty()
                    || self.distribution.is_some()
                    || self.quantiles.is_some()
                    || self.model_version.is_some()
                {
                    return Err(DomainError::Invalid {
                        field: "unavailable range forecast",
                        reason: "requires reasons and cannot claim model output",
                    });
                }
            }
            ForecastStatus::Available => {
                if self.target_at <= self.available_at
                    || self.distribution.is_none()
                    || self.quantiles.is_none()
                    || self.model_version.is_none()
                {
                    return Err(DomainError::Invalid {
                        field: "available range forecast",
                        reason: "requires a future target and complete model output",
                    });
                }
                self.quantiles.as_ref().expect("checked above").validate()?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CloseLocationDistributionV2 {
    pub status: ForecastStatus,
    pub observed_through: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub target_at: DateTime<Utc>,
    pub reason_codes: Vec<Token>,
    pub probabilities: BTreeMap<CloseLocationBucket, ProbabilityF64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub method_version: Option<Token>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub distribution: Option<ForecastDistribution>,
    pub bucket_definition: CloseLocationBucketDefinition,
    pub evidence_status: ResearchEvidenceStatus,
    pub use_scope: ResearchUseScope,
}

impl Validate for CloseLocationDistributionV2 {
    fn validate(&self) -> Result<(), DomainError> {
        if self.observed_through > self.available_at {
            return Err(DomainError::TimeOrder(
                "close-location observed_through is after available_at",
            ));
        }
        validate_reason_codes(&self.reason_codes, "close-location reason_codes")?;
        if self.evidence_status != ResearchEvidenceStatus::BootstrapUnvalidated
            || self.use_scope != ResearchUseScope::Advisory
        {
            return Err(DomainError::Invalid {
                field: "close-location semantics",
                reason: "must remain unvalidated and advisory",
            });
        }
        match self.status {
            ForecastStatus::Unavailable => {
                if self.reason_codes.is_empty()
                    || !self.probabilities.is_empty()
                    || self.method_version.is_some()
                    || self.distribution.is_some()
                {
                    return Err(DomainError::Invalid {
                        field: "unavailable close-location",
                        reason: "requires reasons and cannot claim probabilities",
                    });
                }
            }
            ForecastStatus::Available => {
                if self.target_at <= self.available_at
                    || self.method_version.is_none()
                    || self.distribution != Some(ForecastDistribution::ExperimentalHeuristic)
                    || self.probabilities.keys().copied().collect::<BTreeSet<_>>()
                        != CLOSE_LOCATION_BUCKETS.into_iter().collect()
                {
                    return Err(DomainError::Invalid {
                        field: "available close-location",
                        reason: "requires canonical heuristic probabilities and a future target",
                    });
                }
                let total: f64 = self
                    .probabilities
                    .values()
                    .map(|probability| probability.get())
                    .sum();
                if (total - 1.0).abs() > 1e-9 {
                    return Err(DomainError::Invalid {
                        field: "close-location probabilities",
                        reason: "must sum to one",
                    });
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchContextV2 {
    pub evidence_status: ResearchEvidenceStatus,
    pub use_scope: ResearchUseScope,
    pub action_authority: ActionAuthority,
    pub cross_index_frame: CrossIndexFrameV2,
    pub prior_rth_context: PriorRthContextV2,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub regime: Option<FilteredRegimePosteriorV2>,
    pub regime_reason_codes: Vec<Token>,
    pub forecasts: Vec<SpxRangeForecastV2>,
    pub close_location: CloseLocationDistributionV2,
}

impl ResearchContextV2 {
    fn validate(&self, generated_at: DateTime<Utc>) -> Result<(), DomainError> {
        if self.evidence_status != ResearchEvidenceStatus::BootstrapUnvalidated
            || self.use_scope != ResearchUseScope::Advisory
            || self.action_authority != ActionAuthority::None
        {
            return Err(DomainError::Invalid {
                field: "research context semantics",
                reason: "must remain bootstrap-unvalidated, advisory, and without action authority",
            });
        }
        self.cross_index_frame.validate()?;
        self.prior_rth_context.validate()?;
        if self.cross_index_frame.available_at > generated_at
            || self.prior_rth_context.available_at > generated_at
        {
            return Err(DomainError::TimeOrder(
                "research context predates an input context",
            ));
        }
        validate_reason_codes(&self.regime_reason_codes, "regime_reason_codes")?;
        match &self.regime {
            Some(regime) => {
                regime.validate()?;
                if regime.frame_id != self.cross_index_frame.frame_id {
                    return Err(DomainError::Invalid {
                        field: "filtered regime frame_id",
                        reason: "does not match the cross-index frame",
                    });
                }
                if regime.available_at > generated_at {
                    return Err(DomainError::TimeOrder(
                        "research context predates filtered regime output",
                    ));
                }
            }
            None if self.regime_reason_codes.is_empty() => {
                return Err(DomainError::Invalid {
                    field: "regime_reason_codes",
                    reason: "missing regime requires explicit reasons",
                });
            }
            None => {}
        }
        if self
            .forecasts
            .iter()
            .map(|forecast| forecast.target)
            .ne(FORECAST_TARGET_ORDER)
        {
            return Err(DomainError::Invalid {
                field: "research forecasts",
                reason: "must contain close, high, and low in canonical order",
            });
        }
        for forecast in &self.forecasts {
            forecast.validate()?;
            if forecast.available_at > generated_at {
                return Err(DomainError::TimeOrder(
                    "research context predates a range forecast",
                ));
            }
        }
        self.close_location.validate()?;
        if self.close_location.available_at > generated_at {
            return Err(DomainError::TimeOrder(
                "research context predates close-location output",
            ));
        }
        if self
            .forecasts
            .iter()
            .any(|forecast| forecast.target_at != self.close_location.target_at)
        {
            return Err(DomainError::Invalid {
                field: "research forecast targets",
                reason: "close, high, low, and close-location targets must align",
            });
        }
        self.validate_context_dates()?;
        if self.close_location.status == ForecastStatus::Available
            && self
                .forecasts
                .iter()
                .any(|forecast| forecast.status != ForecastStatus::Available)
        {
            return Err(DomainError::Invalid {
                field: "close-location status",
                reason: "available close-location requires all three range forecasts",
            });
        }
        Ok(())
    }

    fn validate_context_dates(&self) -> Result<(), DomainError> {
        let trading_date = self.cross_index_frame.trading_date_et;
        if self.prior_rth_context.for_trading_date != trading_date {
            return Err(DomainError::Invalid {
                field: "prior RTH for_trading_date",
                reason: "does not match the cross-index trading_date_et",
            });
        }
        if self
            .regime
            .as_ref()
            .is_some_and(|regime| regime.trading_date_et != trading_date)
        {
            return Err(DomainError::Invalid {
                field: "filtered regime trading_date_et",
                reason: "does not match the cross-index trading_date_et",
            });
        }
        if self
            .close_location
            .target_at
            .with_timezone(&New_York)
            .date_naive()
            != trading_date
        {
            return Err(DomainError::Invalid {
                field: "research forecast target_at",
                reason: "does not match the cross-index RTH trading_date_et",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExperimentalResearchSignalsV1 {
    #[serde(deserialize_with = "deserialize_required_option")]
    pub market_regime: Option<MarketRegimeSignalV1>,
    pub range_forecasts: Vec<RangeForecastV1>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
enum ResearchPayload {
    ExperimentalV1(ExperimentalResearchSignalsV1),
    ContextV2(Box<ResearchContextV2>),
}

/// Research ingress accepted by the stable `research_signals` envelope.
///
/// The name is retained for wire compatibility. The payload is selected strictly
/// by its complete v1 or v2 field set and then matched to `schema_version` during
/// validation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResearchSignalsV1 {
    pub schema_version: String,
    pub document_id: Token,
    pub generated_at: DateTime<Utc>,
    #[serde(flatten)]
    payload: Box<ResearchPayload>,
    pub automatic_ordering: bool,
}

impl ResearchSignalsV1 {
    pub fn market_regime(&self) -> Option<&MarketRegimeSignalV1> {
        match self.payload.as_ref() {
            ResearchPayload::ExperimentalV1(payload) => payload.market_regime.as_ref(),
            ResearchPayload::ContextV2(_) => None,
        }
    }

    pub fn range_forecasts(&self) -> &[RangeForecastV1] {
        match self.payload.as_ref() {
            ResearchPayload::ExperimentalV1(payload) => &payload.range_forecasts,
            ResearchPayload::ContextV2(_) => &[],
        }
    }

    pub fn context_v2(&self) -> Option<&ResearchContextV2> {
        match self.payload.as_ref() {
            ResearchPayload::ExperimentalV1(_) => None,
            ResearchPayload::ContextV2(payload) => Some(payload.as_ref()),
        }
    }
}

impl Validate for ResearchSignalsV1 {
    fn validate(&self) -> Result<(), DomainError> {
        if self.automatic_ordering {
            return Err(DomainError::Invalid {
                field: "automatic_ordering",
                reason: "research context cannot place orders",
            });
        }
        match (&*self.schema_version, self.payload.as_ref()) {
            (RESEARCH_SIGNALS_SCHEMA_VERSION, ResearchPayload::ExperimentalV1(payload)) => {
                self.validate_experimental_v1(payload)
            }
            (RESEARCH_CONTEXT_SCHEMA_VERSION, ResearchPayload::ContextV2(payload)) => {
                payload.validate(self.generated_at)
            }
            (RESEARCH_SIGNALS_SCHEMA_VERSION | RESEARCH_CONTEXT_SCHEMA_VERSION, _) => {
                Err(DomainError::Invalid {
                    field: "research payload",
                    reason: "field set does not match schema_version",
                })
            }
            _ => Err(DomainError::SchemaMismatch {
                kind: "research signals",
                expected: "experimental_research_signals.v1 or research_context.v2",
                actual: self.schema_version.clone(),
            }),
        }
    }
}

impl ResearchSignalsV1 {
    fn validate_experimental_v1(
        &self,
        payload: &ExperimentalResearchSignalsV1,
    ) -> Result<(), DomainError> {
        if payload.market_regime.is_none() && payload.range_forecasts.is_empty() {
            return Err(DomainError::Invalid {
                field: "research signals",
                reason: "at least one signal is required",
            });
        }
        if let Some(regime) = &payload.market_regime {
            regime.validate()?;
            if regime.available_at > self.generated_at {
                return Err(DomainError::TimeOrder(
                    "regime available_at is after research generated_at",
                ));
            }
            if self.generated_at >= regime.valid_until {
                return Err(DomainError::TimeOrder(
                    "research generated_at must be before regime valid_until",
                ));
            }
        }
        if payload.range_forecasts.len() > 3 {
            return Err(DomainError::Invalid {
                field: "range_forecasts",
                reason: "at most one forecast per supported kind is allowed",
            });
        }
        let mut kinds: Vec<RangeForecastKind> = payload
            .range_forecasts
            .iter()
            .map(|forecast| forecast.forecast_kind)
            .collect();
        kinds.sort();
        kinds.dedup();
        if kinds.len() != payload.range_forecasts.len() {
            return Err(DomainError::Duplicate("range forecast kind"));
        }
        for forecast in &payload.range_forecasts {
            forecast.validate()?;
            if forecast.available_at > self.generated_at {
                return Err(DomainError::TimeOrder(
                    "range available_at is after research generated_at",
                ));
            }
            if self.generated_at >= forecast.valid_until {
                return Err(DomainError::TimeOrder(
                    "research generated_at must be before range valid_until",
                ));
            }
        }
        if let Some(regime) = &payload.market_regime {
            if payload
                .range_forecasts
                .iter()
                .any(|forecast| regime.session != forecast.session)
            {
                return Err(DomainError::Invalid {
                    field: "research session",
                    reason: "regime and range signals must use the same session",
                });
            }
        } else if let Some(first) = payload.range_forecasts.first()
            && payload
                .range_forecasts
                .iter()
                .any(|forecast| forecast.session != first.session)
        {
            return Err(DomainError::Invalid {
                field: "research session",
                reason: "range signals must use the same session",
            });
        }
        Ok(())
    }
}

fn validate_reason_codes(reason_codes: &[Token], field: &'static str) -> Result<(), DomainError> {
    unique_tokens(reason_codes, field)?;
    if reason_codes.windows(2).any(|pair| pair[0] > pair[1]) {
        return Err(DomainError::Invalid {
            field,
            reason: "must be sorted",
        });
    }
    Ok(())
}

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone as _;

    use super::*;

    fn token(value: &str) -> Token {
        Token::new(value, "test token").unwrap()
    }

    fn regime() -> MarketRegimeSignalV1 {
        let at = Utc.with_ymd_and_hms(2026, 8, 3, 14, 0, 0).unwrap();
        MarketRegimeSignalV1 {
            signal_id: token("regime:1"),
            feature_set_version: token("features:v1"),
            model_version: token("hmm:v1"),
            lineage_id: token("lineage:1"),
            session: MarketSession::Rth,
            observed_through: at,
            available_at: at,
            valid_until: at + chrono::TimeDelta::minutes(5),
            inference_semantics: RegimeInferenceSemantics::CausalFiltered,
            use_scope: ResearchUseScope::Experimental,
            posterior: vec![
                RegimeProbabilityV1 {
                    state_id: token("state_0"),
                    probability: ProbabilityF64::new(0.4, "probability").unwrap(),
                },
                RegimeProbabilityV1 {
                    state_id: token("state_1"),
                    probability: ProbabilityF64::new(0.6, "probability").unwrap(),
                },
            ],
            posterior_entropy: NonNegativeF64::new(0.67, "entropy").unwrap(),
            observation_count: 3,
        }
    }

    #[test]
    fn posterior_must_sum_to_one() {
        let mut signal = regime();
        signal.posterior[1].probability = ProbabilityF64::new(0.5, "probability").unwrap();
        assert!(matches!(
            signal.validate(),
            Err(DomainError::Invalid {
                field: "regime posterior",
                reason: "probabilities must sum to one"
            })
        ));
    }

    #[test]
    fn posterior_entropy_is_bounded_by_state_count() {
        let mut signal = regime();
        signal.posterior_entropy = NonNegativeF64::new(1.0, "entropy").unwrap();
        assert!(signal.validate().is_err());
    }
}
