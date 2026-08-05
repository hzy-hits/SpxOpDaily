use chrono::{DateTime, FixedOffset, NaiveDate, SecondsFormat};
use serde::{Deserialize, Serialize, Serializer};

use crate::validation::{require_schema, unique_tokens};
use crate::{
    DomainError, MarketSession, NonNegativeF64, PositiveF64, ProbabilityF64,
    STRATEGY_DISTRIBUTION_FORECAST_SCHEMA_VERSION, Token, Validate,
};

const MAX_STRATEGY_CANDIDATES: usize = 2;
const VERTICAL_WIDTH_POINTS: f64 = 10.0;
const VERTICAL_WIDTH_TOLERANCE: f64 = 1e-9;
const FLOAT_ABSOLUTE_TOLERANCE: f64 = 1e-6;
const FLOAT_RELATIVE_TOLERANCE: f64 = 1e-9;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProbabilityMeasure {
    RiskNeutral,
    Physical,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProbabilityEventKind {
    TerminalAbove,
    TerminalBelow,
    TerminalBetween,
    UpperFirstTouch,
    LowerFirstTouch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EstimateStatus {
    Available,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EstimateQuality {
    Ready,
    Degraded,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DistributionCalibrationStatus {
    Uncalibrated,
    WalkForwardCalibrated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionSemantics {
    DisplayedQuoteReachProxy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NetPnlBasis {
    DisplayedQuoteReachProxy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NetPnlUnit {
    UsdPerOneSpread,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DistributionCandidateDirection {
    #[serde(rename = "call_vertical_10")]
    CallVertical10,
    #[serde(rename = "put_vertical_10")]
    PutVertical10,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShadowAction {
    NoTrade,
    ManualCandidate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DistributionForecastQuality {
    Ready,
    Degraded,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DistributionEvidenceStatus {
    ResearchUnvalidated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DistributionActionAuthority {
    None,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProbabilityEventDefinitionV1 {
    pub event_id: Token,
    pub kind: ProbabilityEventKind,
    #[serde(serialize_with = "serialize_python_datetime")]
    pub target_at: DateTime<FixedOffset>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub lower_level: Option<PositiveF64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub upper_level: Option<PositiveF64>,
}

impl Validate for ProbabilityEventDefinitionV1 {
    fn validate(&self) -> Result<(), DomainError> {
        match self.kind {
            ProbabilityEventKind::TerminalAbove => {
                if self.lower_level.is_none() || self.upper_level.is_some() {
                    return Err(DomainError::Invalid {
                        field: "probability event levels",
                        reason: "terminal_above requires only lower_level",
                    });
                }
            }
            ProbabilityEventKind::TerminalBelow => {
                if self.lower_level.is_some() || self.upper_level.is_none() {
                    return Err(DomainError::Invalid {
                        field: "probability event levels",
                        reason: "terminal_below requires only upper_level",
                    });
                }
            }
            ProbabilityEventKind::TerminalBetween
            | ProbabilityEventKind::UpperFirstTouch
            | ProbabilityEventKind::LowerFirstTouch => {
                let (Some(lower), Some(upper)) = (self.lower_level, self.upper_level) else {
                    return Err(DomainError::Invalid {
                        field: "probability event levels",
                        reason: "bounded event requires lower_level and upper_level",
                    });
                };
                if lower.get() >= upper.get() {
                    return Err(DomainError::Invalid {
                        field: "probability event levels",
                        reason: "must be strictly ordered",
                    });
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProbabilityEstimateV1 {
    pub measure: ProbabilityMeasure,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub event: Option<ProbabilityEventDefinitionV1>,
    pub status: EstimateStatus,
    pub quality: EstimateQuality,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub probability: Option<ProbabilityF64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub method_version: Option<Token>,
    pub reason_codes: Vec<Token>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub sample_count: Option<u64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub session_count: Option<u64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub interval_low: Option<ProbabilityF64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub interval_high: Option<ProbabilityF64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub trained_through_date: Option<NaiveDate>,
}

impl Validate for ProbabilityEstimateV1 {
    fn validate(&self) -> Result<(), DomainError> {
        if let Some(event) = &self.event {
            event.validate()?;
        }
        validate_reason_codes(&self.reason_codes, "probability reason_codes")?;
        self.validate_evidence_metadata()?;
        match self.status {
            EstimateStatus::Unavailable => {
                if self.quality != EstimateQuality::Unavailable {
                    return Err(DomainError::Invalid {
                        field: "probability quality",
                        reason: "unavailable probability quality must be unavailable",
                    });
                }
                if self.probability.is_some() {
                    return Err(DomainError::Invalid {
                        field: "probability",
                        reason: "unavailable probability must be null",
                    });
                }
                require_reasons(&self.reason_codes, "unavailable probability")?;
            }
            EstimateStatus::Available => {
                if self.quality == EstimateQuality::Unavailable {
                    return Err(DomainError::Invalid {
                        field: "probability quality",
                        reason: "available probability quality cannot be unavailable",
                    });
                }
                if self.event.is_none()
                    || self.probability.is_none()
                    || self.method_version.is_none()
                {
                    return Err(DomainError::Invalid {
                        field: "available probability",
                        reason: "requires event, probability, and method_version",
                    });
                }
                match self.quality {
                    EstimateQuality::Ready if !self.reason_codes.is_empty() => {
                        return Err(DomainError::Invalid {
                            field: "ready probability",
                            reason: "cannot contain degradation reasons",
                        });
                    }
                    EstimateQuality::Degraded if self.reason_codes.is_empty() => {
                        return Err(DomainError::Invalid {
                            field: "degraded probability",
                            reason: "requires reason_codes",
                        });
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }
}

impl ProbabilityEstimateV1 {
    fn validate_evidence_metadata(&self) -> Result<(), DomainError> {
        if self.interval_low.is_none() != self.interval_high.is_none() {
            return Err(DomainError::Invalid {
                field: "probability interval bounds",
                reason: "must be present together",
            });
        }
        if let (Some(low), Some(high)) = (self.interval_low, self.interval_high) {
            if low.get() > high.get() {
                return Err(DomainError::Invalid {
                    field: "probability interval bounds",
                    reason: "must be ordered",
                });
            }
            if self
                .probability
                .is_some_and(|probability| probability < low || probability > high)
            {
                return Err(DomainError::Invalid {
                    field: "probability",
                    reason: "must lie inside its interval",
                });
            }
        }
        match self.measure {
            ProbabilityMeasure::RiskNeutral => self.validate_risk_neutral_evidence(),
            ProbabilityMeasure::Physical => self.validate_physical_evidence(),
        }
    }

    fn validate_risk_neutral_evidence(&self) -> Result<(), DomainError> {
        if self.sample_count.is_some()
            || self.session_count.is_some()
            || self.interval_low.is_some()
            || self.interval_high.is_some()
            || self.trained_through_date.is_some()
        {
            return Err(DomainError::Invalid {
                field: "risk-neutral probability",
                reason: "cannot claim physical sample evidence",
            });
        }
        Ok(())
    }

    fn validate_physical_evidence(&self) -> Result<(), DomainError> {
        let (Some(sample_count), Some(session_count)) = (self.sample_count, self.session_count)
        else {
            return Err(DomainError::Invalid {
                field: "physical probability",
                reason: "requires sample_count and session_count",
            });
        };
        if session_count > sample_count {
            return Err(DomainError::Invalid {
                field: "physical probability session_count",
                reason: "cannot exceed sample_count",
            });
        }
        match self.status {
            EstimateStatus::Unavailable => {
                if self.interval_low.is_some()
                    || self.interval_high.is_some()
                    || self.trained_through_date.is_some()
                {
                    return Err(DomainError::Invalid {
                        field: "unavailable physical probability",
                        reason: "cannot claim interval or training date",
                    });
                }
            }
            EstimateStatus::Available => {
                if self.interval_low.is_none() || self.interval_high.is_none() {
                    return Err(DomainError::Invalid {
                        field: "available physical probability",
                        reason: "requires interval bounds",
                    });
                }
                if sample_count == 0 || session_count == 0 {
                    return Err(DomainError::Invalid {
                        field: "available physical probability",
                        reason: "requires positive sample evidence",
                    });
                }
                if self.trained_through_date.is_none() {
                    return Err(DomainError::Invalid {
                        field: "available physical probability",
                        reason: "requires trained_through_date",
                    });
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionEstimateV1 {
    pub status: EstimateStatus,
    pub execution_semantics: ExecutionSemantics,
    pub limit_debit_points: PositiveF64,
    pub wait_horizon_seconds: u32,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub quote_reach_probability: Option<ProbabilityF64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub actual_fill_probability: Option<ProbabilityF64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub model_version: Option<Token>,
    pub reason_codes: Vec<Token>,
}

impl Validate for ExecutionEstimateV1 {
    fn validate(&self) -> Result<(), DomainError> {
        validate_reason_codes(&self.reason_codes, "execution reason_codes")?;
        if self.wait_horizon_seconds == 0 {
            return Err(DomainError::Invalid {
                field: "execution wait_horizon_seconds",
                reason: "must be positive",
            });
        }
        if self.actual_fill_probability.is_some() {
            return Err(DomainError::Invalid {
                field: "actual_fill_probability",
                reason: "must remain null without order-at-risk evidence",
            });
        }
        match self.status {
            EstimateStatus::Unavailable => {
                if self.quote_reach_probability.is_some() {
                    return Err(DomainError::Invalid {
                        field: "quote_reach_probability",
                        reason: "unavailable quote-reach probability must be null",
                    });
                }
                require_reasons(&self.reason_codes, "unavailable execution")?;
            }
            EstimateStatus::Available => {
                if self.quote_reach_probability.is_none() || self.model_version.is_none() {
                    return Err(DomainError::Invalid {
                        field: "available execution",
                        reason: "requires quote-reach probability and model_version",
                    });
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NetPnlEstimateV1 {
    pub status: EstimateStatus,
    pub basis: NetPnlBasis,
    pub unit: NetPnlUnit,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub expected_net_pnl: Option<f64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub p10_net_pnl: Option<f64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub p50_net_pnl: Option<f64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub p90_net_pnl: Option<f64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub tail_loss_p10: Option<f64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub model_version: Option<Token>,
    pub reason_codes: Vec<Token>,
}

impl Validate for NetPnlEstimateV1 {
    fn validate(&self) -> Result<(), DomainError> {
        validate_reason_codes(&self.reason_codes, "net-PnL reason_codes")?;
        let values = [
            self.expected_net_pnl,
            self.p10_net_pnl,
            self.p50_net_pnl,
            self.p90_net_pnl,
            self.tail_loss_p10,
        ];
        match self.status {
            EstimateStatus::Unavailable => {
                if values.iter().any(Option::is_some) {
                    return Err(DomainError::Invalid {
                        field: "unavailable net-PnL",
                        reason: "all estimate fields must be null",
                    });
                }
                require_reasons(&self.reason_codes, "unavailable net-PnL")?;
            }
            EstimateStatus::Available => {
                if values.iter().any(Option::is_none) || self.model_version.is_none() {
                    return Err(DomainError::Invalid {
                        field: "available net-PnL",
                        reason: "requires all estimate fields and model_version",
                    });
                }
                if values.iter().flatten().any(|value| !value.is_finite()) {
                    return Err(DomainError::Invalid {
                        field: "available net-PnL",
                        reason: "estimate fields must be finite",
                    });
                }
                let p10 = self.p10_net_pnl.expect("availability checked");
                let p50 = self.p50_net_pnl.expect("availability checked");
                let p90 = self.p90_net_pnl.expect("availability checked");
                if p10 > p50 || p50 > p90 {
                    return Err(DomainError::Invalid {
                        field: "net-PnL quantiles",
                        reason: "must be nondecreasing",
                    });
                }
                if self.tail_loss_p10.expect("availability checked") < 0.0 {
                    return Err(DomainError::Invalid {
                        field: "tail_loss_p10",
                        reason: "must be non-negative",
                    });
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateScoreV1 {
    pub status: EstimateStatus,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub tail_risk_penalty: Option<f64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub model_uncertainty_penalty: Option<f64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub liquidity_risk_penalty: Option<f64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub total: Option<f64>,
    pub reason_codes: Vec<Token>,
}

impl CandidateScoreV1 {
    fn validate_against(&self, net_pnl: &NetPnlEstimateV1) -> Result<(), DomainError> {
        if self.status != EstimateStatus::Available || net_pnl.status != EstimateStatus::Available {
            return Err(DomainError::Invalid {
                field: "candidate score",
                reason: "available score requires available net-PnL",
            });
        }
        let expected = net_pnl.expected_net_pnl.expect("availability checked")
            - self.tail_risk_penalty.expect("availability checked")
            - self
                .model_uncertainty_penalty
                .expect("availability checked")
            - self.liquidity_risk_penalty.expect("availability checked");
        let total = self.total.expect("availability checked");
        if !approximately_equal(total, expected) {
            return Err(DomainError::Invalid {
                field: "candidate score total",
                reason: "does not reconcile",
            });
        }
        Ok(())
    }
}

impl Validate for CandidateScoreV1 {
    fn validate(&self) -> Result<(), DomainError> {
        validate_reason_codes(&self.reason_codes, "candidate score reason_codes")?;
        let values = [
            self.tail_risk_penalty,
            self.model_uncertainty_penalty,
            self.liquidity_risk_penalty,
            self.total,
        ];
        match self.status {
            EstimateStatus::Unavailable => {
                if values.iter().any(Option::is_some) {
                    return Err(DomainError::Invalid {
                        field: "unavailable candidate score",
                        reason: "all score fields must be null",
                    });
                }
                require_reasons(&self.reason_codes, "unavailable candidate score")?;
            }
            EstimateStatus::Available => {
                if values.iter().any(Option::is_none) {
                    return Err(DomainError::Invalid {
                        field: "available candidate score",
                        reason: "requires all score fields",
                    });
                }
                if values.iter().flatten().any(|value| !value.is_finite()) {
                    return Err(DomainError::Invalid {
                        field: "available candidate score",
                        reason: "score fields must be finite",
                    });
                }
                if [
                    self.tail_risk_penalty.expect("availability checked"),
                    self.model_uncertainty_penalty
                        .expect("availability checked"),
                    self.liquidity_risk_penalty.expect("availability checked"),
                ]
                .iter()
                .any(|value| *value < 0.0)
                {
                    return Err(DomainError::Invalid {
                        field: "candidate score penalties",
                        reason: "must be non-negative",
                    });
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrategyDistributionCandidateV1 {
    pub candidate_id: Token,
    pub probability_event_id: Token,
    pub direction: DistributionCandidateDirection,
    pub expiry: NaiveDate,
    pub long_contract_id: Token,
    pub short_contract_id: Token,
    pub long_strike: PositiveF64,
    pub short_strike: PositiveF64,
    pub execution: ExecutionEstimateV1,
    pub net_pnl: NetPnlEstimateV1,
    pub score: CandidateScoreV1,
    pub reason_codes: Vec<Token>,
}

impl Validate for StrategyDistributionCandidateV1 {
    fn validate(&self) -> Result<(), DomainError> {
        self.execution.validate()?;
        self.net_pnl.validate()?;
        self.score.validate()?;
        validate_reason_codes(&self.reason_codes, "strategy candidate reason_codes")?;
        if self.long_contract_id == self.short_contract_id {
            return Err(DomainError::Invalid {
                field: "strategy candidate contract ids",
                reason: "must differ",
            });
        }
        let width = match self.direction {
            DistributionCandidateDirection::CallVertical10 => {
                self.short_strike.get() - self.long_strike.get()
            }
            DistributionCandidateDirection::PutVertical10 => {
                self.long_strike.get() - self.short_strike.get()
            }
        };
        if (width - VERTICAL_WIDTH_POINTS).abs() > VERTICAL_WIDTH_TOLERANCE {
            return Err(DomainError::Invalid {
                field: "strategy candidate strikes",
                reason: "must form an exact 10-point debit vertical",
            });
        }
        if self.execution.limit_debit_points.get() >= VERTICAL_WIDTH_POINTS {
            return Err(DomainError::Invalid {
                field: "execution limit_debit_points",
                reason: "must be below vertical width",
            });
        }
        if self.net_pnl.status == EstimateStatus::Available
            && self.execution.status != EstimateStatus::Available
        {
            return Err(DomainError::Invalid {
                field: "available net-PnL",
                reason: "requires available execution estimate",
            });
        }
        if self.score.status == EstimateStatus::Available {
            self.score.validate_against(&self.net_pnl)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShadowDecisionV1 {
    pub action: ShadowAction,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub selected_candidate_id: Option<Token>,
    pub score_threshold: NonNegativeF64,
    pub reason_codes: Vec<Token>,
}

impl Validate for ShadowDecisionV1 {
    fn validate(&self) -> Result<(), DomainError> {
        validate_reason_codes(&self.reason_codes, "shadow decision reason_codes")?;
        match self.action {
            ShadowAction::NoTrade => {
                if self.selected_candidate_id.is_some() {
                    return Err(DomainError::Invalid {
                        field: "NO_TRADE",
                        reason: "cannot select a candidate",
                    });
                }
                require_reasons(&self.reason_codes, "NO_TRADE")?;
            }
            ShadowAction::ManualCandidate => {
                if self.selected_candidate_id.is_none() {
                    return Err(DomainError::Invalid {
                        field: "MANUAL_CANDIDATE",
                        reason: "requires selected_candidate_id",
                    });
                }
                if !self.reason_codes.is_empty() {
                    return Err(DomainError::Invalid {
                        field: "MANUAL_CANDIDATE",
                        reason: "cannot contain block reasons",
                    });
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrategyDistributionForecastV1 {
    pub schema_version: String,
    pub document_id: Token,
    pub source_snapshot_id: Token,
    pub trading_date_et: NaiveDate,
    pub session: MarketSession,
    #[serde(serialize_with = "serialize_python_datetime")]
    pub observed_through: DateTime<FixedOffset>,
    #[serde(serialize_with = "serialize_python_datetime")]
    pub available_at: DateTime<FixedOffset>,
    #[serde(serialize_with = "serialize_python_datetime")]
    pub valid_until: DateTime<FixedOffset>,
    pub model_version: Token,
    pub feature_set_version: Token,
    pub calibration_status: DistributionCalibrationStatus,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub calibration_version: Option<Token>,
    pub policy_version: Token,
    pub evidence_status: DistributionEvidenceStatus,
    pub q_event: ProbabilityEstimateV1,
    pub p_event: ProbabilityEstimateV1,
    pub strategy_candidates: Vec<StrategyDistributionCandidateV1>,
    pub shadow_decision: ShadowDecisionV1,
    pub quality: DistributionForecastQuality,
    pub quality_reason_codes: Vec<Token>,
    pub action_authority: DistributionActionAuthority,
    pub automatic_ordering: bool,
}

impl StrategyDistributionForecastV1 {
    fn validate_identity_time_and_calibration(&self) -> Result<(), DomainError> {
        require_schema(
            &self.schema_version,
            STRATEGY_DISTRIBUTION_FORECAST_SCHEMA_VERSION,
            "strategy distribution forecast",
        )?;
        if self.observed_through > self.available_at {
            return Err(DomainError::TimeOrder(
                "forecast observed_through is after available_at",
            ));
        }
        if self.valid_until <= self.available_at {
            return Err(DomainError::TimeOrder(
                "forecast valid_until must be after available_at",
            ));
        }
        match self.calibration_status {
            DistributionCalibrationStatus::Uncalibrated => {
                if self.calibration_version.is_some() {
                    return Err(DomainError::Invalid {
                        field: "calibration_version",
                        reason: "uncalibrated forecast cannot claim a version",
                    });
                }
            }
            DistributionCalibrationStatus::WalkForwardCalibrated => {
                if self.calibration_version.is_none() {
                    return Err(DomainError::Invalid {
                        field: "calibration_version",
                        reason: "walk-forward calibrated forecast requires a version",
                    });
                }
            }
        }
        Ok(())
    }

    fn validate_probability_pair(&self) -> Result<(), DomainError> {
        self.q_event.validate()?;
        self.p_event.validate()?;
        if self.q_event.measure != ProbabilityMeasure::RiskNeutral {
            return Err(DomainError::Invalid {
                field: "q_event measure",
                reason: "must be risk_neutral",
            });
        }
        if self.p_event.measure != ProbabilityMeasure::Physical {
            return Err(DomainError::Invalid {
                field: "p_event measure",
                reason: "must be physical",
            });
        }
        if self.q_event.event != self.p_event.event {
            return Err(DomainError::Invalid {
                field: "q_event and p_event",
                reason: "must describe the exact same event",
            });
        }
        if let Some(event) = &self.q_event.event {
            if event.target_at <= self.observed_through {
                return Err(DomainError::TimeOrder(
                    "probability event target must be after observed_through",
                ));
            }
        } else if self.q_event.status != EstimateStatus::Unavailable
            || self.p_event.status != EstimateStatus::Unavailable
        {
            return Err(DomainError::Invalid {
                field: "missing probability event",
                reason: "requires unavailable q_event and p_event",
            });
        }
        if self
            .p_event
            .trained_through_date
            .is_some_and(|date| date >= self.trading_date_et)
        {
            return Err(DomainError::Invalid {
                field: "physical probability training",
                reason: "must precede trading_date_et",
            });
        }
        Ok(())
    }

    fn validate_candidates(&self) -> Result<(), DomainError> {
        if self.strategy_candidates.len() > MAX_STRATEGY_CANDIDATES {
            return Err(DomainError::Invalid {
                field: "strategy_candidates",
                reason: "exceeds the bounded contract",
            });
        }
        let candidate_ids: Vec<Token> = self
            .strategy_candidates
            .iter()
            .map(|candidate| candidate.candidate_id.clone())
            .collect();
        validate_sorted_tokens(&candidate_ids, "strategy candidate ids")?;
        for candidate in &self.strategy_candidates {
            candidate.validate()?;
            let Some(event) = &self.q_event.event else {
                return Err(DomainError::Invalid {
                    field: "strategy candidate",
                    reason: "requires a probability event",
                });
            };
            if candidate.probability_event_id != event.event_id {
                return Err(DomainError::Invalid {
                    field: "strategy candidate probability_event_id",
                    reason: "references a different probability event",
                });
            }
            if candidate.expiry != self.trading_date_et {
                return Err(DomainError::Invalid {
                    field: "strategy candidate expiry",
                    reason: "0DTE expiry must match trading_date_et",
                });
            }
        }
        Ok(())
    }

    fn validate_quality(&self) -> Result<(), DomainError> {
        self.shadow_decision.validate()?;
        validate_reason_codes(&self.quality_reason_codes, "forecast quality_reason_codes")?;
        match self.quality {
            DistributionForecastQuality::Ready => {
                if !self.quality_reason_codes.is_empty() {
                    return Err(DomainError::Invalid {
                        field: "ready forecast",
                        reason: "cannot contain quality reasons",
                    });
                }
                if self.q_event.status != EstimateStatus::Available
                    || self.q_event.quality != EstimateQuality::Ready
                    || self.p_event.status != EstimateStatus::Available
                    || self.p_event.quality != EstimateQuality::Ready
                {
                    return Err(DomainError::Invalid {
                        field: "ready forecast",
                        reason: "requires ready q_event and p_event",
                    });
                }
            }
            DistributionForecastQuality::Degraded | DistributionForecastQuality::Unavailable => {
                require_reasons(&self.quality_reason_codes, "non-ready forecast")?;
            }
        }
        Ok(())
    }

    fn validate_shadow_selection(&self) -> Result<(), DomainError> {
        let selected = self
            .shadow_decision
            .selected_candidate_id
            .as_ref()
            .and_then(|id| {
                self.strategy_candidates
                    .iter()
                    .find(|candidate| candidate.candidate_id == *id)
            });
        match self.shadow_decision.action {
            ShadowAction::ManualCandidate => {
                if self.quality != DistributionForecastQuality::Ready {
                    return Err(DomainError::Invalid {
                        field: "MANUAL_CANDIDATE",
                        reason: "requires READY quality",
                    });
                }
                let Some(candidate) = selected else {
                    return Err(DomainError::Invalid {
                        field: "MANUAL_CANDIDATE",
                        reason: "selected candidate is not in strategy_candidates",
                    });
                };
                if candidate.execution.status != EstimateStatus::Available
                    || candidate.net_pnl.status != EstimateStatus::Available
                    || candidate.score.status != EstimateStatus::Available
                {
                    return Err(DomainError::Invalid {
                        field: "MANUAL_CANDIDATE",
                        reason: "requires available execution, net-PnL, and score",
                    });
                }
                if candidate.score.total.expect("availability checked")
                    <= self.shadow_decision.score_threshold.get()
                {
                    return Err(DomainError::Invalid {
                        field: "MANUAL_CANDIDATE score",
                        reason: "must exceed threshold",
                    });
                }
            }
            ShadowAction::NoTrade => {
                if selected.is_some() {
                    return Err(DomainError::Invalid {
                        field: "NO_TRADE",
                        reason: "cannot resolve a selected candidate",
                    });
                }
            }
        }
        if self.strategy_candidates.is_empty()
            && self.shadow_decision.action != ShadowAction::NoTrade
        {
            return Err(DomainError::Invalid {
                field: "empty strategy_candidates",
                reason: "requires NO_TRADE",
            });
        }
        Ok(())
    }
}

impl Validate for StrategyDistributionForecastV1 {
    fn validate(&self) -> Result<(), DomainError> {
        self.validate_identity_time_and_calibration()?;
        self.validate_probability_pair()?;
        self.validate_candidates()?;
        self.validate_quality()?;
        self.validate_shadow_selection()?;
        if self.automatic_ordering {
            return Err(DomainError::Invalid {
                field: "automatic_ordering",
                reason: "strategy distribution forecast cannot authorize ordering",
            });
        }
        Ok(())
    }
}

fn validate_reason_codes(reason_codes: &[Token], field: &'static str) -> Result<(), DomainError> {
    validate_sorted_tokens(reason_codes, field)
}

fn validate_sorted_tokens(values: &[Token], field: &'static str) -> Result<(), DomainError> {
    unique_tokens(values, field)?;
    if values.windows(2).any(|pair| pair[0] > pair[1]) {
        return Err(DomainError::Invalid {
            field,
            reason: "must be sorted",
        });
    }
    Ok(())
}

fn require_reasons(reason_codes: &[Token], field: &'static str) -> Result<(), DomainError> {
    if reason_codes.is_empty() {
        return Err(DomainError::Invalid {
            field,
            reason: "requires reason_codes",
        });
    }
    Ok(())
}

fn approximately_equal(left: f64, right: f64) -> bool {
    let tolerance =
        FLOAT_ABSOLUTE_TOLERANCE.max(FLOAT_RELATIVE_TOLERANCE * left.abs().max(right.abs()));
    (left - right).abs() <= tolerance
}

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

fn serialize_python_datetime<S>(
    value: &DateTime<FixedOffset>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&value.to_rfc3339_opts(SecondsFormat::AutoSi, false))
}
