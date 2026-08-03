use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::validation::{require_schema, unique_tokens};
use crate::{
    DomainError, MarketSession, NonNegativeF64, PositiveF64, ProbabilityF64,
    RESEARCH_SIGNALS_SCHEMA_VERSION, Token, Validate,
};

const POSTERIOR_SUM_TOLERANCE: f64 = 1e-6;
const ENTROPY_TOLERANCE: f64 = 1e-9;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResearchUseScope {
    Experimental,
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchSignalsV1 {
    pub schema_version: String,
    pub document_id: Token,
    pub generated_at: DateTime<Utc>,
    pub market_regime: Option<MarketRegimeSignalV1>,
    pub range_forecasts: Vec<RangeForecastV1>,
    pub automatic_ordering: bool,
}

impl Validate for ResearchSignalsV1 {
    fn validate(&self) -> Result<(), DomainError> {
        require_schema(
            &self.schema_version,
            RESEARCH_SIGNALS_SCHEMA_VERSION,
            "experimental research signals",
        )?;
        if self.automatic_ordering {
            return Err(DomainError::Invalid {
                field: "automatic_ordering",
                reason: "experimental research context cannot place orders",
            });
        }
        if self.market_regime.is_none() && self.range_forecasts.is_empty() {
            return Err(DomainError::Invalid {
                field: "research signals",
                reason: "at least one signal is required",
            });
        }
        if let Some(regime) = &self.market_regime {
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
        if self.range_forecasts.len() > 3 {
            return Err(DomainError::Invalid {
                field: "range_forecasts",
                reason: "at most one forecast per supported kind is allowed",
            });
        }
        let mut kinds: Vec<RangeForecastKind> = self
            .range_forecasts
            .iter()
            .map(|forecast| forecast.forecast_kind)
            .collect();
        kinds.sort();
        kinds.dedup();
        if kinds.len() != self.range_forecasts.len() {
            return Err(DomainError::Duplicate("range forecast kind"));
        }
        for forecast in &self.range_forecasts {
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
        if let Some(regime) = &self.market_regime {
            if self
                .range_forecasts
                .iter()
                .any(|forecast| regime.session != forecast.session)
            {
                return Err(DomainError::Invalid {
                    field: "research session",
                    reason: "regime and range signals must use the same session",
                });
            }
        } else if let Some(first) = self.range_forecasts.first()
            && self
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
