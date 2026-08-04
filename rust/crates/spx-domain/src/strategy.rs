use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

use crate::validation::{require_schema, unique_tokens};
use crate::{
    DECISION_SCHEMA_VERSION, DomainError, EVALUATION_SCHEMA_VERSION, MarketSession, NonNegativeF64,
    OptionRight, PositiveF64, Provider, Token, Validate,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateDirection {
    CallVertical10,
    PutVertical10,
}

impl CandidateDirection {
    pub const fn option_right(self) -> OptionRight {
        match self {
            Self::CallVertical10 => OptionRight::Call,
            Self::PutVertical10 => OptionRight::Put,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CalendarState {
    Open,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MacroPermission {
    Allowed,
    Blocked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanState {
    Clear,
    Conflict,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvaluationRequestV1 {
    pub schema_version: String,
    pub request_id: Token,
    pub strategy_id: Token,
    pub policy_version: Token,
    pub session: MarketSession,
    pub session_date: NaiveDate,
    pub decision_at: DateTime<Utc>,
    pub valid_until: DateTime<Utc>,
    pub direction: CandidateDirection,
    pub long_contract_id: Token,
    pub short_contract_id: Token,
    pub calendar: CalendarState,
    pub macro_permission: MacroPermission,
    pub plan_state: PlanState,
    pub notification_targets: Vec<Token>,
    pub automatic_ordering: bool,
}

impl Validate for EvaluationRequestV1 {
    fn validate(&self) -> Result<(), DomainError> {
        require_schema(
            &self.schema_version,
            EVALUATION_SCHEMA_VERSION,
            "evaluation request",
        )?;
        if self.valid_until <= self.decision_at {
            return Err(DomainError::TimeOrder(
                "valid_until must be after decision_at",
            ));
        }
        if self.long_contract_id == self.short_contract_id {
            return Err(DomainError::Invalid {
                field: "contract ids",
                reason: "long and short legs must differ",
            });
        }
        unique_tokens(&self.notification_targets, "notification target")?;
        if self.notification_targets.is_empty() {
            return Err(DomainError::Invalid {
                field: "notification_targets",
                reason: "production evaluation requires at least one target",
            });
        }
        if self.automatic_ordering {
            return Err(DomainError::Invalid {
                field: "automatic_ordering",
                reason: "automatic ordering is forbidden",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyAction {
    NoTrade,
    ManualCandidate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyBlockReason {
    CalendarClosed,
    MacroEventBlocked,
    ActivePlanConflict,
    TtlExpired,
    DecisionTtlInvalid,
    EvaluationDelayed,
    NotificationTargetUnavailable,
    ProviderNotReady,
    ProviderExternalSessionOwns,
    ExactLegMissing,
    ExactLegWrongProvider,
    ExactLegNotLive,
    ExactLegOneSided,
    ExactLegLockedOrCrossed,
    ExactLegStale,
    ExactLegSkew,
    ExactLegContractMismatch,
    ExactLegExpiryMismatch,
    InvalidVertical,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExactLegEvidenceV1 {
    pub provider: Provider,
    pub long_contract_id: Token,
    pub short_contract_id: Token,
    pub right: OptionRight,
    pub long_strike: PositiveF64,
    pub short_strike: PositiveF64,
    pub long_bid: PositiveF64,
    pub long_ask: PositiveF64,
    pub short_bid: PositiveF64,
    pub short_ask: PositiveF64,
    pub max_age_seconds: NonNegativeF64,
    pub max_skew_seconds: NonNegativeF64,
    pub observed_at: DateTime<Utc>,
}

impl Validate for ExactLegEvidenceV1 {
    fn validate(&self) -> Result<(), DomainError> {
        if self.long_contract_id == self.short_contract_id {
            return Err(DomainError::Invalid {
                field: "exact leg evidence",
                reason: "long and short contract ids must differ",
            });
        }
        if self.long_ask.get() <= self.long_bid.get()
            || self.short_ask.get() <= self.short_bid.get()
        {
            return Err(DomainError::Invalid {
                field: "exact leg evidence",
                reason: "exact NBBO must be unlocked and uncrossed",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrategyDecisionV1 {
    pub schema_version: String,
    pub decision_id: Token,
    pub request_id: Token,
    pub strategy_id: Token,
    pub policy_version: Token,
    pub snapshot_id: Token,
    pub action: StrategyAction,
    pub direction: Option<CandidateDirection>,
    pub evaluated_at: DateTime<Utc>,
    pub valid_until: DateTime<Utc>,
    pub block_reasons: Vec<StrategyBlockReason>,
    pub exact_legs: Option<ExactLegEvidenceV1>,
    pub evidence_hash: Token,
    pub automatic_ordering: bool,
}

impl Validate for StrategyDecisionV1 {
    fn validate(&self) -> Result<(), DomainError> {
        require_schema(
            &self.schema_version,
            DECISION_SCHEMA_VERSION,
            "strategy decision",
        )?;
        if self.valid_until <= self.evaluated_at {
            return Err(DomainError::TimeOrder(
                "decision valid_until must be after evaluated_at",
            ));
        }
        let mut reasons = self.block_reasons.clone();
        reasons.sort();
        reasons.dedup();
        if reasons.len() != self.block_reasons.len() || reasons != self.block_reasons {
            return Err(DomainError::Invalid {
                field: "block_reasons",
                reason: "must be unique and sorted",
            });
        }
        if self.automatic_ordering {
            return Err(DomainError::Invalid {
                field: "automatic_ordering",
                reason: "automatic ordering is forbidden",
            });
        }
        match self.action {
            StrategyAction::NoTrade => {
                if self.direction.is_some() || self.exact_legs.is_some() {
                    return Err(DomainError::Invalid {
                        field: "NO_TRADE",
                        reason: "cannot contain direction or exact legs",
                    });
                }
                if self.block_reasons.is_empty() {
                    return Err(DomainError::Invalid {
                        field: "NO_TRADE",
                        reason: "requires at least one block reason",
                    });
                }
            }
            StrategyAction::ManualCandidate => {
                if self.direction.is_none() || self.exact_legs.is_none() {
                    return Err(DomainError::Invalid {
                        field: "MANUAL_CANDIDATE",
                        reason: "requires direction and exact-leg evidence",
                    });
                }
                if !self.block_reasons.is_empty() {
                    return Err(DomainError::Invalid {
                        field: "MANUAL_CANDIDATE",
                        reason: "cannot contain block reasons",
                    });
                }
                let direction = self.direction.expect("checked above");
                let legs = self.exact_legs.as_ref().expect("checked above");
                legs.validate()?;
                if legs.right != direction.option_right() {
                    return Err(DomainError::Invalid {
                        field: "exact leg evidence",
                        reason: "option right must match candidate direction",
                    });
                }
                let width = match direction {
                    CandidateDirection::CallVertical10 => {
                        legs.short_strike.get() - legs.long_strike.get()
                    }
                    CandidateDirection::PutVertical10 => {
                        legs.long_strike.get() - legs.short_strike.get()
                    }
                };
                if (width - 10.0).abs() > 1e-9 {
                    return Err(DomainError::Invalid {
                        field: "exact leg evidence",
                        reason: "vertical width must be exactly 10 points",
                    });
                }
                let executable_debit = legs.long_ask.get() - legs.short_bid.get();
                if !(0.0..10.0).contains(&executable_debit) || executable_debit == 0.0 {
                    return Err(DomainError::Invalid {
                        field: "exact leg evidence",
                        reason: "executable debit must be within zero and spread width",
                    });
                }
                if legs.observed_at > self.evaluated_at {
                    return Err(DomainError::TimeOrder(
                        "exact leg evidence is after decision evaluated_at",
                    ));
                }
            }
        }
        Ok(())
    }
}
