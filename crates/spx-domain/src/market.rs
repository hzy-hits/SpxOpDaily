use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

use crate::validation::{require_schema, unique_tokens};
use crate::{
    ANALYTICAL_SNAPSHOT_SCHEMA_VERSION, DomainError, NonNegativeF64, PROVIDER_STATE_SCHEMA_VERSION,
    PositiveF64, QUOTE_BATCH_SCHEMA_VERSION, Token, Validate,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Provider {
    Schwab,
    Ibkr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// SPX/SPXW decision window, not a provider venue-session label.
///
/// CME Globex belongs to futures venue metadata. Market closure is represented by the
/// independent strategy calendar state, so neither concept is a legal variant here.
pub enum MarketSession {
    Rth,
    Gth,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuoteBatchMode {
    Incremental,
    ReplaceProviderSnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationalState {
    Starting,
    Live,
    Degraded,
    ExternalSessionOwns,
    Backoff,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransportState {
    Connected,
    Disconnected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthenticationState {
    Authenticated,
    Unauthenticated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntitlementState {
    Live,
    Delayed,
    Frozen,
    Missing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderReasonCode {
    Healthy,
    Starting,
    TransportDisconnected,
    AuthenticationUnavailable,
    EntitlementUnavailable,
    CompetingSession10197,
    StaleFlush,
    RateLimited,
    ProviderError,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderStateV1 {
    pub schema_version: String,
    pub provider: Provider,
    pub observed_at: DateTime<Utc>,
    pub operational: OperationalState,
    pub transport: TransportState,
    pub authentication: AuthenticationState,
    pub entitlement: EntitlementState,
    pub reason_codes: Vec<ProviderReasonCode>,
    pub latency_ms: Option<NonNegativeF64>,
    pub connection_generation: u64,
}

impl ProviderStateV1 {
    pub fn is_live(&self) -> bool {
        self.operational == OperationalState::Live
            && self.transport == TransportState::Connected
            && self.authentication == AuthenticationState::Authenticated
            && self.entitlement == EntitlementState::Live
    }
}

impl Validate for ProviderStateV1 {
    fn validate(&self) -> Result<(), DomainError> {
        require_schema(
            &self.schema_version,
            PROVIDER_STATE_SCHEMA_VERSION,
            "provider state",
        )?;
        let mut reasons = self.reason_codes.clone();
        reasons.sort();
        reasons.dedup();
        if reasons.len() != self.reason_codes.len() {
            return Err(DomainError::Duplicate("provider reason_codes"));
        }
        if self.operational == OperationalState::ExternalSessionOwns
            && !self
                .reason_codes
                .contains(&ProviderReasonCode::CompetingSession10197)
        {
            return Err(DomainError::Invalid {
                field: "provider reason_codes",
                reason: "external session ownership requires 10197 evidence",
            });
        }
        if self.operational == OperationalState::ExternalSessionOwns
            && self.provider != Provider::Ibkr
        {
            return Err(DomainError::Invalid {
                field: "provider",
                reason: "external session ownership applies only to IBKR",
            });
        }
        if self.connection_generation == 0 {
            return Err(DomainError::Invalid {
                field: "connection_generation",
                reason: "must be positive",
            });
        }
        if self.operational == OperationalState::Live && !self.is_live() {
            return Err(DomainError::Invalid {
                field: "provider state",
                reason: "live operational state requires live transport and entitlement",
            });
        }
        if self.is_live() && self.reason_codes != [ProviderReasonCode::Healthy] {
            return Err(DomainError::Invalid {
                field: "provider reason_codes",
                reason: "live provider must contain only healthy",
            });
        }
        if !self.is_live() && self.reason_codes.contains(&ProviderReasonCode::Healthy) {
            return Err(DomainError::Invalid {
                field: "provider reason_codes",
                reason: "healthy reason requires a live provider",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuoteQuality {
    Live,
    Delayed,
    Frozen,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstrumentKind {
    Index,
    Equity,
    Future,
    Option,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OptionRight {
    #[serde(rename = "C")]
    Call,
    #[serde(rename = "P")]
    Put,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OptionContractV1 {
    pub contract_id: Token,
    pub underlier: Token,
    pub trading_class: Token,
    pub expiry: NaiveDate,
    pub strike: PositiveF64,
    pub right: OptionRight,
    pub multiplier: u32,
}

impl Validate for OptionContractV1 {
    fn validate(&self) -> Result<(), DomainError> {
        if self.multiplier == 0 {
            return Err(DomainError::Invalid {
                field: "multiplier",
                reason: "must be positive",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BookSideV1 {
    pub price: PositiveF64,
    pub source_at: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
}

impl Validate for BookSideV1 {
    fn validate(&self) -> Result<(), DomainError> {
        if self.received_at < self.source_at {
            return Err(DomainError::TimeOrder(
                "book side received_at precedes source_at",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuoteV1 {
    pub quote_id: Token,
    pub provider: Provider,
    pub instrument_id: Token,
    pub instrument_kind: InstrumentKind,
    pub market_session: MarketSession,
    pub quality: QuoteQuality,
    pub option: Option<OptionContractV1>,
    pub bid: Option<BookSideV1>,
    pub ask: Option<BookSideV1>,
    pub last: Option<BookSideV1>,
}

impl QuoteV1 {
    pub fn exact_contract_id(&self) -> Option<&Token> {
        self.option.as_ref().map(|option| &option.contract_id)
    }
}

impl Validate for QuoteV1 {
    fn validate(&self) -> Result<(), DomainError> {
        if self.bid.is_none() && self.ask.is_none() && self.last.is_none() {
            return Err(DomainError::Invalid {
                field: "quote",
                reason: "at least one observed price is required",
            });
        }
        for side in [&self.bid, &self.ask, &self.last].into_iter().flatten() {
            side.validate()?;
        }
        match (self.instrument_kind, &self.option) {
            (InstrumentKind::Option, Some(option)) => option.validate()?,
            (InstrumentKind::Option, None) => {
                return Err(DomainError::Invalid {
                    field: "option",
                    reason: "option instrument requires a contract",
                });
            }
            (_, Some(_)) => {
                return Err(DomainError::Invalid {
                    field: "option",
                    reason: "non-option instrument cannot contain a contract",
                });
            }
            (_, None) => {}
        }
        if let (Some(bid), Some(ask)) = (&self.bid, &self.ask)
            && ask.price.get() < bid.price.get()
        {
            return Err(DomainError::Invalid {
                field: "quote",
                reason: "crossed market is forbidden",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuoteBatchV1 {
    pub schema_version: String,
    pub batch_id: Token,
    pub provider: Provider,
    pub mode: QuoteBatchMode,
    pub sequence: u64,
    pub received_at: DateTime<Utc>,
    pub provider_state: ProviderStateV1,
    pub quotes: Vec<QuoteV1>,
}

impl Validate for QuoteBatchV1 {
    fn validate(&self) -> Result<(), DomainError> {
        require_schema(
            &self.schema_version,
            QUOTE_BATCH_SCHEMA_VERSION,
            "quote batch",
        )?;
        self.provider_state.validate()?;
        if self.provider_state.provider != self.provider {
            return Err(DomainError::ProviderMismatch);
        }
        if self.provider_state.observed_at > self.received_at {
            return Err(DomainError::TimeOrder(
                "provider state is after batch received_at",
            ));
        }
        let quote_ids: Vec<Token> = self
            .quotes
            .iter()
            .map(|quote| quote.quote_id.clone())
            .collect();
        unique_tokens(&quote_ids, "quote_id")?;
        let mut quote_identities = Vec::with_capacity(self.quotes.len());
        for quote in &self.quotes {
            quote.validate()?;
            if quote.provider != self.provider {
                return Err(DomainError::ProviderMismatch);
            }
            quote_identities.push((
                quote.provider,
                quote.market_session,
                quote
                    .exact_contract_id()
                    .unwrap_or(&quote.instrument_id)
                    .clone(),
            ));
            for side in [&quote.bid, &quote.ask, &quote.last].into_iter().flatten() {
                if side.received_at > self.received_at {
                    return Err(DomainError::TimeOrder(
                        "quote side received_at is after batch received_at",
                    ));
                }
            }
        }
        quote_identities.sort();
        quote_identities.dedup();
        if quote_identities.len() != self.quotes.len() {
            return Err(DomainError::Duplicate(
                "quote identity within provider session",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AnalyticalOptionSnapshotV1 {
    pub schema_version: String,
    pub snapshot_id: Token,
    pub session: MarketSession,
    pub built_at: DateTime<Utc>,
    pub source_batch_ids: Vec<Token>,
    pub provider_states: Vec<ProviderStateV1>,
    pub quotes: Vec<QuoteV1>,
    pub provenance_hash: Token,
}

impl Validate for AnalyticalOptionSnapshotV1 {
    fn validate(&self) -> Result<(), DomainError> {
        require_schema(
            &self.schema_version,
            ANALYTICAL_SNAPSHOT_SCHEMA_VERSION,
            "analytical option snapshot",
        )?;
        if self.source_batch_ids.is_empty() {
            return Err(DomainError::Invalid {
                field: "source_batch_ids",
                reason: "snapshot requires at least one source batch",
            });
        }
        unique_tokens(&self.source_batch_ids, "source batch id")?;
        let mut providers: Vec<Provider> = self
            .provider_states
            .iter()
            .map(|state| state.provider)
            .collect();
        providers.sort();
        providers.dedup();
        if providers.len() != self.provider_states.len() {
            return Err(DomainError::Duplicate("snapshot provider state"));
        }
        for state in &self.provider_states {
            state.validate()?;
            if state.observed_at > self.built_at {
                return Err(DomainError::TimeOrder(
                    "provider state is after snapshot built_at",
                ));
            }
        }
        let quote_ids: Vec<Token> = self
            .quotes
            .iter()
            .map(|quote| quote.quote_id.clone())
            .collect();
        unique_tokens(&quote_ids, "snapshot quote_id")?;
        for quote in &self.quotes {
            quote.validate()?;
            if quote.market_session != self.session {
                return Err(DomainError::Invalid {
                    field: "market_session",
                    reason: "snapshot cannot mix sessions",
                });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{AuthenticationState, MarketSession};

    #[test]
    fn globex_and_closed_are_not_spx_decision_sessions() {
        assert!(serde_json::from_str::<MarketSession>(r#""globex""#).is_err());
        assert!(serde_json::from_str::<MarketSession>(r#""closed""#).is_err());
        assert_eq!(
            serde_json::from_str::<MarketSession>(r#""gth""#).unwrap(),
            MarketSession::Gth
        );
        assert_eq!(
            serde_json::from_str::<MarketSession>(r#""rth""#).unwrap(),
            MarketSession::Rth
        );
    }

    #[test]
    fn supported_live_providers_cannot_use_not_applicable_authentication() {
        assert!(serde_json::from_str::<AuthenticationState>(r#""not_applicable""#).is_err());
    }
}
