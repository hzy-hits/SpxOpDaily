use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, NaiveDate, Utc};
use serde::Serialize;
use spx_domain::{
    AuthenticationState, BookSideV1, DomainError, EntitlementState, InstrumentKind, MarketSession,
    NonNegativeF64, OperationalState, OptionContractV1, OptionRight, PROVIDER_STATE_SCHEMA_VERSION,
    PositiveF64, Provider, ProviderReasonCode, ProviderStateV1, QUOTE_BATCH_SCHEMA_VERSION,
    QuoteBatchMode, QuoteBatchV1, QuoteQuality, QuoteV1, Token, TransportState, Validate,
    canonical_json_hash,
};
use thiserror::Error;

use crate::legacy::{
    LegacyIbkrHealth, LegacyInstrument, LegacyProviderState, LegacyQuote, LegacySnapshot,
};

#[derive(Debug, Error)]
pub enum MapError {
    #[error("normalized snapshot is timestamped in the future")]
    FutureSnapshot,
    #[error("domain mapping failed: {0}")]
    Domain(#[from] DomainError),
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct MappingStats {
    pub source_quotes: usize,
    pub mapped_quotes: usize,
    pub dropped_other_provider: usize,
    pub dropped_unsupported_instrument: usize,
    pub dropped_unknown_session: usize,
    pub assigned_schwab_rth_policy: usize,
    pub dropped_non_decision_quality: usize,
    pub dropped_invalid_contract: usize,
    pub dropped_without_price: usize,
    pub crossed_books_suppressed: usize,
    pub superseded_duplicates: usize,
}

#[derive(Debug, Clone)]
pub struct MappedBatch {
    pub batch: QuoteBatchV1,
    pub stats: MappingStats,
}

pub fn map_provider_batch(
    snapshot: &LegacySnapshot,
    ibkr_health: Option<&LegacyIbkrHealth>,
    provider: Provider,
    connection_generation: u64,
    sequence: u64,
    source_fingerprint: &str,
    emitted_at: DateTime<Utc>,
) -> Result<MappedBatch, MapError> {
    let snapshot_at = snapshot.source_at().map_err(|_| MapError::FutureSnapshot)?;
    if snapshot_at > emitted_at {
        return Err(MapError::FutureSnapshot);
    }
    let mut stats = MappingStats {
        source_quotes: snapshot.quotes.len(),
        ..MappingStats::default()
    };
    let mut mapped = BTreeMap::new();
    let mut relevant_qualities = BTreeSet::new();
    let mut entitlement_hints = BTreeSet::new();
    let provider_name = provider_name(provider);
    for quote in &snapshot.quotes {
        if quote.provider != provider_name {
            stats.dropped_other_provider += 1;
            continue;
        }
        if let Some(quality) = parse_quality(&quote.quality) {
            relevant_qualities.insert(quality_rank(quality));
        }
        if let Some(rank) = market_data_entitlement_rank(quote.market_data_type.as_ref()) {
            entitlement_hints.insert(rank);
        }
        match map_quote(quote, provider, &mut stats) {
            Ok(Some(value)) => {
                let identity = quote_identity(&value);
                if let Some(previous) = mapped.get(&identity)
                    && latest_received(previous) >= latest_received(&value)
                {
                    stats.superseded_duplicates += 1;
                    continue;
                }
                if mapped.insert(identity, value).is_some() {
                    stats.superseded_duplicates += 1;
                }
            }
            Ok(None) => {}
            Err(_) => stats.dropped_invalid_contract += 1,
        }
    }
    let mut quotes: Vec<QuoteV1> = mapped.into_values().collect();
    stats.mapped_quotes = quotes.len();
    let legacy_state = snapshot
        .provider_states
        .iter()
        .find(|state| state.provider == provider_name);
    let provider_state = map_provider_state(&ProviderStateInput {
        legacy: legacy_state,
        ibkr_health: ibkr_health.filter(|_| provider == Provider::Ibkr),
        provider,
        connection_generation,
        snapshot_at,
        quotes: &quotes,
        relevant_qualities: &relevant_qualities,
        entitlement_hints: &entitlement_hints,
    })?;
    if !provider_state.is_live() {
        quotes.clear();
    }
    let received_at = quotes
        .iter()
        .flat_map(|quote| [&quote.bid, &quote.ask, &quote.last])
        .flatten()
        .map(|side| side.received_at)
        .fold(snapshot_at.max(provider_state.observed_at), DateTime::max);
    let batch = QuoteBatchV1 {
        schema_version: QUOTE_BATCH_SCHEMA_VERSION.to_owned(),
        batch_id: Token::new(
            format!(
                "mirror:{provider_name}:{connection_generation}:{sequence}:{}",
                &source_fingerprint[..source_fingerprint.len().min(24)]
            ),
            "batch_id",
        )?,
        provider,
        mode: QuoteBatchMode::ReplaceProviderSnapshot,
        sequence,
        received_at,
        provider_state,
        quotes,
    };
    batch.validate()?;
    Ok(MappedBatch { batch, stats })
}

pub fn semantic_hash(batch: &QuoteBatchV1) -> Result<String, DomainError> {
    canonical_json_hash(&(
        batch.provider,
        batch.mode,
        batch.provider_state.operational,
        batch.provider_state.transport,
        batch.provider_state.authentication,
        batch.provider_state.entitlement,
        &batch.provider_state.reason_codes,
        &batch.quotes,
    ))
}

pub fn map_source_failure_batch(
    provider: Provider,
    connection_generation: u64,
    sequence: u64,
    failure_fingerprint: &str,
    observed_at: DateTime<Utc>,
) -> Result<QuoteBatchV1, MapError> {
    let provider_name = provider_name(provider);
    let state = ProviderStateV1 {
        schema_version: PROVIDER_STATE_SCHEMA_VERSION.to_owned(),
        provider,
        observed_at,
        operational: OperationalState::Unavailable,
        transport: TransportState::Disconnected,
        authentication: AuthenticationState::Unauthenticated,
        entitlement: EntitlementState::Missing,
        reason_codes: vec![
            ProviderReasonCode::TransportDisconnected,
            ProviderReasonCode::AuthenticationUnavailable,
            ProviderReasonCode::EntitlementUnavailable,
            ProviderReasonCode::ProviderError,
        ],
        latency_ms: None,
        connection_generation,
    };
    let batch = QuoteBatchV1 {
        schema_version: QUOTE_BATCH_SCHEMA_VERSION.to_owned(),
        batch_id: Token::new(
            format!(
                "mirror:{provider_name}:{connection_generation}:{sequence}:failure:{}",
                &failure_fingerprint[..failure_fingerprint.len().min(16)]
            ),
            "batch_id",
        )?,
        provider,
        mode: QuoteBatchMode::ReplaceProviderSnapshot,
        sequence,
        received_at: observed_at,
        provider_state: state,
        quotes: Vec::new(),
    };
    batch.validate()?;
    Ok(batch)
}

fn map_quote(
    quote: &LegacyQuote,
    provider: Provider,
    stats: &mut MappingStats,
) -> Result<Option<QuoteV1>, DomainError> {
    let Some(instrument_kind) = supported_instrument(&quote.instrument) else {
        stats.dropped_unsupported_instrument += 1;
        return Ok(None);
    };
    let session = match quote.market_session.as_deref() {
        Some(value) => {
            let Some(session) = parse_session(Some(value)) else {
                stats.dropped_unknown_session += 1;
                return Ok(None);
            };
            session
        }
        None if provider == Provider::Schwab => {
            // Schwab is the configured SPX/SPXW RTH-only decision provider.
            // This is a provider-policy assignment, not a wall-clock guess;
            // stale/frozen quality remains non-authoritative.
            stats.assigned_schwab_rth_policy += 1;
            MarketSession::Rth
        }
        None => {
            stats.dropped_unknown_session += 1;
            return Ok(None);
        }
    };
    if provider == Provider::Schwab && session != MarketSession::Rth {
        stats.dropped_unknown_session += 1;
        return Ok(None);
    }
    let Some(quality) = parse_quality(&quote.quality) else {
        stats.dropped_non_decision_quality += 1;
        return Ok(None);
    };
    let received_at = quote.last_update_at.unwrap_or(quote.received_at);
    let source_at = quote.quote_time.or(quote.trade_time);
    let mut bid = side(quote.bid, source_at, received_at, "bid")?;
    let mut ask = side(quote.ask, source_at, received_at, "ask")?;
    let last_source_at = quote.trade_time.or(quote.quote_time);
    let last = side(quote.last, last_source_at, received_at, "last")?;
    if let (Some(bid_side), Some(ask_side)) = (&bid, &ask)
        && ask_side.price.get() < bid_side.price.get()
    {
        bid = None;
        ask = None;
        stats.crossed_books_suppressed += 1;
    }
    if bid.is_none() && ask.is_none() && last.is_none() {
        stats.dropped_without_price += 1;
        return Ok(None);
    }
    let provider_symbol = quote
        .provider_symbol
        .as_deref()
        .or(quote.instrument.provider_symbol.as_deref());
    let option = if instrument_kind == InstrumentKind::Option {
        Some(option_contract(&quote.instrument, provider_symbol)?)
    } else {
        None
    };
    let identity = option
        .as_ref()
        .map_or(quote.instrument.canonical_id.as_str(), |value| {
            value.contract_id.as_str()
        });
    let identity_time =
        latest_side_time(bid.as_ref(), ask.as_ref(), last.as_ref()).unwrap_or(quote.received_at);
    let result = QuoteV1 {
        quote_id: Token::new(
            format!(
                "mirror:{}:{}:{}:{}",
                provider_name(provider),
                session_name(session),
                identity,
                identity_time.timestamp_micros()
            ),
            "quote_id",
        )?,
        provider,
        instrument_id: Token::new(&quote.instrument.canonical_id, "instrument_id")?,
        instrument_kind,
        market_session: session,
        quality,
        option,
        bid,
        ask,
        last,
    };
    result.validate()?;
    Ok(Some(result))
}

fn supported_instrument(instrument: &LegacyInstrument) -> Option<InstrumentKind> {
    match instrument.instrument_type.as_str() {
        "index" if instrument.symbol == "SPX" => Some(InstrumentKind::Index),
        "option"
            if instrument.underlier.as_deref() == Some("SPX")
                && instrument.trading_class.as_deref() == Some("SPXW") =>
        {
            Some(InstrumentKind::Option)
        }
        _ => None,
    }
}

fn option_contract(
    instrument: &LegacyInstrument,
    provider_symbol: Option<&str>,
) -> Result<OptionContractV1, DomainError> {
    let expiry =
        instrument
            .expiry
            .as_deref()
            .and_then(parse_expiry)
            .ok_or(DomainError::Invalid {
                field: "expiry",
                reason: "legacy option expiry is missing or invalid",
            })?;
    let strike = PositiveF64::new(instrument.strike.unwrap_or_default(), "strike")?;
    let right = match instrument.right.as_deref() {
        Some("C" | "CALL") => OptionRight::Call,
        Some("P" | "PUT") => OptionRight::Put,
        _ => {
            return Err(DomainError::Invalid {
                field: "right",
                reason: "legacy option right is missing or invalid",
            });
        }
    };
    let multiplier =
        instrument
            .multiplier
            .as_ref()
            .and_then(parse_u32)
            .ok_or(DomainError::Invalid {
                field: "multiplier",
                reason: "legacy option multiplier is missing or invalid",
            })?;
    Ok(OptionContractV1 {
        contract_id: Token::new(
            provider_symbol.ok_or(DomainError::Invalid {
                field: "contract_id",
                reason: "legacy option provider symbol is missing",
            })?,
            "contract_id",
        )?,
        underlier: Token::new("SPX", "underlier")?,
        trading_class: Token::new("SPXW", "trading_class")?,
        expiry,
        strike,
        right,
        multiplier,
    })
}

fn side(
    value: Option<f64>,
    source_at: Option<DateTime<Utc>>,
    received_at: DateTime<Utc>,
    field: &'static str,
) -> Result<Option<BookSideV1>, DomainError> {
    let (Some(value), Some(source_at)) = (value, source_at) else {
        return Ok(None);
    };
    if !value.is_finite() || value <= 0.0 || source_at > received_at {
        return Ok(None);
    }
    let side = BookSideV1 {
        price: PositiveF64::new(value, field)?,
        source_at,
        received_at,
    };
    side.validate()?;
    Ok(Some(side))
}

struct ProviderStateInput<'a> {
    legacy: Option<&'a LegacyProviderState>,
    ibkr_health: Option<&'a LegacyIbkrHealth>,
    provider: Provider,
    connection_generation: u64,
    snapshot_at: DateTime<Utc>,
    quotes: &'a [QuoteV1],
    relevant_qualities: &'a BTreeSet<u8>,
    entitlement_hints: &'a BTreeSet<u8>,
}

fn map_provider_state(input: &ProviderStateInput<'_>) -> Result<ProviderStateV1, DomainError> {
    let connected = input.ibkr_health.map_or_else(
        || {
            input
                .legacy
                .and_then(|state| state.connected)
                .unwrap_or(false)
        },
        |health| health.connected,
    );
    let authenticated = input
        .legacy
        .and_then(|state| state.authenticated)
        .unwrap_or(false);
    let reason = combined_reason(input.legacy, input.ibkr_health);
    let competing = reason
        .as_deref()
        .is_some_and(|value| value.contains("10197"));
    let operational = operational_state(input, connected, authenticated, competing);
    let entitlement = entitlement_state(input, competing);
    let transport = if connected {
        TransportState::Connected
    } else {
        TransportState::Disconnected
    };
    let authentication = if authenticated {
        AuthenticationState::Authenticated
    } else {
        AuthenticationState::Unauthenticated
    };
    let observed_at = provider_observed_at(input, operational);
    let reason_codes = reason_codes(
        operational,
        transport,
        authentication,
        entitlement,
        reason.as_deref(),
    );
    let latency_ms = input
        .legacy
        .and_then(|state| state.latency_ms)
        .filter(|value| value.is_finite() && *value >= 0.0)
        .and_then(|value| NonNegativeF64::new(value, "latency_ms").ok());
    let state = ProviderStateV1 {
        schema_version: PROVIDER_STATE_SCHEMA_VERSION.to_owned(),
        provider: input.provider,
        observed_at,
        operational,
        transport,
        authentication,
        entitlement,
        reason_codes,
        latency_ms,
        connection_generation: input.connection_generation,
    };
    state.validate()?;
    Ok(state)
}

fn operational_state(
    input: &ProviderStateInput<'_>,
    connected: bool,
    authenticated: bool,
    competing: bool,
) -> OperationalState {
    let legacy_available = input
        .legacy
        .is_some_and(|state| state.status == "available");
    let health_ready = input
        .ibkr_health
        .is_none_or(|health| health.data_plane_healthy);
    let has_live = input
        .quotes
        .iter()
        .any(|quote| quote.quality == QuoteQuality::Live);
    let circuit_backoff = input.ibkr_health.is_some_and(|health| {
        health
            .circuit_state
            .as_deref()
            .is_some_and(|state| state.eq_ignore_ascii_case("half_open"))
    });
    if competing && input.provider == Provider::Ibkr {
        OperationalState::ExternalSessionOwns
    } else if circuit_backoff && input.provider == Provider::Ibkr {
        OperationalState::Backoff
    } else if connected && authenticated && legacy_available && health_ready && has_live {
        OperationalState::Live
    } else if !connected
        || input
            .legacy
            .is_some_and(|state| state.status == "unavailable")
    {
        OperationalState::Unavailable
    } else if input.legacy.is_none() {
        OperationalState::Starting
    } else {
        OperationalState::Degraded
    }
}

fn entitlement_state(input: &ProviderStateInput<'_>, competing: bool) -> EntitlementState {
    if competing {
        return EntitlementState::Missing;
    }
    let rank = [3, 2, 1]
        .into_iter()
        .find(|rank| {
            input.relevant_qualities.contains(rank) || input.entitlement_hints.contains(rank)
        })
        .unwrap_or(0);
    match rank {
        3 => EntitlementState::Live,
        2 => EntitlementState::Delayed,
        1 => EntitlementState::Frozen,
        _ => EntitlementState::Missing,
    }
}

fn provider_observed_at(
    input: &ProviderStateInput<'_>,
    operational: OperationalState,
) -> DateTime<Utc> {
    if operational == OperationalState::Live {
        input
            .quotes
            .iter()
            .filter(|quote| quote.quality == QuoteQuality::Live)
            .flat_map(|quote| [&quote.bid, &quote.ask, &quote.last])
            .flatten()
            .map(|side| side.source_at)
            .max()
            .unwrap_or(input.snapshot_at)
    } else {
        input
            .ibkr_health
            .map(|health| health.observed_at)
            .or_else(|| input.legacy.map(|state| state.checked_at))
            .unwrap_or(input.snapshot_at)
            .min(input.snapshot_at)
    }
}

fn reason_codes(
    operational: OperationalState,
    transport: TransportState,
    authentication: AuthenticationState,
    entitlement: EntitlementState,
    reason: Option<&str>,
) -> Vec<ProviderReasonCode> {
    if operational == OperationalState::Live {
        return vec![ProviderReasonCode::Healthy];
    }
    let mut result = Vec::new();
    let reason_lower = reason.unwrap_or_default().to_ascii_lowercase();
    if operational == OperationalState::Starting {
        result.push(ProviderReasonCode::Starting);
    }
    if transport == TransportState::Disconnected {
        result.push(ProviderReasonCode::TransportDisconnected);
    }
    if authentication == AuthenticationState::Unauthenticated {
        result.push(ProviderReasonCode::AuthenticationUnavailable);
    }
    if entitlement == EntitlementState::Missing {
        result.push(ProviderReasonCode::EntitlementUnavailable);
    }
    if reason_lower.contains("10197") {
        result.push(ProviderReasonCode::CompetingSession10197);
    } else if reason_lower.contains("stale") || reason_lower.contains("no price") {
        result.push(ProviderReasonCode::StaleFlush);
    } else if reason_lower.contains("rate") {
        result.push(ProviderReasonCode::RateLimited);
    } else if !reason_lower.is_empty() {
        result.push(ProviderReasonCode::ProviderError);
    }
    if result.is_empty() {
        result.push(ProviderReasonCode::ProviderError);
    }
    result.sort();
    result.dedup();
    result
}

fn combined_reason(
    legacy: Option<&LegacyProviderState>,
    health: Option<&LegacyIbkrHealth>,
) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(value) = legacy.and_then(|state| state.reason.as_deref()) {
        parts.push(value);
    }
    if let Some(value) = health.and_then(|state| state.reason.as_deref())
        && !parts.contains(&value)
    {
        parts.push(value);
    }
    if let Some(value) = health.and_then(|state| state.circuit_state.as_deref())
        && !value.eq_ignore_ascii_case("closed")
    {
        parts.push(value);
    }
    (!parts.is_empty()).then(|| parts.join("; "))
}

fn parse_session(value: Option<&str>) -> Option<MarketSession> {
    match value {
        Some("regular" | "rth") => Some(MarketSession::Rth),
        Some("gth") => Some(MarketSession::Gth),
        _ => None,
    }
}

fn parse_quality(value: &str) -> Option<QuoteQuality> {
    match value {
        "live" => Some(QuoteQuality::Live),
        "delayed" => Some(QuoteQuality::Delayed),
        "frozen" => Some(QuoteQuality::Frozen),
        _ => None,
    }
}

fn market_data_entitlement_rank(value: Option<&serde_json::Value>) -> Option<u8> {
    match value {
        Some(serde_json::Value::String(value)) if value.eq_ignore_ascii_case("live") => Some(3),
        Some(serde_json::Value::String(value)) if value.eq_ignore_ascii_case("delayed") => Some(2),
        Some(serde_json::Value::String(value)) if value.eq_ignore_ascii_case("frozen") => Some(1),
        Some(serde_json::Value::Number(value)) => match value.as_u64() {
            Some(1) => Some(3),
            Some(3) => Some(2),
            Some(2 | 4) => Some(1),
            _ => None,
        },
        _ => None,
    }
}

const fn quality_rank(quality: QuoteQuality) -> u8 {
    match quality {
        QuoteQuality::Frozen => 1,
        QuoteQuality::Delayed => 2,
        QuoteQuality::Live => 3,
    }
}

fn parse_expiry(value: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(value, "%Y%m%d")
        .or_else(|_| NaiveDate::parse_from_str(value, "%Y-%m-%d"))
        .ok()
}

fn parse_u32(value: &serde_json::Value) -> Option<u32> {
    match value {
        serde_json::Value::Number(number) => {
            number.as_u64().and_then(|raw| u32::try_from(raw).ok())
        }
        serde_json::Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

fn quote_identity(quote: &QuoteV1) -> (MarketSession, String) {
    (
        quote.market_session,
        quote
            .exact_contract_id()
            .unwrap_or(&quote.instrument_id)
            .as_str()
            .to_owned(),
    )
}

fn latest_received(quote: &QuoteV1) -> DateTime<Utc> {
    [&quote.bid, &quote.ask, &quote.last]
        .into_iter()
        .flatten()
        .map(|side| side.received_at)
        .max()
        .expect("mapped quote has at least one side")
}

fn latest_side_time(
    bid: Option<&BookSideV1>,
    ask: Option<&BookSideV1>,
    last: Option<&BookSideV1>,
) -> Option<DateTime<Utc>> {
    [bid, ask, last]
        .into_iter()
        .flatten()
        .map(|side| side.source_at)
        .max()
}

const fn provider_name(provider: Provider) -> &'static str {
    match provider {
        Provider::Schwab => "schwab",
        Provider::Ibkr => "ibkr",
    }
}

const fn session_name(session: MarketSession) -> &'static str {
    match session {
        MarketSession::Rth => "rth",
        MarketSession::Gth => "gth",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::legacy::LegacySnapshot;

    fn at(value: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(value)
            .unwrap()
            .with_timezone(&Utc)
    }

    fn snapshot(market_session: Option<&str>, quality: &str, bid: f64, ask: f64) -> LegacySnapshot {
        serde_json::from_value(serde_json::json!({
            "created_at": "2026-08-01T14:30:00Z",
            "as_of": "2026-08-01T14:30:00Z",
            "provider_states": [{
                "provider": "schwab",
                "status": "available",
                "checked_at": "2026-08-01T14:29:59.950Z",
                "reason": null,
                "connected": true,
                "authenticated": true,
                "latency_ms": 12.0
            }],
            "quotes": [{
                "instrument": {
                    "canonical_id": "option:SPX:SPXW:20260801:6300:C",
                    "provider_symbol": "SPXW  260801C06300000",
                    "instrument_type": "option",
                    "symbol": "SPX",
                    "underlier": "SPX",
                    "expiry": "20260801",
                    "strike": 6300.0,
                    "right": "C",
                    "trading_class": "SPXW",
                    "multiplier": "100"
                },
                "provider": "schwab",
                "provider_symbol": "SPXW  260801C06300000",
                "received_at": "2026-08-01T14:29:59.980Z",
                "quality": quality,
                "bid": bid,
                "ask": ask,
                "last": null,
                "quote_time": "2026-08-01T14:29:59.920Z",
                "trade_time": null,
                "last_update_at": "2026-08-01T14:29:59.980Z",
                "market_data_type": "live",
                "source_session": null,
                "market_session": market_session
            }]
        }))
        .unwrap()
    }

    #[test]
    fn schwab_rth_option_maps_to_strict_full_snapshot() {
        let mapped = map_provider_batch(
            &snapshot(Some("regular"), "live", 5.0, 5.2),
            None,
            Provider::Schwab,
            2,
            7,
            &"a".repeat(64),
            at("2026-08-01T14:30:01Z"),
        )
        .unwrap();
        assert_eq!(mapped.batch.mode, QuoteBatchMode::ReplaceProviderSnapshot);
        assert_eq!(mapped.batch.quotes.len(), 1);
        assert!(mapped.batch.provider_state.is_live());
        assert_eq!(mapped.stats.dropped_unknown_session, 0);
        let quote = &mapped.batch.quotes[0];
        assert_eq!(
            quote.instrument_id.as_str(),
            "option:SPX:SPXW:20260801:6300:C"
        );
        assert_eq!(
            quote
                .option
                .as_ref()
                .expect("mapped option contract")
                .contract_id
                .as_str(),
            "SPXW  260801C06300000"
        );
    }

    #[test]
    fn schwab_sessionless_spxw_uses_explicit_rth_provider_policy() {
        let mapped = map_provider_batch(
            &snapshot(None, "live", 5.0, 5.2),
            None,
            Provider::Schwab,
            2,
            7,
            &"b".repeat(64),
            at("2026-08-01T14:30:01Z"),
        )
        .unwrap();
        assert_eq!(mapped.batch.quotes.len(), 1);
        assert!(mapped.batch.provider_state.is_live());
        assert_eq!(mapped.stats.assigned_schwab_rth_policy, 1);
    }

    #[test]
    fn ibkr_sessionless_spxw_stays_fail_closed() {
        let mut source = snapshot(None, "live", 5.0, 5.2);
        source.quotes[0].provider = "ibkr".to_owned();
        source.quotes[0].source_session = Some("ibkr-stream:8".to_owned());
        source.provider_states[0].provider = "ibkr".to_owned();
        let mapped = map_provider_batch(
            &source,
            None,
            Provider::Ibkr,
            2,
            7,
            &"f".repeat(64),
            at("2026-08-01T14:30:01Z"),
        )
        .unwrap();
        assert!(mapped.batch.quotes.is_empty());
        assert!(!mapped.batch.provider_state.is_live());
        assert_eq!(mapped.stats.dropped_unknown_session, 1);
    }

    #[test]
    fn explicit_schwab_globex_is_not_relabelled_as_rth() {
        let mapped = map_provider_batch(
            &snapshot(Some("globex"), "live", 5.0, 5.2),
            None,
            Provider::Schwab,
            2,
            7,
            &"9".repeat(64),
            at("2026-08-01T14:30:01Z"),
        )
        .unwrap();
        assert!(mapped.batch.quotes.is_empty());
        assert!(!mapped.batch.provider_state.is_live());
        assert_eq!(mapped.stats.assigned_schwab_rth_policy, 0);
        assert_eq!(mapped.stats.dropped_unknown_session, 1);
    }

    #[test]
    fn zero_bid_becomes_one_sided_without_price_fabrication() {
        let mapped = map_provider_batch(
            &snapshot(Some("regular"), "live", 0.0, 0.05),
            None,
            Provider::Schwab,
            2,
            7,
            &"c".repeat(64),
            at("2026-08-01T14:30:01Z"),
        )
        .unwrap();
        assert!(mapped.batch.quotes[0].bid.is_none());
        let ask = mapped.batch.quotes[0].ask.as_ref().unwrap().price.get();
        assert!((ask - 0.05).abs() < f64::EPSILON);
    }

    #[test]
    fn crossed_book_is_suppressed_and_not_authoritative() {
        let mapped = map_provider_batch(
            &snapshot(Some("regular"), "live", 5.2, 5.0),
            None,
            Provider::Schwab,
            2,
            7,
            &"d".repeat(64),
            at("2026-08-01T14:30:01Z"),
        )
        .unwrap();
        assert!(mapped.batch.quotes.is_empty());
        assert_eq!(mapped.stats.crossed_books_suppressed, 1);
    }

    #[test]
    fn stale_quote_cannot_make_provider_live() {
        let mapped = map_provider_batch(
            &snapshot(Some("regular"), "stale", 5.0, 5.2),
            None,
            Provider::Schwab,
            2,
            7,
            &"e".repeat(64),
            at("2026-08-01T14:30:01Z"),
        )
        .unwrap();
        assert!(mapped.batch.quotes.is_empty());
        assert!(!mapped.batch.provider_state.is_live());
        assert_eq!(mapped.stats.dropped_non_decision_quality, 1);
    }
}
