use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::Serialize;
use spx_domain::{
    INGRESS_SCHEMA_VERSION, IngressEnvelopeV1, IngressMessageV1, Provider, ProviderStateV1, Token,
    Validate,
};
use thiserror::Error;

use crate::BridgeConfig;
use crate::legacy::{LegacyError, read_ibkr_health, read_snapshot};
use crate::mapper::{MapError, MappingStats, map_provider_batch, map_source_failure_batch};

#[derive(Debug, Error)]
pub enum InspectionError {
    #[error("normalized source failed: {0}")]
    Source(#[from] LegacyError),
    #[error("normalized source mapping failed: {0}")]
    Map(#[from] MapError),
    #[error("inspection envelope failed: {0}")]
    Domain(#[from] spx_domain::DomainError),
    #[error("inspection serialization failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("mapped provider frame exceeds configured core frame bound")]
    FrameTooLarge,
}

#[derive(Debug, Serialize)]
pub struct InspectionReport {
    pub schema_version: &'static str,
    pub inspected_at: DateTime<Utc>,
    pub source_fingerprint: String,
    pub source_at: DateTime<Utc>,
    pub source_bytes: usize,
    pub source_quotes: usize,
    pub ibkr_health_available: bool,
    pub providers: BTreeMap<Provider, ProviderInspection>,
}

#[derive(Debug, Serialize)]
pub struct ProviderInspection {
    pub frame_bytes: usize,
    pub provider_state: ProviderStateV1,
    pub mapped_quotes: usize,
    pub mapping: MappingStats,
}

/// Maps the live legacy files without opening the core socket or mutating bridge state.
///
/// # Errors
///
/// Returns an error for malformed/oversized input, invalid contract mapping, or
/// a mapped provider frame that cannot fit the configured core boundary.
pub fn inspect_source(config: &BridgeConfig) -> Result<InspectionReport, InspectionError> {
    let inspected_at = Utc::now();
    let document = read_snapshot(&config.source_snapshot_path, config.source_max_bytes)?;
    let source_at = document.snapshot.source_at()?;
    let ibkr_health = read_ibkr_health(
        &config.ibkr_health_path,
        config.source_max_bytes.min(1_048_576),
    )
    .ok();
    let mut providers = BTreeMap::new();
    for provider in [Provider::Schwab, Provider::Ibkr] {
        let (batch, mapping) = if provider == Provider::Ibkr && ibkr_health.is_none() {
            (
                map_source_failure_batch(provider, 1, 1, &document.fingerprint, inspected_at)?,
                MappingStats::default(),
            )
        } else {
            let mapped = map_provider_batch(
                &document.snapshot,
                ibkr_health.as_ref(),
                provider,
                1,
                1,
                &document.fingerprint,
                inspected_at,
            )?;
            (mapped.batch, mapped.stats)
        };
        let envelope = IngressEnvelopeV1 {
            schema_version: INGRESS_SCHEMA_VERSION.to_owned(),
            message_id: Token::new(format!("inspect:{}", batch.batch_id), "message_id")?,
            emitted_at: inspected_at,
            message: IngressMessageV1::QuoteBatch(batch.clone()),
        };
        envelope.validate()?;
        let frame_bytes = serde_json::to_vec(&envelope)?.len();
        if frame_bytes > config.max_frame_bytes {
            return Err(InspectionError::FrameTooLarge);
        }
        providers.insert(
            provider,
            ProviderInspection {
                frame_bytes,
                provider_state: batch.provider_state,
                mapped_quotes: batch.quotes.len(),
                mapping,
            },
        );
    }
    Ok(InspectionReport {
        schema_version: "spx_normalized_bridge_inspection.v1",
        inspected_at,
        source_fingerprint: document.fingerprint,
        source_at,
        source_bytes: document.byte_len,
        source_quotes: document.snapshot.quotes.len(),
        ibkr_health_available: ibkr_health.is_some(),
        providers,
    })
}
