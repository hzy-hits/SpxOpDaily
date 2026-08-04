use std::fs::File;
use std::io::Read;
use std::path::Path;

use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use spx_domain::{DeskDataQuality, DeskMapProjectionV1, ResearchSignalsV1, Token, Validate};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum LegacyError {
    #[error("failed to inspect normalized snapshot: {0}")]
    Metadata(#[source] std::io::Error),
    #[error("normalized snapshot exceeds configured byte bound")]
    Oversized,
    #[error("failed to read normalized snapshot: {0}")]
    Read(#[source] std::io::Error),
    #[error("invalid normalized snapshot JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("normalized snapshot has no trustworthy creation timestamp")]
    MissingCreationTime,
}

#[derive(Debug, Clone)]
pub struct LegacyDocument {
    pub snapshot: LegacySnapshot,
    pub fingerprint: String,
    pub byte_len: usize,
}

#[derive(Debug, Clone)]
pub struct LegacyResearchDocument {
    pub signals: ResearchSignalsV1,
    pub fingerprint: String,
    pub byte_len: usize,
}

#[derive(Debug, Clone)]
pub struct LegacyDeskMapDocument {
    pub projection: DeskMapProjectionV1,
    pub fingerprint: String,
    pub byte_len: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LegacySnapshot {
    pub created_at: Option<DateTime<Utc>>,
    pub as_of: Option<DateTime<Utc>>,
    #[serde(default)]
    pub quotes: Vec<LegacyQuote>,
    #[serde(default)]
    pub provider_states: Vec<LegacyProviderState>,
}

impl LegacySnapshot {
    pub fn source_at(&self) -> Result<DateTime<Utc>, LegacyError> {
        self.created_at
            .or(self.as_of)
            .ok_or(LegacyError::MissingCreationTime)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct LegacyProviderState {
    pub provider: String,
    pub status: String,
    pub checked_at: DateTime<Utc>,
    pub reason: Option<String>,
    pub connected: Option<bool>,
    pub authenticated: Option<bool>,
    pub latency_ms: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LegacyQuote {
    pub instrument: LegacyInstrument,
    pub provider: String,
    pub provider_symbol: Option<String>,
    pub received_at: DateTime<Utc>,
    pub quality: String,
    pub bid: Option<f64>,
    pub ask: Option<f64>,
    pub last: Option<f64>,
    pub quote_time: Option<DateTime<Utc>>,
    pub trade_time: Option<DateTime<Utc>>,
    pub last_update_at: Option<DateTime<Utc>>,
    pub market_data_type: Option<Value>,
    pub source_session: Option<String>,
    pub market_session: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LegacyInstrument {
    pub canonical_id: String,
    pub provider_symbol: Option<String>,
    pub instrument_type: String,
    pub symbol: String,
    pub underlier: Option<String>,
    pub expiry: Option<String>,
    pub strike: Option<f64>,
    pub right: Option<String>,
    pub trading_class: Option<String>,
    pub multiplier: Option<Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LegacyIbkrHealth {
    pub observed_at: DateTime<Utc>,
    pub connection_generation: u64,
    pub connected: bool,
    pub data_plane_healthy: bool,
    pub circuit_state: Option<String>,
    pub reason: Option<String>,
}

pub fn read_snapshot(path: &Path, maximum: u64) -> Result<LegacyDocument, LegacyError> {
    let metadata = std::fs::metadata(path).map_err(LegacyError::Metadata)?;
    if metadata.len() > maximum {
        return Err(LegacyError::Oversized);
    }
    let capacity = usize::try_from(metadata.len()).unwrap_or(0);
    let mut bytes = Vec::with_capacity(capacity);
    File::open(path)
        .and_then(|file| file.take(maximum.saturating_add(1)).read_to_end(&mut bytes))
        .map_err(LegacyError::Read)?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > maximum {
        return Err(LegacyError::Oversized);
    }
    let fingerprint = hex_digest(&bytes);
    let snapshot = serde_json::from_slice(&bytes)?;
    Ok(LegacyDocument {
        snapshot,
        fingerprint,
        byte_len: bytes.len(),
    })
}

pub fn read_ibkr_health(path: &Path, maximum: u64) -> Result<LegacyIbkrHealth, LegacyError> {
    let metadata = std::fs::metadata(path).map_err(LegacyError::Metadata)?;
    if metadata.len() > maximum {
        return Err(LegacyError::Oversized);
    }
    let mut bytes = Vec::with_capacity(usize::try_from(metadata.len()).unwrap_or(0));
    File::open(path)
        .and_then(|file| file.take(maximum.saturating_add(1)).read_to_end(&mut bytes))
        .map_err(LegacyError::Read)?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > maximum {
        return Err(LegacyError::Oversized);
    }
    Ok(serde_json::from_slice(&bytes)?)
}

pub fn read_research_signals(
    path: &Path,
    maximum: u64,
) -> Result<LegacyResearchDocument, LegacyError> {
    let metadata = std::fs::metadata(path).map_err(LegacyError::Metadata)?;
    if metadata.len() > maximum {
        return Err(LegacyError::Oversized);
    }
    let mut bytes = Vec::with_capacity(usize::try_from(metadata.len()).unwrap_or(0));
    File::open(path)
        .and_then(|file| file.take(maximum.saturating_add(1)).read_to_end(&mut bytes))
        .map_err(LegacyError::Read)?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > maximum {
        return Err(LegacyError::Oversized);
    }
    let signals: ResearchSignalsV1 = serde_json::from_slice(&bytes)?;
    signals.validate().map_err(|error| {
        LegacyError::Json(serde_json::Error::io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            error,
        )))
    })?;
    Ok(LegacyResearchDocument {
        signals,
        fingerprint: hex_digest(&bytes),
        byte_len: bytes.len(),
    })
}

pub fn read_desk_map_projection(
    path: &Path,
    maximum: u64,
) -> Result<LegacyDeskMapDocument, LegacyError> {
    let metadata = std::fs::metadata(path).map_err(LegacyError::Metadata)?;
    if metadata.len() > maximum {
        return Err(LegacyError::Oversized);
    }
    let mut bytes = Vec::with_capacity(usize::try_from(metadata.len()).unwrap_or(0));
    File::open(path)
        .and_then(|file| file.take(maximum.saturating_add(1)).read_to_end(&mut bytes))
        .map_err(LegacyError::Read)?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > maximum {
        return Err(LegacyError::Oversized);
    }
    let projection = decode_desk_map_projection(&bytes)?;
    Ok(LegacyDeskMapDocument {
        projection,
        fingerprint: hex_digest(&bytes),
        byte_len: bytes.len(),
    })
}

fn decode_desk_map_projection(bytes: &[u8]) -> Result<DeskMapProjectionV1, LegacyError> {
    let mut raw: Value = serde_json::from_slice(bytes)?;
    match decode_and_validate_desk_map(raw.clone()) {
        Ok(projection) => Ok(projection),
        Err(original) => {
            let Some(document) = raw.as_object_mut() else {
                return Err(original);
            };
            let research_field_present = document.contains_key("research_context");
            let embedded_research = document
                .get("research_context")
                .is_some_and(|value| !value.is_null())
                || document
                    .get("research_context_document_id")
                    .is_some_and(|value| !value.is_null());
            if !research_field_present || !embedded_research {
                return Err(original);
            }

            document.insert("research_context".to_owned(), Value::Null);
            document.insert("research_context_document_id".to_owned(), Value::Null);
            let mut projection = decode_and_validate_desk_map(raw)?;
            projection.research_context = None;
            projection.research_context_document_id = None;
            if projection.quality == DeskDataQuality::Ready {
                projection.quality = DeskDataQuality::Degraded;
            }
            let reason = Token::new(
                "research_context_contract_invalid",
                "desk map quality reason",
            )
            .map_err(domain_as_json)?;
            if !projection.quality_reasons.contains(&reason) {
                projection.quality_reasons.push(reason);
            }
            projection.validate().map_err(domain_as_json)?;
            Ok(projection)
        }
    }
}

fn decode_and_validate_desk_map(raw: Value) -> Result<DeskMapProjectionV1, LegacyError> {
    let projection: DeskMapProjectionV1 = serde_json::from_value(raw)?;
    projection.validate().map_err(domain_as_json)?;
    Ok(projection)
}

fn domain_as_json(error: spx_domain::DomainError) -> LegacyError {
    LegacyError::Json(serde_json::Error::io(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        error,
    )))
}

fn hex_digest(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("writing to String is infallible");
    }
    output
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use tempfile::NamedTempFile;

    use super::*;

    #[test]
    fn read_is_bounded_before_decode() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(&[b'x'; 64]).unwrap();
        assert!(matches!(
            read_snapshot(file.path(), 32),
            Err(LegacyError::Oversized)
        ));
    }

    #[test]
    fn malformed_json_is_rejected() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"not-json").unwrap();
        assert!(matches!(
            read_snapshot(file.path(), 1_024),
            Err(LegacyError::Json(_))
        ));
    }

    #[test]
    fn typed_research_artifact_is_bounded_and_validated() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(include_bytes!(
            "../../../../contracts/golden/domain/v1/experimental_research_signals.json"
        ))
        .unwrap();
        let document = read_research_signals(file.path(), 1_048_576).unwrap();
        assert!(document.signals.market_regime().is_some());
        assert_eq!(document.signals.range_forecasts().len(), 3);
        assert_eq!(document.fingerprint.len(), 64);
        assert_eq!(
            document.byte_len,
            usize::try_from(file.as_file().metadata().unwrap().len()).unwrap()
        );
    }

    #[test]
    fn oracle_research_context_v2_is_bounded_and_validated() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(include_bytes!(
            "../../../../contracts/golden/domain/v2/research_context.json"
        ))
        .unwrap();
        let document = read_research_signals(file.path(), 1_048_576).unwrap();
        assert_eq!(document.signals.schema_version, "research_context.v2");
        assert!(document.signals.context_v2().is_some());
    }

    #[test]
    fn desk_map_projection_is_bounded_and_validated() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(include_bytes!(
            "../../../../contracts/golden/domain/v1/desk_map_projection.json"
        ))
        .unwrap();
        let document = read_desk_map_projection(file.path(), 1_048_576).unwrap();
        assert_eq!(document.projection.schema_version, "desk_map_projection.v1");
        let research_context = document
            .projection
            .research_context
            .as_ref()
            .expect("desk map fixture embeds its research context atomically");
        assert_eq!(research_context.schema_version, "research_context.v2");
        assert_eq!(
            Some(&research_context.document_id),
            document.projection.research_context_document_id.as_ref()
        );
        assert_eq!(document.fingerprint.len(), 64);
        assert_eq!(
            document.byte_len,
            usize::try_from(file.as_file().metadata().unwrap().len()).unwrap()
        );
    }

    #[test]
    fn invalid_optional_research_degrades_without_poisoning_valid_desk_facts() {
        let mut value: Value = serde_json::from_str(include_str!(
            "../../../../contracts/golden/domain/v1/desk_map_projection.json"
        ))
        .unwrap();
        value["research_context"]["regime"]["posterior"][0]["probability"] = serde_json::json!(0.9);
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(serde_json::to_string(&value).unwrap().as_bytes())
            .unwrap();

        let document = read_desk_map_projection(file.path(), 1_048_576).unwrap();

        assert!(document.projection.research_context.is_none());
        assert!(document.projection.research_context_document_id.is_none());
        assert_eq!(document.projection.quality, DeskDataQuality::Degraded);
        assert!(
            document
                .projection
                .quality_reasons
                .iter()
                .any(|reason| { reason.as_str() == "research_context_contract_invalid" })
        );
    }

    #[test]
    fn invalid_desk_fact_still_fails_when_optional_research_is_invalid() {
        let mut value: Value = serde_json::from_str(include_str!(
            "../../../../contracts/golden/domain/v1/desk_map_projection.json"
        ))
        .unwrap();
        value["research_context"]["regime"]["posterior"][0]["probability"] = serde_json::json!(0.9);
        value["message"]["execution"] = Value::Null;
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(serde_json::to_string(&value).unwrap().as_bytes())
            .unwrap();

        assert!(matches!(
            read_desk_map_projection(file.path(), 1_048_576),
            Err(LegacyError::Json(_))
        ));
    }

    #[test]
    fn dangling_optional_research_id_degrades_but_missing_field_still_fails() {
        let mut dangling: Value = serde_json::from_str(include_str!(
            "../../../../contracts/golden/domain/v1/desk_map_projection.json"
        ))
        .unwrap();
        dangling["research_context"] = Value::Null;
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(serde_json::to_string(&dangling).unwrap().as_bytes())
            .unwrap();

        let document = read_desk_map_projection(file.path(), 1_048_576).unwrap();
        assert!(document.projection.research_context.is_none());
        assert!(document.projection.research_context_document_id.is_none());
        assert_eq!(document.projection.quality, DeskDataQuality::Degraded);

        dangling.as_object_mut().unwrap().remove("research_context");
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(serde_json::to_string(&dangling).unwrap().as_bytes())
            .unwrap();
        assert!(matches!(
            read_desk_map_projection(file.path(), 1_048_576),
            Err(LegacyError::Json(_))
        ));
    }
}
