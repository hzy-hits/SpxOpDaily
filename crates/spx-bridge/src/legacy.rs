use std::fs::File;
use std::io::Read;
use std::path::Path;

use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
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
}
