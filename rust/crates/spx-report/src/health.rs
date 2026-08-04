use std::fmt::Write as _;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

pub const REPORT_HEALTH_SCHEMA_VERSION: &str = "spx_report_health.v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReportPhase {
    Starting,
    AwaitingSlot,
    AwaitingProjection,
    Generating,
    Backoff,
    Duplicate,
    Persisted,
    Degraded,
    Stopped,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReportCounters {
    pub ticks: u64,
    pub source_failures: u64,
    pub awaiting_current_projection: u64,
    pub ineligible_projections: u64,
    pub duplicate_slots: u64,
    pub generation_attempts: u64,
    pub generation_failures: u64,
    pub persisted_reports: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReportHealth {
    pub schema_version: String,
    pub updated_at: DateTime<Utc>,
    pub phase: ReportPhase,
    pub network_authorized: bool,
    pub projection_path: String,
    pub last_projection_id: Option<String>,
    pub active_slot: Option<String>,
    pub last_persisted_at: Option<DateTime<Utc>>,
    pub next_attempt_at: Option<DateTime<Utc>>,
    pub consecutive_generation_failures: u32,
    pub last_error_code: Option<String>,
    pub last_response_model: Option<String>,
    pub last_finish_reason: Option<String>,
    pub last_visible_content_bytes: Option<usize>,
    pub last_response_sha256: Option<String>,
    pub counters: ReportCounters,
}

impl ReportHealth {
    pub fn new(projection_path: &Path, network_authorized: bool, now: DateTime<Utc>) -> Self {
        Self {
            schema_version: REPORT_HEALTH_SCHEMA_VERSION.to_owned(),
            updated_at: now,
            phase: ReportPhase::Starting,
            network_authorized,
            projection_path: projection_path.display().to_string(),
            last_projection_id: None,
            active_slot: None,
            last_persisted_at: None,
            next_attempt_at: None,
            consecutive_generation_failures: 0,
            last_error_code: None,
            last_response_model: None,
            last_finish_reason: None,
            last_visible_content_bytes: None,
            last_response_sha256: None,
            counters: ReportCounters::default(),
        }
    }

    /// Starts fresh runtime state while carrying forward only historical diagnostics.
    ///
    /// A missing file is the first-start case. Malformed or incompatible health is preserved and
    /// returned as an error instead of being silently overwritten.
    pub(crate) fn start_from_persisted(
        health_path: &Path,
        projection_path: &Path,
        network_authorized: bool,
        now: DateTime<Utc>,
    ) -> Result<Self, HealthError> {
        let mut health = Self::new(projection_path, network_authorized, now);
        let persisted = match Self::load(health_path) {
            Ok(persisted) => persisted,
            Err(HealthError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(health);
            }
            Err(error) => return Err(error),
        };

        health.last_projection_id = persisted.last_projection_id;
        health.last_persisted_at = persisted.last_persisted_at;
        health.last_error_code = persisted.last_error_code;
        health.last_response_model = persisted.last_response_model;
        health.last_finish_reason = persisted.last_finish_reason;
        health.last_visible_content_bytes = persisted.last_visible_content_bytes;
        health.last_response_sha256 = persisted.last_response_sha256;
        health.counters = persisted.counters;
        Ok(health)
    }

    /// Atomically publishes non-sensitive report service health.
    ///
    /// # Errors
    ///
    /// Returns an error if the bounded JSON projection cannot be durably replaced.
    pub fn persist(&self, path: &Path) -> Result<(), HealthError> {
        let parent = path.parent().ok_or(HealthError::MissingParent)?;
        std::fs::create_dir_all(parent)?;
        let temporary_path = temporary_path(path);
        let result = (|| {
            let mut options = std::fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt as _;
                options.mode(0o600);
            }
            let mut file = options.open(&temporary_path)?;
            let payload = serde_json::to_vec_pretty(self)?;
            file.write_all(&payload)?;
            file.write_all(b"\n")?;
            file.sync_all()?;
            std::fs::rename(&temporary_path, path)?;
            std::fs::File::open(parent)?.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(&temporary_path);
        }
        result
    }

    /// Reads and checks a published health projection.
    ///
    /// # Errors
    ///
    /// Returns an error for unreadable, malformed, or unsupported health data.
    pub fn load(path: &Path) -> Result<Self, HealthError> {
        let health: Self = serde_json::from_slice(&std::fs::read(path)?)?;
        if health.schema_version != REPORT_HEALTH_SCHEMA_VERSION {
            return Err(HealthError::SchemaMismatch);
        }
        Ok(health)
    }
}

fn temporary_path(path: &Path) -> PathBuf {
    let mut name = path.file_name().map_or_else(
        || "health".to_owned(),
        |name| name.to_string_lossy().into_owned(),
    );
    write!(name, ".tmp-{}", Uuid::new_v4()).expect("writing to a String cannot fail");
    path.with_file_name(name)
}

#[derive(Debug, Error)]
pub enum HealthError {
    #[error("report health path has no parent")]
    MissingParent,
    #[error("report health I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("report health JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("report health schema is unsupported")]
    SchemaMismatch,
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone as _;
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn health_round_trips_without_report_content() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("health/report.json");
        let now = Utc.with_ymd_and_hms(2026, 8, 4, 14, 0, 0).unwrap();
        let mut health = ReportHealth::new(Path::new("/core/desk-map.json"), true, now);
        health.phase = ReportPhase::Persisted;
        health.last_response_sha256 = Some("a".repeat(64));
        health.persist(&path).unwrap();

        assert_eq!(ReportHealth::load(&path).unwrap(), health);
        let raw = std::fs::read_to_string(path).unwrap();
        assert!(!raw.contains("desk_view"));
        assert!(!raw.contains("reasoning_content"));
    }
}
