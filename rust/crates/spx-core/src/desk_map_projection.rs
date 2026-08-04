use std::path::Path;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use spx_domain::{DeskMapProjectionV1, Token, Validate};
use thiserror::Error;

use crate::projection::{ProjectionError, ProjectionStore};

pub const LATEST_DESK_MAP_PROJECTION_SCHEMA_VERSION: &str = "spx_latest_desk_map_projection.v1";

#[derive(Debug, Error)]
pub enum DeskMapProjectionError {
    #[error("desk map projection I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("desk map projection JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("desk map projection contract failed: {0}")]
    Domain(#[from] spx_domain::DomainError),
    #[error("desk map projection write failed: {0}")]
    Projection(#[from] ProjectionError),
    #[error("desk map available_at collision")]
    TimeCollision,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeskMapDisposition {
    Updated,
    Unchanged,
    Stale,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LatestDeskMapProjectionV1 {
    pub schema_version: String,
    pub published_at: DateTime<Utc>,
    pub message_id: Token,
    pub projection: DeskMapProjectionV1,
}

impl LatestDeskMapProjectionV1 {
    /// Validates the latest-projection wrapper and embedded desk map contract.
    ///
    /// # Errors
    ///
    /// Returns a domain error when the wrapper schema, embedded projection, or
    /// publication time ordering is invalid.
    pub fn validate(&self) -> Result<(), spx_domain::DomainError> {
        if self.schema_version != LATEST_DESK_MAP_PROJECTION_SCHEMA_VERSION {
            return Err(spx_domain::DomainError::SchemaMismatch {
                kind: "latest desk map projection",
                expected: LATEST_DESK_MAP_PROJECTION_SCHEMA_VERSION,
                actual: self.schema_version.clone(),
            });
        }
        self.projection.validate()?;
        if self.projection.available_at > self.published_at {
            return Err(spx_domain::DomainError::TimeOrder(
                "desk map available_at is after published_at",
            ));
        }
        Ok(())
    }
}

pub struct DeskMapProjectionStore {
    store: ProjectionStore,
    latest: Option<LatestDeskMapProjectionV1>,
}

impl DeskMapProjectionStore {
    pub fn open(path: &Path) -> Result<Self, DeskMapProjectionError> {
        let latest = match std::fs::read(path) {
            Ok(bytes) => {
                let value: LatestDeskMapProjectionV1 = serde_json::from_slice(&bytes)?;
                value.validate()?;
                Some(value)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        Ok(Self {
            store: ProjectionStore::new(path),
            latest,
        })
    }

    pub fn apply(
        &mut self,
        message_id: Token,
        projection: DeskMapProjectionV1,
        published_at: DateTime<Utc>,
    ) -> Result<DeskMapDisposition, DeskMapProjectionError> {
        projection.validate()?;
        if projection.valid_until <= published_at {
            return Ok(DeskMapDisposition::Stale);
        }
        if let Some(latest) = &self.latest {
            if projection.available_at < latest.projection.available_at {
                return Ok(DeskMapDisposition::Stale);
            }
            if projection == latest.projection {
                return Ok(DeskMapDisposition::Unchanged);
            }
            if projection.available_at == latest.projection.available_at {
                return Err(DeskMapProjectionError::TimeCollision);
            }
        }
        let next = LatestDeskMapProjectionV1 {
            schema_version: LATEST_DESK_MAP_PROJECTION_SCHEMA_VERSION.to_owned(),
            published_at,
            message_id,
            projection,
        };
        next.validate()?;
        self.store.publish(&next)?;
        self.latest = Some(next);
        Ok(DeskMapDisposition::Updated)
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone as _, Utc};
    use tempfile::TempDir;

    use super::*;

    fn projection(at: DateTime<Utc>) -> DeskMapProjectionV1 {
        serde_json::from_value(serde_json::json!({
            "schema_version": "desk_map_projection.v1",
            "projection_id": "desk-map:test",
            "source_snapshot_id": "snapshot:test",
            "source_slot": "2026-08-04:10:00",
            "trading_date_et": "2026-08-04",
            "session": "rth",
            "observed_through": at,
            "available_at": at,
            "valid_until": at + chrono::TimeDelta::minutes(20),
            "structure_fingerprint": "a".repeat(64),
            "stage": "confirmed",
            "phase": "confirmed",
            "direction": "up",
            "thesis": "breakout",
            "level_kind": "flip_high",
            "level": 7510.0,
            "quality": "ready",
            "quality_reasons": [],
            "research_context_document_id": null,
            "research_context": null,
            "action_authority": "none",
            "automatic_ordering": false,
            "message": {
                "title": "SPX Desk Map",
                "desk_view": "Call breakout confirmed",
                "location": "SPX 7512",
                "structure": "Put Flip Call",
                "primary_path": "Hold above 7510",
                "alternative_path": "Reject below 7510",
                "targets": "7550",
                "execution": "Wait for exact leg",
                "data_quality": "Ready"
            }
        }))
        .unwrap()
    }

    #[test]
    fn persists_latest_and_rejects_same_time_collision() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("desk-map.json");
        let at = Utc.with_ymd_and_hms(2026, 8, 4, 14, 0, 0).unwrap();
        let mut store = DeskMapProjectionStore::open(&path).unwrap();
        assert_eq!(
            store
                .apply(
                    Token::new("message:1", "message").unwrap(),
                    projection(at),
                    at
                )
                .unwrap(),
            DeskMapDisposition::Updated
        );
        assert_eq!(
            store
                .apply(
                    Token::new("message:1", "message").unwrap(),
                    projection(at),
                    at
                )
                .unwrap(),
            DeskMapDisposition::Unchanged
        );
        let mut collision = projection(at);
        collision.projection_id = Token::new("desk-map:other", "projection").unwrap();
        assert!(matches!(
            store.apply(Token::new("message:2", "message").unwrap(), collision, at),
            Err(DeskMapProjectionError::TimeCollision)
        ));
        DeskMapProjectionStore::open(&path).unwrap();
    }

    #[test]
    fn expired_projection_is_stale_and_never_replaces_latest() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("desk-map.json");
        let at = Utc.with_ymd_and_hms(2026, 8, 4, 14, 0, 0).unwrap();
        let mut store = DeskMapProjectionStore::open(&path).unwrap();
        let accepted = projection(at);
        assert_eq!(
            store
                .apply(
                    Token::new("message:accepted", "message").unwrap(),
                    accepted.clone(),
                    at
                )
                .unwrap(),
            DeskMapDisposition::Updated
        );
        let persisted_before = std::fs::read(&path).unwrap();

        let expired = projection(at + chrono::TimeDelta::hours(1));
        assert_eq!(
            store
                .apply(
                    Token::new("message:expired", "message").unwrap(),
                    expired,
                    at + chrono::TimeDelta::hours(2)
                )
                .unwrap(),
            DeskMapDisposition::Stale
        );
        assert_eq!(std::fs::read(&path).unwrap(), persisted_before);

        let reopened = DeskMapProjectionStore::open(&path).unwrap();
        assert_eq!(reopened.latest.unwrap().projection, accepted);
    }
}
