use std::path::Path;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use spx_domain::{StrategyDistributionForecastV1, Token, Validate};
use thiserror::Error;

use crate::projection::{ProjectionError, ProjectionStore};

pub const LATEST_STRATEGY_DISTRIBUTION_PROJECTION_SCHEMA_VERSION: &str =
    "spx_latest_strategy_distribution_projection.v1";

#[derive(Debug, Error)]
pub enum StrategyDistributionProjectionError {
    #[error("strategy distribution projection I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("strategy distribution projection JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("strategy distribution projection contract failed: {0}")]
    Domain(#[from] spx_domain::DomainError),
    #[error("strategy distribution projection write failed: {0}")]
    Projection(#[from] ProjectionError),
    #[error("strategy distribution document_id collision")]
    IdentityCollision,
    #[error("strategy distribution available_at collision")]
    TimeCollision,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyDistributionDisposition {
    Updated,
    Unchanged,
    Stale,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LatestStrategyDistributionProjectionV1 {
    pub schema_version: String,
    pub published_at: DateTime<Utc>,
    pub message_id: Token,
    pub forecast: StrategyDistributionForecastV1,
}

impl LatestStrategyDistributionProjectionV1 {
    /// Validates the latest-projection wrapper and embedded research forecast.
    ///
    /// # Errors
    ///
    /// Returns a domain error when the wrapper schema, embedded forecast, or
    /// publication time ordering is invalid.
    pub fn validate(&self) -> Result<(), spx_domain::DomainError> {
        if self.schema_version != LATEST_STRATEGY_DISTRIBUTION_PROJECTION_SCHEMA_VERSION {
            return Err(spx_domain::DomainError::SchemaMismatch {
                kind: "latest strategy distribution projection",
                expected: LATEST_STRATEGY_DISTRIBUTION_PROJECTION_SCHEMA_VERSION,
                actual: self.schema_version.clone(),
            });
        }
        self.forecast.validate()?;
        if self.forecast.available_at > self.published_at {
            return Err(spx_domain::DomainError::TimeOrder(
                "strategy distribution available_at is after published_at",
            ));
        }
        Ok(())
    }
}

pub struct StrategyDistributionProjectionStore {
    store: ProjectionStore,
    latest: Option<LatestStrategyDistributionProjectionV1>,
}

impl StrategyDistributionProjectionStore {
    pub fn open(path: &Path) -> Result<Self, StrategyDistributionProjectionError> {
        let latest = match std::fs::read(path) {
            Ok(bytes) => {
                let value: LatestStrategyDistributionProjectionV1 = serde_json::from_slice(&bytes)?;
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
        forecast: StrategyDistributionForecastV1,
        published_at: DateTime<Utc>,
    ) -> Result<StrategyDistributionDisposition, StrategyDistributionProjectionError> {
        forecast.validate()?;
        if forecast.valid_until <= published_at {
            return Ok(StrategyDistributionDisposition::Stale);
        }
        if let Some(latest) = &self.latest {
            if forecast.document_id == latest.forecast.document_id {
                if forecast == latest.forecast {
                    return Ok(StrategyDistributionDisposition::Unchanged);
                }
                return Err(StrategyDistributionProjectionError::IdentityCollision);
            }
            if forecast.available_at < latest.forecast.available_at {
                return Ok(StrategyDistributionDisposition::Stale);
            }
            if forecast.available_at == latest.forecast.available_at {
                return Err(StrategyDistributionProjectionError::TimeCollision);
            }
        }
        let next = LatestStrategyDistributionProjectionV1 {
            schema_version: LATEST_STRATEGY_DISTRIBUTION_PROJECTION_SCHEMA_VERSION.to_owned(),
            published_at,
            message_id,
            forecast,
        };
        next.validate()?;
        self.store.publish(&next)?;
        self.latest = Some(next);
        Ok(StrategyDistributionDisposition::Updated)
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeDelta;
    use tempfile::TempDir;

    use super::*;

    fn forecast() -> StrategyDistributionForecastV1 {
        serde_json::from_str(include_str!(
            "../../../../contracts/golden/domain/v1/strategy_distribution_forecast.json"
        ))
        .expect("strategy distribution fixture")
    }

    #[test]
    fn persists_reopens_and_deduplicates_by_document_and_available_time() {
        let temp = TempDir::new().expect("temp directory");
        let path = temp.path().join("strategy-distribution.json");
        let forecast = forecast();
        let published_at = forecast.available_at.with_timezone(&Utc) + TimeDelta::seconds(1);
        let mut store = StrategyDistributionProjectionStore::open(&path).expect("open store");
        assert_eq!(
            store
                .apply(
                    Token::new("message:strategy-distribution:1", "message").unwrap(),
                    forecast.clone(),
                    published_at,
                )
                .expect("first forecast is accepted"),
            StrategyDistributionDisposition::Updated
        );
        assert_eq!(
            store
                .apply(
                    Token::new("message:strategy-distribution:1", "message").unwrap(),
                    forecast.clone(),
                    published_at,
                )
                .expect("identical forecast is unchanged"),
            StrategyDistributionDisposition::Unchanged
        );

        let mut identity_collision = forecast.clone();
        identity_collision.source_snapshot_id =
            Token::new("analytical-option-snapshot:other", "snapshot").unwrap();
        assert!(matches!(
            store.apply(
                Token::new("message:strategy-distribution:2", "message").unwrap(),
                identity_collision,
                published_at,
            ),
            Err(StrategyDistributionProjectionError::IdentityCollision)
        ));

        let mut older = forecast.clone();
        older.document_id =
            Token::new("strategy-distribution:2026-08-05:142959:1", "document").unwrap();
        older.observed_through -= TimeDelta::seconds(2);
        older.available_at -= TimeDelta::seconds(1);
        assert_eq!(
            store
                .apply(
                    Token::new("message:strategy-distribution:3", "message").unwrap(),
                    older,
                    published_at,
                )
                .expect("older forecast is stale"),
            StrategyDistributionDisposition::Stale
        );

        let mut time_collision = forecast;
        time_collision.document_id =
            Token::new("strategy-distribution:2026-08-05:143001:1", "document").unwrap();
        assert!(matches!(
            store.apply(
                Token::new("message:strategy-distribution:4", "message").unwrap(),
                time_collision,
                published_at,
            ),
            Err(StrategyDistributionProjectionError::TimeCollision)
        ));

        let reopened = StrategyDistributionProjectionStore::open(&path).expect("reopen store");
        assert_eq!(
            reopened
                .latest
                .expect("persisted latest forecast")
                .forecast
                .document_id
                .as_str(),
            "strategy-distribution:2026-08-05:143000:1"
        );
    }
}
