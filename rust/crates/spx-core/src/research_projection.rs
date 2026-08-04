use std::path::Path;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use spx_domain::{ResearchSignalsV1, Token, Validate};
use thiserror::Error;

use crate::projection::{ProjectionError, ProjectionStore};

const SCHEMA_VERSION: &str = "spx_latest_research_projection.v1";

#[derive(Debug, Error)]
pub enum ResearchProjectionError {
    #[error("research projection I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("research projection JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("research projection contract failed: {0}")]
    Domain(#[from] spx_domain::DomainError),
    #[error("research projection failed: {0}")]
    Projection(#[from] ProjectionError),
    #[error("research signal generated_at collision")]
    TimeCollision,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResearchDisposition {
    Updated,
    Unchanged,
    Stale,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LatestResearchProjection {
    schema_version: String,
    published_at: DateTime<Utc>,
    message_id: Token,
    signals: ResearchSignalsV1,
}

pub struct ResearchProjectionStore {
    store: ProjectionStore,
    latest: Option<LatestResearchProjection>,
}

impl ResearchProjectionStore {
    pub fn open(path: &Path) -> Result<Self, ResearchProjectionError> {
        let latest = match std::fs::read(path) {
            Ok(bytes) => {
                let value: LatestResearchProjection = serde_json::from_slice(&bytes)?;
                if value.schema_version != SCHEMA_VERSION {
                    return Err(spx_domain::DomainError::SchemaMismatch {
                        kind: "latest research projection",
                        expected: SCHEMA_VERSION,
                        actual: value.schema_version,
                    }
                    .into());
                }
                value.signals.validate()?;
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
        signals: ResearchSignalsV1,
        published_at: DateTime<Utc>,
    ) -> Result<ResearchDisposition, ResearchProjectionError> {
        if let Some(latest) = &self.latest {
            if signals.generated_at < latest.signals.generated_at {
                return Ok(ResearchDisposition::Stale);
            }
            if signals == latest.signals {
                return Ok(ResearchDisposition::Unchanged);
            }
            if signals.generated_at == latest.signals.generated_at {
                return Err(ResearchProjectionError::TimeCollision);
            }
        }
        let next = LatestResearchProjection {
            schema_version: SCHEMA_VERSION.to_owned(),
            published_at,
            message_id,
            signals,
        };
        self.store.publish(&next)?;
        self.latest = Some(next);
        Ok(ResearchDisposition::Updated)
    }
}
