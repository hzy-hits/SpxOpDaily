use spx_domain::DomainError;
use thiserror::Error;

use crate::model::OwnerRole;

#[derive(Debug, Error)]
pub enum LedgerError {
    #[error("ledger I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("domain contract error: {0}")]
    Domain(#[from] DomainError),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("owner lease lost for {0}")]
    OwnerLeaseLost(OwnerRole),
    #[error("owner role mismatch: expected {expected}, got {actual}")]
    OwnerRoleMismatch {
        expected: OwnerRole,
        actual: OwnerRole,
    },
    #[error("claim lost for target {0}")]
    ClaimLost(String),
    #[error("immutable identity collision for {0}")]
    IdentityCollision(String),
    #[error("migration state is unsupported")]
    MigrationDrift,
    #[error("invalid timestamp")]
    InvalidTimestamp,
    #[error("invalid ledger value: {0}")]
    InvalidValue(&'static str),
}
