use std::fmt::{Display, Formatter};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
use sha2::{Digest, Sha256};
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum DomainError {
    #[error("{field} must be non-empty")]
    Empty { field: &'static str },
    #[error("{field} must be finite and positive")]
    NotPositive { field: &'static str },
    #[error("{field} must be finite and non-negative")]
    Negative { field: &'static str },
    #[error("schema mismatch for {kind}: expected {expected}, got {actual}")]
    SchemaMismatch {
        kind: &'static str,
        expected: &'static str,
        actual: String,
    },
    #[error("invalid {field}: {reason}")]
    Invalid {
        field: &'static str,
        reason: &'static str,
    },
    #[error("time ordering violation: {0}")]
    TimeOrder(&'static str),
    #[error("provider mismatch")]
    ProviderMismatch,
    #[error("duplicate value in {0}")]
    Duplicate(&'static str),
    #[error("JSON serialization failed: {0}")]
    Json(String),
}

pub trait Validate {
    /// Verifies all cross-field and semantic invariants for the contract.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError`] when the value cannot safely cross a domain boundary.
    fn validate(&self) -> Result<(), DomainError>;
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Token(String);

impl Token {
    /// Creates a non-empty, NUL-free domain token.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError`] when the value is empty, oversized, or contains a NUL byte.
    pub fn new(value: impl Into<String>, field: &'static str) -> Result<Self, DomainError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(DomainError::Empty { field });
        }
        if value.contains('\0') {
            return Err(DomainError::Invalid {
                field,
                reason: "NUL is forbidden",
            });
        }
        if value.len() > 4_096 {
            return Err(DomainError::Invalid {
                field,
                reason: "must not exceed 4096 UTF-8 bytes",
            });
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for Token {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Serialize for Token {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for Token {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value, "token").map_err(D::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct PositiveF64(f64);

impl PositiveF64 {
    /// Creates a finite, strictly positive floating-point value.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError`] for zero, negative, NaN, or infinite input.
    pub fn new(value: f64, field: &'static str) -> Result<Self, DomainError> {
        if !value.is_finite() || value <= 0.0 {
            return Err(DomainError::NotPositive { field });
        }
        Ok(Self(value))
    }

    pub const fn get(self) -> f64 {
        self.0
    }
}

impl Serialize for PositiveF64 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_f64(self.0)
    }
}

impl<'de> Deserialize<'de> for PositiveF64 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = f64::deserialize(deserializer)?;
        Self::new(value, "positive number").map_err(D::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct NonNegativeF64(f64);

impl NonNegativeF64 {
    /// Creates a finite, non-negative floating-point value.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError`] for negative, NaN, or infinite input.
    pub fn new(value: f64, field: &'static str) -> Result<Self, DomainError> {
        if !value.is_finite() || value < 0.0 {
            return Err(DomainError::Negative { field });
        }
        Ok(Self(value))
    }

    pub const fn get(self) -> f64 {
        self.0
    }
}

impl Serialize for NonNegativeF64 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_f64(self.0)
    }
}

impl<'de> Deserialize<'de> for NonNegativeF64 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = f64::deserialize(deserializer)?;
        Self::new(value, "non-negative number").map_err(D::Error::custom)
    }
}

/// Hashes a contract's deterministic serde JSON encoding with SHA-256.
///
/// # Errors
///
/// Returns [`DomainError::Json`] when the value cannot be serialized.
pub fn canonical_json_hash<T: Serialize>(value: &T) -> Result<String, DomainError> {
    let bytes = serde_json::to_vec(value).map_err(|error| DomainError::Json(error.to_string()))?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

pub(crate) fn require_schema(
    actual: &str,
    expected: &'static str,
    kind: &'static str,
) -> Result<(), DomainError> {
    if actual == expected {
        Ok(())
    } else {
        Err(DomainError::SchemaMismatch {
            kind,
            expected,
            actual: actual.to_owned(),
        })
    }
}

pub(crate) fn unique_tokens(values: &[Token], field: &'static str) -> Result<(), DomainError> {
    let mut sorted = values.to_vec();
    sorted.sort();
    sorted.dedup();
    if sorted.len() == values.len() {
        Ok(())
    } else {
        Err(DomainError::Duplicate(field))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_size_is_bounded() {
        assert!(Token::new("x".repeat(4_096), "test").is_ok());
        assert!(matches!(
            Token::new("x".repeat(4_097), "test"),
            Err(DomainError::Invalid {
                field: "test",
                reason: "must not exceed 4096 UTF-8 bytes"
            })
        ));
    }
}
