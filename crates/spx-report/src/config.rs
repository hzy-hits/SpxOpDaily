use std::num::NonZeroU32;
use std::path::Path;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

const MAX_DEEPSEEK_OUTPUT_TOKENS: u32 = 384_000;
const DEFAULT_REQUEST_TIMEOUT_SECONDS: u64 = 90;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read report writer config: {0}")]
    Read(#[from] std::io::Error),
    #[error("invalid report writer TOML: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("invalid report writer config: {0}")]
    Invalid(&'static str),
}

/// Non-zero, provider-bounded completion token budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct MaxTokens(NonZeroU32);

impl MaxTokens {
    /// Creates a validated completion token budget.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Invalid`] when the value is zero or exceeds the
    /// supported `DeepSeek` completion limit.
    pub fn new(value: u32) -> Result<Self, ConfigError> {
        let value = NonZeroU32::new(value)
            .ok_or(ConfigError::Invalid("max_tokens must be greater than zero"))?;
        if value.get() > MAX_DEEPSEEK_OUTPUT_TOKENS {
            return Err(ConfigError::Invalid("max_tokens must not exceed 384000"));
        }
        Ok(Self(value))
    }

    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

impl Serialize for MaxTokens {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u32(self.get())
    }
}

impl<'de> Deserialize<'de> for MaxTokens {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u32::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

/// Validated environment variable name used to resolve the API key at send time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiKeyEnvironment(String);

impl ApiKeyEnvironment {
    /// Creates an API-key environment reference without reading its value.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Invalid`] when `value` is not an uppercase environment
    /// variable name.
    pub fn new(value: impl Into<String>) -> Result<Self, ConfigError> {
        let value = value.into();
        if !valid_environment_name(&value) {
            return Err(ConfigError::Invalid(
                "api_key_env must be an uppercase environment variable name",
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Serialize for ApiKeyEnvironment {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ApiKeyEnvironment {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

/// Runtime configuration for the `DeepSeek` report writer.
///
/// The provider URL and model are deliberately not configurable. This prevents a
/// deployment override from sending the API key to another host or selecting a model
/// with different response semantics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReportWriterConfig {
    #[serde(default)]
    network_enabled: bool,
    api_key_env: ApiKeyEnvironment,
    max_tokens: MaxTokens,
    #[serde(default = "default_request_timeout_seconds")]
    request_timeout_seconds: u64,
}

impl ReportWriterConfig {
    /// Decodes and validates a report writer TOML document.
    ///
    /// # Errors
    ///
    /// Returns an error when TOML decoding or a configuration invariant fails.
    pub fn from_toml(contents: &str) -> Result<Self, ConfigError> {
        let config: Self = toml::from_str(contents)?;
        config.validate()?;
        Ok(config)
    }

    /// Loads and validates a report writer TOML file.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be read, TOML cannot be decoded, or a
    /// configuration invariant fails.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        Self::from_toml(&std::fs::read_to_string(path)?)
    }

    /// Checks the bounded timeout and token invariants.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Invalid`] when an invariant is violated.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if !(1..=300).contains(&self.request_timeout_seconds) {
            return Err(ConfigError::Invalid(
                "request_timeout_seconds must be within 1..=300",
            ));
        }
        MaxTokens::new(self.max_tokens.get())?;
        ApiKeyEnvironment::new(self.api_key_env.as_str())?;
        Ok(())
    }

    pub const fn network_enabled(&self) -> bool {
        self.network_enabled
    }

    pub const fn max_tokens(&self) -> MaxTokens {
        self.max_tokens
    }

    pub const fn request_timeout_seconds(&self) -> u64 {
        self.request_timeout_seconds
    }

    pub fn api_key_env(&self) -> &ApiKeyEnvironment {
        &self.api_key_env
    }
}

const fn default_request_timeout_seconds() -> u64 {
    DEFAULT_REQUEST_TIMEOUT_SECONDS
}

fn valid_environment_name(value: &str) -> bool {
    let mut bytes = value.bytes();
    matches!(bytes.next(), Some(b'A'..=b'Z' | b'_'))
        && bytes.all(|byte| matches!(byte, b'A'..=b'Z' | b'0'..=b'9' | b'_'))
}
