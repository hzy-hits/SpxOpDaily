use std::collections::HashSet;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use spx_domain::{DeliveryChannel, DomainError, NotificationTargetV1, Token};
use thiserror::Error;

use crate::{ConfigError, ReportWriterConfig};

#[derive(Debug, Error)]
pub enum ServiceConfigError {
    #[error("failed to read report service config: {0}")]
    Read(#[from] std::io::Error),
    #[error("invalid report service TOML: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("invalid report writer config: {0}")]
    Writer(#[from] ConfigError),
    #[error("invalid report target contract: {0}")]
    Domain(#[from] DomainError),
    #[error("invalid report service config: {0}")]
    Invalid(&'static str),
}

/// Typed outbox target configured for every scheduled desk report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReportTargetConfig {
    pub key: String,
    pub channel: DeliveryChannel,
}

impl ReportTargetConfig {
    fn to_domain(&self) -> Result<NotificationTargetV1, DomainError> {
        Ok(NotificationTargetV1 {
            key: Token::new(self.key.clone(), "report target key")?,
            channel: self.channel,
        })
    }
}

/// Complete configuration for the quarter-hour GTH/RTH report service.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReportServiceConfig {
    pub projection_path: PathBuf,
    pub ledger_path: PathBuf,
    pub health_path: PathBuf,
    #[serde(default = "default_poll_interval_millis")]
    pub poll_interval_millis: u64,
    #[serde(default = "default_slot_grace_seconds")]
    pub slot_grace_seconds: i64,
    #[serde(default = "default_owner_lease_seconds")]
    pub owner_lease_seconds: i64,
    #[serde(default = "default_source_max_bytes")]
    pub source_max_bytes: u64,
    #[serde(default = "default_failure_backoff_seconds")]
    pub failure_backoff_seconds: Vec<i64>,
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    pub targets: Vec<ReportTargetConfig>,
    pub writer: ReportWriterConfig,
}

impl ReportServiceConfig {
    /// Loads and strictly validates one report service TOML file.
    ///
    /// # Errors
    ///
    /// Returns an error when the file, TOML, writer, path, timing, or target
    /// contract is invalid.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ServiceConfigError> {
        let contents = std::fs::read_to_string(path)?;
        Self::from_toml(&contents)
    }

    /// Strictly decodes one report service TOML document.
    ///
    /// # Errors
    ///
    /// Returns an error when the TOML contains unknown fields or violates a
    /// service invariant.
    pub fn from_toml(contents: &str) -> Result<Self, ServiceConfigError> {
        let config: Self = toml::from_str(contents)?;
        config.validate()?;
        Ok(config)
    }

    /// Validates path isolation, bounded timing, ownership, and typed targets.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceConfigError::Invalid`] or a target/domain error when an
    /// invariant is violated.
    pub fn validate(&self) -> Result<(), ServiceConfigError> {
        self.writer.validate()?;
        if [&self.projection_path, &self.ledger_path, &self.health_path]
            .iter()
            .any(|path| !path.is_absolute())
        {
            return Err(ServiceConfigError::Invalid(
                "all report runtime paths must be absolute",
            ));
        }
        if self.projection_path == self.ledger_path
            || self.projection_path == self.health_path
            || self.ledger_path == self.health_path
        {
            return Err(ServiceConfigError::Invalid(
                "projection, ledger, and health paths must differ",
            ));
        }
        if !(100..=60_000).contains(&self.poll_interval_millis) {
            return Err(ServiceConfigError::Invalid(
                "poll_interval_millis must be within 100..=60000",
            ));
        }
        if !(30..=300).contains(&self.slot_grace_seconds) {
            return Err(ServiceConfigError::Invalid(
                "slot_grace_seconds must be within 30..=300",
            ));
        }
        let grace_millis = u64::try_from(self.slot_grace_seconds)
            .map_err(|_| ServiceConfigError::Invalid("slot grace is too large"))?
            .saturating_mul(1_000);
        if self.poll_interval_millis > grace_millis {
            return Err(ServiceConfigError::Invalid(
                "poll interval must not exceed the slot grace window",
            ));
        }
        if !(30..=3_600).contains(&self.owner_lease_seconds) {
            return Err(ServiceConfigError::Invalid(
                "owner_lease_seconds must be within 30..=3600",
            ));
        }
        let request_timeout = i64::try_from(self.writer.request_timeout_seconds())
            .map_err(|_| ServiceConfigError::Invalid("writer timeout is too large"))?;
        if self.owner_lease_seconds <= request_timeout.saturating_add(10) {
            return Err(ServiceConfigError::Invalid(
                "owner lease must exceed writer timeout by more than 10 seconds",
            ));
        }
        if !(1_024..=16 * 1_024 * 1_024).contains(&self.source_max_bytes) {
            return Err(ServiceConfigError::Invalid(
                "source_max_bytes must be within 1024..=16777216",
            ));
        }
        if self.failure_backoff_seconds.is_empty() || self.failure_backoff_seconds.len() > 10 {
            return Err(ServiceConfigError::Invalid(
                "failure backoff must contain 1..=10 delays",
            ));
        }
        if self
            .failure_backoff_seconds
            .iter()
            .any(|delay| !(1..=3_600).contains(delay))
        {
            return Err(ServiceConfigError::Invalid(
                "failure backoff delays must be within 1..=3600 seconds",
            ));
        }
        if self
            .failure_backoff_seconds
            .windows(2)
            .any(|window| window[0] > window[1])
        {
            return Err(ServiceConfigError::Invalid(
                "failure backoff delays must be nondecreasing",
            ));
        }
        if !(1..=10).contains(&self.max_attempts) {
            return Err(ServiceConfigError::Invalid(
                "max_attempts must be within 1..=10",
            ));
        }
        if self.targets.is_empty() || self.targets.len() > 16 {
            return Err(ServiceConfigError::Invalid(
                "targets must contain 1..=16 entries",
            ));
        }
        let mut keys = HashSet::with_capacity(self.targets.len());
        for target in &self.targets {
            let typed = target.to_domain()?;
            if !keys.insert(typed.key.as_str().to_owned()) {
                return Err(ServiceConfigError::Invalid("target keys must be unique"));
            }
        }
        Ok(())
    }

    /// Builds the domain targets persisted into `NotificationIntentV2`.
    ///
    /// # Errors
    ///
    /// Returns a domain error if a target key is not a valid bounded token.
    pub fn domain_targets(&self) -> Result<Vec<NotificationTargetV1>, DomainError> {
        self.targets
            .iter()
            .map(ReportTargetConfig::to_domain)
            .collect()
    }
}

const fn default_poll_interval_millis() -> u64 {
    1_000
}

const fn default_slot_grace_seconds() -> i64 {
    180
}

const fn default_owner_lease_seconds() -> i64 {
    180
}

const fn default_source_max_bytes() -> u64 {
    4 * 1_024 * 1_024
}

fn default_failure_backoff_seconds() -> Vec<i64> {
    vec![5, 15, 60, 300]
}

const fn default_max_attempts() -> u32 {
    3
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_toml() -> &'static str {
        r#"
            projection_path = "/var/lib/spx-spark-core/latest/desk-map.json"
            ledger_path = "/var/lib/spx-spark-core/ledger/operations.sqlite"
            health_path = "/var/lib/spx-spark-report/health.json"
            poll_interval_millis = 1000
            slot_grace_seconds = 180
            owner_lease_seconds = 180
            source_max_bytes = 4194304
            failure_backoff_seconds = [5, 15, 60, 300]
            max_attempts = 3
            targets = [
              { key = "bark-primary", channel = "bark" },
              { key = "feishu-primary", channel = "feishu" },
            ]

            [writer]
            network_enabled = false
            api_key_env = "DEEPSEEK_API_KEY"
            max_tokens = 64000
            request_timeout_seconds = 90
        "#
    }

    #[test]
    fn strict_config_builds_typed_targets() {
        let config = ReportServiceConfig::from_toml(valid_toml()).unwrap();
        assert_eq!(config.slot_grace_seconds, 180);
        let targets = config.domain_targets().unwrap();
        assert_eq!(targets.len(), 2);
        assert_eq!(targets[0].channel, DeliveryChannel::Bark);
    }

    #[test]
    fn rejects_unknown_fields_duplicate_targets_and_short_owner_lease() {
        let unknown = format!("{}\nunknown = true", valid_toml());
        assert!(ReportServiceConfig::from_toml(&unknown).is_err());

        let duplicate = valid_toml().replace("feishu-primary", "bark-primary");
        assert!(matches!(
            ReportServiceConfig::from_toml(&duplicate),
            Err(ServiceConfigError::Invalid("target keys must be unique"))
        ));

        let short_lease =
            valid_toml().replace("owner_lease_seconds = 180", "owner_lease_seconds = 90");
        assert!(matches!(
            ReportServiceConfig::from_toml(&short_lease),
            Err(ServiceConfigError::Invalid(
                "owner lease must exceed writer timeout by more than 10 seconds"
            ))
        ));
    }
}
