use std::collections::HashSet;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use spx_domain::{DomainError, Token};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read delivery config: {0}")]
    Read(#[from] std::io::Error),
    #[error("invalid delivery TOML: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("invalid delivery config: {0}")]
    Invalid(&'static str),
    #[error("invalid delivery target contract: {0}")]
    Domain(#[from] DomainError),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryConfig {
    pub ledger_path: PathBuf,
    #[serde(default)]
    pub network_enabled: bool,
    #[serde(default = "default_poll_millis")]
    pub poll_interval_millis: u64,
    #[serde(default = "default_owner_lease_seconds")]
    pub owner_lease_seconds: i64,
    #[serde(default = "default_claim_lease_seconds")]
    pub claim_lease_seconds: i64,
    #[serde(default = "default_request_timeout_seconds")]
    pub request_timeout_seconds: u64,
    #[serde(default = "default_retry_schedule")]
    pub retry_schedule_seconds: Vec<u64>,
    #[serde(default)]
    pub targets: Vec<TargetConfig>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BarkPresentation {
    /// Backward-compatible body presentation: preserve every line up to the safe transport cap.
    #[default]
    Full,
    /// Compact lock-screen presentation: first four non-empty, lightly normalized lines.
    Summary,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum TargetConfig {
    Bark {
        key: String,
        endpoint_env: String,
        #[serde(default)]
        presentation: BarkPresentation,
    },
    Feishu {
        key: String,
        endpoint_env: String,
        #[serde(default)]
        secret_env: Option<String>,
    },
    Webhook {
        key: String,
        endpoint_env: String,
        #[serde(default)]
        bearer_token_env: Option<String>,
    },
}

impl TargetConfig {
    pub fn key(&self) -> &str {
        match self {
            Self::Bark { key, .. } | Self::Feishu { key, .. } | Self::Webhook { key, .. } => key,
        }
    }

    fn endpoint_env(&self) -> &str {
        match self {
            Self::Bark { endpoint_env, .. }
            | Self::Feishu { endpoint_env, .. }
            | Self::Webhook { endpoint_env, .. } => endpoint_env,
        }
    }

    fn validate(&self) -> Result<(), ConfigError> {
        Token::new(self.key().to_owned(), "delivery target key")?;
        if !valid_environment_name(self.endpoint_env()) {
            return Err(ConfigError::Invalid(
                "target endpoint_env must be an environment variable name",
            ));
        }
        if let Self::Webhook {
            bearer_token_env: Some(name),
            ..
        } = self
            && !valid_environment_name(name)
        {
            return Err(ConfigError::Invalid(
                "webhook bearer_token_env must be an environment variable name",
            ));
        }
        if let Self::Feishu {
            secret_env: Some(name),
            ..
        } = self
            && !valid_environment_name(name)
        {
            return Err(ConfigError::Invalid(
                "feishu secret_env must be an environment variable name",
            ));
        }
        Ok(())
    }
}

impl DeliveryConfig {
    /// Loads TOML, applies the bounded delivery environment overrides, and validates it.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be read, TOML cannot be decoded, or the
    /// resulting configuration violates a delivery invariant.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let contents = std::fs::read_to_string(path)?;
        let mut config: Self = toml::from_str(&contents)?;
        if let Some(path) = std::env::var_os("SPX_DELIVERY_LEDGER_PATH") {
            config.ledger_path = PathBuf::from(path);
        }
        if let Ok(value) = std::env::var("SPX_DELIVERY_NETWORK_ENABLED") {
            config.network_enabled = parse_bool(&value)?;
        }
        config.validate()?;
        Ok(config)
    }

    /// Checks lease, timeout, retry, target, and network-safety invariants.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Invalid`] when any invariant is violated.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.ledger_path.as_os_str().is_empty() {
            return Err(ConfigError::Invalid("ledger_path must be non-empty"));
        }
        if !(1..=5_000).contains(&self.poll_interval_millis) {
            return Err(ConfigError::Invalid(
                "poll_interval_millis must be within 1..=5000",
            ));
        }
        if !(5..=3_600).contains(&self.owner_lease_seconds) {
            return Err(ConfigError::Invalid(
                "owner_lease_seconds must be within 5..=3600",
            ));
        }
        if !(2..=300).contains(&self.claim_lease_seconds) {
            return Err(ConfigError::Invalid(
                "claim_lease_seconds must be within 2..=300",
            ));
        }
        let request_timeout = i64::try_from(self.request_timeout_seconds)
            .map_err(|_| ConfigError::Invalid("request timeout is too large"))?;
        if !(1..=120).contains(&request_timeout) || request_timeout >= self.claim_lease_seconds {
            return Err(ConfigError::Invalid(
                "request timeout must be within 1..=120 and shorter than claim lease",
            ));
        }
        if self.claim_lease_seconds >= self.owner_lease_seconds {
            return Err(ConfigError::Invalid(
                "claim lease must be shorter than owner lease",
            ));
        }
        if self.retry_schedule_seconds.is_empty() || self.retry_schedule_seconds.len() > 10 {
            return Err(ConfigError::Invalid(
                "retry schedule must contain 1..=10 delays",
            ));
        }
        if self
            .retry_schedule_seconds
            .iter()
            .any(|delay| !(1..=86_400).contains(delay))
        {
            return Err(ConfigError::Invalid(
                "retry schedule delays must be within 1..=86400 seconds",
            ));
        }
        if self
            .retry_schedule_seconds
            .windows(2)
            .any(|window| window[0] >= window[1])
        {
            return Err(ConfigError::Invalid(
                "retry schedule delays must strictly increase",
            ));
        }
        let mut keys = HashSet::new();
        for target in &self.targets {
            target.validate()?;
            if !keys.insert(target.key()) {
                return Err(ConfigError::Invalid("target keys must be unique"));
            }
        }
        if self.network_enabled && self.targets.is_empty() {
            return Err(ConfigError::Invalid(
                "network-enabled delivery requires at least one target",
            ));
        }
        Ok(())
    }
}

fn valid_environment_name(value: &str) -> bool {
    let mut bytes = value.bytes();
    matches!(bytes.next(), Some(b'A'..=b'Z' | b'_'))
        && bytes.all(|byte| matches!(byte, b'A'..=b'Z' | b'0'..=b'9' | b'_'))
}

fn parse_bool(value: &str) -> Result<bool, ConfigError> {
    match value {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        _ => Err(ConfigError::Invalid(
            "SPX_DELIVERY_NETWORK_ENABLED must be true/false/1/0",
        )),
    }
}

const fn default_poll_millis() -> u64 {
    500
}

const fn default_owner_lease_seconds() -> i64 {
    60
}

const fn default_claim_lease_seconds() -> i64 {
    30
}

const fn default_request_timeout_seconds() -> u64 {
    10
}

fn default_retry_schedule() -> Vec<u64> {
    vec![15, 60, 300, 900]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn network_is_disabled_when_omitted() {
        let config: DeliveryConfig = toml::from_str(
            r#"
                ledger_path = "/tmp/ledger.sqlite"
            "#,
        )
        .unwrap();
        assert!(!config.network_enabled);
        config.validate().unwrap();
    }

    #[test]
    fn bark_presentation_is_typed_and_legacy_config_defaults_to_full() {
        let legacy: DeliveryConfig = toml::from_str(
            r#"
                ledger_path = "/tmp/ledger.sqlite"

                [[targets]]
                type = "bark"
                key = "bark-primary"
                endpoint_env = "SPX_BARK_ENDPOINT"
            "#,
        )
        .unwrap();
        assert!(matches!(
            legacy.targets.as_slice(),
            [TargetConfig::Bark {
                presentation: BarkPresentation::Full,
                ..
            }]
        ));

        let summary: DeliveryConfig = toml::from_str(
            r#"
                ledger_path = "/tmp/ledger.sqlite"

                [[targets]]
                type = "bark"
                key = "bark-friend"
                endpoint_env = "SPX_BARK_FRIEND_ENDPOINT"
                presentation = "summary"
            "#,
        )
        .unwrap();
        assert!(matches!(
            summary.targets.as_slice(),
            [TargetConfig::Bark {
                presentation: BarkPresentation::Summary,
                ..
            }]
        ));
        assert!(
            toml::from_str::<DeliveryConfig>(
                r#"
                ledger_path = "/tmp/ledger.sqlite"
                [[targets]]
                type = "bark"
                key = "bad"
                endpoint_env = "SPX_BARK_ENDPOINT"
                presentation = "key_name_magic"
            "#
            )
            .is_err()
        );
    }

    #[test]
    fn typed_target_keys_must_be_unique() {
        let config: DeliveryConfig = toml::from_str(
            r#"
                ledger_path = "/tmp/ledger.sqlite"
                network_enabled = true

                [[targets]]
                type = "bark"
                key = "primary"
                endpoint_env = "SPX_BARK_ENDPOINT"

                [[targets]]
                type = "feishu"
                key = "primary"
                endpoint_env = "SPX_FEISHU_ENDPOINT"
            "#,
        )
        .unwrap();
        assert!(matches!(
            config.validate(),
            Err(ConfigError::Invalid("target keys must be unique"))
        ));
    }

    #[test]
    fn retry_delays_must_advance_time() {
        let mut config: DeliveryConfig = toml::from_str(
            r#"
                ledger_path = "/tmp/ledger.sqlite"
            "#,
        )
        .unwrap();
        config.retry_schedule_seconds = vec![0];
        assert!(matches!(
            config.validate(),
            Err(ConfigError::Invalid(
                "retry schedule delays must be within 1..=86400 seconds"
            ))
        ));

        config.retry_schedule_seconds = vec![60, 15];
        assert!(matches!(
            config.validate(),
            Err(ConfigError::Invalid(
                "retry schedule delays must strictly increase"
            ))
        ));

        config.retry_schedule_seconds = vec![15, 15];
        assert!(matches!(
            config.validate(),
            Err(ConfigError::Invalid(
                "retry schedule delays must strictly increase"
            ))
        ));

        config.retry_schedule_seconds = vec![15, 60];
        config.validate().unwrap();
    }

    #[test]
    fn feishu_secret_reference_must_be_an_environment_name() {
        let config: DeliveryConfig = toml::from_str(
            r#"
                ledger_path = "/tmp/ledger.sqlite"

                [[targets]]
                type = "feishu"
                key = "primary"
                endpoint_env = "SPX_FEISHU_ENDPOINT"
                secret_env = "not-valid"
            "#,
        )
        .unwrap();
        assert!(matches!(
            config.validate(),
            Err(ConfigError::Invalid(
                "feishu secret_env must be an environment variable name"
            ))
        ));
    }

    #[test]
    fn delivery_timing_bounds_reject_typo_sized_values() {
        let mut config: DeliveryConfig = toml::from_str(
            r#"
                ledger_path = "/tmp/ledger.sqlite"
            "#,
        )
        .unwrap();

        config.owner_lease_seconds = 3_601;
        assert!(config.validate().is_err());

        config.owner_lease_seconds = 60;
        config.claim_lease_seconds = 301;
        assert!(config.validate().is_err());

        config.claim_lease_seconds = 30;
        config.request_timeout_seconds = 121;
        assert!(config.validate().is_err());

        config.request_timeout_seconds = 10;
        config.retry_schedule_seconds = vec![86_401];
        assert!(config.validate().is_err());

        config.retry_schedule_seconds = vec![1; 11];
        assert!(config.validate().is_err());
    }

    #[test]
    fn check_config_validation_rejects_oversized_target_key() {
        let config: DeliveryConfig = toml::from_str(&format!(
            r#"
                ledger_path = "/tmp/ledger.sqlite"

                [[targets]]
                type = "bark"
                key = "{}"
                endpoint_env = "SPX_BARK_ENDPOINT"
            "#,
            "x".repeat(4_097)
        ))
        .unwrap();
        assert!(matches!(config.validate(), Err(ConfigError::Domain(_))));
    }
}
