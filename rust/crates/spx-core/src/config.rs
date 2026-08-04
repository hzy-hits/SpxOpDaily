use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use spx_domain::{DeliveryChannel, Token};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config: {0}")]
    Read(#[from] std::io::Error),
    #[error("invalid TOML config: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("invalid environment override {name}: {value}")]
    Environment { name: &'static str, value: String },
    #[error("environment override {0} is forbidden for destructive maintenance")]
    ForbiddenEnvironment(&'static str),
    #[error("invalid config: {0}")]
    Invalid(&'static str),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoreConfig {
    pub socket_path: PathBuf,
    pub ledger_path: PathBuf,
    pub raw_log_dir: PathBuf,
    pub projection_path: PathBuf,
    pub research_projection_path: PathBuf,
    pub desk_map_projection_path: PathBuf,
    pub max_frame_bytes: usize,
    pub max_connections: usize,
    pub raw_segment_max_bytes: u64,
    pub raw_log_min_free_bytes: u64,
    pub quote_cache_retention_seconds: u32,
    pub quote_cache_max_entries: usize,
    pub batch_identity_cache_max_entries: usize,
    pub owner_lease_seconds: i64,
    pub delivery_max_attempts: u32,
    pub notification_targets: Vec<NotificationTargetConfig>,
    pub decision_max_ttl_seconds: u32,
    pub evaluation_max_delay_seconds: f64,
    pub readiness: ReadinessConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NotificationTargetConfig {
    pub key: Token,
    pub channel: DeliveryChannel,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadinessConfig {
    pub quote_max_age_seconds: f64,
    pub max_side_skew_seconds: f64,
    pub allow_rth_ibkr_fallback: bool,
}

impl CoreConfig {
    /// Loads strict TOML plus the documented bounded environment overrides.
    ///
    /// # Errors
    ///
    /// Returns an error when the file, TOML, environment, or resulting invariants are invalid.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        Self::load_inner(path, true)
    }

    /// Loads configuration for raw-log pruning without permitting a target override.
    ///
    /// # Errors
    ///
    /// Returns an error when `SPX_CORE_RAW_LOG_DIR` is present or the config is invalid.
    pub fn load_for_prune(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        Self::load_inner(path, false)
    }

    fn load_inner(
        path: impl AsRef<Path>,
        allow_raw_log_environment: bool,
    ) -> Result<Self, ConfigError> {
        let contents = std::fs::read_to_string(path)?;
        let mut config: Self = toml::from_str(&contents)?;
        override_path("SPX_CORE_SOCKET_PATH", &mut config.socket_path);
        override_path("SPX_CORE_LEDGER_PATH", &mut config.ledger_path);
        if allow_raw_log_environment {
            override_path("SPX_CORE_RAW_LOG_DIR", &mut config.raw_log_dir);
        } else if std::env::var_os("SPX_CORE_RAW_LOG_DIR").is_some() {
            return Err(ConfigError::ForbiddenEnvironment("SPX_CORE_RAW_LOG_DIR"));
        }
        override_path("SPX_CORE_PROJECTION_PATH", &mut config.projection_path);
        override_path(
            "SPX_CORE_RESEARCH_PROJECTION_PATH",
            &mut config.research_projection_path,
        );
        override_path(
            "SPX_CORE_DESK_MAP_PROJECTION_PATH",
            &mut config.desk_map_projection_path,
        );
        override_usize("SPX_CORE_MAX_FRAME_BYTES", &mut config.max_frame_bytes)?;
        override_usize("SPX_CORE_MAX_CONNECTIONS", &mut config.max_connections)?;
        override_u64(
            "SPX_CORE_RAW_SEGMENT_MAX_BYTES",
            &mut config.raw_segment_max_bytes,
        )?;
        override_u64(
            "SPX_CORE_RAW_LOG_MIN_FREE_BYTES",
            &mut config.raw_log_min_free_bytes,
        )?;
        override_u32(
            "SPX_CORE_QUOTE_CACHE_RETENTION_SECONDS",
            &mut config.quote_cache_retention_seconds,
        )?;
        override_usize(
            "SPX_CORE_QUOTE_CACHE_MAX_ENTRIES",
            &mut config.quote_cache_max_entries,
        )?;
        override_usize(
            "SPX_CORE_BATCH_IDENTITY_CACHE_MAX_ENTRIES",
            &mut config.batch_identity_cache_max_entries,
        )?;
        override_i64(
            "SPX_CORE_OWNER_LEASE_SECONDS",
            &mut config.owner_lease_seconds,
        )?;
        override_u32(
            "SPX_CORE_DELIVERY_MAX_ATTEMPTS",
            &mut config.delivery_max_attempts,
        )?;
        override_u32(
            "SPX_CORE_DECISION_MAX_TTL_SECONDS",
            &mut config.decision_max_ttl_seconds,
        )?;
        override_f64(
            "SPX_CORE_EVALUATION_MAX_DELAY_SECONDS",
            &mut config.evaluation_max_delay_seconds,
        )?;
        override_f64(
            "SPX_CORE_QUOTE_MAX_AGE_SECONDS",
            &mut config.readiness.quote_max_age_seconds,
        )?;
        override_f64(
            "SPX_CORE_MAX_SIDE_SKEW_SECONDS",
            &mut config.readiness.max_side_skew_seconds,
        )?;
        override_bool(
            "SPX_CORE_ALLOW_RTH_IBKR_FALLBACK",
            &mut config.readiness.allow_rth_ibkr_fallback,
        )?;
        config.validate()?;
        Ok(config)
    }

    /// Validates frame, lease, and readiness threshold bounds.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Invalid`] when any bound is unsafe.
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.validate_storage_bounds()?;
        if !(5..=3_600).contains(&self.owner_lease_seconds) {
            return Err(ConfigError::Invalid(
                "owner_lease_seconds must be within 5..=3600",
            ));
        }
        if !(1..=10).contains(&self.delivery_max_attempts) {
            return Err(ConfigError::Invalid(
                "delivery_max_attempts must be within 1..=10",
            ));
        }
        if self.notification_targets.is_empty() {
            return Err(ConfigError::Invalid(
                "notification_targets must be non-empty",
            ));
        }
        let mut target_keys: Vec<_> = self
            .notification_targets
            .iter()
            .map(|target| target.key.clone())
            .collect();
        target_keys.sort();
        target_keys.dedup();
        if target_keys.len() != self.notification_targets.len() {
            return Err(ConfigError::Invalid(
                "notification target keys must be unique",
            ));
        }
        if !(1..=900).contains(&self.decision_max_ttl_seconds) {
            return Err(ConfigError::Invalid(
                "decision_max_ttl_seconds must be within 1..=900",
            ));
        }
        if !self.evaluation_max_delay_seconds.is_finite()
            || self.evaluation_max_delay_seconds <= 0.0
            || self.evaluation_max_delay_seconds > 30.0
        {
            return Err(ConfigError::Invalid(
                "evaluation_max_delay_seconds must be > 0 and <= 30",
            ));
        }
        if !self.readiness.quote_max_age_seconds.is_finite()
            || self.readiness.quote_max_age_seconds <= 0.0
            || self.readiness.quote_max_age_seconds > 60.0
        {
            return Err(ConfigError::Invalid(
                "quote_max_age_seconds must be > 0 and <= 60",
            ));
        }
        if !self.readiness.max_side_skew_seconds.is_finite()
            || self.readiness.max_side_skew_seconds <= 0.0
            || self.readiness.max_side_skew_seconds > self.readiness.quote_max_age_seconds
        {
            return Err(ConfigError::Invalid(
                "max_side_skew_seconds must be positive and no greater than quote age",
            ));
        }
        Ok(())
    }

    fn validate_storage_bounds(&self) -> Result<(), ConfigError> {
        if self.socket_path.as_os_str().is_empty()
            || self.ledger_path.as_os_str().is_empty()
            || self.raw_log_dir.as_os_str().is_empty()
            || self.projection_path.as_os_str().is_empty()
            || self.research_projection_path.as_os_str().is_empty()
            || self.desk_map_projection_path.as_os_str().is_empty()
        {
            return Err(ConfigError::Invalid("runtime paths must be non-empty"));
        }
        if !self.socket_path.is_absolute()
            || !self.ledger_path.is_absolute()
            || !self.raw_log_dir.is_absolute()
            || !self.projection_path.is_absolute()
            || !self.research_projection_path.is_absolute()
            || !self.desk_map_projection_path.is_absolute()
        {
            return Err(ConfigError::Invalid("runtime paths must be absolute"));
        }
        if self.projection_path == self.research_projection_path
            || self.projection_path == self.desk_map_projection_path
            || self.research_projection_path == self.desk_map_projection_path
        {
            return Err(ConfigError::Invalid(
                "core, research, and desk map projection paths must differ",
            ));
        }
        if self.max_frame_bytes == 0 || self.max_frame_bytes > 16 * 1024 * 1024 {
            return Err(ConfigError::Invalid(
                "max_frame_bytes must be within 1..=16777216",
            ));
        }
        if !(1..=64).contains(&self.max_connections) {
            return Err(ConfigError::Invalid(
                "max_connections must be within 1..=64",
            ));
        }
        let minimum_segment_bytes = u64::try_from(self.max_frame_bytes)
            .ok()
            .and_then(|bytes| bytes.checked_add(4096))
            .ok_or(ConfigError::Invalid("max_frame_bytes is not representable"))?;
        if self.raw_segment_max_bytes < minimum_segment_bytes
            || self.raw_segment_max_bytes > 1024 * 1024 * 1024
        {
            return Err(ConfigError::Invalid(
                "raw_segment_max_bytes must cover max_frame_bytes plus 4096 bytes and be <= 1 GiB",
            ));
        }
        if self.raw_log_min_free_bytes < self.raw_segment_max_bytes
            || self.raw_log_min_free_bytes > 10 * 1024 * 1024 * 1024 * 1024
        {
            return Err(ConfigError::Invalid(
                "raw_log_min_free_bytes must cover one maximum segment and be <= 10 TiB",
            ));
        }
        if !(60..=3600).contains(&self.quote_cache_retention_seconds) {
            return Err(ConfigError::Invalid(
                "quote_cache_retention_seconds must be within 60..=3600",
            ));
        }
        if !(256..=100_000).contains(&self.quote_cache_max_entries) {
            return Err(ConfigError::Invalid(
                "quote_cache_max_entries must be within 256..=100000",
            ));
        }
        if !(256..=100_000).contains(&self.batch_identity_cache_max_entries) {
            return Err(ConfigError::Invalid(
                "batch_identity_cache_max_entries must be within 256..=100000",
            ));
        }
        Ok(())
    }
}

fn override_path(name: &'static str, target: &mut PathBuf) {
    if let Some(value) = std::env::var_os(name) {
        *target = PathBuf::from(value);
    }
}

fn override_usize(name: &'static str, target: &mut usize) -> Result<(), ConfigError> {
    if let Ok(value) = std::env::var(name) {
        *target = value.parse().map_err(|_| ConfigError::Environment {
            name,
            value: value.clone(),
        })?;
    }
    Ok(())
}

fn override_i64(name: &'static str, target: &mut i64) -> Result<(), ConfigError> {
    if let Ok(value) = std::env::var(name) {
        *target = value.parse().map_err(|_| ConfigError::Environment {
            name,
            value: value.clone(),
        })?;
    }
    Ok(())
}

fn override_u64(name: &'static str, target: &mut u64) -> Result<(), ConfigError> {
    if let Ok(value) = std::env::var(name) {
        *target = value.parse().map_err(|_| ConfigError::Environment {
            name,
            value: value.clone(),
        })?;
    }
    Ok(())
}

fn override_u32(name: &'static str, target: &mut u32) -> Result<(), ConfigError> {
    if let Ok(value) = std::env::var(name) {
        *target = value.parse().map_err(|_| ConfigError::Environment {
            name,
            value: value.clone(),
        })?;
    }
    Ok(())
}

fn override_f64(name: &'static str, target: &mut f64) -> Result<(), ConfigError> {
    if let Ok(value) = std::env::var(name) {
        *target = value.parse().map_err(|_| ConfigError::Environment {
            name,
            value: value.clone(),
        })?;
    }
    Ok(())
}

fn override_bool(name: &'static str, target: &mut bool) -> Result<(), ConfigError> {
    if let Ok(value) = std::env::var(name) {
        *target = match value.as_str() {
            "true" | "1" => true,
            "false" | "0" => false,
            _ => {
                return Err(ConfigError::Environment { name, value });
            }
        };
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_config() -> CoreConfig {
        CoreConfig {
            socket_path: "/tmp/spx-core.sock".into(),
            ledger_path: "/tmp/spx-core.sqlite".into(),
            raw_log_dir: "/tmp/spx-core-raw".into(),
            projection_path: "/tmp/spx-core-latest.json".into(),
            research_projection_path: "/tmp/spx-core-research.json".into(),
            desk_map_projection_path: "/tmp/spx-core-desk-map.json".into(),
            max_frame_bytes: 1_048_576,
            max_connections: 8,
            raw_segment_max_bytes: 64 * 1024 * 1024,
            raw_log_min_free_bytes: 64 * 1024 * 1024,
            quote_cache_retention_seconds: 300,
            quote_cache_max_entries: 4096,
            batch_identity_cache_max_entries: 4096,
            owner_lease_seconds: 30,
            delivery_max_attempts: 3,
            notification_targets: vec![NotificationTargetConfig {
                key: Token::new("primary", "target").unwrap(),
                channel: DeliveryChannel::Bark,
            }],
            decision_max_ttl_seconds: 60,
            evaluation_max_delay_seconds: 5.0,
            readiness: ReadinessConfig {
                quote_max_age_seconds: 5.0,
                max_side_skew_seconds: 2.0,
                allow_rth_ibkr_fallback: false,
            },
        }
    }

    #[test]
    fn runtime_paths_must_be_non_empty() {
        let mut config = valid_config();
        config.socket_path = PathBuf::new();
        assert!(matches!(
            config.validate(),
            Err(ConfigError::Invalid("runtime paths must be non-empty"))
        ));
    }

    #[test]
    fn runtime_paths_must_be_absolute() {
        for update in [
            |config: &mut CoreConfig| config.socket_path = "run/core.sock".into(),
            |config: &mut CoreConfig| config.ledger_path = "state/core.sqlite".into(),
            |config: &mut CoreConfig| config.raw_log_dir = "frames".into(),
            |config: &mut CoreConfig| config.projection_path = "latest/core.json".into(),
            |config: &mut CoreConfig| {
                config.research_projection_path = "latest/research.json".into();
            },
        ] {
            let mut config = valid_config();
            update(&mut config);
            assert!(matches!(
                config.validate(),
                Err(ConfigError::Invalid("runtime paths must be absolute"))
            ));
        }
    }

    #[test]
    fn raw_segment_must_cover_largest_frame_and_record_overhead() {
        let mut config = valid_config();
        config.raw_segment_max_bytes = config.max_frame_bytes as u64 + 4095;
        assert!(matches!(
            config.validate(),
            Err(ConfigError::Invalid(
                "raw_segment_max_bytes must cover max_frame_bytes plus 4096 bytes and be <= 1 GiB"
            ))
        ));
    }

    #[test]
    fn raw_log_free_space_reserve_is_bounded() {
        let mut config = valid_config();
        config.raw_log_min_free_bytes = config.raw_segment_max_bytes - 1;
        assert!(matches!(
            config.validate(),
            Err(ConfigError::Invalid(
                "raw_log_min_free_bytes must cover one maximum segment and be <= 10 TiB"
            ))
        ));

        let mut config = valid_config();
        config.raw_log_min_free_bytes = 10 * 1024 * 1024 * 1024 * 1024 + 1;
        assert!(matches!(
            config.validate(),
            Err(ConfigError::Invalid(
                "raw_log_min_free_bytes must cover one maximum segment and be <= 10 TiB"
            ))
        ));
    }

    #[test]
    fn hot_cache_bounds_are_strict() {
        let mut config = valid_config();
        config.quote_cache_retention_seconds = 59;
        assert!(config.validate().is_err());

        let mut config = valid_config();
        config.quote_cache_max_entries = 100_001;
        assert!(config.validate().is_err());

        let mut config = valid_config();
        config.batch_identity_cache_max_entries = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn owner_lease_cannot_strand_the_single_writer_for_hours() {
        let mut config = valid_config();
        config.owner_lease_seconds = 3_601;
        assert!(matches!(
            config.validate(),
            Err(ConfigError::Invalid(
                "owner_lease_seconds must be within 5..=3600"
            ))
        ));
    }
}
