use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config: {0}")]
    Read(#[from] std::io::Error),
    #[error("invalid TOML config: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("invalid config: {0}")]
    Invalid(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BridgeConfig {
    pub source_snapshot_path: PathBuf,
    #[serde(default)]
    pub research_signal_path: Option<PathBuf>,
    #[serde(default)]
    pub desk_map_projection_path: Option<PathBuf>,
    #[serde(default)]
    pub strategy_distribution_forecast_path: Option<PathBuf>,
    pub ibkr_health_path: PathBuf,
    pub socket_path: PathBuf,
    pub state_path: PathBuf,
    pub health_path: PathBuf,
    pub poll_interval_ms: u64,
    pub reconnect_backoff_ms: u64,
    pub io_timeout_ms: u64,
    pub max_frame_bytes: usize,
    pub source_max_bytes: u64,
    pub max_rejected_attempts: u32,
}

impl BridgeConfig {
    /// Loads and validates a strict bridge configuration.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be read, TOML is invalid, or a
    /// path/bound is unsafe for the production bridge.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let contents = std::fs::read_to_string(path)?;
        let config: Self = toml::from_str(&contents)?;
        config.validate()?;
        Ok(config)
    }

    /// Validates absolute paths and bounded resource/time limits.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Invalid`] for an unsafe value.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let paths = [
            &self.source_snapshot_path,
            &self.ibkr_health_path,
            &self.socket_path,
            &self.state_path,
            &self.health_path,
        ];
        if paths.iter().any(|path| !path.is_absolute()) {
            return Err(ConfigError::Invalid("all runtime paths must be absolute"));
        }
        if self
            .research_signal_path
            .as_ref()
            .is_some_and(|path| !path.is_absolute())
        {
            return Err(ConfigError::Invalid("all runtime paths must be absolute"));
        }
        if self
            .desk_map_projection_path
            .as_ref()
            .is_some_and(|path| !path.is_absolute())
        {
            return Err(ConfigError::Invalid("all runtime paths must be absolute"));
        }
        if self
            .strategy_distribution_forecast_path
            .as_ref()
            .is_some_and(|path| !path.is_absolute())
        {
            return Err(ConfigError::Invalid("all runtime paths must be absolute"));
        }
        if self.state_path == self.health_path
            || self.state_path == self.source_snapshot_path
            || self.health_path == self.source_snapshot_path
            || self.research_signal_path.as_ref().is_some_and(|path| {
                path == &self.source_snapshot_path
                    || path == &self.state_path
                    || path == &self.health_path
            })
            || self.desk_map_projection_path.as_ref().is_some_and(|path| {
                path == &self.source_snapshot_path
                    || path == &self.state_path
                    || path == &self.health_path
                    || self.research_signal_path.as_ref() == Some(path)
            })
            || self
                .strategy_distribution_forecast_path
                .as_ref()
                .is_some_and(|path| {
                    path == &self.source_snapshot_path
                        || path == &self.state_path
                        || path == &self.health_path
                        || self.research_signal_path.as_ref() == Some(path)
                        || self.desk_map_projection_path.as_ref() == Some(path)
                })
        {
            return Err(ConfigError::Invalid(
                "source, state and health paths must differ",
            ));
        }
        if !(100..=60_000).contains(&self.poll_interval_ms) {
            return Err(ConfigError::Invalid(
                "poll_interval_ms must be within 100..=60000",
            ));
        }
        if !(100..=60_000).contains(&self.reconnect_backoff_ms) {
            return Err(ConfigError::Invalid(
                "reconnect_backoff_ms must be within 100..=60000",
            ));
        }
        if !(100..=30_000).contains(&self.io_timeout_ms) {
            return Err(ConfigError::Invalid(
                "io_timeout_ms must be within 100..=30000",
            ));
        }
        if !(1_024..=16 * 1024 * 1024).contains(&self.max_frame_bytes) {
            return Err(ConfigError::Invalid(
                "max_frame_bytes must be within 1024..=16777216",
            ));
        }
        if !(1_024..=64 * 1024 * 1024).contains(&self.source_max_bytes) {
            return Err(ConfigError::Invalid(
                "source_max_bytes must be within 1024..=67108864",
            ));
        }
        if !(1..=10).contains(&self.max_rejected_attempts) {
            return Err(ConfigError::Invalid(
                "max_rejected_attempts must be within 1..=10",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid() -> BridgeConfig {
        BridgeConfig {
            source_snapshot_path: "/source/state.json".into(),
            research_signal_path: Some("/source/research.json".into()),
            desk_map_projection_path: Some("/source/desk-map.json".into()),
            strategy_distribution_forecast_path: Some("/source/strategy-distribution.json".into()),
            ibkr_health_path: "/source/ibkr.json".into(),
            socket_path: "/run/core.sock".into(),
            state_path: "/state/bridge.json".into(),
            health_path: "/state/health.json".into(),
            poll_interval_ms: 500,
            reconnect_backoff_ms: 1_000,
            io_timeout_ms: 2_000,
            max_frame_bytes: 1_048_576,
            source_max_bytes: 8_388_608,
            max_rejected_attempts: 3,
        }
    }

    #[test]
    fn relative_runtime_path_is_rejected() {
        let mut config = valid();
        config.socket_path = "core.sock".into();
        assert!(matches!(
            config.validate(),
            Err(ConfigError::Invalid("all runtime paths must be absolute"))
        ));
    }

    #[test]
    fn resource_bounds_are_validated() {
        let mut config = valid();
        config.source_max_bytes = 0;
        assert!(config.validate().is_err());
        config = valid();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn legacy_config_without_strategy_distribution_lane_remains_valid() {
        let encoded = toml::to_string(&valid()).unwrap();
        let legacy = encoded
            .lines()
            .filter(|line| !line.starts_with("strategy_distribution_forecast_path ="))
            .collect::<Vec<_>>()
            .join("\n");
        let decoded: BridgeConfig = toml::from_str(&legacy).unwrap();

        assert!(decoded.strategy_distribution_forecast_path.is_none());
        decoded.validate().unwrap();
    }
}
