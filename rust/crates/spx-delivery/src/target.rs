use std::collections::HashMap;

use spx_domain::{DeliveryChannel, DomainError, Token};
use thiserror::Error;

use crate::{BarkPresentation, TargetConfig};

#[derive(Debug, Error)]
pub enum TargetError {
    #[error("invalid target contract: {0}")]
    Domain(#[from] DomainError),
    #[error("duplicate delivery target key")]
    Duplicate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BarkTarget {
    pub key: Token,
    pub endpoint_env: String,
    pub presentation: BarkPresentation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeishuTarget {
    pub key: Token,
    pub endpoint_env: String,
    pub secret_env: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookTarget {
    pub key: Token,
    pub endpoint_env: String,
    pub bearer_token_env: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryTarget {
    Bark(BarkTarget),
    Feishu(FeishuTarget),
    Webhook(WebhookTarget),
}

impl DeliveryTarget {
    pub fn key(&self) -> &Token {
        match self {
            Self::Bark(target) => &target.key,
            Self::Feishu(target) => &target.key,
            Self::Webhook(target) => &target.key,
        }
    }

    pub const fn channel(&self) -> DeliveryChannel {
        match self {
            Self::Bark(_) => DeliveryChannel::Bark,
            Self::Feishu(_) => DeliveryChannel::Feishu,
            Self::Webhook(_) => DeliveryChannel::Webhook,
        }
    }

    pub fn endpoint_env(&self) -> &str {
        match self {
            Self::Bark(target) => &target.endpoint_env,
            Self::Feishu(target) => &target.endpoint_env,
            Self::Webhook(target) => &target.endpoint_env,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TargetRegistry {
    targets: HashMap<String, DeliveryTarget>,
}

impl TargetRegistry {
    /// Builds a registry while preserving each channel's concrete target type.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid domain token or a duplicate target key.
    pub fn new(configs: &[TargetConfig]) -> Result<Self, TargetError> {
        let mut targets = HashMap::with_capacity(configs.len());
        for config in configs {
            let target = match config {
                TargetConfig::Bark {
                    key,
                    endpoint_env,
                    presentation,
                } => DeliveryTarget::Bark(BarkTarget {
                    key: Token::new(key.clone(), "bark target key")?,
                    endpoint_env: endpoint_env.clone(),
                    presentation: *presentation,
                }),
                TargetConfig::Feishu {
                    key,
                    endpoint_env,
                    secret_env,
                } => DeliveryTarget::Feishu(FeishuTarget {
                    key: Token::new(key.clone(), "feishu target key")?,
                    endpoint_env: endpoint_env.clone(),
                    secret_env: secret_env.clone(),
                }),
                TargetConfig::Webhook {
                    key,
                    endpoint_env,
                    bearer_token_env,
                } => DeliveryTarget::Webhook(WebhookTarget {
                    key: Token::new(key.clone(), "webhook target key")?,
                    endpoint_env: endpoint_env.clone(),
                    bearer_token_env: bearer_token_env.clone(),
                }),
            };
            if targets
                .insert(target.key().as_str().to_owned(), target)
                .is_some()
            {
                return Err(TargetError::Duplicate);
            }
        }
        Ok(Self { targets })
    }

    pub fn get(&self, key: &Token) -> Option<&DeliveryTarget> {
        self.targets.get(key.as_str())
    }

    pub fn all(&self) -> impl Iterator<Item = &DeliveryTarget> {
        self.targets.values()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retains_channel_specific_target_types() {
        let registry = TargetRegistry::new(&[
            TargetConfig::Bark {
                key: "bark-primary".to_owned(),
                endpoint_env: "SPX_BARK_ENDPOINT".to_owned(),
                presentation: BarkPresentation::Full,
            },
            TargetConfig::Feishu {
                key: "feishu-primary".to_owned(),
                endpoint_env: "SPX_FEISHU_ENDPOINT".to_owned(),
                secret_env: Some("SPX_FEISHU_SECRET".to_owned()),
            },
            TargetConfig::Webhook {
                key: "audit-webhook".to_owned(),
                endpoint_env: "SPX_WEBHOOK_ENDPOINT".to_owned(),
                bearer_token_env: Some("SPX_WEBHOOK_TOKEN".to_owned()),
            },
        ])
        .unwrap();
        let key = Token::new("audit-webhook", "test key").unwrap();
        assert!(matches!(
            registry.get(&key),
            Some(DeliveryTarget::Webhook(_))
        ));
    }
}
