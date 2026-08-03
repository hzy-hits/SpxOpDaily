use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::validation::{require_schema, unique_tokens};
use crate::{
    DELIVERY_RECEIPT_SCHEMA_VERSION, DomainError, NOTIFICATION_INTENT_SCHEMA_VERSION,
    NOTIFICATION_INTENT_V2_SCHEMA_VERSION, Token, Validate,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryChannel {
    Bark,
    Feishu,
    Webhook,
}

impl DeliveryChannel {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Bark => "bark",
            Self::Feishu => "feishu",
            Self::Webhook => "webhook",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NotificationTargetV1 {
    pub key: Token,
    pub channel: DeliveryChannel,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeskMessageV1 {
    pub title: Token,
    pub desk_view: Token,
    pub execution: Token,
    pub risk: Token,
    pub targets: Token,
    pub data_quality: Token,
}

/// Complete, canonical desk report body.
///
/// Every section is a bounded, non-empty [`Token`]. The contract deliberately keeps the full
/// report instead of carrying a shortened transport projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeskMessageV2 {
    pub title: Token,
    pub desk_view: Token,
    pub location: Token,
    pub structure: Token,
    pub primary_path: Token,
    pub alternative_path: Token,
    pub targets: Token,
    pub execution: Token,
    pub data_quality: Token,
}

impl Validate for DeskMessageV2 {
    fn validate(&self) -> Result<(), DomainError> {
        Ok(())
    }
}

/// Closed lineage variants for decision alerts and independent scheduled reports.
///
/// Binding the lane to its required source identifier makes a trade-ready message without a
/// decision, or a scheduled report without a projection and stable ET slot, unrepresentable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "lane", rename_all = "snake_case", deny_unknown_fields)]
pub enum NotificationLineageV2 {
    TradeReady {
        decision_id: Token,
    },
    ScheduledReport {
        source_projection_id: Token,
        slot: Token,
    },
}

impl NotificationLineageV2 {
    pub const fn lane(&self) -> &'static str {
        match self {
            Self::TradeReady { .. } => "trade_ready",
            Self::ScheduledReport { .. } => "scheduled_report",
        }
    }

    pub const fn decision_id(&self) -> Option<&Token> {
        match self {
            Self::TradeReady { decision_id } => Some(decision_id),
            Self::ScheduledReport { .. } => None,
        }
    }

    pub const fn source_projection_id(&self) -> Option<&Token> {
        match self {
            Self::TradeReady { .. } => None,
            Self::ScheduledReport {
                source_projection_id,
                ..
            } => Some(source_projection_id),
        }
    }

    pub const fn slot(&self) -> Option<&Token> {
        match self {
            Self::TradeReady { .. } => None,
            Self::ScheduledReport { slot, .. } => Some(slot),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NotificationIntentV2 {
    pub schema_version: String,
    pub intent_id: Token,
    pub semantic_id: Token,
    pub lineage: NotificationLineageV2,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub message: DeskMessageV2,
    pub targets: Vec<NotificationTargetV1>,
    pub max_attempts: u32,
}

impl Validate for NotificationIntentV2 {
    fn validate(&self) -> Result<(), DomainError> {
        require_schema(
            &self.schema_version,
            NOTIFICATION_INTENT_V2_SCHEMA_VERSION,
            "notification intent",
        )?;
        self.message.validate()?;
        validate_intent_common(
            self.created_at,
            self.expires_at,
            &self.targets,
            self.max_attempts,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NotificationIntentV1 {
    pub schema_version: String,
    pub intent_id: Token,
    pub semantic_id: Token,
    pub decision_id: Token,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub message: DeskMessageV1,
    pub targets: Vec<NotificationTargetV1>,
    pub max_attempts: u32,
}

impl Validate for NotificationIntentV1 {
    fn validate(&self) -> Result<(), DomainError> {
        require_schema(
            &self.schema_version,
            NOTIFICATION_INTENT_SCHEMA_VERSION,
            "notification intent",
        )?;
        validate_intent_common(
            self.created_at,
            self.expires_at,
            &self.targets,
            self.max_attempts,
        )
    }
}

fn validate_intent_common(
    created_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    targets: &[NotificationTargetV1],
    max_attempts: u32,
) -> Result<(), DomainError> {
    if expires_at <= created_at {
        return Err(DomainError::TimeOrder(
            "notification expires_at must be after created_at",
        ));
    }
    if targets.is_empty() {
        return Err(DomainError::Invalid {
            field: "targets",
            reason: "at least one delivery target is required",
        });
    }
    let target_keys: Vec<Token> = targets.iter().map(|target| target.key.clone()).collect();
    unique_tokens(&target_keys, "target key").and_then(|()| {
        if (1..=10).contains(&max_attempts) {
            Ok(())
        } else {
            Err(DomainError::Invalid {
                field: "max_attempts",
                reason: "must be within 1..=10",
            })
        }
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReceiptOutcome {
    Delivered,
    RetryScheduled,
    DeadLetter,
    Cancelled,
    Expired,
    Uncertain,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryReceiptV1 {
    pub schema_version: String,
    pub receipt_id: Token,
    pub target_id: Token,
    pub intent_id: Token,
    pub target_key: Token,
    pub channel: DeliveryChannel,
    pub outcome: ReceiptOutcome,
    pub attempted_at: DateTime<Utc>,
    pub provider_message_id: Option<Token>,
    pub error_code: Option<Token>,
}

impl Validate for DeliveryReceiptV1 {
    fn validate(&self) -> Result<(), DomainError> {
        require_schema(
            &self.schema_version,
            DELIVERY_RECEIPT_SCHEMA_VERSION,
            "delivery receipt",
        )?;
        match self.outcome {
            ReceiptOutcome::Delivered if self.error_code.is_some() => Err(DomainError::Invalid {
                field: "error_code",
                reason: "delivered receipt cannot contain an error",
            }),
            ReceiptOutcome::RetryScheduled
            | ReceiptOutcome::DeadLetter
            | ReceiptOutcome::Cancelled
            | ReceiptOutcome::Expired
            | ReceiptOutcome::Uncertain
                if self.error_code.is_none() =>
            {
                Err(DomainError::Invalid {
                    field: "error_code",
                    reason: "failed receipt requires a typed error code",
                })
            }
            ReceiptOutcome::RetryScheduled
            | ReceiptOutcome::DeadLetter
            | ReceiptOutcome::Cancelled
            | ReceiptOutcome::Expired
            | ReceiptOutcome::Uncertain
                if self.provider_message_id.is_some() =>
            {
                Err(DomainError::Invalid {
                    field: "provider_message_id",
                    reason: "only a delivered receipt can contain a provider message id",
                })
            }
            ReceiptOutcome::Delivered
            | ReceiptOutcome::RetryScheduled
            | ReceiptOutcome::DeadLetter
            | ReceiptOutcome::Cancelled
            | ReceiptOutcome::Expired
            | ReceiptOutcome::Uncertain => Ok(()),
        }
    }
}
