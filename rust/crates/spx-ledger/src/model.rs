use std::fmt::{Display, Formatter};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use spx_domain::{
    DeliveryChannel, NotificationIntentV1, NotificationIntentV2, OperatorNotificationV1, Token,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnerRole {
    Core,
    Report,
    Delivery,
}

impl OwnerRole {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Core => "core",
            Self::Report => "report",
            Self::Delivery => "delivery",
        }
    }
}

impl Display for OwnerRole {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerLease {
    pub(crate) role: OwnerRole,
    pub(crate) owner_id: String,
    pub(crate) generation: i64,
    pub(crate) lease_until: DateTime<Utc>,
}

impl OwnerLease {
    pub const fn role(&self) -> OwnerRole {
        self.role
    }

    pub fn owner_id(&self) -> &str {
        &self.owner_id
    }

    pub const fn generation(&self) -> i64 {
        self.generation
    }

    pub const fn lease_until(&self) -> DateTime<Utc> {
        self.lease_until
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetStatus {
    Pending,
    Claimed,
    InFlight,
    Delivered,
    DeadLetter,
    Cancelled,
    Expired,
    Uncertain,
}

impl TargetStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Claimed => "claimed",
            Self::InFlight => "in_flight",
            Self::Delivered => "delivered",
            Self::DeadLetter => "dead_letter",
            Self::Cancelled => "cancelled",
            Self::Expired => "expired",
            Self::Uncertain => "uncertain",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimHandle {
    pub(crate) target_id: String,
    pub(crate) claim_token: String,
    pub(crate) lease_sequence: i64,
    pub(crate) owner_generation: i64,
}

impl ClaimHandle {
    pub fn target_id(&self) -> &str {
        &self.target_id
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ClaimedNotificationIntent {
    TradeReady(NotificationIntentV1),
    ScheduledReport(NotificationIntentV2),
    TraderEvent(OperatorNotificationV1),
}

impl ClaimedNotificationIntent {
    pub const fn intent_id(&self) -> &Token {
        match self {
            Self::TradeReady(intent) => &intent.intent_id,
            Self::ScheduledReport(intent) => &intent.intent_id,
            Self::TraderEvent(notification) => &notification.event_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClaimedDelivery {
    pub handle: ClaimHandle,
    pub intent: ClaimedNotificationIntent,
    pub target_key: Token,
    pub channel: DeliveryChannel,
    pub attempt_no: u32,
    pub idempotency_key: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngressWrite {
    Inserted,
    Duplicate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngressCheck {
    New,
    Duplicate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistWrite {
    Inserted,
    Duplicate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorNotificationWrite {
    Inserted,
    Duplicate,
    SemanticSuppressed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorWrite {
    Applied,
    AlreadyAcknowledged,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BeginTransport {
    Started { attempt_id: String },
    Cancelled,
    Expired,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Settlement {
    Delivered {
        provider_message_id: Option<String>,
    },
    Retryable {
        error_code: String,
        retry_at: DateTime<Utc>,
    },
    PermanentFailure {
        error_code: String,
    },
    TransportUncertain {
        error_code: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettlementWrite {
    Delivered,
    RetryScheduled,
    DeadLetter,
    Uncertain,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RecoverySummary {
    pub requeued: u64,
    pub uncertain: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct LedgerHealth {
    pub pending: u64,
    pub claimed: u64,
    pub in_flight: u64,
    pub delivered: u64,
    pub dead_letter: u64,
    pub cancelled: u64,
    pub expired: u64,
    pub uncertain: u64,
    pub unacknowledged_failures: u64,
}
