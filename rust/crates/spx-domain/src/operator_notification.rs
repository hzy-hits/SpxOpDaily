use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::validation::{require_schema, unique_tokens};
use crate::{
    DomainError, NotificationTargetV1, OPERATOR_NOTIFICATION_CANCELLATION_SCHEMA_VERSION,
    OPERATOR_NOTIFICATION_SCHEMA_VERSION, Token, Validate,
};

const MAX_OPERATOR_BODY_BYTES: usize = 65_536;
const OPERATOR_BODY_SECTIONS: [&str; 5] = [
    "## Desk View",
    "## Execution",
    "## Risk",
    "## Targets",
    "## Data Quality",
];

/// Closed operator-facing lifecycle roles for trading notifications.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorNotificationRole {
    Setup,
    TradeReady,
    Cancel,
    Exit,
}

impl OperatorNotificationRole {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Setup => "setup",
            Self::TradeReady => "trade_ready",
            Self::Cancel => "cancel",
            Self::Exit => "exit",
        }
    }
}

/// Frozen, explicitly targeted operator message accepted by the Rust ingress boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorNotificationV1 {
    pub schema_version: String,
    pub event_id: Token,
    pub semantic_id: Token,
    pub opportunity_id: Token,
    pub generation: u32,
    pub role: OperatorNotificationRole,
    pub occurred_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub title: Token,
    pub body: String,
    pub targets: Vec<NotificationTargetV1>,
    pub automatic_ordering: bool,
}

impl Validate for OperatorNotificationV1 {
    fn validate(&self) -> Result<(), DomainError> {
        require_schema(
            &self.schema_version,
            OPERATOR_NOTIFICATION_SCHEMA_VERSION,
            "operator notification",
        )?;
        if self.expires_at <= self.occurred_at {
            return Err(DomainError::TimeOrder(
                "operator notification expires_at must be after occurred_at",
            ));
        }
        if self.body.trim().is_empty() {
            return Err(DomainError::Empty { field: "body" });
        }
        if self.body.contains('\0') {
            return Err(DomainError::Invalid {
                field: "body",
                reason: "NUL is forbidden",
            });
        }
        if self.body.len() > MAX_OPERATOR_BODY_BYTES {
            return Err(DomainError::Invalid {
                field: "body",
                reason: "must not exceed 65536 UTF-8 bytes",
            });
        }
        validate_operator_body_sections(&self.body)?;
        if self.targets.is_empty() {
            return Err(DomainError::Invalid {
                field: "targets",
                reason: "at least one delivery target is required",
            });
        }
        let target_keys: Vec<Token> = self
            .targets
            .iter()
            .map(|target| target.key.clone())
            .collect();
        unique_tokens(&target_keys, "target key")?;
        if self.automatic_ordering {
            return Err(DomainError::Invalid {
                field: "automatic_ordering",
                reason: "operator notifications cannot authorize automatic ordering",
            });
        }
        Ok(())
    }
}

fn validate_operator_body_sections(body: &str) -> Result<(), DomainError> {
    let mut section_index = 0;
    let mut section_has_content = false;

    for line in body.lines() {
        if line.starts_with("## ") {
            if section_index > 0 && !section_has_content {
                return Err(invalid_operator_body_sections());
            }
            if OPERATOR_BODY_SECTIONS.get(section_index) != Some(&line) {
                return Err(invalid_operator_body_sections());
            }
            section_index += 1;
            section_has_content = false;
        } else if !line.trim().is_empty() {
            if section_index == 0 {
                return Err(invalid_operator_body_sections());
            }
            section_has_content = true;
        }
    }

    if section_index != OPERATOR_BODY_SECTIONS.len() || !section_has_content {
        return Err(invalid_operator_body_sections());
    }
    Ok(())
}

const fn invalid_operator_body_sections() -> DomainError {
    DomainError::Invalid {
        field: "body",
        reason: "must contain the five ordered, unique, non-empty operator sections",
    }
}

/// Durable cancellation fence for one exact operator notification event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorNotificationCancellationV1 {
    pub schema_version: String,
    pub event_id: Token,
    pub cancelled_at: DateTime<Utc>,
    pub reason_code: Token,
}

impl Validate for OperatorNotificationCancellationV1 {
    fn validate(&self) -> Result<(), DomainError> {
        require_schema(
            &self.schema_version,
            OPERATOR_NOTIFICATION_CANCELLATION_SCHEMA_VERSION,
            "operator notification cancellation",
        )
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeDelta;

    use super::*;
    use crate::{DeliveryChannel, NotificationTargetV1};

    fn operator_body(desk_view: &str) -> String {
        format!(
            "## Desk View\n{desk_view}\n\n## Execution\nmanual only\n\n## Risk\ndefined risk\n\n## Targets\nnext level\n\n## Data Quality\nlive"
        )
    }

    fn notification() -> OperatorNotificationV1 {
        let occurred_at = "2026-08-04T13:30:00Z".parse().unwrap();
        OperatorNotificationV1 {
            schema_version: OPERATOR_NOTIFICATION_SCHEMA_VERSION.to_owned(),
            event_id: Token::new("event-1", "event_id").unwrap(),
            semantic_id: Token::new("semantic-1", "semantic_id").unwrap(),
            opportunity_id: Token::new("opportunity-1", "opportunity_id").unwrap(),
            generation: 0,
            role: OperatorNotificationRole::TradeReady,
            occurred_at,
            expires_at: occurred_at + TimeDelta::minutes(10),
            title: Token::new("SPX trade ready", "title").unwrap(),
            body: operator_body("trend continuation"),
            targets: vec![NotificationTargetV1 {
                key: Token::new("primary", "target_key").unwrap(),
                channel: DeliveryChannel::Bark,
            }],
            automatic_ordering: false,
        }
    }

    #[test]
    fn validates_full_frozen_operator_message() {
        let mut value = notification();
        let suffix = "\n\n## Execution\nmanual only\n\n## Risk\ndefined risk\n\n## Targets\nnext level\n\n## Data Quality\nlive";
        let prefix = "## Desk View\n";
        value.body = format!(
            "{prefix}{}{suffix}",
            "x".repeat(MAX_OPERATOR_BODY_BYTES - prefix.len() - suffix.len())
        );
        assert_eq!(value.body.len(), MAX_OPERATOR_BODY_BYTES);
        value.validate().unwrap();
        value.body.push('x');
        assert!(value.validate().is_err());
    }

    #[test]
    fn operator_sections_are_exact_ordered_unique_and_non_empty() {
        notification().validate().unwrap();

        for body in [
            operator_body("trend continuation").replace("## Risk\n", ""),
            operator_body("trend continuation").replace(
                "## Execution\nmanual only\n\n## Risk",
                "## Risk\ndefined risk\n\n## Execution",
            ),
            operator_body("trend continuation").replace(
                "## Risk\ndefined risk",
                "## Risk\ndefined risk\n\n## Risk\nduplicate",
            ),
            operator_body("trend continuation").replace("## Risk\ndefined risk", "## Risk\n"),
            format!("preface\n{}", operator_body("trend continuation")),
        ] {
            let mut value = notification();
            value.body = body;
            assert!(matches!(
                value.validate(),
                Err(DomainError::Invalid { field: "body", .. })
            ));
        }
    }

    #[test]
    fn automatic_ordering_and_duplicate_targets_fail_closed() {
        let mut value = notification();
        value.automatic_ordering = true;
        assert!(value.validate().is_err());

        let mut value = notification();
        value.targets.push(value.targets[0].clone());
        assert!(value.validate().is_err());
    }

    #[test]
    fn unknown_fields_and_roles_fail_decode() {
        let mut encoded = serde_json::to_value(notification()).unwrap();
        encoded["future"] = serde_json::json!(true);
        assert!(serde_json::from_value::<OperatorNotificationV1>(encoded).is_err());

        let mut encoded = serde_json::to_value(notification()).unwrap();
        encoded["role"] = serde_json::json!("gamma_ambush");
        assert!(serde_json::from_value::<OperatorNotificationV1>(encoded).is_err());
    }

    #[test]
    fn cancellation_is_strict_and_versioned() {
        let cancellation = OperatorNotificationCancellationV1 {
            schema_version: OPERATOR_NOTIFICATION_CANCELLATION_SCHEMA_VERSION.to_owned(),
            event_id: Token::new("event-1", "event_id").unwrap(),
            cancelled_at: "2026-08-04T13:31:00Z".parse().unwrap(),
            reason_code: Token::new("source_invalidated", "reason_code").unwrap(),
        };
        cancellation.validate().unwrap();
        let mut value = serde_json::to_value(cancellation).unwrap();
        value["future"] = serde_json::json!(true);
        assert!(serde_json::from_value::<OperatorNotificationCancellationV1>(value).is_err());
    }
}
