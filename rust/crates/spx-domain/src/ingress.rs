use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::validation::require_schema;
use crate::{
    CORE_ACK_SCHEMA_VERSION, DeskMapProjectionV1, DomainError, EvaluationRequestV1,
    INGRESS_SCHEMA_VERSION, OperatorNotificationCancellationV1, OperatorNotificationV1,
    QuoteBatchV1, ResearchSignalsV1, StrategyDistributionForecastV1, Token, Validate,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AckStatus {
    Accepted,
    Rejected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoreAckReason {
    Accepted,
    InvalidContractJson,
    InvalidFrameSize,
    ProcessingRejected,
    ServerBusy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoreAckDisposition {
    Applied,
    DuplicateBatch,
    StaleBatch,
    DuplicateIngress,
    DecisionAccepted,
    ResearchUpdated,
    ResearchUnchanged,
    ResearchStale,
    DeskMapUpdated,
    DeskMapUnchanged,
    DeskMapStale,
    StrategyDistributionUpdated,
    StrategyDistributionUnchanged,
    StrategyDistributionStale,
    OperatorNotificationAccepted,
    OperatorNotificationSemanticSuppressed,
    /// The durable fence was committed; an already in-flight transport is not recalled.
    OperatorNotificationCancellationAccepted,
    /// The same durable fence already existed; an already in-flight transport is not recalled.
    OperatorNotificationCancellationDuplicate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoreAckV1 {
    pub schema_version: String,
    pub status: AckStatus,
    pub message_id: Option<Token>,
    pub decision_id: Option<Token>,
    pub reason_code: CoreAckReason,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disposition: Option<CoreAckDisposition>,
}

impl CoreAckV1 {
    pub fn accepted(
        message_id: Token,
        disposition: CoreAckDisposition,
        decision_id: Option<Token>,
    ) -> Self {
        Self {
            schema_version: CORE_ACK_SCHEMA_VERSION.to_owned(),
            status: AckStatus::Accepted,
            message_id: Some(message_id),
            decision_id,
            reason_code: CoreAckReason::Accepted,
            disposition: Some(disposition),
        }
    }

    pub fn rejected(message_id: Option<Token>, reason_code: CoreAckReason) -> Self {
        Self {
            schema_version: CORE_ACK_SCHEMA_VERSION.to_owned(),
            status: AckStatus::Rejected,
            message_id,
            decision_id: None,
            reason_code,
            disposition: None,
        }
    }
}

impl Validate for CoreAckV1 {
    fn validate(&self) -> Result<(), DomainError> {
        require_schema(
            &self.schema_version,
            CORE_ACK_SCHEMA_VERSION,
            "core acknowledgement",
        )?;
        match self.status {
            AckStatus::Accepted => {
                if self.message_id.is_none()
                    || self.reason_code != CoreAckReason::Accepted
                    || self.disposition.is_none()
                {
                    return Err(DomainError::Invalid {
                        field: "core acknowledgement",
                        reason: "accepted acknowledgement requires message, accepted reason, and disposition",
                    });
                }
                let is_decision = self.disposition == Some(CoreAckDisposition::DecisionAccepted);
                if is_decision != self.decision_id.is_some() {
                    return Err(DomainError::Invalid {
                        field: "core acknowledgement decision",
                        reason: "decision id must exist only for an accepted decision",
                    });
                }
            }
            AckStatus::Rejected => {
                if self.reason_code == CoreAckReason::Accepted
                    || self.decision_id.is_some()
                    || self.disposition.is_some()
                {
                    return Err(DomainError::Invalid {
                        field: "core acknowledgement",
                        reason: "rejected acknowledgement cannot contain accepted outcome fields",
                    });
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "payload",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum IngressMessageV1 {
    QuoteBatch(QuoteBatchV1),
    Evaluate(EvaluationRequestV1),
    ResearchSignals(ResearchSignalsV1),
    DeskMapProjection(Box<DeskMapProjectionV1>),
    StrategyDistributionForecast(Box<StrategyDistributionForecastV1>),
    OperatorNotification(Box<OperatorNotificationV1>),
    OperatorNotificationCancellation(OperatorNotificationCancellationV1),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IngressEnvelopeV1 {
    pub schema_version: String,
    pub message_id: Token,
    pub emitted_at: DateTime<Utc>,
    pub message: IngressMessageV1,
}

impl Validate for IngressEnvelopeV1 {
    fn validate(&self) -> Result<(), DomainError> {
        require_schema(
            &self.schema_version,
            INGRESS_SCHEMA_VERSION,
            "ingress envelope",
        )?;
        match &self.message {
            IngressMessageV1::QuoteBatch(batch) => {
                batch.validate()?;
                if batch.received_at > self.emitted_at {
                    return Err(DomainError::TimeOrder(
                        "quote batch received_at is after envelope emitted_at",
                    ));
                }
                Ok(())
            }
            IngressMessageV1::Evaluate(request) => {
                request.validate()?;
                if request.decision_at > self.emitted_at {
                    return Err(DomainError::TimeOrder(
                        "decision_at is after envelope emitted_at",
                    ));
                }
                Ok(())
            }
            IngressMessageV1::ResearchSignals(signals) => {
                signals.validate()?;
                if signals.generated_at > self.emitted_at {
                    return Err(DomainError::TimeOrder(
                        "research generated_at is after envelope emitted_at",
                    ));
                }
                Ok(())
            }
            IngressMessageV1::DeskMapProjection(projection) => {
                projection.validate()?;
                if projection.available_at > self.emitted_at {
                    return Err(DomainError::TimeOrder(
                        "desk map available_at is after envelope emitted_at",
                    ));
                }
                Ok(())
            }
            IngressMessageV1::StrategyDistributionForecast(forecast) => {
                forecast.validate()?;
                if forecast.available_at > self.emitted_at {
                    return Err(DomainError::TimeOrder(
                        "strategy distribution available_at is after envelope emitted_at",
                    ));
                }
                Ok(())
            }
            IngressMessageV1::OperatorNotification(notification) => {
                notification.validate()?;
                if notification.occurred_at > self.emitted_at {
                    return Err(DomainError::TimeOrder(
                        "operator notification occurred_at is after envelope emitted_at",
                    ));
                }
                Ok(())
            }
            IngressMessageV1::OperatorNotificationCancellation(cancellation) => {
                cancellation.validate()?;
                if cancellation.cancelled_at > self.emitted_at {
                    return Err(DomainError::TimeOrder(
                        "operator notification cancelled_at is after envelope emitted_at",
                    ));
                }
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_message_kind_fails_decode() {
        let payload = r#"{
            "schema_version":"spx_ingress.v1",
            "message_id":"message-1",
            "emitted_at":"2026-08-01T14:00:00Z",
            "message":{"kind":"future_kind","payload":{}}
        }"#;
        assert!(serde_json::from_str::<IngressEnvelopeV1>(payload).is_err());
    }

    #[test]
    fn typed_ack_round_trips_for_bridge_clients() {
        let ack = CoreAckV1::accepted(
            Token::new("message-1", "message_id").unwrap(),
            CoreAckDisposition::Applied,
            None,
        );
        ack.validate().unwrap();
        let encoded = serde_json::to_vec(&ack).unwrap();
        let decoded: CoreAckV1 = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded, ack);
    }

    #[test]
    fn rejected_ack_keeps_existing_reason_code_wire_value() {
        let ack = CoreAckV1::rejected(None, CoreAckReason::InvalidFrameSize);
        ack.validate().unwrap();
        assert_eq!(
            serde_json::to_value(ack).unwrap(),
            serde_json::json!({
                "schema_version": "spx_core_ack.v1",
                "status": "rejected",
                "message_id": null,
                "decision_id": null,
                "reason_code": "invalid_frame_size"
            })
        );
    }

    #[test]
    fn ack_unknown_fields_and_invalid_shapes_fail_closed() {
        let unknown = r#"{
            "schema_version":"spx_core_ack.v1",
            "status":"rejected",
            "message_id":null,
            "decision_id":null,
            "reason_code":"processing_rejected",
            "future":true
        }"#;
        assert!(serde_json::from_str::<CoreAckV1>(unknown).is_err());

        let invalid = CoreAckV1 {
            schema_version: CORE_ACK_SCHEMA_VERSION.to_owned(),
            status: AckStatus::Accepted,
            message_id: Some(Token::new("message-1", "message_id").unwrap()),
            decision_id: None,
            reason_code: CoreAckReason::ProcessingRejected,
            disposition: Some(CoreAckDisposition::Applied),
        };
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn cancellation_must_not_occur_after_envelope_emission() {
        let payload = r#"{
            "schema_version":"spx_ingress.v1",
            "message_id":"cancel-message-1",
            "emitted_at":"2026-08-04T14:00:00Z",
            "message":{
                "kind":"operator_notification_cancellation",
                "payload":{
                    "schema_version":"operator_notification_cancellation.v1",
                    "event_id":"event-1",
                    "cancelled_at":"2026-08-04T14:00:01Z",
                    "reason_code":"source_invalidated"
                }
            }
        }"#;
        let envelope: IngressEnvelopeV1 = serde_json::from_str(payload).unwrap();
        assert!(matches!(
            envelope.validate(),
            Err(DomainError::TimeOrder(_))
        ));
    }
}
