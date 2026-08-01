use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::validation::require_schema;
use crate::{
    DomainError, EvaluationRequestV1, INGRESS_SCHEMA_VERSION, QuoteBatchV1, Token, Validate,
};

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
}
