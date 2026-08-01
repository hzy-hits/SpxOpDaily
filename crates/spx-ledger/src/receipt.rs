use chrono::{DateTime, Utc};
use rusqlite::{Transaction, params};
use spx_domain::{
    DELIVERY_RECEIPT_SCHEMA_VERSION, DeliveryChannel, DeliveryReceiptV1, ReceiptOutcome, Token,
    Validate,
};
use uuid::Uuid;

use crate::LedgerError;
use crate::db::micros;

pub(crate) enum Receipt<'a> {
    Delivered {
        attempt_id: &'a str,
        provider_message_id: Option<&'a str>,
    },
    RetryScheduled {
        attempt_id: &'a str,
        reason_code: &'a str,
    },
    RetryExhausted {
        attempt_id: &'a str,
        reason_code: &'a str,
    },
    PermanentFailureAfterTransport {
        attempt_id: &'a str,
        reason_code: &'a str,
    },
    PermanentFailureBeforeTransport {
        reason_code: &'a str,
    },
    CancelledBeforeTransport {
        reason_code: &'a str,
    },
    ExpiredBeforeTransport,
    TransportUncertain {
        attempt_id: &'a str,
        reason_code: &'a str,
    },
    ClaimRecovered,
}

struct ReceiptFields<'a> {
    attempt_id: Option<&'a str>,
    outcome: &'static str,
    attempted: bool,
    ok: bool,
    queued_for_retry: bool,
    reason_code: &'a str,
    provider_message_id: Option<&'a str>,
}

impl<'a> Receipt<'a> {
    fn fields(&self) -> ReceiptFields<'a> {
        match self {
            Self::Delivered {
                attempt_id,
                provider_message_id,
            } => ReceiptFields {
                attempt_id: Some(attempt_id),
                outcome: "delivered",
                attempted: true,
                ok: true,
                queued_for_retry: false,
                reason_code: "delivered",
                provider_message_id: *provider_message_id,
            },
            Self::RetryScheduled {
                attempt_id,
                reason_code,
            } => ReceiptFields {
                attempt_id: Some(attempt_id),
                outcome: "retryable_failure",
                attempted: true,
                ok: false,
                queued_for_retry: true,
                reason_code,
                provider_message_id: None,
            },
            Self::RetryExhausted {
                attempt_id,
                reason_code,
            } => ReceiptFields {
                attempt_id: Some(attempt_id),
                outcome: "retry_exhausted",
                attempted: true,
                ok: false,
                queued_for_retry: false,
                reason_code,
                provider_message_id: None,
            },
            Self::PermanentFailureAfterTransport {
                attempt_id,
                reason_code,
            } => ReceiptFields {
                attempt_id: Some(attempt_id),
                outcome: "permanent_failure",
                attempted: true,
                ok: false,
                queued_for_retry: false,
                reason_code,
                provider_message_id: None,
            },
            Self::PermanentFailureBeforeTransport { reason_code } => ReceiptFields {
                attempt_id: None,
                outcome: "permanent_failure",
                attempted: false,
                ok: false,
                queued_for_retry: false,
                reason_code,
                provider_message_id: None,
            },
            Self::CancelledBeforeTransport { reason_code } => ReceiptFields {
                attempt_id: None,
                outcome: "cancelled_before_transport",
                attempted: false,
                ok: false,
                queued_for_retry: false,
                reason_code,
                provider_message_id: None,
            },
            Self::ExpiredBeforeTransport => ReceiptFields {
                attempt_id: None,
                outcome: "expired_before_transport",
                attempted: false,
                ok: false,
                queued_for_retry: false,
                reason_code: "ttl_expired",
                provider_message_id: None,
            },
            Self::TransportUncertain {
                attempt_id,
                reason_code,
            } => ReceiptFields {
                attempt_id: Some(attempt_id),
                outcome: "transport_uncertain",
                attempted: true,
                ok: false,
                queued_for_retry: false,
                reason_code,
                provider_message_id: None,
            },
            Self::ClaimRecovered => ReceiptFields {
                attempt_id: None,
                outcome: "claim_recovered",
                attempted: false,
                ok: false,
                queued_for_retry: true,
                reason_code: "claim_recovered_before_transport",
                provider_message_id: None,
            },
        }
    }
}

pub(crate) fn insert_receipt(
    transaction: &Transaction<'_>,
    target_id: &str,
    receipt: &Receipt<'_>,
    now: DateTime<Utc>,
) -> Result<(), LedgerError> {
    let fields = receipt.fields();
    let (intent_id, target_key, channel): (String, String, String) = transaction.query_row(
        "SELECT t.event_id, t.target_key, t.channel
         FROM notification_targets t WHERE t.target_id = ?1",
        [target_id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    let channel = match channel.as_str() {
        "bark" => DeliveryChannel::Bark,
        "feishu" => DeliveryChannel::Feishu,
        "webhook" => DeliveryChannel::Webhook,
        _ => return Err(LedgerError::InvalidValue("receipt channel")),
    };
    let receipt_id = Token::new(Uuid::now_v7().to_string(), "receipt_id")?;
    let outcome = match receipt {
        Receipt::Delivered { .. } => ReceiptOutcome::Delivered,
        Receipt::RetryScheduled { .. } | Receipt::ClaimRecovered => ReceiptOutcome::RetryScheduled,
        Receipt::RetryExhausted { .. }
        | Receipt::PermanentFailureAfterTransport { .. }
        | Receipt::PermanentFailureBeforeTransport { .. } => ReceiptOutcome::DeadLetter,
        Receipt::CancelledBeforeTransport { .. } => ReceiptOutcome::Cancelled,
        Receipt::ExpiredBeforeTransport => ReceiptOutcome::Expired,
        Receipt::TransportUncertain { .. } => ReceiptOutcome::Uncertain,
    };
    let wire_receipt = DeliveryReceiptV1 {
        schema_version: DELIVERY_RECEIPT_SCHEMA_VERSION.to_owned(),
        receipt_id: receipt_id.clone(),
        target_id: Token::new(target_id, "target_id")?,
        intent_id: Token::new(intent_id, "intent_id")?,
        target_key: Token::new(target_key, "target_key")?,
        channel,
        outcome,
        attempted_at: now,
        provider_message_id: fields
            .provider_message_id
            .map(|value| Token::new(value, "provider_message_id"))
            .transpose()?,
        error_code: (outcome != ReceiptOutcome::Delivered)
            .then(|| Token::new(fields.reason_code, "error_code"))
            .transpose()?,
    };
    wire_receipt.validate()?;
    let payload_json = serde_json::to_string(&wire_receipt)?;
    let inserted = transaction.execute(
        "INSERT INTO delivery_receipts (
            receipt_id, target_id, intent_id, target_key, channel,
            attempt_id, outcome, attempted, ok,
            queued_for_retry, reason_code, provider_message_id, occurred_at_us, payload_json
         ) SELECT ?1, t.target_id, t.event_id, t.target_key, t.channel,
                  ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11
           FROM notification_targets t WHERE t.target_id = ?2",
        params![
            receipt_id.as_str(),
            target_id,
            fields.attempt_id,
            fields.outcome,
            fields.attempted,
            fields.ok,
            fields.queued_for_retry,
            fields.reason_code,
            fields.provider_message_id,
            micros(now),
            payload_json
        ],
    )?;
    if inserted == 1 {
        Ok(())
    } else {
        Err(LedgerError::InvalidValue("receipt target"))
    }
}
