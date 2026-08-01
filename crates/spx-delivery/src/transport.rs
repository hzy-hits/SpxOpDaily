use std::time::Duration;

use serde::{Deserialize, Serialize};
use spx_domain::DeliveryChannel;

use crate::{DeliveryTarget, RenderedMessage};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportRequest {
    pub event_id: String,
    pub idempotency_key: String,
    pub message: RenderedMessage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportResult {
    Delivered { provider_message_id: Option<String> },
    Retryable { error_code: String },
    PermanentFailure { error_code: String },
    Uncertain { error_code: String },
}

pub trait Transport: Send + Sync {
    fn send(&self, target: &DeliveryTarget, request: &TransportRequest) -> TransportResult;
}

#[derive(Debug, Clone)]
pub struct HttpTransport {
    agent: ureq::Agent,
}

impl HttpTransport {
    pub(crate) fn new(timeout: Duration) -> Self {
        let config = ureq::Agent::config_builder()
            .https_only(true)
            .timeout_global(Some(timeout))
            .build();
        Self {
            agent: config.into(),
        }
    }
}

impl Transport for HttpTransport {
    fn send(&self, target: &DeliveryTarget, request: &TransportRequest) -> TransportResult {
        let Ok(endpoint) = std::env::var(target.endpoint_env()) else {
            return permanent("endpoint_secret_unavailable");
        };
        if !endpoint.starts_with("https://") {
            return permanent("endpoint_must_use_https");
        }
        let payload = payload_for(target, request);
        let mut builder = self
            .agent
            .post(endpoint)
            .header("Idempotency-Key", &request.idempotency_key);
        if let DeliveryTarget::Webhook(target) = target
            && let Some(environment_name) = &target.bearer_token_env
        {
            let Ok(token) = std::env::var(environment_name) else {
                return permanent("authorization_secret_unavailable");
            };
            builder = builder.header("Authorization", format!("Bearer {token}"));
        }
        match builder.send_json(&payload) {
            Ok(mut response) if response.status().is_success() => {
                let provider_message_id = response
                    .headers()
                    .get("x-request-id")
                    .and_then(|value| value.to_str().ok())
                    .map(ToOwned::to_owned);
                match target.channel() {
                    DeliveryChannel::Webhook => TransportResult::Delivered {
                        provider_message_id,
                    },
                    channel @ (DeliveryChannel::Bark | DeliveryChannel::Feishu) => {
                        match response.body_mut().read_json::<ProviderAck>() {
                            Ok(ack) => match ack.outcome(channel) {
                                ProviderAckOutcome::Delivered => TransportResult::Delivered {
                                    provider_message_id,
                                },
                                ProviderAckOutcome::Rejected => permanent("provider_rejected"),
                                ProviderAckOutcome::Missing => uncertain("provider_ack_missing"),
                            },
                            Err(_) => uncertain("provider_ack_invalid"),
                        }
                    }
                }
            }
            Ok(response) => classify_http_status(response.status().as_u16()),
            Err(error) => classify_error(&error),
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum Payload<'a> {
    Bark {
        title: &'a str,
        body: &'a str,
        group: &'static str,
    },
    Feishu {
        msg_type: &'static str,
        content: FeishuContent,
    },
    Webhook {
        event_id: &'a str,
        idempotency_key: &'a str,
        title: &'a str,
        body: &'a str,
    },
}

#[derive(Debug, Serialize)]
struct FeishuContent {
    text: String,
}

#[derive(Debug, Deserialize)]
struct ProviderAck {
    code: Option<ProviderCode>,
    #[serde(rename = "StatusCode")]
    legacy_status_code: Option<ProviderCode>,
}

impl ProviderAck {
    fn outcome(&self, channel: DeliveryChannel) -> ProviderAckOutcome {
        let (code, expected) = match channel {
            DeliveryChannel::Bark => (self.code.as_ref(), 200),
            DeliveryChannel::Feishu => (self.code.as_ref().or(self.legacy_status_code.as_ref()), 0),
            DeliveryChannel::Webhook => return ProviderAckOutcome::Delivered,
        };
        match code {
            Some(code) if code.equals(expected) => ProviderAckOutcome::Delivered,
            Some(_) => ProviderAckOutcome::Rejected,
            None => ProviderAckOutcome::Missing,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProviderAckOutcome {
    Delivered,
    Rejected,
    Missing,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ProviderCode {
    Integer(i64),
    Text(String),
}

impl ProviderCode {
    fn equals(&self, expected: i64) -> bool {
        match self {
            Self::Integer(actual) => *actual == expected,
            Self::Text(actual) => actual.parse::<i64>() == Ok(expected),
        }
    }
}

fn payload_for<'a>(target: &DeliveryTarget, request: &'a TransportRequest) -> Payload<'a> {
    match target {
        DeliveryTarget::Bark(_) => Payload::Bark {
            title: &request.message.title,
            body: &request.message.body,
            group: "SPX Spark",
        },
        DeliveryTarget::Feishu(_) => Payload::Feishu {
            msg_type: "text",
            content: FeishuContent {
                text: format!("{}\n\n{}", request.message.title, request.message.body),
            },
        },
        DeliveryTarget::Webhook(_) => Payload::Webhook {
            event_id: &request.event_id,
            idempotency_key: &request.idempotency_key,
            title: &request.message.title,
            body: &request.message.body,
        },
    }
}

fn classify_error(error: &ureq::Error) -> TransportResult {
    match error {
        ureq::Error::StatusCode(status) => classify_http_status(*status),
        ureq::Error::Timeout(_) => uncertain("transport_timeout"),
        ureq::Error::BadUri(_)
        | ureq::Error::Http(_)
        | ureq::Error::InvalidProxyUrl
        | ureq::Error::RequireHttpsOnly(_)
        | ureq::Error::BodyExceedsLimit(_)
        | ureq::Error::Json(_) => permanent("transport_request_invalid"),
        _ => uncertain("transport_interrupted"),
    }
}

fn classify_http_status(status: u16) -> TransportResult {
    match status {
        429 => retryable("http_429"),
        500..=599 => uncertain("http_5xx_outcome_unknown"),
        400..=499 => permanent("http_4xx"),
        _ => permanent("unexpected_http_status"),
    }
}

fn retryable(code: &str) -> TransportResult {
    TransportResult::Retryable {
        error_code: code.to_owned(),
    }
}

fn permanent(code: &str) -> TransportResult {
    TransportResult::PermanentFailure {
        error_code: code.to_owned(),
    }
}

fn uncertain(code: &str) -> TransportResult {
    TransportResult::Uncertain {
        error_code: code.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_explicit_pre_transport_throttling_is_retryable() {
        assert!(matches!(
            classify_http_status(429),
            TransportResult::Retryable { .. }
        ));
        assert!(matches!(
            classify_http_status(503),
            TransportResult::Uncertain { .. }
        ));
        assert!(matches!(
            classify_http_status(408),
            TransportResult::PermanentFailure { .. }
        ));
        assert!(matches!(
            classify_http_status(413),
            TransportResult::PermanentFailure { .. }
        ));
        assert!(matches!(
            classify_http_status(302),
            TransportResult::PermanentFailure { .. }
        ));
    }

    #[test]
    fn channel_acknowledgements_must_confirm_delivery() {
        let bark: ProviderAck = serde_json::from_str(r#"{"code": 200}"#).unwrap();
        let feishu: ProviderAck = serde_json::from_str(r#"{"code": "0"}"#).unwrap();
        let legacy_feishu: ProviderAck = serde_json::from_str(r#"{"StatusCode": 0}"#).unwrap();
        let missing: ProviderAck = serde_json::from_str(r"{}").unwrap();
        assert_eq!(
            bark.outcome(DeliveryChannel::Bark),
            ProviderAckOutcome::Delivered
        );
        assert_eq!(
            feishu.outcome(DeliveryChannel::Feishu),
            ProviderAckOutcome::Delivered
        );
        assert_eq!(
            legacy_feishu.outcome(DeliveryChannel::Feishu),
            ProviderAckOutcome::Delivered
        );
        assert_eq!(
            bark.outcome(DeliveryChannel::Feishu),
            ProviderAckOutcome::Rejected
        );
        assert_eq!(
            missing.outcome(DeliveryChannel::Bark),
            ProviderAckOutcome::Missing
        );
    }
}
