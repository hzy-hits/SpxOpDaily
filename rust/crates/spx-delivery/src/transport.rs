use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use chrono::Utc;
use hmac::{Hmac, KeyInit as _, Mac as _};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use spx_domain::DeliveryChannel;

use crate::{BarkPresentation, DeliveryTarget, RenderedMessage};

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
        let payload = match payload_for(target, request, Utc::now().timestamp()) {
            Ok(payload) => payload,
            Err(result) => return result,
        };
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
        body: String,
        group: &'static str,
    },
    Feishu {
        msg_type: &'static str,
        content: FeishuContent,
        #[serde(skip_serializing_if = "Option::is_none")]
        timestamp: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        sign: Option<String>,
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

fn payload_for<'a>(
    target: &DeliveryTarget,
    request: &'a TransportRequest,
    timestamp: i64,
) -> Result<Payload<'a>, TransportResult> {
    Ok(match target {
        DeliveryTarget::Bark(target) => Payload::Bark {
            title: &request.message.title,
            body: match target.presentation {
                BarkPresentation::Full => bark_full_body(&request.message.body),
                BarkPresentation::Summary => bark_lockscreen_summary(&request.message.body),
            },
            group: "SPX Spark",
        },
        DeliveryTarget::Feishu(target) => {
            let (timestamp, sign) = if let Some(secret_env) = &target.secret_env {
                let secret = std::env::var(secret_env)
                    .map_err(|_| permanent("feishu_secret_unavailable"))?;
                let timestamp_text = timestamp.to_string();
                let signature = feishu_signature(&secret, timestamp)
                    .map_err(|()| permanent("feishu_signing_failed"))?;
                (Some(timestamp_text), Some(signature))
            } else {
                (None, None)
            };
            Payload::Feishu {
                msg_type: "text",
                content: FeishuContent {
                    text: format!("{}\n\n{}", request.message.title, request.message.body),
                },
                timestamp,
                sign,
            }
        }
        DeliveryTarget::Webhook(_) => Payload::Webhook {
            event_id: &request.event_id,
            idempotency_key: &request.idempotency_key,
            title: &request.message.title,
            body: &request.message.body,
        },
    })
}

const BARK_SUMMARY_MAX_LINES: usize = 4;
const BARK_BODY_MAX_CHARS: usize = 1_500;
const BARK_BODY_MAX_BYTES: usize = 4_096;
const ELLIPSIS: char = '…';

fn bark_lockscreen_summary(body: &str) -> String {
    let plain = strip_markdown_light(body);
    let lines: Vec<_> = plain
        .lines()
        .filter(|line| !line.trim().is_empty())
        .take(BARK_SUMMARY_MAX_LINES)
        .collect();
    let summary = if lines.is_empty() {
        plain
    } else {
        lines.join("\n").trim().to_owned()
    };
    truncate_unicode_with_ellipsis(&summary, BARK_BODY_MAX_CHARS, BARK_BODY_MAX_BYTES)
}

fn bark_full_body(body: &str) -> String {
    truncate_unicode_with_ellipsis(body, BARK_BODY_MAX_CHARS, BARK_BODY_MAX_BYTES)
}

fn strip_markdown_light(body: &str) -> String {
    body.lines()
        .map(|raw| {
            let trimmed_start = raw.trim_start();
            let without_heading = trimmed_start
                .strip_prefix('#')
                .map_or(trimmed_start, |rest| {
                    rest.trim_start_matches('#').trim_start()
                });
            let bullet = without_heading
                .strip_prefix("- ")
                .or_else(|| without_heading.strip_prefix("* "));
            let line =
                bullet.map_or_else(|| without_heading.to_owned(), |rest| format!("• {rest}"));
            line.replace("**", "")
                .replace('`', "")
                .trim_end()
                .to_owned()
        })
        .collect::<Vec<_>>()
        .join("\n")
        .trim()
        .to_owned()
}

fn truncate_unicode_with_ellipsis(value: &str, max_chars: usize, max_bytes: usize) -> String {
    if value.chars().count() <= max_chars && value.len() <= max_bytes {
        return value.to_owned();
    }
    let mut output = String::new();
    for (chars, character) in value.chars().enumerate() {
        if chars + 1 >= max_chars
            || output.len() + character.len_utf8() + ELLIPSIS.len_utf8() > max_bytes
        {
            break;
        }
        output.push(character);
    }
    while output.chars().last().is_some_and(char::is_whitespace) {
        output.pop();
    }
    output.push(ELLIPSIS);
    output
}

fn feishu_signature(secret: &str, timestamp: i64) -> Result<String, ()> {
    let string_to_sign = format!("{timestamp}\n{secret}");
    let mac = Hmac::<Sha256>::new_from_slice(string_to_sign.as_bytes()).map_err(|_| ())?;
    Ok(BASE64_STANDARD.encode(mac.finalize().into_bytes()))
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
    use crate::{BarkTarget, FeishuTarget};
    use spx_domain::Token;

    fn request(body: String) -> TransportRequest {
        TransportRequest {
            event_id: "event-1".to_owned(),
            idempotency_key: "event-1:target-1".to_owned(),
            message: RenderedMessage {
                title: "SPX 交易机会".to_owned(),
                body,
            },
        }
    }

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

    #[test]
    fn feishu_signature_matches_the_existing_production_contract() {
        assert_eq!(
            feishu_signature("secret", 1_700_000_000).unwrap(),
            "fiWS2+gh28DOydAv7hzONH/mDn9+b1Y4Y5ivXWXy8vA="
        );
    }

    #[test]
    fn bark_summary_is_unicode_safe_bounded_and_feishu_keeps_full_body() {
        let mode_body = "## 结论\n**顺势做多**：等待 `7565` 回踩\n\n## 执行\n风险边界清晰\n第五行只在完整模式出现";
        let full = DeliveryTarget::Bark(BarkTarget {
            key: Token::new("bark-primary", "target key").unwrap(),
            endpoint_env: "SPX_BARK_ENDPOINT".to_owned(),
            presentation: BarkPresentation::Full,
        });
        let Payload::Bark { body, .. } =
            payload_for(&full, &request(mode_body.to_owned()), 1).unwrap()
        else {
            panic!("expected full bark payload");
        };
        assert_eq!(body, mode_body);
        assert!(body.contains("第五行只在完整模式出现"));

        let summary = DeliveryTarget::Bark(BarkTarget {
            key: Token::new("bark-friend", "target key").unwrap(),
            endpoint_env: "SPX_BARK_FRIEND_ENDPOINT".to_owned(),
            presentation: BarkPresentation::Summary,
        });
        let Payload::Bark { body, .. } =
            payload_for(&summary, &request(mode_body.to_owned()), 1).unwrap()
        else {
            panic!("expected summary bark payload");
        };
        assert!(body.contains("顺势做多"));
        assert!(!body.contains("**"));
        assert!(!body.contains('`'));
        assert!(!body.contains("第五行只在完整模式出现"));

        let full_body = format!(
            "## 结论\n**顺势做多**：等待 `7565` 回踩\n\n## 执行\n{}\n完整飞书结尾",
            "风险边界清晰；".repeat(800)
        );
        let request = request(full_body.clone());
        let bark = DeliveryTarget::Bark(BarkTarget {
            key: Token::new("bark-primary", "target key").unwrap(),
            endpoint_env: "SPX_BARK_ENDPOINT".to_owned(),
            presentation: BarkPresentation::Full,
        });
        let Payload::Bark { body, .. } = payload_for(&bark, &request, 1).unwrap() else {
            panic!("expected bark payload");
        };
        assert!(body.contains("**顺势做多**"));
        assert!(body.chars().count() <= BARK_BODY_MAX_CHARS);
        assert!(body.len() <= BARK_BODY_MAX_BYTES);
        assert!(body.ends_with('…'));
        assert!(std::str::from_utf8(body.as_bytes()).is_ok());

        let feishu = DeliveryTarget::Feishu(FeishuTarget {
            key: Token::new("feishu-primary", "target key").unwrap(),
            endpoint_env: "SPX_FEISHU_ENDPOINT".to_owned(),
            secret_env: None,
        });
        let Payload::Feishu { content, .. } = payload_for(&feishu, &request, 1).unwrap() else {
            panic!("expected feishu payload");
        };
        assert_eq!(
            content.text,
            format!("{}\n\n{full_body}", request.message.title)
        );
    }
}
