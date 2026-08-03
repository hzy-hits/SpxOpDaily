use std::fmt::{Debug, Formatter};
use std::time::Duration;

use thiserror::Error;

pub const DEEPSEEK_CHAT_COMPLETIONS_URL: &str = "https://api.deepseek.com/v1/chat/completions";

/// Complete request handed to a transport. It contains only the API-key environment
/// variable name, never the credential value.
#[derive(Clone, PartialEq)]
pub struct TransportRequest {
    endpoint: &'static str,
    api_key_env: String,
    body: serde_json::Value,
}

impl Debug for TransportRequest {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TransportRequest")
            .field("endpoint", &self.endpoint)
            .field("api_key_env", &self.api_key_env)
            .field("body", &"<redacted>")
            .finish()
    }
}

impl TransportRequest {
    pub(crate) fn new(api_key_env: String, body: serde_json::Value) -> Self {
        Self {
            endpoint: DEEPSEEK_CHAT_COMPLETIONS_URL,
            api_key_env,
            body,
        }
    }

    pub const fn endpoint(&self) -> &'static str {
        self.endpoint
    }

    pub fn api_key_env(&self) -> &str {
        &self.api_key_env
    }

    pub const fn body(&self) -> &serde_json::Value {
        &self.body
    }
}

/// HTTP response body preserved exactly as received by the transport.
#[derive(Clone, PartialEq, Eq)]
pub struct TransportResponse {
    status: u16,
    body: String,
}

impl TransportResponse {
    pub fn new(status: u16, body: impl Into<String>) -> Self {
        Self {
            status,
            body: body.into(),
        }
    }

    pub(crate) fn into_parts(self) -> (u16, String) {
        (self.status, self.body)
    }
}

impl Debug for TransportResponse {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TransportResponse")
            .field("status", &self.status)
            .field("body", &"<redacted>")
            .field("body_bytes", &self.body.len())
            .finish()
    }
}

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("API key environment variable is unavailable: {env_name}")]
    ApiKeyUnavailable { env_name: String },
    #[error("API key environment variable is not valid UTF-8: {env_name}")]
    ApiKeyInvalidEncoding { env_name: String },
    #[error("DeepSeek request failed")]
    Request(#[source] ureq::Error),
    #[error("failed to read the complete DeepSeek response")]
    ResponseRead(#[source] ureq::Error),
    #[error("DeepSeek response is not valid UTF-8")]
    ResponseEncoding(#[source] std::string::FromUtf8Error),
}

pub trait Transport: Send + Sync {
    /// Sends one non-streaming completion request.
    ///
    /// # Errors
    ///
    /// Returns an error when credentials are unavailable or the HTTP exchange cannot
    /// produce a complete response body.
    fn send(&self, request: &TransportRequest) -> Result<TransportResponse, TransportError>;
}

#[derive(Debug, Clone)]
pub struct DeepSeekHttpTransport {
    agent: ureq::Agent,
}

impl DeepSeekHttpTransport {
    pub(crate) fn new(timeout: Duration) -> Self {
        let config = ureq::Agent::config_builder()
            .https_only(true)
            .http_status_as_error(false)
            .timeout_global(Some(timeout))
            .build();
        Self {
            agent: config.into(),
        }
    }
}

impl Transport for DeepSeekHttpTransport {
    fn send(&self, request: &TransportRequest) -> Result<TransportResponse, TransportError> {
        let api_key = match std::env::var(request.api_key_env()) {
            Ok(value) if !value.trim().is_empty() => value,
            Ok(_) | Err(std::env::VarError::NotPresent) => {
                return Err(TransportError::ApiKeyUnavailable {
                    env_name: request.api_key_env().to_owned(),
                });
            }
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err(TransportError::ApiKeyInvalidEncoding {
                    env_name: request.api_key_env().to_owned(),
                });
            }
        };
        let mut response = self
            .agent
            .post(request.endpoint())
            .header("Authorization", format!("Bearer {api_key}"))
            .send_json(request.body())
            .map_err(TransportError::Request)?;
        let status = response.status().as_u16();
        let bytes = response
            .body_mut()
            .read_to_vec()
            .map_err(TransportError::ResponseRead)?;
        let body = String::from_utf8(bytes).map_err(TransportError::ResponseEncoding)?;
        Ok(TransportResponse::new(status, body))
    }
}
