use std::fmt::{Debug, Display, Formatter};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use spx_domain::{DeskMapProjectionV1, DeskMessageV2, Token, Validate};

use crate::{
    DeepSeekHttpTransport, ReportWriterConfig, Transport, TransportRequest, TransportResponse,
};

pub const DEEPSEEK_MODEL_ID: &str = "deepseek-v4-flash";
pub const RESEARCH_UNAVAILABLE_DISCLOSURE: &str =
    "Research context: unavailable; no HMM, range, close-location, or market-maker inference.";

const DESK_MESSAGE_SYSTEM_PROMPT: &str = r"You are the SPX desk report writer.
Return exactly one JSON object and no surrounding prose or Markdown fence.
The object must contain exactly these string fields:
title, desk_view, location, structure, primary_path, alternative_path, targets, execution, data_quality.
Every field must be non-empty. Do not add, remove, rename, or nest fields.
Use the complete desk_map_projection.v1 JSON and explicit research-context block supplied by the user as the sole factual authority.
Preserve every decision-relevant condition, level, lifecycle state, target, execution constraint, and data-quality limitation.
Embedded research_context.v2 is bootstrap-unvalidated advisory evidence with no action authority; preserve its status and reason codes and never call a latent state market-maker behavior.
research_context_status=embedded_contract_valid means only that the wire contract passed; every nested regime and forecast availability/status remains authoritative and any unavailable component must be disclosed.
When research_context_status is unavailable, data_quality must explicitly say research is unavailable and must make no HMM, range, or close-location claim.
You may reorganize or add clarification, but every output section (and the title) must contain at least as many UTF-8 bytes as its corresponding source message field.
Do not shorten the report for a notification transport. Do not invent orders, fills, positions, probabilities, or market-maker behavior.";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
enum DeepSeekModel {
    #[serde(rename = "deepseek-v4-flash")]
    V4Flash,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReasoningEffort {
    Max,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ThinkingMode {
    Enabled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
struct Thinking {
    #[serde(rename = "type")]
    mode: ThinkingMode,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ReportPrompt {
    system: String,
    user: String,
}

impl Debug for ReportPrompt {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReportPrompt")
            .field("system", &"<redacted>")
            .field("system_bytes", &self.system.len())
            .field("user", &"<redacted>")
            .field("user_bytes", &self.user.len())
            .finish()
    }
}

impl ReportPrompt {
    pub fn new(system: impl Into<String>, user: impl Into<String>) -> Self {
        Self {
            system: system.into(),
            user: user.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum MessageRole {
    System,
    User,
}

#[derive(Debug, Serialize)]
struct Message<'a> {
    role: MessageRole,
    content: &'a str,
}

#[derive(Debug, Serialize)]
struct CompletionRequest<'a> {
    model: DeepSeekModel,
    messages: [Message<'a>; 2],
    thinking: Thinking,
    reasoning_effort: ReasoningEffort,
    response_format: ResponseFormat,
    max_tokens: crate::MaxTokens,
    stream: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ResponseFormatKind {
    JsonObject,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
struct ResponseFormat {
    #[serde(rename = "type")]
    kind: ResponseFormatKind,
}

#[derive(Debug, Deserialize)]
struct CompletionResponse {
    model: String,
    choices: Vec<CompletionChoice>,
}

#[derive(Debug, Deserialize)]
struct CompletionChoice {
    finish_reason: Option<String>,
    message: CompletionMessage,
}

#[derive(Debug, Deserialize)]
struct CompletionMessage {
    content: Option<String>,
}

/// Non-sensitive metadata for response reconciliation and audit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResponseMetadata {
    pub http_status: u16,
    pub raw_response_bytes: usize,
    pub raw_response_sha256: String,
    pub response_model: Option<String>,
    pub finish_reason: Option<String>,
    pub visible_content_bytes: Option<usize>,
}

impl ResponseMetadata {
    fn new(http_status: u16, raw_response: &str) -> Self {
        Self {
            http_status,
            raw_response_bytes: raw_response.len(),
            raw_response_sha256: hex::encode(Sha256::digest(raw_response.as_bytes())),
            response_model: None,
            finish_reason: None,
            visible_content_bytes: None,
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ReportWriterOutput {
    pub content: String,
    pub metadata: ResponseMetadata,
}

impl Debug for ReportWriterOutput {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReportWriterOutput")
            .field("content", &"<redacted>")
            .field("content_bytes", &self.content.len())
            .field("metadata", &self.metadata)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct DeskReportOutput {
    pub message: DeskMessageV2,
    pub visible_content: String,
    pub metadata: ResponseMetadata,
}

impl Debug for DeskReportOutput {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DeskReportOutput")
            .field("message", &"<redacted>")
            .field("visible_content", &"<redacted>")
            .field("visible_content_bytes", &self.visible_content.len())
            .field("metadata", &self.metadata)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReportWriterErrorCode {
    NetworkNotAuthorized,
    InvalidConfig,
    ProjectionInvalid,
    RequestSerialization,
    Transport,
    HttpStatus,
    InvalidProviderJson,
    InvalidChoiceCount,
    UnexpectedModel,
    MissingFinishReason,
    OutputTruncated,
    RejectedFinishReason,
    MissingContent,
    DeskMessageInvalidJson,
    DeskMessageInvalidContract,
    OutputCompressed,
    ResearchDisclosureFailed,
}

impl ReportWriterErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NetworkNotAuthorized => "network_not_authorized",
            Self::InvalidConfig => "invalid_config",
            Self::ProjectionInvalid => "projection_invalid",
            Self::RequestSerialization => "request_serialization_failed",
            Self::Transport => "transport_failed",
            Self::HttpStatus => "http_status_failed",
            Self::InvalidProviderJson => "invalid_provider_json",
            Self::InvalidChoiceCount => "invalid_choice_count",
            Self::UnexpectedModel => "unexpected_model",
            Self::MissingFinishReason => "missing_finish_reason",
            Self::OutputTruncated => "output_truncated",
            Self::RejectedFinishReason => "rejected_finish_reason",
            Self::MissingContent => "missing_content",
            Self::DeskMessageInvalidJson => "desk_message_invalid_json",
            Self::DeskMessageInvalidContract => "desk_message_invalid_contract",
            Self::OutputCompressed => "output_compressed",
            Self::ResearchDisclosureFailed => "research_disclosure_failed",
        }
    }
}

impl Display for ReportWriterErrorCode {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReportWriterError {
    code: ReportWriterErrorCode,
    metadata: Option<ResponseMetadata>,
}

impl ReportWriterError {
    const fn new(code: ReportWriterErrorCode) -> Self {
        Self {
            code,
            metadata: None,
        }
    }

    const fn with_metadata(code: ReportWriterErrorCode, metadata: ResponseMetadata) -> Self {
        Self {
            code,
            metadata: Some(metadata),
        }
    }

    pub const fn code(&self) -> ReportWriterErrorCode {
        self.code
    }

    pub const fn metadata(&self) -> Option<&ResponseMetadata> {
        self.metadata.as_ref()
    }
}

impl Display for ReportWriterError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.code, formatter)
    }
}

impl std::error::Error for ReportWriterError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NetworkGate {
    config_enabled: bool,
    caller_allowed: bool,
}

impl NetworkGate {
    const fn new(config_enabled: bool, caller_allowed: bool) -> Self {
        Self {
            config_enabled,
            caller_allowed,
        }
    }

    const fn authorized(self) -> bool {
        self.config_enabled && self.caller_allowed
    }
}

pub struct ReportWriterClient<T: Transport> {
    config: ReportWriterConfig,
    transport: T,
    gate: NetworkGate,
}

impl ReportWriterClient<DeepSeekHttpTransport> {
    /// Creates an HTTPS-backed writer after both network authorization gates pass.
    ///
    /// # Errors
    ///
    /// Returns a typed error when configuration is invalid or either network gate is false.
    pub fn new_http(
        config: ReportWriterConfig,
        allow_network: bool,
    ) -> Result<Self, ReportWriterError> {
        let timeout = Duration::from_secs(config.request_timeout_seconds());
        let transport = DeepSeekHttpTransport::new(timeout);
        Self::new(config, allow_network, transport)
    }
}

impl<T: Transport> ReportWriterClient<T> {
    /// Creates a writer with an injected transport after both network gates pass.
    ///
    /// # Errors
    ///
    /// Returns a typed error when configuration is invalid or either network gate is false.
    pub fn new(
        config: ReportWriterConfig,
        allow_network: bool,
        transport: T,
    ) -> Result<Self, ReportWriterError> {
        config
            .validate()
            .map_err(|_| ReportWriterError::new(ReportWriterErrorCode::InvalidConfig))?;
        let gate = NetworkGate::new(config.network_enabled(), allow_network);
        if !gate.authorized() {
            return Err(ReportWriterError::new(
                ReportWriterErrorCode::NetworkNotAuthorized,
            ));
        }
        Ok(Self {
            config,
            transport,
            gate,
        })
    }

    /// Generates a complete non-streaming visible response.
    ///
    /// No output text is trimmed or truncated. Provider reasoning and the raw response
    /// envelope are discarded after non-sensitive audit metadata is computed.
    ///
    /// # Errors
    ///
    /// Returns a typed error when authorization is absent, transport fails, or the
    /// provider response violates the fixed response contract.
    pub fn write(&self, prompt: &ReportPrompt) -> Result<ReportWriterOutput, ReportWriterError> {
        self.require_network()?;
        let request = CompletionRequest {
            model: DeepSeekModel::V4Flash,
            messages: [
                Message {
                    role: MessageRole::System,
                    content: &prompt.system,
                },
                Message {
                    role: MessageRole::User,
                    content: &prompt.user,
                },
            ],
            thinking: Thinking {
                mode: ThinkingMode::Enabled,
            },
            reasoning_effort: ReasoningEffort::Max,
            response_format: ResponseFormat {
                kind: ResponseFormatKind::JsonObject,
            },
            max_tokens: self.config.max_tokens(),
            stream: false,
        };
        let body = serde_json::to_value(request)
            .map_err(|_| ReportWriterError::new(ReportWriterErrorCode::RequestSerialization))?;
        let transport_request =
            TransportRequest::new(self.config.api_key_env().as_str().to_owned(), body);
        let response = self
            .transport
            .send(&transport_request)
            .map_err(|_| ReportWriterError::new(ReportWriterErrorCode::Transport))?;
        parse_response(response)
    }

    /// Writes one canonical `DeskMessageV2` from a complete `DeskMapProjectionV1`.
    ///
    /// The full projection JSON is included in the prompt without field, line, or
    /// character reduction. The visible model response must be exactly one strict
    /// `DeskMessageV2` JSON object and pass the domain validation boundary.
    ///
    /// # Errors
    ///
    /// Returns a typed error when the source projection is invalid, the provider call
    /// fails, or the visible response is not a valid canonical desk message.
    pub fn write_desk_map(
        &self,
        projection: &DeskMapProjectionV1,
    ) -> Result<DeskReportOutput, ReportWriterError> {
        projection
            .validate()
            .map_err(|_| ReportWriterError::new(ReportWriterErrorCode::ProjectionInvalid))?;
        let projection_json = serde_json::to_string_pretty(projection)
            .map_err(|_| ReportWriterError::new(ReportWriterErrorCode::RequestSerialization))?;
        let research_input = match &projection.research_context {
            Some(research_context) => {
                let research_json =
                    serde_json::to_string_pretty(research_context).map_err(|_| {
                        ReportWriterError::new(ReportWriterErrorCode::RequestSerialization)
                    })?;
                format!(
                    "research_context_status=embedded_contract_valid\nresearch_context.v2 JSON follows:\n{research_json}"
                )
            }
            None => format!(
                "research_context_status=unavailable\nRequired data_quality disclosure: {RESEARCH_UNAVAILABLE_DISCLOSURE}"
            ),
        };
        let prompt = ReportPrompt::new(
            DESK_MESSAGE_SYSTEM_PROMPT,
            format!("desk_map_projection.v1 JSON follows:\n{projection_json}\n\n{research_input}"),
        );
        let output = self.write(&prompt)?;
        let mut message: DeskMessageV2 = serde_json::from_str(&output.content).map_err(|_| {
            ReportWriterError::with_metadata(
                ReportWriterErrorCode::DeskMessageInvalidJson,
                output.metadata.clone(),
            )
        })?;
        message.validate().map_err(|_| {
            ReportWriterError::with_metadata(
                ReportWriterErrorCode::DeskMessageInvalidContract,
                output.metadata.clone(),
            )
        })?;
        if message_is_compressed(&message, &projection.message) {
            return Err(ReportWriterError::with_metadata(
                ReportWriterErrorCode::OutputCompressed,
                output.metadata,
            ));
        }
        if projection.research_context.is_none()
            && !message
                .data_quality
                .as_str()
                .contains(RESEARCH_UNAVAILABLE_DISCLOSURE)
        {
            message.data_quality = Token::new(
                format!(
                    "{}\n{RESEARCH_UNAVAILABLE_DISCLOSURE}",
                    message.data_quality
                ),
                "desk report data_quality",
            )
            .map_err(|_| {
                ReportWriterError::with_metadata(
                    ReportWriterErrorCode::ResearchDisclosureFailed,
                    output.metadata.clone(),
                )
            })?;
        }
        Ok(DeskReportOutput {
            message,
            visible_content: output.content,
            metadata: output.metadata,
        })
    }

    fn require_network(&self) -> Result<(), ReportWriterError> {
        if self.gate.authorized() {
            Ok(())
        } else {
            Err(ReportWriterError::new(
                ReportWriterErrorCode::NetworkNotAuthorized,
            ))
        }
    }
}

fn message_is_compressed(actual: &DeskMessageV2, source: &DeskMessageV2) -> bool {
    let actual_fields = [
        &actual.title,
        &actual.desk_view,
        &actual.location,
        &actual.structure,
        &actual.primary_path,
        &actual.alternative_path,
        &actual.targets,
        &actual.execution,
        &actual.data_quality,
    ];
    let source_fields = [
        &source.title,
        &source.desk_view,
        &source.location,
        &source.structure,
        &source.primary_path,
        &source.alternative_path,
        &source.targets,
        &source.execution,
        &source.data_quality,
    ];
    actual_fields
        .into_iter()
        .zip(source_fields)
        .any(|(actual, source)| actual.as_str().len() < source.as_str().len())
}

fn parse_response(response: TransportResponse) -> Result<ReportWriterOutput, ReportWriterError> {
    let (status, raw_response) = response.into_parts();
    let mut metadata = ResponseMetadata::new(status, &raw_response);
    if !(200..300).contains(&status) {
        return Err(ReportWriterError::with_metadata(
            ReportWriterErrorCode::HttpStatus,
            metadata,
        ));
    }
    let parsed: CompletionResponse = serde_json::from_str(&raw_response).map_err(|_| {
        ReportWriterError::with_metadata(
            ReportWriterErrorCode::InvalidProviderJson,
            metadata.clone(),
        )
    })?;
    metadata.response_model = Some(parsed.model.clone());
    if parsed.model != DEEPSEEK_MODEL_ID {
        return Err(ReportWriterError::with_metadata(
            ReportWriterErrorCode::UnexpectedModel,
            metadata,
        ));
    }
    let [choice] = parsed.choices.try_into().map_err(|_| {
        ReportWriterError::with_metadata(
            ReportWriterErrorCode::InvalidChoiceCount,
            metadata.clone(),
        )
    })?;
    let finish_reason = choice.finish_reason.ok_or_else(|| {
        ReportWriterError::with_metadata(
            ReportWriterErrorCode::MissingFinishReason,
            metadata.clone(),
        )
    })?;
    metadata.finish_reason = Some(finish_reason.clone());
    if finish_reason == "length" {
        return Err(ReportWriterError::with_metadata(
            ReportWriterErrorCode::OutputTruncated,
            metadata,
        ));
    }
    if finish_reason != "stop" {
        return Err(ReportWriterError::with_metadata(
            ReportWriterErrorCode::RejectedFinishReason,
            metadata,
        ));
    }
    let content = choice.message.content.ok_or_else(|| {
        ReportWriterError::with_metadata(ReportWriterErrorCode::MissingContent, metadata.clone())
    })?;
    if content.trim().is_empty() {
        return Err(ReportWriterError::with_metadata(
            ReportWriterErrorCode::MissingContent,
            metadata,
        ));
    }
    metadata.visible_content_bytes = Some(content.len());
    Ok(ReportWriterOutput { content, metadata })
}
