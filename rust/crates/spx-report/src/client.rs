use std::fmt::{Debug, Display, Formatter};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use spx_domain::{
    DeskDirection, DeskMapProjectionV1, DeskMessageV2, DeskStage, MarketSession, Token, Validate,
};

use crate::{
    DeepSeekHttpTransport, ReportWriterConfig, Transport, TransportRequest, TransportResponse,
};

pub const DEEPSEEK_MODEL_ID: &str = "deepseek-v4-flash";
pub const RESEARCH_UNAVAILABLE_DISCLOSURE: &str =
    "Research context: unavailable; no HMM, range, close-location, or market-maker inference.";
pub const RESEARCH_ADVISORY_DISCLOSURE: &str =
    "研究：HMM/区间为未校准 advisory，仅辅助 Base Case，不产生交易方向或授权 READY。";

const DESK_MESSAGE_SYSTEM_PROMPT: &str = r"You are the SPX desk report writer.
Return exactly one JSON object and no surrounding prose or Markdown fence.
The object must contain exactly these string fields:
title, desk_view, location, structure, primary_path, alternative_path, targets, execution, data_quality.
Every field must be non-empty. Do not add, remove, rename, or nest fields.
Write every operator-facing field value in Simplified Chinese. Do not paraphrase a Chinese source projection into English prose. Keep only English tokens that already appear in the source or this contract: NO TRADE, READY, HOLD, PAUSED, WAIT, CLOSED, LONG / CALL, SHORT / PUT, ES, SPX, SPXW, VWAP, OR, Flip, Gamma, NBBO, GTH, RTH, and strike/ticker labels.
Use the complete desk_map_projection.v1 JSON and explicit research-context status supplied by the user as the sole factual authority.
Write an operator-facing compact report, not a transcript of the source object. Synthesize repeated facts and omit internal detail that does not change the human decision.
Use the fixed fields as this presentation contract: desk_view is Base Case and the current human decision; location plus structure explain Why; primary_path is the next Trigger; alternative_path is Invalidation or the genuinely distinct alternative; targets contains only active structural or trade targets; execution states exactly what the operator may do; data_quality states the Primary Data Impact.
Preserve decision-critical conditions, lifecycle state, current location, active level, trigger, invalidation, target, exact-leg ask cap, TTL, and R/R when they exist. Do not repeat every timestamp, diagnostic count, unavailable component, or numeric value merely because it appears in the source.
Embedded research_context.v2 is bootstrap-unvalidated advisory evidence with no action authority. When a usable advisory forecast is present, integrate one decision-relevant horizon into Base Case and label it 未校准研究观点. A source-supplied forecast probability may be shown only as 未校准研究概率; never invent, calibrate, round into false certainty, or present a latent state as market-maker behavior. Research may inform Base Case but must never create trade direction, READY, a trigger, or an order.
When research is present, keep at most one short research-background line. Label it 未校准 and 不产生方向. HMM state weights, P/Q diagnostics, Gamma and model internals belong to research evidence, not to the human action or execution fields. Never rename P−Q as edge because execution costs and net-PnL labels are not yet available.
research_context_status=embedded_contract_valid means only that the wire contract passed; nested availability remains authoritative. Summarize the one most useful available research result instead of dumping every posterior, quantile, state ID, model version, or reason code.
GTH and RTH desk maps are different products. When session is gth, do not mention cash SPX/NDX/DJI/RUT, RTH close or high/low forecasts, close-location, HMM/bootstrap research, or ES/SPY cash confirmation. GTH facts are the chain-implied or ES coordinate, live option walls, ES 15m/60m flow, and whether price has accepted or rejected a level. Expected overnight N/A is not a data outage.
When research_context_status is gth_not_applicable, omit research entirely; data_quality must not say research is unavailable.
When session is gth, the 15-minute card is a live structure scan: show the 5-20Δ short / 10-wide iron condor map and the width-scan result. It is not an empty health heartbeat and not a trade ticket. desk_view and execution must not say 可看 or READY, and must not name an unpassed Call/Put debit spread as the action. A Flip zone is structure, not a debit vertical. Ranked winners are delivered on a separate trade_ready card. Preserve NO TRADE when the source decision is NO_TRADE.
When research_context_status is unavailable, data_quality must explicitly say research is unavailable and must make no HMM, range, or close-location claim.
Direction may come only from an explicit price trigger confirmed by ES flow in the source projection. Gamma describes only the feedback mechanism that may suppress or amplify an already observed move; Gamma must never be presented as the source of an up or down direction.
Dealer sign is unknown. Do not claim that market makers are buying, selling, forced to hedge, or causing a directional move.
Preserve 方向来源 in primary_path and NO TRADE in desk_view when the source contains them. Do not require Gamma, dealer-sign, HMM or P/Q prose in the visible report.
When typed direction is none, title, desk_view, and execution must not say LONG, SHORT, 做多, or 做空, and execution must not say READY. An unvalidated research bias may still be stated as advisory context when it is clearly separated from trade direction. When an up source contains LONG / CALL, or a down source contains SHORT / PUT, preserve that exact label in desk_view.
Preserve READY, HOLD, PAUSED, WAIT, and CLOSED from source execution in output execution. Shorten source fields aggressively when doing so does not remove an active trigger, invalidation, target, exact-leg limit, TTL or R/R.
Lead with the human decision and its reason. Translate lifecycle and quality into plain language; do not expose schema names, raw field names, hashes, internal identifiers, action_authority, automatic_ordering, or raw enum dumps unless they change what the operator may safely do.
In data_quality, state the single most important human impact first. Never expose raw audit codes or reason-code lists in any visible field; they remain in the source artifact for audit.
Keep one useful sentence in each section and enough concrete evidence to support the Base Case. Do not invent orders, fills, positions, probabilities, or market-maker behavior.
A Flip zone is structure, not a debit vertical. If 最近候选 is 无, do not mention a retained Put/Call spread, 待评估 candidate, or parked opportunity ID.";

const EXECUTION_STATE_MARKERS: [&str; 5] = ["READY", "HOLD", "PAUSED", "WAIT", "CLOSED"];

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
    SemanticMarkerFieldMismatch,
    DirectionAuthorityViolation,
    DirectionLabelMissing,
    ExecutionStateMarkerMissing,
    CriticalFactMissing,
    FieldCompressionDetected,
    InternalDetailLeak,
    ResearchAdvisoryMissing,
    ResearchDisclosureFailed,
    OperatorLanguageViolation,
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
            Self::SemanticMarkerFieldMismatch => "semantic_marker_field_mismatch",
            Self::DirectionAuthorityViolation => "direction_authority_violation",
            Self::DirectionLabelMissing => "direction_label_missing",
            Self::ExecutionStateMarkerMissing => "execution_state_marker_missing",
            Self::CriticalFactMissing => "critical_fact_missing",
            Self::FieldCompressionDetected => "field_compression_detected",
            Self::InternalDetailLeak => "internal_detail_leak",
            Self::ResearchAdvisoryMissing => "research_advisory_missing",
            Self::ResearchDisclosureFailed => "research_disclosure_failed",
            Self::OperatorLanguageViolation => "operator_language_violation",
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
        let research_input = match (&projection.research_context, projection.session) {
            (Some(_), _) => "research_context_status=embedded_contract_valid\nThe complete research_context.v2 appears once inside desk_map_projection.v1; do not duplicate it in the report."
                .to_owned(),
            (None, MarketSession::Gth) => {
                "research_context_status=gth_not_applicable\nGTH desk maps are a live 5-20Δ short / 10-wide iron-condor and width-scan card, not a health heartbeat. Do not mention HMM, cash index, close-location, session high/low, bootstrap research, that research is unavailable, 可看, or an unpassed Call/Put debit spread as the action."
                    .to_owned()
            }
            (None, _) => format!(
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
        if operator_language_is_english_paraphrase(&message, &projection.message) {
            return Err(ReportWriterError::with_metadata(
                ReportWriterErrorCode::OperatorLanguageViolation,
                output.metadata.clone(),
            ));
        }
        apply_research_disclosure(&mut message, projection, &output.metadata)?;
        validate_rendered_message(&message, projection, &output.metadata)?;
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

fn apply_research_disclosure(
    message: &mut DeskMessageV2,
    projection: &DeskMapProjectionV1,
    metadata: &ResponseMetadata,
) -> Result<(), ReportWriterError> {
    let disclosure = if projection.session == MarketSession::Gth
        && projection.research_context.is_none()
    {
        None
    } else if projection.research_context.is_none()
        && !message
            .data_quality
            .as_str()
            .contains(RESEARCH_UNAVAILABLE_DISCLOSURE)
    {
        Some(RESEARCH_UNAVAILABLE_DISCLOSURE)
    } else if projection.research_context.is_some() && !research_advisory_is_disclosed(message) {
        Some(RESEARCH_ADVISORY_DISCLOSURE)
    } else {
        None
    };
    if let Some(disclosure) = disclosure {
        message.data_quality = Token::new(
            format!("{}\n{disclosure}", message.data_quality),
            "desk report data_quality",
        )
        .map_err(|_| {
            ReportWriterError::with_metadata(
                ReportWriterErrorCode::ResearchDisclosureFailed,
                metadata.clone(),
            )
        })?;
    }
    Ok(())
}

fn validate_rendered_message(
    message: &DeskMessageV2,
    projection: &DeskMapProjectionV1,
    metadata: &ResponseMetadata,
) -> Result<(), ReportWriterError> {
    let code = if semantic_marker_field_mismatch(message, &projection.message) {
        Some(ReportWriterErrorCode::SemanticMarkerFieldMismatch)
    } else if projection.direction == DeskDirection::None
        && none_direction_has_actionable_language(message)
    {
        Some(ReportWriterErrorCode::DirectionAuthorityViolation)
    } else if direction_label_is_missing(message, projection) {
        Some(ReportWriterErrorCode::DirectionLabelMissing)
    } else if execution_state_marker_is_missing(message, &projection.message) {
        Some(ReportWriterErrorCode::ExecutionStateMarkerMissing)
    } else if critical_numeric_fact_is_missing(message, projection) {
        Some(ReportWriterErrorCode::CriticalFactMissing)
    } else if visible_internal_detail_leaked(message, projection) {
        Some(ReportWriterErrorCode::InternalDetailLeak)
    } else {
        None
    };
    match code {
        Some(code) => Err(ReportWriterError::with_metadata(code, metadata.clone())),
        None => Ok(()),
    }
}

fn semantic_marker_field_mismatch(actual: &DeskMessageV2, source: &DeskMessageV2) -> bool {
    (message_contains_marker(source, "方向来源")
        && !actual.primary_path.as_str().contains("方向来源"))
        || (message_contains_marker(source, "NO TRADE")
            && !actual.desk_view.as_str().contains("NO TRADE"))
}

fn message_contains_marker(message: &DeskMessageV2, marker: &str) -> bool {
    [
        &message.title,
        &message.desk_view,
        &message.location,
        &message.structure,
        &message.primary_path,
        &message.alternative_path,
        &message.targets,
        &message.execution,
        &message.data_quality,
    ]
    .into_iter()
    .any(|field| field.as_str().contains(marker))
}

fn research_advisory_is_disclosed(message: &DeskMessageV2) -> bool {
    let text = message_text(message);
    let explicitly_uncalibrated = text.contains("HMM未校准")
        || text.contains("未校准研究")
        || text.contains("未校准 advisory");
    let explicitly_non_actionable = text.contains("不是上涨概率")
        || text.contains("不产生交易方向")
        || text.contains("不改变价格方向")
        || text.contains("不授权 READY")
        || text.contains("不改变价格触发或READY");
    explicitly_uncalibrated && explicitly_non_actionable
}

fn operator_language_is_english_paraphrase(actual: &DeskMessageV2, source: &DeskMessageV2) -> bool {
    const MIN_CJK_CHARS: usize = 8;
    cjk_char_count(&message_text(source)) >= MIN_CJK_CHARS
        && cjk_char_count(&message_text(actual)) < MIN_CJK_CHARS
}

fn cjk_char_count(text: &str) -> usize {
    text.chars()
        .filter(|&character| {
            ('\u{4E00}'..='\u{9FFF}').contains(&character)
                || ('\u{3400}'..='\u{4DBF}').contains(&character)
        })
        .count()
}

fn none_direction_has_actionable_language(message: &DeskMessageV2) -> bool {
    let directional_language = [&message.title, &message.desk_view, &message.execution]
        .into_iter()
        .any(|field| {
            let text = field.as_str();
            contains_ascii_word_ignore_case(text, "LONG")
                || contains_ascii_word_ignore_case(text, "SHORT")
                || text.contains("做多")
                || text.contains("做空")
        });
    directional_language || contains_ascii_word(message.execution.as_str(), "READY")
}

fn direction_label_is_missing(actual: &DeskMessageV2, projection: &DeskMapProjectionV1) -> bool {
    let required_label = match projection.direction {
        DeskDirection::Up if message_contains_marker(&projection.message, "LONG / CALL") => {
            Some("LONG / CALL")
        }
        DeskDirection::Down if message_contains_marker(&projection.message, "SHORT / PUT") => {
            Some("SHORT / PUT")
        }
        DeskDirection::Up | DeskDirection::Down | DeskDirection::None => None,
    };
    required_label.is_some_and(|label| !actual.desk_view.as_str().contains(label))
}

fn execution_state_marker_is_missing(actual: &DeskMessageV2, source: &DeskMessageV2) -> bool {
    EXECUTION_STATE_MARKERS.into_iter().any(|marker| {
        contains_ascii_word(source.execution.as_str(), marker)
            && !contains_ascii_word(actual.execution.as_str(), marker)
    })
}

fn critical_numeric_fact_is_missing(
    actual: &DeskMessageV2,
    projection: &DeskMapProjectionV1,
) -> bool {
    if projection.level.is_some_and(|level| {
        let level_context = format!(
            "{}\n{}\n{}",
            actual.location, actual.structure, actual.primary_path
        );
        !numeric_atoms(&level_context).contains(&normalized_number(level.get()))
    }) {
        return true;
    }
    if !matches!(projection.stage, DeskStage::Ready | DeskStage::Active) {
        return false;
    }
    [
        (
            projection.message.primary_path.as_str(),
            actual.primary_path.as_str(),
        ),
        (
            projection.message.alternative_path.as_str(),
            actual.alternative_path.as_str(),
        ),
        (projection.message.targets.as_str(), actual.targets.as_str()),
        (
            projection.message.execution.as_str(),
            actual.execution.as_str(),
        ),
    ]
    .into_iter()
    .any(|(source, rendered)| {
        let rendered_numbers = numeric_atoms(rendered);
        numeric_atoms(source)
            .into_iter()
            .any(|required| !rendered_numbers.contains(&required))
    })
}

fn visible_internal_detail_leaked(
    message: &DeskMessageV2,
    projection: &DeskMapProjectionV1,
) -> bool {
    let text = message_text(message);
    let normalized_text = text.to_ascii_lowercase();
    if [
        "schema_version",
        "projection_id",
        "source_snapshot_id",
        "source_slot",
        "structure_fingerprint",
        "research_context_document_id",
        "document_id",
        "frame_id",
        "lineage_id",
        "observed_through",
        "available_at",
        "valid_until",
        "quality_reasons",
        "regime_reason_codes",
        "reason_codes",
        "state_id",
        "model_version",
        "parameter_mode",
        "evidence_status",
        "use_scope",
        "posterior",
        "action_authority",
        "automatic_ordering",
        "desk_map_projection.v1",
        "research_context.v2",
    ]
    .into_iter()
    .any(|forbidden| normalized_text.contains(forbidden))
    {
        return true;
    }
    if [
        projection.projection_id.as_str(),
        projection.source_snapshot_id.as_str(),
    ]
    .into_iter()
    .any(|identifier| normalized_text.contains(&identifier.to_ascii_lowercase()))
    {
        return true;
    }
    if projection
        .research_context_document_id
        .as_ref()
        .is_some_and(|identifier| {
            normalized_text.contains(&identifier.as_str().to_ascii_lowercase())
        })
    {
        return true;
    }
    if projection.quality_reasons.iter().any(|reason| {
        let raw = reason.as_str();
        (raw.contains('_') || raw.contains(':'))
            && normalized_text.contains(&raw.to_ascii_lowercase())
    }) {
        return true;
    }
    text.split(|character: char| !character.is_ascii_hexdigit())
        .any(|word| word.len() == 64 && word.bytes().all(|byte| byte.is_ascii_hexdigit()))
}

fn message_text(message: &DeskMessageV2) -> String {
    [
        &message.title,
        &message.desk_view,
        &message.location,
        &message.structure,
        &message.primary_path,
        &message.alternative_path,
        &message.targets,
        &message.execution,
        &message.data_quality,
    ]
    .into_iter()
    .map(Token::as_str)
    .collect::<Vec<_>>()
    .join("\n")
}

fn numeric_atoms(text: &str) -> Vec<u64> {
    let bytes = text.as_bytes();
    let mut values = Vec::new();
    let mut cursor = 0;
    while cursor < bytes.len() {
        let signed = matches!(bytes[cursor], b'+' | b'-')
            && bytes.get(cursor + 1).is_some_and(u8::is_ascii_digit);
        if !bytes[cursor].is_ascii_digit() && !signed {
            cursor += 1;
            continue;
        }
        let start = cursor;
        cursor += usize::from(signed);
        while cursor < bytes.len() && bytes[cursor].is_ascii_digit() {
            cursor += 1;
        }
        if bytes.get(cursor) == Some(&b'.') && bytes.get(cursor + 1).is_some_and(u8::is_ascii_digit)
        {
            cursor += 1;
            while cursor < bytes.len() && bytes[cursor].is_ascii_digit() {
                cursor += 1;
            }
        }
        if let Ok(value) = text[start..cursor].parse::<f64>() {
            values.push(normalized_number(value));
        }
    }
    values
}

fn normalized_number(value: f64) -> u64 {
    let normalized = if value == 0.0 { 0.0_f64 } else { value };
    normalized.to_bits()
}

fn contains_ascii_word(text: &str, expected: &str) -> bool {
    text.split(|character: char| !character.is_ascii_alphanumeric())
        .any(|word| word == expected)
}

fn contains_ascii_word_ignore_case(text: &str, expected: &str) -> bool {
    text.split(|character: char| !character.is_ascii_alphanumeric())
        .any(|word| word.eq_ignore_ascii_case(expected))
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
