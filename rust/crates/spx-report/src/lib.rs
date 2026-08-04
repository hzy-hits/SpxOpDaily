//! DeepSeek-backed report writing with fail-closed response handling.
//!
//! This crate owns the fenced scheduled-report runtime and the strict `DeepSeek` writer.
//! It reads only validated core projections, writes through the operational ledger's
//! report-owner contract, and has no strategy, delivery transport, or broker authority.

mod client;
mod config;
mod health;
mod service;
mod service_config;
mod source;
mod transport;

pub use client::{
    DEEPSEEK_MODEL_ID, DeskReportOutput, RESEARCH_UNAVAILABLE_DISCLOSURE, ReasoningEffort,
    ReportPrompt, ReportWriterClient, ReportWriterError, ReportWriterErrorCode, ReportWriterOutput,
    ResponseMetadata, ThinkingMode,
};
pub use config::{ApiKeyEnvironment, ConfigError, MaxTokens, ReportWriterConfig};
pub use health::{
    HealthError, REPORT_HEALTH_SCHEMA_VERSION, ReportCounters, ReportHealth, ReportPhase,
};
pub use service::{
    DeskMessageWriter, OwnedReportLedger, ReportPersistDisposition, ReportService,
    ReportServiceError, ReportTick, ScheduledReportStore,
};
pub use service_config::{ReportServiceConfig, ReportTargetConfig, ServiceConfigError};
pub use source::{
    ProjectionEligibility, ProjectionSourceError, ProjectionSourceErrorCode, ReportSlot,
    active_report_slot, projection_eligibility, read_latest_projection,
};
pub use transport::{
    DEEPSEEK_CHAT_COMPLETIONS_URL, DeepSeekHttpTransport, Transport, TransportError,
    TransportRequest, TransportResponse,
};
