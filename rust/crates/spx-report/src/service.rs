use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use spx_domain::{
    DeskDataQuality, DeskLevelPhase, DeskMapProjectionV1, DeskMessageV2, DeskStage,
    NOTIFICATION_INTENT_V2_SCHEMA_VERSION, NotificationIntentV2, NotificationLineageV2, Token,
    Validate,
};
use spx_ledger::{Ledger, LedgerError, OwnerLease, OwnerRole, PersistWrite};
use thiserror::Error;

use crate::{
    DEEPSEEK_MODEL_ID, DeskReportOutput, HealthError, ProjectionEligibility,
    ProjectionSourceErrorCode, ReportHealth, ReportPhase, ReportServiceConfig, ReportWriterClient,
    ReportWriterError, ReportWriterErrorCode, ResponseMetadata, ServiceConfigError, Transport,
    active_report_slot, read_latest_projection,
};

/// One strict-writer failure with optional non-sensitive provider audit metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeskMessageWriteFailure {
    code: ReportWriterErrorCode,
    metadata: Option<ResponseMetadata>,
}

impl DeskMessageWriteFailure {
    pub const fn new(code: ReportWriterErrorCode) -> Self {
        Self {
            code,
            metadata: None,
        }
    }

    pub const fn with_metadata(code: ReportWriterErrorCode, metadata: ResponseMetadata) -> Self {
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

    fn projection_fallback_metadata(&self) -> Option<&ResponseMetadata> {
        let validation_failed = matches!(
            self.code,
            ReportWriterErrorCode::DeskMessageInvalidJson
                | ReportWriterErrorCode::DeskMessageInvalidContract
                | ReportWriterErrorCode::SemanticMarkerFieldMismatch
                | ReportWriterErrorCode::DirectionAuthorityViolation
                | ReportWriterErrorCode::DirectionLabelMissing
                | ReportWriterErrorCode::ExecutionStateMarkerMissing
                | ReportWriterErrorCode::CriticalFactMissing
                | ReportWriterErrorCode::FieldCompressionDetected
                | ReportWriterErrorCode::InternalDetailLeak
                | ReportWriterErrorCode::ResearchAdvisoryMissing
                | ReportWriterErrorCode::ResearchDisclosureFailed
                | ReportWriterErrorCode::OperatorLanguageViolation
        );
        self.metadata.as_ref().filter(|metadata| {
            validation_failed
                && (200..300).contains(&metadata.http_status)
                && metadata.response_model.as_deref() == Some(DEEPSEEK_MODEL_ID)
                && metadata.finish_reason.as_deref() == Some("stop")
                && metadata.visible_content_bytes.is_some()
        })
    }
}

impl From<ReportWriterError> for DeskMessageWriteFailure {
    fn from(error: ReportWriterError) -> Self {
        Self {
            code: error.code(),
            metadata: error.metadata().cloned(),
        }
    }
}

pub trait DeskMessageWriter: Send + Sync {
    /// Produces one fully validated, operator-facing canonical desk message.
    ///
    /// # Errors
    ///
    /// Returns a typed writer code when the provider or output contract fails.
    fn write_message(
        &self,
        projection: &DeskMapProjectionV1,
    ) -> Result<DeskReportOutput, DeskMessageWriteFailure>;
}

impl<T: Transport> DeskMessageWriter for ReportWriterClient<T> {
    fn write_message(
        &self,
        projection: &DeskMapProjectionV1,
    ) -> Result<DeskReportOutput, DeskMessageWriteFailure> {
        self.write_desk_map(projection).map_err(Into::into)
    }
}

pub trait ScheduledReportStore {
    /// Refreshes the exclusive report-writer ownership fence.
    ///
    /// # Errors
    ///
    /// Returns a ledger error when ownership was lost or storage failed.
    fn refresh(&mut self, now: DateTime<Utc>) -> Result<(), LedgerError>;

    /// Checks the stable ET slot without mutating report or outbox state.
    ///
    /// # Errors
    ///
    /// Returns a ledger error when ownership was lost or storage failed.
    fn exists(&self, slot: &Token, now: DateTime<Utc>) -> Result<bool, LedgerError>;

    /// Atomically persists one complete scheduled report and its typed targets.
    ///
    /// # Errors
    ///
    /// Returns a ledger error for ownership, identity, contract, or storage failure.
    fn persist(
        &self,
        intent: &NotificationIntentV2,
        now: DateTime<Utc>,
    ) -> Result<PersistWrite, LedgerError>;

    /// Releases the report-writer ownership fence.
    ///
    /// # Errors
    ///
    /// Returns a ledger error if the current generation no longer owns the role.
    fn shutdown(&mut self) -> Result<(), LedgerError>;
}

pub struct OwnedReportLedger {
    ledger: Ledger,
    owner: OwnerLease,
    owner_duration: TimeDelta,
    released: bool,
}

impl OwnedReportLedger {
    /// Opens the operational ledger and acquires `OwnerRole::Report`.
    ///
    /// # Errors
    ///
    /// Returns a ledger error when migrations, storage, or exclusive ownership fail.
    pub fn open(
        ledger_path: &std::path::Path,
        owner_id: &str,
        now: DateTime<Utc>,
        owner_lease_seconds: i64,
    ) -> Result<Self, LedgerError> {
        let ledger = Ledger::open(ledger_path)?;
        let owner_duration = TimeDelta::seconds(owner_lease_seconds);
        let owner = ledger.acquire_owner(OwnerRole::Report, owner_id, now, owner_duration)?;
        Ok(Self {
            ledger,
            owner,
            owner_duration,
            released: false,
        })
    }
}

impl ScheduledReportStore for OwnedReportLedger {
    fn refresh(&mut self, now: DateTime<Utc>) -> Result<(), LedgerError> {
        self.ledger
            .refresh_owner(&mut self.owner, now, self.owner_duration)
    }

    fn exists(&self, slot: &Token, now: DateTime<Utc>) -> Result<bool, LedgerError> {
        self.ledger.scheduled_report_exists(&self.owner, slot, now)
    }

    fn persist(
        &self,
        intent: &NotificationIntentV2,
        now: DateTime<Utc>,
    ) -> Result<PersistWrite, LedgerError> {
        self.ledger
            .persist_scheduled_report(&self.owner, intent, now)
    }

    fn shutdown(&mut self) -> Result<(), LedgerError> {
        if !self.released {
            self.ledger.release_owner(&self.owner)?;
            self.released = true;
        }
        Ok(())
    }
}

impl Drop for OwnedReportLedger {
    fn drop(&mut self) {
        if !self.released {
            let _ = self.ledger.release_owner(&self.owner);
            self.released = true;
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NetworkGate {
    config_enabled: bool,
    caller_allowed: bool,
}

impl NetworkGate {
    const fn authorized(self) -> bool {
        self.config_enabled && self.caller_allowed
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GenerationBackoff {
    projection_id: String,
    failures: usize,
    next_attempt_at: DateTime<Utc>,
    error_code: ReportWriterErrorCode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReportPersistDisposition {
    Inserted,
    Duplicate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum ReportTick {
    AwaitingSlot,
    SourceUnavailable {
        code: ProjectionSourceErrorCode,
    },
    Ineligible {
        reason: ProjectionEligibility,
    },
    Backoff {
        error_code: ReportWriterErrorCode,
        next_attempt_at: DateTime<Utc>,
    },
    Duplicate {
        slot: String,
    },
    ExpiredAfterGeneration {
        slot: String,
    },
    Persisted {
        slot: String,
        disposition: ReportPersistDisposition,
    },
}

#[derive(Debug, Error)]
pub enum ReportServiceError {
    #[error("report network is not authorized by both config and caller")]
    NetworkNotAuthorized,
    #[error("report service configuration failed: {0}")]
    Config(#[from] ServiceConfigError),
    #[error("report ledger failed: {0}")]
    Ledger(#[from] LedgerError),
    #[error("report domain contract failed: {0}")]
    Domain(#[from] spx_domain::DomainError),
    #[error("report health projection failed: {0}")]
    Health(#[from] HealthError),
    #[error("report time arithmetic failed")]
    InvalidTime,
}

pub struct ReportService<W: DeskMessageWriter, L: ScheduledReportStore> {
    config: ReportServiceConfig,
    writer: W,
    store: L,
    gate: NetworkGate,
    health: ReportHealth,
    backoff: Option<GenerationBackoff>,
    stopped: bool,
}

impl<W: DeskMessageWriter, L: ScheduledReportStore> ReportService<W, L> {
    /// Creates a report service with injected writer and ledger boundaries.
    ///
    /// # Errors
    ///
    /// Returns an error when configuration or either network authorization gate fails,
    /// or when initial health cannot be published.
    pub fn open(
        config: ReportServiceConfig,
        allow_network: bool,
        writer: W,
        store: L,
        now: DateTime<Utc>,
    ) -> Result<Self, ReportServiceError> {
        config.validate()?;
        let gate = NetworkGate {
            config_enabled: config.writer.network_enabled(),
            caller_allowed: allow_network,
        };
        if !gate.authorized() {
            return Err(ReportServiceError::NetworkNotAuthorized);
        }
        let health = ReportHealth::start_from_persisted(
            &config.health_path,
            &config.projection_path,
            gate.authorized(),
            now,
        )?;
        health.persist(&config.health_path)?;
        Ok(Self {
            config,
            writer,
            store,
            gate,
            health,
            backoff: None,
            stopped: false,
        })
    }

    /// Runs one production poll using fresh wall-clock time after model generation.
    ///
    /// # Errors
    ///
    /// Returns an error for lost ownership, invalid internal contracts, or health storage
    /// failure. Provider failures become bounded in-memory backoff outcomes.
    pub fn run_once(&mut self) -> Result<ReportTick, ReportServiceError> {
        self.run_once_with_completion_clock(Utc::now(), Utc::now)
    }

    /// Runs one deterministic poll for tests and bounded manual inspection.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::run_once`].
    pub fn run_once_at(&mut self, now: DateTime<Utc>) -> Result<ReportTick, ReportServiceError> {
        self.run_once_with_completion_clock(now, || now)
    }

    /// Polls until a stop signal is observed.
    ///
    /// # Errors
    ///
    /// Returns on an ownership, contract, or durable health failure. Model failures stay
    /// inside the bounded retry loop.
    pub fn run_until(&mut self, stop: &AtomicBool) -> Result<(), ReportServiceError> {
        self.require_network()?;
        while !stop.load(Ordering::Relaxed) {
            self.run_once()?;
            sleep_interruptibly(self.config.poll_interval_millis, stop);
        }
        Ok(())
    }

    pub const fn health(&self) -> &ReportHealth {
        &self.health
    }

    /// Publishes stopped health and releases the report ownership fence.
    ///
    /// # Errors
    ///
    /// Returns an error when health persistence or lease release fails.
    pub fn shutdown(&mut self, now: DateTime<Utc>) -> Result<(), ReportServiceError> {
        if self.stopped {
            return Ok(());
        }
        self.health.phase = ReportPhase::Stopped;
        self.health.updated_at = now;
        self.health.persist(&self.config.health_path)?;
        self.store.shutdown()?;
        self.stopped = true;
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    fn run_once_with_completion_clock<F>(
        &mut self,
        now: DateTime<Utc>,
        completion_clock: F,
    ) -> Result<ReportTick, ReportServiceError>
    where
        F: FnOnce() -> DateTime<Utc>,
    {
        self.require_network()?;
        self.health.updated_at = now;
        self.health.counters.ticks = self.health.counters.ticks.saturating_add(1);
        self.health.last_error_code = None;
        let Some(slot) = active_report_slot(now, self.config.slot_grace_seconds) else {
            self.health.phase = ReportPhase::AwaitingSlot;
            self.health.active_slot = None;
            self.health.next_attempt_at = None;
            self.persist_health()?;
            return Ok(ReportTick::AwaitingSlot);
        };
        self.health.active_slot = Some(slot.ledger_slot().to_owned());

        let latest = match read_latest_projection(
            &self.config.projection_path,
            self.config.source_max_bytes,
        ) {
            Ok(latest) => latest,
            Err(error) => {
                let code = error.code();
                self.health.phase = if code == ProjectionSourceErrorCode::Missing {
                    ReportPhase::AwaitingProjection
                } else {
                    ReportPhase::Degraded
                };
                self.health.counters.source_failures =
                    self.health.counters.source_failures.saturating_add(1);
                self.health.last_error_code = Some(format!("source_{}", code.as_str()));
                self.persist_health()?;
                return Ok(ReportTick::SourceUnavailable { code });
            }
        };
        let projection = &latest.projection;
        let projection_id = projection.projection_id.as_str().to_owned();
        self.health.last_projection_id = Some(projection_id.clone());
        let eligibility = crate::projection_eligibility(&latest, &slot, now);
        if eligibility != ProjectionEligibility::Eligible {
            if matches!(
                eligibility,
                ProjectionEligibility::AwaitingCurrentProjection
                    | ProjectionEligibility::NotYetAvailable
            ) {
                self.health.phase = ReportPhase::AwaitingProjection;
                self.health.counters.awaiting_current_projection = self
                    .health
                    .counters
                    .awaiting_current_projection
                    .saturating_add(1);
            } else {
                self.health.phase = ReportPhase::Degraded;
                self.health.counters.ineligible_projections = self
                    .health
                    .counters
                    .ineligible_projections
                    .saturating_add(1);
            }
            self.health.last_error_code = Some(format!("projection_{}", eligibility.as_str()));
            self.persist_health()?;
            return Ok(ReportTick::Ineligible {
                reason: eligibility,
            });
        }

        let slot_token = Token::new(slot.ledger_slot().to_owned(), "scheduled report slot")?;
        self.store.refresh(now)?;
        if self.store.exists(&slot_token, now)? {
            self.backoff = None;
            self.health.phase = ReportPhase::Duplicate;
            self.health.next_attempt_at = None;
            self.health.consecutive_generation_failures = 0;
            self.health.counters.duplicate_slots =
                self.health.counters.duplicate_slots.saturating_add(1);
            self.persist_health()?;
            return Ok(ReportTick::Duplicate {
                slot: slot.ledger_slot().to_owned(),
            });
        }

        if let Some(backoff) = &self.backoff
            && backoff.projection_id == projection_id
            && now < backoff.next_attempt_at
        {
            self.health.phase = ReportPhase::Backoff;
            self.health.next_attempt_at = Some(backoff.next_attempt_at);
            self.health.last_error_code = Some(backoff.error_code.as_str().to_owned());
            self.persist_health()?;
            return Ok(ReportTick::Backoff {
                error_code: backoff.error_code,
                next_attempt_at: backoff.next_attempt_at,
            });
        }
        if self
            .backoff
            .as_ref()
            .is_some_and(|backoff| backoff.projection_id != projection_id)
        {
            self.backoff = None;
            self.health.consecutive_generation_failures = 0;
        }

        self.health.phase = ReportPhase::Generating;
        self.health.next_attempt_at = None;
        self.health.counters.generation_attempts =
            self.health.counters.generation_attempts.saturating_add(1);
        self.persist_health()?;
        let result = self.writer.write_message(projection);
        let completed_at = completion_clock();
        self.health.updated_at = completed_at;
        let (message, response_metadata, fallback_reason) = match result {
            Ok(output) => (output.message, output.metadata, None),
            Err(failure) => {
                if let Some(metadata) = failure.projection_fallback_metadata().cloned() {
                    projection.message.validate()?;
                    (projection.message.clone(), metadata, Some(failure.code()))
                } else {
                    if let Some(metadata) = failure.metadata() {
                        self.record_response_metadata(metadata);
                    }
                    let error_code = failure.code();
                    self.health.last_fallback_reason = None;
                    let next_attempt_at = self.record_generation_failure(
                        &projection_id,
                        error_code,
                        completed_at,
                        slot.closes_at(),
                    )?;
                    self.persist_health()?;
                    return Ok(ReportTick::Backoff {
                        error_code,
                        next_attempt_at,
                    });
                }
            }
        };
        let message = scheduled_operator_projection(projection, message)?;
        self.record_response_metadata(&response_metadata);
        self.health.last_fallback_reason = fallback_reason.map(|reason| reason.as_str().to_owned());
        if fallback_reason.is_some() {
            self.health.counters.projection_message_fallbacks = self
                .health
                .counters
                .projection_message_fallbacks
                .saturating_add(1);
        }

        if projection_expired_after_generation(completed_at, projection.valid_until) {
            self.backoff = None;
            self.health.phase = ReportPhase::Degraded;
            self.health.last_error_code = Some("projection_expired_after_generation".to_owned());
            self.persist_health()?;
            return Ok(ReportTick::ExpiredAfterGeneration {
                slot: slot.ledger_slot().to_owned(),
            });
        }

        self.store.refresh(completed_at)?;
        if self.store.exists(&slot_token, completed_at)? {
            self.backoff = None;
            self.health.phase = ReportPhase::Duplicate;
            self.health.counters.duplicate_slots =
                self.health.counters.duplicate_slots.saturating_add(1);
            self.persist_health()?;
            return Ok(ReportTick::Duplicate {
                slot: slot.ledger_slot().to_owned(),
            });
        }

        let intent = build_intent(
            projection,
            &slot_token,
            message,
            self.config.domain_targets()?,
            self.config.max_attempts,
            completed_at,
        )?;
        let disposition = match self.store.persist(&intent, completed_at)? {
            PersistWrite::Inserted => ReportPersistDisposition::Inserted,
            PersistWrite::Duplicate => ReportPersistDisposition::Duplicate,
        };
        self.backoff = None;
        self.health.phase = ReportPhase::Persisted;
        self.health.last_persisted_at = Some(completed_at);
        self.health.next_attempt_at = None;
        self.health.consecutive_generation_failures = 0;
        self.health.last_error_code = None;
        self.health.counters.persisted_reports =
            self.health.counters.persisted_reports.saturating_add(1);
        self.persist_health()?;
        Ok(ReportTick::Persisted {
            slot: slot.ledger_slot().to_owned(),
            disposition,
        })
    }

    fn record_generation_failure(
        &mut self,
        projection_id: &str,
        error_code: ReportWriterErrorCode,
        failed_at: DateTime<Utc>,
        slot_closes_at: DateTime<Utc>,
    ) -> Result<DateTime<Utc>, ReportServiceError> {
        let prior_failures = self
            .backoff
            .as_ref()
            .filter(|backoff| backoff.projection_id == projection_id)
            .map_or(0, |backoff| backoff.failures);
        let delay_index = prior_failures.min(self.config.failure_backoff_seconds.len() - 1);
        let next = failed_at
            .checked_add_signed(TimeDelta::seconds(
                self.config.failure_backoff_seconds[delay_index],
            ))
            .ok_or(ReportServiceError::InvalidTime)?;
        let next_attempt_at = next.min(slot_closes_at);
        let failures = prior_failures.saturating_add(1);
        self.backoff = Some(GenerationBackoff {
            projection_id: projection_id.to_owned(),
            failures,
            next_attempt_at,
            error_code,
        });
        self.health.phase = ReportPhase::Backoff;
        self.health.next_attempt_at = Some(next_attempt_at);
        self.health.consecutive_generation_failures = u32::try_from(failures).unwrap_or(u32::MAX);
        self.health.last_error_code = Some(error_code.as_str().to_owned());
        self.health.counters.generation_failures =
            self.health.counters.generation_failures.saturating_add(1);
        Ok(next_attempt_at)
    }

    fn require_network(&self) -> Result<(), ReportServiceError> {
        if self.gate.authorized() {
            Ok(())
        } else {
            Err(ReportServiceError::NetworkNotAuthorized)
        }
    }

    fn record_response_metadata(&mut self, metadata: &crate::ResponseMetadata) {
        self.health
            .last_response_model
            .clone_from(&metadata.response_model);
        self.health
            .last_finish_reason
            .clone_from(&metadata.finish_reason);
        self.health.last_visible_content_bytes = metadata.visible_content_bytes;
        self.health.last_response_sha256 = Some(metadata.raw_response_sha256.clone());
    }

    fn persist_health(&self) -> Result<(), ReportServiceError> {
        self.health.persist(&self.config.health_path)?;
        Ok(())
    }
}

fn build_intent(
    projection: &DeskMapProjectionV1,
    slot: &Token,
    message: spx_domain::DeskMessageV2,
    targets: Vec<spx_domain::NotificationTargetV1>,
    max_attempts: u32,
    created_at: DateTime<Utc>,
) -> Result<NotificationIntentV2, spx_domain::DomainError> {
    let identity = format!("{}|{}", projection.projection_id, slot);
    let digest = hex::encode(Sha256::digest(identity.as_bytes()));
    let intent = NotificationIntentV2 {
        schema_version: NOTIFICATION_INTENT_V2_SCHEMA_VERSION.to_owned(),
        intent_id: Token::new(format!("scheduled-report:{digest}"), "report intent_id")?,
        semantic_id: Token::new(format!("desk-map-slot:{digest}"), "report semantic_id")?,
        lineage: NotificationLineageV2::ScheduledReport {
            source_projection_id: projection.projection_id.clone(),
            slot: slot.clone(),
        },
        created_at,
        expires_at: projection.valid_until,
        message,
        targets,
        max_attempts,
    };
    intent.validate()?;
    Ok(intent)
}

fn scheduled_operator_projection(
    projection: &DeskMapProjectionV1,
    writer_message: DeskMessageV2,
) -> Result<DeskMessageV2, spx_domain::DomainError> {
    if !standing_status_required(projection) {
        return Ok(writer_message);
    }

    let source = &projection.message;
    let location = source
        .location
        .as_str()
        .lines()
        .next()
        .unwrap_or(source.location.as_str())
        .split('·')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .take(2)
        .collect::<Vec<_>>()
        .join(" · ");
    let data_quality = match projection.quality {
        DeskDataQuality::Ready => "READY · 行情仅用于等待下一结构",
        DeskDataQuality::Degraded => "DEGRADED · 数据降级，保持 NO TRADE",
        DeskDataQuality::Unavailable => "UNAVAILABLE · 行情不足，保持 NO TRADE",
    };
    let structure = source
        .structure
        .as_str()
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or("当前结构不可用");
    let standing = DeskMessageV2 {
        title: Token::new("SPX Desk Map · STANDBY", "standing title")?,
        desk_view: Token::new("NO TRADE · 当前无有效机会", "standing desk view")?,
        location: Token::new(
            if location.is_empty() {
                "当前坐标不可用".to_owned()
            } else {
                location
            },
            "standing location",
        )?,
        structure: Token::new(format!("参考结构 · {structure}"), "standing structure")?,
        primary_path: Token::new("方向来源：暂无；等待新的价格事件确认", "standing trigger")?,
        alternative_path: Token::new("新结构形成后重新计算", "standing invalidation")?,
        targets: Token::new("无有效交易目标", "standing targets")?,
        execution: Token::new("WAIT · 当前无人工操作", "standing execution")?,
        data_quality: Token::new(data_quality, "standing data quality")?,
    };
    standing.validate()?;
    Ok(standing)
}

fn standing_status_required(projection: &DeskMapProjectionV1) -> bool {
    matches!(
        projection.stage,
        DeskStage::Exit | DeskStage::Invalidated | DeskStage::Expired
    ) || matches!(
        projection.phase,
        DeskLevelPhase::Invalidated | DeskLevelPhase::Expired
    )
}

fn sleep_interruptibly(milliseconds: u64, stop: &AtomicBool) {
    let mut remaining = milliseconds;
    while remaining > 0 && !stop.load(Ordering::Relaxed) {
        let slice = remaining.min(100);
        thread::sleep(Duration::from_millis(slice));
        remaining -= slice;
    }
}

fn projection_expired_after_generation(
    completed_at: DateTime<Utc>,
    projection_valid_until: DateTime<Utc>,
) -> bool {
    completed_at >= projection_valid_until
}

#[cfg(test)]
mod tests {
    use chrono::{TimeDelta, TimeZone as _};

    use super::{
        DEEPSEEK_MODEL_ID, DeskMessageWriteFailure, ReportWriterErrorCode, ResponseMetadata,
        projection_expired_after_generation, scheduled_operator_projection,
    };
    use spx_domain::{DeskDirection, DeskLevelPhase, DeskMapProjectionV1, DeskStage, Token};

    #[test]
    fn slot_grace_limits_generation_start_not_valid_completion() {
        let slot_start = chrono::Utc.with_ymd_and_hms(2026, 8, 4, 14, 0, 0).unwrap();
        let completed_after_grace = slot_start + TimeDelta::seconds(181);
        let projection_valid_until = slot_start + TimeDelta::minutes(20);

        assert!(!projection_expired_after_generation(
            completed_after_grace,
            projection_valid_until
        ));
        assert!(projection_expired_after_generation(
            projection_valid_until,
            projection_valid_until
        ));
    }

    #[test]
    fn projection_fallback_requires_stop_and_a_message_validation_error() {
        let metadata = ResponseMetadata {
            http_status: 200,
            raw_response_bytes: 512,
            raw_response_sha256: "a".repeat(64),
            response_model: Some(DEEPSEEK_MODEL_ID.to_owned()),
            finish_reason: Some("stop".to_owned()),
            visible_content_bytes: Some(128),
        };
        let validation_codes = [
            ReportWriterErrorCode::DeskMessageInvalidJson,
            ReportWriterErrorCode::DeskMessageInvalidContract,
            ReportWriterErrorCode::SemanticMarkerFieldMismatch,
            ReportWriterErrorCode::DirectionAuthorityViolation,
            ReportWriterErrorCode::DirectionLabelMissing,
            ReportWriterErrorCode::ExecutionStateMarkerMissing,
            ReportWriterErrorCode::CriticalFactMissing,
            ReportWriterErrorCode::FieldCompressionDetected,
            ReportWriterErrorCode::InternalDetailLeak,
            ReportWriterErrorCode::ResearchAdvisoryMissing,
            ReportWriterErrorCode::ResearchDisclosureFailed,
            ReportWriterErrorCode::OperatorLanguageViolation,
        ];
        for code in validation_codes {
            assert!(
                DeskMessageWriteFailure::with_metadata(code, metadata.clone())
                    .projection_fallback_metadata()
                    .is_some(),
                "{code} should permit the deterministic projection fallback"
            );
        }

        for code in [
            ReportWriterErrorCode::Transport,
            ReportWriterErrorCode::HttpStatus,
            ReportWriterErrorCode::UnexpectedModel,
            ReportWriterErrorCode::RejectedFinishReason,
            ReportWriterErrorCode::MissingContent,
        ] {
            assert!(
                DeskMessageWriteFailure::with_metadata(code, metadata.clone())
                    .projection_fallback_metadata()
                    .is_none(),
                "{code} must remain fail closed"
            );
        }

        let mut http_failure = metadata.clone();
        http_failure.http_status = 503;
        let mut model_failure = metadata.clone();
        model_failure.response_model = Some("deepseek-other".to_owned());
        let mut finish_failure = metadata;
        finish_failure.finish_reason = Some("length".to_owned());
        for invalid_metadata in [http_failure, model_failure, finish_failure] {
            assert!(
                DeskMessageWriteFailure::with_metadata(
                    ReportWriterErrorCode::DeskMessageInvalidJson,
                    invalid_metadata,
                )
                .projection_fallback_metadata()
                .is_none()
            );
        }
    }

    #[test]
    fn expired_projection_becomes_a_short_standing_status() {
        let mut projection: DeskMapProjectionV1 = serde_json::from_str(include_str!(
            "../../../../contracts/golden/domain/v1/desk_map_projection.json"
        ))
        .unwrap();
        projection.stage = DeskStage::Expired;
        projection.phase = DeskLevelPhase::Expired;
        projection.direction = DeskDirection::None;
        let source = projection.message.clone();

        let standing = scheduled_operator_projection(&projection, source).unwrap();

        assert_eq!(standing.desk_view.as_str(), "NO TRADE · 当前无有效机会");
        assert_eq!(standing.title.as_str(), "SPX Desk Map · STANDBY");
        assert_eq!(standing.execution.as_str(), "WAIT · 当前无人工操作");
        assert_eq!(standing.targets.as_str(), "无有效交易目标");
        assert!(standing.structure.as_str().starts_with("参考结构 · "));
        assert!(
            standing
                .primary_path
                .as_str()
                .starts_with("方向来源：暂无；")
        );
        let visible = serde_json::to_string(&standing).unwrap();
        assert!(!visible.contains("原路径已结束"));
        assert!(!visible.contains("已过期"));
        assert!(!visible.contains("模型权重"));
        assert!(!visible.contains("P−Q"));
    }

    #[test]
    fn terminal_projection_ignores_hostile_writer_direction_title_and_old_trigger() {
        let mut projection: DeskMapProjectionV1 = serde_json::from_str(include_str!(
            "../../../../contracts/golden/domain/v1/desk_map_projection.json"
        ))
        .unwrap();
        projection.stage = DeskStage::Expired;
        projection.phase = DeskLevelPhase::Expired;
        projection.direction = DeskDirection::Up;
        projection.message.location =
            Token::new("SPX 7762.30 · ES 7790.88 · Flip 7760", "source location").unwrap();
        projection.message.structure = Token::new(
            "Put/Flip/Call 7700 / 7740–7760 / 7780\nGamma 不参与方向",
            "source structure",
        )
        .unwrap();

        let mut hostile_writer = projection.message.clone();
        hostile_writer.title = Token::new("SPX LONG / CALL READY", "hostile title").unwrap();
        hostile_writer.location = Token::new("LLM old location 7000", "hostile location").unwrap();
        hostile_writer.structure =
            Token::new("LLM old structure 7000", "hostile structure").unwrap();
        hostile_writer.primary_path = Token::new(
            "方向来源 LONG / CALL\n下一触发 复用旧事件 7760",
            "hostile trigger",
        )
        .unwrap();

        let standing = scheduled_operator_projection(&projection, hostile_writer).unwrap();

        assert_eq!(standing.title.as_str(), "SPX Desk Map · STANDBY");
        assert_eq!(standing.location.as_str(), "SPX 7762.30 · ES 7790.88");
        assert_eq!(
            standing.structure.as_str(),
            "参考结构 · Put/Flip/Call 7700 / 7740–7760 / 7780"
        );
        assert_eq!(
            standing.primary_path.as_str(),
            "方向来源：暂无；等待新的价格事件确认"
        );
        let visible = serde_json::to_string(&standing).unwrap();
        assert!(!visible.contains("LONG"));
        assert!(!visible.contains("CALL"));
        assert!(!visible.contains("复用旧事件"));
        assert!(!visible.contains("LLM old"));
    }

    #[test]
    fn actionable_projection_keeps_the_writer_message() {
        let projection: DeskMapProjectionV1 = serde_json::from_str(include_str!(
            "../../../../contracts/golden/domain/v1/desk_map_projection.json"
        ))
        .unwrap();
        let source = projection.message.clone();

        let projected = scheduled_operator_projection(&projection, source.clone()).unwrap();

        assert_eq!(projected, source);
    }

    #[test]
    fn directionless_watching_projection_keeps_its_specific_trigger() {
        let mut projection: DeskMapProjectionV1 = serde_json::from_str(include_str!(
            "../../../../contracts/golden/domain/v1/desk_map_projection.json"
        ))
        .unwrap();
        projection.stage = DeskStage::Watching;
        projection.phase = DeskLevelPhase::Testing;
        projection.direction = DeskDirection::None;
        let source = projection.message.clone();

        let projected = scheduled_operator_projection(&projection, source.clone()).unwrap();

        assert_eq!(projected, source);
    }
}
