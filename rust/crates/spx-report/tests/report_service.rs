use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use chrono::{DateTime, TimeDelta, TimeZone as _, Utc};
use sha2::{Digest as _, Sha256};
use spx_core::{LATEST_DESK_MAP_PROJECTION_SCHEMA_VERSION, LatestDeskMapProjectionV1};
use spx_domain::{DeskMapProjectionV1, MarketSession, NotificationIntentV2, Token, Validate};
use spx_ledger::{LedgerError, LedgerReader, PersistWrite};
use spx_report::{
    DEEPSEEK_MODEL_ID, DeskMessageWriteFailure, DeskMessageWriter, DeskReportOutput, HealthError,
    OwnedReportLedger, ProjectionEligibility, RESEARCH_UNAVAILABLE_DISCLOSURE, ReportHealth,
    ReportPersistDisposition, ReportPhase, ReportService, ReportServiceConfig, ReportServiceError,
    ReportTick, ReportWriterClient, ReportWriterErrorCode, ResponseMetadata, ScheduledReportStore,
    Transport, TransportError, TransportRequest, TransportResponse,
};
use tempfile::TempDir;

#[derive(Debug, Clone, Copy)]
enum WriterOutcome {
    Success,
    Failure(ReportWriterErrorCode),
}

#[derive(Clone)]
struct FakeWriter {
    outcomes: Arc<Mutex<VecDeque<WriterOutcome>>>,
    calls: Arc<Mutex<Vec<String>>>,
}

impl FakeWriter {
    fn new(outcomes: impl IntoIterator<Item = WriterOutcome>) -> Self {
        Self {
            outcomes: Arc::new(Mutex::new(outcomes.into_iter().collect())),
            calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn calls(&self) -> usize {
        self.calls.lock().unwrap().len()
    }
}

impl DeskMessageWriter for FakeWriter {
    fn write_message(
        &self,
        projection: &DeskMapProjectionV1,
    ) -> Result<DeskReportOutput, DeskMessageWriteFailure> {
        self.calls
            .lock()
            .unwrap()
            .push(projection.projection_id.as_str().to_owned());
        match self.outcomes.lock().unwrap().pop_front().unwrap() {
            WriterOutcome::Failure(code) => Err(DeskMessageWriteFailure::new(code)),
            WriterOutcome::Success => {
                let mut message = projection.message.clone();
                if projection.research_context.is_none() && projection.session != MarketSession::Gth
                {
                    message.data_quality = token(&format!(
                        "{}\n{RESEARCH_UNAVAILABLE_DISCLOSURE}",
                        message.data_quality
                    ));
                }
                let visible_content = serde_json::to_string(&message).unwrap();
                let raw_hash = hex::encode(Sha256::digest(visible_content.as_bytes()));
                Ok(DeskReportOutput {
                    message,
                    visible_content: visible_content.clone(),
                    metadata: ResponseMetadata {
                        http_status: 200,
                        raw_response_bytes: visible_content.len(),
                        raw_response_sha256: raw_hash,
                        response_model: Some(DEEPSEEK_MODEL_ID.to_owned()),
                        finish_reason: Some("stop".to_owned()),
                        visible_content_bytes: Some(visible_content.len()),
                    },
                })
            }
        }
    }
}

#[derive(Clone)]
struct StaticTransport {
    response: TransportResponse,
    calls: Arc<Mutex<u64>>,
}

impl StaticTransport {
    fn new(response: TransportResponse) -> Self {
        Self {
            response,
            calls: Arc::new(Mutex::new(0)),
        }
    }

    fn calls(&self) -> u64 {
        *self.calls.lock().unwrap()
    }
}

impl Transport for StaticTransport {
    fn send(&self, _request: &TransportRequest) -> Result<TransportResponse, TransportError> {
        *self.calls.lock().unwrap() += 1;
        Ok(self.response.clone())
    }
}

#[derive(Clone, Default)]
struct MemoryStore {
    slots: Arc<Mutex<HashSet<String>>>,
    intents: Arc<Mutex<Vec<NotificationIntentV2>>>,
    exists_calls: Arc<Mutex<u64>>,
}

impl MemoryStore {
    fn with_slot(slot: &str) -> Self {
        let store = Self::default();
        store.slots.lock().unwrap().insert(slot.to_owned());
        store
    }

    fn intents(&self) -> Vec<NotificationIntentV2> {
        self.intents.lock().unwrap().clone()
    }

    fn exists_calls(&self) -> u64 {
        *self.exists_calls.lock().unwrap()
    }
}

impl ScheduledReportStore for MemoryStore {
    fn refresh(&mut self, _now: DateTime<Utc>) -> Result<(), LedgerError> {
        Ok(())
    }

    fn exists(&self, slot: &Token, _now: DateTime<Utc>) -> Result<bool, LedgerError> {
        *self.exists_calls.lock().unwrap() += 1;
        Ok(self.slots.lock().unwrap().contains(slot.as_str()))
    }

    fn persist(
        &self,
        intent: &NotificationIntentV2,
        _now: DateTime<Utc>,
    ) -> Result<PersistWrite, LedgerError> {
        let slot = intent.lineage.slot().unwrap().as_str().to_owned();
        let inserted = self.slots.lock().unwrap().insert(slot);
        self.intents.lock().unwrap().push(intent.clone());
        Ok(if inserted {
            PersistWrite::Inserted
        } else {
            PersistWrite::Duplicate
        })
    }

    fn shutdown(&mut self) -> Result<(), LedgerError> {
        Ok(())
    }
}

fn token(value: &str) -> Token {
    Token::new(value.to_owned(), "report service test").unwrap()
}

fn config(temp: &TempDir, network_enabled: bool, backoff: &[i64]) -> ReportServiceConfig {
    let projection_path = temp.path().join("core/latest/desk-map.json");
    let ledger_path = temp.path().join("ledger/operations.sqlite");
    let health_path = temp.path().join("report/health.json");
    ReportServiceConfig::from_toml(&format!(
        r#"
            projection_path = "{}"
            ledger_path = "{}"
            health_path = "{}"
            poll_interval_millis = 100
            slot_grace_seconds = 180
            owner_lease_seconds = 180
            source_max_bytes = 4194304
            failure_backoff_seconds = {:?}
            max_attempts = 3
            targets = [
              {{ key = "bark-primary", channel = "bark" }},
              {{ key = "feishu-primary", channel = "feishu" }},
            ]

            [writer]
            network_enabled = {network_enabled}
            api_key_env = "DEEPSEEK_API_KEY"
            max_tokens = 64000
            request_timeout_seconds = 90
        "#,
        projection_path.display(),
        ledger_path.display(),
        health_path.display(),
        backoff,
    ))
    .unwrap()
}

fn projection(
    projection_id: &str,
    source_slot: &str,
    available_at: DateTime<Utc>,
) -> DeskMapProjectionV1 {
    serde_json::from_value(serde_json::json!({
        "schema_version": "desk_map_projection.v1",
        "projection_id": projection_id,
        "source_snapshot_id": format!("snapshot:{projection_id}"),
        "source_slot": source_slot,
        "trading_date_et": "2026-08-04",
        "session": "rth",
        "observed_through": available_at - TimeDelta::seconds(1),
        "available_at": available_at,
        "valid_until": available_at + TimeDelta::minutes(20),
        "structure_fingerprint": "a".repeat(64),
        "stage": "confirmed",
        "phase": "confirmed",
        "direction": "up",
        "thesis": "breakout",
        "level_kind": "flip_high",
        "level": 7510.0,
        "quality": "ready",
        "quality_reasons": [],
        "research_context_document_id": null,
        "research_context": null,
        "action_authority": "none",
        "automatic_ordering": false,
        "message": {
            "title": "SPX RTH Desk Map · 10:00 ET",
            "desk_view": "Breakout confirmed only while price accepts above the flip band.",
            "location": "SPX 7512; VWAP 7504; OR15 high 7508.",
            "structure": "Put wall 7450; flip band 7490-7510; call wall 7550.",
            "primary_path": "Hold 7510 on retest, then probe 7550 without chasing extension.",
            "alternative_path": "Reject below 7510 and rotate through 7490 toward the lower wall.",
            "targets": "Upside 7550 then 7575; downside 7490 then 7450.",
            "execution": "Manual observation only; exact-leg readiness and ask cap are required.",
            "data_quality": "Schwab RTH structure ready; no embedded research context."
        }
    }))
    .unwrap()
}

fn gth_projection(
    projection_id: &str,
    source_slot: &str,
    available_at: DateTime<Utc>,
) -> DeskMapProjectionV1 {
    let mut projection = projection(projection_id, source_slot, available_at);
    projection.session = MarketSession::Gth;
    projection.source_slot = token(source_slot);
    projection.valid_until = available_at + TimeDelta::minutes(65);
    projection.message.title = token("SPX GTH Desk Map · 21:30 ET");
    projection
}

fn write_latest(path: &std::path::Path, projection: DeskMapProjectionV1) {
    projection.validate().unwrap();
    let latest = LatestDeskMapProjectionV1 {
        schema_version: LATEST_DESK_MAP_PROJECTION_SCHEMA_VERSION.to_owned(),
        published_at: projection.available_at + TimeDelta::seconds(1),
        message_id: token(&format!("message:{}", projection.projection_id)),
        projection,
    };
    latest.validate().unwrap();
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, serde_json::to_vec_pretty(&latest).unwrap()).unwrap();
}

fn ten_am() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 8, 4, 14, 0, 0).unwrap()
}

fn deepseek_response(
    status: u16,
    model: &str,
    finish_reason: &str,
    content: &str,
) -> TransportResponse {
    TransportResponse::new(
        status,
        serde_json::json!({
            "model": model,
            "choices": [{
                "finish_reason": finish_reason,
                "message": {"content": content}
            }]
        })
        .to_string(),
    )
}

#[test]
fn waits_for_current_slot_then_persists_once_through_real_ledger() {
    let temp = TempDir::new().unwrap();
    let config = config(&temp, true, &[5, 15]);
    let slot_start = ten_am();
    write_latest(
        &config.projection_path,
        projection(
            "desk-map:old-0930",
            "2026-08-04:09:30",
            slot_start - TimeDelta::seconds(1),
        ),
    );
    let writer = FakeWriter::new([WriterOutcome::Success]);
    let inspector = writer.clone();
    let store = OwnedReportLedger::open(
        &config.ledger_path,
        "report-service-e2e-owner",
        slot_start,
        config.owner_lease_seconds,
    )
    .unwrap();
    let mut service = ReportService::open(config.clone(), true, writer, store, slot_start).unwrap();

    assert_eq!(
        service
            .run_once_at(slot_start + TimeDelta::seconds(30))
            .unwrap(),
        ReportTick::Ineligible {
            reason: ProjectionEligibility::AwaitingCurrentProjection
        }
    );
    assert_eq!(inspector.calls(), 0);

    let current_available = slot_start + TimeDelta::seconds(113);
    write_latest(
        &config.projection_path,
        projection(
            "desk-map:current-1000",
            "2026-08-04:10:00",
            current_available,
        ),
    );
    let generated_at = current_available + TimeDelta::seconds(1);
    assert_eq!(
        service.run_once_at(generated_at).unwrap(),
        ReportTick::Persisted {
            slot: "2026-08-04T10:00:00-04:00".to_owned(),
            disposition: ReportPersistDisposition::Inserted
        }
    );
    assert_eq!(inspector.calls(), 1);
    assert_eq!(
        service.run_once_at(generated_at).unwrap(),
        ReportTick::Duplicate {
            slot: "2026-08-04T10:00:00-04:00".to_owned()
        }
    );
    assert_eq!(inspector.calls(), 1, "dedup must run before DeepSeek");
    service.shutdown(generated_at).unwrap();

    let ledger = LedgerReader::open_existing(&config.ledger_path).unwrap();
    assert_eq!(ledger.health().unwrap().pending, 2);
    let health = ReportHealth::load(&config.health_path).unwrap();
    assert_eq!(
        health.last_response_model.as_deref(),
        Some(DEEPSEEK_MODEL_ID)
    );
    assert_eq!(health.last_finish_reason.as_deref(), Some("stop"));
    assert!(health.last_visible_content_bytes.unwrap() > 283);
    assert_eq!(health.last_response_sha256.as_deref().unwrap().len(), 64);
}

#[test]
fn stop_validation_failure_persists_the_validated_projection_message_once() {
    let temp = TempDir::new().unwrap();
    let config = config(&temp, true, &[5]);
    let now = ten_am() + TimeDelta::seconds(10);
    let source = projection("desk-map:fallback", "2026-08-04:10:00", ten_am());
    let expected_message = source.message.clone();
    write_latest(&config.projection_path, source);

    let transport = StaticTransport::new(deepseek_response(
        200,
        DEEPSEEK_MODEL_ID,
        "stop",
        "not a desk-message JSON object",
    ));
    let transport_inspector = transport.clone();
    let writer = ReportWriterClient::new(config.writer.clone(), true, transport).unwrap();
    let store = MemoryStore::default();
    let store_inspector = store.clone();
    let mut service = ReportService::open(config.clone(), true, writer, store, now).unwrap();

    assert_eq!(
        service.run_once_at(now).unwrap(),
        ReportTick::Persisted {
            slot: "2026-08-04T10:00:00-04:00".to_owned(),
            disposition: ReportPersistDisposition::Inserted,
        }
    );
    assert_eq!(transport_inspector.calls(), 1);
    let intents = store_inspector.intents();
    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].message, expected_message);

    let health = ReportHealth::load(&config.health_path).unwrap();
    assert_eq!(health.phase, ReportPhase::Persisted);
    assert_eq!(
        health.last_fallback_reason.as_deref(),
        Some("desk_message_invalid_json")
    );
    assert_eq!(health.counters.projection_message_fallbacks, 1);
    assert_eq!(health.counters.generation_failures, 0);
    assert_eq!(
        health.last_response_model.as_deref(),
        Some(DEEPSEEK_MODEL_ID)
    );
    assert_eq!(health.last_finish_reason.as_deref(), Some("stop"));
    assert!(health.last_error_code.is_none());

    assert!(matches!(
        service.run_once_at(now).unwrap(),
        ReportTick::Duplicate { .. }
    ));
    assert_eq!(transport_inspector.calls(), 1);
    assert_eq!(
        ReportHealth::load(&config.health_path)
            .unwrap()
            .counters
            .projection_message_fallbacks,
        1
    );
}

#[test]
fn stop_semantic_failure_also_uses_the_projection_message_fallback() {
    let temp = TempDir::new().unwrap();
    let config = config(&temp, true, &[5]);
    let now = ten_am() + TimeDelta::seconds(10);
    let source = projection("desk-map:semantic-fallback", "2026-08-04:10:00", ten_am());
    let expected_message = source.message.clone();
    let mut leaked_message = source.message.clone();
    leaked_message.data_quality = token("schema_version=desk_map_projection.v1");
    let leaked_content = serde_json::to_string(&leaked_message).unwrap();
    write_latest(&config.projection_path, source);

    let transport = StaticTransport::new(deepseek_response(
        200,
        DEEPSEEK_MODEL_ID,
        "stop",
        &leaked_content,
    ));
    let writer = ReportWriterClient::new(config.writer.clone(), true, transport).unwrap();
    let store = MemoryStore::default();
    let store_inspector = store.clone();
    let mut service = ReportService::open(config.clone(), true, writer, store, now).unwrap();

    assert!(matches!(
        service.run_once_at(now).unwrap(),
        ReportTick::Persisted { .. }
    ));
    let intents = store_inspector.intents();
    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].message, expected_message);
    let health = ReportHealth::load(&config.health_path).unwrap();
    assert_eq!(
        health.last_fallback_reason.as_deref(),
        Some("internal_detail_leak")
    );
    assert_eq!(health.counters.projection_message_fallbacks, 1);
}

#[test]
fn omitted_p_vs_q_diagnostics_persist_the_compact_writer_message() {
    let temp = TempDir::new().unwrap();
    let config = config(&temp, true, &[5]);
    let now = ten_am() + TimeDelta::seconds(10);
    let mut source = projection("desk-map:pq-fallback", "2026-08-04:10:00", ten_am());
    source.message.desk_view = token(
        "NO TRADE\nP/Q研究（未校准，不产生方向） 5分钟上行终值跟随：P 62%（前日止，n=98/14日，区间52%–71%） · Q代理 49% · P−Q +13pp；未扣点差/滑点，真实成交与净收益标签尚不可用 → NO TRADE",
    );
    let mut compressed = source.message.clone();
    compressed.desk_view = token("NO TRADE · 等待下一个价格触发");
    write_latest(&config.projection_path, source);

    let transport = StaticTransport::new(deepseek_response(
        200,
        DEEPSEEK_MODEL_ID,
        "stop",
        &serde_json::to_string(&compressed).unwrap(),
    ));
    let writer = ReportWriterClient::new(config.writer.clone(), true, transport).unwrap();
    let store = MemoryStore::default();
    let store_inspector = store.clone();
    let mut service = ReportService::open(config.clone(), true, writer, store, now).unwrap();

    assert!(matches!(
        service.run_once_at(now).unwrap(),
        ReportTick::Persisted { .. }
    ));
    let intents = store_inspector.intents();
    assert_eq!(intents.len(), 1);
    assert_eq!(
        intents[0].message.desk_view.as_str(),
        "NO TRADE · 等待下一个价格触发"
    );
    assert!(!intents[0].message.desk_view.as_str().contains("P/Q"));
    assert!(!intents[0].message.desk_view.as_str().contains("P−Q"));
    let health = ReportHealth::load(&config.health_path).unwrap();
    assert!(health.last_fallback_reason.is_none());
    assert_eq!(health.counters.projection_message_fallbacks, 0);
}

#[test]
fn non_stop_provider_completion_still_backs_off_without_fallback() {
    let temp = TempDir::new().unwrap();
    let config = config(&temp, true, &[5]);
    let now = ten_am() + TimeDelta::seconds(10);
    let source = projection("desk-map:truncated-provider", "2026-08-04:10:00", ten_am());
    let content = serde_json::to_string(&source.message).unwrap();
    write_latest(&config.projection_path, source);
    let transport = StaticTransport::new(deepseek_response(
        200,
        DEEPSEEK_MODEL_ID,
        "length",
        &content,
    ));
    let writer = ReportWriterClient::new(config.writer.clone(), true, transport).unwrap();
    let store = MemoryStore::default();
    let store_inspector = store.clone();
    let mut service = ReportService::open(config.clone(), true, writer, store, now).unwrap();

    assert_eq!(
        service.run_once_at(now).unwrap(),
        ReportTick::Backoff {
            error_code: ReportWriterErrorCode::OutputTruncated,
            next_attempt_at: now + TimeDelta::seconds(5),
        }
    );
    assert!(store_inspector.intents().is_empty());
    let health = ReportHealth::load(&config.health_path).unwrap();
    assert!(health.last_fallback_reason.is_none());
    assert_eq!(health.counters.projection_message_fallbacks, 0);
    assert_eq!(health.counters.generation_failures, 1);
    assert_eq!(health.last_finish_reason.as_deref(), Some("length"));
}

#[test]
fn gth_half_hour_slot_persists_through_the_same_typed_outbox() {
    let temp = TempDir::new().unwrap();
    let config = config(&temp, true, &[5]);
    let slot_start = Utc.with_ymd_and_hms(2026, 8, 4, 1, 30, 0).unwrap();
    let available_at = slot_start + TimeDelta::seconds(1);
    write_latest(
        &config.projection_path,
        gth_projection("desk-map:gth-2130", "2026-08-04:gth:21:30", available_at),
    );
    let writer = FakeWriter::new([WriterOutcome::Success]);
    let store = MemoryStore::default();
    let store_inspector = store.clone();
    let mut service = ReportService::open(
        config,
        true,
        writer,
        store,
        available_at + TimeDelta::seconds(1),
    )
    .unwrap();

    assert_eq!(
        service
            .run_once_at(available_at + TimeDelta::seconds(1))
            .unwrap(),
        ReportTick::Persisted {
            slot: "2026-08-03T21:30:00-04:00".to_owned(),
            disposition: ReportPersistDisposition::Inserted,
        }
    );
    let intents = store_inspector.intents();
    assert_eq!(intents.len(), 1);
    assert_eq!(
        intents[0].lineage.slot().unwrap().as_str(),
        "2026-08-03T21:30:00-04:00"
    );
}

#[test]
fn gth_open_quarter_snapshot_does_not_generate_a_human_report() {
    let temp = TempDir::new().unwrap();
    let config = config(&temp, true, &[5]);
    let slot_start = Utc.with_ymd_and_hms(2026, 8, 4, 0, 15, 0).unwrap();
    let available_at = slot_start + TimeDelta::seconds(1);
    write_latest(
        &config.projection_path,
        gth_projection("desk-map:gth-2015", "2026-08-04:gth:20:15", available_at),
    );
    let writer = FakeWriter::new([WriterOutcome::Success]);
    let writer_inspector = writer.clone();
    let store = MemoryStore::default();
    let store_inspector = store.clone();
    let mut service = ReportService::open(
        config,
        true,
        writer,
        store,
        available_at + TimeDelta::seconds(1),
    )
    .unwrap();

    assert_eq!(
        service
            .run_once_at(available_at + TimeDelta::seconds(1))
            .unwrap(),
        ReportTick::AwaitingSlot
    );
    assert_eq!(writer_inspector.calls(), 0);
    assert!(store_inspector.intents().is_empty());
}

#[test]
fn writer_failure_uses_memory_backoff_then_retries_without_persisting_failure() {
    let temp = TempDir::new().unwrap();
    let config = config(&temp, true, &[5]);
    let now = ten_am() + TimeDelta::seconds(10);
    write_latest(
        &config.projection_path,
        projection("desk-map:backoff", "2026-08-04:10:00", ten_am()),
    );
    let writer = FakeWriter::new([
        WriterOutcome::Failure(ReportWriterErrorCode::SemanticMarkerFieldMismatch),
        WriterOutcome::Success,
    ]);
    let inspector = writer.clone();
    let store = MemoryStore::default();
    let store_inspector = store.clone();
    let mut service = ReportService::open(config, true, writer, store, now).unwrap();

    assert_eq!(
        service.run_once_at(now).unwrap(),
        ReportTick::Backoff {
            error_code: ReportWriterErrorCode::SemanticMarkerFieldMismatch,
            next_attempt_at: now + TimeDelta::seconds(5)
        }
    );
    assert!(store_inspector.intents().is_empty());
    assert_eq!(
        service.run_once_at(now + TimeDelta::seconds(4)).unwrap(),
        ReportTick::Backoff {
            error_code: ReportWriterErrorCode::SemanticMarkerFieldMismatch,
            next_attempt_at: now + TimeDelta::seconds(5)
        }
    );
    assert_eq!(inspector.calls(), 1);
    assert_eq!(
        service.run_once_at(now + TimeDelta::seconds(5)).unwrap(),
        ReportTick::Persisted {
            slot: "2026-08-04T10:00:00-04:00".to_owned(),
            disposition: ReportPersistDisposition::Inserted
        }
    );
    assert_eq!(inspector.calls(), 2);
    let intents = store_inspector.intents();
    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].targets.len(), 2);
    assert_eq!(intents[0].max_attempts, 3);
    assert!(
        intents[0]
            .message
            .data_quality
            .as_str()
            .contains(RESEARCH_UNAVAILABLE_DISCLOSURE)
    );
    assert!(store_inspector.exists_calls() >= 4);
}

#[test]
fn existing_slot_and_network_gates_prevent_model_calls() {
    let temp = TempDir::new().unwrap();
    let enabled = config(&temp, true, &[5]);
    let now = ten_am() + TimeDelta::seconds(10);
    write_latest(
        &enabled.projection_path,
        projection("desk-map:duplicate", "2026-08-04:10:00", ten_am()),
    );
    let writer = FakeWriter::new([WriterOutcome::Success]);
    let inspector = writer.clone();
    let store = MemoryStore::with_slot("2026-08-04T10:00:00-04:00");
    let mut service = ReportService::open(enabled, true, writer, store, now).unwrap();
    assert!(matches!(
        service.run_once_at(now).unwrap(),
        ReportTick::Duplicate { .. }
    ));
    assert_eq!(inspector.calls(), 0);

    let disabled = config(&temp, false, &[5]);
    let denied = ReportService::open(
        disabled,
        true,
        FakeWriter::new([WriterOutcome::Success]),
        MemoryStore::default(),
        now,
    )
    .err()
    .unwrap();
    assert_eq!(
        denied.to_string(),
        "report network is not authorized by both config and caller"
    );
}

#[test]
fn service_restart_restores_diagnostics_without_restoring_runtime_state() {
    let temp = TempDir::new().unwrap();
    let config = config(&temp, true, &[5]);
    let prior_at = ten_am();
    let restart_at = prior_at + TimeDelta::minutes(5);
    let mut prior = ReportHealth::new(
        std::path::Path::new("/retired/desk-map.json"),
        false,
        prior_at,
    );
    prior.phase = ReportPhase::Generating;
    prior.last_projection_id = Some("desk-map:before-restart".to_owned());
    prior.active_slot = Some("2026-08-04T10:00:00-04:00".to_owned());
    prior.last_persisted_at = Some(prior_at - TimeDelta::minutes(30));
    prior.next_attempt_at = Some(prior_at + TimeDelta::seconds(5));
    prior.consecutive_generation_failures = 2;
    prior.last_error_code = Some("provider_rate_limited".to_owned());
    prior.last_response_model = Some(DEEPSEEK_MODEL_ID.to_owned());
    prior.last_finish_reason = Some("stop".to_owned());
    prior.last_visible_content_bytes = Some(3_782);
    prior.last_response_sha256 = Some("b".repeat(64));
    prior.last_fallback_reason = Some("critical_fact_missing".to_owned());
    prior.counters.ticks = 11;
    prior.counters.projection_message_fallbacks = 2;
    prior.counters.persisted_reports = 3;
    prior.persist(&config.health_path).unwrap();

    let service = ReportService::open(
        config.clone(),
        true,
        FakeWriter::new([]),
        MemoryStore::default(),
        restart_at,
    )
    .unwrap();
    let health = service.health();
    assert_eq!(health.updated_at, restart_at);
    assert_eq!(health.phase, ReportPhase::Starting);
    assert!(health.network_authorized);
    assert_eq!(
        health.projection_path,
        config.projection_path.display().to_string()
    );
    assert!(health.active_slot.is_none());
    assert!(health.next_attempt_at.is_none());
    assert_eq!(health.consecutive_generation_failures, 0);
    assert_eq!(health.last_projection_id, prior.last_projection_id);
    assert_eq!(health.last_persisted_at, prior.last_persisted_at);
    assert_eq!(health.last_error_code, prior.last_error_code);
    assert_eq!(health.last_response_model, prior.last_response_model);
    assert_eq!(health.last_finish_reason, prior.last_finish_reason);
    assert_eq!(
        health.last_visible_content_bytes,
        prior.last_visible_content_bytes
    );
    assert_eq!(health.last_response_sha256, prior.last_response_sha256);
    assert_eq!(health.last_fallback_reason, prior.last_fallback_reason);
    assert_eq!(health.counters, prior.counters);
    assert_eq!(ReportHealth::load(&config.health_path).unwrap(), *health);
}

#[test]
fn corrupt_or_incompatible_health_prevents_service_start_without_overwrite() {
    let temp = TempDir::new().unwrap();
    let config = config(&temp, true, &[5]);
    let now = ten_am();
    std::fs::create_dir_all(config.health_path.parent().unwrap()).unwrap();

    let corrupt = b"{not-json".to_vec();
    std::fs::write(&config.health_path, &corrupt).unwrap();
    let error = ReportService::open(
        config.clone(),
        true,
        FakeWriter::new([]),
        MemoryStore::default(),
        now,
    )
    .err()
    .unwrap();
    assert!(matches!(
        error,
        ReportServiceError::Health(HealthError::Json(_))
    ));
    assert_eq!(std::fs::read(&config.health_path).unwrap(), corrupt);

    let mut incompatible =
        serde_json::to_value(ReportHealth::new(&config.projection_path, true, now)).unwrap();
    incompatible["schema_version"] = serde_json::json!("spx_report_health.v2");
    let incompatible = serde_json::to_vec_pretty(&incompatible).unwrap();
    std::fs::write(&config.health_path, &incompatible).unwrap();
    let error = ReportService::open(
        config.clone(),
        true,
        FakeWriter::new([]),
        MemoryStore::default(),
        now,
    )
    .err()
    .unwrap();
    assert!(matches!(
        error,
        ReportServiceError::Health(HealthError::SchemaMismatch)
    ));
    assert_eq!(std::fs::read(&config.health_path).unwrap(), incompatible);
}
