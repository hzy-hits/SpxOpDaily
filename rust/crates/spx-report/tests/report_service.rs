use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use chrono::{DateTime, TimeDelta, TimeZone as _, Utc};
use sha2::{Digest as _, Sha256};
use spx_core::{LATEST_DESK_MAP_PROJECTION_SCHEMA_VERSION, LatestDeskMapProjectionV1};
use spx_domain::{DeskMapProjectionV1, MarketSession, NotificationIntentV2, Token, Validate};
use spx_ledger::{LedgerError, LedgerReader, PersistWrite};
use spx_report::{
    DEEPSEEK_MODEL_ID, DeskMessageWriter, DeskReportOutput, OwnedReportLedger,
    ProjectionEligibility, RESEARCH_UNAVAILABLE_DISCLOSURE, ReportHealth, ReportPersistDisposition,
    ReportService, ReportServiceConfig, ReportTick, ReportWriterErrorCode, ResponseMetadata,
    ScheduledReportStore,
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
    ) -> Result<DeskReportOutput, ReportWriterErrorCode> {
        self.calls
            .lock()
            .unwrap()
            .push(projection.projection_id.as_str().to_owned());
        match self.outcomes.lock().unwrap().pop_front().unwrap() {
            WriterOutcome::Failure(code) => Err(code),
            WriterOutcome::Success => {
                let mut message = projection.message.clone();
                if projection.research_context.is_none() {
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
fn writer_failure_uses_memory_backoff_then_retries_without_persisting_failure() {
    let temp = TempDir::new().unwrap();
    let config = config(&temp, true, &[5]);
    let now = ten_am() + TimeDelta::seconds(10);
    write_latest(
        &config.projection_path,
        projection("desk-map:backoff", "2026-08-04:10:00", ten_am()),
    );
    let writer = FakeWriter::new([
        WriterOutcome::Failure(ReportWriterErrorCode::OutputCompressed),
        WriterOutcome::Success,
    ]);
    let inspector = writer.clone();
    let store = MemoryStore::default();
    let store_inspector = store.clone();
    let mut service = ReportService::open(config, true, writer, store, now).unwrap();

    assert_eq!(
        service.run_once_at(now).unwrap(),
        ReportTick::Backoff {
            error_code: ReportWriterErrorCode::OutputCompressed,
            next_attempt_at: now + TimeDelta::seconds(5)
        }
    );
    assert!(store_inspector.intents().is_empty());
    assert_eq!(
        service.run_once_at(now + TimeDelta::seconds(4)).unwrap(),
        ReportTick::Backoff {
            error_code: ReportWriterErrorCode::OutputCompressed,
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
