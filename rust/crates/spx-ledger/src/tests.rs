use chrono::{DateTime, TimeDelta, TimeZone, Utc};
use spx_domain::{
    DECISION_SCHEMA_VERSION, DeliveryChannel, DeskMessageV1, DeskMessageV2,
    NOTIFICATION_INTENT_SCHEMA_VERSION, NOTIFICATION_INTENT_V2_SCHEMA_VERSION,
    NotificationIntentV1, NotificationIntentV2, NotificationLineageV2, NotificationTargetV1,
    StrategyAction, StrategyBlockReason, StrategyDecisionV1, Token, Validate, canonical_json_hash,
};
use tempfile::TempDir;

use super::*;

fn at(second: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(1_785_590_400 + second, 0).unwrap()
}

fn token(value: &str) -> Token {
    Token::new(value, "test token").unwrap()
}

fn decision(now: DateTime<Utc>, manual: bool) -> StrategyDecisionV1 {
    StrategyDecisionV1 {
        schema_version: DECISION_SCHEMA_VERSION.to_owned(),
        decision_id: token(if manual {
            "decision-manual"
        } else {
            "decision-no-trade"
        }),
        request_id: token(if manual {
            "request-manual"
        } else {
            "request-no-trade"
        }),
        strategy_id: token("strategy-v1"),
        policy_version: token("policy-v1"),
        snapshot_id: token("snapshot-v1"),
        action: if manual {
            StrategyAction::ManualCandidate
        } else {
            StrategyAction::NoTrade
        },
        direction: None,
        evaluated_at: now,
        valid_until: now + TimeDelta::seconds(30),
        block_reasons: if manual {
            Vec::new()
        } else {
            vec![StrategyBlockReason::ProviderNotReady]
        },
        exact_legs: None,
        evidence_hash: token("evidence-hash"),
        automatic_ordering: false,
    }
}

fn intent(now: DateTime<Utc>) -> NotificationIntentV1 {
    NotificationIntentV1 {
        schema_version: NOTIFICATION_INTENT_SCHEMA_VERSION.to_owned(),
        intent_id: token("intent-manual"),
        semantic_id: token("semantic-manual"),
        decision_id: token("decision-manual"),
        created_at: now,
        expires_at: now + TimeDelta::seconds(30),
        message: DeskMessageV1 {
            title: token("SPX manual candidate"),
            desk_view: token("range"),
            execution: token("manual only"),
            risk: token("no automatic order"),
            targets: token("test"),
            data_quality: token("live exact NBBO"),
        },
        targets: vec![
            NotificationTargetV1 {
                key: token("bark-primary"),
                channel: DeliveryChannel::Bark,
            },
            NotificationTargetV1 {
                key: token("feishu-primary"),
                channel: DeliveryChannel::Feishu,
            },
        ],
        max_attempts: 3,
    }
}

fn scheduled_report(now: DateTime<Utc>) -> NotificationIntentV2 {
    NotificationIntentV2 {
        schema_version: NOTIFICATION_INTENT_V2_SCHEMA_VERSION.to_owned(),
        intent_id: token("report-event-2026-08-04-1000-et"),
        semantic_id: token("desk-map-2026-08-04-1000-et"),
        lineage: NotificationLineageV2::ScheduledReport {
            source_projection_id: token("projection-2026-08-04-095959-et"),
            slot: token("2026-08-04:10:00"),
        },
        created_at: now,
        expires_at: now + TimeDelta::minutes(20),
        message: DeskMessageV2 {
            title: token("SPX RTH Desk Map · 10:00 ET"),
            desk_view: token("Bullish above VWAP; neutral inside the opening range."),
            location: token("SPX 7568; OR15 high 7565; VWAP 7558."),
            structure: token("Call wall 7580; gamma flip 7550; put wall 7525."),
            primary_path: token(&"Hold 7565, retest, then probe 7580. ".repeat(90)),
            alternative_path: token("Lose 7558 and accept below VWAP, then rotate toward 7550."),
            targets: token("Upside 7580/7595; downside 7550/7525."),
            execution: token("Observe the retest; no automatic order and no chase."),
            data_quality: token("Schwab RTH live; surface quality degraded by clipped mass."),
        },
        targets: vec![NotificationTargetV1 {
            key: token("bark-primary"),
            channel: DeliveryChannel::Bark,
        }],
        max_attempts: 3,
    }
}

fn create_v1_ledger_with_outbox(path: &std::path::Path) {
    let connection = rusqlite::Connection::open(path).unwrap();
    connection
        .execute_batch(
            "PRAGMA foreign_keys = ON;
             PRAGMA trusted_schema = OFF;",
        )
        .unwrap();
    connection
        .execute_batch(crate::schema::MIGRATION_BOOTSTRAP)
        .unwrap();
    connection
        .execute_batch(crate::schema::MIGRATION_1)
        .unwrap();
    connection
        .execute(
            "INSERT INTO schema_migrations (
                version, name, checksum_sha256, applied_at_us
             ) VALUES (1, 'initial_operational_ledger', ?1, ?2)",
            rusqlite::params![
                canonical_json_hash(&crate::schema::MIGRATION_1).unwrap(),
                at(0).timestamp_micros()
            ],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO decisions (
                decision_id, request_id, action, policy_version, snapshot_id,
                evaluated_at_us, valid_until_us, payload_json, payload_sha256,
                writer_generation, created_at_us
             ) VALUES (
                'legacy-decision', 'legacy-request', 'manual_candidate', 'policy-v1',
                'snapshot-v1', 1, 2, '{}', ?1, 1, 1
             )",
            ["0".repeat(64)],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO notification_events (
                event_id, semantic_id, decision_id, lane, occurred_at_us, expires_at_us,
                payload_json, payload_sha256, target_set_sha256, writer_generation, created_at_us
             ) VALUES (
                'legacy-event', 'legacy-semantic', 'legacy-decision', 'trade_ready', 1, 2,
                '{}', ?1, ?2, 1, 1
             )",
            rusqlite::params!["1".repeat(64), "2".repeat(64)],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO notification_targets (
                target_id, event_id, target_key, channel, status, attempt_count,
                max_attempts, replay_generation, lease_sequence, next_attempt_at_us, updated_at_us
             ) VALUES (
                'legacy-target', 'legacy-event', 'bark-primary', 'bark', 'pending', 0,
                3, 0, 0, 1, 1
             )",
            [],
        )
        .unwrap();
}

fn manual_decision(now: DateTime<Utc>) -> StrategyDecisionV1 {
    let mut decision = decision(now, true);
    decision.direction = Some(spx_domain::CandidateDirection::CallVertical10);
    decision.exact_legs = Some(spx_domain::ExactLegEvidenceV1 {
        provider: spx_domain::Provider::Schwab,
        long_contract_id: token("long"),
        short_contract_id: token("short"),
        right: spx_domain::OptionRight::Call,
        long_strike: spx_domain::PositiveF64::new(6000.0, "strike").unwrap(),
        short_strike: spx_domain::PositiveF64::new(6010.0, "strike").unwrap(),
        long_bid: spx_domain::PositiveF64::new(3.0, "bid").unwrap(),
        long_ask: spx_domain::PositiveF64::new(3.2, "ask").unwrap(),
        short_bid: spx_domain::PositiveF64::new(1.0, "bid").unwrap(),
        short_ask: spx_domain::PositiveF64::new(1.2, "ask").unwrap(),
        max_age_seconds: spx_domain::NonNegativeF64::new(0.5, "age").unwrap(),
        max_skew_seconds: spx_domain::NonNegativeF64::new(0.1, "skew").unwrap(),
        observed_at: now,
    });
    decision
}

fn seed_manual_notification(ledger: &Ledger, now: DateTime<Utc>) -> (OwnerLease, OwnerLease) {
    let core = ledger
        .acquire_owner(
            OwnerRole::Core,
            "core-owner-00000001",
            now,
            TimeDelta::seconds(60),
        )
        .unwrap();
    ledger
        .persist_decision(&core, &manual_decision(now), Some(&intent(now)), now)
        .unwrap();
    let delivery = ledger
        .acquire_owner(
            OwnerRole::Delivery,
            "delivery-owner-0001",
            now,
            TimeDelta::seconds(60),
        )
        .unwrap();
    (core, delivery)
}

fn start_transport(
    ledger: &Ledger,
    delivery: &OwnerLease,
    claimed: &ClaimedDelivery,
    now: DateTime<Utc>,
) -> String {
    match ledger
        .begin_transport(delivery, claimed, now, TimeDelta::seconds(10))
        .unwrap()
    {
        BeginTransport::Started { attempt_id } => attempt_id,
        BeginTransport::Cancelled | BeginTransport::Expired => {
            panic!("seeded live claim must enter transport")
        }
    }
}

#[test]
fn sqlite_rejects_illegal_target_status() {
    let temp = TempDir::new().unwrap();
    let ledger = Ledger::open(temp.path().join("ledger.sqlite")).unwrap();
    let connection = ledger.connection().unwrap();
    let error = connection
        .execute(
            "INSERT INTO notification_targets (
                target_id, event_id, target_key, channel, status, attempt_count, max_attempts,
                replay_generation, lease_sequence, next_attempt_at_us, updated_at_us
             ) VALUES ('t', 'missing', 'bark', 'bark', 'typo', 0, 1, 0, 0, 1, 1)",
            [],
        )
        .unwrap_err();
    assert!(error.to_string().contains("CHECK") || error.to_string().contains("FOREIGN"));
}

#[test]
fn sqlite_rejects_cross_table_attempt_forgery() {
    let temp = TempDir::new().unwrap();
    let ledger = Ledger::open(temp.path().join("ledger.sqlite")).unwrap();
    let now = at(0);
    let (_, delivery) = seed_manual_notification(&ledger, now);
    let claimed = ledger
        .claim_next(&delivery, now, TimeDelta::seconds(10))
        .unwrap()
        .unwrap();

    for (attempt_id, replay_delta, attempt_delta, key_suffix, started_offset_us) in [
        ("forged-replay", 1_i64, 0_i64, "", 1_i64),
        ("forged-attempt-no", 0, 1, "", 1),
        ("forged-idempotency", 0, 0, ":forged", 1),
        ("forged-start-time", 0, 0, "", 10_000_000),
    ] {
        let mut connection = ledger.connection().unwrap();
        let transaction = connection.transaction().unwrap();
        transaction
            .execute(
                "UPDATE notification_targets SET
                    status = 'in_flight', attempt_count = 1, current_attempt_id = ?1
                 WHERE target_id = ?2",
                rusqlite::params![attempt_id, claimed.handle.target_id()],
            )
            .unwrap();
        let forged = transaction
            .execute(
                "INSERT INTO delivery_attempts (
                    attempt_id, target_id, channel, claim_token, owner_generation,
                    replay_generation, attempt_no, lease_sequence, idempotency_key, started_at_us
                 ) SELECT ?1, target_id, channel, claim_token, claim_owner_generation,
                          replay_generation + ?2, attempt_count + ?3, lease_sequence,
                          event_id || ':' || target_id || ?4, claimed_at_us + ?5
                   FROM notification_targets WHERE target_id = ?6",
                rusqlite::params![
                    attempt_id,
                    replay_delta,
                    attempt_delta,
                    key_suffix,
                    started_offset_us,
                    claimed.handle.target_id()
                ],
            )
            .unwrap_err();
        assert!(forged.to_string().contains("attempt_target_mismatch"));
    }

    let missing_attempt = ledger
        .connection()
        .unwrap()
        .execute(
            "UPDATE notification_targets SET
                status = 'in_flight', attempt_count = 1,
                current_attempt_id = 'forged-missing-attempt'
             WHERE target_id = ?1",
            [claimed.handle.target_id()],
        )
        .unwrap_err();
    assert!(missing_attempt.to_string().contains("FOREIGN KEY"));

    let attempt_id = start_transport(&ledger, &delivery, &claimed, now + TimeDelta::seconds(1));
    let mismatched_attempt = ledger
        .connection()
        .unwrap()
        .execute(
            "INSERT INTO delivery_attempts (
                attempt_id, target_id, channel, claim_token, owner_generation,
                replay_generation, attempt_no, lease_sequence, idempotency_key, started_at_us
             ) SELECT 'forged-attempt', target_id, 'webhook', claim_token, owner_generation,
                      replay_generation, attempt_no + 1, lease_sequence + 1,
                      idempotency_key || ':forged', started_at_us + 1
               FROM delivery_attempts WHERE attempt_id = ?1",
            [&attempt_id],
        )
        .unwrap_err();
    assert!(
        mismatched_attempt
            .to_string()
            .contains("attempt_target_mismatch")
    );
}

#[test]
fn sqlite_rejects_cross_table_receipt_forgery() {
    let temp = TempDir::new().unwrap();
    let ledger = Ledger::open(temp.path().join("ledger.sqlite")).unwrap();
    let now = at(0);
    let (_, delivery) = seed_manual_notification(&ledger, now);
    let claimed = ledger
        .claim_next(&delivery, now, TimeDelta::seconds(10))
        .unwrap()
        .unwrap();
    let attempt_id = start_transport(&ledger, &delivery, &claimed, now + TimeDelta::seconds(1));
    ledger
        .settle(
            &delivery,
            &claimed.handle,
            &attempt_id,
            &Settlement::Delivered {
                provider_message_id: None,
            },
            now + TimeDelta::seconds(2),
        )
        .unwrap();
    let mismatched_receipt = ledger
        .connection()
        .unwrap()
        .execute(
            "INSERT INTO delivery_receipts (
                receipt_id, target_id, intent_id, target_key, channel, attempt_id,
                outcome, attempted, ok, queued_for_retry, reason_code,
                provider_message_id, occurred_at_us, payload_json
             ) SELECT 'forged-receipt', target_id, intent_id, target_key, 'webhook', NULL,
                      'permanent_failure', 0, 0, 0, 'forged', NULL, occurred_at_us + 1,
                      json_set(payload_json,
                          '$.receipt_id', 'forged-receipt',
                          '$.channel', 'webhook',
                          '$.outcome', 'dead_letter',
                          '$.error_code', 'forged')
               FROM delivery_receipts WHERE attempt_id = ?1",
            [&attempt_id],
        )
        .unwrap_err();
    assert!(
        mismatched_receipt
            .to_string()
            .contains("receipt_provenance_mismatch")
    );
}

#[test]
fn expired_owner_reacquires_with_a_new_generation_without_displacing_a_competitor() {
    let temp = TempDir::new().unwrap();
    let ledger = Ledger::open(temp.path().join("ledger.sqlite")).unwrap();
    let now = at(0);
    let mut first = ledger
        .acquire_owner(
            OwnerRole::Core,
            "core-owner-00000001",
            now,
            TimeDelta::seconds(5),
        )
        .unwrap();
    let first_generation = first.generation();

    ledger
        .refresh_owner(
            &mut first,
            now + TimeDelta::seconds(6),
            TimeDelta::seconds(5),
        )
        .unwrap();
    assert_eq!(first.generation(), first_generation + 1);

    ledger
        .acquire_owner(
            OwnerRole::Core,
            "competing-core-owner",
            now + TimeDelta::seconds(12),
            TimeDelta::seconds(5),
        )
        .unwrap();
    assert!(matches!(
        ledger.refresh_owner(
            &mut first,
            now + TimeDelta::seconds(13),
            TimeDelta::seconds(5),
        ),
        Err(LedgerError::OwnerLeaseLost(OwnerRole::Core))
    ));
}

#[test]
fn graceful_owner_release_allows_immediate_replacement() {
    let temp = TempDir::new().unwrap();
    let ledger = Ledger::open(temp.path().join("ledger.sqlite")).unwrap();
    let now = at(0);
    let first = ledger
        .acquire_owner(
            OwnerRole::Core,
            "core-owner-00000001",
            now,
            TimeDelta::seconds(30),
        )
        .unwrap();
    ledger.release_owner(&first).unwrap();
    let replacement = ledger
        .acquire_owner(
            OwnerRole::Core,
            "replacement-core-owner",
            now + TimeDelta::seconds(1),
            TimeDelta::seconds(30),
        )
        .unwrap();
    assert_eq!(replacement.generation(), first.generation() + 1);
    assert!(matches!(
        ledger.release_owner(&first),
        Err(LedgerError::OwnerLeaseLost(OwnerRole::Core))
    ));
}

#[test]
fn expired_owner_cannot_persist_by_reusing_an_old_decision_timestamp() {
    let temp = TempDir::new().unwrap();
    let ledger = Ledger::open(temp.path().join("ledger.sqlite")).unwrap();
    let now = at(0);
    let owner = ledger
        .acquire_owner(
            OwnerRole::Core,
            "core-owner-00000001",
            now,
            TimeDelta::seconds(5),
        )
        .unwrap();

    assert!(matches!(
        ledger.persist_decision(
            &owner,
            &manual_decision(now),
            Some(&intent(now)),
            now + TimeDelta::seconds(6),
        ),
        Err(LedgerError::OwnerLeaseLost(OwnerRole::Core))
    ));
    assert_eq!(ledger.health().unwrap(), LedgerHealth::default());
}

#[test]
fn duplicate_decision_and_targets_are_idempotent() {
    let temp = TempDir::new().unwrap();
    let ledger = Ledger::open(temp.path().join("ledger.sqlite")).unwrap();
    let now = at(0);
    let owner = ledger
        .acquire_owner(
            OwnerRole::Core,
            "core-owner-00000001",
            now,
            TimeDelta::seconds(30),
        )
        .unwrap();
    let mut decision = decision(now, true);
    decision.direction = Some(spx_domain::CandidateDirection::CallVertical10);
    decision.exact_legs = Some(spx_domain::ExactLegEvidenceV1 {
        provider: spx_domain::Provider::Schwab,
        long_contract_id: token("long"),
        short_contract_id: token("short"),
        right: spx_domain::OptionRight::Call,
        long_strike: spx_domain::PositiveF64::new(6000.0, "strike").unwrap(),
        short_strike: spx_domain::PositiveF64::new(6010.0, "strike").unwrap(),
        long_bid: spx_domain::PositiveF64::new(3.0, "bid").unwrap(),
        long_ask: spx_domain::PositiveF64::new(3.2, "ask").unwrap(),
        short_bid: spx_domain::PositiveF64::new(1.0, "bid").unwrap(),
        short_ask: spx_domain::PositiveF64::new(1.2, "ask").unwrap(),
        max_age_seconds: spx_domain::NonNegativeF64::new(0.5, "age").unwrap(),
        max_skew_seconds: spx_domain::NonNegativeF64::new(0.1, "skew").unwrap(),
        observed_at: now,
    });
    assert_eq!(
        ledger
            .persist_decision(&owner, &decision, Some(&intent(now)), now)
            .unwrap(),
        PersistWrite::Inserted
    );
    assert_eq!(
        ledger
            .persist_decision(&owner, &decision, Some(&intent(now)), now)
            .unwrap(),
        PersistWrite::Duplicate
    );
    assert_eq!(ledger.health().unwrap().pending, 2);
    let configured_attempts: i64 = ledger
        .connection()
        .unwrap()
        .query_row(
            "SELECT MIN(max_attempts) FROM notification_targets",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(configured_attempts, 3);
}

#[test]
fn scheduled_report_persists_full_v2_body_without_a_fake_decision() {
    let temp = TempDir::new().unwrap();
    let ledger = Ledger::open(temp.path().join("ledger.sqlite")).unwrap();
    let now = at(0);
    let core = ledger
        .acquire_owner(
            OwnerRole::Core,
            "core-owner-00000001",
            now,
            TimeDelta::minutes(30),
        )
        .unwrap();
    let owner = ledger
        .acquire_owner(
            OwnerRole::Report,
            "report-owner-00001",
            now,
            TimeDelta::minutes(30),
        )
        .unwrap();
    let delivery = ledger
        .acquire_owner(
            OwnerRole::Delivery,
            "delivery-owner-0001",
            now,
            TimeDelta::minutes(30),
        )
        .unwrap();
    assert_eq!(core.role(), OwnerRole::Core);
    assert_eq!(owner.role(), OwnerRole::Report);
    assert_eq!(delivery.role(), OwnerRole::Delivery);
    let report = scheduled_report(now);
    report.validate().unwrap();

    assert_eq!(
        ledger
            .persist_scheduled_report(&owner, &report, now)
            .unwrap(),
        PersistWrite::Inserted
    );
    assert!(matches!(
        ledger.persist_scheduled_report(&core, &report, now),
        Err(LedgerError::OwnerRoleMismatch {
            expected: OwnerRole::Report,
            actual: OwnerRole::Core
        })
    ));
    assert_eq!(
        ledger
            .persist_scheduled_report(&owner, &report, now)
            .unwrap(),
        PersistWrite::Duplicate
    );

    let connection = ledger.connection().unwrap();
    let (decision_id, projection_id, slot, lane, payload_json): (
        Option<String>,
        Option<String>,
        Option<String>,
        String,
        String,
    ) = connection
        .query_row(
            "SELECT decision_id, source_projection_id, report_slot, lane, payload_json
             FROM notification_events WHERE event_id = ?1",
            [report.intent_id.as_str()],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .unwrap();
    assert!(decision_id.is_none());
    assert_eq!(
        projection_id.as_deref(),
        Some("projection-2026-08-04-095959-et")
    );
    assert_eq!(slot.as_deref(), Some("2026-08-04:10:00"));
    assert_eq!(lane, "scheduled_report");
    assert_eq!(payload_json, serde_json::to_string(&report).unwrap());
    let persisted: NotificationIntentV2 = serde_json::from_str(&payload_json).unwrap();
    assert_eq!(persisted, report);
    assert!(persisted.message.primary_path.as_str().len() > 3_000);
    assert_eq!(ledger.health().unwrap().pending, 1);

    let claimed = ledger
        .claim_next(&delivery, now, TimeDelta::seconds(10))
        .unwrap()
        .unwrap();
    assert!(matches!(
        claimed.intent,
        ClaimedNotificationIntent::ScheduledReport(_)
    ));
}

#[test]
fn fresh_ledger_installs_both_forward_migrations() {
    let temp = TempDir::new().unwrap();
    let ledger = Ledger::open(temp.path().join("ledger.sqlite")).unwrap();
    let migration_count: i64 = ledger
        .connection()
        .unwrap()
        .query_row("SELECT COUNT(*) FROM schema_migrations", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(migration_count, 2);
}

#[test]
fn scheduled_report_semantic_id_and_et_slot_are_collision_safe() {
    let temp = TempDir::new().unwrap();
    let ledger = Ledger::open(temp.path().join("ledger.sqlite")).unwrap();
    let now = at(0);
    let owner = ledger
        .acquire_owner(
            OwnerRole::Report,
            "report-owner-00001",
            now,
            TimeDelta::minutes(30),
        )
        .unwrap();
    let report = scheduled_report(now);
    assert!(
        !ledger
            .scheduled_report_exists(&owner, report.lineage.slot().unwrap(), now)
            .unwrap()
    );
    ledger
        .persist_scheduled_report(&owner, &report, now)
        .unwrap();
    assert!(
        ledger
            .scheduled_report_exists(&owner, report.lineage.slot().unwrap(), now)
            .unwrap()
    );

    let mut same_slot = report.clone();
    same_slot.intent_id = token("different-event-same-slot");
    same_slot.semantic_id = token("different-semantic-same-slot");
    assert!(matches!(
        ledger.persist_scheduled_report(&owner, &same_slot, now),
        Err(LedgerError::IdentityCollision(_))
    ));

    let mut same_semantic = report;
    same_semantic.intent_id = token("different-event-same-semantic");
    same_semantic.lineage = NotificationLineageV2::ScheduledReport {
        source_projection_id: token("another-projection"),
        slot: token("2026-08-04:10:30"),
    };
    assert!(matches!(
        ledger.persist_scheduled_report(&owner, &same_semantic, now),
        Err(LedgerError::IdentityCollision(_))
    ));
    let event_count: i64 = ledger
        .connection()
        .unwrap()
        .query_row("SELECT COUNT(*) FROM notification_events", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(event_count, 1);
}

#[test]
fn v2_lineage_rejects_missing_lane_specific_source_ids() {
    let report = scheduled_report(at(0));
    let mut value = serde_json::to_value(&report).unwrap();
    value["lineage"]
        .as_object_mut()
        .unwrap()
        .remove("source_projection_id");
    assert!(serde_json::from_value::<NotificationIntentV2>(value).is_err());

    let mut value = serde_json::to_value(&report).unwrap();
    value["lineage"]["lane"] = serde_json::json!("trade_ready");
    value["lineage"]
        .as_object_mut()
        .unwrap()
        .remove("source_projection_id");
    value["lineage"].as_object_mut().unwrap().remove("slot");
    assert!(serde_json::from_value::<NotificationIntentV2>(value).is_err());

    let trade_ready = NotificationLineageV2::TradeReady {
        decision_id: token("decision-manual"),
    };
    assert_eq!(trade_ready.lane(), "trade_ready");
    assert_eq!(
        trade_ready.decision_id().map(Token::as_str),
        Some("decision-manual")
    );
    assert!(trade_ready.source_projection_id().is_none());
    assert!(trade_ready.slot().is_none());
}

#[test]
fn sqlite_rejects_lane_source_mismatches_even_when_api_is_bypassed() {
    let temp = TempDir::new().unwrap();
    let ledger = Ledger::open(temp.path().join("ledger.sqlite")).unwrap();
    let connection = ledger.connection().unwrap();
    let trade_ready_error = connection
        .execute(
            "INSERT INTO notification_events (
                event_id, semantic_id, decision_id, source_projection_id, report_slot, lane,
                occurred_at_us, expires_at_us, payload_json, payload_sha256, target_set_sha256,
                writer_generation, created_at_us
             ) VALUES (
                'invalid-trade-event', 'invalid-trade-semantic', NULL, NULL, NULL,
                'trade_ready', 1, 2, '{}', ?1, ?2, 1, 1
             )",
            rusqlite::params!["0".repeat(64), "1".repeat(64)],
        )
        .unwrap_err();
    assert!(trade_ready_error.to_string().contains("CHECK"));

    let mut report = scheduled_report(at(0));
    report.intent_id = token("invalid-report-event");
    report.semantic_id = token("invalid-report-semantic");
    let payload_json = serde_json::to_string(&report).unwrap();
    let payload_hash = canonical_json_hash(&report).unwrap();
    let scheduled_error = connection
        .execute(
            "INSERT INTO notification_events (
                event_id, semantic_id, decision_id, source_projection_id, report_slot, lane,
                occurred_at_us, expires_at_us, payload_json, payload_sha256, target_set_sha256,
                writer_generation, created_at_us
             ) VALUES (
                ?1, ?2, NULL, NULL, ?3, 'scheduled_report', 1, 2, ?4, ?5, ?6, 1, 1
             )",
            rusqlite::params![
                report.intent_id.as_str(),
                report.semantic_id.as_str(),
                report.lineage.slot().unwrap().as_str(),
                payload_json,
                payload_hash,
                "2".repeat(64)
            ],
        )
        .unwrap_err();
    assert!(scheduled_error.to_string().contains("CHECK"));
}

#[test]
fn intent_lifetime_cannot_outlive_its_decision() {
    let temp = TempDir::new().unwrap();
    let ledger = Ledger::open(temp.path().join("ledger.sqlite")).unwrap();
    let now = at(0);
    let owner = ledger
        .acquire_owner(
            OwnerRole::Core,
            "core-owner-00000001",
            now,
            TimeDelta::seconds(30),
        )
        .unwrap();
    let decision = manual_decision(now);
    let mut invalid_intent = intent(now);
    invalid_intent.expires_at = decision.valid_until + TimeDelta::seconds(1);
    assert!(matches!(
        ledger.persist_decision(&owner, &decision, Some(&invalid_intent), now),
        Err(LedgerError::InvalidValue(
            "intent lifetime must be contained by decision lifetime"
        ))
    ));
}

#[test]
fn sqlite_rejects_exhausted_pending_target() {
    let temp = TempDir::new().unwrap();
    let ledger = Ledger::open(temp.path().join("ledger.sqlite")).unwrap();
    let now = at(0);
    seed_manual_notification(&ledger, now);
    let error = ledger
        .connection()
        .unwrap()
        .execute(
            "UPDATE notification_targets SET attempt_count = max_attempts
             WHERE status = 'pending'",
            [],
        )
        .unwrap_err();
    assert!(error.to_string().contains("CHECK"));
}

#[test]
fn stale_claim_after_transport_start_becomes_uncertain() {
    let temp = TempDir::new().unwrap();
    let ledger = Ledger::open(temp.path().join("ledger.sqlite")).unwrap();
    let now = at(0);
    let core = ledger
        .acquire_owner(
            OwnerRole::Core,
            "core-owner-00000001",
            now,
            TimeDelta::seconds(60),
        )
        .unwrap();
    let mut decision = decision(now, true);
    decision.direction = Some(spx_domain::CandidateDirection::CallVertical10);
    decision.exact_legs = Some(spx_domain::ExactLegEvidenceV1 {
        provider: spx_domain::Provider::Schwab,
        long_contract_id: token("long"),
        short_contract_id: token("short"),
        right: spx_domain::OptionRight::Call,
        long_strike: spx_domain::PositiveF64::new(6000.0, "strike").unwrap(),
        short_strike: spx_domain::PositiveF64::new(6010.0, "strike").unwrap(),
        long_bid: spx_domain::PositiveF64::new(3.0, "bid").unwrap(),
        long_ask: spx_domain::PositiveF64::new(3.2, "ask").unwrap(),
        short_bid: spx_domain::PositiveF64::new(1.0, "bid").unwrap(),
        short_ask: spx_domain::PositiveF64::new(1.2, "ask").unwrap(),
        max_age_seconds: spx_domain::NonNegativeF64::new(0.5, "age").unwrap(),
        max_skew_seconds: spx_domain::NonNegativeF64::new(0.1, "skew").unwrap(),
        observed_at: now,
    });
    ledger
        .persist_decision(&core, &decision, Some(&intent(now)), now)
        .unwrap();
    let delivery = ledger
        .acquire_owner(
            OwnerRole::Delivery,
            "delivery-owner-0001",
            now,
            TimeDelta::seconds(60),
        )
        .unwrap();
    let claimed = ledger
        .claim_next(&delivery, now, TimeDelta::seconds(2))
        .unwrap()
        .unwrap();
    match ledger
        .begin_transport(
            &delivery,
            &claimed,
            now + TimeDelta::seconds(1),
            TimeDelta::seconds(2),
        )
        .unwrap()
    {
        BeginTransport::Started { .. } => {}
        BeginTransport::Cancelled | BeginTransport::Expired => {
            panic!("seeded live claim must enter transport")
        }
    }
    assert_eq!(ledger.health().unwrap().in_flight, 1);
    let recovered = ledger
        .recover_stale_claims(&delivery, now + TimeDelta::seconds(3))
        .unwrap();
    assert_eq!(recovered.uncertain, 1);
    assert_eq!(ledger.health().unwrap().uncertain, 1);
}

#[test]
fn begin_transport_renews_the_lease_for_the_transport_window() {
    let temp = TempDir::new().unwrap();
    let ledger = Ledger::open(temp.path().join("ledger.sqlite")).unwrap();
    let now = at(0);
    let (_, delivery) = seed_manual_notification(&ledger, now);
    let claimed = ledger
        .claim_next(&delivery, now, TimeDelta::seconds(5))
        .unwrap()
        .unwrap();
    let begin_at = now + TimeDelta::seconds(4);
    let attempt_id = match ledger
        .begin_transport(&delivery, &claimed, begin_at, TimeDelta::seconds(5))
        .unwrap()
    {
        BeginTransport::Started { attempt_id } => attempt_id,
        BeginTransport::Cancelled | BeginTransport::Expired => {
            panic!("seeded live claim must enter transport")
        }
    };

    let lease_until_us: i64 = ledger
        .connection()
        .unwrap()
        .query_row(
            "SELECT lease_until_us FROM notification_targets WHERE target_id = ?1",
            [claimed.handle.target_id()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        lease_until_us,
        (begin_at + TimeDelta::seconds(5)).timestamp_micros()
    );
    assert_eq!(
        ledger
            .recover_stale_claims(&delivery, now + TimeDelta::seconds(6))
            .unwrap(),
        RecoverySummary::default()
    );
    assert_eq!(
        ledger
            .settle(
                &delivery,
                &claimed.handle,
                &attempt_id,
                &Settlement::Delivered {
                    provider_message_id: None,
                },
                now + TimeDelta::seconds(8),
            )
            .unwrap(),
        SettlementWrite::Delivered
    );
}

#[test]
fn cancellation_after_claim_is_a_normal_begin_transport_outcome() {
    let temp = TempDir::new().unwrap();
    let ledger = Ledger::open(temp.path().join("ledger.sqlite")).unwrap();
    let now = at(0);
    let (core, delivery) = seed_manual_notification(&ledger, now);
    let claimed = ledger
        .claim_next(&delivery, now, TimeDelta::seconds(10))
        .unwrap()
        .unwrap();
    ledger
        .cancel_event(
            &core,
            "intent-manual",
            "source_cancelled",
            now + TimeDelta::seconds(1),
        )
        .unwrap();

    assert_eq!(
        ledger
            .begin_transport(
                &delivery,
                &claimed,
                now + TimeDelta::seconds(2),
                TimeDelta::seconds(10),
            )
            .unwrap(),
        BeginTransport::Cancelled
    );
}

#[test]
fn expiry_after_claim_is_a_normal_outcome_after_the_claim_lease_elapsed() {
    let temp = TempDir::new().unwrap();
    let ledger = Ledger::open(temp.path().join("ledger.sqlite")).unwrap();
    let now = at(0);
    let (_, delivery) = seed_manual_notification(&ledger, now);
    let claimed = ledger
        .claim_next(&delivery, now, TimeDelta::seconds(2))
        .unwrap()
        .unwrap();

    assert_eq!(
        ledger
            .begin_transport(
                &delivery,
                &claimed,
                now + TimeDelta::seconds(31),
                TimeDelta::seconds(10),
            )
            .unwrap(),
        BeginTransport::Expired
    );
}

#[test]
fn stale_claim_before_transport_does_not_consume_attempt() {
    let temp = TempDir::new().unwrap();
    let ledger = Ledger::open(temp.path().join("ledger.sqlite")).unwrap();
    let now = at(0);
    let (_, delivery) = seed_manual_notification(&ledger, now);
    let first_claim = ledger
        .claim_next(&delivery, now, TimeDelta::seconds(2))
        .unwrap()
        .unwrap();
    assert_eq!(first_claim.attempt_no, 1);

    let recovered = ledger
        .recover_stale_claims(&delivery, now + TimeDelta::seconds(3))
        .unwrap();
    assert_eq!(recovered.requeued, 1);
    let second_claim = ledger
        .claim_next(
            &delivery,
            now + TimeDelta::seconds(3),
            TimeDelta::seconds(2),
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        second_claim.handle.target_id(),
        first_claim.handle.target_id()
    );
    assert_eq!(second_claim.attempt_no, 1);
}

#[test]
fn cancellation_does_not_rewrite_an_irreversible_in_flight_attempt() {
    let temp = TempDir::new().unwrap();
    let ledger = Ledger::open(temp.path().join("ledger.sqlite")).unwrap();
    let now = at(0);
    let (core, delivery) = seed_manual_notification(&ledger, now);
    let claimed = ledger
        .claim_next(&delivery, now, TimeDelta::seconds(10))
        .unwrap()
        .unwrap();
    let attempt_id = start_transport(&ledger, &delivery, &claimed, now + TimeDelta::seconds(1));

    ledger
        .cancel_event(
            &core,
            "intent-manual",
            "source_cancelled",
            now + TimeDelta::seconds(2),
        )
        .unwrap();
    let health = ledger.health().unwrap();
    assert_eq!(health.in_flight, 1);
    assert_eq!(health.cancelled, 1);
    ledger
        .settle(
            &delivery,
            &claimed.handle,
            &attempt_id,
            &Settlement::Delivered {
                provider_message_id: None,
            },
            now + TimeDelta::seconds(3),
        )
        .unwrap();
    let health = ledger.health().unwrap();
    assert_eq!(health.in_flight, 0);
    assert_eq!(health.delivered, 1);
    assert_eq!(health.cancelled, 1);
    assert_eq!(health.uncertain, 0);
}

#[test]
fn retry_exhaustion_preserves_retryable_transport_evidence() {
    let temp = TempDir::new().unwrap();
    let ledger = Ledger::open(temp.path().join("ledger.sqlite")).unwrap();
    let now = at(0);
    let core = ledger
        .acquire_owner(
            OwnerRole::Core,
            "core-owner-00000001",
            now,
            TimeDelta::seconds(60),
        )
        .unwrap();
    let mut one_attempt = intent(now);
    one_attempt.max_attempts = 1;
    ledger
        .persist_decision(&core, &manual_decision(now), Some(&one_attempt), now)
        .unwrap();
    let delivery = ledger
        .acquire_owner(
            OwnerRole::Delivery,
            "delivery-owner-0001",
            now,
            TimeDelta::seconds(60),
        )
        .unwrap();
    let claimed = ledger
        .claim_next(&delivery, now, TimeDelta::seconds(10))
        .unwrap()
        .unwrap();
    let attempt_id = start_transport(&ledger, &delivery, &claimed, now + TimeDelta::seconds(1));
    let disposition = ledger
        .settle(
            &delivery,
            &claimed.handle,
            &attempt_id,
            &Settlement::Retryable {
                error_code: "http_503".to_owned(),
                retry_at: now + TimeDelta::seconds(10),
            },
            now + TimeDelta::seconds(2),
        )
        .unwrap();
    assert_eq!(disposition, SettlementWrite::DeadLetter);
    let receipt: (String, bool) = ledger
        .connection()
        .unwrap()
        .query_row(
            "SELECT outcome, queued_for_retry FROM delivery_receipts WHERE attempt_id = ?1",
            [&attempt_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(receipt, ("retry_exhausted".to_owned(), false));
}

#[test]
fn settlement_requires_the_matching_attempt() {
    let temp = TempDir::new().unwrap();
    let ledger = Ledger::open(temp.path().join("ledger.sqlite")).unwrap();
    let now = at(0);
    let (_, delivery) = seed_manual_notification(&ledger, now);
    let claimed = ledger
        .claim_next(&delivery, now, TimeDelta::seconds(10))
        .unwrap()
        .unwrap();
    let attempt_id = start_transport(&ledger, &delivery, &claimed, now + TimeDelta::seconds(1));
    assert!(matches!(
        ledger.settle(
            &delivery,
            &claimed.handle,
            "wrong-attempt-id",
            &Settlement::Delivered {
                provider_message_id: None,
            },
            now + TimeDelta::seconds(2),
        ),
        Err(LedgerError::ClaimLost(_))
    ));
    ledger
        .settle(
            &delivery,
            &claimed.handle,
            &attempt_id,
            &Settlement::Delivered {
                provider_message_id: Some("provider-message-1".to_owned()),
            },
            now + TimeDelta::seconds(2),
        )
        .unwrap();
    let connection = ledger.connection().unwrap();
    let provider_message_id: Option<String> = connection
        .query_row(
            "SELECT provider_message_id FROM delivery_receipts WHERE attempt_id = ?1",
            [&attempt_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(provider_message_id.as_deref(), Some("provider-message-1"));
    let (channel, payload_json): (String, String) = connection
        .query_row(
            "SELECT channel, payload_json FROM delivery_receipts WHERE attempt_id = ?1",
            [&attempt_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let wire: spx_domain::DeliveryReceiptV1 = serde_json::from_str(&payload_json).unwrap();
    assert_eq!(channel, "bark");
    assert_eq!(wire.channel, DeliveryChannel::Bark);
    assert_eq!(wire.outcome, spx_domain::ReceiptOutcome::Delivered);
}

#[test]
fn append_only_delivery_evidence_rejects_bypass_updates_and_deletes() {
    let temp = TempDir::new().unwrap();
    let ledger = Ledger::open(temp.path().join("ledger.sqlite")).unwrap();
    let now = at(0);
    let (_, delivery) = seed_manual_notification(&ledger, now);
    let claimed = ledger
        .claim_next(&delivery, now, TimeDelta::seconds(10))
        .unwrap()
        .unwrap();
    let attempt_id = start_transport(&ledger, &delivery, &claimed, now + TimeDelta::seconds(1));
    ledger
        .settle(
            &delivery,
            &claimed.handle,
            &attempt_id,
            &Settlement::Delivered {
                provider_message_id: Some("provider-message-immutable".to_owned()),
            },
            now + TimeDelta::seconds(2),
        )
        .unwrap();

    let connection = ledger.connection().unwrap();
    let receipt_id: String = connection
        .query_row(
            "SELECT receipt_id FROM delivery_receipts WHERE attempt_id = ?1",
            [&attempt_id],
            |row| row.get(0),
        )
        .unwrap();
    let receipt_error = connection
        .execute(
            "UPDATE delivery_receipts SET reason_code = 'tampered' WHERE receipt_id = ?1",
            [&receipt_id],
        )
        .unwrap_err();
    assert!(receipt_error.to_string().contains("receipt_immutable"));
    let attempt_error = connection
        .execute(
            "UPDATE delivery_attempts SET idempotency_key = 'tampered' WHERE attempt_id = ?1",
            [&attempt_id],
        )
        .unwrap_err();
    assert!(attempt_error.to_string().contains("attempt_immutable"));
    let target_error = connection
        .execute(
            "UPDATE notification_targets SET channel = 'webhook' WHERE target_id = ?1",
            [claimed.handle.target_id()],
        )
        .unwrap_err();
    assert!(
        target_error
            .to_string()
            .contains("target_identity_immutable")
    );
    let delete_error = connection
        .execute(
            "DELETE FROM delivery_receipts WHERE receipt_id = ?1",
            [&receipt_id],
        )
        .unwrap_err();
    assert!(delete_error.to_string().contains("receipt_immutable"));
}

#[test]
fn expiry_creates_an_immutable_receipt_per_target() {
    let temp = TempDir::new().unwrap();
    let ledger = Ledger::open(temp.path().join("ledger.sqlite")).unwrap();
    let now = at(0);
    let (_, delivery) = seed_manual_notification(&ledger, now);
    assert!(
        ledger
            .claim_next(
                &delivery,
                now + TimeDelta::seconds(31),
                TimeDelta::seconds(2),
            )
            .unwrap()
            .is_none()
    );
    assert_eq!(ledger.health().unwrap().expired, 2);
    let connection = ledger.connection().unwrap();
    let receipts: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM delivery_receipts
             WHERE outcome = 'expired_before_transport' AND attempted = 0",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(receipts, 2);
}

#[test]
fn migration_checksum_drift_refuses_to_open() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("ledger.sqlite");
    let ledger = Ledger::open(&path).unwrap();
    ledger
        .connection()
        .unwrap()
        .execute(
            "UPDATE schema_migrations SET checksum_sha256 = ?1 WHERE version = 1",
            ["0".repeat(64)],
        )
        .unwrap();
    assert!(matches!(
        Ledger::open(path),
        Err(LedgerError::MigrationDrift)
    ));
}

#[test]
fn known_migration_prefix_accepts_a_forward_compatible_tail() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("ledger.sqlite");
    let ledger = Ledger::open(&path).unwrap();
    ledger
        .connection()
        .unwrap()
        .execute(
            "INSERT INTO schema_migrations (
                version, name, checksum_sha256, applied_at_us
             ) VALUES (3, 'future_backward_compatible_extension', ?1, ?2)",
            rusqlite::params!["f".repeat(64), at(1).timestamp_micros()],
        )
        .unwrap();

    Ledger::open(&path).unwrap();
    LedgerReader::open_existing(&path).unwrap();
}

#[test]
fn v1_ledger_upgrades_to_v2_without_losing_existing_outbox_rows() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("ledger.sqlite");
    create_v1_ledger_with_outbox(&path);

    let ledger = Ledger::open(&path).unwrap();
    let connection = ledger.connection().unwrap();
    let migrations: i64 = connection
        .query_row("SELECT COUNT(*) FROM schema_migrations", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(migrations, 2);
    let lineage: (Option<String>, Option<String>, Option<String>) = connection
        .query_row(
            "SELECT decision_id, source_projection_id, report_slot
             FROM notification_events WHERE event_id = 'legacy-event'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(lineage.0.as_deref(), Some("legacy-decision"));
    assert!(lineage.1.is_none());
    assert!(lineage.2.is_none());
    let preserved_target: i64 = connection
        .query_row(
            "SELECT COUNT(*)
             FROM notification_targets t
             JOIN notification_events e ON e.event_id = t.event_id
             WHERE t.target_id = 'legacy-target' AND e.event_id = 'legacy-event'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(preserved_target, 1);
    let foreign_key_violations: i64 = connection
        .query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(foreign_key_violations, 0);
    LedgerReader::open_existing(path).unwrap();
}

#[test]
fn read_only_health_refuses_missing_path_without_creating_it() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("missing/ledger.sqlite");
    assert!(LedgerReader::open_existing(&path).is_err());
    assert!(!path.exists());
    assert!(!path.parent().unwrap().exists());
}

#[test]
fn read_only_health_opens_only_an_initialized_ledger() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("ledger.sqlite");
    Ledger::open(&path).unwrap();
    let reader = LedgerReader::open_existing(&path).unwrap();
    reader.quick_check().unwrap();
    assert_eq!(reader.health().unwrap(), LedgerHealth::default());
}

#[test]
fn operator_acknowledgement_and_replay_preserve_failure_history() {
    let temp = TempDir::new().unwrap();
    let ledger = Ledger::open(temp.path().join("ledger.sqlite")).unwrap();
    let now = at(0);
    let (_, delivery) = seed_manual_notification(&ledger, now);
    let claimed = ledger
        .claim_next(&delivery, now, TimeDelta::seconds(10))
        .unwrap()
        .unwrap();
    let target_id = claimed.handle.target_id().to_owned();
    let attempt_id = start_transport(&ledger, &delivery, &claimed, now + TimeDelta::seconds(1));
    ledger
        .settle(
            &delivery,
            &claimed.handle,
            &attempt_id,
            &Settlement::TransportUncertain {
                error_code: "connection_interrupted".to_owned(),
            },
            now + TimeDelta::seconds(2),
        )
        .unwrap();
    assert_eq!(ledger.health().unwrap().unacknowledged_failures, 1);
    assert_eq!(
        ledger
            .acknowledge_failure(
                &target_id,
                "operator-test",
                "reviewed_transport_log",
                now + TimeDelta::seconds(3),
            )
            .unwrap(),
        OperatorWrite::Applied
    );
    assert_eq!(
        ledger
            .acknowledge_failure(
                &target_id,
                "operator-test",
                "reviewed_transport_log",
                now + TimeDelta::seconds(3),
            )
            .unwrap(),
        OperatorWrite::AlreadyAcknowledged
    );
    assert_eq!(ledger.health().unwrap().unacknowledged_failures, 0);

    ledger
        .replay_failure(
            &target_id,
            "operator-test",
            "sink_confirmed_not_delivered",
            now + TimeDelta::seconds(4),
        )
        .unwrap();
    let connection = ledger.connection().unwrap();
    let target: (String, i64, i64) = connection
        .query_row(
            "SELECT status, replay_generation, attempt_count
             FROM notification_targets WHERE target_id = ?1",
            [target_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(target, ("pending".to_owned(), 1, 0));
    let history: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM delivery_attempts a
             JOIN delivery_receipts r ON r.attempt_id = a.attempt_id",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(history, 1);
}
