use chrono::{DateTime, TimeDelta, TimeZone, Utc};
use spx_domain::{
    DECISION_SCHEMA_VERSION, DeliveryChannel, DeskMessageV1, NOTIFICATION_INTENT_SCHEMA_VERSION,
    NotificationIntentV1, NotificationTargetV1, StrategyAction, StrategyBlockReason,
    StrategyDecisionV1, Token,
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
    match ledger.begin_transport(delivery, claimed, now).unwrap() {
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
    start_transport(&ledger, &delivery, &claimed, now + TimeDelta::seconds(1));
    assert_eq!(ledger.health().unwrap().in_flight, 1);
    let recovered = ledger
        .recover_stale_claims(&delivery, now + TimeDelta::seconds(3))
        .unwrap();
    assert_eq!(recovered.uncertain, 1);
    assert_eq!(ledger.health().unwrap().uncertain, 1);
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
