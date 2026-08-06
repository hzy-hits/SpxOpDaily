use chrono::{DateTime, Utc};
use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};
use spx_domain::{
    NotificationIntentV1, NotificationIntentV2, NotificationLineageV2, NotificationTargetV1,
    OperatorNotificationRole, OperatorNotificationV1, StrategyAction, StrategyDecisionV1, Token,
    Validate, canonical_json_hash,
};

use crate::db::micros;
use crate::receipt::{Receipt, insert_receipt};
use crate::{
    IngressCheck, IngressWrite, Ledger, LedgerError, OperatorNotificationWrite, OwnerLease,
    OwnerRole, PersistWrite,
};

impl Ledger {
    /// Records successful ingress processing with collision-safe idempotency.
    ///
    /// # Errors
    ///
    /// Returns an error for lost ownership, identity collision, or storage failure.
    pub fn record_ingress_once(
        &self,
        lease: &OwnerLease,
        message_id: &Token,
        payload_sha256: &str,
        observed_at: DateTime<Utc>,
    ) -> Result<IngressWrite, LedgerError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::require_owner_in_transaction(&transaction, lease, OwnerRole::Core, observed_at)?;
        let inserted = transaction.execute(
            "INSERT OR IGNORE INTO ingress_messages (
                message_id, payload_sha256, observed_at_us, writer_generation
             ) VALUES (?1, ?2, ?3, ?4)",
            params![
                message_id.as_str(),
                payload_sha256,
                micros(observed_at),
                lease.generation
            ],
        )?;
        let outcome = if inserted == 1 {
            IngressWrite::Inserted
        } else {
            let existing: String = transaction.query_row(
                "SELECT payload_sha256 FROM ingress_messages WHERE message_id = ?1",
                [message_id.as_str()],
                |row| row.get(0),
            )?;
            if existing != payload_sha256 {
                return Err(LedgerError::IdentityCollision(message_id.to_string()));
            }
            IngressWrite::Duplicate
        };
        transaction.commit()?;
        Ok(outcome)
    }

    /// Checks whether an ingress identity is new, an exact duplicate, or a collision.
    ///
    /// # Errors
    ///
    /// Returns an error for lost ownership, identity collision, or storage failure.
    pub fn check_ingress(
        &self,
        lease: &OwnerLease,
        message_id: &Token,
        payload_sha256: &str,
        now: DateTime<Utc>,
    ) -> Result<IngressCheck, LedgerError> {
        self.require_owner(lease, OwnerRole::Core, now)?;
        let connection = self.connection()?;
        let existing = connection
            .query_row(
                "SELECT payload_sha256 FROM ingress_messages WHERE message_id = ?1",
                [message_id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        match existing {
            None => Ok(IngressCheck::New),
            Some(existing) if existing == payload_sha256 => Ok(IngressCheck::Duplicate),
            Some(_) => Err(LedgerError::IdentityCollision(message_id.to_string())),
        }
    }

    /// Atomically stores a decision and its optional notification targets.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid contracts, lost ownership, identity collision, or storage
    /// failure.
    pub fn persist_decision(
        &self,
        lease: &OwnerLease,
        decision: &StrategyDecisionV1,
        intent: Option<&NotificationIntentV1>,
        now: DateTime<Utc>,
    ) -> Result<PersistWrite, LedgerError> {
        decision.validate()?;
        if let Some(intent) = intent {
            intent.validate()?;
            if intent.decision_id != decision.decision_id {
                return Err(LedgerError::InvalidValue("intent decision id"));
            }
            if decision.action != StrategyAction::ManualCandidate {
                return Err(LedgerError::InvalidValue("NO_TRADE notification intent"));
            }
            if intent.created_at != decision.evaluated_at
                || intent.expires_at > decision.valid_until
            {
                return Err(LedgerError::InvalidValue(
                    "intent lifetime must be contained by decision lifetime",
                ));
            }
        }
        let decision_json = serde_json::to_string(decision)?;
        let decision_hash = canonical_json_hash(decision)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::require_owner_in_transaction(&transaction, lease, OwnerRole::Core, now)?;
        let inserted = transaction.execute(
            "INSERT OR IGNORE INTO decisions (
                decision_id, request_id, action, policy_version, snapshot_id,
                evaluated_at_us, valid_until_us, payload_json, payload_sha256,
                writer_generation, created_at_us
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                decision.decision_id.as_str(),
                decision.request_id.as_str(),
                action_name(decision.action),
                decision.policy_version.as_str(),
                decision.snapshot_id.as_str(),
                micros(decision.evaluated_at),
                micros(decision.valid_until),
                decision_json,
                decision_hash,
                lease.generation,
                micros(now)
            ],
        )?;
        if inserted == 0 {
            verify_hash(
                &transaction,
                "decisions",
                "decision_id",
                decision.decision_id.as_str(),
                &decision_hash,
            )?;
        }
        if let Some(intent) = intent {
            Self::insert_intent(&transaction, lease, intent, now)?;
        }
        transaction.commit()?;
        Ok(if inserted == 1 {
            PersistWrite::Inserted
        } else {
            PersistWrite::Duplicate
        })
    }

    /// Atomically stores one independent scheduled desk report and its outbox targets.
    ///
    /// The report is linked to the source projection and stable ET slot carried by its typed
    /// lineage. It never creates or references a synthetic strategy decision.
    ///
    /// # Errors
    ///
    /// Returns an error for a non-scheduled V2 intent, an invalid contract, lost ownership,
    /// identity or slot collision, or storage failure.
    pub fn persist_scheduled_report(
        &self,
        lease: &OwnerLease,
        intent: &NotificationIntentV2,
        now: DateTime<Utc>,
    ) -> Result<PersistWrite, LedgerError> {
        intent.validate()?;
        let NotificationLineageV2::ScheduledReport {
            source_projection_id,
            slot,
        } = &intent.lineage
        else {
            return Err(LedgerError::InvalidValue(
                "scheduled report requires scheduled lineage",
            ));
        };

        let payload_json = serde_json::to_string(intent)?;
        let payload_hash = canonical_json_hash(intent)?;
        let targets = sorted_targets(&intent.targets);
        let target_hash = canonical_json_hash(&targets)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::require_owner_in_transaction(&transaction, lease, OwnerRole::Report, now)?;

        let existing = matching_scheduled_reports(&transaction, intent, slot)?;

        let outcome = match existing.as_slice() {
            [] => {
                transaction.execute(
                    "INSERT INTO notification_events (
                        event_id, semantic_id, decision_id, source_projection_id, report_slot,
                        lane, occurred_at_us, expires_at_us, payload_json, payload_sha256,
                        target_set_sha256, writer_generation, created_at_us
                     ) VALUES (?1, ?2, NULL, ?3, ?4, 'scheduled_report', ?5, ?6, ?7, ?8,
                        ?9, ?10, ?11)",
                    params![
                        intent.intent_id.as_str(),
                        intent.semantic_id.as_str(),
                        source_projection_id.as_str(),
                        slot.as_str(),
                        micros(intent.created_at),
                        micros(intent.expires_at),
                        payload_json,
                        payload_hash,
                        target_hash,
                        lease.generation,
                        micros(now)
                    ],
                )?;
                insert_targets(
                    &transaction,
                    intent.intent_id.as_str(),
                    &targets,
                    intent.max_attempts,
                    intent.created_at,
                )?;
                PersistWrite::Inserted
            }
            [stored]
                if stored.matches_exact(
                    intent,
                    source_projection_id,
                    slot,
                    &payload_hash,
                    &target_hash,
                ) =>
            {
                insert_targets(
                    &transaction,
                    intent.intent_id.as_str(),
                    &targets,
                    intent.max_attempts,
                    intent.created_at,
                )?;
                PersistWrite::Duplicate
            }
            _ => {
                return Err(LedgerError::IdentityCollision(format!(
                    "scheduled_report:{}/{}",
                    intent.semantic_id, slot
                )));
            }
        };
        transaction.commit()?;
        Ok(outcome)
    }

    /// Atomically stores one operator lifecycle event and its explicitly configured targets.
    ///
    /// This lane is independent of strategy decisions and cannot carry broker authority. A READY
    /// or EXIT transition atomically supersedes older pending or claimed targets for the same
    /// opportunity generation. An in-flight transport has crossed the irreversible boundary and
    /// cannot be recalled by a later transition.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid contract, attempt bound, lost ownership, identity
    /// collision, or storage failure.
    pub fn persist_operator_notification(
        &self,
        lease: &OwnerLease,
        notification: &OperatorNotificationV1,
        max_attempts: u32,
        now: DateTime<Utc>,
    ) -> Result<OperatorNotificationWrite, LedgerError> {
        notification.validate()?;
        if !(1..=10).contains(&max_attempts) {
            return Err(LedgerError::InvalidValue("max_attempts"));
        }

        let payload_json = serde_json::to_string(notification)?;
        let payload_hash = canonical_json_hash(notification)?;
        let targets = sorted_targets(&notification.targets);
        let target_hash = canonical_json_hash(&targets)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::require_owner_in_transaction(&transaction, lease, OwnerRole::Core, now)?;
        let existing = matching_operator_notifications(&transaction, notification)?;

        let outcome = match existing.as_slice() {
            [] => {
                if enforce_operator_lifecycle(&transaction, notification)?
                    == OperatorLifecycleDecision::Suppress
                {
                    transaction.commit()?;
                    return Ok(OperatorNotificationWrite::SemanticSuppressed);
                }
                supersede_prior_operator_targets(&transaction, notification, now)?;
                transaction.execute(
                    "INSERT INTO notification_events (
                        event_id, semantic_id, decision_id, source_projection_id, report_slot,
                        lane, occurred_at_us, expires_at_us, payload_json, payload_sha256,
                        target_set_sha256, writer_generation, created_at_us
                     ) VALUES (?1, ?2, NULL, NULL, NULL, 'trader_event', ?3, ?4, ?5, ?6,
                        ?7, ?8, ?9)",
                    params![
                        notification.event_id.as_str(),
                        notification.semantic_id.as_str(),
                        micros(notification.occurred_at),
                        micros(notification.expires_at),
                        payload_json,
                        payload_hash,
                        target_hash,
                        lease.generation,
                        micros(now)
                    ],
                )?;
                insert_targets(
                    &transaction,
                    notification.event_id.as_str(),
                    &targets,
                    max_attempts,
                    notification.occurred_at,
                )?;
                OperatorNotificationWrite::Inserted
            }
            [stored] if stored.matches_exact(notification, &payload_hash, &target_hash) => {
                insert_targets(
                    &transaction,
                    notification.event_id.as_str(),
                    &targets,
                    max_attempts,
                    notification.occurred_at,
                )?;
                OperatorNotificationWrite::Duplicate
            }
            _ => {
                return Err(LedgerError::IdentityCollision(format!(
                    "trader_event:{}/{}",
                    notification.event_id, notification.semantic_id
                )));
            }
        };
        transaction.commit()?;
        Ok(outcome)
    }

    /// Replays the original operator disposition after an exact ingress duplicate.
    ///
    /// The caller must first prove the ingress `message_id` and payload hash are an exact
    /// duplicate. A stored immutable operator event means the first attempt was accepted; an
    /// absent event means the first attempt completed as semantic suppression. This avoids
    /// turning a lost suppression acknowledgement into a false delivery success.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid notification, lost ownership, or identity collision.
    pub fn replay_operator_notification(
        &self,
        lease: &OwnerLease,
        notification: &OperatorNotificationV1,
        now: DateTime<Utc>,
    ) -> Result<OperatorNotificationWrite, LedgerError> {
        notification.validate()?;
        let payload_hash = canonical_json_hash(notification)?;
        let targets = sorted_targets(&notification.targets);
        let target_hash = canonical_json_hash(&targets)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        Self::require_owner_in_transaction(&transaction, lease, OwnerRole::Core, now)?;
        let existing = matching_operator_notifications(&transaction, notification)?;
        let outcome = match existing.as_slice() {
            [] => OperatorNotificationWrite::SemanticSuppressed,
            [stored] if stored.matches_exact(notification, &payload_hash, &target_hash) => {
                OperatorNotificationWrite::Duplicate
            }
            _ => {
                return Err(LedgerError::IdentityCollision(format!(
                    "trader_event:{}/{}",
                    notification.event_id, notification.semantic_id
                )));
            }
        };
        transaction.commit()?;
        Ok(outcome)
    }

    /// Checks whether the stable ET report slot is already present without mutating the outbox.
    ///
    /// # Errors
    ///
    /// Returns an error for a lost or non-report owner lease, or storage failure.
    pub fn scheduled_report_exists(
        &self,
        lease: &OwnerLease,
        slot: &Token,
        now: DateTime<Utc>,
    ) -> Result<bool, LedgerError> {
        self.require_owner(lease, OwnerRole::Report, now)?;
        let connection = self.connection()?;
        Ok(connection.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM notification_events
                WHERE lane = 'scheduled_report' AND report_slot = ?1
             )",
            [slot.as_str()],
            |row| row.get(0),
        )?)
    }

    /// Establishes a cancellation fence and safely terminates unsent targets.
    ///
    /// An in-flight target is irreversible and remains in flight until its response settles or
    /// its lease is recovered as uncertain. Success therefore means the durable fence exists; it
    /// does not claim that an already started transport was recalled.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid input, lost ownership, identity collision, or storage failure.
    pub fn cancel_event(
        &self,
        lease: &OwnerLease,
        event_id: &str,
        reason_code: &str,
        now: DateTime<Utc>,
    ) -> Result<PersistWrite, LedgerError> {
        self.cancel_event_at(lease, event_id, reason_code, now, now)
    }

    /// Establishes a cancellation fence with separate causal and processing timestamps.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid input, lost ownership, identity collision, or storage failure.
    pub fn cancel_event_at(
        &self,
        lease: &OwnerLease,
        event_id: &str,
        reason_code: &str,
        cancelled_at: DateTime<Utc>,
        processing_at: DateTime<Utc>,
    ) -> Result<PersistWrite, LedgerError> {
        if event_id.trim().is_empty() || reason_code.trim().is_empty() {
            return Err(LedgerError::InvalidValue("cancellation"));
        }
        if cancelled_at > processing_at {
            return Err(LedgerError::InvalidValue("cancellation timestamp"));
        }
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::require_owner_in_transaction(&transaction, lease, OwnerRole::Core, processing_at)?;
        let inserted = transaction.execute(
            "INSERT OR IGNORE INTO notification_cancellations (
                event_id, reason_code, cancelled_at_us, writer_generation
             ) VALUES (?1, ?2, ?3, ?4)",
            params![
                event_id,
                reason_code,
                micros(cancelled_at),
                lease.generation
            ],
        )?;
        if inserted == 0 {
            let existing_reason: String = transaction.query_row(
                "SELECT reason_code FROM notification_cancellations WHERE event_id = ?1",
                [event_id],
                |row| row.get(0),
            )?;
            if existing_reason != reason_code {
                return Err(LedgerError::IdentityCollision(event_id.to_owned()));
            }
        }
        let targets = {
            let mut statement = transaction.prepare(
                "SELECT t.target_id, t.status
                 FROM notification_targets t
                 WHERE t.event_id = ?1
                   AND t.status IN ('pending', 'claimed')",
            )?;
            statement
                .query_map([event_id], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<Result<Vec<_>, _>>()?
        };
        for (target_id, previous_status) in targets {
            let changed = transaction.execute(
                "UPDATE notification_targets SET
                    status = 'cancelled', next_attempt_at_us = NULL,
                    claim_owner_id = NULL, claim_owner_generation = NULL, claim_token = NULL,
                    claimed_at_us = NULL, lease_until_us = NULL, current_attempt_id = NULL,
                    terminal_at_us = ?1, updated_at_us = ?1,
                    last_error_code = ?2
                 WHERE target_id = ?3 AND status = ?4",
                params![
                    micros(processing_at),
                    reason_code,
                    target_id,
                    previous_status
                ],
            )?;
            if changed != 1 {
                return Err(LedgerError::InvalidValue("cancellation target transition"));
            }
            let receipt = Receipt::CancelledBeforeTransport { reason_code };
            insert_receipt(&transaction, &target_id, &receipt, processing_at)?;
        }
        transaction.commit()?;
        Ok(if inserted == 1 {
            PersistWrite::Inserted
        } else {
            PersistWrite::Duplicate
        })
    }

    fn insert_intent(
        transaction: &Transaction<'_>,
        lease: &OwnerLease,
        intent: &NotificationIntentV1,
        now: DateTime<Utc>,
    ) -> Result<(), LedgerError> {
        let payload_json = serde_json::to_string(intent)?;
        let payload_hash = canonical_json_hash(intent)?;
        let targets = sorted_targets(&intent.targets);
        let target_hash = canonical_json_hash(&targets)?;
        let inserted = transaction.execute(
            "INSERT OR IGNORE INTO notification_events (
                event_id, semantic_id, decision_id, lane, occurred_at_us, expires_at_us,
                payload_json, payload_sha256, target_set_sha256, writer_generation, created_at_us
             ) VALUES (?1, ?2, ?3, 'trade_ready', ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                intent.intent_id.as_str(),
                intent.semantic_id.as_str(),
                intent.decision_id.as_str(),
                micros(intent.created_at),
                micros(intent.expires_at),
                payload_json,
                payload_hash,
                target_hash,
                lease.generation,
                micros(now)
            ],
        )?;
        if inserted == 0 {
            verify_hash(
                transaction,
                "notification_events",
                "event_id",
                intent.intent_id.as_str(),
                &payload_hash,
            )?;
            let existing_target_hash: String = transaction.query_row(
                "SELECT target_set_sha256 FROM notification_events WHERE event_id = ?1",
                [intent.intent_id.as_str()],
                |row| row.get(0),
            )?;
            if existing_target_hash != target_hash {
                return Err(LedgerError::IdentityCollision(intent.intent_id.to_string()));
            }
        }
        insert_targets(
            transaction,
            intent.intent_id.as_str(),
            &targets,
            intent.max_attempts,
            intent.created_at,
        )?;
        Ok(())
    }
}

struct StoredScheduledReport {
    event_id: String,
    semantic_id: String,
    decision_id: Option<String>,
    source_projection_id: Option<String>,
    slot: Option<String>,
    lane: String,
    payload_hash: String,
    target_hash: String,
}

struct StoredOperatorNotification {
    event_id: String,
    semantic_id: String,
    decision_id: Option<String>,
    source_projection_id: Option<String>,
    slot: Option<String>,
    lane: String,
    payload_hash: String,
    target_hash: String,
}

impl StoredOperatorNotification {
    fn matches_exact(
        &self,
        notification: &OperatorNotificationV1,
        payload_hash: &str,
        target_hash: &str,
    ) -> bool {
        self.event_id == notification.event_id.as_str()
            && self.semantic_id == notification.semantic_id.as_str()
            && self.decision_id.is_none()
            && self.source_projection_id.is_none()
            && self.slot.is_none()
            && self.lane == "trader_event"
            && self.payload_hash == payload_hash
            && self.target_hash == target_hash
    }
}

fn matching_operator_notifications(
    transaction: &Transaction<'_>,
    notification: &OperatorNotificationV1,
) -> Result<Vec<StoredOperatorNotification>, LedgerError> {
    let mut statement = transaction.prepare(
        "SELECT event_id, semantic_id, decision_id, source_projection_id,
                report_slot, lane, payload_sha256, target_set_sha256
         FROM notification_events
         WHERE event_id = ?1 OR semantic_id = ?2
         ORDER BY event_id
         LIMIT 2",
    )?;
    Ok(statement
        .query_map(
            params![
                notification.event_id.as_str(),
                notification.semantic_id.as_str()
            ],
            |row| {
                Ok(StoredOperatorNotification {
                    event_id: row.get(0)?,
                    semantic_id: row.get(1)?,
                    decision_id: row.get(2)?,
                    source_projection_id: row.get(3)?,
                    slot: row.get(4)?,
                    lane: row.get(5)?,
                    payload_hash: row.get(6)?,
                    target_hash: row.get(7)?,
                })
            },
        )?
        .collect::<Result<Vec<_>, _>>()?)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OperatorLifecycleDecision {
    Persist,
    Suppress,
}

fn enforce_operator_lifecycle(
    transaction: &Transaction<'_>,
    notification: &OperatorNotificationV1,
) -> Result<OperatorLifecycleDecision, LedgerError> {
    let existing = {
        let mut statement = transaction.prepare(
            "SELECT event_id,
                    CAST(json_extract(payload_json, '$.generation') AS INTEGER),
                    json_extract(payload_json, '$.role')
             FROM notification_events
             WHERE lane = 'trader_event'
               AND json_extract(payload_json, '$.opportunity_id') = ?1
             ORDER BY occurred_at_us, event_id",
        )?;
        statement
            .query_map([notification.opportunity_id.as_str()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    let incoming_generation = i64::from(notification.generation);
    if existing
        .iter()
        .any(|(_, generation, _)| *generation > incoming_generation)
    {
        return Ok(OperatorLifecycleDecision::Suppress);
    }
    let same_generation: Vec<_> = existing
        .iter()
        .filter(|(_, generation, _)| *generation == incoming_generation)
        .collect();
    if same_generation.iter().any(|(_, _, role)| role == "exit") {
        return Ok(OperatorLifecycleDecision::Suppress);
    }
    match notification.role {
        OperatorNotificationRole::Setup
            if same_generation
                .iter()
                .any(|(_, _, role)| role == "trade_ready") =>
        {
            Ok(OperatorLifecycleDecision::Suppress)
        }
        OperatorNotificationRole::TradeReady
            if same_generation
                .iter()
                .any(|(_, _, role)| role == "trade_ready") =>
        {
            Ok(OperatorLifecycleDecision::Suppress)
        }
        _ => Ok(OperatorLifecycleDecision::Persist),
    }
}

fn supersede_prior_operator_targets(
    transaction: &Transaction<'_>,
    notification: &OperatorNotificationV1,
    now: DateTime<Utc>,
) -> Result<(), LedgerError> {
    let (include_trade_ready, reason_code) = match notification.role {
        OperatorNotificationRole::Setup => return Ok(()),
        OperatorNotificationRole::TradeReady => (false, "superseded_by_trade_ready"),
        OperatorNotificationRole::Exit => (true, "superseded_by_exit"),
    };
    let targets = {
        let mut statement = transaction.prepare(
            "SELECT t.target_id, t.status
             FROM notification_targets t
             JOIN notification_events e ON e.event_id = t.event_id
             WHERE e.lane = 'trader_event'
               AND json_extract(e.payload_json, '$.opportunity_id') = ?1
               AND CAST(json_extract(e.payload_json, '$.generation') AS INTEGER) = ?2
               AND (
                    json_extract(e.payload_json, '$.role') = 'setup'
                    OR (?3 = 1 AND json_extract(e.payload_json, '$.role') = 'trade_ready')
               )
               AND t.status IN ('pending', 'claimed')
             ORDER BY t.target_id",
        )?;
        statement
            .query_map(
                params![
                    notification.opportunity_id.as_str(),
                    i64::from(notification.generation),
                    include_trade_ready
                ],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )?
            .collect::<Result<Vec<_>, _>>()?
    };
    for (target_id, previous_status) in targets {
        let changed = transaction.execute(
            "UPDATE notification_targets SET
                status = 'cancelled', next_attempt_at_us = NULL,
                claim_owner_id = NULL, claim_owner_generation = NULL, claim_token = NULL,
                claimed_at_us = NULL, lease_until_us = NULL, current_attempt_id = NULL,
                terminal_at_us = ?1, updated_at_us = ?1, last_error_code = ?2
             WHERE target_id = ?3 AND status = ?4",
            params![micros(now), reason_code, target_id, previous_status],
        )?;
        if changed != 1 {
            return Err(LedgerError::InvalidValue(
                "operator supersession target transition",
            ));
        }
        let receipt = Receipt::CancelledBeforeTransport { reason_code };
        insert_receipt(transaction, &target_id, &receipt, now)?;
    }
    Ok(())
}

impl StoredScheduledReport {
    fn matches_exact(
        &self,
        intent: &NotificationIntentV2,
        source_projection_id: &Token,
        slot: &Token,
        payload_hash: &str,
        target_hash: &str,
    ) -> bool {
        self.event_id == intent.intent_id.as_str()
            && self.semantic_id == intent.semantic_id.as_str()
            && self.decision_id.is_none()
            && self.source_projection_id.as_deref() == Some(source_projection_id.as_str())
            && self.slot.as_deref() == Some(slot.as_str())
            && self.lane == "scheduled_report"
            && self.payload_hash == payload_hash
            && self.target_hash == target_hash
    }
}

fn matching_scheduled_reports(
    transaction: &Transaction<'_>,
    intent: &NotificationIntentV2,
    slot: &Token,
) -> Result<Vec<StoredScheduledReport>, LedgerError> {
    let mut statement = transaction.prepare(
        "SELECT event_id, semantic_id, decision_id, source_projection_id,
                report_slot, lane, payload_sha256, target_set_sha256
         FROM notification_events
         WHERE event_id = ?1 OR semantic_id = ?2
            OR (lane = 'scheduled_report' AND report_slot = ?3)
         ORDER BY event_id
         LIMIT 2",
    )?;
    Ok(statement
        .query_map(
            params![
                intent.intent_id.as_str(),
                intent.semantic_id.as_str(),
                slot.as_str()
            ],
            |row| {
                Ok(StoredScheduledReport {
                    event_id: row.get(0)?,
                    semantic_id: row.get(1)?,
                    decision_id: row.get(2)?,
                    source_projection_id: row.get(3)?,
                    slot: row.get(4)?,
                    lane: row.get(5)?,
                    payload_hash: row.get(6)?,
                    target_hash: row.get(7)?,
                })
            },
        )?
        .collect::<Result<Vec<_>, _>>()?)
}

fn sorted_targets(targets: &[NotificationTargetV1]) -> Vec<&NotificationTargetV1> {
    let mut targets: Vec<_> = targets.iter().collect();
    targets.sort_unstable_by_key(|target| target.key.as_str());
    targets
}

fn insert_targets(
    transaction: &Transaction<'_>,
    intent_id: &str,
    targets: &[&NotificationTargetV1],
    max_attempts: u32,
    created_at: DateTime<Utc>,
) -> Result<(), LedgerError> {
    for target in targets {
        let target_key = target.key.as_str();
        let target_id = format!("{intent_id}:{target_key}");
        transaction.execute(
            "INSERT OR IGNORE INTO notification_targets (
                target_id, event_id, target_key, channel,
                status, attempt_count, max_attempts,
                replay_generation, lease_sequence, next_attempt_at_us, updated_at_us
             ) VALUES (?1, ?2, ?3, ?4, 'pending', 0, ?5, 0, 0, ?6, ?6)",
            params![
                target_id,
                intent_id,
                target_key,
                target.channel.as_str(),
                i64::from(max_attempts),
                micros(created_at)
            ],
        )?;
    }
    Ok(())
}

fn action_name(action: StrategyAction) -> &'static str {
    match action {
        StrategyAction::NoTrade => "no_trade",
        StrategyAction::ManualCandidate => "manual_candidate",
    }
}

fn verify_hash(
    transaction: &Transaction<'_>,
    table: &str,
    id_column: &str,
    id: &str,
    expected_hash: &str,
) -> Result<(), LedgerError> {
    let sql = format!("SELECT payload_sha256 FROM {table} WHERE {id_column} = ?1");
    let existing: String = transaction.query_row(&sql, [id], |row| row.get(0))?;
    if existing == expected_hash {
        Ok(())
    } else {
        Err(LedgerError::IdentityCollision(id.to_owned()))
    }
}
