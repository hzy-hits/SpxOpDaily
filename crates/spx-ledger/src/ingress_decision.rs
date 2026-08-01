use chrono::{DateTime, Utc};
use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};
use spx_domain::{
    NotificationIntentV1, StrategyAction, StrategyDecisionV1, Token, Validate, canonical_json_hash,
};

use crate::db::micros;
use crate::receipt::{Receipt, insert_receipt};
use crate::{IngressCheck, IngressWrite, Ledger, LedgerError, OwnerLease, OwnerRole, PersistWrite};

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

    /// Establishes a cancellation fence and safely terminates unsent targets.
    ///
    /// An in-flight target is irreversible and remains in flight until its response settles or
    /// its lease is recovered as uncertain.
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
    ) -> Result<(), LedgerError> {
        if event_id.trim().is_empty() || reason_code.trim().is_empty() {
            return Err(LedgerError::InvalidValue("cancellation"));
        }
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::require_owner_in_transaction(&transaction, lease, OwnerRole::Core, now)?;
        let inserted = transaction.execute(
            "INSERT OR IGNORE INTO notification_cancellations (
                event_id, reason_code, cancelled_at_us, writer_generation
             ) VALUES (?1, ?2, ?3, ?4)",
            params![event_id, reason_code, micros(now), lease.generation],
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
                params![micros(now), reason_code, target_id, previous_status],
            )?;
            if changed != 1 {
                return Err(LedgerError::InvalidValue("cancellation target transition"));
            }
            let receipt = Receipt::CancelledBeforeTransport { reason_code };
            insert_receipt(&transaction, &target_id, &receipt, now)?;
        }
        transaction.commit()?;
        Ok(())
    }

    fn insert_intent(
        transaction: &Transaction<'_>,
        lease: &OwnerLease,
        intent: &NotificationIntentV1,
        now: DateTime<Utc>,
    ) -> Result<(), LedgerError> {
        let payload_json = serde_json::to_string(intent)?;
        let payload_hash = canonical_json_hash(intent)?;
        let mut targets: Vec<_> = intent.targets.iter().collect();
        targets.sort_unstable_by_key(|target| target.key.as_str());
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
        for target in targets {
            let target_key = target.key.as_str();
            let target_id = format!("{}:{target_key}", intent.intent_id);
            transaction.execute(
                "INSERT OR IGNORE INTO notification_targets (
                    target_id, event_id, target_key, channel,
                    status, attempt_count, max_attempts,
                    replay_generation, lease_sequence, next_attempt_at_us, updated_at_us
                 ) VALUES (?1, ?2, ?3, ?4, 'pending', 0, ?5, 0, 0, ?6, ?6)",
                params![
                    target_id,
                    intent.intent_id.as_str(),
                    target_key,
                    target.channel.as_str(),
                    i64::from(intent.max_attempts),
                    micros(intent.created_at)
                ],
            )?;
        }
        Ok(())
    }
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
