use chrono::{DateTime, Utc};
use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};
use uuid::Uuid;

use crate::db::micros;
use crate::{Ledger, LedgerError, OperatorWrite};

impl Ledger {
    /// Records that an operator reviewed a terminal delivery failure without changing its outcome.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid operator metadata, an unknown or ineligible target, or storage
    /// failure.
    pub fn acknowledge_failure(
        &self,
        target_id: &str,
        actor: &str,
        reason_code: &str,
        now: DateTime<Utc>,
    ) -> Result<OperatorWrite, LedgerError> {
        validate_operator_input(target_id, actor, reason_code, now)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let state = transaction
            .query_row(
                "SELECT status, operator_ack_at_us
                 FROM notification_targets WHERE target_id = ?1",
                [target_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<i64>>(1)?)),
            )
            .optional()?
            .ok_or(LedgerError::InvalidValue("unknown target_id"))?;
        if !matches!(state.0.as_str(), "dead_letter" | "expired" | "uncertain") {
            return Err(LedgerError::InvalidValue(
                "only terminal failures can be acknowledged",
            ));
        }
        if state.1.is_some() {
            transaction.commit()?;
            return Ok(OperatorWrite::AlreadyAcknowledged);
        }
        let changed = transaction.execute(
            "UPDATE notification_targets SET operator_ack_at_us = ?1, updated_at_us = ?1
             WHERE target_id = ?2 AND operator_ack_at_us IS NULL",
            params![micros(now), target_id],
        )?;
        if changed != 1 {
            return Err(LedgerError::InvalidValue("operator acknowledgement"));
        }
        insert_operator_action(
            &transaction,
            target_id,
            "acknowledge",
            reason_code,
            actor,
            now,
        )?;
        transaction.commit()?;
        Ok(OperatorWrite::Applied)
    }

    /// Requeues an unexpired dead-letter or uncertain target under a new replay generation.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid operator metadata, an unknown/ineligible/expired target, or
    /// storage failure.
    pub fn replay_failure(
        &self,
        target_id: &str,
        actor: &str,
        reason_code: &str,
        now: DateTime<Utc>,
    ) -> Result<(), LedgerError> {
        validate_operator_input(target_id, actor, reason_code, now)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (status, expires_at): (String, i64) = transaction
            .query_row(
                "SELECT t.status, e.expires_at_us
                 FROM notification_targets t
                 JOIN notification_events e ON e.event_id = t.event_id
                 WHERE t.target_id = ?1",
                [target_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?
            .ok_or(LedgerError::InvalidValue("unknown target_id"))?;
        if !matches!(status.as_str(), "dead_letter" | "uncertain") {
            return Err(LedgerError::InvalidValue(
                "only dead_letter or uncertain targets can be replayed",
            ));
        }
        if expires_at <= micros(now) {
            return Err(LedgerError::InvalidValue(
                "expired target cannot be replayed",
            ));
        }
        let changed = transaction.execute(
            "UPDATE notification_targets SET
                status = 'pending', attempt_count = 0,
                replay_generation = replay_generation + 1,
                next_attempt_at_us = ?1, delivered_at_us = NULL, terminal_at_us = NULL,
                operator_ack_at_us = NULL, last_error_code = 'operator_replay',
                updated_at_us = ?1
             WHERE target_id = ?2 AND status IN ('dead_letter', 'uncertain')",
            params![micros(now), target_id],
        )?;
        if changed != 1 {
            return Err(LedgerError::InvalidValue("operator replay transition"));
        }
        insert_operator_action(&transaction, target_id, "replay", reason_code, actor, now)?;
        transaction.commit()?;
        Ok(())
    }
}

fn validate_operator_input(
    target_id: &str,
    actor: &str,
    reason_code: &str,
    now: DateTime<Utc>,
) -> Result<(), LedgerError> {
    if target_id.trim().is_empty()
        || target_id.contains('\0')
        || actor.trim().is_empty()
        || actor.len() > 128
        || actor.contains('\0')
        || reason_code.trim().is_empty()
        || reason_code.len() > 128
        || reason_code.contains('\0')
        || micros(now) <= 0
    {
        return Err(LedgerError::InvalidValue("operator action"));
    }
    Ok(())
}

fn insert_operator_action(
    transaction: &Transaction<'_>,
    target_id: &str,
    action: &str,
    reason_code: &str,
    actor: &str,
    now: DateTime<Utc>,
) -> Result<(), LedgerError> {
    transaction.execute(
        "INSERT INTO operator_actions (
            action_id, target_id, action, reason_code, actor, occurred_at_us
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            Uuid::now_v7().to_string(),
            target_id,
            action,
            reason_code,
            actor,
            micros(now)
        ],
    )?;
    Ok(())
}
