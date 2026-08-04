use chrono::{DateTime, Utc};
use rusqlite::{Transaction, TransactionBehavior, params};

use crate::db::micros;
use crate::receipt::{Receipt, insert_receipt};
use crate::{
    ClaimHandle, Ledger, LedgerError, LedgerHealth, LedgerReader, OwnerLease, OwnerRole,
    Settlement, SettlementWrite,
};

impl Ledger {
    /// Atomically settles the matching attempt and appends its immutable receipt.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid settlement data, a mismatched attempt, lost fencing, or
    /// storage failure.
    pub fn settle(
        &self,
        lease: &OwnerLease,
        handle: &ClaimHandle,
        attempt_id: &str,
        settlement: &Settlement,
        now: DateTime<Utc>,
    ) -> Result<SettlementWrite, LedgerError> {
        validate_settlement(attempt_id, settlement, now)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::require_owner_in_transaction(&transaction, lease, OwnerRole::Delivery, now)?;
        Self::require_in_flight_in_transaction(&transaction, lease, handle, now)?;
        if !attempt_matches_current_target(&transaction, handle, attempt_id)? {
            return Err(LedgerError::ClaimLost(handle.target_id.clone()));
        }
        let plan = settlement_plan(&transaction, &handle.target_id, settlement, now)?;
        let disposition = match plan.status {
            "delivered" => SettlementWrite::Delivered,
            "pending" => SettlementWrite::RetryScheduled,
            "dead_letter" => SettlementWrite::DeadLetter,
            "uncertain" => SettlementWrite::Uncertain,
            _ => return Err(LedgerError::InvalidValue("settlement target status")),
        };
        let changed = transaction.execute(
            "UPDATE notification_targets SET
                status = ?1, next_attempt_at_us = ?2,
                claim_owner_id = NULL, claim_owner_generation = NULL, claim_token = NULL,
                claimed_at_us = NULL, lease_until_us = NULL, current_attempt_id = NULL,
                delivered_at_us = ?3, terminal_at_us = ?4,
                last_error_code = ?5, updated_at_us = ?6
             WHERE target_id = ?7 AND status = 'in_flight' AND claim_token = ?8
               AND lease_sequence = ?9 AND current_attempt_id = ?10",
            params![
                plan.status,
                plan.next_attempt,
                plan.delivered,
                plan.terminal,
                plan.error_code,
                micros(now),
                handle.target_id,
                handle.claim_token,
                handle.lease_sequence,
                attempt_id
            ],
        )?;
        if changed != 1 {
            return Err(LedgerError::ClaimLost(handle.target_id.clone()));
        }
        let receipt = match (settlement, disposition) {
            (
                Settlement::Delivered {
                    provider_message_id,
                },
                SettlementWrite::Delivered,
            ) => Receipt::Delivered {
                attempt_id,
                provider_message_id: provider_message_id.as_deref(),
            },
            (Settlement::Retryable { error_code, .. }, SettlementWrite::RetryScheduled) => {
                Receipt::RetryScheduled {
                    attempt_id,
                    reason_code: error_code,
                }
            }
            (Settlement::Retryable { error_code, .. }, SettlementWrite::DeadLetter) => {
                Receipt::RetryExhausted {
                    attempt_id,
                    reason_code: error_code,
                }
            }
            (Settlement::PermanentFailure { error_code }, SettlementWrite::DeadLetter) => {
                Receipt::PermanentFailureAfterTransport {
                    attempt_id,
                    reason_code: error_code,
                }
            }
            (Settlement::TransportUncertain { error_code }, SettlementWrite::Uncertain) => {
                Receipt::TransportUncertain {
                    attempt_id,
                    reason_code: error_code,
                }
            }
            _ => return Err(LedgerError::InvalidValue("settlement disposition mismatch")),
        };
        insert_receipt(&transaction, &handle.target_id, &receipt, now)?;
        transaction.commit()?;
        Ok(disposition)
    }

    /// Returns derived target counts and the unacknowledged failure count.
    ///
    /// # Errors
    ///
    /// Returns an error for unknown persisted states or storage failure.
    pub fn health(&self) -> Result<LedgerHealth, LedgerError> {
        read_health(&self.connection()?)
    }
}

fn attempt_matches_current_target(
    transaction: &Transaction<'_>,
    handle: &ClaimHandle,
    attempt_id: &str,
) -> Result<bool, LedgerError> {
    transaction
        .query_row(
            "SELECT EXISTS(
                SELECT 1
                FROM delivery_attempts a
                JOIN notification_targets t ON t.target_id = a.target_id
                WHERE a.attempt_id = ?1 AND a.target_id = ?2 AND a.claim_token = ?3
                  AND a.owner_generation = ?4 AND a.lease_sequence = ?5
                  AND t.status = 'in_flight'
                  AND t.current_attempt_id = a.attempt_id
                  AND t.channel = a.channel
                  AND t.claim_owner_generation = a.owner_generation
                  AND t.replay_generation = a.replay_generation
                  AND t.attempt_count = a.attempt_no
                  AND a.idempotency_key = t.event_id || ':' || t.target_id
                  AND a.started_at_us >= t.claimed_at_us
                  AND a.started_at_us < t.lease_until_us
             )",
            params![
                attempt_id,
                handle.target_id,
                handle.claim_token,
                handle.owner_generation,
                handle.lease_sequence
            ],
            |row| row.get::<_, bool>(0),
        )
        .map_err(Into::into)
}

impl LedgerReader {
    /// Returns delivery-state counts without opening the ledger for writes.
    ///
    /// # Errors
    ///
    /// Returns an error for unknown persisted states or storage failure.
    pub fn health(&self) -> Result<LedgerHealth, LedgerError> {
        read_health(&self.connection()?)
    }
}

fn read_health(connection: &rusqlite::Connection) -> Result<LedgerHealth, LedgerError> {
    let mut health = LedgerHealth::default();
    let mut statement =
        connection.prepare("SELECT status, COUNT(*) FROM notification_targets GROUP BY status")?;
    {
        let rows = statement.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?;
        for row in rows {
            let (status, raw_count) = row?;
            let count = u64::try_from(raw_count)
                .map_err(|_| LedgerError::InvalidValue("negative status count"))?;
            match status.as_str() {
                "pending" => health.pending = count,
                "claimed" => health.claimed = count,
                "in_flight" => health.in_flight = count,
                "delivered" => health.delivered = count,
                "dead_letter" => health.dead_letter = count,
                "cancelled" => health.cancelled = count,
                "expired" => health.expired = count,
                "uncertain" => health.uncertain = count,
                _ => return Err(LedgerError::InvalidValue("target status")),
            }
        }
    }
    let unacknowledged: i64 = connection.query_row(
        "SELECT COUNT(*) FROM notification_targets
             WHERE status IN ('dead_letter', 'expired', 'uncertain')
               AND operator_ack_at_us IS NULL",
        [],
        |row| row.get(0),
    )?;
    health.unacknowledged_failures = u64::try_from(unacknowledged)
        .map_err(|_| LedgerError::InvalidValue("negative failure count"))?;
    Ok(health)
}

struct SettlementPlan {
    status: &'static str,
    next_attempt: Option<i64>,
    terminal: Option<i64>,
    delivered: Option<i64>,
    error_code: String,
}

fn validate_settlement(
    attempt_id: &str,
    settlement: &Settlement,
    now: DateTime<Utc>,
) -> Result<(), LedgerError> {
    if attempt_id.trim().is_empty() {
        return Err(LedgerError::InvalidValue("attempt_id"));
    }
    match settlement {
        Settlement::Delivered {
            provider_message_id: Some(provider_message_id),
        } if provider_message_id.trim().is_empty() => {
            Err(LedgerError::InvalidValue("provider_message_id"))
        }
        Settlement::Retryable {
            error_code,
            retry_at,
        } if error_code.trim().is_empty() || *retry_at <= now => {
            Err(LedgerError::InvalidValue("retry settlement"))
        }
        Settlement::PermanentFailure { error_code }
        | Settlement::TransportUncertain { error_code }
            if error_code.trim().is_empty() =>
        {
            Err(LedgerError::InvalidValue("settlement error_code"))
        }
        Settlement::Delivered { .. }
        | Settlement::Retryable { .. }
        | Settlement::PermanentFailure { .. }
        | Settlement::TransportUncertain { .. } => Ok(()),
    }
}

fn settlement_plan(
    transaction: &Transaction<'_>,
    target_id: &str,
    settlement: &Settlement,
    now: DateTime<Utc>,
) -> Result<SettlementPlan, LedgerError> {
    let terminal_at = Some(micros(now));
    let plan = match settlement {
        Settlement::Delivered { .. } => SettlementPlan {
            status: "delivered",
            next_attempt: None,
            terminal: terminal_at,
            delivered: terminal_at,
            error_code: "delivered".to_owned(),
        },
        Settlement::Retryable {
            error_code,
            retry_at,
        } => {
            let (attempt_count, max_attempts): (i64, i64) = transaction.query_row(
                "SELECT attempt_count, max_attempts
                 FROM notification_targets WHERE target_id = ?1",
                [target_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            if attempt_count >= max_attempts {
                SettlementPlan {
                    status: "dead_letter",
                    next_attempt: None,
                    terminal: terminal_at,
                    delivered: None,
                    error_code: error_code.clone(),
                }
            } else {
                SettlementPlan {
                    status: "pending",
                    next_attempt: Some(micros(*retry_at)),
                    terminal: None,
                    delivered: None,
                    error_code: error_code.clone(),
                }
            }
        }
        Settlement::PermanentFailure { error_code } => SettlementPlan {
            status: "dead_letter",
            next_attempt: None,
            terminal: terminal_at,
            delivered: None,
            error_code: error_code.clone(),
        },
        Settlement::TransportUncertain { error_code } => SettlementPlan {
            status: "uncertain",
            next_attempt: None,
            terminal: terminal_at,
            delivered: None,
            error_code: error_code.clone(),
        },
    };
    Ok(plan)
}
