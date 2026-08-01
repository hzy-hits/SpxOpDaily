use chrono::{DateTime, TimeDelta, Utc};
use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};
use spx_domain::{DeliveryChannel, NotificationIntentV1, Token, Validate};
use uuid::Uuid;

use crate::db::micros;
use crate::receipt::{Receipt, insert_receipt};
use crate::{
    BeginTransport, ClaimHandle, ClaimedDelivery, Ledger, LedgerError, OwnerLease, OwnerRole,
    RecoverySummary,
};

impl Ledger {
    /// Recovers expired claims, requeuing pre-transport work and fencing started work as unknown.
    ///
    /// # Errors
    ///
    /// Returns an error for lost ownership or storage failure.
    pub fn recover_stale_claims(
        &self,
        lease: &OwnerLease,
        now: DateTime<Utc>,
    ) -> Result<RecoverySummary, LedgerError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::require_owner_in_transaction(&transaction, lease, OwnerRole::Delivery, now)?;
        let stale = {
            let mut statement = transaction.prepare(
                "SELECT t.target_id, t.status, t.current_attempt_id
                 FROM notification_targets t
                 WHERE t.status IN ('claimed', 'in_flight') AND t.lease_until_us <= ?1",
            )?;
            statement
                .query_map([micros(now)], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()?
        };
        let mut summary = RecoverySummary::default();
        for (target_id, status, attempt_id) in stale {
            match status.as_str() {
                "in_flight" => {
                    let attempt_id = attempt_id.ok_or(LedgerError::InvalidValue(
                        "in_flight target without attempt",
                    ))?;
                    transaction.execute(
                        "UPDATE notification_targets SET
                        status = 'uncertain', next_attempt_at_us = NULL,
                        claim_owner_id = NULL, claim_owner_generation = NULL,
                        claim_token = NULL, claimed_at_us = NULL, lease_until_us = NULL,
                        current_attempt_id = NULL, terminal_at_us = ?2, updated_at_us = ?2,
                        last_error_code = 'worker_lost_after_transport_start'
                     WHERE target_id = ?1 AND status = 'in_flight'",
                        params![target_id, micros(now)],
                    )?;
                    insert_receipt(
                        &transaction,
                        &target_id,
                        &Receipt::TransportUncertain {
                            attempt_id: &attempt_id,
                            reason_code: "worker_lost_after_transport_start",
                        },
                        now,
                    )?;
                    summary.uncertain += 1;
                }
                "claimed" => {
                    if attempt_id.is_some() {
                        return Err(LedgerError::InvalidValue("claimed target has attempt"));
                    }
                    transaction.execute(
                        "UPDATE notification_targets SET
                        status = 'pending', next_attempt_at_us = ?2,
                        claim_owner_id = NULL, claim_owner_generation = NULL,
                        claim_token = NULL, claimed_at_us = NULL, lease_until_us = NULL,
                        current_attempt_id = NULL, updated_at_us = ?2,
                        last_error_code = 'claim_recovered_before_transport'
                     WHERE target_id = ?1 AND status = 'claimed'",
                        params![target_id, micros(now)],
                    )?;
                    insert_receipt(&transaction, &target_id, &Receipt::ClaimRecovered, now)?;
                    summary.requeued += 1;
                }
                _ => return Err(LedgerError::InvalidValue("recoverable target status")),
            }
        }
        transaction.commit()?;
        Ok(summary)
    }

    /// Claims the next due target using a random token and monotonic lease sequence.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid duration, lost ownership, corrupt payload, or storage failure.
    pub fn claim_next(
        &self,
        lease: &OwnerLease,
        now: DateTime<Utc>,
        claim_duration: TimeDelta,
    ) -> Result<Option<ClaimedDelivery>, LedgerError> {
        let claim_until = now
            .checked_add_signed(claim_duration)
            .ok_or(LedgerError::InvalidTimestamp)?;
        if claim_until <= now {
            return Err(LedgerError::InvalidValue("claim duration"));
        }
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::require_owner_in_transaction(&transaction, lease, OwnerRole::Delivery, now)?;
        expire_due(&transaction, now)?;
        let selected = transaction
            .query_row(
                "SELECT t.target_id, t.event_id, t.target_key, t.channel, t.attempt_count,
                        t.replay_generation, e.payload_json
                 FROM notification_targets t
                 JOIN notification_events e ON e.event_id = t.event_id
                 WHERE t.status = 'pending'
                   AND t.next_attempt_at_us <= ?1
                   AND t.attempt_count < t.max_attempts
                   AND e.expires_at_us > ?1
                 ORDER BY e.expires_at_us ASC, e.occurred_at_us ASC, t.target_id ASC
                 LIMIT 1",
                [micros(now)],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, i64>(5)?,
                        row.get::<_, String>(6)?,
                    ))
                },
            )
            .optional()?;
        let Some((target_id, event_id, target_key, channel, attempt_count, _, payload_json)) =
            selected
        else {
            transaction.commit()?;
            return Ok(None);
        };
        let claim_token = Uuid::new_v4().to_string();
        let lease_sequence: i64 = transaction.query_row(
            "SELECT lease_sequence + 1 FROM notification_targets WHERE target_id = ?1",
            [&target_id],
            |row| row.get(0),
        )?;
        let changed = transaction.execute(
            "UPDATE notification_targets SET
                status = 'claimed', lease_sequence = ?2, next_attempt_at_us = NULL,
                claim_owner_id = ?3, claim_owner_generation = ?4, claim_token = ?5,
                claimed_at_us = ?6, lease_until_us = ?7, updated_at_us = ?6
             WHERE target_id = ?1 AND status = 'pending'",
            params![
                target_id,
                lease_sequence,
                lease.owner_id,
                lease.generation,
                claim_token,
                micros(now),
                micros(claim_until)
            ],
        )?;
        if changed != 1 {
            return Err(LedgerError::ClaimLost(target_id));
        }
        let intent: NotificationIntentV1 = serde_json::from_str(&payload_json)?;
        intent.validate()?;
        let target_key = Token::new(target_key, "target_key")?;
        let channel = parse_channel(&channel)?;
        let handle = ClaimHandle {
            target_id: target_id.clone(),
            claim_token,
            lease_sequence,
            owner_generation: lease.generation,
        };
        let claimed = ClaimedDelivery {
            handle,
            intent,
            target_key,
            channel,
            attempt_no: u32::try_from(attempt_count + 1)
                .map_err(|_| LedgerError::InvalidValue("attempt_count"))?,
            idempotency_key: format!("{event_id}:{target_id}"),
        };
        transaction.commit()?;
        Ok(Some(claimed))
    }

    /// Atomically rechecks cancellation/expiry and enters the irreversible transport state.
    ///
    /// # Errors
    ///
    /// Returns an error for a lost claim/owner, exhausted attempts, or storage failure.
    pub fn begin_transport(
        &self,
        lease: &OwnerLease,
        claimed: &ClaimedDelivery,
        now: DateTime<Utc>,
    ) -> Result<BeginTransport, LedgerError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::require_owner_in_transaction(&transaction, lease, OwnerRole::Delivery, now)?;
        Self::require_claim_in_transaction(&transaction, lease, &claimed.handle, now)?;
        let state = transaction.query_row(
            "SELECT e.event_id, e.expires_at_us,
                    EXISTS(SELECT 1 FROM notification_cancellations c WHERE c.event_id = e.event_id)
             FROM notification_targets t
             JOIN notification_events e ON e.event_id = t.event_id
             WHERE t.target_id = ?1",
            [&claimed.handle.target_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, bool>(2)?,
                ))
            },
        )?;
        let outcome = if state.2 {
            terminal_without_transport(
                &transaction,
                &claimed.handle,
                now,
                "cancelled",
                "source_cancelled",
                &Receipt::CancelledBeforeTransport {
                    reason_code: "source_cancelled",
                },
            )?;
            BeginTransport::Cancelled
        } else if state.1 <= micros(now) {
            terminal_without_transport(
                &transaction,
                &claimed.handle,
                now,
                "expired",
                "ttl_expired",
                &Receipt::ExpiredBeforeTransport,
            )?;
            BeginTransport::Expired
        } else {
            let attempt_id = Uuid::now_v7().to_string();
            let changed = transaction.execute(
                "UPDATE notification_targets SET
                    status = 'in_flight', attempt_count = attempt_count + 1,
                    current_attempt_id = ?2, updated_at_us = ?1
                 WHERE target_id = ?3 AND status = 'claimed'
                   AND claim_token = ?4 AND lease_sequence = ?5
                   AND attempt_count + 1 = ?6 AND attempt_count < max_attempts",
                params![
                    micros(now),
                    attempt_id,
                    claimed.handle.target_id,
                    claimed.handle.claim_token,
                    claimed.handle.lease_sequence,
                    i64::from(claimed.attempt_no)
                ],
            )?;
            if changed != 1 {
                return Err(LedgerError::ClaimLost(claimed.handle.target_id.clone()));
            }
            let inserted = transaction.execute(
                "INSERT INTO delivery_attempts (
                    attempt_id, target_id, channel, claim_token, owner_generation,
                    replay_generation, attempt_no, lease_sequence, idempotency_key, started_at_us
                 ) SELECT ?1, target_id, channel, ?2, ?3, replay_generation, ?4, ?5, ?6, ?7
                   FROM notification_targets WHERE target_id = ?8 AND status = 'in_flight'",
                params![
                    attempt_id,
                    claimed.handle.claim_token,
                    lease.generation,
                    i64::from(claimed.attempt_no),
                    claimed.handle.lease_sequence,
                    claimed.idempotency_key,
                    micros(now),
                    claimed.handle.target_id
                ],
            )?;
            if inserted != 1 {
                return Err(LedgerError::ClaimLost(claimed.handle.target_id.clone()));
            }
            BeginTransport::Started { attempt_id }
        };
        transaction.commit()?;
        Ok(outcome)
    }

    /// Permanently rejects a claimed target before transport when its adapter is unavailable.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid input, a lost claim/owner fence, or storage failure.
    pub fn reject_before_transport(
        &self,
        lease: &OwnerLease,
        handle: &ClaimHandle,
        error_code: &str,
        now: DateTime<Utc>,
    ) -> Result<(), LedgerError> {
        if error_code.trim().is_empty() {
            return Err(LedgerError::InvalidValue("pre-transport error_code"));
        }
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::require_owner_in_transaction(&transaction, lease, OwnerRole::Delivery, now)?;
        Self::require_claim_in_transaction(&transaction, lease, handle, now)?;
        terminal_without_transport(
            &transaction,
            handle,
            now,
            "dead_letter",
            error_code,
            &Receipt::PermanentFailureBeforeTransport {
                reason_code: error_code,
            },
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn require_claim_in_transaction(
        transaction: &Transaction<'_>,
        lease: &OwnerLease,
        handle: &ClaimHandle,
        now: DateTime<Utc>,
    ) -> Result<(), LedgerError> {
        if handle.owner_generation != lease.generation {
            return Err(LedgerError::ClaimLost(handle.target_id.clone()));
        }
        let valid = transaction.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM notification_targets
                WHERE target_id = ?1 AND status = 'claimed'
                  AND claim_owner_id = ?2 AND claim_owner_generation = ?3
                  AND claim_token = ?4 AND lease_sequence = ?5 AND lease_until_us > ?6
             )",
            params![
                handle.target_id,
                lease.owner_id,
                lease.generation,
                handle.claim_token,
                handle.lease_sequence,
                micros(now)
            ],
            |row| row.get::<_, bool>(0),
        )?;
        if valid {
            Ok(())
        } else {
            Err(LedgerError::ClaimLost(handle.target_id.clone()))
        }
    }

    pub(crate) fn require_in_flight_in_transaction(
        transaction: &Transaction<'_>,
        lease: &OwnerLease,
        handle: &ClaimHandle,
        now: DateTime<Utc>,
    ) -> Result<(), LedgerError> {
        if handle.owner_generation != lease.generation {
            return Err(LedgerError::ClaimLost(handle.target_id.clone()));
        }
        let valid = transaction.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM notification_targets
                WHERE target_id = ?1 AND status = 'in_flight'
                  AND claim_owner_id = ?2 AND claim_owner_generation = ?3
                  AND claim_token = ?4 AND lease_sequence = ?5 AND lease_until_us > ?6
             )",
            params![
                handle.target_id,
                lease.owner_id,
                lease.generation,
                handle.claim_token,
                handle.lease_sequence,
                micros(now)
            ],
            |row| row.get::<_, bool>(0),
        )?;
        if valid {
            Ok(())
        } else {
            Err(LedgerError::ClaimLost(handle.target_id.clone()))
        }
    }
}

fn expire_due(transaction: &Transaction<'_>, now: DateTime<Utc>) -> Result<(), LedgerError> {
    let targets = {
        let mut statement = transaction.prepare(
            "SELECT target_id FROM notification_targets
             WHERE status = 'pending' AND event_id IN (
                SELECT event_id FROM notification_events WHERE expires_at_us <= ?1
             )",
        )?;
        statement
            .query_map([micros(now)], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?
    };
    for target_id in targets {
        let changed = transaction.execute(
            "UPDATE notification_targets SET
                status = 'expired', next_attempt_at_us = NULL, terminal_at_us = ?1,
                updated_at_us = ?1, last_error_code = 'ttl_expired'
             WHERE target_id = ?2 AND status = 'pending'",
            params![micros(now), target_id],
        )?;
        if changed != 1 {
            return Err(LedgerError::InvalidValue("expiry target transition"));
        }
        insert_receipt(
            transaction,
            &target_id,
            &Receipt::ExpiredBeforeTransport,
            now,
        )?;
    }
    Ok(())
}

fn terminal_without_transport(
    transaction: &Transaction<'_>,
    handle: &ClaimHandle,
    now: DateTime<Utc>,
    status: &str,
    reason_code: &str,
    receipt: &Receipt<'_>,
) -> Result<(), LedgerError> {
    let changed = transaction.execute(
        "UPDATE notification_targets SET
            status = ?1, next_attempt_at_us = NULL,
            claim_owner_id = NULL, claim_owner_generation = NULL, claim_token = NULL,
            claimed_at_us = NULL, lease_until_us = NULL, current_attempt_id = NULL,
            terminal_at_us = ?2, updated_at_us = ?2,
            last_error_code = ?3
         WHERE target_id = ?4 AND status = 'claimed' AND claim_token = ?5
           AND lease_sequence = ?6",
        params![
            status,
            micros(now),
            reason_code,
            handle.target_id,
            handle.claim_token,
            handle.lease_sequence
        ],
    )?;
    if changed != 1 {
        return Err(LedgerError::ClaimLost(handle.target_id.clone()));
    }
    insert_receipt(transaction, &handle.target_id, receipt, now)
}

fn parse_channel(channel: &str) -> Result<DeliveryChannel, LedgerError> {
    match channel {
        "bark" => Ok(DeliveryChannel::Bark),
        "feishu" => Ok(DeliveryChannel::Feishu),
        "webhook" => Ok(DeliveryChannel::Webhook),
        _ => Err(LedgerError::InvalidValue("delivery channel")),
    }
}
