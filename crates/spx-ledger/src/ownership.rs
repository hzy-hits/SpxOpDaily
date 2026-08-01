use chrono::{DateTime, TimeDelta, Utc};
use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};

use crate::db::micros;
use crate::{Ledger, LedgerError, OwnerLease, OwnerRole};

impl Ledger {
    /// Acquires an exclusive, generation-fenced runtime role lease.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid lease input, a live competing owner, or storage failure.
    pub fn acquire_owner(
        &self,
        role: OwnerRole,
        owner_id: &str,
        now: DateTime<Utc>,
        lease_duration: TimeDelta,
    ) -> Result<OwnerLease, LedgerError> {
        validate_owner_id(owner_id)?;
        let lease_until = now
            .checked_add_signed(lease_duration)
            .ok_or(LedgerError::InvalidTimestamp)?;
        if lease_until <= now {
            return Err(LedgerError::InvalidValue("owner lease duration"));
        }
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing = transaction
            .query_row(
                "SELECT owner_id, generation, lease_until_us, active
                 FROM runtime_owners WHERE role = ?1",
                [role.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, bool>(3)?,
                    ))
                },
            )
            .optional()?;
        let generation = match existing {
            None => 1,
            Some((current_owner, generation, current_until, true))
                if current_owner == owner_id && current_until > micros(now) =>
            {
                generation
            }
            Some((_, generation, current_until, active))
                if !active || current_until <= micros(now) =>
            {
                generation
                    .checked_add(1)
                    .ok_or(LedgerError::InvalidValue("owner generation overflow"))?
            }
            Some(_) => return Err(LedgerError::OwnerLeaseLost(role)),
        };
        transaction.execute(
            "INSERT INTO runtime_owners (
                role, owner_id, generation, active,
                acquired_at_us, heartbeat_at_us, lease_until_us
             ) VALUES (?1, ?2, ?3, 1, ?4, ?4, ?5)
             ON CONFLICT(role) DO UPDATE SET
                owner_id = excluded.owner_id,
                generation = excluded.generation,
                active = 1,
                acquired_at_us = excluded.acquired_at_us,
                heartbeat_at_us = excluded.heartbeat_at_us,
                lease_until_us = excluded.lease_until_us",
            params![
                role.as_str(),
                owner_id,
                generation,
                micros(now),
                micros(lease_until)
            ],
        )?;
        transaction.commit()?;
        Ok(OwnerLease {
            role,
            owner_id: owner_id.to_owned(),
            generation,
            lease_until,
        })
    }

    /// Renews an owner lease only while its owner ID and generation remain current.
    ///
    /// # Errors
    ///
    /// Returns an error when ownership was lost, the duration is invalid, or storage fails.
    pub fn renew_owner(
        &self,
        lease: &mut OwnerLease,
        now: DateTime<Utc>,
        lease_duration: TimeDelta,
    ) -> Result<(), LedgerError> {
        let lease_until = now
            .checked_add_signed(lease_duration)
            .ok_or(LedgerError::InvalidTimestamp)?;
        let connection = self.connection()?;
        let changed = connection.execute(
            "UPDATE runtime_owners SET heartbeat_at_us = ?1, lease_until_us = ?2
             WHERE role = ?3 AND owner_id = ?4 AND generation = ?5
               AND active = 1 AND lease_until_us > ?1",
            params![
                micros(now),
                micros(lease_until),
                lease.role.as_str(),
                lease.owner_id,
                lease.generation
            ],
        )?;
        if changed != 1 {
            return Err(LedgerError::OwnerLeaseLost(lease.role));
        }
        lease.lease_until = lease_until;
        Ok(())
    }

    /// Keeps a live lease current, or atomically reacquires an expired lease with a new fence.
    ///
    /// A competing live owner is never displaced. Reacquisition after an idle period advances
    /// the generation so handles issued under the expired lease remain fenced out.
    ///
    /// # Errors
    ///
    /// Returns an error when a competing owner holds the role, input is invalid, or storage fails.
    pub fn refresh_owner(
        &self,
        lease: &mut OwnerLease,
        now: DateTime<Utc>,
        lease_duration: TimeDelta,
    ) -> Result<(), LedgerError> {
        if lease.lease_until > now {
            return self.renew_owner(lease, now, lease_duration);
        }
        let reacquired = self.acquire_owner(lease.role, &lease.owner_id, now, lease_duration)?;
        *lease = reacquired;
        Ok(())
    }

    /// Releases exactly the caller's current owner generation.
    ///
    /// A stale process cannot release a replacement owner's lease. Crashes still rely on TTL;
    /// this operation is for normal command completion and graceful service shutdown.
    ///
    /// # Errors
    ///
    /// Returns an error when the lease was already lost or storage fails.
    pub fn release_owner(&self, lease: &OwnerLease) -> Result<(), LedgerError> {
        let connection = self.connection()?;
        let changed = connection.execute(
            "UPDATE runtime_owners SET active = 0
             WHERE role = ?1 AND owner_id = ?2 AND generation = ?3 AND active = 1",
            params![lease.role.as_str(), lease.owner_id, lease.generation],
        )?;
        if changed == 1 {
            Ok(())
        } else {
            Err(LedgerError::OwnerLeaseLost(lease.role))
        }
    }

    pub(crate) fn require_owner(
        &self,
        lease: &OwnerLease,
        expected: OwnerRole,
        now: DateTime<Utc>,
    ) -> Result<(), LedgerError> {
        if lease.role != expected {
            return Err(LedgerError::OwnerRoleMismatch {
                expected,
                actual: lease.role,
            });
        }
        let connection = self.connection()?;
        let valid = connection.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM runtime_owners
                WHERE role = ?1 AND owner_id = ?2 AND generation = ?3
                  AND active = 1 AND lease_until_us > ?4
             )",
            params![
                lease.role.as_str(),
                lease.owner_id,
                lease.generation,
                micros(now)
            ],
            |row| row.get::<_, bool>(0),
        )?;
        if valid {
            Ok(())
        } else {
            Err(LedgerError::OwnerLeaseLost(lease.role))
        }
    }

    pub(crate) fn require_owner_in_transaction(
        transaction: &Transaction<'_>,
        lease: &OwnerLease,
        expected: OwnerRole,
        now: DateTime<Utc>,
    ) -> Result<(), LedgerError> {
        if lease.role != expected {
            return Err(LedgerError::OwnerRoleMismatch {
                expected,
                actual: lease.role,
            });
        }
        let valid = transaction.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM runtime_owners
                WHERE role = ?1 AND owner_id = ?2 AND generation = ?3
                  AND active = 1 AND lease_until_us > ?4
             )",
            params![
                lease.role.as_str(),
                lease.owner_id,
                lease.generation,
                micros(now)
            ],
            |row| row.get::<_, bool>(0),
        )?;
        if valid {
            Ok(())
        } else {
            Err(LedgerError::OwnerLeaseLost(lease.role))
        }
    }
}

fn validate_owner_id(owner_id: &str) -> Result<(), LedgerError> {
    if (16..=128).contains(&owner_id.trim().len()) {
        Ok(())
    } else {
        Err(LedgerError::InvalidValue("owner_id"))
    }
}
