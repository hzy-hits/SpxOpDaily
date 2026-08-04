use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::Write as _;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use rustix::fs::{FlockOperation, Mode, OFlags, flock, open};
use serde::{Deserialize, Serialize};
use spx_domain::{IngressEnvelopeV1, IngressMessageV1, Provider, Validate};
use thiserror::Error;
use uuid::Uuid;

const STATE_SCHEMA: &str = "spx_normalized_bridge_state.v1";

#[derive(Debug, Error)]
pub enum StateError {
    #[error("bridge state already exists")]
    AlreadyExists,
    #[error("bridge state does not exist; initialize it explicitly")]
    Missing,
    #[error("bridge state path is not a regular file")]
    UnsafePath,
    #[error("another normalized bridge process owns the state lock")]
    AlreadyRunning,
    #[error("bridge state schema mismatch")]
    SchemaMismatch,
    #[error("bridge state has an invalid provider set")]
    ProviderSet,
    #[error("bridge state counter overflow")]
    CounterOverflow,
    #[error("an ingress frame is already pending")]
    PendingExists,
    #[error("no ingress frame is pending")]
    NoPending,
    #[error("pending acknowledgement does not match message id")]
    AckMismatch,
    #[error("source-failure progress does not match the pending frame")]
    SourceFailureMismatch,
    #[error("pending ingress frame is invalid or inconsistent with bridge state")]
    InvalidPending,
    #[error("bridge state I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("bridge state JSON failed: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BridgeState {
    pub schema_version: String,
    pub bridge_id: String,
    pub boot_count: u64,
    pub providers: BTreeMap<Provider, ProviderCursor>,
    pub pending: Option<PendingFrame>,
    pub active_source_failure: Option<SourceFailureProgress>,
    pub last_completed_source_fingerprint: Option<String>,
    pub last_completed_source_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderCursor {
    pub connection_generation: u64,
    pub sequence: u64,
    pub source_epoch: Option<String>,
    pub last_semantic_hash: Option<String>,
    pub last_ack_message_id: Option<String>,
    pub last_ack_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PendingFrame {
    pub provider: Provider,
    pub source_fingerprint: String,
    pub semantic_hash: String,
    pub purpose: PendingPurpose,
    pub rejected_attempts: u32,
    pub envelope: IngressEnvelopeV1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PendingPurpose {
    Snapshot,
    SourceFailure,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceFailureProgress {
    pub fingerprint: String,
    pub acknowledged_providers: BTreeSet<Provider>,
}

/// Process-lifetime advisory lock for the bridge's single durable producer state.
pub(crate) struct RuntimeLock {
    _file: File,
}

impl RuntimeLock {
    pub(crate) fn acquire(state_path: &Path) -> Result<Self, StateError> {
        let parent = state_path.parent().ok_or(StateError::UnsafePath)?;
        let name = state_path
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or(StateError::UnsafePath)?;
        let path = parent.join(format!("{name}.lock"));
        let descriptor = open(
            &path,
            OFlags::CREATE | OFlags::RDWR | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::from_raw_mode(0o600),
        )
        .map_err(|error| StateError::Io(std::io::Error::from(error)))?;
        let file = File::from(descriptor);
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        flock(&file, FlockOperation::NonBlockingLockExclusive).map_err(|error| {
            if error == rustix::io::Errno::WOULDBLOCK {
                StateError::AlreadyRunning
            } else {
                StateError::Io(std::io::Error::from(error))
            }
        })?;
        Ok(Self { _file: file })
    }
}

impl BridgeState {
    /// Creates a state file exactly once. Existing state is never overwritten.
    ///
    /// # Errors
    ///
    /// Returns an error for an existing target or durable filesystem failure.
    pub fn initialize(path: &Path) -> Result<Self, StateError> {
        if path.exists() {
            return Err(StateError::AlreadyExists);
        }
        let state = Self::new();
        create_json_once(path, &state)?;
        Ok(state)
    }

    /// Loads and validates durable bridge state.
    ///
    /// # Errors
    ///
    /// Returns an error for missing, unsafe, corrupt, or incompatible state.
    pub fn load(path: &Path) -> Result<Self, StateError> {
        let metadata = std::fs::symlink_metadata(path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                StateError::Missing
            } else {
                StateError::Io(error)
            }
        })?;
        if !metadata.file_type().is_file() {
            return Err(StateError::UnsafePath);
        }
        let state: Self = serde_json::from_reader(File::open(path)?)?;
        state.validate()?;
        Ok(state)
    }

    /// Persists the complete cursor/pending state by atomic same-directory replacement.
    ///
    /// # Errors
    ///
    /// Returns an error for unsafe paths, invalid state, or durable I/O failure.
    pub fn persist(&self, path: &Path) -> Result<(), StateError> {
        self.validate()?;
        atomic_write_json(path, self)
    }

    /// Advances the durable producer generation for a new bridge process.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::CounterOverflow`] when a monotonic counter is exhausted.
    pub fn begin_boot(&mut self) -> Result<(), StateError> {
        self.boot_count = self
            .boot_count
            .checked_add(1)
            .ok_or(StateError::CounterOverflow)?;
        for cursor in self.providers.values_mut() {
            cursor.connection_generation = cursor
                .connection_generation
                .checked_add(1)
                .ok_or(StateError::CounterOverflow)?;
            cursor.sequence = 0;
        }
        if let Some(failure) = self.active_source_failure.as_mut() {
            failure.acknowledged_providers.clear();
        }
        Ok(())
    }

    /// Starts or resumes a durable two-provider source-failure clearing round.
    pub fn begin_source_failure(&mut self, fingerprint: String) {
        if self
            .active_source_failure
            .as_ref()
            .is_none_or(|failure| failure.fingerprint != fingerprint)
        {
            self.active_source_failure = Some(SourceFailureProgress {
                fingerprint,
                acknowledged_providers: BTreeSet::new(),
            });
        }
    }

    /// Clears provider progress after reconnect so the core receives a full fence again.
    pub fn reset_source_failure_progress(&mut self) -> bool {
        if let Some(failure) = self.active_source_failure.as_mut() {
            let changed = !failure.acknowledged_providers.is_empty();
            failure.acknowledged_providers.clear();
            changed
        } else {
            false
        }
    }

    pub fn source_failure_acknowledged(&self, provider: Provider) -> bool {
        self.active_source_failure
            .as_ref()
            .is_some_and(|failure| failure.acknowledged_providers.contains(&provider))
    }

    /// Reserves the next provider cursor, advancing generation on a source epoch change.
    ///
    /// # Errors
    ///
    /// Returns an error for a missing provider or counter overflow.
    pub fn next_cursor(
        &mut self,
        provider: Provider,
        source_epoch: Option<String>,
    ) -> Result<(u64, u64), StateError> {
        let cursor = self
            .providers
            .get_mut(&provider)
            .ok_or(StateError::ProviderSet)?;
        if source_epoch.is_some() && source_epoch != cursor.source_epoch {
            cursor.connection_generation = cursor
                .connection_generation
                .checked_add(1)
                .ok_or(StateError::CounterOverflow)?;
            cursor.sequence = 0;
            cursor.source_epoch = source_epoch;
        }
        cursor.sequence = cursor
            .sequence
            .checked_add(1)
            .ok_or(StateError::CounterOverflow)?;
        Ok((cursor.connection_generation, cursor.sequence))
    }

    /// Reports whether a provider source epoch would advance the local generation.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::ProviderSet`] when the provider cursor is absent.
    pub fn source_epoch_changed(
        &self,
        provider: Provider,
        source_epoch: Option<&str>,
    ) -> Result<bool, StateError> {
        let cursor = self
            .providers
            .get(&provider)
            .ok_or(StateError::ProviderSet)?;
        Ok(source_epoch.is_some() && source_epoch != cursor.source_epoch.as_deref())
    }

    /// Stores the exact envelope that must be retried until its outcome is known.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::PendingExists`] when another frame is already in flight.
    pub fn set_pending(
        &mut self,
        provider: Provider,
        source_fingerprint: String,
        semantic_hash: String,
        purpose: PendingPurpose,
        envelope: IngressEnvelopeV1,
    ) -> Result<(), StateError> {
        if self.pending.is_some() {
            return Err(StateError::PendingExists);
        }
        self.pending = Some(PendingFrame {
            provider,
            source_fingerprint,
            semantic_hash,
            purpose,
            rejected_attempts: 0,
            envelope,
        });
        Ok(())
    }

    /// Records one typed core rejection against the unchanged pending envelope.
    ///
    /// # Errors
    ///
    /// Returns an error when no frame is pending or the counter overflows.
    pub fn record_rejection(&mut self) -> Result<u32, StateError> {
        let pending = self.pending.as_mut().ok_or(StateError::NoPending)?;
        pending.rejected_attempts = pending
            .rejected_attempts
            .checked_add(1)
            .ok_or(StateError::CounterOverflow)?;
        Ok(pending.rejected_attempts)
    }

    /// Advances the ACK cursor only when the core message identity matches exactly.
    ///
    /// # Errors
    ///
    /// Returns an error for a missing pending frame, mismatched ID, or provider state.
    pub fn acknowledge(
        &mut self,
        message_id: &str,
        acknowledged_at: DateTime<Utc>,
    ) -> Result<(), StateError> {
        let pending = self.pending.as_ref().ok_or(StateError::NoPending)?;
        if pending.envelope.message_id.as_str() != message_id {
            return Err(StateError::AckMismatch);
        }
        let provider = pending.provider;
        let semantic_hash = pending.semantic_hash.clone();
        if pending.purpose == PendingPurpose::SourceFailure {
            let progress = self
                .active_source_failure
                .as_mut()
                .ok_or(StateError::SourceFailureMismatch)?;
            if progress.fingerprint != pending.source_fingerprint {
                return Err(StateError::SourceFailureMismatch);
            }
            progress.acknowledged_providers.insert(provider);
        }
        let cursor = self
            .providers
            .get_mut(&provider)
            .ok_or(StateError::ProviderSet)?;
        cursor.last_ack_message_id = Some(message_id.to_owned());
        cursor.last_ack_at = Some(acknowledged_at);
        cursor.last_semantic_hash = Some(semantic_hash);
        self.pending = None;
        Ok(())
    }

    pub fn complete_source(&mut self, fingerprint: String, source_at: DateTime<Utc>) {
        self.active_source_failure = None;
        self.last_completed_source_fingerprint = Some(fingerprint);
        self.last_completed_source_at = Some(source_at);
    }

    fn new() -> Self {
        let providers = [Provider::Schwab, Provider::Ibkr]
            .into_iter()
            .map(|provider| {
                (
                    provider,
                    ProviderCursor {
                        connection_generation: 0,
                        sequence: 0,
                        source_epoch: None,
                        last_semantic_hash: None,
                        last_ack_message_id: None,
                        last_ack_at: None,
                    },
                )
            })
            .collect();
        Self {
            schema_version: STATE_SCHEMA.to_owned(),
            bridge_id: Uuid::new_v4().to_string(),
            boot_count: 0,
            providers,
            pending: None,
            active_source_failure: None,
            last_completed_source_fingerprint: None,
            last_completed_source_at: None,
        }
    }

    fn validate(&self) -> Result<(), StateError> {
        if self.schema_version != STATE_SCHEMA {
            return Err(StateError::SchemaMismatch);
        }
        if self.providers.len() != 2
            || !self.providers.contains_key(&Provider::Schwab)
            || !self.providers.contains_key(&Provider::Ibkr)
        {
            return Err(StateError::ProviderSet);
        }
        if self.boot_count > 0
            && self
                .providers
                .values()
                .any(|cursor| cursor.connection_generation == 0)
        {
            return Err(StateError::ProviderSet);
        }
        if self.active_source_failure.as_ref().is_some_and(|failure| {
            failure.fingerprint.is_empty()
                || failure
                    .acknowledged_providers
                    .iter()
                    .any(|provider| !self.providers.contains_key(provider))
        }) {
            return Err(StateError::ProviderSet);
        }
        if self.pending.as_ref().is_some_and(|pending| {
            pending.purpose == PendingPurpose::SourceFailure
                && self
                    .active_source_failure
                    .as_ref()
                    .is_none_or(|failure| failure.fingerprint != pending.source_fingerprint)
        }) {
            return Err(StateError::SourceFailureMismatch);
        }
        if self.pending.as_ref().is_some_and(|pending| {
            pending.source_fingerprint.is_empty()
                || pending.semantic_hash.is_empty()
                || !self.providers.contains_key(&pending.provider)
                || pending.envelope.validate().is_err()
                || !matches!(
                    &pending.envelope.message,
                    IngressMessageV1::QuoteBatch(batch) if batch.provider == pending.provider
                )
                || (pending.purpose == PendingPurpose::SourceFailure
                    && self.active_source_failure.as_ref().is_some_and(|failure| {
                        failure.acknowledged_providers.contains(&pending.provider)
                    }))
        }) {
            return Err(StateError::InvalidPending);
        }
        Ok(())
    }
}

pub(crate) fn atomic_write_json<T: Serialize>(path: &Path, value: &T) -> Result<(), StateError> {
    if let Ok(metadata) = std::fs::symlink_metadata(path)
        && !metadata.file_type().is_file()
    {
        return Err(StateError::UnsafePath);
    }
    let parent = path.parent().ok_or(StateError::UnsafePath)?;
    std::fs::create_dir_all(parent)?;
    let temporary = temporary_path(path);
    let write_result = (|| -> Result<(), StateError> {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        serde_json::to_writer(&mut file, value)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        std::fs::rename(&temporary, path)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if write_result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    write_result
}

fn create_json_once<T: Serialize>(path: &Path, value: &T) -> Result<(), StateError> {
    let parent = path.parent().ok_or(StateError::UnsafePath)?;
    std::fs::create_dir_all(parent)?;
    let temporary = temporary_path(path);
    let write_result = (|| -> Result<(), StateError> {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        serde_json::to_writer(&mut file, value)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        std::fs::hard_link(&temporary, path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                StateError::AlreadyExists
            } else {
                StateError::Io(error)
            }
        })?;
        std::fs::remove_file(&temporary)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if write_result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    write_result
}

fn temporary_path(path: &Path) -> PathBuf {
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("bridge-state");
    path.with_file_name(format!(".{name}.{}.tmp", Uuid::new_v4()))
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn initialization_is_explicit_and_no_clobber() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("bridge.json");
        BridgeState::initialize(&path).unwrap();
        assert!(matches!(
            BridgeState::initialize(&path),
            Err(StateError::AlreadyExists)
        ));
        assert_eq!(BridgeState::load(&path).unwrap().boot_count, 0);
    }

    #[test]
    fn boot_and_source_epoch_advance_monotonically() {
        let mut state = BridgeState::new();
        state.begin_boot().unwrap();
        assert_eq!(
            state
                .next_cursor(Provider::Ibkr, Some("428".into()))
                .unwrap(),
            (2, 1)
        );
        assert_eq!(
            state
                .next_cursor(Provider::Ibkr, Some("428".into()))
                .unwrap(),
            (2, 2)
        );
        assert_eq!(
            state
                .next_cursor(Provider::Ibkr, Some("429".into()))
                .unwrap(),
            (3, 1)
        );
    }

    #[test]
    fn runtime_lock_fences_a_second_bridge_process() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("bridge.json");
        BridgeState::initialize(&path).unwrap();
        let first = RuntimeLock::acquire(&path).unwrap();
        assert!(matches!(
            RuntimeLock::acquire(&path),
            Err(StateError::AlreadyRunning)
        ));
        drop(first);
        RuntimeLock::acquire(&path).unwrap();
    }

    #[test]
    fn source_failure_progress_is_durable_and_recovery_clears_it() {
        let mut state = BridgeState::new();
        state.begin_source_failure("failure-a".to_owned());
        state
            .active_source_failure
            .as_mut()
            .unwrap()
            .acknowledged_providers
            .insert(Provider::Schwab);
        state.begin_source_failure("failure-a".to_owned());
        assert!(state.source_failure_acknowledged(Provider::Schwab));

        state.begin_source_failure("failure-b".to_owned());
        assert!(!state.source_failure_acknowledged(Provider::Schwab));
        state
            .active_source_failure
            .as_mut()
            .unwrap()
            .acknowledged_providers
            .insert(Provider::Ibkr);
        assert!(state.reset_source_failure_progress());
        assert!(!state.source_failure_acknowledged(Provider::Ibkr));

        state.complete_source(
            "snapshot".to_owned(),
            "2026-08-01T14:30:00Z".parse().unwrap(),
        );
        assert!(state.active_source_failure.is_none());
    }
}
