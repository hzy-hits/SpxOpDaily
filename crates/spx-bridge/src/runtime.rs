use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use chrono::Utc;
use spx_domain::{
    AckStatus, CoreAckDisposition, CoreAckReason, INGRESS_SCHEMA_VERSION, IngressEnvelopeV1,
    IngressMessageV1, Provider, Token, Validate,
};
use thiserror::Error;
use tracing::{error, info, warn};

use crate::BridgeConfig;
use crate::client::{ClientError, CoreClient};
use crate::health::{BridgeHealth, BridgePhase};
use crate::legacy::{LegacyDocument, LegacyIbkrHealth, read_ibkr_health, read_snapshot};
use crate::mapper::{
    MapError, MappingStats, map_provider_batch, map_source_failure_batch, semantic_hash,
};
use crate::state::{BridgeState, PendingPurpose, RuntimeLock, StateError};

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("bridge state failed: {0}")]
    State(#[from] StateError),
    #[error("normalized source mapping failed: {0}")]
    Map(#[from] MapError),
    #[error("core permanently rejected ingress: {0:?}")]
    PermanentRejection(CoreAckReason),
    #[error("core rejected identical ingress too many times")]
    RejectionLimit,
    #[error("core reported a stale mirror cursor; state rollback is possible")]
    StaleCursor,
    #[error("normalized source timestamp regressed")]
    SourceTimeRegression,
    #[error("ingress envelope construction failed: {0}")]
    Envelope(#[from] spx_domain::DomainError),
    #[error("bridge produced a locally invalid ingress frame: {0}")]
    LocalIngress(ClientError),
}

pub struct BridgeRuntime {
    config: BridgeConfig,
    state: BridgeState,
    health: BridgeHealth,
    mappings: BTreeMap<Provider, MappingStats>,
    client: Option<CoreClient>,
    needs_resync: bool,
    next_connect_not_before: Option<Instant>,
    _runtime_lock: RuntimeLock,
}

impl BridgeRuntime {
    /// Opens an explicitly initialized bridge state and fences this process boot.
    ///
    /// # Errors
    ///
    /// Returns an error when state cannot be loaded, advanced, or durably persisted.
    pub fn open(config: BridgeConfig) -> Result<Self, RuntimeError> {
        let runtime_lock = RuntimeLock::acquire(&config.state_path)?;
        let mut state = BridgeState::load(&config.state_path)?;
        state.begin_boot()?;
        state.persist(&config.state_path)?;
        let now = Utc::now();
        let health = BridgeHealth::new(&config.source_snapshot_path, &state, now);
        health.persist(&config.health_path)?;
        Ok(Self {
            config,
            state,
            health,
            mappings: BTreeMap::new(),
            client: None,
            needs_resync: true,
            next_connect_not_before: None,
            _runtime_lock: runtime_lock,
        })
    }

    /// Mirrors complete provider snapshots until a termination signal is observed.
    ///
    /// Transport uncertainty is retried with the exact persisted envelope. Contract
    /// poison, cursor rollback, or counter/state corruption halts the bridge.
    ///
    /// # Errors
    ///
    /// Returns an error only for a permanent safety boundary failure.
    pub fn run(mut self, stop: &Arc<AtomicBool>) -> Result<(), RuntimeError> {
        while !stop.load(Ordering::Relaxed) {
            if let Err(failure) = self.tick(stop) {
                return self.halt(failure);
            }
        }
        self.health.phase = BridgePhase::Degraded;
        self.health.socket_connected = false;
        self.health.last_error = Some("bridge stopped by signal".to_owned());
        self.persist_health();
        Ok(())
    }

    fn tick(&mut self, stop: &AtomicBool) -> Result<(), RuntimeError> {
        if self.client.is_none() {
            self.connect(stop)?;
            if self.client.is_none() {
                return Ok(());
            }
        }
        if self.state.pending.is_some() {
            match self.send_pending()? {
                SendDisposition::Acknowledged => {}
                SendDisposition::Reconnect => {
                    self.disconnect("core rejected or disconnected before acknowledgement");
                    return Ok(());
                }
            }
        }
        let document = match read_snapshot(
            &self.config.source_snapshot_path,
            self.config.source_max_bytes,
        ) {
            Ok(document) => document,
            Err(failure) => {
                self.handle_source_failure(&failure.to_string())?;
                Self::sleep(self.config.poll_interval_ms, stop);
                return Ok(());
            }
        };
        self.handle_document(document)?;
        Self::sleep(self.config.poll_interval_ms, stop);
        Ok(())
    }

    fn handle_document(&mut self, document: LegacyDocument) -> Result<(), RuntimeError> {
        let source_at = match document.snapshot.source_at() {
            Ok(value) => value,
            Err(failure) => return self.handle_source_failure(&failure.to_string()),
        };
        let unchanged =
            self.state.last_completed_source_fingerprint.as_deref() == Some(&document.fingerprint);
        if unchanged && !self.needs_resync && self.state.active_source_failure.is_none() {
            self.health.phase = BridgePhase::Ready;
            self.health.socket_connected = true;
            self.health.observe_source(
                document.fingerprint,
                source_at,
                document.byte_len,
                Utc::now(),
            );
            self.persist_health();
            return Ok(());
        }
        if self
            .state
            .last_completed_source_at
            .is_some_and(|previous| source_at < previous)
        {
            return Err(RuntimeError::SourceTimeRegression);
        }
        let ibkr_health = self.read_ibkr_health_fail_closed();
        self.health.phase = if self.needs_resync {
            BridgePhase::SocketSyncFence
        } else {
            BridgePhase::SnapshotSync
        };
        self.health.socket_connected = true;
        self.health.counters.source_documents =
            self.health.counters.source_documents.saturating_add(1);
        self.health.observe_source(
            document.fingerprint.clone(),
            source_at,
            document.byte_len,
            Utc::now(),
        );
        self.persist_health();
        match self.process_document(&document, ibkr_health.as_ref()) {
            Ok(()) => self.complete_document(&document, source_at),
            Err(ProcessError::Transport(failure)) => {
                self.disconnect(&failure.to_string());
                Ok(())
            }
            Err(ProcessError::Fatal(failure)) => Err(failure),
        }
    }

    fn read_ibkr_health_fail_closed(&self) -> Option<LegacyIbkrHealth> {
        match read_ibkr_health(
            &self.config.ibkr_health_path,
            self.config.source_max_bytes.min(1_048_576),
        ) {
            Ok(value) => Some(value),
            Err(failure) => {
                warn!(error = %failure, "IBKR health is unavailable; IBKR mirror will fail closed");
                None
            }
        }
    }

    fn complete_document(
        &mut self,
        document: &LegacyDocument,
        source_at: chrono::DateTime<Utc>,
    ) -> Result<(), RuntimeError> {
        self.state
            .complete_source(document.fingerprint.clone(), source_at);
        self.state.persist(&self.config.state_path)?;
        self.needs_resync = false;
        self.health.phase = BridgePhase::Ready;
        self.health.last_error = None;
        self.health.last_resync_at = Some(Utc::now());
        self.health
            .refresh_state(&self.state, &self.mappings, Utc::now());
        self.persist_health();
        Ok(())
    }

    fn connect(&mut self, stop: &AtomicBool) -> Result<(), RuntimeError> {
        self.health.phase = BridgePhase::SocketSyncFence;
        if let Some(not_before) = self.next_connect_not_before {
            Self::sleep_duration(not_before.saturating_duration_since(Instant::now()), stop);
            if stop.load(Ordering::Relaxed) {
                return Ok(());
            }
            self.next_connect_not_before = None;
        }
        match CoreClient::connect(
            &self.config.socket_path,
            Duration::from_millis(self.config.io_timeout_ms),
            self.config.max_frame_bytes,
        ) {
            Ok(client) => {
                if self.state.reset_source_failure_progress() {
                    self.state.persist(&self.config.state_path)?;
                }
                self.client = Some(client);
                self.needs_resync = true;
                self.next_connect_not_before = None;
                self.health.socket_connected = true;
                self.health.last_error = None;
                self.health.counters.reconnects = self.health.counters.reconnects.saturating_add(1);
                self.persist_health();
                info!(path = %self.config.socket_path.display(), "normalized bridge connected to core");
            }
            Err(failure) => {
                self.health.phase = BridgePhase::Degraded;
                self.health.socket_connected = false;
                self.health.last_error = Some(failure.to_string());
                self.persist_health();
                self.schedule_reconnect();
            }
        }
        Ok(())
    }

    fn process_document(
        &mut self,
        document: &LegacyDocument,
        ibkr_health: Option<&LegacyIbkrHealth>,
    ) -> Result<(), ProcessError> {
        for provider in [Provider::Schwab, Provider::Ibkr] {
            let now = Utc::now();
            let source_epoch = source_epoch(provider, document, ibkr_health);
            let epoch_changed = self
                .state
                .source_epoch_changed(provider, source_epoch.as_deref())
                .map_err(RuntimeError::from)?;
            let mut proposed = self.state.clone();
            let (generation, sequence) = proposed
                .next_cursor(provider, source_epoch)
                .map_err(RuntimeError::from)?;
            let (batch, stats) = if provider == Provider::Ibkr && ibkr_health.is_none() {
                (
                    map_source_failure_batch(
                        provider,
                        generation,
                        sequence,
                        &document.fingerprint,
                        now,
                    )
                    .map_err(RuntimeError::from)?,
                    MappingStats::default(),
                )
            } else {
                let mapped = map_provider_batch(
                    &document.snapshot,
                    ibkr_health,
                    provider,
                    generation,
                    sequence,
                    &document.fingerprint,
                    now,
                )
                .map_err(RuntimeError::from)?;
                (mapped.batch, mapped.stats)
            };
            let semantic_hash = semantic_hash(&batch).map_err(RuntimeError::from)?;
            let unchanged = self
                .state
                .providers
                .get(&provider)
                .and_then(|cursor| cursor.last_semantic_hash.as_deref())
                == Some(semantic_hash.as_str());
            self.mappings.insert(provider, stats);
            if unchanged
                && !epoch_changed
                && !self.needs_resync
                && self.state.active_source_failure.is_none()
            {
                continue;
            }
            let message_id = Token::new(format!("message:{}", batch.batch_id), "message_id")
                .map_err(RuntimeError::from)?;
            let envelope = IngressEnvelopeV1 {
                schema_version: INGRESS_SCHEMA_VERSION.to_owned(),
                message_id,
                emitted_at: now,
                message: IngressMessageV1::QuoteBatch(batch),
            };
            envelope.validate().map_err(RuntimeError::from)?;
            proposed
                .set_pending(
                    provider,
                    document.fingerprint.clone(),
                    semantic_hash,
                    PendingPurpose::Snapshot,
                    envelope,
                )
                .map_err(RuntimeError::from)?;
            proposed
                .persist(&self.config.state_path)
                .map_err(RuntimeError::from)?;
            self.state = proposed;
            self.health
                .refresh_state(&self.state, &self.mappings, Utc::now());
            self.persist_health();
            match self.send_pending().map_err(ProcessError::Fatal)? {
                SendDisposition::Acknowledged => {}
                SendDisposition::Reconnect => {
                    return Err(ProcessError::Transport(ClientError::Io(
                        std::io::Error::new(
                            std::io::ErrorKind::ConnectionAborted,
                            "core requested ingress retry",
                        ),
                    )));
                }
            }
        }
        Ok(())
    }

    fn send_pending(&mut self) -> Result<SendDisposition, RuntimeError> {
        let envelope = self
            .state
            .pending
            .as_ref()
            .ok_or(StateError::NoPending)?
            .envelope
            .clone();
        let Some(client) = self.client.as_mut() else {
            return Ok(SendDisposition::Reconnect);
        };
        let ack = match client.send(&envelope) {
            Ok(ack) => ack,
            Err(failure) if failure.is_preflight_failure() => {
                return Err(RuntimeError::LocalIngress(failure));
            }
            Err(failure) => {
                warn!(error = %failure, message_id = %envelope.message_id, "ingress outcome unknown; preserving exact pending frame");
                return Ok(SendDisposition::Reconnect);
            }
        };
        match ack.status {
            AckStatus::Accepted => {
                if ack.disposition == Some(CoreAckDisposition::StaleBatch) {
                    self.health.counters.stale_acks =
                        self.health.counters.stale_acks.saturating_add(1);
                    return Err(RuntimeError::StaleCursor);
                }
                if matches!(
                    ack.disposition,
                    Some(CoreAckDisposition::DuplicateBatch | CoreAckDisposition::DuplicateIngress)
                ) {
                    self.health.counters.duplicate_acks =
                        self.health.counters.duplicate_acks.saturating_add(1);
                }
                self.state.acknowledge(
                    ack.message_id
                        .as_ref()
                        .expect("validated accepted ack has message id")
                        .as_str(),
                    Utc::now(),
                )?;
                self.state.persist(&self.config.state_path)?;
                self.health.counters.accepted_frames =
                    self.health.counters.accepted_frames.saturating_add(1);
                self.health.last_matching_ack_at = Some(Utc::now());
                self.health
                    .refresh_state(&self.state, &self.mappings, Utc::now());
                self.persist_health();
                Ok(SendDisposition::Acknowledged)
            }
            AckStatus::Rejected => {
                self.health.counters.rejected_acks =
                    self.health.counters.rejected_acks.saturating_add(1);
                match ack.reason_code {
                    CoreAckReason::InvalidContractJson | CoreAckReason::InvalidFrameSize => {
                        Err(RuntimeError::PermanentRejection(ack.reason_code))
                    }
                    CoreAckReason::ProcessingRejected => {
                        let attempts = self.state.record_rejection()?;
                        self.state.persist(&self.config.state_path)?;
                        self.persist_health();
                        if attempts >= self.config.max_rejected_attempts {
                            Err(RuntimeError::RejectionLimit)
                        } else {
                            Ok(SendDisposition::Reconnect)
                        }
                    }
                    CoreAckReason::ServerBusy => Ok(SendDisposition::Reconnect),
                    CoreAckReason::Accepted => {
                        Err(RuntimeError::PermanentRejection(CoreAckReason::Accepted))
                    }
                }
            }
        }
    }

    fn handle_source_failure(&mut self, reason: &str) -> Result<(), RuntimeError> {
        self.health.phase = BridgePhase::Degraded;
        self.health.counters.source_errors = self.health.counters.source_errors.saturating_add(1);
        self.health.last_error = Some(reason.to_owned());
        self.persist_health();
        let fingerprint = failure_fingerprint(reason);
        self.state.begin_source_failure(fingerprint.clone());
        self.state.persist(&self.config.state_path)?;
        for provider in [Provider::Schwab, Provider::Ibkr] {
            if self.state.source_failure_acknowledged(provider) {
                continue;
            }
            let mut proposed = self.state.clone();
            let (generation, sequence) = proposed.next_cursor(provider, None)?;
            let now = Utc::now();
            let batch =
                map_source_failure_batch(provider, generation, sequence, &fingerprint, now)?;
            let semantic_hash = semantic_hash(&batch)?;
            let envelope = IngressEnvelopeV1 {
                schema_version: INGRESS_SCHEMA_VERSION.to_owned(),
                message_id: Token::new(format!("message:{}", batch.batch_id), "message_id")?,
                emitted_at: now,
                message: IngressMessageV1::QuoteBatch(batch),
            };
            envelope.validate()?;
            proposed.set_pending(
                provider,
                fingerprint.clone(),
                semantic_hash,
                PendingPurpose::SourceFailure,
                envelope,
            )?;
            proposed.persist(&self.config.state_path)?;
            self.state = proposed;
            match self.send_pending()? {
                SendDisposition::Acknowledged => {}
                SendDisposition::Reconnect => {
                    self.disconnect("source failure fence outcome is unknown");
                    return Ok(());
                }
            }
        }
        self.needs_resync = false;
        Ok(())
    }

    fn disconnect(&mut self, reason: &str) {
        self.client = None;
        self.needs_resync = true;
        self.health.phase = BridgePhase::Degraded;
        self.health.socket_connected = false;
        self.health.last_error = Some(reason.to_owned());
        self.schedule_reconnect();
        self.persist_health();
        warn!(
            reason,
            "normalized bridge disconnected; exact pending frame retained"
        );
    }

    fn halt<T>(&mut self, failure: RuntimeError) -> Result<T, RuntimeError> {
        self.health.phase = BridgePhase::Halted;
        self.health.socket_connected = self.client.is_some();
        self.health.last_error = Some(failure.to_string());
        self.health
            .refresh_state(&self.state, &self.mappings, Utc::now());
        self.persist_health();
        error!(error = %failure, "normalized bridge halted");
        Err(failure)
    }

    fn persist_health(&self) {
        if let Err(failure) = self.health.persist(&self.config.health_path) {
            error!(error = %failure, "failed to persist bridge health projection");
        }
    }

    fn sleep(milliseconds: u64, stop: &AtomicBool) {
        Self::sleep_duration(Duration::from_millis(milliseconds), stop);
    }

    fn sleep_duration(duration: Duration, stop: &AtomicBool) {
        let slice = Duration::from_millis(100);
        let mut remaining = duration;
        while !stop.load(Ordering::Relaxed) && !remaining.is_zero() {
            let duration = remaining.min(slice);
            thread::sleep(duration);
            remaining = remaining.saturating_sub(duration);
        }
    }

    fn schedule_reconnect(&mut self) {
        self.next_connect_not_before =
            Instant::now().checked_add(Duration::from_millis(self.config.reconnect_backoff_ms));
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SendDisposition {
    Acknowledged,
    Reconnect,
}

#[derive(Debug, Error)]
enum ProcessError {
    #[error("transport failed: {0}")]
    Transport(ClientError),
    #[error("fatal bridge failure: {0}")]
    Fatal(#[from] RuntimeError),
}

fn source_epoch(
    provider: Provider,
    document: &LegacyDocument,
    ibkr_health: Option<&LegacyIbkrHealth>,
) -> Option<String> {
    if provider != Provider::Ibkr {
        return None;
    }
    ibkr_health
        .map(|health| format!("ibkr-stream:{}", health.connection_generation))
        .or_else(|| {
            document
                .snapshot
                .quotes
                .iter()
                .filter(|quote| quote.provider == "ibkr")
                .filter_map(|quote| quote.source_session.clone())
                .max()
        })
}

fn failure_fingerprint(reason: &str) -> String {
    use sha2::{Digest as _, Sha256};
    let digest = Sha256::digest(reason.as_bytes());
    let mut result = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut result, "{byte:02x}").expect("writing to String is infallible");
    }
    result
}

#[cfg(test)]
mod tests {
    use std::io::{Read as _, Write as _};
    use std::os::unix::net::UnixListener;
    use std::sync::{Arc, Mutex};

    use chrono::TimeDelta;
    use spx_domain::{CoreAckV1, OperationalState};
    use tempfile::TempDir;

    use super::*;

    fn config(temp: &TempDir) -> BridgeConfig {
        BridgeConfig {
            source_snapshot_path: temp.path().join("state.json"),
            ibkr_health_path: temp.path().join("ibkr.json"),
            socket_path: temp.path().join("core.sock"),
            state_path: temp.path().join("bridge-state.json"),
            health_path: temp.path().join("bridge-health.json"),
            poll_interval_ms: 100,
            reconnect_backoff_ms: 100,
            io_timeout_ms: 1_000,
            max_frame_bytes: 1_048_576,
            source_max_bytes: 1_048_576,
            max_rejected_attempts: 3,
        }
    }

    fn write_sources(config: &BridgeConfig) -> Vec<u8> {
        let source_at = Utc::now() - TimeDelta::seconds(1);
        let snapshot = serde_json::to_vec(&serde_json::json!({
            "created_at": source_at,
            "as_of": source_at,
            "quotes": [],
            "provider_states": [
                {
                    "provider": "schwab",
                    "status": "available",
                    "checked_at": source_at,
                    "reason": null,
                    "connected": true,
                    "authenticated": true,
                    "latency_ms": 1.0
                },
                {
                    "provider": "ibkr",
                    "status": "available",
                    "checked_at": source_at,
                    "reason": null,
                    "connected": true,
                    "authenticated": true,
                    "latency_ms": 1.0
                }
            ]
        }))
        .unwrap();
        std::fs::write(&config.source_snapshot_path, &snapshot).unwrap();
        std::fs::write(
            &config.ibkr_health_path,
            serde_json::to_vec(&serde_json::json!({
                "observed_at": source_at,
                "connection_generation": 7,
                "connected": true,
                "data_plane_healthy": true,
                "circuit_state": "closed",
                "reason": null
            }))
            .unwrap(),
        )
        .unwrap();
        snapshot
    }

    #[test]
    fn source_failure_clears_both_providers_and_same_snapshot_recovery_resyncs() {
        let temp = TempDir::new().unwrap();
        let config = config(&temp);
        let original_snapshot = write_sources(&config);
        BridgeState::initialize(&config.state_path).unwrap();
        let listener = UnixListener::bind(&config.socket_path).unwrap();
        let observed = Arc::new(Mutex::new(Vec::new()));
        let server_observed = Arc::clone(&observed);
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            loop {
                let mut length = [0_u8; 4];
                match stream.read_exact(&mut length) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => break,
                    Err(error) => panic!("failed to read request length: {error}"),
                }
                let mut bytes = vec![0_u8; u32::from_be_bytes(length) as usize];
                stream.read_exact(&mut bytes).unwrap();
                let envelope: IngressEnvelopeV1 = serde_json::from_slice(&bytes).unwrap();
                let IngressMessageV1::QuoteBatch(batch) = &envelope.message else {
                    panic!("bridge sent a non-quote ingress message");
                };
                server_observed
                    .lock()
                    .unwrap()
                    .push((batch.provider, batch.provider_state.operational));
                let ack =
                    CoreAckV1::accepted(envelope.message_id, CoreAckDisposition::Applied, None);
                let encoded = serde_json::to_vec(&ack).unwrap();
                stream
                    .write_all(&u32::try_from(encoded.len()).unwrap().to_be_bytes())
                    .unwrap();
                stream.write_all(&encoded).unwrap();
            }
        });

        let mut runtime = BridgeRuntime::open(config.clone()).unwrap();
        let stop = AtomicBool::new(false);
        runtime.tick(&stop).unwrap();
        std::fs::remove_file(&config.source_snapshot_path).unwrap();
        runtime.tick(&stop).unwrap();
        std::fs::write(&config.source_snapshot_path, original_snapshot).unwrap();
        runtime.tick(&stop).unwrap();

        assert!(runtime.state.pending.is_none());
        assert!(runtime.state.active_source_failure.is_none());
        runtime.disconnect("test transport uncertainty");
        assert!(
            runtime
                .next_connect_not_before
                .is_some_and(|not_before| not_before > Instant::now())
        );
        drop(runtime);
        server.join().unwrap();
        let observed = observed.lock().unwrap();
        assert_eq!(observed.len(), 6);
        for round in observed.chunks_exact(2) {
            assert_eq!(round[0].0, Provider::Schwab);
            assert_eq!(round[1].0, Provider::Ibkr);
        }
        assert!(
            observed[2..4]
                .iter()
                .all(|(_, state)| *state == OperationalState::Unavailable)
        );
        assert!(
            observed[4..6]
                .iter()
                .all(|(_, state)| *state != OperationalState::Unavailable)
        );
    }
}
