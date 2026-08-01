use std::collections::BTreeMap;
use std::path::Path;

use chrono::{DateTime, Utc};
use serde::Serialize;
use spx_domain::Provider;

use crate::mapper::MappingStats;
use crate::state::{BridgeState, StateError, atomic_write_json};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BridgePhase {
    Boot,
    SocketSyncFence,
    SnapshotSync,
    Ready,
    Degraded,
    Halted,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct BridgeCounters {
    pub source_documents: u64,
    pub accepted_frames: u64,
    pub duplicate_acks: u64,
    pub stale_acks: u64,
    pub rejected_acks: u64,
    pub reconnects: u64,
    pub source_errors: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProviderHealth {
    pub connection_generation: u64,
    pub sequence: u64,
    pub source_epoch: Option<String>,
    pub last_ack_message_id: Option<String>,
    pub last_ack_at: Option<DateTime<Utc>>,
    pub mapping: Option<MappingStats>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BridgeHealth {
    pub schema_version: &'static str,
    pub bridge_id: String,
    pub updated_at: DateTime<Utc>,
    pub phase: BridgePhase,
    pub socket_connected: bool,
    pub source_path: String,
    pub source_fingerprint: Option<String>,
    pub source_at: Option<DateTime<Utc>>,
    pub source_age_seconds: Option<f64>,
    pub source_bytes: Option<usize>,
    pub pending_message_id: Option<String>,
    pub last_matching_ack_at: Option<DateTime<Utc>>,
    pub last_resync_at: Option<DateTime<Utc>>,
    pub providers: BTreeMap<Provider, ProviderHealth>,
    pub counters: BridgeCounters,
    pub last_error: Option<String>,
}

impl BridgeHealth {
    pub fn new(source_path: &Path, state: &BridgeState, now: DateTime<Utc>) -> Self {
        Self {
            schema_version: "spx_normalized_bridge_health.v1",
            bridge_id: state.bridge_id.clone(),
            updated_at: now,
            phase: BridgePhase::Boot,
            socket_connected: false,
            source_path: source_path.display().to_string(),
            source_fingerprint: None,
            source_at: None,
            source_age_seconds: None,
            source_bytes: None,
            pending_message_id: state
                .pending
                .as_ref()
                .map(|pending| pending.envelope.message_id.to_string()),
            last_matching_ack_at: None,
            last_resync_at: None,
            providers: provider_health(state, &BTreeMap::new()),
            counters: BridgeCounters::default(),
            last_error: None,
        }
    }

    pub fn refresh_state(
        &mut self,
        state: &BridgeState,
        mappings: &BTreeMap<Provider, MappingStats>,
        now: DateTime<Utc>,
    ) {
        self.updated_at = now;
        self.pending_message_id = state
            .pending
            .as_ref()
            .map(|pending| pending.envelope.message_id.to_string());
        self.providers = provider_health(state, mappings);
    }

    pub fn observe_source(
        &mut self,
        fingerprint: String,
        source_at: DateTime<Utc>,
        byte_len: usize,
        now: DateTime<Utc>,
    ) {
        self.source_fingerprint = Some(fingerprint);
        self.source_at = Some(source_at);
        self.source_age_seconds = (now >= source_at).then(|| {
            (now - source_at)
                .to_std()
                .map_or(f64::INFINITY, |d| d.as_secs_f64())
        });
        self.source_bytes = Some(byte_len);
    }

    pub fn persist(&self, path: &Path) -> Result<(), StateError> {
        atomic_write_json(path, self)
    }
}

fn provider_health(
    state: &BridgeState,
    mappings: &BTreeMap<Provider, MappingStats>,
) -> BTreeMap<Provider, ProviderHealth> {
    state
        .providers
        .iter()
        .map(|(provider, cursor)| {
            (
                *provider,
                ProviderHealth {
                    connection_generation: cursor.connection_generation,
                    sequence: cursor.sequence,
                    source_epoch: cursor.source_epoch.clone(),
                    last_ack_message_id: cursor.last_ack_message_id.clone(),
                    last_ack_at: cursor.last_ack_at,
                    mapping: mappings.get(provider).cloned(),
                },
            )
        })
        .collect()
}
