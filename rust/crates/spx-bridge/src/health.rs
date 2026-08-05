use std::collections::BTreeMap;
use std::path::Path;

use chrono::{DateTime, Utc};
use serde::Serialize;
use spx_domain::{CoreAckDisposition, Provider};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResearchLaneStatus {
    Disabled,
    AwaitingSource,
    AwaitingAck,
    Accepted,
    Unchanged,
    Stale,
    Missing,
    Rejected,
    TransportUnknown,
}

impl ResearchLaneStatus {
    fn degrades_bridge(self) -> bool {
        !matches!(
            self,
            Self::Disabled
                | Self::AwaitingSource
                | Self::AwaitingAck
                | Self::Accepted
                | Self::Unchanged
                | Self::Missing
        )
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ResearchCounters {
    pub source_documents: u64,
    pub accepted_acks: u64,
    pub unchanged_acks: u64,
    pub stale_acks: u64,
    pub rejected_acks: u64,
    pub source_errors: u64,
    pub transport_errors: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ResearchLaneHealth {
    pub configured: bool,
    pub source_path: Option<String>,
    pub status: ResearchLaneStatus,
    pub source_schema_version: Option<String>,
    pub source_fingerprint: Option<String>,
    pub source_generated_at: Option<DateTime<Utc>>,
    pub source_age_seconds: Option<f64>,
    pub source_bytes: Option<usize>,
    pub last_ack_message_id: Option<String>,
    pub last_ack_disposition: Option<CoreAckDisposition>,
    pub last_ack_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub counters: ResearchCounters,
}

impl ResearchLaneHealth {
    fn new(source_path: Option<&Path>) -> Self {
        Self {
            configured: source_path.is_some(),
            source_path: source_path.map(|path| path.display().to_string()),
            status: if source_path.is_some() {
                ResearchLaneStatus::AwaitingSource
            } else {
                ResearchLaneStatus::Disabled
            },
            source_schema_version: None,
            source_fingerprint: None,
            source_generated_at: None,
            source_age_seconds: None,
            source_bytes: None,
            last_ack_message_id: None,
            last_ack_disposition: None,
            last_ack_at: None,
            last_error: None,
            counters: ResearchCounters::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeskMapLaneStatus {
    Disabled,
    AwaitingSource,
    AwaitingAck,
    Accepted,
    Unchanged,
    Stale,
    Expired,
    Missing,
    Rejected,
    TransportUnknown,
}

impl DeskMapLaneStatus {
    fn degrades_bridge(self) -> bool {
        !matches!(self, Self::Disabled | Self::Accepted | Self::Unchanged)
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct DeskMapCounters {
    pub source_documents: u64,
    pub accepted_acks: u64,
    pub unchanged_acks: u64,
    pub stale_acks: u64,
    pub rejected_acks: u64,
    pub source_errors: u64,
    pub transport_errors: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct DeskMapLaneHealth {
    pub configured: bool,
    pub source_path: Option<String>,
    pub status: DeskMapLaneStatus,
    pub source_schema_version: Option<String>,
    pub source_fingerprint: Option<String>,
    pub source_available_at: Option<DateTime<Utc>>,
    pub source_age_seconds: Option<f64>,
    pub source_bytes: Option<usize>,
    pub last_ack_message_id: Option<String>,
    pub last_ack_disposition: Option<CoreAckDisposition>,
    pub last_ack_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub counters: DeskMapCounters,
}

impl DeskMapLaneHealth {
    fn new(source_path: Option<&Path>) -> Self {
        Self {
            configured: source_path.is_some(),
            source_path: source_path.map(|path| path.display().to_string()),
            status: if source_path.is_some() {
                DeskMapLaneStatus::AwaitingSource
            } else {
                DeskMapLaneStatus::Disabled
            },
            source_schema_version: None,
            source_fingerprint: None,
            source_available_at: None,
            source_age_seconds: None,
            source_bytes: None,
            last_ack_message_id: None,
            last_ack_disposition: None,
            last_ack_at: None,
            last_error: None,
            counters: DeskMapCounters::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyDistributionLaneStatus {
    Disabled,
    AwaitingSource,
    AwaitingAck,
    Accepted,
    Unchanged,
    Stale,
    Expired,
    Missing,
    Rejected,
    TransportUnknown,
}

impl StrategyDistributionLaneStatus {
    fn degrades_bridge(self) -> bool {
        !matches!(
            self,
            Self::Disabled
                | Self::AwaitingSource
                | Self::AwaitingAck
                | Self::Accepted
                | Self::Unchanged
                | Self::Missing
        )
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct StrategyDistributionCounters {
    pub source_documents: u64,
    pub accepted_acks: u64,
    pub unchanged_acks: u64,
    pub stale_acks: u64,
    pub rejected_acks: u64,
    pub source_errors: u64,
    pub transport_errors: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyDistributionLaneHealth {
    pub configured: bool,
    pub source_path: Option<String>,
    pub status: StrategyDistributionLaneStatus,
    pub source_schema_version: Option<String>,
    pub source_document_id: Option<String>,
    pub source_fingerprint: Option<String>,
    pub source_available_at: Option<DateTime<Utc>>,
    pub source_age_seconds: Option<f64>,
    pub source_bytes: Option<usize>,
    pub last_ack_message_id: Option<String>,
    pub last_ack_disposition: Option<CoreAckDisposition>,
    pub last_ack_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub counters: StrategyDistributionCounters,
}

#[derive(Debug, Clone)]
pub struct StrategyDistributionSourceObservation {
    pub schema_version: String,
    pub document_id: String,
    pub fingerprint: String,
    pub available_at: DateTime<Utc>,
    pub byte_len: usize,
}

impl StrategyDistributionLaneHealth {
    fn new(source_path: Option<&Path>) -> Self {
        Self {
            configured: source_path.is_some(),
            source_path: source_path.map(|path| path.display().to_string()),
            status: if source_path.is_some() {
                StrategyDistributionLaneStatus::AwaitingSource
            } else {
                StrategyDistributionLaneStatus::Disabled
            },
            source_schema_version: None,
            source_document_id: None,
            source_fingerprint: None,
            source_available_at: None,
            source_age_seconds: None,
            source_bytes: None,
            last_ack_message_id: None,
            last_ack_disposition: None,
            last_ack_at: None,
            last_error: None,
            counters: StrategyDistributionCounters::default(),
        }
    }
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
    pub research: ResearchLaneHealth,
    pub desk_map: DeskMapLaneHealth,
    pub strategy_distribution: StrategyDistributionLaneHealth,
    pub counters: BridgeCounters,
    pub last_error: Option<String>,
    #[serde(skip)]
    quote_phase: BridgePhase,
}

impl BridgeHealth {
    pub fn new(
        source_path: &Path,
        research_source_path: Option<&Path>,
        desk_map_source_path: Option<&Path>,
        strategy_distribution_source_path: Option<&Path>,
        state: &BridgeState,
        now: DateTime<Utc>,
    ) -> Self {
        let mut health = Self {
            schema_version: "spx_normalized_bridge_health.v2",
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
            research: ResearchLaneHealth::new(research_source_path),
            desk_map: DeskMapLaneHealth::new(desk_map_source_path),
            strategy_distribution: StrategyDistributionLaneHealth::new(
                strategy_distribution_source_path,
            ),
            counters: BridgeCounters::default(),
            last_error: None,
            quote_phase: BridgePhase::Boot,
        };
        health.reconcile_phase();
        health
    }

    pub fn set_quote_phase(&mut self, phase: BridgePhase) {
        self.quote_phase = phase;
        self.reconcile_phase();
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

    pub fn observe_research_source(
        &mut self,
        schema_version: String,
        fingerprint: String,
        generated_at: DateTime<Utc>,
        byte_len: usize,
        already_acknowledged: bool,
        now: DateTime<Utc>,
    ) {
        self.updated_at = now;
        if self.research.source_fingerprint.as_deref() != Some(&fingerprint) {
            self.research.counters.source_documents =
                self.research.counters.source_documents.saturating_add(1);
            self.research.status = ResearchLaneStatus::AwaitingAck;
        } else if already_acknowledged {
            self.research.status = match self.research.last_ack_disposition {
                Some(CoreAckDisposition::ResearchStale) => ResearchLaneStatus::Stale,
                Some(
                    CoreAckDisposition::ResearchUnchanged | CoreAckDisposition::DuplicateIngress,
                ) => ResearchLaneStatus::Unchanged,
                _ => ResearchLaneStatus::Accepted,
            };
        }
        self.research.source_schema_version = Some(schema_version);
        self.research.source_fingerprint = Some(fingerprint);
        self.research.source_generated_at = Some(generated_at);
        self.research.source_age_seconds = (now >= generated_at).then(|| {
            (now - generated_at)
                .to_std()
                .map_or(f64::INFINITY, |duration| duration.as_secs_f64())
        });
        self.research.source_bytes = Some(byte_len);
        self.research.last_error = None;
        self.reconcile_phase();
    }

    pub fn reject_research_source(
        &mut self,
        status: ResearchLaneStatus,
        error: String,
        now: DateTime<Utc>,
    ) {
        debug_assert!(matches!(
            status,
            ResearchLaneStatus::Missing | ResearchLaneStatus::Rejected
        ));
        self.updated_at = now;
        self.research.status = status;
        self.research.last_error = Some(error);
        self.research.counters.source_errors =
            self.research.counters.source_errors.saturating_add(1);
        self.reconcile_phase();
    }

    pub fn acknowledge_research(
        &mut self,
        message_id: String,
        disposition: CoreAckDisposition,
        now: DateTime<Utc>,
    ) {
        self.updated_at = now;
        self.research.last_ack_message_id = Some(message_id);
        self.research.last_ack_disposition = Some(disposition);
        self.research.last_ack_at = Some(now);
        self.research.last_error = None;
        match disposition {
            CoreAckDisposition::ResearchUpdated => {
                self.research.status = ResearchLaneStatus::Accepted;
                self.research.counters.accepted_acks =
                    self.research.counters.accepted_acks.saturating_add(1);
            }
            CoreAckDisposition::ResearchUnchanged | CoreAckDisposition::DuplicateIngress => {
                self.research.status = ResearchLaneStatus::Unchanged;
                self.research.counters.unchanged_acks =
                    self.research.counters.unchanged_acks.saturating_add(1);
            }
            CoreAckDisposition::ResearchStale => {
                self.research.status = ResearchLaneStatus::Stale;
                self.research.counters.stale_acks =
                    self.research.counters.stale_acks.saturating_add(1);
            }
            _ => {
                self.research.status = ResearchLaneStatus::Rejected;
                self.research.last_error = Some(format!(
                    "unexpected accepted research disposition: {disposition:?}"
                ));
                self.research.counters.rejected_acks =
                    self.research.counters.rejected_acks.saturating_add(1);
            }
        }
        self.reconcile_phase();
    }

    pub fn reject_research_ack(&mut self, error: String, now: DateTime<Utc>) {
        self.updated_at = now;
        self.research.status = ResearchLaneStatus::Rejected;
        self.research.last_error = Some(error);
        self.research.counters.rejected_acks =
            self.research.counters.rejected_acks.saturating_add(1);
        self.reconcile_phase();
    }

    pub fn mark_research_transport_unknown(&mut self, error: String, now: DateTime<Utc>) {
        self.updated_at = now;
        self.research.status = ResearchLaneStatus::TransportUnknown;
        self.research.last_error = Some(error);
        self.research.counters.transport_errors =
            self.research.counters.transport_errors.saturating_add(1);
        self.reconcile_phase();
    }

    pub fn observe_desk_map_source(
        &mut self,
        schema_version: String,
        fingerprint: String,
        available_at: DateTime<Utc>,
        byte_len: usize,
        already_acknowledged: bool,
        now: DateTime<Utc>,
    ) {
        self.updated_at = now;
        if self.desk_map.source_fingerprint.as_deref() != Some(&fingerprint) {
            self.desk_map.counters.source_documents =
                self.desk_map.counters.source_documents.saturating_add(1);
            self.desk_map.status = DeskMapLaneStatus::AwaitingAck;
        } else if already_acknowledged {
            self.desk_map.status = match self.desk_map.last_ack_disposition {
                Some(CoreAckDisposition::DeskMapStale) => DeskMapLaneStatus::Stale,
                Some(
                    CoreAckDisposition::DeskMapUnchanged | CoreAckDisposition::DuplicateIngress,
                ) => DeskMapLaneStatus::Unchanged,
                _ => DeskMapLaneStatus::Accepted,
            };
        }
        self.desk_map.source_schema_version = Some(schema_version);
        self.desk_map.source_fingerprint = Some(fingerprint);
        self.desk_map.source_available_at = Some(available_at);
        self.desk_map.source_age_seconds = (now >= available_at).then(|| {
            (now - available_at)
                .to_std()
                .map_or(f64::INFINITY, |duration| duration.as_secs_f64())
        });
        self.desk_map.source_bytes = Some(byte_len);
        self.desk_map.last_error = None;
        self.reconcile_phase();
    }

    pub fn reject_desk_map_source(
        &mut self,
        status: DeskMapLaneStatus,
        error: String,
        now: DateTime<Utc>,
    ) {
        debug_assert!(matches!(
            status,
            DeskMapLaneStatus::Expired | DeskMapLaneStatus::Missing | DeskMapLaneStatus::Rejected
        ));
        self.updated_at = now;
        self.desk_map.status = status;
        self.desk_map.last_error = Some(error);
        self.desk_map.counters.source_errors =
            self.desk_map.counters.source_errors.saturating_add(1);
        self.reconcile_phase();
    }

    pub fn acknowledge_desk_map(
        &mut self,
        message_id: String,
        disposition: CoreAckDisposition,
        now: DateTime<Utc>,
    ) {
        self.updated_at = now;
        self.desk_map.last_ack_message_id = Some(message_id);
        self.desk_map.last_ack_disposition = Some(disposition);
        self.desk_map.last_ack_at = Some(now);
        self.desk_map.last_error = None;
        match disposition {
            CoreAckDisposition::DeskMapUpdated => {
                self.desk_map.status = DeskMapLaneStatus::Accepted;
                self.desk_map.counters.accepted_acks =
                    self.desk_map.counters.accepted_acks.saturating_add(1);
            }
            CoreAckDisposition::DeskMapUnchanged | CoreAckDisposition::DuplicateIngress => {
                self.desk_map.status = DeskMapLaneStatus::Unchanged;
                self.desk_map.counters.unchanged_acks =
                    self.desk_map.counters.unchanged_acks.saturating_add(1);
            }
            CoreAckDisposition::DeskMapStale => {
                self.desk_map.status = DeskMapLaneStatus::Stale;
                self.desk_map.counters.stale_acks =
                    self.desk_map.counters.stale_acks.saturating_add(1);
            }
            _ => {
                self.desk_map.status = DeskMapLaneStatus::Rejected;
                self.desk_map.last_error = Some(format!(
                    "unexpected accepted desk map disposition: {disposition:?}"
                ));
                self.desk_map.counters.rejected_acks =
                    self.desk_map.counters.rejected_acks.saturating_add(1);
            }
        }
        self.reconcile_phase();
    }

    pub fn reject_desk_map_ack(&mut self, error: String, now: DateTime<Utc>) {
        self.updated_at = now;
        self.desk_map.status = DeskMapLaneStatus::Rejected;
        self.desk_map.last_error = Some(error);
        self.desk_map.counters.rejected_acks =
            self.desk_map.counters.rejected_acks.saturating_add(1);
        self.reconcile_phase();
    }

    pub fn mark_desk_map_transport_unknown(&mut self, error: String, now: DateTime<Utc>) {
        self.updated_at = now;
        self.desk_map.status = DeskMapLaneStatus::TransportUnknown;
        self.desk_map.last_error = Some(error);
        self.desk_map.counters.transport_errors =
            self.desk_map.counters.transport_errors.saturating_add(1);
        self.reconcile_phase();
    }

    pub fn observe_strategy_distribution_source(
        &mut self,
        source: StrategyDistributionSourceObservation,
        already_acknowledged: bool,
        now: DateTime<Utc>,
    ) {
        self.updated_at = now;
        if self.strategy_distribution.source_fingerprint.as_deref() != Some(&source.fingerprint) {
            self.strategy_distribution.counters.source_documents = self
                .strategy_distribution
                .counters
                .source_documents
                .saturating_add(1);
            self.strategy_distribution.status = StrategyDistributionLaneStatus::AwaitingAck;
        } else if already_acknowledged {
            self.strategy_distribution.status =
                match self.strategy_distribution.last_ack_disposition {
                    Some(CoreAckDisposition::StrategyDistributionStale) => {
                        StrategyDistributionLaneStatus::Stale
                    }
                    Some(
                        CoreAckDisposition::StrategyDistributionUnchanged
                        | CoreAckDisposition::DuplicateIngress,
                    ) => StrategyDistributionLaneStatus::Unchanged,
                    _ => StrategyDistributionLaneStatus::Accepted,
                };
        }
        self.strategy_distribution.source_schema_version = Some(source.schema_version);
        self.strategy_distribution.source_document_id = Some(source.document_id);
        self.strategy_distribution.source_fingerprint = Some(source.fingerprint);
        self.strategy_distribution.source_available_at = Some(source.available_at);
        self.strategy_distribution.source_age_seconds = (now >= source.available_at).then(|| {
            (now - source.available_at)
                .to_std()
                .map_or(f64::INFINITY, |duration| duration.as_secs_f64())
        });
        self.strategy_distribution.source_bytes = Some(source.byte_len);
        self.strategy_distribution.last_error = None;
        self.reconcile_phase();
    }

    pub fn reject_strategy_distribution_source(
        &mut self,
        status: StrategyDistributionLaneStatus,
        error: String,
        now: DateTime<Utc>,
    ) {
        debug_assert!(matches!(
            status,
            StrategyDistributionLaneStatus::Expired
                | StrategyDistributionLaneStatus::Missing
                | StrategyDistributionLaneStatus::Rejected
        ));
        self.updated_at = now;
        self.strategy_distribution.status = status;
        self.strategy_distribution.last_error = Some(error);
        self.strategy_distribution.counters.source_errors = self
            .strategy_distribution
            .counters
            .source_errors
            .saturating_add(1);
        self.reconcile_phase();
    }

    pub fn acknowledge_strategy_distribution(
        &mut self,
        message_id: String,
        disposition: CoreAckDisposition,
        now: DateTime<Utc>,
    ) {
        self.updated_at = now;
        self.strategy_distribution.last_ack_message_id = Some(message_id);
        self.strategy_distribution.last_ack_disposition = Some(disposition);
        self.strategy_distribution.last_ack_at = Some(now);
        self.strategy_distribution.last_error = None;
        match disposition {
            CoreAckDisposition::StrategyDistributionUpdated => {
                self.strategy_distribution.status = StrategyDistributionLaneStatus::Accepted;
                self.strategy_distribution.counters.accepted_acks = self
                    .strategy_distribution
                    .counters
                    .accepted_acks
                    .saturating_add(1);
            }
            CoreAckDisposition::StrategyDistributionUnchanged
            | CoreAckDisposition::DuplicateIngress => {
                self.strategy_distribution.status = StrategyDistributionLaneStatus::Unchanged;
                self.strategy_distribution.counters.unchanged_acks = self
                    .strategy_distribution
                    .counters
                    .unchanged_acks
                    .saturating_add(1);
            }
            CoreAckDisposition::StrategyDistributionStale => {
                self.strategy_distribution.status = StrategyDistributionLaneStatus::Stale;
                self.strategy_distribution.counters.stale_acks = self
                    .strategy_distribution
                    .counters
                    .stale_acks
                    .saturating_add(1);
            }
            _ => {
                self.strategy_distribution.status = StrategyDistributionLaneStatus::Rejected;
                self.strategy_distribution.last_error = Some(format!(
                    "unexpected accepted strategy distribution disposition: {disposition:?}"
                ));
                self.strategy_distribution.counters.rejected_acks = self
                    .strategy_distribution
                    .counters
                    .rejected_acks
                    .saturating_add(1);
            }
        }
        self.reconcile_phase();
    }

    pub fn reject_strategy_distribution_ack(&mut self, error: String, now: DateTime<Utc>) {
        self.updated_at = now;
        self.strategy_distribution.status = StrategyDistributionLaneStatus::Rejected;
        self.strategy_distribution.last_error = Some(error);
        self.strategy_distribution.counters.rejected_acks = self
            .strategy_distribution
            .counters
            .rejected_acks
            .saturating_add(1);
        self.reconcile_phase();
    }

    pub fn mark_strategy_distribution_transport_unknown(
        &mut self,
        error: String,
        now: DateTime<Utc>,
    ) {
        self.updated_at = now;
        self.strategy_distribution.status = StrategyDistributionLaneStatus::TransportUnknown;
        self.strategy_distribution.last_error = Some(error);
        self.strategy_distribution.counters.transport_errors = self
            .strategy_distribution
            .counters
            .transport_errors
            .saturating_add(1);
        self.reconcile_phase();
    }

    pub fn persist(&self, path: &Path) -> Result<(), StateError> {
        atomic_write_json(path, self)
    }

    fn reconcile_phase(&mut self) {
        self.phase = if self.quote_phase == BridgePhase::Ready
            && (self.research.status.degrades_bridge()
                || self.desk_map.status.degrades_bridge()
                || self.strategy_distribution.status.degrades_bridge())
        {
            BridgePhase::Degraded
        } else {
            self.quote_phase
        };
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

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn research_rejection_cannot_be_hidden_by_quote_ready() {
        let temp = TempDir::new().unwrap();
        let state_path = temp.path().join("state.json");
        let state = BridgeState::initialize(&state_path).unwrap();
        let research_path = temp.path().join("research.json");
        let now = Utc::now();
        let mut health = BridgeHealth::new(
            &temp.path().join("quotes.json"),
            Some(&research_path),
            None,
            None,
            &state,
            now,
        );

        health.set_quote_phase(BridgePhase::Ready);
        assert_eq!(health.phase, BridgePhase::Ready);
        health.observe_research_source(
            "research_context.v2".to_owned(),
            "a".repeat(64),
            now,
            512,
            false,
            now,
        );
        health.acknowledge_research(
            "message:research:test".to_owned(),
            CoreAckDisposition::ResearchUpdated,
            now,
        );
        assert_eq!(health.phase, BridgePhase::Ready);

        health.reject_research_source(
            ResearchLaneStatus::Rejected,
            "schema mismatch".to_owned(),
            now,
        );
        health.set_quote_phase(BridgePhase::Ready);
        assert_eq!(health.phase, BridgePhase::Degraded);
        assert_eq!(health.research.status, ResearchLaneStatus::Rejected);
        assert_eq!(
            health.research.last_error.as_deref(),
            Some("schema mismatch")
        );
    }

    #[test]
    fn optional_research_missing_and_awaiting_states_do_not_degrade_quote_readiness() {
        let temp = TempDir::new().unwrap();
        let state_path = temp.path().join("state.json");
        let state = BridgeState::initialize(&state_path).unwrap();
        let research_path = temp.path().join("research.json");
        let now = Utc::now();
        let mut health = BridgeHealth::new(
            &temp.path().join("quotes.json"),
            Some(&research_path),
            None,
            None,
            &state,
            now,
        );

        health.set_quote_phase(BridgePhase::Ready);
        assert_eq!(health.research.status, ResearchLaneStatus::AwaitingSource);
        assert_eq!(health.phase, BridgePhase::Ready);

        health.observe_research_source(
            "research_context.v2".to_owned(),
            "a".repeat(64),
            now,
            512,
            false,
            now,
        );
        assert_eq!(health.research.status, ResearchLaneStatus::AwaitingAck);
        assert_eq!(health.phase, BridgePhase::Ready);

        health.reject_research_source(
            ResearchLaneStatus::Missing,
            "optional source absent".to_owned(),
            now,
        );
        assert_eq!(health.research.status, ResearchLaneStatus::Missing);
        assert_eq!(health.phase, BridgePhase::Ready);
    }

    #[test]
    fn desk_map_rejection_cannot_be_hidden_by_quote_ready() {
        let temp = TempDir::new().unwrap();
        let state_path = temp.path().join("state.json");
        let state = BridgeState::initialize(&state_path).unwrap();
        let desk_map_path = temp.path().join("desk-map.json");
        let now = Utc::now();
        let mut health = BridgeHealth::new(
            &temp.path().join("quotes.json"),
            None,
            Some(&desk_map_path),
            None,
            &state,
            now,
        );

        health.set_quote_phase(BridgePhase::Ready);
        assert_eq!(health.phase, BridgePhase::Degraded);
        health.observe_desk_map_source(
            "desk_map_projection.v1".to_owned(),
            "b".repeat(64),
            now,
            768,
            false,
            now,
        );
        health.acknowledge_desk_map(
            "message:desk-map:test".to_owned(),
            CoreAckDisposition::DeskMapUpdated,
            now,
        );
        assert_eq!(health.phase, BridgePhase::Ready);
        assert_eq!(
            health.desk_map.source_schema_version.as_deref(),
            Some("desk_map_projection.v1")
        );
        assert_eq!(
            health.desk_map.source_fingerprint.as_deref(),
            Some("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
        );
        assert_eq!(health.desk_map.source_available_at, Some(now));
        assert_eq!(health.desk_map.source_age_seconds, Some(0.0));
        assert_eq!(health.desk_map.source_bytes, Some(768));
        assert_eq!(
            health.desk_map.last_ack_message_id.as_deref(),
            Some("message:desk-map:test")
        );
        assert_eq!(
            health.desk_map.last_ack_disposition,
            Some(CoreAckDisposition::DeskMapUpdated)
        );
        assert_eq!(health.desk_map.last_ack_at, Some(now));
        assert_eq!(health.desk_map.counters.source_documents, 1);
        assert_eq!(health.desk_map.counters.accepted_acks, 1);

        health.reject_desk_map_source(
            DeskMapLaneStatus::Rejected,
            "schema mismatch".to_owned(),
            now,
        );
        health.set_quote_phase(BridgePhase::Ready);
        assert_eq!(health.phase, BridgePhase::Degraded);
        assert_eq!(health.desk_map.status, DeskMapLaneStatus::Rejected);
    }

    #[test]
    fn strategy_distribution_ack_and_rejection_are_visible_without_quote_coupling() {
        let temp = TempDir::new().unwrap();
        let state = BridgeState::initialize(&temp.path().join("state.json")).unwrap();
        let source_path = temp.path().join("strategy-distribution.json");
        let now = Utc::now();
        let mut health = BridgeHealth::new(
            &temp.path().join("quotes.json"),
            None,
            None,
            Some(&source_path),
            &state,
            now,
        );

        health.set_quote_phase(BridgePhase::Ready);
        assert_eq!(health.phase, BridgePhase::Ready);
        health.observe_strategy_distribution_source(
            StrategyDistributionSourceObservation {
                schema_version: "strategy_distribution_forecast.v1".to_owned(),
                document_id: "strategy-distribution:test:1".to_owned(),
                fingerprint: "c".repeat(64),
                available_at: now,
                byte_len: 1_024,
            },
            false,
            now,
        );
        assert_eq!(
            health.strategy_distribution.status,
            StrategyDistributionLaneStatus::AwaitingAck
        );
        health.acknowledge_strategy_distribution(
            "message:strategy-distribution:test".to_owned(),
            CoreAckDisposition::StrategyDistributionUpdated,
            now,
        );
        assert_eq!(health.phase, BridgePhase::Ready);
        assert_eq!(
            health.strategy_distribution.status,
            StrategyDistributionLaneStatus::Accepted
        );
        assert_eq!(
            health.strategy_distribution.source_document_id.as_deref(),
            Some("strategy-distribution:test:1")
        );
        assert_eq!(health.strategy_distribution.counters.source_documents, 1);
        assert_eq!(health.strategy_distribution.counters.accepted_acks, 1);

        health.reject_strategy_distribution_source(
            StrategyDistributionLaneStatus::Rejected,
            "schema mismatch".to_owned(),
            now,
        );
        health.set_quote_phase(BridgePhase::Ready);
        assert_eq!(health.phase, BridgePhase::Degraded);
        assert_eq!(
            health.strategy_distribution.status,
            StrategyDistributionLaneStatus::Rejected
        );
    }

    #[test]
    fn additive_strategy_distribution_health_keeps_v2_consumers_compatible() {
        let temp = TempDir::new().unwrap();
        let state = BridgeState::initialize(&temp.path().join("state.json")).unwrap();
        let now = Utc::now();
        let mut health = BridgeHealth::new(
            &temp.path().join("quotes.json"),
            None,
            None,
            None,
            &state,
            now,
        );
        health.set_quote_phase(BridgePhase::Ready);

        let encoded = serde_json::to_value(&health).unwrap();
        assert_eq!(encoded["schema_version"], "spx_normalized_bridge_health.v2");
        assert!(encoded.get("research").is_some());
        assert!(encoded.get("desk_map").is_some());
        assert_eq!(encoded["strategy_distribution"]["configured"], false);
        assert_eq!(encoded["strategy_distribution"]["status"], "disabled");
        assert_eq!(health.phase, BridgePhase::Ready);
    }
}
