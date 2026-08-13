use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use serde::Serialize;
use spx_ledger::{
    BeginTransport, ClaimedDelivery, ClaimedNotificationIntent, Ledger, LedgerError, LedgerHealth,
    OwnerLease, OwnerRole, RecoverySummary, Settlement, SettlementWrite,
};
use thiserror::Error;

use crate::{
    ConfigError, DeliveryConfig, TargetError, TargetRegistry, Transport, TransportRequest,
    TransportResult, render_desk_message, render_desk_message_v2, transport::HttpTransport,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NetworkGate {
    config_enabled: bool,
    cli_allowed: bool,
}

impl NetworkGate {
    pub const fn new(config_enabled: bool, cli_allowed: bool) -> Self {
        Self {
            config_enabled,
            cli_allowed,
        }
    }

    pub const fn authorized(self) -> bool {
        self.config_enabled && self.cli_allowed
    }
}

#[derive(Debug, Error)]
pub enum WorkerError {
    #[error("delivery network is not authorized by both config and CLI")]
    NetworkNotAuthorized,
    #[error("delivery config error: {0}")]
    Config(#[from] ConfigError),
    #[error("delivery target error: {0}")]
    Target(#[from] TargetError),
    #[error("delivery ledger error: {0}")]
    Ledger(#[from] LedgerError),
    #[error("invalid delivery duration")]
    InvalidDuration,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize)]
pub struct WorkerSummary {
    pub claimed: u64,
    pub delivered: u64,
    pub retry_scheduled: u64,
    pub dead_letter: u64,
    pub uncertain: u64,
    pub cancelled: u64,
    pub expired: u64,
    pub recovered_before_transport: u64,
    pub recovered_uncertain: u64,
}

pub struct DeliveryWorker<T: Transport> {
    ledger: Ledger,
    owner: OwnerLease,
    config: DeliveryConfig,
    targets: TargetRegistry,
    transport: T,
    gate: NetworkGate,
    owner_released: bool,
}

impl DeliveryWorker<HttpTransport> {
    /// Opens a delivery worker backed by the HTTPS transport.
    ///
    /// # Errors
    ///
    /// Returns an error when configuration, target construction, ledger opening, or
    /// exclusive owner acquisition fails.
    pub fn open_http(
        config: DeliveryConfig,
        allow_network: bool,
        owner_id: &str,
        now: DateTime<Utc>,
    ) -> Result<Self, WorkerError> {
        let timeout = Duration::from_secs(config.request_timeout_seconds);
        Self::open(
            config,
            allow_network,
            owner_id,
            now,
            HttpTransport::new(timeout),
        )
    }
}

impl<T: Transport> DeliveryWorker<T> {
    /// Opens a worker with an injected transport, primarily for bounded adapters and tests.
    ///
    /// # Errors
    ///
    /// Returns an error when configuration, target construction, ledger opening, or
    /// exclusive owner acquisition fails.
    pub fn open(
        config: DeliveryConfig,
        allow_network: bool,
        owner_id: &str,
        now: DateTime<Utc>,
        transport: T,
    ) -> Result<Self, WorkerError> {
        config.validate()?;
        let gate = NetworkGate::new(config.network_enabled, allow_network);
        if !gate.authorized() {
            return Err(WorkerError::NetworkNotAuthorized);
        }
        let targets = TargetRegistry::new(&config.targets)?;
        let ledger = Ledger::open(&config.ledger_path)?;
        let owner_duration = TimeDelta::seconds(config.owner_lease_seconds);
        let owner = ledger.acquire_owner(OwnerRole::Delivery, owner_id, now, owner_duration)?;
        Ok(Self {
            ledger,
            owner,
            config,
            targets,
            transport,
            gate,
            owner_released: false,
        })
    }

    /// Runs the delivery loop until an operation fails or the process is stopped.
    ///
    /// # Errors
    ///
    /// Returns an error if dual network authorization is absent or a ledger operation fails.
    pub fn run(&mut self) -> Result<(), WorkerError> {
        let stop = AtomicBool::new(false);
        self.run_until(&stop)
    }

    /// Runs the delivery loop until the supplied process-stop flag is set.
    ///
    /// # Errors
    ///
    /// Returns an error if dual network authorization is absent or a ledger operation fails.
    pub fn run_until(&mut self, stop: &AtomicBool) -> Result<(), WorkerError> {
        self.require_network()?;
        while !stop.load(Ordering::Relaxed) {
            let summary = self.run_once()?;
            if summary.claimed == 0
                && summary.recovered_before_transport == 0
                && summary.recovered_uncertain == 0
            {
                let mut remaining = self.config.poll_interval_millis;
                while remaining > 0 && !stop.load(Ordering::Relaxed) {
                    let slice = remaining.min(100);
                    thread::sleep(Duration::from_millis(slice));
                    remaining -= slice;
                }
            }
        }
        Ok(())
    }

    /// Processes at most one pending target delivery.
    ///
    /// # Errors
    ///
    /// Returns an error if dual network authorization is absent, ownership is lost, the
    /// target is unavailable, or a ledger transition fails.
    pub fn run_once(&mut self) -> Result<WorkerSummary, WorkerError> {
        self.run_once_inner(Utc::now(), None)
    }

    /// Runs the ledger integrity check and returns delivery state counts.
    ///
    /// # Errors
    ///
    /// Returns an error if the integrity check or health query fails.
    pub fn health(&self) -> Result<LedgerHealth, WorkerError> {
        self.ledger.quick_check()?;
        Ok(self.ledger.health()?)
    }

    /// Releases the delivery owner fence so a replacement can start immediately.
    ///
    /// # Errors
    ///
    /// Returns an error if this process no longer owns the recorded generation.
    pub fn shutdown(&mut self) -> Result<(), WorkerError> {
        if !self.owner_released {
            self.ledger.release_owner(&self.owner)?;
            self.owner_released = true;
        }
        Ok(())
    }

    #[cfg(test)]
    fn run_once_at(&mut self, now: DateTime<Utc>) -> Result<WorkerSummary, WorkerError> {
        self.run_once_at_boundaries(now, now, now)
    }

    #[cfg(test)]
    fn run_once_at_boundaries(
        &mut self,
        claim_at: DateTime<Utc>,
        begin_at: DateTime<Utc>,
        completed_at: DateTime<Utc>,
    ) -> Result<WorkerSummary, WorkerError> {
        self.run_once_inner(claim_at, Some((begin_at, completed_at)))
    }

    fn run_once_inner(
        &mut self,
        now: DateTime<Utc>,
        boundary_override: Option<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> Result<WorkerSummary, WorkerError> {
        self.require_network()?;
        let owner_duration = TimeDelta::seconds(self.config.owner_lease_seconds);
        self.ledger
            .refresh_owner(&mut self.owner, now, owner_duration)?;
        let recovery = self.ledger.recover_stale_claims(&self.owner, now)?;
        let claim_duration = TimeDelta::seconds(self.config.claim_lease_seconds);
        let Some(claimed) = self.ledger.claim_next(&self.owner, now, claim_duration)? else {
            return Ok(summary_with_recovery(recovery));
        };
        self.process_claimed_at(&claimed, boundary_override, recovery)
    }

    fn process_claimed_at(
        &self,
        claimed: &ClaimedDelivery,
        boundary_override: Option<(DateTime<Utc>, DateTime<Utc>)>,
        recovery: RecoverySummary,
    ) -> Result<WorkerSummary, WorkerError> {
        let mut summary = summary_with_recovery(recovery);
        summary.claimed = 1;
        let Some(target) = self.targets.get(&claimed.target_key) else {
            let rejected_at = boundary_override.map_or_else(Utc::now, |times| times.0);
            self.ledger.reject_before_transport(
                &self.owner,
                &claimed.handle,
                "target_not_configured",
                rejected_at,
            )?;
            summary.dead_letter = 1;
            return Ok(summary);
        };
        if target.channel() != claimed.channel {
            let rejected_at = boundary_override.map_or_else(Utc::now, |times| times.0);
            self.ledger.reject_before_transport(
                &self.owner,
                &claimed.handle,
                "target_channel_mismatch",
                rejected_at,
            )?;
            summary.dead_letter = 1;
            return Ok(summary);
        }
        let rendered = match &claimed.intent {
            ClaimedNotificationIntent::TradeReady(intent) => render_desk_message(&intent.message),
            ClaimedNotificationIntent::ScheduledReport(intent) => {
                render_desk_message_v2(&intent.message)
            }
            ClaimedNotificationIntent::TraderEvent(notification) => {
                crate::render_operator_notification(notification)
            }
        };
        let begin_at = boundary_override.map_or_else(Utc::now, |times| times.0);
        let transport_lease_duration = TimeDelta::seconds(self.config.claim_lease_seconds);
        let attempt_id = match self.ledger.begin_transport(
            &self.owner,
            claimed,
            begin_at,
            transport_lease_duration,
        )? {
            BeginTransport::Cancelled => {
                summary.cancelled = 1;
                return Ok(summary);
            }
            BeginTransport::Expired => {
                summary.expired = 1;
                return Ok(summary);
            }
            BeginTransport::Started { attempt_id } => attempt_id,
        };
        let request = TransportRequest {
            event_id: claimed.intent.intent_id().as_str().to_owned(),
            idempotency_key: claimed.idempotency_key.clone(),
            message: rendered,
        };
        let result = self.transport.send(target, &request);
        let completed_at = boundary_override.map_or_else(Utc::now, |times| times.1);
        let settlement = match result {
            TransportResult::Delivered {
                provider_message_id,
            } => Settlement::Delivered {
                provider_message_id,
            },
            TransportResult::Retryable { error_code } => Settlement::Retryable {
                error_code,
                retry_at: retry_at(
                    completed_at,
                    claimed.attempt_no,
                    &self.config.retry_schedule_seconds,
                )?,
            },
            TransportResult::PermanentFailure { error_code } => {
                Settlement::PermanentFailure { error_code }
            }
            TransportResult::Uncertain { error_code } => {
                Settlement::TransportUncertain { error_code }
            }
        };
        match self.ledger.settle(
            &self.owner,
            &claimed.handle,
            &attempt_id,
            &settlement,
            completed_at,
        )? {
            SettlementWrite::Delivered => summary.delivered = 1,
            SettlementWrite::RetryScheduled => summary.retry_scheduled = 1,
            SettlementWrite::DeadLetter => summary.dead_letter = 1,
            SettlementWrite::Uncertain => summary.uncertain = 1,
        }
        Ok(summary)
    }

    fn require_network(&self) -> Result<(), WorkerError> {
        if self.gate.authorized() {
            Ok(())
        } else {
            Err(WorkerError::NetworkNotAuthorized)
        }
    }
}

impl<T: Transport> Drop for DeliveryWorker<T> {
    fn drop(&mut self) {
        if !self.owner_released {
            let _ = self.ledger.release_owner(&self.owner);
            self.owner_released = true;
        }
    }
}

fn retry_at(
    completed_at: DateTime<Utc>,
    attempt_no: u32,
    schedule: &[u64],
) -> Result<DateTime<Utc>, WorkerError> {
    let attempt_index =
        usize::try_from(attempt_no.saturating_sub(1)).map_err(|_| WorkerError::InvalidDuration)?;
    let delay = schedule[attempt_index.min(schedule.len() - 1)];
    let delay = i64::try_from(delay).map_err(|_| WorkerError::InvalidDuration)?;
    completed_at
        .checked_add_signed(TimeDelta::seconds(delay))
        .ok_or(WorkerError::InvalidDuration)
}

fn summary_with_recovery(recovery: RecoverySummary) -> WorkerSummary {
    WorkerSummary {
        recovered_before_transport: recovery.requeued,
        recovered_uncertain: recovery.uncertain,
        ..WorkerSummary::default()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::path::PathBuf;
    use std::sync::Mutex;

    use chrono::TimeZone;
    use spx_domain::{
        CandidateDirection, DECISION_SCHEMA_VERSION, DeskMessageV1, DeskMessageV2,
        ExactLegEvidenceV1, NOTIFICATION_INTENT_SCHEMA_VERSION,
        NOTIFICATION_INTENT_V2_SCHEMA_VERSION, NonNegativeF64, NotificationIntentV1,
        NotificationIntentV2, NotificationLineageV2, NotificationTargetV1,
        OPERATOR_NOTIFICATION_SCHEMA_VERSION, OperatorNotificationRole, OperatorNotificationV1,
        OptionRight, PositiveF64, Provider, StrategyAction, StrategyDecisionV1, Token,
    };
    use spx_ledger::PersistWrite;
    use tempfile::TempDir;

    use super::*;

    #[derive(Debug)]
    struct MockTransport {
        results: Mutex<VecDeque<TransportResult>>,
        requests: Mutex<Vec<TransportRequest>>,
    }

    impl MockTransport {
        fn new(result: TransportResult) -> Self {
            Self {
                results: Mutex::new(VecDeque::from([result])),
                requests: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> usize {
            self.requests.lock().unwrap().len()
        }
    }

    impl Transport for MockTransport {
        fn send(
            &self,
            _target: &crate::DeliveryTarget,
            request: &TransportRequest,
        ) -> TransportResult {
            self.requests.lock().unwrap().push(request.clone());
            self.results.lock().unwrap().pop_front().unwrap()
        }
    }

    struct Fixture {
        _temp: TempDir,
        worker: DeliveryWorker<MockTransport>,
        now: DateTime<Utc>,
    }

    fn token(value: &str) -> Token {
        Token::new(value, "delivery test").unwrap()
    }

    fn fixture(result: TransportResult) -> Fixture {
        fixture_with_max_attempts(result, 5)
    }

    fn fixture_with_max_attempts(result: TransportResult, max_attempts: u32) -> Fixture {
        let temp = TempDir::new().unwrap();
        let now = Utc.timestamp_opt(1_785_590_400, 0).unwrap();
        let ledger_path = temp.path().join("ledger.sqlite");
        let ledger = Ledger::open(&ledger_path).unwrap();
        let core = ledger
            .acquire_owner(
                OwnerRole::Core,
                "core-owner-delivery-test",
                now,
                TimeDelta::seconds(60),
            )
            .unwrap();
        let decision = decision(now);
        assert_eq!(
            ledger
                .persist_decision(&core, &decision, Some(&intent(now, max_attempts)), now,)
                .unwrap(),
            PersistWrite::Inserted
        );
        let config = delivery_config(ledger_path);
        let worker = DeliveryWorker::open(
            config,
            true,
            "delivery-owner-test-0001",
            now,
            MockTransport::new(result),
        )
        .unwrap();
        Fixture {
            _temp: temp,
            worker,
            now,
        }
    }

    fn scheduled_fixture(result: TransportResult) -> Fixture {
        let temp = TempDir::new().unwrap();
        let now = Utc.timestamp_opt(1_785_590_400, 0).unwrap();
        let ledger_path = temp.path().join("ledger.sqlite");
        let ledger = Ledger::open(&ledger_path).unwrap();
        let report_owner = ledger
            .acquire_owner(
                OwnerRole::Report,
                "report-owner-delivery-test",
                now,
                TimeDelta::seconds(60),
            )
            .unwrap();
        ledger
            .persist_scheduled_report(&report_owner, &scheduled_intent(now), now)
            .unwrap();
        let worker = DeliveryWorker::open(
            delivery_config(ledger_path),
            true,
            "delivery-owner-test-0001",
            now,
            MockTransport::new(result),
        )
        .unwrap();
        Fixture {
            _temp: temp,
            worker,
            now,
        }
    }

    fn operator_fixture(result: TransportResult) -> Fixture {
        let temp = TempDir::new().unwrap();
        let now = Utc.timestamp_opt(1_785_590_400, 0).unwrap();
        let ledger_path = temp.path().join("ledger.sqlite");
        let ledger = Ledger::open(&ledger_path).unwrap();
        let core = ledger
            .acquire_owner(
                OwnerRole::Core,
                "core-owner-operator-test",
                now,
                TimeDelta::seconds(60),
            )
            .unwrap();
        ledger
            .persist_operator_notification(&core, &operator_intent(now), 5, now)
            .unwrap();
        let worker = DeliveryWorker::open(
            delivery_config(ledger_path),
            true,
            "delivery-owner-test-0001",
            now,
            MockTransport::new(result),
        )
        .unwrap();
        Fixture {
            _temp: temp,
            worker,
            now,
        }
    }

    fn superseded_operator_fixture(result: TransportResult) -> Fixture {
        let temp = TempDir::new().unwrap();
        let now = Utc.timestamp_opt(1_785_590_400, 0).unwrap();
        let ledger_path = temp.path().join("ledger.sqlite");
        let ledger = Ledger::open(&ledger_path).unwrap();
        let core = ledger
            .acquire_owner(
                OwnerRole::Core,
                "core-owner-supersede-test",
                now,
                TimeDelta::seconds(60),
            )
            .unwrap();
        for (offset, role) in [
            OperatorNotificationRole::Setup,
            OperatorNotificationRole::TradeReady,
            OperatorNotificationRole::Exit,
        ]
        .into_iter()
        .enumerate()
        {
            ledger
                .persist_operator_notification(
                    &core,
                    &operator_intent_with_role(now, role),
                    5,
                    now + TimeDelta::seconds(i64::try_from(offset).unwrap()),
                )
                .unwrap();
        }
        let worker = DeliveryWorker::open(
            delivery_config(ledger_path),
            true,
            "delivery-owner-test-0001",
            now + TimeDelta::seconds(3),
            MockTransport::new(result),
        )
        .unwrap();
        Fixture {
            _temp: temp,
            worker,
            now: now + TimeDelta::seconds(3),
        }
    }

    fn delivery_config(ledger_path: PathBuf) -> DeliveryConfig {
        DeliveryConfig {
            ledger_path,
            network_enabled: true,
            poll_interval_millis: 1,
            owner_lease_seconds: 60,
            claim_lease_seconds: 20,
            request_timeout_seconds: 5,
            retry_schedule_seconds: vec![15, 60],
            targets: vec![crate::TargetConfig::Bark {
                key: "bark-primary".to_owned(),
                endpoint_env: "SPX_TEST_BARK_ENDPOINT".to_owned(),
                presentation: crate::BarkPresentation::Full,
            }],
        }
    }

    fn decision(now: DateTime<Utc>) -> StrategyDecisionV1 {
        StrategyDecisionV1 {
            schema_version: DECISION_SCHEMA_VERSION.to_owned(),
            decision_id: token("decision-delivery"),
            request_id: token("request-delivery"),
            strategy_id: token("strategy-delivery"),
            policy_version: token("policy-v1"),
            snapshot_id: token("snapshot-v1"),
            action: StrategyAction::ManualCandidate,
            direction: Some(CandidateDirection::CallVertical10),
            evaluated_at: now,
            valid_until: now + TimeDelta::seconds(30),
            block_reasons: Vec::new(),
            exact_legs: Some(ExactLegEvidenceV1 {
                provider: Provider::Schwab,
                long_contract_id: token("long-leg"),
                short_contract_id: token("short-leg"),
                right: OptionRight::Call,
                long_strike: PositiveF64::new(6000.0, "strike").unwrap(),
                short_strike: PositiveF64::new(6010.0, "strike").unwrap(),
                long_bid: PositiveF64::new(3.0, "bid").unwrap(),
                long_ask: PositiveF64::new(3.2, "ask").unwrap(),
                short_bid: PositiveF64::new(1.0, "bid").unwrap(),
                short_ask: PositiveF64::new(1.2, "ask").unwrap(),
                max_age_seconds: NonNegativeF64::new(0.5, "age").unwrap(),
                max_skew_seconds: NonNegativeF64::new(0.1, "skew").unwrap(),
                observed_at: now,
            }),
            evidence_hash: token("evidence-hash"),
            automatic_ordering: false,
        }
    }

    fn intent(now: DateTime<Utc>, max_attempts: u32) -> NotificationIntentV1 {
        NotificationIntentV1 {
            schema_version: NOTIFICATION_INTENT_SCHEMA_VERSION.to_owned(),
            intent_id: token("intent-delivery"),
            semantic_id: token("semantic-delivery"),
            decision_id: token("decision-delivery"),
            created_at: now,
            expires_at: now + TimeDelta::seconds(30),
            message: DeskMessageV1 {
                title: token("SPX manual candidate"),
                desk_view: token("Range"),
                execution: token("Manual only"),
                risk: token("No automatic order"),
                targets: token("6000/6010 call vertical"),
                data_quality: token("Fresh exact NBBO"),
            },
            targets: vec![NotificationTargetV1 {
                key: token("bark-primary"),
                channel: spx_domain::DeliveryChannel::Bark,
            }],
            max_attempts,
        }
    }

    fn scheduled_intent(now: DateTime<Utc>) -> NotificationIntentV2 {
        NotificationIntentV2 {
            schema_version: NOTIFICATION_INTENT_V2_SCHEMA_VERSION.to_owned(),
            intent_id: token("report-event-1000-et"),
            semantic_id: token("desk-map-1000-et"),
            lineage: NotificationLineageV2::ScheduledReport {
                source_projection_id: token("projection-095959-et"),
                slot: token("2026-08-04:10:00"),
            },
            created_at: now,
            expires_at: now + TimeDelta::seconds(30),
            message: DeskMessageV2 {
                title: token("SPX RTH Desk Map · 10:00 ET"),
                desk_view: token("Bullish  above VWAP"),
                location: token("SPX 7568 | OR15 7565"),
                structure: token("Put 7525 | Flip 7550 | Call 7580"),
                primary_path: token(&format!("Hold 7565\n{} tail", "x".repeat(3_500))),
                alternative_path: token("Lose VWAP and rotate to 7550"),
                targets: token("7580 / 7595"),
                execution: token("Wait for retest; no chase"),
                data_quality: token("DEGRADED: clipped mass 28.4%"),
            },
            targets: vec![NotificationTargetV1 {
                key: token("bark-primary"),
                channel: spx_domain::DeliveryChannel::Bark,
            }],
            max_attempts: 5,
        }
    }

    fn operator_intent(now: DateTime<Utc>) -> OperatorNotificationV1 {
        operator_intent_with_role(now, OperatorNotificationRole::TradeReady)
    }

    fn operator_intent_with_role(
        now: DateTime<Utc>,
        role: OperatorNotificationRole,
    ) -> OperatorNotificationV1 {
        let role_name = role.as_str();
        OperatorNotificationV1 {
            schema_version: OPERATOR_NOTIFICATION_SCHEMA_VERSION.to_owned(),
            event_id: token(&format!("operator-event-{role_name}")),
            semantic_id: token(&format!("operator-semantic-{role_name}")),
            opportunity_id: token("operator-opportunity-delivery"),
            generation: 0,
            role,
            occurred_at: now,
            expires_at: now + TimeDelta::seconds(30),
            title: token(&format!("SPX {role_name} · frozen")),
            body: format!(
                "## Desk View\n第一行  保留空格\n{}\n末尾  \n\n## Execution\nmanual only\n\n## Risk\ndefined risk\n\n## Targets\nnext level\n\n## Data Quality\nlive",
                "结构正文；".repeat(1_000)
            ),
            targets: vec![NotificationTargetV1 {
                key: token("bark-primary"),
                channel: spx_domain::DeliveryChannel::Bark,
            }],
            automatic_ordering: false,
        }
    }

    #[test]
    fn delivered_mock_settles_once_with_deterministic_render() {
        let mut fixture = fixture(TransportResult::Delivered {
            provider_message_id: Some("provider-1".to_owned()),
        });
        let summary = fixture.worker.run_once_at(fixture.now).unwrap();
        assert_eq!(summary.delivered, 1);
        assert_eq!(fixture.worker.health().unwrap().delivered, 1);
        let second = fixture
            .worker
            .run_once_at(fixture.now + TimeDelta::seconds(1))
            .unwrap();
        assert_eq!(second.claimed, 0);
        assert_eq!(fixture.worker.transport.calls(), 1);
        let requests = fixture.worker.transport.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].message.body.starts_with("Desk View\n"));
    }

    #[test]
    fn scheduled_report_is_claimed_rendered_in_full_and_sent_once() {
        let mut fixture = scheduled_fixture(TransportResult::Delivered {
            provider_message_id: Some("provider-report-1".to_owned()),
        });
        let summary = fixture.worker.run_once_at(fixture.now).unwrap();
        assert_eq!(summary.delivered, 1);
        assert_eq!(fixture.worker.health().unwrap().delivered, 1);
        let second = fixture
            .worker
            .run_once_at(fixture.now + TimeDelta::seconds(1))
            .unwrap();
        assert_eq!(second.claimed, 0);
        assert_eq!(fixture.worker.transport.calls(), 1);
        let requests = fixture.worker.transport.requests.lock().unwrap();
        let [request] = requests.as_slice() else {
            panic!("exactly one scheduled report must be sent");
        };
        assert_eq!(request.event_id, "report-event-1000-et");
        assert_eq!(request.message.title, "SPX RTH Desk Map · 10:00 ET");
        assert!(request.message.body.contains("Bullish  above VWAP"));
        assert!(request.message.body.contains("位置  SPX 7568 | OR15 7565"));
        assert!(request.message.body.contains("结构  Put 7525"));
        assert!(request.message.body.contains("触发  Hold 7565\n"));
        assert!(request.message.body.contains(&"x".repeat(3_500)));
        assert!(request.message.body.contains("失效  Lose VWAP"));
        assert!(request.message.body.contains("目标  7580 / 7595"));
        assert!(request.message.body.contains("执行  Wait for retest"));
        assert!(
            request
                .message
                .body
                .ends_with("数据  DEGRADED: clipped mass 28.4%")
        );
    }

    #[test]
    fn operator_event_sends_the_frozen_title_and_body_once() {
        let expected = operator_intent(Utc.timestamp_opt(1_785_590_400, 0).unwrap());
        let mut fixture = operator_fixture(TransportResult::Delivered {
            provider_message_id: Some("provider-operator-1".to_owned()),
        });
        assert_eq!(
            fixture.worker.run_once_at(fixture.now).unwrap().delivered,
            1
        );
        assert_eq!(
            fixture
                .worker
                .run_once_at(fixture.now + TimeDelta::seconds(1))
                .unwrap()
                .claimed,
            0
        );
        let requests = fixture.worker.transport.requests.lock().unwrap();
        let [request] = requests.as_slice() else {
            panic!("exactly one operator event must be sent");
        };
        assert_eq!(request.event_id, expected.event_id.as_str());
        let rendered = crate::render_operator_notification(&expected);
        assert_eq!(request.message.title, rendered.title);
        assert_eq!(request.message.body, rendered.body);
    }

    #[test]
    fn ready_then_exit_before_delivery_sends_only_exit() {
        let mut fixture = superseded_operator_fixture(TransportResult::Delivered {
            provider_message_id: Some("provider-exit-1".to_owned()),
        });
        assert_eq!(fixture.worker.health().unwrap().cancelled, 2);
        assert_eq!(
            fixture.worker.run_once_at(fixture.now).unwrap().delivered,
            1
        );
        assert_eq!(
            fixture
                .worker
                .run_once_at(fixture.now + TimeDelta::seconds(1))
                .unwrap()
                .claimed,
            0
        );
        let requests = fixture.worker.transport.requests.lock().unwrap();
        let [request] = requests.as_slice() else {
            panic!("only the terminal exit may be sent");
        };
        assert_eq!(request.event_id, "operator-event-exit");
        assert_eq!(request.message.title, "SPX exit · frozen");
    }

    #[test]
    fn explicit_retryable_mock_schedules_retry() {
        let mut fixture = fixture(TransportResult::Retryable {
            error_code: "http_429".to_owned(),
        });
        let summary = fixture.worker.run_once_at(fixture.now).unwrap();
        assert_eq!(summary.retry_scheduled, 1);
        assert_eq!(fixture.worker.health().unwrap().pending, 1);
        assert_eq!(fixture.worker.transport.calls(), 1);
    }

    #[test]
    fn final_retryable_attempt_reports_dead_letter_not_retry_scheduled() {
        let mut fixture = fixture_with_max_attempts(
            TransportResult::Retryable {
                error_code: "http_429".to_owned(),
            },
            1,
        );
        let summary = fixture.worker.run_once_at(fixture.now).unwrap();
        assert_eq!(summary.retry_scheduled, 0);
        assert_eq!(summary.dead_letter, 1);
        assert_eq!(fixture.worker.health().unwrap().dead_letter, 1);
    }

    #[test]
    fn permanent_mock_dead_letters() {
        let mut fixture = fixture(TransportResult::PermanentFailure {
            error_code: "http_4xx".to_owned(),
        });
        let summary = fixture.worker.run_once_at(fixture.now).unwrap();
        assert_eq!(summary.dead_letter, 1);
        assert_eq!(fixture.worker.health().unwrap().dead_letter, 1);
    }

    #[test]
    fn uncertain_mock_is_terminal_and_not_blindly_retried() {
        let mut fixture = fixture(TransportResult::Uncertain {
            error_code: "transport_timeout".to_owned(),
        });
        let summary = fixture.worker.run_once_at(fixture.now).unwrap();
        assert_eq!(summary.uncertain, 1);
        assert_eq!(fixture.worker.health().unwrap().uncertain, 1);
        let second = fixture
            .worker
            .run_once_at(fixture.now + TimeDelta::seconds(1))
            .unwrap();
        assert_eq!(second.claimed, 0);
        assert_eq!(fixture.worker.transport.calls(), 1);
    }

    #[test]
    fn expiry_crossed_after_claim_never_calls_transport() {
        let fixture = fixture(TransportResult::Delivered {
            provider_message_id: None,
        });
        let claimed = fixture
            .worker
            .ledger
            .claim_next(
                &fixture.worker.owner,
                fixture.now + TimeDelta::seconds(12),
                TimeDelta::seconds(20),
            )
            .unwrap()
            .unwrap();
        let summary = fixture
            .worker
            .process_claimed_at(
                &claimed,
                Some((
                    fixture.now + TimeDelta::seconds(31),
                    fixture.now + TimeDelta::seconds(31),
                )),
                RecoverySummary::default(),
            )
            .unwrap();
        assert_eq!(summary.expired, 1);
        assert_eq!(fixture.worker.transport.calls(), 0);
        assert_eq!(fixture.worker.health().unwrap().expired, 1);
    }

    #[test]
    fn cancellation_after_claim_does_not_fail_or_call_transport() {
        let fixture = fixture(TransportResult::Delivered {
            provider_message_id: None,
        });
        let claimed = fixture
            .worker
            .ledger
            .claim_next(
                &fixture.worker.owner,
                fixture.now,
                TimeDelta::seconds(fixture.worker.config.claim_lease_seconds),
            )
            .unwrap()
            .unwrap();
        let core = fixture
            .worker
            .ledger
            .acquire_owner(
                OwnerRole::Core,
                "core-owner-delivery-test",
                fixture.now + TimeDelta::seconds(1),
                TimeDelta::seconds(60),
            )
            .unwrap();
        fixture
            .worker
            .ledger
            .cancel_event(
                &core,
                claimed.intent.intent_id().as_str(),
                "source_cancelled",
                fixture.now + TimeDelta::seconds(1),
            )
            .unwrap();

        let summary = fixture
            .worker
            .process_claimed_at(
                &claimed,
                Some((
                    fixture.now + TimeDelta::seconds(2),
                    fixture.now + TimeDelta::seconds(2),
                )),
                RecoverySummary::default(),
            )
            .unwrap();
        assert_eq!(summary.cancelled, 1);
        assert_eq!(fixture.worker.transport.calls(), 0);
    }

    #[test]
    fn graceful_shutdown_releases_owner_for_immediate_replacement() {
        let mut fixture = fixture(TransportResult::Delivered {
            provider_message_id: None,
        });
        let first_generation = fixture.worker.owner.generation();
        fixture.worker.shutdown().unwrap();
        let replacement = fixture
            .worker
            .ledger
            .acquire_owner(
                OwnerRole::Delivery,
                "delivery-owner-replacement",
                fixture.now + TimeDelta::seconds(1),
                TimeDelta::seconds(60),
            )
            .unwrap();
        assert_eq!(replacement.generation(), first_generation + 1);
    }

    #[test]
    fn missing_target_adapter_dead_letters_without_transport_or_restart_loop() {
        let mut fixture = fixture(TransportResult::Delivered {
            provider_message_id: None,
        });
        fixture.worker.targets = TargetRegistry::new(&[]).unwrap();

        let summary = fixture.worker.run_once_at(fixture.now).unwrap();
        assert_eq!(summary.dead_letter, 1);
        assert_eq!(fixture.worker.transport.calls(), 0);
        assert_eq!(fixture.worker.health().unwrap().dead_letter, 1);
    }

    #[test]
    fn changed_adapter_channel_is_rejected_before_transport() {
        let mut fixture = fixture(TransportResult::Delivered {
            provider_message_id: None,
        });
        fixture.worker.targets = TargetRegistry::new(&[crate::TargetConfig::Feishu {
            key: "bark-primary".to_owned(),
            endpoint_env: "SPX_TEST_FEISHU_ENDPOINT".to_owned(),
            secret_env: None,
        }])
        .unwrap();

        let summary = fixture.worker.run_once_at(fixture.now).unwrap();
        assert_eq!(summary.dead_letter, 1);
        assert_eq!(fixture.worker.transport.calls(), 0);
        assert_eq!(fixture.worker.health().unwrap().dead_letter, 1);
    }

    #[test]
    fn network_requires_both_independent_permissions() {
        let mut fixture = fixture(TransportResult::Delivered {
            provider_message_id: None,
        });
        fixture.worker.gate = NetworkGate::new(true, false);
        assert!(matches!(
            fixture.worker.run_once_at(fixture.now),
            Err(WorkerError::NetworkNotAuthorized)
        ));
        assert_eq!(fixture.worker.transport.calls(), 0);
        assert_eq!(fixture.worker.health().unwrap().pending, 1);
    }

    #[test]
    fn unauthorized_open_does_not_touch_the_ledger() {
        let temp = TempDir::new().unwrap();
        let ledger_path = temp.path().join("not-created.sqlite");
        let config = DeliveryConfig {
            ledger_path: ledger_path.clone(),
            network_enabled: true,
            poll_interval_millis: 1,
            owner_lease_seconds: 60,
            claim_lease_seconds: 20,
            request_timeout_seconds: 5,
            retry_schedule_seconds: vec![15],
            targets: vec![crate::TargetConfig::Bark {
                key: "bark-primary".to_owned(),
                endpoint_env: "SPX_TEST_BARK_ENDPOINT".to_owned(),
                presentation: crate::BarkPresentation::Full,
            }],
        };
        let result = DeliveryWorker::open(
            config,
            false,
            "delivery-owner-not-authorized",
            Utc::now(),
            MockTransport::new(TransportResult::Delivered {
                provider_message_id: None,
            }),
        );
        assert!(matches!(result, Err(WorkerError::NetworkNotAuthorized)));
        assert!(!ledger_path.exists());
    }
}
