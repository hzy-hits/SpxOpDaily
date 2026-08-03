use chrono::{DateTime, TimeDelta, Utc};
use serde::Serialize;
use spx_domain::{
    DECISION_SCHEMA_VERSION, DeskMessageV1, DomainError, EvaluationRequestV1, IngressEnvelopeV1,
    IngressMessageV1, NOTIFICATION_INTENT_SCHEMA_VERSION, NotificationIntentV1,
    NotificationTargetV1, StrategyAction, StrategyBlockReason, StrategyDecisionV1, Token, Validate,
    canonical_json_hash,
};
use spx_ledger::{
    IngressCheck, IngressWrite, Ledger, LedgerError, OwnerLease, OwnerRole, PersistWrite,
};
use thiserror::Error;
use uuid::Uuid;

use crate::projection::{ProjectionError, ProjectionStore};
use crate::quote_book::{ApplyBatch, QuoteBook, QuoteBookError};
use crate::raw_log::{AppendDurability, RawLog, RawLogError};
use crate::readiness::assess_readiness;
use crate::research_projection::{
    ResearchDisposition, ResearchProjectionError, ResearchProjectionStore,
};
use crate::{CoreConfig, ReadinessAssessment};

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("domain contract error: {0}")]
    Domain(#[from] DomainError),
    #[error("quote book error: {0}")]
    QuoteBook(#[from] QuoteBookError),
    #[error("ledger error: {0}")]
    Ledger(#[from] LedgerError),
    #[error("raw log error: {0}")]
    RawLog(#[from] RawLogError),
    #[error("projection error: {0}")]
    Projection(#[from] ProjectionError),
    #[error("research projection error: {0}")]
    ResearchProjection(#[from] ResearchProjectionError),
    #[error("invalid owner lease configuration")]
    OwnerLeaseConfiguration,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum CoreOutcome {
    Duplicate {
        message_id: Token,
    },
    QuoteBatch {
        message_id: Token,
        disposition: QuoteDisposition,
    },
    Decision {
        message_id: Token,
        decision: Box<StrategyDecisionV1>,
        notification_enqueued: bool,
        persist_disposition: PersistDisposition,
    },
    ResearchSignals {
        message_id: Token,
        disposition: ResearchDisposition,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QuoteDisposition {
    Applied,
    DuplicateBatch,
    StaleBatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PersistDisposition {
    Inserted,
    Duplicate,
}

#[derive(Debug, Serialize)]
struct LatestProjection<'a> {
    schema_version: &'static str,
    published_at: DateTime<Utc>,
    outcome: &'a CoreOutcome,
    ledger: spx_ledger::LedgerHealth,
}

pub struct CoreEngine {
    config: CoreConfig,
    ledger: Ledger,
    owner: OwnerLease,
    quote_book: QuoteBook,
    raw_log: RawLog,
    projection: ProjectionStore,
    research_projection: ResearchProjectionStore,
    owner_released: bool,
}

impl CoreEngine {
    /// Opens the fenced core runtime and acquires exclusive core ownership.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid config, ledger, raw-log, projection, or owner failures.
    pub fn open(config: CoreConfig, now: DateTime<Utc>) -> Result<Self, CoreError> {
        config
            .validate()
            .map_err(|_| CoreError::OwnerLeaseConfiguration)?;
        let ledger = Ledger::open(&config.ledger_path)?;
        ledger.quick_check()?;
        let raw_log = RawLog::with_min_free_bytes(
            &config.raw_log_dir,
            config.raw_segment_max_bytes,
            config.raw_log_min_free_bytes,
        )?;
        let projection = ProjectionStore::new(&config.projection_path);
        let research_projection = ResearchProjectionStore::open(&config.research_projection_path)?;
        let quote_book = QuoteBook::new(
            config.quote_cache_retention_seconds,
            config.quote_cache_max_entries,
            config.batch_identity_cache_max_entries,
        )?;
        let owner_id = format!("core:{}", Uuid::new_v4());
        let owner = ledger.acquire_owner(
            OwnerRole::Core,
            &owner_id,
            now,
            TimeDelta::seconds(config.owner_lease_seconds),
        )?;
        Ok(Self {
            config,
            ledger,
            owner,
            quote_book,
            raw_log,
            projection,
            research_projection,
            owner_released: false,
        })
    }

    /// Validates, audits, processes, projects, and finally records one ingress envelope.
    ///
    /// # Errors
    ///
    /// Returns an error when any domain, quote-book, ledger, audit, or projection boundary fails.
    pub fn process(
        &mut self,
        envelope: IngressEnvelopeV1,
        processing_at: DateTime<Utc>,
    ) -> Result<CoreOutcome, CoreError> {
        envelope.validate()?;
        self.renew_owner_if_needed(processing_at)?;
        let durability = match &envelope.message {
            IngressMessageV1::QuoteBatch(_) => AppendDurability::Buffered,
            IngressMessageV1::Evaluate(_) | IngressMessageV1::ResearchSignals(_) => {
                AppendDurability::Durable
            }
        };
        let payload_sha256 = self.raw_log.append(&envelope, processing_at, durability)?;
        if envelope.emitted_at > processing_at {
            return Err(
                DomainError::TimeOrder("envelope emitted_at is after processing_at").into(),
            );
        }
        if self.ledger.check_ingress(
            &self.owner,
            &envelope.message_id,
            &payload_sha256,
            processing_at,
        )? == IngressCheck::Duplicate
        {
            return Ok(CoreOutcome::Duplicate {
                message_id: envelope.message_id,
            });
        }
        let ingress_message_id = envelope.message_id.clone();
        let message_id = envelope.message_id;
        let outcome = match envelope.message {
            IngressMessageV1::QuoteBatch(batch) => {
                let disposition = match self.quote_book.apply(batch)? {
                    ApplyBatch::Applied => QuoteDisposition::Applied,
                    ApplyBatch::Duplicate => QuoteDisposition::DuplicateBatch,
                    ApplyBatch::Stale => QuoteDisposition::StaleBatch,
                };
                CoreOutcome::QuoteBatch {
                    message_id,
                    disposition,
                }
            }
            IngressMessageV1::Evaluate(request) => {
                self.evaluate(message_id, &request, processing_at)?
            }
            IngressMessageV1::ResearchSignals(signals) => {
                let disposition =
                    self.research_projection
                        .apply(message_id.clone(), signals, processing_at)?;
                CoreOutcome::ResearchSignals {
                    message_id,
                    disposition,
                }
            }
        };
        self.publish(&outcome, processing_at)?;
        let recorded = self.ledger.record_ingress_once(
            &self.owner,
            &ingress_message_id,
            &payload_sha256,
            processing_at,
        )?;
        if recorded == IngressWrite::Duplicate {
            return Ok(CoreOutcome::Duplicate {
                message_id: ingress_message_id,
            });
        }
        Ok(outcome)
    }

    /// Releases the core owner fence so a replacement can start immediately.
    ///
    /// # Errors
    ///
    /// Returns an error if this process no longer owns the recorded generation.
    pub fn shutdown(&mut self) -> Result<(), CoreError> {
        if !self.owner_released {
            self.ledger.release_owner(&self.owner)?;
            self.owner_released = true;
        }
        Ok(())
    }

    /// Renews or reacquires the fenced core role during idle ingress periods.
    ///
    /// # Errors
    ///
    /// Returns an error when a competing owner has taken the role or storage fails.
    pub fn heartbeat(&mut self, now: DateTime<Utc>) -> Result<(), CoreError> {
        self.renew_owner_if_needed(now)
    }

    fn evaluate(
        &mut self,
        message_id: Token,
        request: &EvaluationRequestV1,
        processing_at: DateTime<Utc>,
    ) -> Result<CoreOutcome, CoreError> {
        if request.decision_at > processing_at {
            return Err(DomainError::TimeOrder("decision_at is after processing_at").into());
        }
        let snapshot = self.quote_book.snapshot(request.session, processing_at)?;
        let mut assessment =
            assess_readiness(request, &snapshot, processing_at, &self.config.readiness);
        add_hard_gate_reasons(request, processing_at, &self.config, &mut assessment);
        let evidence_hash = canonical_json_hash(&(
            snapshot.provenance_hash.as_str(),
            request.request_id.as_str(),
            assessment.block_reasons(),
            assessment.exact_legs(),
        ))?;
        let decision_hash = canonical_json_hash(&(
            request.request_id.as_str(),
            request.policy_version.as_str(),
            snapshot.snapshot_id.as_str(),
        ))?;
        let action = if assessment.ready() {
            StrategyAction::ManualCandidate
        } else {
            StrategyAction::NoTrade
        };
        let (exact_legs, block_reasons) = assessment.into_decision_parts();
        let decision = StrategyDecisionV1 {
            schema_version: DECISION_SCHEMA_VERSION.to_owned(),
            decision_id: Token::new(format!("decision:{}", &decision_hash[..24]), "decision_id")?,
            request_id: request.request_id.clone(),
            strategy_id: request.strategy_id.clone(),
            policy_version: request.policy_version.clone(),
            snapshot_id: snapshot.snapshot_id,
            action,
            direction: (action == StrategyAction::ManualCandidate).then_some(request.direction),
            evaluated_at: request.decision_at,
            valid_until: request.valid_until,
            block_reasons,
            exact_legs,
            evidence_hash: Token::new(evidence_hash, "evidence_hash")?,
            automatic_ordering: false,
        };
        decision.validate()?;
        let intent = if decision.action == StrategyAction::ManualCandidate {
            Some(build_notification(
                request,
                &decision,
                self.config.delivery_max_attempts,
                &self.config,
            )?)
        } else {
            None
        };
        let persist =
            self.ledger
                .persist_decision(&self.owner, &decision, intent.as_ref(), processing_at)?;
        Ok(CoreOutcome::Decision {
            message_id,
            decision: Box::new(decision),
            notification_enqueued: intent.is_some(),
            persist_disposition: match persist {
                PersistWrite::Inserted => PersistDisposition::Inserted,
                PersistWrite::Duplicate => PersistDisposition::Duplicate,
            },
        })
    }

    fn publish(&self, outcome: &CoreOutcome, published_at: DateTime<Utc>) -> Result<(), CoreError> {
        let projection = LatestProjection {
            schema_version: "spx_core_projection.v1",
            published_at,
            outcome,
            ledger: self.ledger.health()?,
        };
        self.projection.publish(&projection)?;
        Ok(())
    }

    fn renew_owner_if_needed(&mut self, now: DateTime<Utc>) -> Result<(), CoreError> {
        let renewal_margin = TimeDelta::seconds(self.config.owner_lease_seconds / 3);
        if self.owner.lease_until() - now <= renewal_margin {
            self.ledger.refresh_owner(
                &mut self.owner,
                now,
                TimeDelta::seconds(self.config.owner_lease_seconds),
            )?;
        }
        Ok(())
    }
}

impl Drop for CoreEngine {
    fn drop(&mut self) {
        if !self.owner_released {
            let _ = self.ledger.release_owner(&self.owner);
            self.owner_released = true;
        }
    }
}

fn add_hard_gate_reasons(
    request: &EvaluationRequestV1,
    processing_at: DateTime<Utc>,
    config: &CoreConfig,
    assessment: &mut ReadinessAssessment,
) {
    if request.calendar == spx_domain::CalendarState::Closed {
        assessment.block(StrategyBlockReason::CalendarClosed);
    }
    if request.macro_permission == spx_domain::MacroPermission::Blocked {
        assessment.block(StrategyBlockReason::MacroEventBlocked);
    }
    if request.plan_state == spx_domain::PlanState::Conflict {
        assessment.block(StrategyBlockReason::ActivePlanConflict);
    }
    if request.notification_targets.iter().any(|target| {
        !config
            .notification_targets
            .iter()
            .any(|configured| &configured.key == target)
    }) {
        assessment.block(StrategyBlockReason::NotificationTargetUnavailable);
    }
    if processing_at >= request.valid_until {
        assessment.block(StrategyBlockReason::TtlExpired);
    }
    let evaluation_delay = signed_seconds(processing_at, request.decision_at);
    if evaluation_delay > config.evaluation_max_delay_seconds {
        assessment.block(StrategyBlockReason::EvaluationDelayed);
    }
    if signed_seconds(request.valid_until, request.decision_at)
        > f64::from(config.decision_max_ttl_seconds)
    {
        assessment.block(StrategyBlockReason::DecisionTtlInvalid);
    }
}

fn signed_seconds(later: DateTime<Utc>, earlier: DateTime<Utc>) -> f64 {
    if later >= earlier {
        (later - earlier)
            .to_std()
            .map_or(f64::INFINITY, |duration| duration.as_secs_f64())
    } else {
        -((earlier - later)
            .to_std()
            .map_or(f64::INFINITY, |duration| duration.as_secs_f64()))
    }
}

fn build_notification(
    request: &EvaluationRequestV1,
    decision: &StrategyDecisionV1,
    max_attempts: u32,
    config: &CoreConfig,
) -> Result<NotificationIntentV1, DomainError> {
    let legs = decision.exact_legs.as_ref().ok_or(DomainError::Invalid {
        field: "exact_legs",
        reason: "manual candidate requires exact legs",
    })?;
    let targets: Vec<NotificationTargetV1> = request
        .notification_targets
        .iter()
        .map(|key| {
            config
                .notification_targets
                .iter()
                .find(|configured| &configured.key == key)
                .map(|configured| NotificationTargetV1 {
                    key: key.clone(),
                    channel: configured.channel,
                })
                .ok_or(DomainError::Invalid {
                    field: "notification_targets",
                    reason: "target is not configured",
                })
        })
        .collect::<Result<_, _>>()?;
    let intent_hash = canonical_json_hash(&(decision.decision_id.as_str(), &targets))?;
    let direction = match request.direction {
        spx_domain::CandidateDirection::CallVertical10 => "Call vertical 10",
        spx_domain::CandidateDirection::PutVertical10 => "Put vertical 10",
    };
    let intent = NotificationIntentV1 {
        schema_version: NOTIFICATION_INTENT_SCHEMA_VERSION.to_owned(),
        intent_id: Token::new(format!("intent:{}", &intent_hash[..24]), "intent_id")?,
        semantic_id: Token::new(
            format!("{}:{}", request.strategy_id, request.request_id),
            "semantic_id",
        )?,
        decision_id: decision.decision_id.clone(),
        created_at: decision.evaluated_at,
        expires_at: decision.valid_until,
        message: DeskMessageV1 {
            title: Token::new("SPX Manual Candidate", "title")?,
            desk_view: Token::new(direction, "desk_view")?,
            execution: Token::new(
                format!(
                    "{} / {}; executable debit {:.2}",
                    legs.long_contract_id,
                    legs.short_contract_id,
                    legs.long_ask.get() - legs.short_bid.get()
                ),
                "execution",
            )?,
            risk: Token::new("Manual review only; automatic ordering disabled", "risk")?,
            targets: Token::new(
                format!(
                    "long {:.0}, short {:.0}",
                    legs.long_strike.get(),
                    legs.short_strike.get()
                ),
                "targets",
            )?,
            data_quality: Token::new(
                format!(
                    "{:?} exact live NBBO; max age {:.3}s; max skew {:.3}s",
                    legs.provider,
                    legs.max_age_seconds.get(),
                    legs.max_skew_seconds.get()
                ),
                "data_quality",
            )?,
        },
        targets,
        max_attempts,
    };
    intent.validate()?;
    Ok(intent)
}
