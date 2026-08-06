use chrono::{DateTime, NaiveDate, TimeDelta, Utc};
use spx_core::{
    ApplyBatch, CoreConfig, CoreEngine, CoreError, CoreOutcome, NotificationTargetConfig,
    QuoteBook, QuoteBookError, ReadinessConfig,
};
use spx_domain::{
    AuthenticationState, BookSideV1, CalendarState, CandidateDirection, DeliveryChannel,
    DeskMapProjectionV1, EntitlementState, EvaluationRequestV1, INGRESS_SCHEMA_VERSION,
    IngressEnvelopeV1, IngressMessageV1, InstrumentKind, MacroPermission, MarketSession,
    NotificationTargetV1, OPERATOR_NOTIFICATION_CANCELLATION_SCHEMA_VERSION,
    OPERATOR_NOTIFICATION_SCHEMA_VERSION, OperationalState, OperatorNotificationCancellationV1,
    OperatorNotificationRole, OperatorNotificationV1, OptionContractV1, OptionRight,
    PROVIDER_STATE_SCHEMA_VERSION, PlanState, PositiveF64, Provider, ProviderReasonCode,
    ProviderStateV1, QUOTE_BATCH_SCHEMA_VERSION, QuoteBatchMode, QuoteBatchV1, QuoteQuality,
    QuoteV1, ResearchSignalsV1, StrategyAction, StrategyBlockReason,
    StrategyDistributionForecastV1, Token, TransportState,
};
use spx_ledger::Ledger;
use tempfile::TempDir;

fn token(value: impl Into<String>) -> Token {
    Token::new(value, "test token").expect("test token must be valid")
}

fn operator_body(desk_view: &str) -> String {
    format!(
        "## Desk View\n{desk_view}\n\n## Execution\nmanual only\n\n## Risk\ndefined risk\n\n## Targets\nnext level\n\n## Data Quality\nlive"
    )
}

fn at(value: &str) -> DateTime<Utc> {
    value.parse().expect("test timestamp must be valid")
}

fn positive(value: f64) -> PositiveF64 {
    PositiveF64::new(value, "test number").expect("test number must be positive")
}

fn config(temp: &TempDir) -> CoreConfig {
    CoreConfig {
        socket_path: temp.path().join("run/core.sock"),
        ledger_path: temp.path().join("state/spx-ledger.sqlite"),
        raw_log_dir: temp.path().join("raw"),
        projection_path: temp.path().join("projection/latest.json"),
        research_projection_path: temp.path().join("projection/research.json"),
        desk_map_projection_path: temp.path().join("projection/desk-map.json"),
        strategy_distribution_projection_path: temp
            .path()
            .join("projection/strategy-distribution.json"),
        max_frame_bytes: 1_048_576,
        max_connections: 8,
        raw_segment_max_bytes: 64 * 1024 * 1024,
        raw_log_min_free_bytes: 64 * 1024 * 1024,
        quote_cache_retention_seconds: 300,
        quote_cache_max_entries: 4096,
        batch_identity_cache_max_entries: 4096,
        owner_lease_seconds: 30,
        delivery_max_attempts: 5,
        notification_targets: vec![NotificationTargetConfig {
            key: token("desk-bark"),
            channel: DeliveryChannel::Bark,
        }],
        decision_max_ttl_seconds: 120,
        evaluation_max_delay_seconds: 2.0,
        readiness: ReadinessConfig {
            quote_max_age_seconds: 5.0,
            max_side_skew_seconds: 5.0,
            allow_rth_ibkr_fallback: false,
        },
    }
}

fn provider_state(
    provider: Provider,
    observed_at: DateTime<Utc>,
    operational: OperationalState,
) -> ProviderStateV1 {
    let live = operational == OperationalState::Live;
    ProviderStateV1 {
        schema_version: PROVIDER_STATE_SCHEMA_VERSION.to_owned(),
        provider,
        observed_at,
        operational,
        transport: if live {
            TransportState::Connected
        } else {
            TransportState::Disconnected
        },
        authentication: AuthenticationState::Authenticated,
        entitlement: if live {
            EntitlementState::Live
        } else {
            EntitlementState::Missing
        },
        reason_codes: if operational == OperationalState::ExternalSessionOwns {
            vec![ProviderReasonCode::CompetingSession10197]
        } else if live {
            vec![ProviderReasonCode::Healthy]
        } else {
            vec![ProviderReasonCode::ProviderError]
        },
        latency_ms: None,
        connection_generation: 1,
    }
}

fn side(price: f64, timestamp: DateTime<Utc>) -> BookSideV1 {
    BookSideV1 {
        price: positive(price),
        source_at: timestamp,
        received_at: timestamp + TimeDelta::milliseconds(50),
    }
}

fn option_quote(
    provider: Provider,
    session: MarketSession,
    contract_id: &str,
    strike: f64,
    bid: f64,
    ask: f64,
    timestamp: DateTime<Utc>,
) -> QuoteV1 {
    QuoteV1 {
        quote_id: token(format!("quote:{provider:?}:{contract_id}")),
        provider,
        instrument_id: token(contract_id),
        instrument_kind: InstrumentKind::Option,
        market_session: session,
        quality: QuoteQuality::Live,
        option: Some(OptionContractV1 {
            contract_id: token(contract_id),
            underlier: token("SPX"),
            trading_class: token("SPXW"),
            expiry: NaiveDate::from_ymd_opt(2026, 7, 31).expect("valid test date"),
            strike: positive(strike),
            right: OptionRight::Call,
            multiplier: 100,
        }),
        bid: Some(side(bid, timestamp)),
        ask: Some(side(ask, timestamp + TimeDelta::milliseconds(100))),
        last: None,
    }
}

fn quote_envelope(
    message_id: &str,
    provider: Provider,
    session: MarketSession,
    operational: OperationalState,
    sequence: u64,
    quote_at: DateTime<Utc>,
) -> IngressEnvelopeV1 {
    let received_at = quote_at + TimeDelta::milliseconds(500);
    IngressEnvelopeV1 {
        schema_version: INGRESS_SCHEMA_VERSION.to_owned(),
        message_id: token(message_id),
        emitted_at: received_at,
        message: IngressMessageV1::QuoteBatch(QuoteBatchV1 {
            schema_version: QUOTE_BATCH_SCHEMA_VERSION.to_owned(),
            batch_id: token(format!("batch:{provider:?}:{sequence}")),
            provider,
            mode: QuoteBatchMode::Incremental,
            sequence,
            received_at,
            provider_state: provider_state(provider, quote_at, operational),
            quotes: if operational == OperationalState::Live {
                vec![
                    option_quote(
                        provider,
                        session,
                        "SPXW-20260731-C-6350",
                        6350.0,
                        3.8,
                        4.0,
                        quote_at,
                    ),
                    option_quote(
                        provider,
                        session,
                        "SPXW-20260731-C-6360",
                        6360.0,
                        1.5,
                        1.7,
                        quote_at + TimeDelta::milliseconds(200),
                    ),
                ]
            } else {
                Vec::new()
            },
        }),
    }
}

fn configure_quote_batch(
    envelope: &mut IngressEnvelopeV1,
    connection_generation: u64,
    include_quotes: bool,
) {
    let IngressMessageV1::QuoteBatch(batch) = &mut envelope.message else {
        panic!("test envelope must contain a quote batch");
    };
    batch.provider_state.connection_generation = connection_generation;
    if !include_quotes {
        batch.quotes.clear();
    }
}

fn apply_quote_batch(
    book: &mut QuoteBook,
    mut envelope: IngressEnvelopeV1,
    connection_generation: u64,
    include_quotes: bool,
) {
    configure_quote_batch(&mut envelope, connection_generation, include_quotes);
    let IngressMessageV1::QuoteBatch(batch) = envelope.message else {
        panic!("test envelope must contain a quote batch");
    };
    assert_eq!(book.apply(batch).unwrap(), ApplyBatch::Applied);
}

fn evaluation_envelope(
    message_id: &str,
    session: MarketSession,
    decision_at: DateTime<Utc>,
) -> IngressEnvelopeV1 {
    IngressEnvelopeV1 {
        schema_version: INGRESS_SCHEMA_VERSION.to_owned(),
        message_id: token(message_id),
        emitted_at: decision_at,
        message: IngressMessageV1::Evaluate(EvaluationRequestV1 {
            schema_version: spx_domain::EVALUATION_SCHEMA_VERSION.to_owned(),
            request_id: token(format!("request:{message_id}")),
            strategy_id: token("manual-vertical-v1"),
            policy_version: token("policy-2026-08-01"),
            session,
            session_date: NaiveDate::from_ymd_opt(2026, 7, 31).expect("valid test date"),
            decision_at,
            valid_until: decision_at + TimeDelta::minutes(1),
            direction: CandidateDirection::CallVertical10,
            long_contract_id: token("SPXW-20260731-C-6350"),
            short_contract_id: token("SPXW-20260731-C-6360"),
            calendar: CalendarState::Open,
            macro_permission: MacroPermission::Allowed,
            plan_state: PlanState::Clear,
            notification_targets: vec![token("desk-bark")],
            automatic_ordering: false,
        }),
    }
}

fn decision(outcome: CoreOutcome) -> spx_domain::StrategyDecisionV1 {
    match outcome {
        CoreOutcome::Decision { decision, .. } => *decision,
        other => panic!("expected decision, got {other:?}"),
    }
}

#[test]
fn rth_schwab_fresh_exact_legs_enqueues_manual_candidate() {
    let temp = TempDir::new().expect("temp directory");
    let config = config(&temp);
    let now = at("2026-07-31T14:30:00Z");
    let mut engine = CoreEngine::open(config.clone(), now).expect("open core");
    engine
        .process(
            quote_envelope(
                "message:quotes:1",
                Provider::Schwab,
                MarketSession::Rth,
                OperationalState::Live,
                1,
                now - TimeDelta::seconds(1),
            ),
            now,
        )
        .expect("accept quote batch");

    let outcome = engine
        .process(
            evaluation_envelope("message:evaluate:1", MarketSession::Rth, now),
            now,
        )
        .expect("evaluate candidate");
    let decision = decision(outcome);
    assert_eq!(decision.action, StrategyAction::ManualCandidate);
    assert!(decision.block_reasons.is_empty());
    assert!(!decision.automatic_ordering);

    let health = Ledger::open(&config.ledger_path)
        .expect("open ledger")
        .health()
        .expect("read ledger health");
    assert_eq!(health.pending, 1);
    assert_eq!(health.delivered, 0);
}

#[test]
fn gth_schwab_is_fail_closed() {
    let temp = TempDir::new().expect("temp directory");
    let now = at("2026-07-31T08:30:00Z");
    let mut engine = CoreEngine::open(config(&temp), now).expect("open core");
    engine
        .process(
            quote_envelope(
                "message:quotes:gth-schwab",
                Provider::Schwab,
                MarketSession::Gth,
                OperationalState::Live,
                1,
                now - TimeDelta::seconds(1),
            ),
            now,
        )
        .expect("accept quote batch");
    let result = engine
        .process(
            evaluation_envelope("message:evaluate:gth-schwab", MarketSession::Gth, now),
            now,
        )
        .expect("produce no-trade decision");
    let decision = decision(result);
    assert_eq!(decision.action, StrategyAction::NoTrade);
    assert_eq!(
        decision.block_reasons,
        vec![StrategyBlockReason::ProviderNotReady]
    );
}

#[test]
fn ibkr_10197_blocks_gth_without_displacing_external_session() {
    let temp = TempDir::new().expect("temp directory");
    let now = at("2026-07-31T08:30:00Z");
    let mut engine = CoreEngine::open(config(&temp), now).expect("open core");
    engine
        .process(
            quote_envelope(
                "message:quotes:10197",
                Provider::Ibkr,
                MarketSession::Gth,
                OperationalState::ExternalSessionOwns,
                1,
                now - TimeDelta::seconds(1),
            ),
            now,
        )
        .expect("accept provider state");
    let result = engine
        .process(
            evaluation_envelope("message:evaluate:10197", MarketSession::Gth, now),
            now,
        )
        .expect("produce no-trade decision");
    let decision = decision(result);
    assert_eq!(decision.action, StrategyAction::NoTrade);
    assert_eq!(
        decision.block_reasons,
        vec![StrategyBlockReason::ProviderExternalSessionOwns]
    );
}

#[test]
fn stale_provider_state_blocks_even_when_quotes_look_fresh() {
    let temp = TempDir::new().expect("temp directory");
    let now = at("2026-07-31T14:30:00Z");
    let mut envelope = quote_envelope(
        "message:quotes:stale-provider",
        Provider::Schwab,
        MarketSession::Rth,
        OperationalState::Live,
        1,
        now - TimeDelta::seconds(1),
    );
    if let IngressMessageV1::QuoteBatch(batch) = &mut envelope.message {
        batch.provider_state.observed_at = now - TimeDelta::minutes(5);
    }
    let mut engine = CoreEngine::open(config(&temp), now).expect("open core");
    engine.process(envelope, now).expect("accept quote batch");
    let result = engine
        .process(
            evaluation_envelope("message:evaluate:stale-provider", MarketSession::Rth, now),
            now,
        )
        .expect("produce no-trade decision");
    let decision = decision(result);
    assert_eq!(decision.action, StrategyAction::NoTrade);
    assert!(
        decision
            .block_reasons
            .contains(&StrategyBlockReason::ProviderNotReady)
    );
}

#[test]
fn rejected_evaluation_can_be_retried_after_quotes_arrive() {
    let temp = TempDir::new().expect("temp directory");
    let now = at("2026-07-31T14:30:00Z");
    let mut engine = CoreEngine::open(config(&temp), now).expect("open core");
    let evaluation = evaluation_envelope("message:evaluate:retry", MarketSession::Rth, now);
    assert!(engine.process(evaluation.clone(), now).is_err());

    engine
        .process(
            quote_envelope(
                "message:quotes:retry",
                Provider::Schwab,
                MarketSession::Rth,
                OperationalState::Live,
                1,
                now - TimeDelta::seconds(1),
            ),
            now,
        )
        .expect("accept quote batch");
    let result = engine
        .process(evaluation, now)
        .expect("retry should be evaluated, not suppressed as duplicate");
    assert_eq!(decision(result).action, StrategyAction::ManualCandidate);
}

#[test]
fn same_batch_identity_with_different_payload_is_a_collision() {
    let now = at("2026-07-31T14:30:00Z");
    let envelope = quote_envelope(
        "message:quotes:collision",
        Provider::Schwab,
        MarketSession::Rth,
        OperationalState::Live,
        1,
        now - TimeDelta::seconds(1),
    );
    let IngressMessageV1::QuoteBatch(batch) = envelope.message else {
        panic!("test envelope must contain a quote batch");
    };
    let mut book = QuoteBook::default();
    assert_eq!(book.apply(batch.clone()).unwrap(), ApplyBatch::Applied);
    assert_eq!(book.apply(batch.clone()).unwrap(), ApplyBatch::Duplicate);

    let mut collision = batch;
    collision.quotes[0].ask.as_mut().unwrap().price = positive(4.1);
    assert!(matches!(
        book.apply(collision),
        Err(QuoteBookError::BatchIdentityCollision(_))
    ));
}

#[test]
fn recent_batch_identity_collision_survives_cursor_advance() {
    let now = at("2026-07-31T14:30:00Z");
    let IngressMessageV1::QuoteBatch(first) = quote_envelope(
        "message:quotes:identity-first",
        Provider::Schwab,
        MarketSession::Rth,
        OperationalState::Live,
        1,
        now,
    )
    .message
    else {
        panic!("test envelope must contain a quote batch");
    };
    let IngressMessageV1::QuoteBatch(second) = quote_envelope(
        "message:quotes:identity-second",
        Provider::Schwab,
        MarketSession::Rth,
        OperationalState::Live,
        2,
        now + TimeDelta::seconds(1),
    )
    .message
    else {
        panic!("test envelope must contain a quote batch");
    };
    let mut collision = first.clone();
    collision.sequence = 3;
    collision.received_at += TimeDelta::seconds(2);

    let mut book = QuoteBook::new(300, 16, 16).expect("bounded quote book");
    assert_eq!(book.apply(first).unwrap(), ApplyBatch::Applied);
    assert_eq!(book.apply(second).unwrap(), ApplyBatch::Applied);
    assert!(matches!(
        book.apply(collision),
        Err(QuoteBookError::BatchIdentityCollision(_))
    ));
}

#[test]
fn quote_book_prunes_by_watermark_and_entry_limit() {
    let now = at("2026-07-31T14:30:00Z");
    let IngressMessageV1::QuoteBatch(first) = quote_envelope(
        "message:quotes:bounded-first",
        Provider::Schwab,
        MarketSession::Rth,
        OperationalState::Live,
        1,
        now,
    )
    .message
    else {
        panic!("test envelope must contain a quote batch");
    };
    let mut entry_bounded = QuoteBook::new(300, 1, 16).expect("entry-bounded quote book");
    entry_bounded
        .apply(first.clone())
        .expect("apply first batch");
    assert_eq!(
        entry_bounded
            .snapshot(MarketSession::Rth, now)
            .expect("bounded snapshot")
            .quotes
            .len(),
        1
    );

    let IngressMessageV1::QuoteBatch(mut later) = quote_envelope(
        "message:quotes:bounded-later",
        Provider::Schwab,
        MarketSession::Rth,
        OperationalState::Live,
        2,
        now + TimeDelta::seconds(61),
    )
    .message
    else {
        panic!("test envelope must contain a quote batch");
    };
    later.quotes.clear();
    let mut time_bounded = QuoteBook::new(60, 16, 16).expect("time-bounded quote book");
    time_bounded.apply(first).expect("apply first batch");
    time_bounded.apply(later).expect("advance watermark");
    assert!(
        time_bounded
            .snapshot(MarketSession::Rth, now + TimeDelta::seconds(61))
            .expect("pruned snapshot")
            .quotes
            .is_empty()
    );
}

#[test]
fn same_provider_contract_is_isolated_by_market_session() {
    let now = at("2026-07-31T08:30:00Z");
    let mut book = QuoteBook::default();
    apply_quote_batch(
        &mut book,
        quote_envelope(
            "message:quotes:session-gth",
            Provider::Ibkr,
            MarketSession::Gth,
            OperationalState::Live,
            1,
            now,
        ),
        1,
        true,
    );
    apply_quote_batch(
        &mut book,
        quote_envelope(
            "message:quotes:session-rth",
            Provider::Ibkr,
            MarketSession::Rth,
            OperationalState::Live,
            2,
            now + TimeDelta::seconds(1),
        ),
        1,
        true,
    );

    let built_at = now + TimeDelta::seconds(2);
    let gth = book.snapshot(MarketSession::Gth, built_at).unwrap();
    let rth = book.snapshot(MarketSession::Rth, built_at).unwrap();
    assert_eq!(gth.quotes.len(), 2);
    assert_eq!(rth.quotes.len(), 2);
    assert!(
        gth.quotes
            .iter()
            .all(|quote| quote.market_session == MarketSession::Gth)
    );
    assert!(
        rth.quotes
            .iter()
            .all(|quote| quote.market_session == MarketSession::Rth)
    );
}

#[test]
fn replace_provider_snapshot_removes_omitted_quotes_and_accepts_empty_live_snapshot() {
    let now = at("2026-07-31T14:30:00Z");
    let mut book = QuoteBook::default();
    let IngressMessageV1::QuoteBatch(mut initial) = quote_envelope(
        "message:quotes:replace-initial",
        Provider::Schwab,
        MarketSession::Rth,
        OperationalState::Live,
        1,
        now,
    )
    .message
    else {
        panic!("test envelope must contain a quote batch");
    };
    initial.mode = QuoteBatchMode::ReplaceProviderSnapshot;
    assert_eq!(book.apply(initial).unwrap(), ApplyBatch::Applied);

    let IngressMessageV1::QuoteBatch(gth_incremental) = quote_envelope(
        "message:quotes:replace-gth-incremental",
        Provider::Schwab,
        MarketSession::Gth,
        OperationalState::Live,
        2,
        now + TimeDelta::seconds(1),
    )
    .message
    else {
        panic!("test envelope must contain a quote batch");
    };
    assert_eq!(book.apply(gth_incremental).unwrap(), ApplyBatch::Applied);

    let IngressMessageV1::QuoteBatch(mut replacement) = quote_envelope(
        "message:quotes:replace-one",
        Provider::Schwab,
        MarketSession::Rth,
        OperationalState::Live,
        3,
        now + TimeDelta::seconds(2),
    )
    .message
    else {
        panic!("test envelope must contain a quote batch");
    };
    replacement.mode = QuoteBatchMode::ReplaceProviderSnapshot;
    replacement.quotes.truncate(1);
    assert_eq!(book.apply(replacement).unwrap(), ApplyBatch::Applied);
    assert_eq!(
        book.snapshot(MarketSession::Rth, now + TimeDelta::seconds(3))
            .unwrap()
            .quotes
            .len(),
        1
    );
    assert!(
        book.snapshot(MarketSession::Gth, now + TimeDelta::seconds(3))
            .unwrap()
            .quotes
            .is_empty()
    );

    let IngressMessageV1::QuoteBatch(mut empty) = quote_envelope(
        "message:quotes:replace-empty",
        Provider::Schwab,
        MarketSession::Rth,
        OperationalState::Live,
        4,
        now + TimeDelta::seconds(3),
    )
    .message
    else {
        panic!("test envelope must contain a quote batch");
    };
    empty.mode = QuoteBatchMode::ReplaceProviderSnapshot;
    empty.quotes.clear();
    assert_eq!(book.apply(empty).unwrap(), ApplyBatch::Applied);
    assert!(
        book.snapshot(MarketSession::Rth, now + TimeDelta::seconds(4))
            .unwrap()
            .quotes
            .is_empty()
    );
}

#[test]
fn delayed_evaluation_is_no_trade_even_if_decision_time_quotes_were_fresh() {
    let temp = TempDir::new().expect("temp directory");
    let now = at("2026-07-31T14:30:00Z");
    let mut engine = CoreEngine::open(config(&temp), now).expect("open core");
    engine
        .process(
            quote_envelope(
                "message:quotes:delayed-evaluation",
                Provider::Schwab,
                MarketSession::Rth,
                OperationalState::Live,
                1,
                now - TimeDelta::seconds(1),
            ),
            now,
        )
        .expect("accept quote batch");
    let result = engine
        .process(
            evaluation_envelope("message:evaluate:delayed", MarketSession::Rth, now),
            now + TimeDelta::seconds(3),
        )
        .expect("produce delayed no-trade decision");
    let decision = decision(result);
    assert_eq!(decision.action, StrategyAction::NoTrade);
    assert!(
        decision
            .block_reasons
            .contains(&StrategyBlockReason::EvaluationDelayed)
    );
}

#[test]
fn unconfigured_notification_target_is_a_no_trade_gate() {
    let temp = TempDir::new().expect("temp directory");
    let core_config = config(&temp);
    let now = at("2026-07-31T14:30:00Z");
    let mut engine = CoreEngine::open(core_config.clone(), now).expect("open core");
    engine
        .process(
            quote_envelope(
                "message:quotes:target-gate",
                Provider::Schwab,
                MarketSession::Rth,
                OperationalState::Live,
                1,
                now - TimeDelta::seconds(1),
            ),
            now,
        )
        .expect("accept quote batch");
    let mut evaluation =
        evaluation_envelope("message:evaluate:target-gate", MarketSession::Rth, now);
    let IngressMessageV1::Evaluate(request) = &mut evaluation.message else {
        panic!("test envelope must contain an evaluation");
    };
    request.notification_targets = vec![token("unknown-target")];

    let result = engine
        .process(evaluation, now)
        .expect("unconfigured target must produce a fail-closed decision");
    let decision = decision(result);
    assert_eq!(decision.action, StrategyAction::NoTrade);
    assert_eq!(
        decision.block_reasons,
        [StrategyBlockReason::NotificationTargetUnavailable]
    );
    assert_eq!(
        Ledger::open(&core_config.ledger_path)
            .expect("open ledger")
            .health()
            .expect("ledger health")
            .pending,
        0
    );
}

#[test]
fn graceful_engine_shutdown_allows_immediate_replacement() {
    let temp = TempDir::new().unwrap();
    let config = config(&temp);
    let now = at("2026-07-31T14:30:00Z");
    let mut first = CoreEngine::open(config.clone(), now).unwrap();
    first.shutdown().unwrap();
    CoreEngine::open(config, now + TimeDelta::seconds(1)).unwrap();
}

#[test]
fn restart_deduplicates_old_ingress_and_new_generation_full_resync_rebuilds_quotes() {
    let temp = TempDir::new().expect("temp directory");
    let core_config = config(&temp);
    let opened_at = at("2026-07-31T14:30:00Z");
    let old_batch = quote_envelope(
        "message:quotes:before-restart",
        Provider::Schwab,
        MarketSession::Rth,
        OperationalState::Live,
        1,
        opened_at - TimeDelta::seconds(1),
    );
    let mut first = CoreEngine::open(core_config.clone(), opened_at).expect("open first core");
    first
        .process(old_batch.clone(), opened_at)
        .expect("accept initial quote batch");
    first.shutdown().expect("release first core owner");
    drop(first);

    let restarted_at = opened_at + TimeDelta::seconds(2);
    let mut restarted = CoreEngine::open(core_config, restarted_at).expect("open replacement core");
    assert!(matches!(
        restarted
            .process(old_batch, restarted_at)
            .expect("old ingress is an exact duplicate"),
        CoreOutcome::Duplicate { .. }
    ));

    let evaluation = evaluation_envelope(
        "message:evaluate:after-restart",
        MarketSession::Rth,
        restarted_at,
    );
    assert!(matches!(
        restarted.process(evaluation.clone(), restarted_at),
        Err(CoreError::QuoteBook(QuoteBookError::Empty))
    ));

    let mut full_resync = quote_envelope(
        "message:quotes:full-resync-generation-two",
        Provider::Schwab,
        MarketSession::Rth,
        OperationalState::Live,
        1,
        restarted_at - TimeDelta::seconds(1),
    );
    let IngressMessageV1::QuoteBatch(batch) = &mut full_resync.message else {
        panic!("test envelope must contain a quote batch");
    };
    batch.batch_id = token("batch:Schwab:generation-2:1");
    batch.provider_state.connection_generation = 2;
    batch.mode = QuoteBatchMode::ReplaceProviderSnapshot;
    restarted
        .process(full_resync, restarted_at)
        .expect("fresh full snapshot rebuilds the hot quote book");

    let decision = decision(
        restarted
            .process(evaluation, restarted_at)
            .expect("evaluation succeeds after full resync"),
    );
    assert_eq!(decision.action, StrategyAction::ManualCandidate);
}

#[test]
fn first_ingress_after_idle_lease_expiry_reacquires_and_processes() {
    let temp = TempDir::new().expect("temp directory");
    let mut core_config = config(&temp);
    core_config.owner_lease_seconds = 5;
    let opened_at = at("2026-07-31T14:30:00Z");
    let resumed_at = opened_at + TimeDelta::seconds(6);
    let mut engine = CoreEngine::open(core_config, opened_at).expect("open core");

    let outcome = engine
        .process(
            quote_envelope(
                "message:quotes:after-idle",
                Provider::Schwab,
                MarketSession::Rth,
                OperationalState::Live,
                1,
                resumed_at - TimeDelta::seconds(1),
            ),
            resumed_at,
        )
        .expect("idle core should reacquire before processing");
    assert!(matches!(outcome, CoreOutcome::QuoteBatch { .. }));
}

#[test]
fn snapshot_sources_include_batches_for_retained_quotes() {
    let now = at("2026-07-31T08:30:00Z");
    let mut book = QuoteBook::default();
    apply_quote_batch(
        &mut book,
        quote_envelope(
            "message:quotes:generation-one",
            Provider::Ibkr,
            MarketSession::Gth,
            OperationalState::Live,
            10,
            now - TimeDelta::seconds(3),
        ),
        1,
        true,
    );
    apply_quote_batch(
        &mut book,
        quote_envelope(
            "message:quotes:same-generation-state",
            Provider::Ibkr,
            MarketSession::Gth,
            OperationalState::Live,
            11,
            now - TimeDelta::seconds(2),
        ),
        1,
        false,
    );

    let snapshot = book.snapshot(MarketSession::Gth, now).unwrap();
    assert_eq!(snapshot.quotes.len(), 2);
    assert_eq!(snapshot.source_batch_ids.len(), 2);
    for expected in ["batch:Ibkr:10", "batch:Ibkr:11"] {
        assert!(
            snapshot
                .source_batch_ids
                .iter()
                .any(|batch_id| batch_id.as_str() == expected)
        );
    }
}

#[test]
fn non_live_boundary_clears_snapshot_quotes_and_same_generation_cannot_revive_them() {
    let now = at("2026-07-31T08:30:00Z");
    let mut book = QuoteBook::default();
    for (operational, sequence, include_quotes) in [
        (OperationalState::Live, 10, true),
        (OperationalState::ExternalSessionOwns, 11, false),
        (OperationalState::Live, 12, false),
    ] {
        apply_quote_batch(
            &mut book,
            quote_envelope(
                &format!("message:quotes:same-generation:{sequence}"),
                Provider::Ibkr,
                MarketSession::Gth,
                operational,
                sequence,
                now - TimeDelta::seconds(13 - i64::try_from(sequence).unwrap()),
            ),
            1,
            include_quotes,
        );
    }

    let snapshot = book.snapshot(MarketSession::Gth, now).unwrap();
    assert!(snapshot.quotes.is_empty());
    assert_eq!(
        snapshot
            .source_batch_ids
            .iter()
            .map(Token::as_str)
            .collect::<Vec<_>>(),
        vec!["batch:Ibkr:12"]
    );
    assert!(snapshot.provider_states[0].is_live());
}

#[test]
fn higher_connection_generation_resets_snapshot_quotes_and_sources() {
    let now = at("2026-07-31T08:30:00Z");
    let mut book = QuoteBook::default();
    apply_quote_batch(
        &mut book,
        quote_envelope(
            "message:quotes:generation-one",
            Provider::Ibkr,
            MarketSession::Gth,
            OperationalState::Live,
            10,
            now - TimeDelta::seconds(2),
        ),
        1,
        true,
    );
    apply_quote_batch(
        &mut book,
        quote_envelope(
            "message:quotes:generation-two",
            Provider::Ibkr,
            MarketSession::Gth,
            OperationalState::Live,
            1,
            now - TimeDelta::seconds(1),
        ),
        2,
        false,
    );

    let snapshot = book.snapshot(MarketSession::Gth, now).unwrap();
    assert!(snapshot.quotes.is_empty());
    assert_eq!(
        snapshot
            .source_batch_ids
            .iter()
            .map(Token::as_str)
            .collect::<Vec<_>>(),
        vec!["batch:Ibkr:1"]
    );
    assert_eq!(snapshot.provider_states[0].connection_generation, 2);
}

#[test]
fn gth_live_reconnect_with_empty_generation_does_not_reuse_old_ibkr_legs() {
    let temp = TempDir::new().expect("temp directory");
    let now = at("2026-07-31T08:30:00Z");
    let mut engine = CoreEngine::open(config(&temp), now).expect("open core");

    let mut generation_one = quote_envelope(
        "message:quotes:ibkr-gen1-live",
        Provider::Ibkr,
        MarketSession::Gth,
        OperationalState::Live,
        10,
        now - TimeDelta::seconds(3),
    );
    configure_quote_batch(&mut generation_one, 1, true);
    engine
        .process(generation_one, now)
        .expect("accept generation-one exact legs");

    let mut competing_session = quote_envelope(
        "message:quotes:ibkr-10197",
        Provider::Ibkr,
        MarketSession::Gth,
        OperationalState::ExternalSessionOwns,
        11,
        now - TimeDelta::seconds(2),
    );
    configure_quote_batch(&mut competing_session, 1, false);
    engine
        .process(competing_session, now)
        .expect("accept 10197 provider state");

    let mut generation_two = quote_envelope(
        "message:quotes:ibkr-gen2-live-empty",
        Provider::Ibkr,
        MarketSession::Gth,
        OperationalState::Live,
        1,
        now - TimeDelta::seconds(1),
    );
    configure_quote_batch(&mut generation_two, 2, false);
    engine
        .process(generation_two, now)
        .expect("accept empty live reconnect generation");

    let outcome = engine
        .process(
            evaluation_envelope("message:evaluate:ibkr-gen2-empty", MarketSession::Gth, now),
            now,
        )
        .expect("produce a fail-closed decision");
    let CoreOutcome::Decision {
        decision,
        notification_enqueued,
        ..
    } = outcome
    else {
        panic!("expected decision outcome");
    };
    assert_eq!(decision.action, StrategyAction::NoTrade);
    assert_eq!(
        decision.block_reasons,
        vec![StrategyBlockReason::ExactLegMissing]
    );
    assert!(!notification_enqueued);
}

#[test]
fn gth_same_generation_live_empty_after_10197_does_not_revive_old_ibkr_legs() {
    let temp = TempDir::new().expect("temp directory");
    let now = at("2026-07-31T08:30:00Z");
    let mut engine = CoreEngine::open(config(&temp), now).expect("open core");

    let mut initial_live = quote_envelope(
        "message:quotes:ibkr-same-gen-initial",
        Provider::Ibkr,
        MarketSession::Gth,
        OperationalState::Live,
        10,
        now - TimeDelta::seconds(3),
    );
    configure_quote_batch(&mut initial_live, 1, true);
    engine
        .process(initial_live, now)
        .expect("accept initial exact legs");

    let mut competing_session = quote_envelope(
        "message:quotes:ibkr-same-gen-10197",
        Provider::Ibkr,
        MarketSession::Gth,
        OperationalState::ExternalSessionOwns,
        11,
        now - TimeDelta::seconds(2),
    );
    configure_quote_batch(&mut competing_session, 1, false);
    engine
        .process(competing_session, now)
        .expect("accept 10197 provider state");

    let mut live_empty = quote_envelope(
        "message:quotes:ibkr-same-gen-live-empty",
        Provider::Ibkr,
        MarketSession::Gth,
        OperationalState::Live,
        12,
        now - TimeDelta::seconds(1),
    );
    configure_quote_batch(&mut live_empty, 1, false);
    engine
        .process(live_empty, now)
        .expect("accept same-generation empty live state");

    let outcome = engine
        .process(
            evaluation_envelope(
                "message:evaluate:ibkr-same-gen-empty",
                MarketSession::Gth,
                now,
            ),
            now,
        )
        .expect("produce a fail-closed decision");
    let decision = decision(outcome);
    assert_eq!(decision.action, StrategyAction::NoTrade);
    assert_eq!(
        decision.block_reasons,
        vec![StrategyBlockReason::ExactLegMissing]
    );
}

#[test]
fn future_decision_time_is_rejected_without_a_future_projection() {
    let temp = TempDir::new().expect("temp directory");
    let core_config = config(&temp);
    let now = at("2026-07-31T14:30:00Z");
    let mut engine = CoreEngine::open(core_config.clone(), now).expect("open core");
    engine
        .process(
            quote_envelope(
                "message:quotes:future-decision",
                Provider::Schwab,
                MarketSession::Rth,
                OperationalState::Live,
                1,
                now - TimeDelta::seconds(1),
            ),
            now,
        )
        .expect("accept causal quote batch");

    let future_decision_at = now + TimeDelta::seconds(1);
    let error = engine
        .process(
            evaluation_envelope(
                "message:evaluate:future-decision",
                MarketSession::Rth,
                future_decision_at,
            ),
            now,
        )
        .expect_err("future decision time must fail closed");
    assert!(error.to_string().contains("after processing_at"));

    let projection: serde_json::Value = serde_json::from_slice(
        &std::fs::read(&core_config.projection_path).expect("read latest projection"),
    )
    .expect("decode latest projection");
    let published_at: DateTime<Utc> = projection["published_at"]
        .as_str()
        .expect("projection published_at")
        .parse()
        .expect("valid projection timestamp");
    assert!(published_at <= now);
    assert_eq!(projection["outcome"]["outcome"], "quote_batch");

    let health = Ledger::open(&core_config.ledger_path)
        .expect("open ledger")
        .health()
        .expect("ledger health");
    assert_eq!(health.pending, 0);
}

#[test]
fn future_quote_batch_event_time_is_rejected_before_it_enters_the_book() {
    let temp = TempDir::new().expect("temp directory");
    let now = at("2026-07-31T14:30:00Z");
    let mut engine = CoreEngine::open(config(&temp), now).expect("open core");

    let error = engine
        .process(
            quote_envelope(
                "message:quotes:future-batch",
                Provider::Schwab,
                MarketSession::Rth,
                OperationalState::Live,
                1,
                now,
            ),
            now,
        )
        .expect_err("future quote batch event time must fail closed");
    assert!(
        error
            .to_string()
            .contains("envelope emitted_at is after processing_at")
    );

    let evaluation = evaluation_envelope(
        "message:evaluate:after-future-batch",
        MarketSession::Rth,
        now,
    );
    assert!(
        engine.process(evaluation, now).is_err(),
        "rejected future batch must not populate the quote book"
    );
}

#[test]
fn quote_events_after_decision_time_are_no_trade_instead_of_zero_age() {
    let temp = TempDir::new().expect("temp directory");
    let core_config = config(&temp);
    let decision_at = at("2026-07-31T14:30:00Z");
    let processing_at = decision_at + TimeDelta::milliseconds(500);
    let mut quotes = quote_envelope(
        "message:quotes:future-events",
        Provider::Schwab,
        MarketSession::Rth,
        OperationalState::Live,
        1,
        decision_at - TimeDelta::seconds(1),
    );
    let IngressMessageV1::QuoteBatch(batch) = &mut quotes.message else {
        panic!("test envelope must contain a quote batch");
    };
    let future_source = decision_at + TimeDelta::milliseconds(100);
    let future_received = decision_at + TimeDelta::milliseconds(200);
    for quote in &mut batch.quotes {
        for side in [&mut quote.bid, &mut quote.ask, &mut quote.last]
            .into_iter()
            .flatten()
        {
            side.source_at = future_source;
            side.received_at = future_received;
        }
    }
    batch.received_at = processing_at;
    quotes.emitted_at = processing_at;

    let mut engine = CoreEngine::open(core_config.clone(), decision_at).expect("open core");
    engine
        .process(quotes, processing_at)
        .expect("future-timestamp batch may be audited and retained");
    let outcome = engine
        .process(
            evaluation_envelope(
                "message:evaluate:future-events",
                MarketSession::Rth,
                decision_at,
            ),
            processing_at,
        )
        .expect("future exact-leg timestamps must produce a decision-time abstention");
    let decision = decision(outcome);
    assert_eq!(decision.action, StrategyAction::NoTrade);
    assert_eq!(decision.block_reasons, [StrategyBlockReason::ExactLegStale]);
    assert!(decision.exact_legs.is_none());

    let projection: serde_json::Value = serde_json::from_slice(
        &std::fs::read(&core_config.projection_path).expect("read latest projection"),
    )
    .expect("decode latest projection");
    let published_at: DateTime<Utc> = projection["published_at"]
        .as_str()
        .expect("projection published_at")
        .parse()
        .expect("valid projection timestamp");
    let evaluated_at: DateTime<Utc> = projection["outcome"]["decision"]["evaluated_at"]
        .as_str()
        .expect("decision evaluated_at")
        .parse()
        .expect("valid decision timestamp");
    assert!(published_at >= evaluated_at);
}

#[test]
fn provider_state_after_decision_time_is_a_typed_abstention() {
    let temp = TempDir::new().expect("temp directory");
    let decision_at = at("2026-07-31T14:30:00Z");
    let processing_at = decision_at + TimeDelta::milliseconds(500);
    let mut quotes = quote_envelope(
        "message:quotes:future-provider-state",
        Provider::Schwab,
        MarketSession::Rth,
        OperationalState::Live,
        1,
        decision_at - TimeDelta::seconds(1),
    );
    let IngressMessageV1::QuoteBatch(batch) = &mut quotes.message else {
        panic!("test envelope must contain a quote batch");
    };
    batch.provider_state.observed_at = decision_at + TimeDelta::milliseconds(100);
    batch.received_at = processing_at;
    quotes.emitted_at = processing_at;

    let mut engine = CoreEngine::open(config(&temp), decision_at).expect("open core");
    engine
        .process(quotes, processing_at)
        .expect("retain the processing-time provider observation");
    let outcome = engine
        .process(
            evaluation_envelope(
                "message:evaluate:future-provider-state",
                MarketSession::Rth,
                decision_at,
            ),
            processing_at,
        )
        .expect("future provider state must abstain instead of failing processing");
    let decision = decision(outcome);
    assert_eq!(decision.action, StrategyAction::NoTrade);
    assert_eq!(
        decision.block_reasons,
        [StrategyBlockReason::ProviderNotReady]
    );
}

#[test]
fn experimental_research_updates_only_the_durable_research_projection() {
    let temp = TempDir::new().expect("temp directory");
    let core_config = config(&temp);
    let signals: ResearchSignalsV1 = serde_json::from_str(include_str!(
        "../../../../contracts/golden/domain/v1/experimental_research_signals.json"
    ))
    .expect("research fixture");
    let processing_at = signals.generated_at + TimeDelta::seconds(1);
    let envelope = IngressEnvelopeV1 {
        schema_version: INGRESS_SCHEMA_VERSION.to_owned(),
        message_id: token("message:research:test"),
        emitted_at: signals.generated_at,
        message: IngressMessageV1::ResearchSignals(signals),
    };
    let mut engine = CoreEngine::open(core_config.clone(), processing_at).expect("open core");
    let outcome = engine
        .process(envelope.clone(), processing_at)
        .expect("research frame accepted");
    assert!(matches!(
        outcome,
        CoreOutcome::ResearchSignals {
            disposition: spx_core::ResearchDisposition::Updated,
            ..
        }
    ));
    assert!(matches!(
        engine
            .process(envelope, processing_at + TimeDelta::milliseconds(1))
            .expect("duplicate research frame"),
        CoreOutcome::Duplicate { .. }
    ));

    let projection: serde_json::Value = serde_json::from_slice(
        &std::fs::read(&core_config.research_projection_path).expect("research projection"),
    )
    .expect("valid research projection");
    assert_eq!(
        projection["schema_version"],
        "spx_latest_research_projection.v1"
    );
    assert_eq!(
        projection["signals"]["range_forecasts"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    let raw = std::fs::read_dir(&core_config.raw_log_dir)
        .expect("raw log directory")
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .path()
                .extension()
                .is_some_and(|value| value == "ndjson")
        })
        .map(|entry| std::fs::read_to_string(entry.path()).expect("raw segment"))
        .collect::<String>();
    assert!(raw.contains("\"kind\":\"research_signals\""));

    engine.shutdown().expect("release core owner");
    let health = Ledger::open(&core_config.ledger_path)
        .expect("open ledger")
        .health()
        .expect("ledger health");
    assert_eq!(health, spx_ledger::LedgerHealth::default());
}

#[test]
fn research_context_v2_updates_and_reopens_the_durable_projection() {
    let temp = TempDir::new().expect("temp directory");
    let core_config = config(&temp);
    let signals: ResearchSignalsV1 = serde_json::from_str(include_str!(
        "../../../../contracts/golden/domain/v2/research_context.json"
    ))
    .expect("research context v2 fixture");
    let processing_at = signals.generated_at + TimeDelta::seconds(1);
    let envelope = IngressEnvelopeV1 {
        schema_version: INGRESS_SCHEMA_VERSION.to_owned(),
        message_id: token("message:research:v2-test"),
        emitted_at: signals.generated_at,
        message: IngressMessageV1::ResearchSignals(signals),
    };
    let mut engine = CoreEngine::open(core_config.clone(), processing_at).expect("open core");
    assert!(matches!(
        engine
            .process(envelope, processing_at)
            .expect("research context v2 accepted"),
        CoreOutcome::ResearchSignals {
            disposition: spx_core::ResearchDisposition::Updated,
            ..
        }
    ));
    engine.shutdown().expect("release core owner");
    drop(engine);

    let projection: serde_json::Value = serde_json::from_slice(
        &std::fs::read(&core_config.research_projection_path).expect("research projection"),
    )
    .expect("valid research projection");
    assert_eq!(
        projection["schema_version"],
        "spx_latest_research_projection.v1"
    );
    assert_eq!(
        projection["signals"]["schema_version"],
        "research_context.v2"
    );
    assert_eq!(
        projection["signals"]["document_id"],
        "research-context:35b62b513a9e9327cab0e069"
    );

    CoreEngine::open(core_config, processing_at + TimeDelta::seconds(1))
        .expect("core reopens a persisted v2 research projection");
}

#[test]
fn desk_map_projection_updates_only_the_rust_report_source_projection() {
    let temp = TempDir::new().expect("temp directory");
    let core_config = config(&temp);
    let projection: DeskMapProjectionV1 = serde_json::from_str(include_str!(
        "../../../../contracts/golden/domain/v1/desk_map_projection.json"
    ))
    .expect("desk map fixture");
    let expected_projection_id = projection.projection_id.as_str().to_owned();
    let processing_at = projection.available_at + TimeDelta::seconds(1);
    let envelope = IngressEnvelopeV1 {
        schema_version: INGRESS_SCHEMA_VERSION.to_owned(),
        message_id: token("message:desk-map:test"),
        emitted_at: projection.available_at,
        message: IngressMessageV1::DeskMapProjection(Box::new(projection)),
    };
    let mut engine = CoreEngine::open(core_config.clone(), processing_at).expect("open core");
    assert!(matches!(
        engine
            .process(envelope, processing_at)
            .expect("desk map accepted"),
        CoreOutcome::DeskMapProjection {
            disposition: spx_core::DeskMapDisposition::Updated,
            ..
        }
    ));
    engine.shutdown().expect("release core owner");
    drop(engine);

    let latest: spx_core::LatestDeskMapProjectionV1 = serde_json::from_slice(
        &std::fs::read(&core_config.desk_map_projection_path).expect("desk map projection"),
    )
    .expect("valid latest desk map projection");
    latest.validate().expect("latest projection validates");
    assert_eq!(
        latest.projection.projection_id.as_str(),
        expected_projection_id
    );
    let research_context = latest
        .projection
        .research_context
        .as_ref()
        .expect("core must persist the embedded research context atomically");
    assert_eq!(research_context.schema_version, "research_context.v2");
    assert_eq!(
        Some(&research_context.document_id),
        latest.projection.research_context_document_id.as_ref()
    );
    let health = Ledger::open(&core_config.ledger_path)
        .expect("open ledger")
        .health()
        .expect("ledger health");
    assert_eq!(health, spx_ledger::LedgerHealth::default());
}

#[test]
fn strategy_distribution_ingress_updates_only_its_durable_advisory_projection() {
    let temp = TempDir::new().expect("temp directory");
    let core_config = config(&temp);
    let forecast: StrategyDistributionForecastV1 = serde_json::from_str(include_str!(
        "../../../../contracts/golden/domain/v1/strategy_distribution_forecast.json"
    ))
    .expect("strategy distribution fixture");
    let processing_at = forecast.available_at.with_timezone(&Utc) + TimeDelta::seconds(1);
    let envelope = IngressEnvelopeV1 {
        schema_version: INGRESS_SCHEMA_VERSION.to_owned(),
        message_id: token("message:strategy-distribution:test"),
        emitted_at: forecast.available_at.with_timezone(&Utc),
        message: IngressMessageV1::StrategyDistributionForecast(Box::new(forecast.clone())),
    };
    let mut engine = CoreEngine::open(core_config.clone(), processing_at).expect("open core");
    assert!(matches!(
        engine
            .process(envelope, processing_at)
            .expect("strategy distribution accepted"),
        CoreOutcome::StrategyDistributionForecast {
            disposition: spx_core::StrategyDistributionDisposition::Updated,
            ..
        }
    ));

    let repeated = IngressEnvelopeV1 {
        schema_version: INGRESS_SCHEMA_VERSION.to_owned(),
        message_id: token("message:strategy-distribution:repeat"),
        emitted_at: forecast.available_at.with_timezone(&Utc),
        message: IngressMessageV1::StrategyDistributionForecast(Box::new(forecast)),
    };
    assert!(matches!(
        engine
            .process(repeated, processing_at + TimeDelta::milliseconds(1))
            .expect("same document is unchanged"),
        CoreOutcome::StrategyDistributionForecast {
            disposition: spx_core::StrategyDistributionDisposition::Unchanged,
            ..
        }
    ));
    engine.shutdown().expect("release core owner");
    drop(engine);

    let latest: spx_core::LatestStrategyDistributionProjectionV1 = serde_json::from_slice(
        &std::fs::read(&core_config.strategy_distribution_projection_path)
            .expect("strategy distribution projection"),
    )
    .expect("valid latest strategy distribution projection");
    latest.validate().expect("latest projection validates");
    assert_eq!(
        latest.forecast.document_id.as_str(),
        "strategy-distribution:2026-08-05:143000:1"
    );
    assert_eq!(
        latest.forecast.shadow_decision.action,
        spx_domain::ShadowAction::NoTrade
    );
    assert!(!latest.forecast.automatic_ordering);

    let health = Ledger::open(&core_config.ledger_path)
        .expect("open ledger")
        .health()
        .expect("ledger health");
    assert_eq!(health, spx_ledger::LedgerHealth::default());
}

#[test]
fn operator_notification_ingress_persists_once_and_rejects_target_drift() {
    let temp = TempDir::new().expect("temp directory");
    let core_config = config(&temp);
    let now = at("2026-08-04T14:30:00Z");
    let notification = OperatorNotificationV1 {
        schema_version: OPERATOR_NOTIFICATION_SCHEMA_VERSION.to_owned(),
        event_id: token("operator-event-ready-1"),
        semantic_id: token("operator-semantic-ready-1"),
        opportunity_id: token("opportunity-7565-call"),
        generation: 0,
        role: OperatorNotificationRole::TradeReady,
        occurred_at: now,
        expires_at: now + TimeDelta::minutes(10),
        title: token("SPX Trade Ready"),
        body: operator_body(&"x".repeat(8_000)),
        targets: vec![NotificationTargetV1 {
            key: token("desk-bark"),
            channel: DeliveryChannel::Bark,
        }],
        automatic_ordering: false,
    };
    let envelope = IngressEnvelopeV1 {
        schema_version: INGRESS_SCHEMA_VERSION.to_owned(),
        message_id: token("message-operator-ready-1"),
        emitted_at: now,
        message: IngressMessageV1::OperatorNotification(Box::new(notification.clone())),
    };
    let mut engine = CoreEngine::open(core_config.clone(), now).expect("open core");
    assert!(matches!(
        engine
            .process(envelope.clone(), now + TimeDelta::seconds(1))
            .expect("operator notification accepted"),
        CoreOutcome::OperatorNotification {
            role: OperatorNotificationRole::TradeReady,
            disposition: spx_core::OperatorNotificationDisposition::Inserted,
            ..
        }
    ));
    assert!(matches!(
        engine
            .process(envelope, now + TimeDelta::seconds(2))
            .expect("exact ingress duplicate accepted"),
        CoreOutcome::OperatorNotification {
            role: OperatorNotificationRole::TradeReady,
            disposition: spx_core::OperatorNotificationDisposition::Duplicate,
            ..
        }
    ));
    assert_eq!(
        Ledger::open(&core_config.ledger_path)
            .expect("open ledger")
            .health()
            .expect("ledger health")
            .pending,
        1
    );

    let mut mismatched = notification;
    mismatched.event_id = token("operator-event-target-drift");
    mismatched.semantic_id = token("operator-semantic-target-drift");
    mismatched.targets[0].channel = DeliveryChannel::Feishu;
    let failure = engine
        .process(
            IngressEnvelopeV1 {
                schema_version: INGRESS_SCHEMA_VERSION.to_owned(),
                message_id: token("message-operator-target-drift"),
                emitted_at: now + TimeDelta::seconds(2),
                message: IngressMessageV1::OperatorNotification(Box::new(mismatched)),
            },
            now + TimeDelta::seconds(3),
        )
        .expect_err("target drift must fail closed");
    assert!(matches!(failure, CoreError::Domain(_)));
}

#[test]
fn late_setup_after_ready_or_exit_is_accepted_but_semantically_suppressed() {
    for leading_role in [
        OperatorNotificationRole::TradeReady,
        OperatorNotificationRole::Exit,
    ] {
        let temp = TempDir::new().expect("temp directory");
        let core_config = config(&temp);
        let now = at("2026-08-04T14:30:00Z");
        let mut engine = CoreEngine::open(core_config.clone(), now).expect("open core");
        let leading_name = leading_role.as_str();
        let leading = OperatorNotificationV1 {
            schema_version: OPERATOR_NOTIFICATION_SCHEMA_VERSION.to_owned(),
            event_id: token(format!("event-leading-{leading_name}")),
            semantic_id: token(format!("semantic-leading-{leading_name}")),
            opportunity_id: token("opportunity-out-of-order"),
            generation: 0,
            role: leading_role,
            occurred_at: now,
            expires_at: now + TimeDelta::minutes(10),
            title: token(format!("SPX {leading_name}")),
            body: operator_body(leading_name),
            targets: vec![NotificationTargetV1 {
                key: token("desk-bark"),
                channel: DeliveryChannel::Bark,
            }],
            automatic_ordering: false,
        };
        engine
            .process(
                IngressEnvelopeV1 {
                    schema_version: INGRESS_SCHEMA_VERSION.to_owned(),
                    message_id: token(format!("message-leading-{leading_name}")),
                    emitted_at: now,
                    message: IngressMessageV1::OperatorNotification(Box::new(leading)),
                },
                now + TimeDelta::seconds(1),
            )
            .expect("leading transition accepted");

        let late_setup = OperatorNotificationV1 {
            schema_version: OPERATOR_NOTIFICATION_SCHEMA_VERSION.to_owned(),
            event_id: token(format!("event-late-setup-{leading_name}")),
            semantic_id: token(format!("semantic-late-setup-{leading_name}")),
            opportunity_id: token("opportunity-out-of-order"),
            generation: 0,
            role: OperatorNotificationRole::Setup,
            occurred_at: now - TimeDelta::seconds(1),
            expires_at: now + TimeDelta::minutes(10),
            title: token("SPX late setup"),
            body: operator_body("late setup"),
            targets: vec![NotificationTargetV1 {
                key: token("desk-bark"),
                channel: DeliveryChannel::Bark,
            }],
            automatic_ordering: false,
        };
        let late_envelope = IngressEnvelopeV1 {
            schema_version: INGRESS_SCHEMA_VERSION.to_owned(),
            message_id: token(format!("message-late-setup-{leading_name}")),
            emitted_at: now + TimeDelta::seconds(1),
            message: IngressMessageV1::OperatorNotification(Box::new(late_setup)),
        };
        let outcome = engine
            .process(late_envelope.clone(), now + TimeDelta::seconds(2))
            .expect("late setup is accepted as semantic suppression");
        assert!(matches!(
            outcome,
            CoreOutcome::OperatorNotification {
                disposition: spx_core::OperatorNotificationDisposition::SemanticSuppressed,
                ..
            }
        ));
        assert!(matches!(
            engine
                .process(late_envelope, now + TimeDelta::seconds(3))
                .expect("lost suppression acknowledgement replays suppression"),
            CoreOutcome::OperatorNotification {
                disposition: spx_core::OperatorNotificationDisposition::SemanticSuppressed,
                ..
            }
        ));
        let health = Ledger::open(&core_config.ledger_path)
            .expect("open ledger")
            .health()
            .expect("ledger health");
        assert_eq!(health.pending, 1);
        assert_eq!(health.cancelled, 0);
    }
}

#[test]
fn cancellation_ingress_fences_a_later_operator_event_and_is_idempotent() {
    let temp = TempDir::new().expect("temp directory");
    let core_config = config(&temp);
    let now = at("2026-08-04T14:30:00Z");
    let cancellation = OperatorNotificationCancellationV1 {
        schema_version: OPERATOR_NOTIFICATION_CANCELLATION_SCHEMA_VERSION.to_owned(),
        event_id: token("operator-event-cancelled-before-insert"),
        cancelled_at: now,
        reason_code: token("source_invalidated"),
    };
    let envelope = IngressEnvelopeV1 {
        schema_version: INGRESS_SCHEMA_VERSION.to_owned(),
        message_id: token("message-operator-cancel-1"),
        emitted_at: now,
        message: IngressMessageV1::OperatorNotificationCancellation(cancellation.clone()),
    };
    let mut engine = CoreEngine::open(core_config.clone(), now).expect("open core");
    assert!(matches!(
        engine
            .process(envelope.clone(), now + TimeDelta::seconds(1))
            .expect("cancellation accepted"),
        CoreOutcome::OperatorNotificationCancellation {
            persist_disposition: spx_core::PersistDisposition::Inserted,
            ..
        }
    ));
    assert!(matches!(
        engine
            .process(envelope, now + TimeDelta::seconds(2))
            .expect("same ingress id is idempotent"),
        CoreOutcome::Duplicate { .. }
    ));
    assert!(matches!(
        engine
            .process(
                IngressEnvelopeV1 {
                    schema_version: INGRESS_SCHEMA_VERSION.to_owned(),
                    message_id: token("message-operator-cancel-2"),
                    emitted_at: now,
                    message: IngressMessageV1::OperatorNotificationCancellation(cancellation),
                },
                now + TimeDelta::seconds(3),
            )
            .expect("same cancellation under a new ingress id is idempotent"),
        CoreOutcome::OperatorNotificationCancellation {
            persist_disposition: spx_core::PersistDisposition::Duplicate,
            ..
        }
    ));

    let notification = OperatorNotificationV1 {
        schema_version: OPERATOR_NOTIFICATION_SCHEMA_VERSION.to_owned(),
        event_id: token("operator-event-cancelled-before-insert"),
        semantic_id: token("operator-semantic-cancelled-before-insert"),
        opportunity_id: token("opportunity-cancelled-before-insert"),
        generation: 0,
        role: OperatorNotificationRole::TradeReady,
        occurred_at: now,
        expires_at: now + TimeDelta::minutes(10),
        title: token("SPX Trade Ready"),
        body: operator_body("Cancelled source must never deliver"),
        targets: vec![NotificationTargetV1 {
            key: token("desk-bark"),
            channel: DeliveryChannel::Bark,
        }],
        automatic_ordering: false,
    };
    let failure = engine
        .process(
            IngressEnvelopeV1 {
                schema_version: INGRESS_SCHEMA_VERSION.to_owned(),
                message_id: token("message-operator-after-cancel"),
                emitted_at: now + TimeDelta::seconds(3),
                message: IngressMessageV1::OperatorNotification(Box::new(notification)),
            },
            now + TimeDelta::seconds(4),
        )
        .expect_err("durable cancellation fence must reject later event");
    assert!(matches!(failure, CoreError::Ledger(_)));
    assert_eq!(
        Ledger::open(&core_config.ledger_path)
            .expect("open ledger")
            .health()
            .expect("ledger health"),
        spx_ledger::LedgerHealth::default()
    );
}

#[test]
fn committed_cancellation_without_ingress_record_replays_as_fence_duplicate() {
    let temp = TempDir::new().expect("temp directory");
    let core_config = config(&temp);
    let now = at("2026-08-04T14:30:00Z");
    let ledger = Ledger::open(&core_config.ledger_path).expect("open ledger");
    let owner = ledger
        .acquire_owner(
            spx_ledger::OwnerRole::Core,
            "core-owner-crash-window",
            now,
            TimeDelta::seconds(30),
        )
        .expect("acquire core owner");
    assert_eq!(
        ledger
            .cancel_event_at(
                &owner,
                "operator-event-crash-window",
                "source_invalidated",
                now,
                now,
            )
            .expect("commit fence without ingress record"),
        spx_ledger::PersistWrite::Inserted
    );
    ledger.release_owner(&owner).expect("release core owner");

    let cancellation = OperatorNotificationCancellationV1 {
        schema_version: OPERATOR_NOTIFICATION_CANCELLATION_SCHEMA_VERSION.to_owned(),
        event_id: token("operator-event-crash-window"),
        cancelled_at: now,
        reason_code: token("source_invalidated"),
    };
    let envelope = IngressEnvelopeV1 {
        schema_version: INGRESS_SCHEMA_VERSION.to_owned(),
        message_id: token("message-cancel-crash-window"),
        emitted_at: now,
        message: IngressMessageV1::OperatorNotificationCancellation(cancellation),
    };
    let mut engine = CoreEngine::open(core_config, now + TimeDelta::seconds(1)).expect("open core");
    assert!(matches!(
        engine
            .process(envelope.clone(), now + TimeDelta::seconds(1))
            .expect("replay commits missing ingress identity"),
        CoreOutcome::OperatorNotificationCancellation {
            persist_disposition: spx_core::PersistDisposition::Duplicate,
            ..
        }
    ));
    assert!(matches!(
        engine
            .process(envelope, now + TimeDelta::seconds(2))
            .expect("recorded ingress now short-circuits safely"),
        CoreOutcome::Duplicate { .. }
    ));
}
