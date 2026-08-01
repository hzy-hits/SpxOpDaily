use std::fs;
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde::de::DeserializeOwned;
use spx_domain::{
    CandidateDirection, DeliveryChannel, DeliveryReceiptV1, DomainError, EntitlementState,
    IngressEnvelopeV1, MarketSession, NotificationIntentV1, OperationalState, OptionRight,
    Provider, ProviderReasonCode, ProviderStateV1, QuoteBatchV1, QuoteQuality, ReceiptOutcome,
    StrategyAction, StrategyBlockReason, StrategyDecisionV1, Validate, canonical_json_hash,
};

fn fixture_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/domain/v1")
        .join(name)
}

fn fixture_text(name: &str) -> String {
    fs::read_to_string(fixture_path(name)).expect("golden fixture must be readable")
}

fn decode_valid_canonical<T>(name: &str, expected_hash: &str) -> T
where
    T: DeserializeOwned + Serialize + Validate,
{
    let raw = fixture_text(name);
    let value: T = serde_json::from_str(&raw).expect("golden fixture must decode");
    value.validate().expect("golden fixture must validate");

    let canonical_pretty =
        serde_json::to_string_pretty(&value).expect("golden fixture must serialize") + "\n";
    assert_eq!(raw, canonical_pretty, "{name} must use canonical JSON");

    let actual_hash = canonical_json_hash(&value).expect("canonical hash must serialize");
    assert_eq!(actual_hash, expected_hash, "{name} canonical hash drifted");

    value
}

#[test]
fn schwab_rth_quote_batch_is_valid_and_canonical() {
    let batch: QuoteBatchV1 = decode_valid_canonical(
        "quote_batch_schwab_rth.json",
        "b3e5b9485501115113f3ee5dc4f64f4d962d52972c740c3766fd7d5d894a2490",
    );

    assert_eq!(batch.provider, Provider::Schwab);
    assert!(batch.provider_state.is_live());
    assert!(
        batch
            .quotes
            .iter()
            .all(|quote| quote.market_session == MarketSession::Rth
                && quote.quality == QuoteQuality::Live)
    );
}

#[test]
fn ibkr_gth_quote_batch_is_valid_and_canonical() {
    let batch: QuoteBatchV1 = decode_valid_canonical(
        "quote_batch_ibkr_gth.json",
        "9b534738542692a1a03e859d3e25d00667d9fc414d43bb409258b7b487542825",
    );

    assert_eq!(batch.provider, Provider::Ibkr);
    assert!(batch.provider_state.is_live());
    assert!(
        batch
            .quotes
            .iter()
            .all(|quote| quote.market_session == MarketSession::Gth
                && quote.quality == QuoteQuality::Live)
    );
    assert!(batch.quotes.iter().all(|quote| {
        quote
            .option
            .as_ref()
            .is_some_and(|option| option.trading_class.as_str() == "SPXW")
    }));
}

#[test]
fn ibkr_10197_external_session_conflict_is_valid_and_fail_closed() {
    let state: ProviderStateV1 = decode_valid_canonical(
        "provider_state_ibkr_10197.json",
        "99a0d9426d8fa996f08b07ccc3d689bee6df0b4428782feea81fa604d492b1ee",
    );

    assert_eq!(state.provider, Provider::Ibkr);
    assert_eq!(state.operational, OperationalState::ExternalSessionOwns);
    assert_eq!(state.entitlement, EntitlementState::Missing);
    assert_eq!(
        state.reason_codes,
        [ProviderReasonCode::CompetingSession10197]
    );
    assert!(!state.is_live());
}

#[test]
fn no_trade_decision_is_valid_and_canonical() {
    let decision: StrategyDecisionV1 = decode_valid_canonical(
        "strategy_decision_no_trade.json",
        "7da6c14c8701a5792a2600a64a228c41e2eff102e84b960a16d3b6f80f5bde7f",
    );

    assert_eq!(decision.action, StrategyAction::NoTrade);
    assert_eq!(
        decision.block_reasons,
        [StrategyBlockReason::ProviderExternalSessionOwns]
    );
    assert!(decision.direction.is_none());
    assert!(decision.exact_legs.is_none());
}

#[test]
fn manual_call_vertical_is_valid_canonical_and_exactly_ten_points() {
    let decision: StrategyDecisionV1 = decode_valid_canonical(
        "strategy_decision_manual_call_vertical.json",
        "44ada97ec37900ee7897a2dde92bc3721f4dc208f910f17c1f6502aee9b8b295",
    );

    assert_eq!(decision.action, StrategyAction::ManualCandidate);
    assert_eq!(decision.direction, Some(CandidateDirection::CallVertical10));
    assert_eq!(
        decision.exact_legs.as_ref().map(|legs| legs.right),
        Some(OptionRight::Call)
    );
    decision
        .validate()
        .expect("manual call vertical must pass the domain boundary");
}

#[test]
fn notification_intent_is_valid_and_canonical() {
    let intent: NotificationIntentV1 = decode_valid_canonical(
        "notification_intent.json",
        "e2d799f02b0a5f6aa43bf97eed0dbaada264b5d78133882de87ef5d253d3b98a",
    );

    assert_eq!(intent.targets.len(), 2);
    assert!(
        intent
            .message
            .execution
            .as_str()
            .contains("Manual review only")
    );
    assert!(intent.message.data_quality.as_str().contains("Schwab RTH"));
}

#[test]
fn delivery_receipt_is_valid_and_canonical() {
    let receipt: DeliveryReceiptV1 = decode_valid_canonical(
        "delivery_receipt_delivered.json",
        "53acd45b67c83148ab8083b53242475bd8c45fce02b66b47a09b22c525da46a9",
    );

    assert_eq!(receipt.channel, DeliveryChannel::Bark);
    assert_eq!(receipt.outcome, ReceiptOutcome::Delivered);
    assert!(receipt.provider_message_id.is_some());
    assert!(receipt.error_code.is_none());
}

#[test]
fn unknown_field_is_rejected_during_decode() {
    let error = serde_json::from_str::<QuoteBatchV1>(&fixture_text(
        "invalid/quote_batch_unknown_field.json",
    ))
    .expect_err("unknown fields must fail closed");

    assert!(error.to_string().contains("unknown field"));
}

#[test]
fn unknown_ingress_message_wrapper_field_is_rejected() {
    let batch: serde_json::Value =
        serde_json::from_str(&fixture_text("quote_batch_schwab_rth.json")).unwrap();
    let envelope = serde_json::json!({
        "schema_version": "spx_ingress.v1",
        "message_id": "message:unknown-wrapper-field",
        "emitted_at": "2026-07-31T14:30:00Z",
        "message": {
            "kind": "quote_batch",
            "payload": batch,
            "unexpected": true
        }
    });
    serde_json::from_value::<IngressEnvelopeV1>(envelope)
        .expect_err("unknown message wrapper fields must fail closed");
}

#[test]
fn invalid_enum_is_rejected_during_decode() {
    let error = serde_json::from_str::<ProviderStateV1>(&fixture_text(
        "invalid/provider_state_invalid_enum.json",
    ))
    .expect_err("unknown enum variants must fail closed");

    assert!(error.to_string().contains("unknown variant `realtime`"));
}

#[test]
fn provider_mismatch_is_rejected_during_validation() {
    let batch: QuoteBatchV1 =
        serde_json::from_str(&fixture_text("invalid/quote_batch_provider_mismatch.json"))
            .expect("provider mismatch fixture must decode before validation");

    assert_eq!(batch.validate(), Err(DomainError::ProviderMismatch));
}

#[test]
fn wrong_width_manual_vertical_is_rejected_by_domain_boundary() {
    let decision: StrategyDecisionV1 =
        serde_json::from_str(&fixture_text("invalid/strategy_decision_wrong_width.json"))
            .expect("wrong-width fixture must decode before validation");

    assert_eq!(
        decision.validate(),
        Err(DomainError::Invalid {
            field: "exact leg evidence",
            reason: "vertical width must be exactly 10 points",
        })
    );
}
