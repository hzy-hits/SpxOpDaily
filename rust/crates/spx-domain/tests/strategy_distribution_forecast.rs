use serde_json::{Value, json};
use spx_domain::{
    DistributionActionAuthority, EstimateStatus, ProbabilityMeasure, ShadowAction,
    StrategyDistributionForecastV1, Validate, canonical_json_hash,
};

const FIXTURE: &str =
    include_str!("../../../../contracts/golden/domain/v1/strategy_distribution_forecast.json");

fn fixture_value() -> Value {
    serde_json::from_str(FIXTURE).expect("Python to_dict fixture must be JSON")
}

fn decode(value: Value) -> StrategyDistributionForecastV1 {
    serde_json::from_value(value).expect("mutated fixture must remain syntactically typed")
}

#[test]
fn python_to_dict_fixture_is_strict_valid_and_canonical() {
    let forecast: StrategyDistributionForecastV1 =
        serde_json::from_str(FIXTURE).expect("Python to_dict fixture must decode");
    forecast
        .validate()
        .expect("Python to_dict fixture must cross the Rust domain boundary");

    let canonical = serde_json::to_string_pretty(&forecast).expect("fixture must serialize") + "\n";
    assert_eq!(canonical, FIXTURE);
    assert_eq!(
        canonical_json_hash(&forecast).expect("fixture must hash"),
        "cb86214c38c0233a4fcb260134cee609b14a83911c46b44cb1dc023a948144a1"
    );

    assert_eq!(forecast.action_authority, DistributionActionAuthority::None);
    assert!(!forecast.automatic_ordering);
    assert_eq!(forecast.shadow_decision.action, ShadowAction::NoTrade);
    assert_eq!(forecast.q_event.measure, ProbabilityMeasure::RiskNeutral);
    assert_eq!(forecast.p_event.measure, ProbabilityMeasure::Physical);
    assert_eq!(forecast.q_event.event, forecast.p_event.event);
    assert!(forecast.q_event.sample_count.is_none());
    assert_eq!(forecast.p_event.sample_count, Some(40));
    assert_eq!(forecast.p_event.session_count, Some(8));
    assert_eq!(forecast.p_event.n_raw, Some(40));
    assert_eq!(
        forecast.p_event.n_effective.map(|value| value.get()),
        Some(8.0)
    );
    assert_eq!(forecast.p_event.historical_sessions.len(), 8);

    let candidate = &forecast.strategy_candidates[0];
    assert!(candidate.execution.actual_fill_probability.is_none());
    assert_eq!(candidate.net_pnl.status, EstimateStatus::Unavailable);
    assert!(candidate.net_pnl.expected_net_pnl.is_none());
    assert!(candidate.net_pnl.p10_net_pnl.is_none());
    assert!(candidate.net_pnl.p50_net_pnl.is_none());
    assert!(candidate.net_pnl.p90_net_pnl.is_none());
}

#[test]
fn safety_authority_is_closed_at_decode_and_validation() {
    let mut unknown_authority = fixture_value();
    unknown_authority["action_authority"] = json!("manual");
    serde_json::from_value::<StrategyDistributionForecastV1>(unknown_authority)
        .expect_err("unknown action authority must fail typed decoding");

    let mut automatic = fixture_value();
    automatic["automatic_ordering"] = json!(true);
    decode(automatic)
        .validate()
        .expect_err("automatic ordering must fail closed");
}

#[test]
fn q_and_p_require_the_same_event_and_causal_physical_training() {
    let mut mismatched_event = fixture_value();
    mismatched_event["p_event"]["event"]["event_id"] = json!("event:other");
    decode(mismatched_event)
        .validate()
        .expect_err("Q and P must describe exactly the same event");

    let mut same_day_training = fixture_value();
    same_day_training["p_event"]["trained_through_date"] = json!("2026-08-05");
    decode(same_day_training)
        .validate()
        .expect_err("physical training must precede the forecast trading date");
}

#[test]
fn physical_evidence_is_bounded_and_q_cannot_claim_it() {
    let mut q_claims_samples = fixture_value();
    q_claims_samples["q_event"]["sample_count"] = json!(1);
    decode(q_claims_samples)
        .validate()
        .expect_err("risk-neutral estimates cannot claim physical sample evidence");

    let mut too_many_sessions = fixture_value();
    too_many_sessions["p_event"]["session_count"] = json!(41);
    decode(too_many_sessions)
        .validate()
        .expect_err("session_count cannot exceed sample_count");

    let mut probability_outside_interval = fixture_value();
    probability_outside_interval["p_event"]["interval_high"] = json!(0.38);
    decode(probability_outside_interval)
        .validate()
        .expect_err("physical probability must lie inside its evidence interval");
}

#[test]
fn quote_reach_never_becomes_actual_fill_and_unavailable_pnl_stays_null() {
    let mut fabricated_fill = fixture_value();
    fabricated_fill["strategy_candidates"][0]["execution"]["actual_fill_probability"] = json!(0.5);
    decode(fabricated_fill)
        .validate()
        .expect_err("displayed quote reach must not become actual fill evidence");

    let mut fabricated_pnl = fixture_value();
    fabricated_pnl["strategy_candidates"][0]["net_pnl"]["p50_net_pnl"] = json!(0.0);
    decode(fabricated_pnl)
        .validate()
        .expect_err("unavailable net-PnL must keep every estimate null");
}

#[test]
fn unavailable_q_and_p_can_formally_represent_a_null_event_no_trade() {
    let mut value = fixture_value();
    for measure in ["q_event", "p_event"] {
        value[measure]["event"] = Value::Null;
        value[measure]["status"] = json!("unavailable");
        value[measure]["quality"] = json!("unavailable");
        value[measure]["probability"] = Value::Null;
        value[measure]["method_version"] = Value::Null;
        value[measure]["reason_codes"] = json!(["direction_unavailable"]);
    }
    value["p_event"]["sample_count"] = json!(0);
    value["p_event"]["session_count"] = json!(0);
    value["p_event"]["interval_low"] = Value::Null;
    value["p_event"]["interval_high"] = Value::Null;
    value["p_event"]["trained_through_date"] = Value::Null;
    value["p_event"]["n_raw"] = json!(0);
    value["p_event"]["n_effective"] = Value::Null;
    value["p_event"]["historical_sessions"] = json!([]);
    value["strategy_candidates"] = json!([]);
    value["shadow_decision"]["reason_codes"] = json!(["direction_unavailable"]);
    value["quality"] = json!("unavailable");
    value["quality_reason_codes"] = json!(["direction_unavailable"]);

    decode(value)
        .validate()
        .expect("both unavailable estimates may omit the shared event");
}

#[test]
fn required_nullable_and_unknown_fields_fail_typed_decode() {
    let mut missing_actual_fill = fixture_value();
    missing_actual_fill["strategy_candidates"][0]["execution"]
        .as_object_mut()
        .expect("execution is an object")
        .remove("actual_fill_probability");
    serde_json::from_value::<StrategyDistributionForecastV1>(missing_actual_fill)
        .expect_err("required nullable actual_fill_probability must be explicit");

    let mut missing_sample_count = fixture_value();
    missing_sample_count["p_event"]
        .as_object_mut()
        .expect("p_event is an object")
        .remove("sample_count");
    serde_json::from_value::<StrategyDistributionForecastV1>(missing_sample_count)
        .expect_err("required nullable physical sample_count must be explicit");

    let mut unknown_nested = fixture_value();
    unknown_nested["p_event"]["confidence"] = json!("high");
    serde_json::from_value::<StrategyDistributionForecastV1>(unknown_nested)
        .expect_err("unknown nested fields must fail closed");
}

#[test]
fn additive_neighbour_metadata_can_be_absent_in_a_persisted_v1_projection() {
    let mut legacy = fixture_value();
    for measure in ["q_event", "p_event"] {
        let estimate = legacy[measure]
            .as_object_mut()
            .expect("probability estimate is an object");
        estimate.remove("n_raw");
        estimate.remove("n_effective");
        estimate.remove("historical_sessions");
    }
    let forecast: StrategyDistributionForecastV1 = serde_json::from_value(legacy)
        .expect("pre-metadata v1 projection must decode during rolling upgrade");
    forecast
        .validate()
        .expect("pre-metadata v1 projection must remain valid");
    assert!(forecast.p_event.n_raw.is_none());
    assert!(forecast.p_event.n_effective.is_none());
    assert!(forecast.p_event.historical_sessions.is_empty());
}
