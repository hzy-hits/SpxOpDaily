use chrono::{DateTime, Utc};
use spx_domain::{
    AnalyticalOptionSnapshotV1, CandidateDirection, EntitlementState, EvaluationRequestV1,
    ExactLegEvidenceV1, MarketSession, NonNegativeF64, OperationalState, OptionContractV1,
    Provider, QuoteQuality, QuoteV1, StrategyBlockReason, TransportState,
};

use crate::ReadinessConfig;

#[derive(Debug, Clone, PartialEq)]
pub struct ReadinessAssessment {
    provider: Option<Provider>,
    exact_legs: Option<ExactLegEvidenceV1>,
    block_reasons: Vec<StrategyBlockReason>,
}

impl ReadinessAssessment {
    pub fn ready(&self) -> bool {
        self.exact_legs.is_some() && self.block_reasons.is_empty()
    }

    pub const fn provider(&self) -> Option<Provider> {
        self.provider
    }

    pub fn exact_legs(&self) -> Option<&ExactLegEvidenceV1> {
        self.exact_legs.as_ref()
    }

    pub fn block_reasons(&self) -> &[StrategyBlockReason] {
        &self.block_reasons
    }

    pub(crate) fn block(&mut self, reason: StrategyBlockReason) {
        self.block_reasons.push(reason);
        self.normalize();
    }

    pub(crate) fn into_decision_parts(
        self,
    ) -> (Option<ExactLegEvidenceV1>, Vec<StrategyBlockReason>) {
        (self.exact_legs, self.block_reasons)
    }

    fn normalize(&mut self) {
        self.block_reasons.sort();
        self.block_reasons.dedup();
        if !self.block_reasons.is_empty() {
            self.exact_legs = None;
        }
    }
}

pub fn assess_readiness(
    request: &EvaluationRequestV1,
    snapshot: &AnalyticalOptionSnapshotV1,
    processing_at: DateTime<Utc>,
    config: &ReadinessConfig,
) -> ReadinessAssessment {
    let mut block_reasons = Vec::new();
    let provider = select_provider(request, snapshot, processing_at, config, &mut block_reasons);
    let exact_legs = provider.and_then(|provider| {
        exact_leg_evidence(
            request,
            snapshot,
            processing_at,
            provider,
            config,
            &mut block_reasons,
        )
    });
    let mut assessment = ReadinessAssessment {
        provider,
        exact_legs,
        block_reasons,
    };
    assessment.normalize();
    assessment
}

fn select_provider(
    request: &EvaluationRequestV1,
    snapshot: &AnalyticalOptionSnapshotV1,
    processing_at: DateTime<Utc>,
    config: &ReadinessConfig,
    reasons: &mut Vec<StrategyBlockReason>,
) -> Option<Provider> {
    let required = match request.session {
        MarketSession::Gth => [Some(Provider::Ibkr), None],
        MarketSession::Rth => [
            Some(Provider::Schwab),
            config.allow_rth_ibkr_fallback.then_some(Provider::Ibkr),
        ],
    };
    for provider in required.into_iter().flatten() {
        let state = snapshot
            .provider_states
            .iter()
            .find(|state| state.provider == provider);
        if let Some(state) = state {
            let age = seconds_between(request.decision_at, state.observed_at);
            if state.observed_at > processing_at || age < 0.0 || age > config.quote_max_age_seconds
            {
                continue;
            }
            if state.operational == OperationalState::ExternalSessionOwns {
                if request.session == MarketSession::Gth {
                    reasons.push(StrategyBlockReason::ProviderExternalSessionOwns);
                    return None;
                }
                continue;
            }
            if state.operational == OperationalState::Live
                && state.transport == TransportState::Connected
                && state.entitlement == EntitlementState::Live
                && state.is_live()
            {
                return Some(provider);
            }
        }
    }
    reasons.push(StrategyBlockReason::ProviderNotReady);
    None
}

fn exact_leg_evidence(
    request: &EvaluationRequestV1,
    snapshot: &AnalyticalOptionSnapshotV1,
    processing_at: DateTime<Utc>,
    provider: Provider,
    config: &ReadinessConfig,
    reasons: &mut Vec<StrategyBlockReason>,
) -> Option<ExactLegEvidenceV1> {
    let long = find_contract(snapshot, &request.long_contract_id, provider);
    let short = find_contract(snapshot, &request.short_contract_id, provider);
    if long.is_none() || short.is_none() {
        let exists_other_provider = snapshot.quotes.iter().any(|quote| {
            quote.exact_contract_id().is_some_and(|id| {
                id == &request.long_contract_id || id == &request.short_contract_id
            }) && quote.provider != provider
        });
        reasons.push(if exists_other_provider {
            StrategyBlockReason::ExactLegWrongProvider
        } else {
            StrategyBlockReason::ExactLegMissing
        });
        return None;
    }
    let long = long.expect("checked above");
    let short = short.expect("checked above");
    if long.quality != QuoteQuality::Live || short.quality != QuoteQuality::Live {
        reasons.push(StrategyBlockReason::ExactLegNotLive);
    }
    let (Some(long_contract), Some(short_contract)) = (&long.option, &short.option) else {
        reasons.push(StrategyBlockReason::ExactLegContractMismatch);
        return None;
    };
    validate_contracts(request, long_contract, short_contract, reasons);
    let (Some(long_bid), Some(long_ask), Some(short_bid), Some(short_ask)) =
        (&long.bid, &long.ask, &short.bid, &short.ask)
    else {
        reasons.push(StrategyBlockReason::ExactLegOneSided);
        return None;
    };
    if long_ask.price.get() <= long_bid.price.get()
        || short_ask.price.get() <= short_bid.price.get()
    {
        reasons.push(StrategyBlockReason::ExactLegLockedOrCrossed);
    }
    let timestamps = [
        long_bid.source_at,
        long_ask.source_at,
        short_bid.source_at,
        short_ask.source_at,
    ];
    let received = [
        long_bid.received_at,
        long_ask.received_at,
        short_bid.received_at,
        short_ask.received_at,
    ];
    let mut ages = Vec::with_capacity(8);
    for timestamp in timestamps.into_iter().chain(received) {
        let age = seconds_between(request.decision_at, timestamp);
        if timestamp > processing_at || age < 0.0 || age > config.quote_max_age_seconds {
            reasons.push(StrategyBlockReason::ExactLegStale);
        }
        ages.push(age);
    }
    let source_skew = span_seconds(&timestamps);
    let receipt_skew = span_seconds(&received);
    let max_skew = source_skew.max(receipt_skew);
    if max_skew > config.max_side_skew_seconds {
        reasons.push(StrategyBlockReason::ExactLegSkew);
    }
    if !reasons.is_empty() {
        return None;
    }
    let debit = long_ask.price.get() - short_bid.price.get();
    if !(0.0..10.0).contains(&debit) || debit == 0.0 {
        reasons.push(StrategyBlockReason::InvalidVertical);
        return None;
    }
    Some(ExactLegEvidenceV1 {
        provider,
        long_contract_id: request.long_contract_id.clone(),
        short_contract_id: request.short_contract_id.clone(),
        right: request.direction.option_right(),
        long_strike: long_contract.strike,
        short_strike: short_contract.strike,
        long_bid: long_bid.price,
        long_ask: long_ask.price,
        short_bid: short_bid.price,
        short_ask: short_ask.price,
        max_age_seconds: NonNegativeF64::new(
            ages.into_iter().fold(0.0, f64::max),
            "max_age_seconds",
        )
        .expect("calculated age is finite and non-negative"),
        max_skew_seconds: NonNegativeF64::new(max_skew, "max_skew_seconds")
            .expect("calculated skew is finite and non-negative"),
        observed_at: request.decision_at,
    })
}

fn find_contract<'a>(
    snapshot: &'a AnalyticalOptionSnapshotV1,
    contract_id: &spx_domain::Token,
    provider: Provider,
) -> Option<&'a QuoteV1> {
    snapshot.quotes.iter().find(|quote| {
        quote.provider == provider
            && quote
                .exact_contract_id()
                .is_some_and(|candidate| candidate == contract_id)
    })
}

fn validate_contracts(
    request: &EvaluationRequestV1,
    long: &OptionContractV1,
    short: &OptionContractV1,
    reasons: &mut Vec<StrategyBlockReason>,
) {
    if long.expiry != request.session_date || short.expiry != request.session_date {
        reasons.push(StrategyBlockReason::ExactLegExpiryMismatch);
    }
    let required_right = request.direction.option_right();
    if long.right != required_right
        || short.right != required_right
        || long.underlier.as_str() != "SPX"
        || short.underlier.as_str() != "SPX"
        || long.trading_class.as_str() != "SPXW"
        || short.trading_class.as_str() != "SPXW"
    {
        reasons.push(StrategyBlockReason::ExactLegContractMismatch);
    }
    let width = match request.direction {
        CandidateDirection::CallVertical10 => short.strike.get() - long.strike.get(),
        CandidateDirection::PutVertical10 => long.strike.get() - short.strike.get(),
    };
    if (width - 10.0).abs() > f64::EPSILON {
        reasons.push(StrategyBlockReason::InvalidVertical);
    }
}

fn seconds_between(later: DateTime<Utc>, earlier: DateTime<Utc>) -> f64 {
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

fn span_seconds(values: &[DateTime<Utc>]) -> f64 {
    let minimum = values.iter().min().expect("non-empty timestamp array");
    let maximum = values.iter().max().expect("non-empty timestamp array");
    seconds_between(*maximum, *minimum)
}
