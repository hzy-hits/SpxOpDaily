#![forbid(unsafe_code)]

mod delivery;
mod ingress;
mod market;
mod operator_notification;
mod report;
mod research;
mod strategy;
mod validation;

pub use delivery::{
    DeliveryChannel, DeliveryReceiptV1, DeskMessageV1, DeskMessageV2, NotificationIntentV1,
    NotificationIntentV2, NotificationLineageV2, NotificationTargetV1, ReceiptOutcome,
};
pub use ingress::{
    AckStatus, CoreAckDisposition, CoreAckReason, CoreAckV1, IngressEnvelopeV1, IngressMessageV1,
};
pub use market::{
    AnalyticalOptionSnapshotV1, AuthenticationState, BookSideV1, EntitlementState, InstrumentKind,
    MarketSession, OperationalState, OptionContractV1, OptionRight, Provider, ProviderReasonCode,
    ProviderStateV1, QuoteBatchMode, QuoteBatchV1, QuoteQuality, QuoteV1, TransportState,
};
pub use operator_notification::{
    OperatorNotificationCancellationV1, OperatorNotificationRole, OperatorNotificationV1,
};
pub use report::{
    DESK_MAP_PROJECTION_SCHEMA_VERSION, DeskDataQuality, DeskDirection, DeskLevelPhase,
    DeskMapProjectionV1, DeskStage, DeskThesis, ReportActionAuthority,
};
pub use research::{
    ForecastDistribution, MarketRegimeSignalV1, RangeForecastKind, RangeForecastV1,
    RegimeInferenceSemantics, RegimeProbabilityV1, ResearchSignalsV1, ResearchUseScope,
};
pub use strategy::{
    CalendarState, CandidateDirection, EvaluationRequestV1, ExactLegEvidenceV1, MacroPermission,
    PlanState, StrategyAction, StrategyBlockReason, StrategyDecisionV1,
};
pub use validation::{
    DomainError, NonNegativeF64, PositiveF64, ProbabilityF64, Token, Validate, canonical_json_hash,
};

pub const INGRESS_SCHEMA_VERSION: &str = "spx_ingress.v1";
pub const CORE_ACK_SCHEMA_VERSION: &str = "spx_core_ack.v1";
pub const QUOTE_BATCH_SCHEMA_VERSION: &str = "quote_batch.v1";
pub const ANALYTICAL_SNAPSHOT_SCHEMA_VERSION: &str = "analytical_option_snapshot.v1";
pub const PROVIDER_STATE_SCHEMA_VERSION: &str = "provider_state.v1";
pub const EVALUATION_SCHEMA_VERSION: &str = "strategy_evaluation_request.v1";
pub const DECISION_SCHEMA_VERSION: &str = "strategy_decision.v1";
pub const NOTIFICATION_INTENT_SCHEMA_VERSION: &str = "notification_intent.v1";
pub const NOTIFICATION_INTENT_V2_SCHEMA_VERSION: &str = "notification_intent.v2";
pub const OPERATOR_NOTIFICATION_SCHEMA_VERSION: &str = "operator_notification.v1";
pub const OPERATOR_NOTIFICATION_CANCELLATION_SCHEMA_VERSION: &str =
    "operator_notification_cancellation.v1";
pub const DELIVERY_RECEIPT_SCHEMA_VERSION: &str = "delivery_receipt.v1";
pub const RESEARCH_SIGNALS_SCHEMA_VERSION: &str = "experimental_research_signals.v1";
