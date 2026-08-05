#![forbid(unsafe_code)]

mod config;
mod desk_map_projection;
mod engine;
mod projection;
mod quote_book;
mod raw_log;
mod readiness;
mod research_projection;
mod server;
mod strategy_distribution_projection;

pub use config::{CoreConfig, NotificationTargetConfig, ReadinessConfig};
pub use desk_map_projection::{
    DeskMapDisposition, LATEST_DESK_MAP_PROJECTION_SCHEMA_VERSION, LatestDeskMapProjectionV1,
};
pub use engine::{
    CoreEngine, CoreError, CoreOutcome, OperatorNotificationDisposition, PersistDisposition,
    QuoteDisposition,
};
pub use quote_book::{ApplyBatch, QuoteBook, QuoteBookError};
pub use raw_log::{RawLogPruneReport, prune_raw_log};
pub use readiness::{ReadinessAssessment, assess_readiness};
pub use research_projection::ResearchDisposition;
pub use server::serve_unix;
pub use spx_domain::{AckStatus, CoreAckDisposition, CoreAckReason, CoreAckV1};
pub use strategy_distribution_projection::{
    LATEST_STRATEGY_DISTRIBUTION_PROJECTION_SCHEMA_VERSION, LatestStrategyDistributionProjectionV1,
    StrategyDistributionDisposition,
};
