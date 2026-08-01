#![forbid(unsafe_code)]

mod config;
mod engine;
mod projection;
mod quote_book;
mod raw_log;
mod readiness;
mod server;

pub use config::{CoreConfig, NotificationTargetConfig, ReadinessConfig};
pub use engine::{CoreEngine, CoreError, CoreOutcome, PersistDisposition, QuoteDisposition};
pub use quote_book::{ApplyBatch, QuoteBook, QuoteBookError};
pub use raw_log::{RawLogPruneReport, prune_raw_log};
pub use readiness::{ReadinessAssessment, assess_readiness};
pub use server::serve_unix;
pub use spx_domain::{AckStatus, CoreAckDisposition, CoreAckReason, CoreAckV1};
