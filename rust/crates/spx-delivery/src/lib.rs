#![forbid(unsafe_code)]

mod config;
mod render;
mod target;
mod transport;
mod worker;

pub use config::{BarkPresentation, ConfigError, DeliveryConfig, TargetConfig};
pub use render::{
    RenderedMessage, render_desk_message, render_desk_message_v2, render_operator_notification,
};
pub use target::{
    BarkTarget, DeliveryTarget, FeishuTarget, TargetError, TargetRegistry, WebhookTarget,
};
pub use transport::{HttpTransport, Transport, TransportRequest, TransportResult};
pub use worker::{DeliveryWorker, NetworkGate, WorkerError, WorkerSummary};
