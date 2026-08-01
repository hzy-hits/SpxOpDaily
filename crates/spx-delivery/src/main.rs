use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use anyhow::Context;
use chrono::Utc;
use clap::{Parser, Subcommand};
use signal_hook::consts::{SIGINT, SIGTERM};
use signal_hook::flag;
use spx_delivery::{DeliveryConfig, DeliveryWorker};
use spx_ledger::{Ledger, LedgerReader, OperatorWrite};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(name = "spx-delivery")]
#[command(about = "Fenced SPX Spark notification delivery worker")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    CheckConfig {
        #[arg(long, default_value = "config/delivery.toml")]
        config: PathBuf,
    },
    Run {
        #[arg(long, default_value = "config/delivery.toml")]
        config: PathBuf,
        #[arg(long)]
        allow_network: bool,
    },
    Once {
        #[arg(long, default_value = "config/delivery.toml")]
        config: PathBuf,
        #[arg(long)]
        allow_network: bool,
    },
    Health {
        #[arg(long, default_value = "config/delivery.toml")]
        config: PathBuf,
    },
    Acknowledge {
        #[arg(long, default_value = "config/delivery.toml")]
        config: PathBuf,
        #[arg(long)]
        target_id: String,
        #[arg(long)]
        actor: String,
        #[arg(long)]
        reason: String,
    },
    Replay {
        #[arg(long, default_value = "config/delivery.toml")]
        config: PathBuf,
        #[arg(long)]
        target_id: String,
        #[arg(long)]
        actor: String,
        #[arg(long)]
        reason: String,
    },
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    match Cli::parse().command {
        Command::CheckConfig { config } => {
            load_config(&config)?;
            println!("{{\"status\":\"ok\"}}");
        }
        Command::Run {
            config,
            allow_network,
        } => {
            let config = load_config(&config)?;
            let owner_id = format!("delivery:{}", Uuid::new_v4());
            let mut worker =
                DeliveryWorker::open_http(config, allow_network, &owner_id, Utc::now())?;
            let stop = termination_flag()?;
            worker.run_until(&stop)?;
            worker.shutdown()?;
        }
        Command::Once {
            config,
            allow_network,
        } => {
            let config = load_config(&config)?;
            let owner_id = format!("delivery:{}", Uuid::new_v4());
            let mut worker =
                DeliveryWorker::open_http(config, allow_network, &owner_id, Utc::now())?;
            let summary = worker.run_once()?;
            worker.shutdown()?;
            println!("{}", serde_json::to_string(&summary)?);
        }
        Command::Health { config } => {
            let config = load_config(&config)?;
            let ledger = LedgerReader::open_existing(config.ledger_path)?;
            ledger.quick_check()?;
            let health = ledger.health()?;
            println!("{}", serde_json::to_string(&health)?);
            if health.unacknowledged_failures > 0 {
                anyhow::bail!("delivery ledger has unacknowledged failures");
            }
        }
        Command::Acknowledge {
            config,
            target_id,
            actor,
            reason,
        } => {
            let config = load_config(&config)?;
            let ledger = Ledger::open(config.ledger_path)?;
            let result = ledger.acknowledge_failure(&target_id, &actor, &reason, Utc::now())?;
            let disposition = match result {
                OperatorWrite::Applied => "applied",
                OperatorWrite::AlreadyAcknowledged => "already_acknowledged",
            };
            println!(
                "{}",
                serde_json::json!({"target_id": target_id, "disposition": disposition})
            );
        }
        Command::Replay {
            config,
            target_id,
            actor,
            reason,
        } => {
            let config = load_config(&config)?;
            let ledger = Ledger::open(config.ledger_path)?;
            ledger.replay_failure(&target_id, &actor, &reason, Utc::now())?;
            println!(
                "{}",
                serde_json::json!({"target_id": target_id, "disposition": "requeued"})
            );
        }
    }
    Ok(())
}

fn termination_flag() -> anyhow::Result<Arc<AtomicBool>> {
    let stop = Arc::new(AtomicBool::new(false));
    flag::register(SIGINT, Arc::clone(&stop))?;
    flag::register(SIGTERM, Arc::clone(&stop))?;
    Ok(stop)
}

fn load_config(path: &Path) -> anyhow::Result<DeliveryConfig> {
    DeliveryConfig::load(path).with_context(|| format!("failed to load {}", path.display()))
}
