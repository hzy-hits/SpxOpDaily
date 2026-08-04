use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use anyhow::Context as _;
use clap::{Parser, Subcommand};
use signal_hook::consts::{SIGINT, SIGTERM};
use signal_hook::flag;
use spx_bridge::{BridgeConfig, BridgeRuntime, BridgeState, inspect_source};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "spx-bridge")]
#[command(about = "Fail-closed Python normalized snapshot to Rust core bridge")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    CheckConfig {
        #[arg(long, default_value = "config/bridge.toml")]
        config: PathBuf,
    },
    InitState {
        #[arg(long, default_value = "config/bridge.toml")]
        config: PathBuf,
    },
    Inspect {
        #[arg(long, default_value = "config/bridge.toml")]
        config: PathBuf,
    },
    Run {
        #[arg(long, default_value = "config/bridge.toml")]
        config: PathBuf,
    },
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    match Cli::parse().command {
        Command::CheckConfig { config } => {
            BridgeConfig::load(&config)
                .with_context(|| format!("failed to load {}", config.display()))?;
            println!("{{\"status\":\"ok\"}}");
        }
        Command::InitState { config } => {
            let config = BridgeConfig::load(&config)
                .with_context(|| format!("failed to load {}", config.display()))?;
            BridgeState::initialize(&config.state_path).with_context(|| {
                format!(
                    "failed to initialize bridge state at {}",
                    config.state_path.display()
                )
            })?;
            println!("{{\"status\":\"initialized\"}}");
        }
        Command::Inspect { config } => {
            let config = BridgeConfig::load(&config)
                .with_context(|| format!("failed to load {}", config.display()))?;
            println!(
                "{}",
                serde_json::to_string_pretty(&inspect_source(&config)?)?
            );
        }
        Command::Run { config } => {
            let config = BridgeConfig::load(&config)
                .with_context(|| format!("failed to load {}", config.display()))?;
            let runtime = BridgeRuntime::open(config)?;
            runtime.run(&termination_flag()?)?;
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
