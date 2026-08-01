use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use anyhow::Context;
use chrono::Utc;
use clap::{Parser, Subcommand};
use signal_hook::consts::{SIGINT, SIGTERM};
use signal_hook::flag;
use spx_core::{CoreConfig, CoreEngine, serve_unix};
use spx_domain::IngressEnvelopeV1;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "spx-core")]
#[command(about = "Strict SPX Spark production core")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    CheckConfig {
        #[arg(long, default_value = "config/core.toml")]
        config: PathBuf,
    },
    Serve {
        #[arg(long, default_value = "config/core.toml")]
        config: PathBuf,
    },
    Process {
        #[arg(long, default_value = "config/core.toml")]
        config: PathBuf,
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        use_emitted_at: bool,
    },
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let cli = Cli::parse();
    match cli.command {
        Command::CheckConfig { config } => {
            CoreConfig::load(&config)
                .with_context(|| format!("failed to load {}", config.display()))?;
            println!("{{\"status\":\"ok\"}}");
        }
        Command::Serve { config } => {
            let config = CoreConfig::load(&config)
                .with_context(|| format!("failed to load {}", config.display()))?;
            let socket = config.socket_path.clone();
            let max_frame_bytes = config.max_frame_bytes;
            let max_connections = config.max_connections;
            let engine = CoreEngine::open(config, Utc::now())?;
            let stop = termination_flag()?;
            serve_unix(engine, socket, max_frame_bytes, max_connections, &stop)?;
        }
        Command::Process {
            config,
            input,
            use_emitted_at,
        } => {
            let config = CoreConfig::load(&config)
                .with_context(|| format!("failed to load {}", config.display()))?;
            let payload = std::fs::read(&input)
                .with_context(|| format!("failed to read {}", input.display()))?;
            let envelope: IngressEnvelopeV1 = serde_json::from_slice(&payload)
                .with_context(|| format!("invalid envelope in {}", input.display()))?;
            let processing_at = if use_emitted_at {
                envelope.emitted_at
            } else {
                Utc::now()
            };
            let mut engine = CoreEngine::open(config, processing_at)?;
            let outcome = engine.process(envelope, processing_at)?;
            engine.shutdown()?;
            println!("{}", serde_json::to_string_pretty(&outcome)?);
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
