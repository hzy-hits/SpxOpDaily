use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use anyhow::Context as _;
use chrono::Utc;
use clap::{Parser, Subcommand};
use signal_hook::consts::{SIGINT, SIGTERM};
use signal_hook::flag;
use spx_report::{
    OwnedReportLedger, ReportHealth, ReportService, ReportServiceConfig, ReportWriterClient,
};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(name = "spx-report")]
#[command(about = "Fenced SPX Spark GTH/RTH scheduled-report writer")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    CheckConfig {
        #[arg(long, default_value = "config/report.toml")]
        config: PathBuf,
    },
    Run {
        #[arg(long, default_value = "config/report.toml")]
        config: PathBuf,
        #[arg(long)]
        allow_network: bool,
    },
    Once {
        #[arg(long, default_value = "config/report.toml")]
        config: PathBuf,
        #[arg(long)]
        allow_network: bool,
    },
    Health {
        #[arg(long, default_value = "config/report.toml")]
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
            load_config(&config)?;
            println!("{{\"status\":\"ok\"}}");
        }
        Command::Run {
            config,
            allow_network,
        } => {
            let config = load_config(&config)?;
            let mut service = open_service(config, allow_network, Utc::now())?;
            let stop = termination_flag()?;
            let run_result = service.run_until(&stop);
            let shutdown_result = service.shutdown(Utc::now());
            run_result?;
            shutdown_result?;
        }
        Command::Once {
            config,
            allow_network,
        } => {
            let config = load_config(&config)?;
            let mut service = open_service(config, allow_network, Utc::now())?;
            let tick = service.run_once()?;
            service.shutdown(Utc::now())?;
            println!("{}", serde_json::to_string(&tick)?);
        }
        Command::Health { config } => {
            let config = load_config(&config)?;
            let health = ReportHealth::load(&config.health_path)?;
            println!("{}", serde_json::to_string_pretty(&health)?);
        }
    }
    Ok(())
}

fn open_service(
    config: ReportServiceConfig,
    allow_network: bool,
    now: chrono::DateTime<Utc>,
) -> anyhow::Result<
    ReportService<ReportWriterClient<spx_report::DeepSeekHttpTransport>, OwnedReportLedger>,
> {
    let writer = ReportWriterClient::new_http(config.writer.clone(), allow_network)?;
    let owner_id = format!("report:{}", Uuid::new_v4());
    let store = OwnedReportLedger::open(
        &config.ledger_path,
        &owner_id,
        now,
        config.owner_lease_seconds,
    )?;
    Ok(ReportService::open(
        config,
        allow_network,
        writer,
        store,
        now,
    )?)
}

fn load_config(path: &Path) -> anyhow::Result<ReportServiceConfig> {
    ReportServiceConfig::load(path).with_context(|| format!("failed to load {}", path.display()))
}

fn termination_flag() -> anyhow::Result<Arc<AtomicBool>> {
    let stop = Arc::new(AtomicBool::new(false));
    flag::register(SIGINT, Arc::clone(&stop))?;
    flag::register(SIGTERM, Arc::clone(&stop))?;
    Ok(stop)
}
