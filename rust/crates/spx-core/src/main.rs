use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use anyhow::Context;
use chrono::{Days, NaiveDate, Utc};
use clap::{Parser, Subcommand};
use signal_hook::consts::{SIGINT, SIGTERM};
use signal_hook::flag;
use spx_core::{
    CoreConfig, CoreEngine, RawLogArchiveBarrierStatus, archive_completed_utc_backlog,
    archive_completed_utc_day, prune_raw_log, prune_raw_log_with_archive, serve_unix,
};
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
    ArchiveFrames {
        #[arg(long, default_value = "config/core.toml")]
        config: PathBuf,
        #[arg(long, conflicts_with = "backlog_days")]
        utc_date: Option<NaiveDate>,
        #[arg(long, conflicts_with = "utc_date")]
        backlog_days: Option<u32>,
        #[arg(long)]
        archive_root: PathBuf,
    },
    PruneFrames {
        #[arg(long, default_value = "config/core.toml")]
        config: PathBuf,
        #[arg(long)]
        keep_completed_days: u32,
        #[arg(long)]
        max_total_bytes: u64,
        #[arg(long)]
        dry_run: bool,
        #[arg(long)]
        require_archive_root: Option<PathBuf>,
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
        Command::ArchiveFrames {
            config,
            utc_date,
            backlog_days,
            archive_root,
        } => run_archive_frames(&config, utc_date, backlog_days, &archive_root)?,
        Command::PruneFrames {
            config,
            keep_completed_days,
            max_total_bytes,
            dry_run,
            require_archive_root,
        } => run_prune_frames(
            &config,
            keep_completed_days,
            max_total_bytes,
            dry_run,
            require_archive_root,
        )?,
    }
    Ok(())
}

fn run_archive_frames(
    config_path: &Path,
    utc_date: Option<NaiveDate>,
    backlog_days: Option<u32>,
    archive_root: &Path,
) -> anyhow::Result<()> {
    let config = CoreConfig::load_for_prune(config_path)
        .with_context(|| format!("failed to load {}", config_path.display()))?;
    let now = Utc::now();
    if let Some(backlog_days) = backlog_days {
        let report =
            archive_completed_utc_backlog(&config.raw_log_dir, archive_root, backlog_days, now)?;
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        let utc_date = match utc_date {
            Some(date) => date,
            None => now
                .date_naive()
                .checked_sub_days(Days::new(1))
                .context("previous UTC date is not representable")?,
        };
        let report = archive_completed_utc_day(&config.raw_log_dir, archive_root, utc_date, now)?;
        println!("{}", serde_json::to_string_pretty(&report)?);
    }
    Ok(())
}

fn run_prune_frames(
    config_path: &Path,
    keep_completed_days: u32,
    max_total_bytes: u64,
    dry_run: bool,
    require_archive_root: Option<PathBuf>,
) -> anyhow::Result<()> {
    let config = CoreConfig::load_for_prune(config_path)
        .with_context(|| format!("failed to load {}", config_path.display()))?;
    anyhow::ensure!(
        max_total_bytes >= config.raw_segment_max_bytes,
        "max_total_bytes must be at least raw_segment_max_bytes"
    );
    anyhow::ensure!(
        dry_run || require_archive_root.is_some(),
        "--require-archive-root is mandatory for an actual prune"
    );
    let current_utc_date = Utc::now().date_naive();
    let report = match require_archive_root {
        Some(archive_root) => prune_raw_log_with_archive(
            &config.raw_log_dir,
            current_utc_date,
            keep_completed_days,
            max_total_bytes,
            dry_run,
            archive_root,
        )?,
        None => prune_raw_log(
            &config.raw_log_dir,
            current_utc_date,
            keep_completed_days,
            max_total_bytes,
            dry_run,
        )?,
    };
    println!("{}", serde_json::to_string_pretty(&report)?);
    anyhow::ensure!(
        report.archive_barrier_status != RawLogArchiveBarrierStatus::Withheld,
        "raw log prune withheld because one or more candidates lack a verified matching archive"
    );
    anyhow::ensure!(
        report.limit_satisfied_after_plan,
        "raw log size cap cannot be met without deleting current or future UTC segments"
    );
    Ok(())
}

fn termination_flag() -> anyhow::Result<Arc<AtomicBool>> {
    let stop = Arc::new(AtomicBool::new(false));
    flag::register(SIGINT, Arc::clone(&stop))?;
    flag::register(SIGTERM, Arc::clone(&stop))?;
    Ok(stop)
}
