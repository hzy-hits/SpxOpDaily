use std::fs::{self, File};
use std::io::Read as _;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use chrono::{Days, Utc};
use rustix::fs::{FlockOperation, Mode, OFlags, flock, open};
use spx_core::{CoreConfig, NotificationTargetConfig, ReadinessConfig};
use spx_domain::{DeliveryChannel, Token};
use tempfile::TempDir;

const DIRECTORY_LOCK_FILE: &str = ".spx-raw-log.lock";

fn config(temp: &TempDir, raw_log_dir: &std::path::Path) -> CoreConfig {
    CoreConfig {
        socket_path: temp.path().join("run/core.sock"),
        ledger_path: temp.path().join("state/core.sqlite"),
        raw_log_dir: raw_log_dir.to_path_buf(),
        projection_path: temp.path().join("latest/core.json"),
        max_frame_bytes: 1_048_576,
        max_connections: 8,
        raw_segment_max_bytes: 64 * 1024 * 1024,
        raw_log_min_free_bytes: 64 * 1024 * 1024,
        quote_cache_retention_seconds: 300,
        quote_cache_max_entries: 4096,
        batch_identity_cache_max_entries: 4096,
        owner_lease_seconds: 30,
        delivery_max_attempts: 3,
        notification_targets: vec![NotificationTargetConfig {
            key: Token::new("prune-test", "target").expect("valid target"),
            channel: DeliveryChannel::Webhook,
        }],
        decision_max_ttl_seconds: 60,
        evaluation_max_delay_seconds: 2.0,
        readiness: ReadinessConfig {
            quote_max_age_seconds: 5.0,
            max_side_skew_seconds: 2.0,
            allow_rth_ibkr_fallback: false,
        },
    }
}

fn write_config(temp: &TempDir, raw_log_dir: &std::path::Path) -> std::path::PathBuf {
    let path = temp.path().join("core.toml");
    let encoded = toml::to_string(&config(temp, raw_log_dir)).expect("encode config");
    fs::write(&path, encoded).expect("write config");
    path
}

#[test]
fn prune_cli_obeys_the_cross_process_directory_lock() {
    let temp = TempDir::new().expect("temporary directory");
    let raw = temp.path().join("frames");
    fs::create_dir(&raw).expect("create raw directory");
    let raw = fs::canonicalize(raw).expect("canonical raw directory");
    let old_date = Utc::now()
        .date_naive()
        .checked_sub_days(Days::new(10))
        .expect("old date");
    let old_segment = raw.join(format!("{old_date}.0000.ndjson"));
    fs::write(&old_segment, b"expired").expect("write old segment");
    let config_path = write_config(&temp, &raw);

    let descriptor = open(
        raw.join(DIRECTORY_LOCK_FILE),
        OFlags::CREATE | OFlags::RDWR | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::from_raw_mode(0o600),
    )
    .expect("open directory lock");
    let shared_lock = File::from(descriptor);
    flock(&shared_lock, FlockOperation::LockShared).expect("hold append-side shared lock");

    let mut child = Command::new(env!("CARGO_BIN_EXE_spx-core"))
        .args([
            "prune-frames",
            "--config",
            config_path.to_str().expect("config path"),
            "--keep-completed-days",
            "1",
            "--max-total-bytes",
            "67108864",
        ])
        .env_remove("SPX_CORE_RAW_LOG_DIR")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn prune process");
    thread::sleep(Duration::from_millis(150));
    assert!(child.try_wait().expect("poll locked child").is_none());

    drop(shared_lock);
    let deadline = Instant::now() + Duration::from_secs(3);
    let status = loop {
        if let Some(status) = child.try_wait().expect("poll unlocked child") {
            break status;
        }
        if Instant::now() >= deadline {
            child.kill().expect("kill stuck prune process");
            let _ = child.wait();
            panic!("prune process did not acquire the released lock");
        }
        thread::sleep(Duration::from_millis(20));
    };
    let mut stderr = String::new();
    child
        .stderr
        .take()
        .expect("child stderr")
        .read_to_string(&mut stderr)
        .expect("read child stderr");
    assert!(status.success(), "prune failed: {stderr}");
    assert!(!old_segment.exists());
}

#[test]
fn prune_cli_rejects_raw_log_environment_redirection() {
    let temp = TempDir::new().expect("temporary directory");
    let configured = temp.path().join("configured-frames");
    let redirected = temp.path().join("redirected-frames");
    fs::create_dir(&configured).expect("create configured directory");
    fs::create_dir(&redirected).expect("create redirected directory");
    let configured = fs::canonicalize(configured).expect("canonical configured directory");
    let redirected = fs::canonicalize(redirected).expect("canonical redirected directory");
    let config_path = write_config(&temp, &configured);

    let output = Command::new(env!("CARGO_BIN_EXE_spx-core"))
        .args([
            "prune-frames",
            "--config",
            config_path.to_str().expect("config path"),
            "--keep-completed-days",
            "7",
            "--max-total-bytes",
            "67108864",
            "--dry-run",
        ])
        .env("SPX_CORE_RAW_LOG_DIR", &redirected)
        .output()
        .expect("run prune process");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("SPX_CORE_RAW_LOG_DIR"), "stderr: {stderr}");
    assert!(!configured.join(DIRECTORY_LOCK_FILE).exists());
    assert!(!redirected.join(DIRECTORY_LOCK_FILE).exists());
}
