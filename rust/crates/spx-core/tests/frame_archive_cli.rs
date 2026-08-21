use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use chrono::{DateTime, Days, NaiveDate, Utc};
use serde::Serialize;
use spx_core::{CoreConfig, NotificationTargetConfig, ReadinessConfig};
use spx_domain::{DeliveryChannel, IngressEnvelopeV1, Token, Validate, canonical_json_hash};
use tempfile::TempDir;

#[derive(Serialize)]
struct TestRawRecord<'a> {
    observed_at: DateTime<Utc>,
    payload_sha256: &'a str,
    payload: &'a IngressEnvelopeV1,
}

fn config(temp: &TempDir, raw_log_dir: &Path) -> CoreConfig {
    CoreConfig {
        socket_path: temp.path().join("run/core.sock"),
        ledger_path: temp.path().join("state/core.sqlite"),
        raw_log_dir: raw_log_dir.to_path_buf(),
        projection_path: temp.path().join("latest/core.json"),
        research_projection_path: temp.path().join("latest/research.json"),
        desk_map_projection_path: temp.path().join("latest/desk-map.json"),
        strategy_distribution_projection_path: temp
            .path()
            .join("latest/strategy-distribution.json"),
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
            key: Token::new("archive-test", "target").expect("valid target"),
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

fn write_config(temp: &TempDir, raw_log_dir: &Path) -> PathBuf {
    let path = temp.path().join("core.toml");
    fs::write(
        &path,
        toml::to_string(&config(temp, raw_log_dir)).expect("encode config"),
    )
    .expect("write config");
    path
}

fn write_valid_segment(raw_log_dir: &Path, date: NaiveDate) {
    let batch: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../contracts/golden/domain/v1/quote_batch_schwab_rth.json"
    ))
    .expect("valid quote batch fixture");
    let envelope: IngressEnvelopeV1 = serde_json::from_value(serde_json::json!({
        "schema_version": "spx_ingress.v1",
        "message_id": "message:frame-archive-cli-test",
        "emitted_at": "2026-07-31T14:30:00Z",
        "message": {
            "kind": "quote_batch",
            "payload": batch
        }
    }))
    .expect("valid envelope");
    envelope.validate().expect("valid envelope contract");
    let payload_sha256 = canonical_json_hash(&envelope).expect("payload hash");
    let record = TestRawRecord {
        observed_at: date.and_hms_opt(12, 0, 0).expect("time").and_utc(),
        payload_sha256: &payload_sha256,
        payload: &envelope,
    };
    let mut encoded = serde_json::to_vec(&record).expect("encode record");
    encoded.push(b'\n');
    fs::write(raw_log_dir.join(format!("{date}.0000.ndjson")), encoded).expect("write segment");
}

#[test]
fn archive_then_manifest_gated_prune_runs_end_to_end() {
    let temp = TempDir::new().expect("temporary directory");
    let raw = temp.path().join("frames");
    let archive = temp.path().join("archive");
    fs::create_dir(&raw).expect("create raw directory");
    fs::create_dir(&archive).expect("create archive directory");
    let raw = fs::canonicalize(raw).expect("canonical raw directory");
    let archive = fs::canonicalize(archive).expect("canonical archive directory");
    let date = Utc::now()
        .date_naive()
        .checked_sub_days(Days::new(10))
        .expect("old date");
    write_valid_segment(&raw, date);
    let config = write_config(&temp, &raw);

    let archived = Command::new(env!("CARGO_BIN_EXE_spx-core"))
        .args([
            "archive-frames",
            "--config",
            config.to_str().expect("config path"),
            "--utc-date",
            &date.to_string(),
            "--archive-root",
            archive.to_str().expect("archive path"),
        ])
        .env_remove("SPX_CORE_RAW_LOG_DIR")
        .output()
        .expect("run archive command");
    assert!(
        archived.status.success(),
        "archive stderr: {}",
        String::from_utf8_lossy(&archived.stderr)
    );
    let archive_report: serde_json::Value =
        serde_json::from_slice(&archived.stdout).expect("archive JSON report");
    assert_eq!(archive_report["status"], "created");

    let pruned = Command::new(env!("CARGO_BIN_EXE_spx-core"))
        .args([
            "prune-frames",
            "--config",
            config.to_str().expect("config path"),
            "--keep-completed-days",
            "1",
            "--max-total-bytes",
            "67108864",
            "--require-archive-root",
            archive.to_str().expect("archive path"),
        ])
        .env_remove("SPX_CORE_RAW_LOG_DIR")
        .output()
        .expect("run prune command");
    assert!(
        pruned.status.success(),
        "prune stderr: {}",
        String::from_utf8_lossy(&pruned.stderr)
    );
    let prune_report: serde_json::Value =
        serde_json::from_slice(&pruned.stdout).expect("prune JSON report");
    assert_eq!(prune_report["archive_barrier_status"], "satisfied");
    assert_eq!(prune_report["removed_files"], 1);
    assert!(!raw.join(format!("{date}.0000.ndjson")).exists());
}

#[test]
fn backlog_cli_is_oldest_first_and_bounded() {
    let temp = TempDir::new().expect("temporary directory");
    let raw = temp.path().join("frames");
    let archive = temp.path().join("archive");
    fs::create_dir(&raw).expect("create raw directory");
    fs::create_dir(&archive).expect("create archive directory");
    let raw = fs::canonicalize(raw).expect("canonical raw directory");
    let archive = fs::canonicalize(archive).expect("canonical archive directory");
    let today = Utc::now().date_naive();
    let dates = [
        today.checked_sub_days(Days::new(4)).expect("first date"),
        today.checked_sub_days(Days::new(3)).expect("second date"),
        today.checked_sub_days(Days::new(2)).expect("third date"),
    ];
    for date in dates {
        write_valid_segment(&raw, date);
    }
    let config = write_config(&temp, &raw);

    let output = Command::new(env!("CARGO_BIN_EXE_spx-core"))
        .args([
            "archive-frames",
            "--config",
            config.to_str().expect("config path"),
            "--backlog-days",
            "2",
            "--archive-root",
            archive.to_str().expect("archive path"),
        ])
        .env_remove("SPX_CORE_RAW_LOG_DIR")
        .output()
        .expect("run backlog command");
    assert!(
        output.status.success(),
        "backlog stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("backlog JSON report");
    assert_eq!(report["candidate_days"], 3);
    assert_eq!(report["days"].as_array().expect("daily reports").len(), 2);
    assert_eq!(report["selected_days"][0], dates[0].to_string());
    assert_eq!(report["selected_days"][1], dates[1].to_string());
    assert!(!archive.join(format!("date={}", dates[2])).exists());
}

#[test]
fn archive_cli_rejects_current_utc_date() {
    let temp = TempDir::new().expect("temporary directory");
    let raw = temp.path().join("frames");
    fs::create_dir(&raw).expect("create raw directory");
    let raw = fs::canonicalize(raw).expect("canonical raw directory");
    let config = write_config(&temp, &raw);
    let archive = temp.path().join("archive");
    let today = Utc::now().date_naive();

    let output = Command::new(env!("CARGO_BIN_EXE_spx-core"))
        .args([
            "archive-frames",
            "--config",
            config.to_str().expect("config path"),
            "--utc-date",
            &today.to_string(),
            "--archive-root",
            archive.to_str().expect("archive path"),
        ])
        .env_remove("SPX_CORE_RAW_LOG_DIR")
        .output()
        .expect("run archive command");
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("must be before current UTC date"));
    assert!(!archive.exists());
}
