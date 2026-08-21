#[test]
fn production_retention_archives_before_manifest_gated_prune() {
    let service = include_str!("../../../systemd/spx-rust-frame-retention.service");
    let archive = service
        .find("spx-core archive-frames")
        .expect("archive command");
    let prune = service
        .find("spx-core prune-frames")
        .expect("prune command");

    assert!(archive < prune, "archive must run before prune");
    assert!(service.contains("--backlog-days 7"));
    assert!(service.contains("--keep-completed-days 1"));
    assert!(service.contains("--max-total-bytes 17179869184"));
    assert!(service.contains("--require-archive-root"));
    assert!(service.contains("TimeoutStartSec=6h"));
    assert!(service.contains("ReadWritePaths=-/srv/data/spx-spark/rust-core-shadow/archive"));
}

#[test]
fn production_retention_runs_once_after_the_session_finalizer_window() {
    let timer = include_str!("../../../systemd/spx-rust-frame-retention.timer");

    assert!(timer.contains("OnCalendar=*-*-* 18:30:00 America/New_York"));
    assert!(timer.contains("Persistent=true"));
    assert!(!timer.contains("OnCalendar=hourly"));
}
