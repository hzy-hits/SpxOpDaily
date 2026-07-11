CREATE TABLE sessions (
    session_date TEXT PRIMARY KEY,
    market TEXT NOT NULL,
    status TEXT NOT NULL,
    opened_at TEXT,
    closed_at TEXT,
    data_quality TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE strategy_versions (
    strategy_name TEXT NOT NULL,
    strategy_version TEXT NOT NULL,
    activated_at TEXT NOT NULL,
    git_commit TEXT,
    config_sha256 TEXT,
    metadata_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (strategy_name, strategy_version)
);

CREATE TABLE events (
    event_key TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    session_date TEXT NOT NULL,
    source_at TEXT NOT NULL,
    available_at TEXT NOT NULL,
    received_at TEXT,
    phase TEXT,
    direction TEXT,
    data_quality TEXT NOT NULL,
    schema_version INTEGER NOT NULL CHECK (schema_version > 0),
    attributes_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    CHECK (available_at >= source_at)
);

CREATE INDEX events_session_source_idx ON events(session_date, source_at);
CREATE INDEX events_type_source_idx ON events(event_type, source_at);

CREATE TABLE feature_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    event_key TEXT REFERENCES events(event_key),
    captured_at TEXT NOT NULL,
    available_at TEXT NOT NULL,
    gamma_regime TEXT,
    schema_version INTEGER NOT NULL CHECK (schema_version > 0),
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    CHECK (available_at >= captured_at)
);

CREATE INDEX feature_snapshots_event_idx ON feature_snapshots(event_key, available_at);

CREATE TABLE decisions (
    decision_id TEXT PRIMARY KEY,
    event_key TEXT REFERENCES events(event_key),
    feature_snapshot_id TEXT REFERENCES feature_snapshots(snapshot_id),
    strategy_name TEXT NOT NULL,
    strategy_version TEXT NOT NULL,
    decision_at TEXT NOT NULL,
    available_at TEXT NOT NULL,
    status TEXT NOT NULL,
    action TEXT NOT NULL,
    side TEXT NOT NULL,
    reason TEXT,
    gamma_regime TEXT,
    attributes_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    CHECK (available_at <= decision_at)
);

CREATE INDEX decisions_event_idx ON decisions(event_key, decision_at);
CREATE INDEX decisions_strategy_idx
    ON decisions(strategy_name, strategy_version, decision_at);
CREATE INDEX decisions_side_idx ON decisions(side, decision_at);

CREATE TABLE decision_legs (
    decision_id TEXT NOT NULL REFERENCES decisions(decision_id),
    leg_index INTEGER NOT NULL CHECK (leg_index >= 0),
    instrument_id TEXT NOT NULL,
    right_code TEXT,
    expiry TEXT,
    strike REAL,
    quantity REAL,
    bid REAL,
    ask REAL,
    delta REAL,
    gamma REAL,
    theta REAL,
    vega REAL,
    quote_source_at TEXT NOT NULL,
    quote_available_at TEXT NOT NULL,
    attributes_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (decision_id, leg_index),
    CHECK (quote_available_at >= quote_source_at)
);

CREATE INDEX decision_legs_instrument_idx ON decision_legs(instrument_id, quote_source_at);

CREATE TABLE alert_deliveries (
    delivery_id TEXT PRIMARY KEY,
    decision_id TEXT NOT NULL REFERENCES decisions(decision_id),
    channel TEXT NOT NULL,
    provider TEXT,
    status TEXT NOT NULL,
    attempted_at TEXT NOT NULL,
    sent_at TEXT,
    veto_reason TEXT,
    error_code TEXT,
    message_fingerprint TEXT,
    attributes_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    CHECK (sent_at IS NULL OR sent_at >= attempted_at)
);

CREATE INDEX alert_deliveries_decision_idx
    ON alert_deliveries(decision_id, attempted_at);
CREATE INDEX alert_deliveries_status_idx ON alert_deliveries(status, attempted_at);

CREATE TABLE outcomes (
    outcome_id TEXT PRIMARY KEY,
    event_key TEXT NOT NULL REFERENCES events(event_key),
    decision_id TEXT REFERENCES decisions(decision_id),
    horizon_minutes INTEGER NOT NULL CHECK (horizon_minutes > 0),
    status TEXT NOT NULL,
    target_at TEXT NOT NULL,
    sampled_at TEXT,
    hypothesis_direction TEXT,
    spx_return_bps REAL,
    spx_mfe_bps REAL,
    spx_mae_bps REAL,
    option_return_bps REAL,
    option_pnl REAL,
    attributes_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE UNIQUE INDEX outcomes_event_decision_horizon_idx
    ON outcomes(event_key, COALESCE(decision_id, ''), horizon_minutes);
CREATE INDEX outcomes_decision_idx ON outcomes(decision_id, horizon_minutes);
CREATE INDEX outcomes_target_idx ON outcomes(target_at);

CREATE TABLE compaction_manifests (
    manifest_id TEXT PRIMARY KEY,
    source_path TEXT NOT NULL,
    source_sha256 TEXT NOT NULL,
    source_size INTEGER NOT NULL CHECK (source_size >= 0),
    source_mtime_ns INTEGER NOT NULL CHECK (source_mtime_ns >= 0),
    output_path TEXT,
    output_sha256 TEXT,
    row_count INTEGER NOT NULL CHECK (row_count >= 0),
    min_received_at TEXT,
    max_received_at TEXT,
    schema_version TEXT NOT NULL,
    writer_version TEXT NOT NULL,
    completed_at TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (source_path, source_sha256),
    CHECK ((output_path IS NULL) = (output_sha256 IS NULL)),
    CHECK (
        min_received_at IS NULL OR max_received_at IS NULL OR
        max_received_at >= min_received_at
    )
);

CREATE INDEX compaction_manifests_completed_idx
    ON compaction_manifests(completed_at, status);
