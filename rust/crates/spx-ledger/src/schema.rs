pub const MIGRATION_BOOTSTRAP: &str = r"
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY CHECK (version > 0),
    name TEXT NOT NULL CHECK (length(trim(name)) > 0),
    checksum_sha256 TEXT NOT NULL CHECK (
        length(checksum_sha256) = 64
        AND checksum_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    applied_at_us INTEGER NOT NULL CHECK (applied_at_us > 0)
) STRICT;
";

pub const MIGRATION_1: &str = r"
CREATE TABLE IF NOT EXISTS runtime_owners (
    role TEXT PRIMARY KEY CHECK (role IN ('core', 'delivery')),
    owner_id TEXT NOT NULL CHECK (length(trim(owner_id)) BETWEEN 16 AND 128),
    generation INTEGER NOT NULL CHECK (generation > 0),
    active INTEGER NOT NULL CHECK (active IN (0, 1)),
    acquired_at_us INTEGER NOT NULL CHECK (acquired_at_us > 0),
    heartbeat_at_us INTEGER NOT NULL CHECK (heartbeat_at_us > 0),
    lease_until_us INTEGER NOT NULL CHECK (lease_until_us > 0),
    CHECK (acquired_at_us <= heartbeat_at_us),
    CHECK (heartbeat_at_us < lease_until_us)
) STRICT;

CREATE TABLE IF NOT EXISTS ingress_messages (
    message_id TEXT PRIMARY KEY CHECK (length(trim(message_id)) > 0),
    payload_sha256 TEXT NOT NULL CHECK (
        length(payload_sha256) = 64
        AND payload_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    observed_at_us INTEGER NOT NULL CHECK (observed_at_us > 0),
    writer_generation INTEGER NOT NULL CHECK (writer_generation > 0)
) STRICT;

CREATE TABLE IF NOT EXISTS decisions (
    decision_id TEXT PRIMARY KEY CHECK (length(trim(decision_id)) > 0),
    request_id TEXT NOT NULL UNIQUE CHECK (length(trim(request_id)) > 0),
    action TEXT NOT NULL CHECK (action IN ('no_trade', 'manual_candidate')),
    policy_version TEXT NOT NULL CHECK (length(trim(policy_version)) > 0),
    snapshot_id TEXT NOT NULL CHECK (length(trim(snapshot_id)) > 0),
    evaluated_at_us INTEGER NOT NULL CHECK (evaluated_at_us > 0),
    valid_until_us INTEGER NOT NULL CHECK (valid_until_us > evaluated_at_us),
    payload_json TEXT NOT NULL CHECK (
        json_valid(payload_json) AND json_type(payload_json) = 'object'
    ),
    payload_sha256 TEXT NOT NULL CHECK (
        length(payload_sha256) = 64
        AND payload_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    writer_generation INTEGER NOT NULL CHECK (writer_generation > 0),
    created_at_us INTEGER NOT NULL CHECK (created_at_us > 0)
) STRICT;

CREATE TABLE IF NOT EXISTS notification_cancellations (
    event_id TEXT PRIMARY KEY CHECK (length(trim(event_id)) > 0),
    reason_code TEXT NOT NULL CHECK (length(trim(reason_code)) > 0),
    cancelled_at_us INTEGER NOT NULL CHECK (cancelled_at_us > 0),
    writer_generation INTEGER NOT NULL CHECK (writer_generation > 0)
) STRICT;

CREATE TABLE IF NOT EXISTS notification_events (
    event_id TEXT PRIMARY KEY CHECK (length(trim(event_id)) > 0),
    semantic_id TEXT NOT NULL UNIQUE CHECK (length(trim(semantic_id)) > 0),
    decision_id TEXT NOT NULL REFERENCES decisions(decision_id) ON DELETE RESTRICT,
    lane TEXT NOT NULL CHECK (lane IN (
        'position_safety', 'execution_safety', 'trade_ready',
        'market_warning', 'ops_transition', 'scheduled_report'
    )),
    occurred_at_us INTEGER NOT NULL CHECK (occurred_at_us > 0),
    expires_at_us INTEGER NOT NULL CHECK (expires_at_us > occurred_at_us),
    payload_json TEXT NOT NULL CHECK (
        json_valid(payload_json) AND json_type(payload_json) = 'object'
    ),
    payload_sha256 TEXT NOT NULL CHECK (
        length(payload_sha256) = 64
        AND payload_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    target_set_sha256 TEXT NOT NULL CHECK (
        length(target_set_sha256) = 64
        AND target_set_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    writer_generation INTEGER NOT NULL CHECK (writer_generation > 0),
    created_at_us INTEGER NOT NULL CHECK (created_at_us > 0)
) STRICT;

CREATE TRIGGER IF NOT EXISTS notification_events_cancellation_fence
BEFORE INSERT ON notification_events
WHEN EXISTS (
    SELECT 1 FROM notification_cancellations WHERE event_id = NEW.event_id
)
BEGIN
    SELECT RAISE(ABORT, 'cancellation_fenced');
END;

CREATE TRIGGER IF NOT EXISTS decisions_immutable_update
BEFORE UPDATE ON decisions BEGIN SELECT RAISE(ABORT, 'decision_immutable'); END;
CREATE TRIGGER IF NOT EXISTS decisions_immutable_delete
BEFORE DELETE ON decisions BEGIN SELECT RAISE(ABORT, 'decision_immutable'); END;
CREATE TRIGGER IF NOT EXISTS events_immutable_update
BEFORE UPDATE ON notification_events BEGIN SELECT RAISE(ABORT, 'event_immutable'); END;
CREATE TRIGGER IF NOT EXISTS events_immutable_delete
BEFORE DELETE ON notification_events BEGIN SELECT RAISE(ABORT, 'event_immutable'); END;
CREATE TRIGGER IF NOT EXISTS ingress_messages_immutable_update
BEFORE UPDATE ON ingress_messages BEGIN SELECT RAISE(ABORT, 'ingress_immutable'); END;
CREATE TRIGGER IF NOT EXISTS ingress_messages_immutable_delete
BEFORE DELETE ON ingress_messages BEGIN SELECT RAISE(ABORT, 'ingress_immutable'); END;
CREATE TRIGGER IF NOT EXISTS cancellations_immutable_update
BEFORE UPDATE ON notification_cancellations BEGIN SELECT RAISE(ABORT, 'cancellation_immutable'); END;
CREATE TRIGGER IF NOT EXISTS cancellations_immutable_delete
BEFORE DELETE ON notification_cancellations BEGIN SELECT RAISE(ABORT, 'cancellation_immutable'); END;

CREATE TABLE IF NOT EXISTS notification_targets (
    target_id TEXT PRIMARY KEY CHECK (length(trim(target_id)) > 0),
    event_id TEXT NOT NULL REFERENCES notification_events(event_id) ON DELETE RESTRICT,
    target_key TEXT NOT NULL CHECK (length(trim(target_key)) > 0),
    channel TEXT NOT NULL CHECK (channel IN ('bark', 'feishu', 'webhook')),
    status TEXT NOT NULL CHECK (status IN (
        'pending', 'claimed', 'in_flight', 'delivered', 'dead_letter',
        'cancelled', 'expired', 'uncertain'
    )),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    max_attempts INTEGER NOT NULL CHECK (max_attempts > 0),
    replay_generation INTEGER NOT NULL DEFAULT 0 CHECK (replay_generation >= 0),
    lease_sequence INTEGER NOT NULL DEFAULT 0 CHECK (lease_sequence >= 0),
    next_attempt_at_us INTEGER,
    claim_owner_id TEXT,
    claim_owner_generation INTEGER,
    claim_token TEXT,
    claimed_at_us INTEGER,
    lease_until_us INTEGER,
    current_attempt_id TEXT,
    delivered_at_us INTEGER,
    terminal_at_us INTEGER,
    last_error_code TEXT,
    operator_ack_at_us INTEGER,
    updated_at_us INTEGER NOT NULL CHECK (updated_at_us > 0),
    UNIQUE (event_id, target_key),
    FOREIGN KEY (target_id, current_attempt_id)
        REFERENCES delivery_attempts(target_id, attempt_id)
        DEFERRABLE INITIALLY DEFERRED,
    CHECK (attempt_count <= max_attempts),
    CHECK (status != 'pending' OR attempt_count < max_attempts),
    CHECK (
        (status = 'pending'
            AND next_attempt_at_us IS NOT NULL
            AND claim_owner_id IS NULL AND claim_owner_generation IS NULL
            AND claim_token IS NULL AND current_attempt_id IS NULL
            AND claimed_at_us IS NULL AND lease_until_us IS NULL
            AND delivered_at_us IS NULL AND terminal_at_us IS NULL)
        OR
        (status = 'claimed'
            AND next_attempt_at_us IS NULL
            AND claim_owner_id IS NOT NULL AND claim_owner_generation > 0
            AND claim_token IS NOT NULL AND current_attempt_id IS NULL
            AND claimed_at_us IS NOT NULL AND lease_until_us > claimed_at_us
            AND delivered_at_us IS NULL AND terminal_at_us IS NULL)
        OR
        (status = 'in_flight'
            AND attempt_count > 0 AND next_attempt_at_us IS NULL
            AND claim_owner_id IS NOT NULL AND claim_owner_generation > 0
            AND claim_token IS NOT NULL AND current_attempt_id IS NOT NULL
            AND claimed_at_us IS NOT NULL AND lease_until_us > claimed_at_us
            AND delivered_at_us IS NULL AND terminal_at_us IS NULL)
        OR
        (status = 'delivered'
            AND next_attempt_at_us IS NULL
            AND claim_owner_id IS NULL AND claim_owner_generation IS NULL
            AND claim_token IS NULL AND current_attempt_id IS NULL
            AND claimed_at_us IS NULL AND lease_until_us IS NULL
            AND delivered_at_us IS NOT NULL
            AND terminal_at_us = delivered_at_us)
        OR
        (status IN ('dead_letter', 'cancelled', 'expired', 'uncertain')
            AND next_attempt_at_us IS NULL
            AND claim_owner_id IS NULL AND claim_owner_generation IS NULL
            AND claim_token IS NULL AND current_attempt_id IS NULL
            AND claimed_at_us IS NULL AND lease_until_us IS NULL
            AND delivered_at_us IS NULL AND terminal_at_us IS NOT NULL)
    ),
    CHECK (
        operator_ack_at_us IS NULL
        OR status IN ('dead_letter', 'expired', 'uncertain')
    )
) STRICT;

CREATE INDEX IF NOT EXISTS targets_due_idx
ON notification_targets(status, next_attempt_at_us)
WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS targets_lease_idx
ON notification_targets(status, lease_until_us)
WHERE status IN ('claimed', 'in_flight');

CREATE INDEX IF NOT EXISTS targets_unacked_failure_idx
ON notification_targets(status, terminal_at_us)
WHERE status IN ('dead_letter', 'expired', 'uncertain')
  AND operator_ack_at_us IS NULL;

CREATE TRIGGER IF NOT EXISTS target_identity_immutable
BEFORE UPDATE OF event_id, target_key, channel, max_attempts ON notification_targets
BEGIN SELECT RAISE(ABORT, 'target_identity_immutable'); END;

CREATE TABLE IF NOT EXISTS delivery_attempts (
    attempt_id TEXT PRIMARY KEY CHECK (length(trim(attempt_id)) > 0),
    target_id TEXT NOT NULL REFERENCES notification_targets(target_id) ON DELETE RESTRICT,
    channel TEXT NOT NULL CHECK (channel IN ('bark', 'feishu', 'webhook')),
    claim_token TEXT NOT NULL CHECK (length(trim(claim_token)) > 0),
    owner_generation INTEGER NOT NULL CHECK (owner_generation > 0),
    replay_generation INTEGER NOT NULL CHECK (replay_generation >= 0),
    attempt_no INTEGER NOT NULL CHECK (attempt_no > 0),
    lease_sequence INTEGER NOT NULL CHECK (lease_sequence > 0),
    idempotency_key TEXT NOT NULL CHECK (length(trim(idempotency_key)) > 0),
    started_at_us INTEGER NOT NULL CHECK (started_at_us > 0),
    UNIQUE (target_id, attempt_id),
    UNIQUE (target_id, replay_generation, attempt_no),
    UNIQUE (target_id, lease_sequence)
) STRICT;

CREATE TRIGGER IF NOT EXISTS attempt_matches_current_target
BEFORE INSERT ON delivery_attempts
WHEN NOT EXISTS (
    SELECT 1 FROM notification_targets t
    WHERE t.target_id = NEW.target_id
      AND t.status = 'in_flight'
      AND t.current_attempt_id = NEW.attempt_id
      AND t.channel = NEW.channel
      AND t.claim_token = NEW.claim_token
      AND t.claim_owner_generation = NEW.owner_generation
      AND t.replay_generation = NEW.replay_generation
      AND t.attempt_count = NEW.attempt_no
      AND t.lease_sequence = NEW.lease_sequence
      AND NEW.idempotency_key = t.event_id || ':' || t.target_id
      AND NEW.started_at_us >= t.claimed_at_us
      AND NEW.started_at_us < t.lease_until_us
)
BEGIN SELECT RAISE(ABORT, 'attempt_target_mismatch'); END;

CREATE TABLE IF NOT EXISTS delivery_receipts (
    receipt_id TEXT PRIMARY KEY CHECK (length(trim(receipt_id)) > 0),
    target_id TEXT NOT NULL REFERENCES notification_targets(target_id) ON DELETE RESTRICT,
    intent_id TEXT NOT NULL CHECK (length(trim(intent_id)) > 0),
    target_key TEXT NOT NULL CHECK (length(trim(target_key)) > 0),
    channel TEXT NOT NULL CHECK (channel IN ('bark', 'feishu', 'webhook')),
    attempt_id TEXT UNIQUE,
    outcome TEXT NOT NULL CHECK (outcome IN (
        'delivered', 'retryable_failure', 'retry_exhausted', 'permanent_failure',
        'cancelled_before_transport', 'expired_before_transport',
        'transport_uncertain', 'claim_recovered'
    )),
    attempted INTEGER NOT NULL CHECK (attempted IN (0, 1)),
    ok INTEGER NOT NULL CHECK (ok IN (0, 1)),
    queued_for_retry INTEGER NOT NULL CHECK (queued_for_retry IN (0, 1)),
    reason_code TEXT NOT NULL CHECK (length(trim(reason_code)) > 0),
    provider_message_id TEXT,
    occurred_at_us INTEGER NOT NULL CHECK (occurred_at_us > 0),
    payload_json TEXT NOT NULL CHECK (
        json_valid(payload_json) AND json_type(payload_json) = 'object'
    ),
    FOREIGN KEY (target_id, attempt_id)
        REFERENCES delivery_attempts(target_id, attempt_id) ON DELETE RESTRICT,
    CHECK (
        (attempted = 1 AND attempt_id IS NOT NULL)
        OR (attempted = 0 AND attempt_id IS NULL)
    ),
    CHECK (
        (outcome = 'delivered' AND ok = 1)
        OR (outcome != 'delivered' AND ok = 0)
    ),
    CHECK (
        (outcome IN ('retryable_failure', 'claim_recovered') AND queued_for_retry = 1)
        OR (outcome NOT IN ('retryable_failure', 'claim_recovered') AND queued_for_retry = 0)
    ),
    CHECK (provider_message_id IS NULL OR outcome = 'delivered'),
    CHECK (
        json_extract(payload_json, '$.schema_version') IS 'delivery_receipt.v1'
        AND json_extract(payload_json, '$.receipt_id') IS receipt_id
        AND json_extract(payload_json, '$.target_id') IS target_id
        AND json_extract(payload_json, '$.intent_id') IS intent_id
        AND json_extract(payload_json, '$.target_key') IS target_key
        AND json_extract(payload_json, '$.channel') IS channel
        AND json_extract(payload_json, '$.provider_message_id') IS provider_message_id
        AND (
            (outcome = 'delivered' AND json_extract(payload_json, '$.error_code') IS NULL)
            OR
            (outcome != 'delivered' AND json_extract(payload_json, '$.error_code') IS reason_code)
        )
        AND json_extract(payload_json, '$.outcome') IS CASE
            WHEN outcome = 'delivered' THEN 'delivered'
            WHEN outcome IN ('retryable_failure', 'claim_recovered') THEN 'retry_scheduled'
            WHEN outcome IN ('retry_exhausted', 'permanent_failure') THEN 'dead_letter'
            WHEN outcome = 'cancelled_before_transport' THEN 'cancelled'
            WHEN outcome = 'expired_before_transport' THEN 'expired'
            WHEN outcome = 'transport_uncertain' THEN 'uncertain'
        END
    ),
    CHECK (
        (attempted = 1 AND outcome IN (
            'delivered', 'retryable_failure', 'retry_exhausted',
            'permanent_failure', 'transport_uncertain'
        ))
        OR (attempted = 0 AND outcome IN (
            'permanent_failure', 'cancelled_before_transport',
            'expired_before_transport', 'claim_recovered'
        ))
    )
) STRICT;

CREATE TRIGGER IF NOT EXISTS receipt_matches_target_and_attempt
BEFORE INSERT ON delivery_receipts
WHEN NOT EXISTS (
    SELECT 1 FROM notification_targets t
    WHERE t.target_id = NEW.target_id
      AND t.event_id = NEW.intent_id
      AND t.target_key = NEW.target_key
      AND t.channel = NEW.channel
)
OR (
    NEW.attempt_id IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 FROM delivery_attempts a
        WHERE a.attempt_id = NEW.attempt_id
          AND a.target_id = NEW.target_id
          AND a.channel = NEW.channel
    )
)
BEGIN SELECT RAISE(ABORT, 'receipt_provenance_mismatch'); END;

CREATE TABLE IF NOT EXISTS operator_actions (
    action_id TEXT PRIMARY KEY CHECK (length(trim(action_id)) > 0),
    target_id TEXT NOT NULL REFERENCES notification_targets(target_id) ON DELETE RESTRICT,
    action TEXT NOT NULL CHECK (action IN ('acknowledge', 'replay', 'cancel')),
    reason_code TEXT NOT NULL CHECK (length(trim(reason_code)) > 0),
    actor TEXT NOT NULL CHECK (length(trim(actor)) > 0),
    occurred_at_us INTEGER NOT NULL CHECK (occurred_at_us > 0)
) STRICT;

CREATE TRIGGER IF NOT EXISTS attempts_immutable_update
BEFORE UPDATE ON delivery_attempts BEGIN SELECT RAISE(ABORT, 'attempt_immutable'); END;
CREATE TRIGGER IF NOT EXISTS attempts_immutable_delete
BEFORE DELETE ON delivery_attempts BEGIN SELECT RAISE(ABORT, 'attempt_immutable'); END;
CREATE TRIGGER IF NOT EXISTS receipts_immutable_update
BEFORE UPDATE ON delivery_receipts BEGIN SELECT RAISE(ABORT, 'receipt_immutable'); END;
CREATE TRIGGER IF NOT EXISTS receipts_immutable_delete
BEFORE DELETE ON delivery_receipts BEGIN SELECT RAISE(ABORT, 'receipt_immutable'); END;
CREATE TRIGGER IF NOT EXISTS operator_actions_immutable_update
BEFORE UPDATE ON operator_actions BEGIN SELECT RAISE(ABORT, 'operator_action_immutable'); END;
CREATE TRIGGER IF NOT EXISTS operator_actions_immutable_delete
BEFORE DELETE ON operator_actions BEGIN SELECT RAISE(ABORT, 'operator_action_immutable'); END;
";

pub const MIGRATION_2: &str = r"
CREATE TABLE runtime_owners_v2 (
    role TEXT PRIMARY KEY CHECK (role IN ('core', 'report', 'delivery')),
    owner_id TEXT NOT NULL CHECK (length(trim(owner_id)) BETWEEN 16 AND 128),
    generation INTEGER NOT NULL CHECK (generation > 0),
    active INTEGER NOT NULL CHECK (active IN (0, 1)),
    acquired_at_us INTEGER NOT NULL CHECK (acquired_at_us > 0),
    heartbeat_at_us INTEGER NOT NULL CHECK (heartbeat_at_us > 0),
    lease_until_us INTEGER NOT NULL CHECK (lease_until_us > 0),
    CHECK (acquired_at_us <= heartbeat_at_us),
    CHECK (heartbeat_at_us < lease_until_us)
) STRICT;

INSERT INTO runtime_owners_v2 (
    role, owner_id, generation, active, acquired_at_us, heartbeat_at_us, lease_until_us
)
SELECT role, owner_id, generation, active, acquired_at_us, heartbeat_at_us, lease_until_us
FROM runtime_owners;

DROP TABLE runtime_owners;
ALTER TABLE runtime_owners_v2 RENAME TO runtime_owners;

DROP TRIGGER notification_events_cancellation_fence;
DROP TRIGGER events_immutable_update;
DROP TRIGGER events_immutable_delete;

CREATE TABLE notification_events_v2 (
    event_id TEXT PRIMARY KEY CHECK (length(trim(event_id)) > 0),
    semantic_id TEXT NOT NULL UNIQUE CHECK (length(trim(semantic_id)) > 0),
    decision_id TEXT REFERENCES decisions(decision_id) ON DELETE RESTRICT,
    source_projection_id TEXT CHECK (
        source_projection_id IS NULL OR length(trim(source_projection_id)) > 0
    ),
    report_slot TEXT CHECK (report_slot IS NULL OR length(trim(report_slot)) > 0),
    lane TEXT NOT NULL CHECK (lane IN (
        'position_safety', 'execution_safety', 'trade_ready',
        'market_warning', 'ops_transition', 'scheduled_report'
    )),
    occurred_at_us INTEGER NOT NULL CHECK (occurred_at_us > 0),
    expires_at_us INTEGER NOT NULL CHECK (expires_at_us > occurred_at_us),
    payload_json TEXT NOT NULL CHECK (
        json_valid(payload_json) AND json_type(payload_json) = 'object'
    ),
    payload_sha256 TEXT NOT NULL CHECK (
        length(payload_sha256) = 64
        AND payload_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    target_set_sha256 TEXT NOT NULL CHECK (
        length(target_set_sha256) = 64
        AND target_set_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    writer_generation INTEGER NOT NULL CHECK (writer_generation > 0),
    created_at_us INTEGER NOT NULL CHECK (created_at_us > 0),
    CHECK (
        (lane = 'scheduled_report'
            AND decision_id IS NULL
            AND source_projection_id IS NOT NULL
            AND report_slot IS NOT NULL)
        OR
        (lane != 'scheduled_report'
            AND decision_id IS NOT NULL
            AND source_projection_id IS NULL
            AND report_slot IS NULL)
    ),
    CHECK (
        lane != 'scheduled_report'
        OR (
            json_extract(payload_json, '$.schema_version') IS 'notification_intent.v2'
            AND json_extract(payload_json, '$.intent_id') IS event_id
            AND json_extract(payload_json, '$.semantic_id') IS semantic_id
            AND json_extract(payload_json, '$.lineage.lane') IS 'scheduled_report'
            AND json_extract(payload_json, '$.lineage.source_projection_id')
                IS source_projection_id
            AND json_extract(payload_json, '$.lineage.slot') IS report_slot
            AND json_type(payload_json, '$.message.title') = 'text'
            AND length(trim(json_extract(payload_json, '$.message.title'))) > 0
            AND json_type(payload_json, '$.message.desk_view') = 'text'
            AND length(trim(json_extract(payload_json, '$.message.desk_view'))) > 0
            AND json_type(payload_json, '$.message.location') = 'text'
            AND length(trim(json_extract(payload_json, '$.message.location'))) > 0
            AND json_type(payload_json, '$.message.structure') = 'text'
            AND length(trim(json_extract(payload_json, '$.message.structure'))) > 0
            AND json_type(payload_json, '$.message.primary_path') = 'text'
            AND length(trim(json_extract(payload_json, '$.message.primary_path'))) > 0
            AND json_type(payload_json, '$.message.alternative_path') = 'text'
            AND length(trim(json_extract(payload_json, '$.message.alternative_path'))) > 0
            AND json_type(payload_json, '$.message.targets') = 'text'
            AND length(trim(json_extract(payload_json, '$.message.targets'))) > 0
            AND json_type(payload_json, '$.message.execution') = 'text'
            AND length(trim(json_extract(payload_json, '$.message.execution'))) > 0
            AND json_type(payload_json, '$.message.data_quality') = 'text'
            AND length(trim(json_extract(payload_json, '$.message.data_quality'))) > 0
        )
    )
) STRICT;

INSERT INTO notification_events_v2 (
    event_id, semantic_id, decision_id, source_projection_id, report_slot, lane,
    occurred_at_us, expires_at_us, payload_json, payload_sha256, target_set_sha256,
    writer_generation, created_at_us
)
SELECT
    event_id, semantic_id, decision_id, NULL, NULL, lane,
    occurred_at_us, expires_at_us, payload_json, payload_sha256, target_set_sha256,
    writer_generation, created_at_us
FROM notification_events;

DROP TABLE notification_events;
ALTER TABLE notification_events_v2 RENAME TO notification_events;

CREATE UNIQUE INDEX scheduled_report_slot_uidx
ON notification_events(report_slot)
WHERE lane = 'scheduled_report';

CREATE TRIGGER notification_events_cancellation_fence
BEFORE INSERT ON notification_events
WHEN EXISTS (
    SELECT 1 FROM notification_cancellations WHERE event_id = NEW.event_id
)
BEGIN
    SELECT RAISE(ABORT, 'cancellation_fenced');
END;

CREATE TRIGGER events_immutable_update
BEFORE UPDATE ON notification_events BEGIN SELECT RAISE(ABORT, 'event_immutable'); END;
CREATE TRIGGER events_immutable_delete
BEFORE DELETE ON notification_events BEGIN SELECT RAISE(ABORT, 'event_immutable'); END;
";

pub const MIGRATION_3: &str = r"
DROP TRIGGER notification_events_cancellation_fence;
DROP TRIGGER events_immutable_update;
DROP TRIGGER events_immutable_delete;

CREATE TABLE notification_events_v3 (
    event_id TEXT PRIMARY KEY CHECK (length(trim(event_id)) > 0),
    semantic_id TEXT NOT NULL UNIQUE CHECK (length(trim(semantic_id)) > 0),
    decision_id TEXT REFERENCES decisions(decision_id) ON DELETE RESTRICT,
    source_projection_id TEXT CHECK (
        source_projection_id IS NULL OR length(trim(source_projection_id)) > 0
    ),
    report_slot TEXT CHECK (report_slot IS NULL OR length(trim(report_slot)) > 0),
    lane TEXT NOT NULL CHECK (lane IN (
        'position_safety', 'execution_safety', 'trade_ready',
        'market_warning', 'ops_transition', 'scheduled_report', 'trader_event'
    )),
    occurred_at_us INTEGER NOT NULL CHECK (occurred_at_us > 0),
    expires_at_us INTEGER NOT NULL CHECK (expires_at_us > occurred_at_us),
    payload_json TEXT NOT NULL CHECK (
        json_valid(payload_json) AND json_type(payload_json) = 'object'
    ),
    payload_sha256 TEXT NOT NULL CHECK (
        length(payload_sha256) = 64
        AND payload_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    target_set_sha256 TEXT NOT NULL CHECK (
        length(target_set_sha256) = 64
        AND target_set_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    writer_generation INTEGER NOT NULL CHECK (writer_generation > 0),
    created_at_us INTEGER NOT NULL CHECK (created_at_us > 0),
    CHECK (
        (lane = 'scheduled_report'
            AND decision_id IS NULL
            AND source_projection_id IS NOT NULL
            AND report_slot IS NOT NULL)
        OR
        (lane = 'trader_event'
            AND decision_id IS NULL
            AND source_projection_id IS NULL
            AND report_slot IS NULL)
        OR
        (lane NOT IN ('scheduled_report', 'trader_event')
            AND decision_id IS NOT NULL
            AND source_projection_id IS NULL
            AND report_slot IS NULL)
    ),
    CHECK (
        lane != 'scheduled_report'
        OR (
            json_extract(payload_json, '$.schema_version') IS 'notification_intent.v2'
            AND json_extract(payload_json, '$.intent_id') IS event_id
            AND json_extract(payload_json, '$.semantic_id') IS semantic_id
            AND json_extract(payload_json, '$.lineage.lane') IS 'scheduled_report'
            AND json_extract(payload_json, '$.lineage.source_projection_id')
                IS source_projection_id
            AND json_extract(payload_json, '$.lineage.slot') IS report_slot
            AND json_type(payload_json, '$.message.title') = 'text'
            AND length(trim(json_extract(payload_json, '$.message.title'))) > 0
            AND json_type(payload_json, '$.message.desk_view') = 'text'
            AND length(trim(json_extract(payload_json, '$.message.desk_view'))) > 0
            AND json_type(payload_json, '$.message.location') = 'text'
            AND length(trim(json_extract(payload_json, '$.message.location'))) > 0
            AND json_type(payload_json, '$.message.structure') = 'text'
            AND length(trim(json_extract(payload_json, '$.message.structure'))) > 0
            AND json_type(payload_json, '$.message.primary_path') = 'text'
            AND length(trim(json_extract(payload_json, '$.message.primary_path'))) > 0
            AND json_type(payload_json, '$.message.alternative_path') = 'text'
            AND length(trim(json_extract(payload_json, '$.message.alternative_path'))) > 0
            AND json_type(payload_json, '$.message.targets') = 'text'
            AND length(trim(json_extract(payload_json, '$.message.targets'))) > 0
            AND json_type(payload_json, '$.message.execution') = 'text'
            AND length(trim(json_extract(payload_json, '$.message.execution'))) > 0
            AND json_type(payload_json, '$.message.data_quality') = 'text'
            AND length(trim(json_extract(payload_json, '$.message.data_quality'))) > 0
        )
    ),
    CHECK (
        lane != 'trader_event'
        OR (
            json_extract(payload_json, '$.schema_version') IS 'operator_notification.v1'
            AND json_extract(payload_json, '$.event_id') IS event_id
            AND json_extract(payload_json, '$.semantic_id') IS semantic_id
            AND json_type(payload_json, '$.opportunity_id') = 'text'
            AND length(trim(json_extract(payload_json, '$.opportunity_id'))) > 0
            AND json_type(payload_json, '$.generation') = 'integer'
            AND json_extract(payload_json, '$.generation') >= 0
            AND json_extract(payload_json, '$.role') IN ('setup', 'trade_ready', 'exit')
            AND json_type(payload_json, '$.occurred_at') = 'text'
            AND json_type(payload_json, '$.expires_at') = 'text'
            AND json_type(payload_json, '$.title') = 'text'
            AND length(trim(json_extract(payload_json, '$.title'))) > 0
            AND json_type(payload_json, '$.body') = 'text'
            AND length(trim(json_extract(payload_json, '$.body'))) > 0
            AND json_type(payload_json, '$.targets') = 'array'
            AND json_array_length(json_extract(payload_json, '$.targets')) > 0
            AND json_extract(payload_json, '$.automatic_ordering') IS 0
        )
    )
) STRICT;

INSERT INTO notification_events_v3 (
    event_id, semantic_id, decision_id, source_projection_id, report_slot, lane,
    occurred_at_us, expires_at_us, payload_json, payload_sha256, target_set_sha256,
    writer_generation, created_at_us
)
SELECT
    event_id, semantic_id, decision_id, source_projection_id, report_slot, lane,
    occurred_at_us, expires_at_us, payload_json, payload_sha256, target_set_sha256,
    writer_generation, created_at_us
FROM notification_events;

DROP TABLE notification_events;
ALTER TABLE notification_events_v3 RENAME TO notification_events;

CREATE UNIQUE INDEX scheduled_report_slot_uidx
ON notification_events(report_slot)
WHERE lane = 'scheduled_report';

CREATE TRIGGER notification_events_cancellation_fence
BEFORE INSERT ON notification_events
WHEN EXISTS (
    SELECT 1 FROM notification_cancellations WHERE event_id = NEW.event_id
)
BEGIN
    SELECT RAISE(ABORT, 'cancellation_fenced');
END;

CREATE TRIGGER events_immutable_update
BEFORE UPDATE ON notification_events BEGIN SELECT RAISE(ABORT, 'event_immutable'); END;
CREATE TRIGGER events_immutable_delete
BEFORE DELETE ON notification_events BEGIN SELECT RAISE(ABORT, 'event_immutable'); END;
";

pub const MIGRATION_4: &str = r"
DROP TRIGGER notification_events_cancellation_fence;
DROP TRIGGER events_immutable_update;
DROP TRIGGER events_immutable_delete;

CREATE TABLE notification_events_v4 (
    event_id TEXT PRIMARY KEY CHECK (length(trim(event_id)) > 0),
    semantic_id TEXT NOT NULL UNIQUE CHECK (length(trim(semantic_id)) > 0),
    decision_id TEXT REFERENCES decisions(decision_id) ON DELETE RESTRICT,
    source_projection_id TEXT CHECK (
        source_projection_id IS NULL OR length(trim(source_projection_id)) > 0
    ),
    report_slot TEXT CHECK (report_slot IS NULL OR length(trim(report_slot)) > 0),
    lane TEXT NOT NULL CHECK (lane IN (
        'position_safety', 'execution_safety', 'trade_ready',
        'market_warning', 'ops_transition', 'scheduled_report', 'trader_event'
    )),
    occurred_at_us INTEGER NOT NULL CHECK (occurred_at_us > 0),
    expires_at_us INTEGER NOT NULL CHECK (expires_at_us > occurred_at_us),
    payload_json TEXT NOT NULL CHECK (
        json_valid(payload_json) AND json_type(payload_json) = 'object'
    ),
    payload_sha256 TEXT NOT NULL CHECK (
        length(payload_sha256) = 64
        AND payload_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    target_set_sha256 TEXT NOT NULL CHECK (
        length(target_set_sha256) = 64
        AND target_set_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    writer_generation INTEGER NOT NULL CHECK (writer_generation > 0),
    created_at_us INTEGER NOT NULL CHECK (created_at_us > 0),
    CHECK (
        (lane = 'scheduled_report'
            AND decision_id IS NULL
            AND source_projection_id IS NOT NULL
            AND report_slot IS NOT NULL)
        OR
        (lane = 'trader_event'
            AND decision_id IS NULL
            AND source_projection_id IS NULL
            AND report_slot IS NULL)
        OR
        (lane NOT IN ('scheduled_report', 'trader_event')
            AND decision_id IS NOT NULL
            AND source_projection_id IS NULL
            AND report_slot IS NULL)
    ),
    CHECK (
        lane != 'scheduled_report'
        OR (
            json_extract(payload_json, '$.schema_version') IS 'notification_intent.v2'
            AND json_extract(payload_json, '$.intent_id') IS event_id
            AND json_extract(payload_json, '$.semantic_id') IS semantic_id
            AND json_extract(payload_json, '$.lineage.lane') IS 'scheduled_report'
            AND json_extract(payload_json, '$.lineage.source_projection_id')
                IS source_projection_id
            AND json_extract(payload_json, '$.lineage.slot') IS report_slot
            AND json_type(payload_json, '$.message.title') = 'text'
            AND length(trim(json_extract(payload_json, '$.message.title'))) > 0
            AND json_type(payload_json, '$.message.desk_view') = 'text'
            AND length(trim(json_extract(payload_json, '$.message.desk_view'))) > 0
            AND json_type(payload_json, '$.message.location') = 'text'
            AND length(trim(json_extract(payload_json, '$.message.location'))) > 0
            AND json_type(payload_json, '$.message.structure') = 'text'
            AND length(trim(json_extract(payload_json, '$.message.structure'))) > 0
            AND json_type(payload_json, '$.message.primary_path') = 'text'
            AND length(trim(json_extract(payload_json, '$.message.primary_path'))) > 0
            AND json_type(payload_json, '$.message.alternative_path') = 'text'
            AND length(trim(json_extract(payload_json, '$.message.alternative_path'))) > 0
            AND json_type(payload_json, '$.message.targets') = 'text'
            AND length(trim(json_extract(payload_json, '$.message.targets'))) > 0
            AND json_type(payload_json, '$.message.execution') = 'text'
            AND length(trim(json_extract(payload_json, '$.message.execution'))) > 0
            AND json_type(payload_json, '$.message.data_quality') = 'text'
            AND length(trim(json_extract(payload_json, '$.message.data_quality'))) > 0
        )
    ),
    CHECK (
        lane != 'trader_event'
        OR (
            json_extract(payload_json, '$.schema_version') IS 'operator_notification.v1'
            AND json_extract(payload_json, '$.event_id') IS event_id
            AND json_extract(payload_json, '$.semantic_id') IS semantic_id
            AND json_type(payload_json, '$.opportunity_id') = 'text'
            AND length(trim(json_extract(payload_json, '$.opportunity_id'))) > 0
            AND json_type(payload_json, '$.generation') = 'integer'
            AND json_extract(payload_json, '$.generation') >= 0
            AND json_extract(payload_json, '$.role') IN (
                'setup', 'trade_ready', 'cancel', 'exit'
            )
            AND json_type(payload_json, '$.occurred_at') = 'text'
            AND json_type(payload_json, '$.expires_at') = 'text'
            AND json_type(payload_json, '$.title') = 'text'
            AND length(trim(json_extract(payload_json, '$.title'))) > 0
            AND json_type(payload_json, '$.body') = 'text'
            AND length(trim(json_extract(payload_json, '$.body'))) > 0
            AND json_type(payload_json, '$.targets') = 'array'
            AND json_array_length(json_extract(payload_json, '$.targets')) > 0
            AND json_extract(payload_json, '$.automatic_ordering') IS 0
        )
    )
) STRICT;

INSERT INTO notification_events_v4 (
    event_id, semantic_id, decision_id, source_projection_id, report_slot, lane,
    occurred_at_us, expires_at_us, payload_json, payload_sha256, target_set_sha256,
    writer_generation, created_at_us
)
SELECT
    event_id, semantic_id, decision_id, source_projection_id, report_slot, lane,
    occurred_at_us, expires_at_us, payload_json, payload_sha256, target_set_sha256,
    writer_generation, created_at_us
FROM notification_events;

DROP TABLE notification_events;
ALTER TABLE notification_events_v4 RENAME TO notification_events;

CREATE UNIQUE INDEX scheduled_report_slot_uidx
ON notification_events(report_slot)
WHERE lane = 'scheduled_report';

CREATE TRIGGER notification_events_cancellation_fence
BEFORE INSERT ON notification_events
WHEN EXISTS (
    SELECT 1 FROM notification_cancellations WHERE event_id = NEW.event_id
)
BEGIN
    SELECT RAISE(ABORT, 'cancellation_fenced');
END;

CREATE TRIGGER events_immutable_update
BEFORE UPDATE ON notification_events BEGIN SELECT RAISE(ABORT, 'event_immutable'); END;
CREATE TRIGGER events_immutable_delete
BEFORE DELETE ON notification_events BEGIN SELECT RAISE(ABORT, 'event_immutable'); END;
";
