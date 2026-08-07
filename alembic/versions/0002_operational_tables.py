"""Add the unified operational fact tables."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op


revision: str = "0002_operational_tables"
down_revision: str | None = "0001_notification_tables"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _timestamps() -> tuple[sa.Column[object], sa.Column[object]]:
    return (
        sa.Column("created_at", sa.Text(), nullable=False),
        sa.Column("updated_at", sa.Text(), nullable=False),
    )


def upgrade() -> None:
    op.create_table(
        "sessions",
        sa.Column("session_date", sa.Text(), primary_key=True),
        sa.Column("market", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("opened_at", sa.Text()),
        sa.Column("closed_at", sa.Text()),
        sa.Column("data_quality", sa.Text(), nullable=False),
        sa.Column("metadata_json", sa.Text(), nullable=False),
        *_timestamps(),
        sa.CheckConstraint(
            "closed_at IS NULL OR opened_at IS NULL OR closed_at >= opened_at",
            name="ck_sessions_clock",
        ),
    )
    op.create_table(
        "events",
        sa.Column("event_key", sa.Text(), primary_key=True),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("session_date", sa.Text(), nullable=False),
        sa.Column("source_at", sa.Text(), nullable=False),
        sa.Column("available_at", sa.Text(), nullable=False),
        sa.Column("received_at", sa.Text()),
        sa.Column("phase", sa.Text()),
        sa.Column("direction", sa.Text()),
        sa.Column("data_quality", sa.Text(), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("attributes_json", sa.Text(), nullable=False),
        sa.Column("created_at", sa.Text(), nullable=False),
        sa.CheckConstraint("schema_version > 0", name="ck_events_schema_version"),
        sa.CheckConstraint("available_at >= source_at", name="ck_events_available_at"),
    )
    op.create_index("ix_events_session_source", "events", ["session_date", "source_at"])
    op.create_index("ix_events_type_source", "events", ["event_type", "source_at"])
    op.create_table(
        "decisions",
        sa.Column("decision_id", sa.Text(), primary_key=True),
        sa.Column("event_key", sa.Text(), sa.ForeignKey("events.event_key")),
        sa.Column("feature_snapshot_id", sa.Text(), sa.ForeignKey("events.event_key")),
        sa.Column("session_date", sa.Text()),
        sa.Column("strategy_name", sa.Text(), nullable=False),
        sa.Column("strategy_version", sa.Text(), nullable=False),
        sa.Column("decision_at", sa.Text(), nullable=False),
        sa.Column("available_at", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("action", sa.Text(), nullable=False),
        sa.Column("side", sa.Text(), nullable=False),
        sa.Column("reason", sa.Text()),
        sa.Column("gamma_regime", sa.Text()),
        sa.Column("attributes_json", sa.Text(), nullable=False),
        sa.Column("created_at", sa.Text(), nullable=False),
        sa.CheckConstraint("available_at <= decision_at", name="ck_decisions_available_at"),
    )
    op.create_index("ix_decisions_event", "decisions", ["event_key", "decision_at"])
    op.create_index(
        "ix_decisions_strategy",
        "decisions",
        ["strategy_name", "strategy_version", "decision_at"],
    )
    op.create_index("ix_decisions_session", "decisions", ["session_date", "decision_at"])
    op.create_table(
        "decision_legs",
        sa.Column(
            "decision_id",
            sa.Text(),
            sa.ForeignKey("decisions.decision_id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("leg_index", sa.Integer(), primary_key=True),
        sa.Column("instrument_id", sa.Text(), nullable=False),
        sa.Column("right_code", sa.Text()),
        sa.Column("expiry", sa.Text()),
        sa.Column("strike", sa.Float()),
        sa.Column("quantity", sa.Float()),
        sa.Column("bid", sa.Float()),
        sa.Column("ask", sa.Float()),
        sa.Column("delta", sa.Float()),
        sa.Column("gamma", sa.Float()),
        sa.Column("theta", sa.Float()),
        sa.Column("vega", sa.Float()),
        sa.Column("quote_source_at", sa.Text(), nullable=False),
        sa.Column("quote_available_at", sa.Text(), nullable=False),
        sa.Column("attributes_json", sa.Text(), nullable=False),
        sa.Column("created_at", sa.Text(), nullable=False),
        sa.CheckConstraint("leg_index >= 0", name="ck_decision_legs_index"),
        sa.CheckConstraint(
            "quote_available_at >= quote_source_at",
            name="ck_decision_legs_available_at",
        ),
    )
    op.create_index(
        "ix_decision_legs_instrument",
        "decision_legs",
        ["instrument_id", "quote_source_at"],
    )
    op.create_table(
        "outcomes",
        sa.Column("outcome_id", sa.Text(), primary_key=True),
        sa.Column("event_key", sa.Text(), sa.ForeignKey("events.event_key"), nullable=False),
        sa.Column("decision_id", sa.Text(), sa.ForeignKey("decisions.decision_id")),
        sa.Column("horizon_minutes", sa.Integer(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("target_at", sa.Text(), nullable=False),
        sa.Column("sampled_at", sa.Text()),
        sa.Column("hypothesis_direction", sa.Text()),
        sa.Column("spx_return_bps", sa.Float()),
        sa.Column("spx_mfe_bps", sa.Float()),
        sa.Column("spx_mae_bps", sa.Float()),
        sa.Column("option_return_bps", sa.Float()),
        sa.Column("option_pnl", sa.Float()),
        sa.Column("attributes_json", sa.Text(), nullable=False),
        sa.Column("created_at", sa.Text(), nullable=False),
        sa.CheckConstraint("horizon_minutes > 0", name="ck_outcomes_horizon"),
    )
    op.create_index(
        "ux_outcomes_event_decision_horizon",
        "outcomes",
        ["event_key", "decision_id", "horizon_minutes"],
        unique=True,
    )
    op.create_index("ix_outcomes_target", "outcomes", ["target_at"])
    op.create_table(
        "provider_incidents",
        sa.Column("incident_id", sa.Text(), primary_key=True),
        sa.Column("provider", sa.Text(), nullable=False),
        sa.Column("incident_type", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("reason_code", sa.Text()),
        sa.Column("started_at", sa.Text(), nullable=False),
        sa.Column("last_seen_at", sa.Text(), nullable=False),
        sa.Column("resolved_at", sa.Text()),
        sa.Column("source_at", sa.Text()),
        sa.Column("received_at", sa.Text(), nullable=False),
        sa.Column("attributes_json", sa.Text(), nullable=False),
        *_timestamps(),
        sa.CheckConstraint("status IN ('open','resolved')", name="ck_provider_incidents_status"),
        sa.CheckConstraint("last_seen_at >= started_at", name="ck_provider_incidents_seen"),
        sa.CheckConstraint(
            "resolved_at IS NULL OR resolved_at >= started_at",
            name="ck_provider_incidents_resolved",
        ),
        sa.CheckConstraint(
            "source_at IS NULL OR received_at >= source_at",
            name="ck_provider_incidents_received",
        ),
    )
    op.create_index(
        "ix_provider_incidents_provider_started",
        "provider_incidents",
        ["provider", "started_at"],
    )
    op.create_index(
        "ix_provider_incidents_status_seen",
        "provider_incidents",
        ["status", "last_seen_at"],
    )
    op.create_table(
        "compaction_manifests",
        sa.Column("manifest_id", sa.Text(), primary_key=True),
        sa.Column("source_path", sa.Text(), nullable=False),
        sa.Column("source_sha256", sa.Text(), nullable=False),
        sa.Column("source_size", sa.Integer(), nullable=False),
        sa.Column("source_mtime_ns", sa.Integer(), nullable=False),
        sa.Column("output_path", sa.Text()),
        sa.Column("output_sha256", sa.Text()),
        sa.Column("row_count", sa.Integer(), nullable=False),
        sa.Column("min_received_at", sa.Text()),
        sa.Column("max_received_at", sa.Text()),
        sa.Column("schema_version", sa.Text(), nullable=False),
        sa.Column("writer_version", sa.Text(), nullable=False),
        sa.Column("dataset", sa.Text(), nullable=False),
        sa.Column("completed_at", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("created_at", sa.Text(), nullable=False),
        sa.UniqueConstraint("source_path", "source_sha256", name="uq_compaction_source"),
        sa.CheckConstraint("source_size >= 0", name="ck_compaction_source_size"),
        sa.CheckConstraint("source_mtime_ns >= 0", name="ck_compaction_source_mtime"),
        sa.CheckConstraint("row_count >= 0", name="ck_compaction_row_count"),
        sa.CheckConstraint(
            "(output_path IS NULL) = (output_sha256 IS NULL)",
            name="ck_compaction_output_pair",
        ),
        sa.CheckConstraint(
            "min_received_at IS NULL OR max_received_at IS NULL "
            "OR max_received_at >= min_received_at",
            name="ck_compaction_received_range",
        ),
    )
    op.create_index(
        "ix_compaction_completed",
        "compaction_manifests",
        ["completed_at", "status"],
    )


def downgrade() -> None:
    op.drop_index("ix_compaction_completed", table_name="compaction_manifests")
    op.drop_table("compaction_manifests")
    op.drop_index("ix_provider_incidents_status_seen", table_name="provider_incidents")
    op.drop_index("ix_provider_incidents_provider_started", table_name="provider_incidents")
    op.drop_table("provider_incidents")
    op.drop_index("ix_outcomes_target", table_name="outcomes")
    op.drop_index("ux_outcomes_event_decision_horizon", table_name="outcomes")
    op.drop_table("outcomes")
    op.drop_index("ix_decision_legs_instrument", table_name="decision_legs")
    op.drop_table("decision_legs")
    op.drop_index("ix_decisions_session", table_name="decisions")
    op.drop_index("ix_decisions_strategy", table_name="decisions")
    op.drop_index("ix_decisions_event", table_name="decisions")
    op.drop_table("decisions")
    op.drop_index("ix_events_type_source", table_name="events")
    op.drop_index("ix_events_session_source", table_name="events")
    op.drop_table("events")
    op.drop_table("sessions")
