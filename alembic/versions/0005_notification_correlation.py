"""Persist the decision correlation id on notification events.

The delivery worker loads an event by logical event id, after the in-memory
envelope is gone. A column keeps the decision trace available on the durable
event and lets receipts/ledger rows join without a sidecar lookup.
"""
from alembic import op
import sqlalchemy as sa

revision = "0005_notification_correlation"
down_revision = "0004_session_card_cover"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("notification_events", sa.Column("correlation_id", sa.Text(), nullable=True))
    op.create_index(
        "ix_notification_events_correlation",
        "notification_events",
        ["correlation_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_notification_events_correlation", table_name="notification_events")
    op.drop_column("notification_events", "correlation_id")
