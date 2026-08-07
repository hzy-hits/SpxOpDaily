"""Add the per-target notification delivery contract."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op


revision: str = "0002_notification_delivery_contract"
down_revision: str | None = "0001_notification_tables"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("notification_events") as batch:
        batch.add_column(
            sa.Column("logical_event_id", sa.Text(), nullable=False, server_default="")
        )
        batch.add_column(sa.Column("source", sa.Text(), nullable=False, server_default=""))
        batch.add_column(sa.Column("kind", sa.Text(), nullable=False, server_default=""))
        batch.add_column(sa.Column("lane", sa.Text(), nullable=False, server_default=""))
        batch.add_column(sa.Column("expires_at", sa.DateTime(), nullable=True))
        batch.add_column(sa.Column("payload_sha256", sa.Text(), nullable=False, server_default=""))
        batch.add_column(sa.Column("cancelled_at", sa.DateTime(), nullable=True))
        batch.add_column(sa.Column("cancel_reason", sa.Text(), nullable=True))
        batch.add_column(sa.Column("last_error", sa.Text(), nullable=True))
        batch.create_index(
            "ix_notification_events_logical_event",
            ["logical_event_id"],
            unique=False,
        )
    with op.batch_alter_table("notification_attempts") as batch:
        batch.add_column(
            sa.Column("attempted", sa.Boolean(), nullable=False, server_default=sa.false())
        )
        batch.add_column(sa.Column("ok", sa.Boolean(), nullable=False, server_default=sa.false()))


def downgrade() -> None:
    with op.batch_alter_table("notification_attempts") as batch:
        batch.drop_column("ok")
        batch.drop_column("attempted")
    with op.batch_alter_table("notification_events") as batch:
        batch.drop_index("ix_notification_events_logical_event")
        batch.drop_column("last_error")
        batch.drop_column("cancel_reason")
        batch.drop_column("cancelled_at")
        batch.drop_column("payload_sha256")
        batch.drop_column("expires_at")
        batch.drop_column("lane")
        batch.drop_column("kind")
        batch.drop_column("source")
        batch.drop_column("logical_event_id")
