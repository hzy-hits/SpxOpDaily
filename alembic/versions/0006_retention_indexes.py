"""Index foreign-key checks used by bounded operational-history retention."""
from alembic import op

revision = "0006_retention_indexes"
down_revision = "0005_notification_correlation"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index("ix_decisions_feature_snapshot", "decisions", ["feature_snapshot_id"])
    op.create_index("ix_outcomes_decision", "outcomes", ["decision_id"])


def downgrade() -> None:
    op.drop_index("ix_outcomes_decision", table_name="outcomes")
    op.drop_index("ix_decisions_feature_snapshot", table_name="decisions")
