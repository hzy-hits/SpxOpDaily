"""Add the time-ordered strategy observation lookup index."""

from collections.abc import Sequence

from alembic import op


revision: str = "0003_strategy_due_index"
down_revision: str | None = "0002_operational_tables"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_index(
        "ix_decisions_strategy_decision_at",
        "decisions",
        ["strategy_name", "decision_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_decisions_strategy_decision_at",
        table_name="decisions",
    )
