"""Cover session authorization queries without reading research payloads."""
from alembic import op

revision = "0004_session_card_cover"
down_revision = "0003_strategy_due_index"
branch_labels = None
depends_on = None

FIELDS = {
    "card_opportunity": "$.candidate.opportunity_id",
    "card_direction": "$.candidate.direction",
    "card_setup": "$.candidate.setup_kind",
    "card_trigger": "$.candidate.trigger_level",
    "card_mode": "$.market_facts.session.mode",
}


def upgrade() -> None:
    for name, path in FIELDS.items():
        kind = "REAL" if name == "card_trigger" else "TEXT"
        op.execute(f"ALTER TABLE decisions ADD COLUMN {name} {kind}")
    assignments = [f"{name}=CAST(json_extract(attributes_json, '{path}') AS TEXT)"
                   for name, path in FIELDS.items() if name != "card_trigger"]
    assignments.append("card_trigger=CASE WHEN json_type(attributes_json, '$.candidate.trigger_level') "
                       "IN ('integer','real','true','false') THEN "
                       "json_extract(attributes_json, '$.candidate.trigger_level') END")
    op.execute("CREATE INDEX ix_decisions_session_card ON decisions "
               "(session_date,decision_at,decision_id,card_opportunity,card_direction,"
               "card_setup,card_trigger,card_mode) WHERE "
               "strategy_name='strategy_signal_engine_v2' AND status='selected'")
    op.execute("UPDATE decisions INDEXED BY ix_decisions_session_card SET " + ",".join(assignments) +
               " WHERE strategy_name='strategy_signal_engine_v2' AND status='selected'")



def downgrade() -> None:
    op.drop_index("ix_decisions_session_card", table_name="decisions")
    for name in reversed(FIELDS):
        op.execute(f"ALTER TABLE decisions DROP COLUMN {name}")
