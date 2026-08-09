from __future__ import annotations

from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import subprocess
import sys

from spx_spark.data_platform.research.strategy_policy_backfill import (
    outcome_censor_distribution,
)
from spx_spark.infrastructure.operational_db import (
    persist_strategy_decision,
    persist_strategy_outcome,
)


NOW = datetime(2026, 8, 7, 18, 0, tzinfo=timezone.utc)


def _migrate(root: Path) -> Path:
    result = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        check=False,
        capture_output=True,
        text=True,
        env=os.environ | {"SPX_DATA_ROOT": str(root)},
    )
    assert result.returncode == 0, result.stderr
    return root / "spx.sqlite"


def _decision() -> dict[str, object]:
    source_at = NOW - timedelta(seconds=1)
    return {
        "schema_version": "strategy_decision.v2",
        "decision_id": "strategy:test-censor",
        "policy_version": "strategy_policy.bootstrap.v2",
        "decision_at": NOW.isoformat(),
        "available_at": NOW.isoformat(),
        "session_date": "2026-08-07",
        "decision_type": "CALL_DEBIT_VERTICAL",
        "candidate": {
            "direction": "UP",
            "opportunity_id": "strategy-opportunity:test-censor",
            "long": {
                "contract_id": "option:SPX:SPXW:20260807:7730:C",
                "strike": 7730.0,
                "right": "C",
                "provider": "schwab",
                "bid": 8.1,
                "ask": 8.4,
                "source_at": source_at.isoformat(),
            },
            "short": {
                "contract_id": "option:SPX:SPXW:20260807:7740:C",
                "strike": 7740.0,
                "right": "C",
                "provider": "schwab",
                "bid": 3.1,
                "ask": 3.3,
                "source_at": source_at.isoformat(),
            },
        },
        "market_facts": {"spot": {"spx": 7741.0}},
        "regime": {"path_state": "TREND", "terminal_state": "TREND_UP"},
        "desk_view": {"reason": "trend_pullback"},
        "why_not": {"reasons": []},
        "execution": {"action": "MANUAL_LIMIT", "automatic_ordering": False},
        "action_authority": "manual",
    }


def _outcome(
    horizon_minutes: int,
    *,
    status: str,
    censor_kind: str | None = None,
) -> dict[str, object]:
    target_at = NOW + timedelta(minutes=horizon_minutes)
    return {
        "decision_id": "strategy:test-censor",
        "horizon_minutes": horizon_minutes,
        "status": status,
        "target_at": target_at.isoformat(),
        "sampled_at": (target_at + timedelta(seconds=1)).isoformat(),
        "hypothesis_direction": "up",
        "spx_return_bps": None,
        "option_return_bps": None,
        "attributes": {
            "schema_version": "strategy_outcome_mark.v2",
            "censor_kind": censor_kind,
        },
    }


def test_outcome_censor_distribution_maps_legacy_exit_quote_unavailable(tmp_path: Path) -> None:
    database = _migrate(tmp_path)
    persist_strategy_decision(_decision(), database_path=database)
    persist_strategy_outcome(
        _outcome(5, status="censored", censor_kind="service_gap"),
        database_path=database,
    )
    persist_strategy_outcome(
        _outcome(10, status="censored", censor_kind="breach_quote_unavailable"),
        database_path=database,
    )
    persist_strategy_outcome(
        _outcome(15, status="exit_quote_unavailable"),
        database_path=database,
    )

    assert outcome_censor_distribution(
        database_path=database,
        session_date="2026-08-07",
    ) == {
        "breach_quote_unavailable": 1,
        "quote_gap": 1,
        "service_gap": 1,
    }
