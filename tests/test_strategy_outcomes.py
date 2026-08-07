from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sqlite3
import subprocess
import sys

from spx_spark.application.order_map.strategy_outcomes import (
    observe_due_strategy_outcomes,
)
from spx_spark.infrastructure.operational_db import persist_strategy_decision
from spx_spark.marketdata import InstrumentId, MarketDataQuality, Provider, Quote
from spx_spark.storage import LatestState


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


def _leg(strike: float, bid: float, ask: float) -> dict[str, object]:
    return {
        "contract_id": f"option:SPX:SPXW:20260807:{strike:g}:C",
        "strike": strike,
        "right": "C",
        "provider": "schwab",
        "bid": bid,
        "ask": ask,
        "source_at": (NOW - timedelta(seconds=1)).isoformat(),
    }


def _decision() -> dict[str, object]:
    shadow = {
        "strategy_type": "CALL_BUTTERFLY",
        "direction": "NEUTRAL",
        "opportunity_id": "strategy-opportunity:shadow-pin",
        "legs": [
            _leg(7735.0, 4.8, 5.0),
            _leg(7740.0, 2.0, 2.2),
            _leg(7745.0, 0.6, 0.8),
        ],
        "shadow_only": True,
        "rejection_reasons": ["candidate_utility_not_positive"],
    }
    return {
        "schema_version": "strategy_decision.v1",
        "decision_id": "strategy:shadow-pin",
        "policy_version": "strategy_policy.bootstrap.v1",
        "decision_at": NOW.isoformat(),
        "available_at": NOW.isoformat(),
        "session_date": "2026-08-07",
        "decision_type": "NO_TRADE",
        "candidate": None,
        "market_facts": {"spot": {"spx": 7741.0}},
        "regime": {"path_state": "BALANCED", "terminal_state": "PIN_STABLE"},
        "desk_view": {"reason": "candidate_utility_not_positive"},
        "why_not": {
            "reasons": ["candidate_utility_not_positive"],
            "nearest_candidate": shadow,
        },
        "execution": {"action": "WAIT", "automatic_ordering": False},
        "action_authority": "none",
    }


def _quote(instrument: InstrumentId, bid: float, ask: float, now: datetime) -> Quote:
    return Quote(
        instrument=instrument,
        provider=Provider.SCHWAB,
        received_at=now,
        quote_time=now,
        quality=MarketDataQuality.LIVE,
        bid=bid,
        ask=ask,
    )


def test_shadow_candidate_records_fresh_exit_mark_without_claiming_fill(
    tmp_path: Path,
) -> None:
    database = _migrate(tmp_path)
    persist_strategy_decision(_decision(), database_path=database)
    sampled_at = NOW + timedelta(minutes=5, seconds=3)
    quotes = (
        _quote(InstrumentId.index("SPX"), 7741.9, 7742.1, sampled_at),
        _quote(InstrumentId.option("SPX", expiry="20260807", strike=7735, right="C", trading_class="SPXW"), 4.5, 4.7, sampled_at),
        _quote(InstrumentId.option("SPX", expiry="20260807", strike=7740, right="C", trading_class="SPXW"), 2.0, 2.2, sampled_at),
        _quote(InstrumentId.option("SPX", expiry="20260807", strike=7745, right="C", trading_class="SPXW"), 0.6, 0.8, sampled_at),
    )
    latest = LatestState(
        created_at=sampled_at,
        as_of=sampled_at,
        quotes=quotes,
        best_quotes=quotes,
    )

    result = observe_due_strategy_outcomes(
        latest,
        now=sampled_at,
        database_path=database,
    )

    assert result["observed"] == 1
    assert result["statuses"] == {"observed": 1}
    with sqlite3.connect(database) as connection:
        row = connection.execute(
            "SELECT status, spx_return_bps, option_return_bps, option_pnl, attributes_json "
            "FROM outcomes"
        ).fetchone()
    attributes = json.loads(row[4])
    assert row[0] == "observed"
    assert row[1] > 0
    assert row[2] < 0
    assert row[3] is None
    assert attributes["entry_combo_ask"] == 1.8
    assert attributes["exit_combo_bid"] == 0.7
    assert attributes["gross_option_pnl"] == -110.0
    assert attributes["net_option_pnl"] is None
    assert attributes["fill_status"] == "not_observed_no_order_capability"

    repeated = observe_due_strategy_outcomes(
        latest,
        now=sampled_at + timedelta(seconds=1),
        database_path=database,
    )
    assert repeated["observed"] == 0
