from __future__ import annotations

from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import subprocess
import sys

from spx_spark.data_platform.research.strategy_policy_backfill import (
    build_policy_ev_table,
    mark_duplicate_opportunities,
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
        "policy_version": "strategy_policy.bootstrap.v3",
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


def _policy_decision(
    decision_id: str,
    *,
    session_date: str = "2026-08-07",
    setup_kind: str = "TREND_PULLBACK",
    direction: str = "UP",
    terminal_state: str = "TREND_UP",
) -> dict[str, object]:
    decision = _decision()
    decision["decision_id"] = decision_id
    decision["session_date"] = session_date
    decision["decision_at"] = NOW.isoformat()
    decision["available_at"] = NOW.isoformat()
    candidate = dict(decision["candidate"])
    candidate["setup_kind"] = setup_kind
    candidate["direction"] = direction
    candidate["opportunity_id"] = f"strategy-opportunity:{decision_id}"
    decision["candidate"] = candidate
    decision["regime"] = {"path_state": "TREND", "terminal_state": terminal_state}
    return decision


def _policy_row(
    decision_id: str,
    policy_pnl_points: float,
    *,
    session_date: str = "2026-08-07",
    setup_kind: str = "TREND_PULLBACK",
    direction: str = "UP",
    terminal_state: str = "TREND_UP",
) -> dict[str, object]:
    return {
        "decision_id": decision_id,
        "session_date": session_date,
        "setup_kind": setup_kind,
        "direction": direction,
        "regime_terminal_state": terminal_state,
        "policy_pnl_points": policy_pnl_points,
        "duplicate_of": None,
        "outbox_accepted": None,
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


def test_mark_duplicate_opportunities_flags_later_rows_with_same_event_key() -> None:
    rows = mark_duplicate_opportunities(
        [
            {
                "decision_id": "dec-1",
                "event_key": "strategy-opportunity:2026-08-07:o1",
                "decision_at": "2026-08-07T18:00:00+00:00",
            },
            {
                "decision_id": "dec-2",
                "event_key": "strategy-opportunity:2026-08-07:o1",
                "decision_at": "2026-08-07T18:01:00+00:00",
            },
            {
                "decision_id": "dec-3",
                "event_key": "strategy-opportunity:2026-08-07:o1",
                "decision_at": "2026-08-07T18:02:00+00:00",
            },
        ]
    )

    assert [row["duplicate_of"] for row in rows] == [None, "dec-1", "dec-1"]
    assert sum(row["duplicate_of"] is None for row in rows) == 1


def test_mark_duplicate_opportunities_excludes_unaccepted_opportunities() -> None:
    rows = mark_duplicate_opportunities(
        [
            {
                "decision_id": "dec-o1-a",
                "event_key": "strategy-opportunity:2026-08-07:strategy-opportunity:o1",
                "opportunity_id": "strategy-opportunity:o1",
                "decision_at": "2026-08-07T18:00:00+00:00",
            },
            {
                "decision_id": "dec-o1-b",
                "event_key": "strategy-opportunity:2026-08-07:strategy-opportunity:o1",
                "opportunity_id": "strategy-opportunity:o1",
                "decision_at": "2026-08-07T18:01:00+00:00",
            },
            {
                "decision_id": "dec-o2",
                "event_key": "strategy-opportunity:2026-08-07:strategy-opportunity:o2",
                "opportunity_id": "strategy-opportunity:o2",
                "decision_at": "2026-08-07T18:00:30+00:00",
            },
        ],
        accepted_opportunity_ids={"strategy-opportunity:o1"},
    )

    by_id = {row["decision_id"]: row for row in rows}
    assert by_id["dec-o1-a"]["duplicate_of"] is None
    assert by_id["dec-o1-a"]["outbox_accepted"] is True
    assert by_id["dec-o1-b"]["duplicate_of"] == "dec-o1-a"
    assert by_id["dec-o1-b"]["outbox_accepted"] is True
    # Never accepted by outbox: keep the row but do not count it as primary.
    assert by_id["dec-o2"]["duplicate_of"] is None
    assert by_id["dec-o2"]["outbox_accepted"] is False


def test_build_policy_ev_table_groups_values_and_counts_censored(tmp_path: Path) -> None:
    database = _migrate(tmp_path)
    rows = []
    for index in range(20):
        decision_id = f"dec-{index}"
        persist_strategy_decision(
            _policy_decision(decision_id),
            database_path=database,
        )
        rows.append(_policy_row(decision_id, float(index)))

    persist_strategy_decision(
        _policy_decision("dec-censored"),
        database_path=database,
    )
    persist_strategy_outcome(
        _outcome(20, status="censored", censor_kind="service_gap")
        | {"decision_id": "dec-censored"},
        database_path=database,
    )

    table = build_policy_ev_table(
        rows,
        database_path=database,
        session_date="2026-08-07",
    )

    bucket = table["buckets"]["TREND_PULLBACK|UP|TREND_UP"]
    assert table["schema_version"] == "policy_ev_table.v1"
    assert table["management_policy_version"] == "management_policy.v2"
    assert table["source_sessions"] == ["2026-08-07"]
    assert bucket == {
        "n": 20,
        "ev_points": 9.5,
        "p25": 4.75,
        "p75": 14.25,
        "n_censored": 0,
        "reason": None,
    }


def test_build_policy_ev_table_uses_low_sample_reason_and_legacy_censor_mapping(
    tmp_path: Path,
) -> None:
    database = _migrate(tmp_path)
    rows = []
    for index in range(19):
        decision_id = f"dec-low-{index}"
        persist_strategy_decision(
            _policy_decision(
                decision_id,
                setup_kind="BREAKOUT_ACCEPTANCE",
                direction="DOWN",
                terminal_state="TREND_DOWN",
            ),
            database_path=database,
        )
        rows.append(
            _policy_row(
                decision_id,
                float(index),
                setup_kind="BREAKOUT_ACCEPTANCE",
                direction="DOWN",
                terminal_state="TREND_DOWN",
            )
        )

    persist_strategy_decision(
        _policy_decision(
            "dec-legacy-censor",
            setup_kind="BREAKOUT_ACCEPTANCE",
            direction="DOWN",
            terminal_state="TREND_DOWN",
        ),
        database_path=database,
    )
    persist_strategy_outcome(
        _outcome(20, status="exit_quote_unavailable")
        | {"decision_id": "dec-legacy-censor"},
        database_path=database,
    )

    table = build_policy_ev_table(
        rows,
        database_path=database,
        session_date="2026-08-07",
    )

    assert table["buckets"]["BREAKOUT_ACCEPTANCE|DOWN|TREND_DOWN"] == {
        "n": 19,
        "ev_points": None,
        "p25": None,
        "p75": None,
        "n_censored": 0,
        "reason": "low_sample",
    }
