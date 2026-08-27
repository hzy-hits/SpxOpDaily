from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest

from spx_spark.app_settings import get_settings
from spx_spark.infrastructure.operational_db import (
    OperationalDecisionConflict,
    persist_strategy_decision,
    persist_strategy_shadow_candidates,
    read_due_strategy_observations,
    read_strategy_decisions,
)


NOW = datetime(2026, 8, 7, 14, 5, tzinfo=timezone.utc)


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


def _alembic(root: Path, revision: str) -> None:
    result = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", revision],
        check=False,
        capture_output=True,
        text=True,
        env=os.environ | {"SPX_DATA_ROOT": str(root)},
    )
    assert result.returncode == 0, result.stderr


def _no_trade() -> dict[str, object]:
    return {
        "schema_version": "strategy_decision.v1",
        "decision_id": "strategy:no-trade",
        "policy_version": "strategy_signal_engine_v2.bootstrap.1",
        "decision_at": NOW.isoformat(),
        "available_at": NOW.isoformat(),
        "session_date": "2026-08-07",
        "decision_type": "NO_TRADE",
        "candidate": None,
        "regime": {"path_state": "RANGE"},
        "desk_view": {"reason": "confirmed_price_trigger_unavailable"},
        "why_not": {"reasons": ["confirmed_price_trigger_unavailable"]},
        "execution": {"action": "WAIT", "automatic_ordering": False},
    }


def _candidate() -> dict[str, object]:
    source_at = NOW - timedelta(seconds=1)
    return {
        **_no_trade(),
        "decision_id": "strategy:call-vertical",
        "decision_type": "CALL_DEBIT_VERTICAL",
        "candidate": {
            "direction": "UP",
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
        "why_not": {"reasons": []},
        "execution": {"action": "MANUAL_LIMIT", "automatic_ordering": False},
    }


def _shadow_candidate(suffix: str, *, offset: float = 0.0) -> dict[str, object]:
    source_at = NOW - timedelta(seconds=1)
    return {
        "candidate_id": f"shadow:{suffix}",
        "opportunity_id": f"strategy-opportunity:{suffix}",
        "strategy_type": "CALL_DEBIT_VERTICAL",
        "setup_kind": "TREND_PULLBACK",
        "direction": "UP",
        "quote": {"bid": 2.4 + offset, "ask": 2.8 + offset},
        "long": {
            "contract_id": f"option:SPX:SPXW:20260807:{7750 + offset:g}:C",
            "strike": 7750.0 + offset,
            "right": "C",
            "provider": "schwab",
            "bid": 5.1 + offset,
            "ask": 5.4 + offset,
            "source_at": source_at.isoformat(),
        },
        "short": {
            "contract_id": f"option:SPX:SPXW:20260807:{7760 + offset:g}:C",
            "strike": 7760.0 + offset,
            "right": "C",
            "provider": "schwab",
            "bid": 2.3 + offset,
            "ask": 2.6 + offset,
            "source_at": source_at.isoformat(),
        },
    }


def test_no_trade_is_idempotent_and_replay_reads_sql(tmp_path: Path) -> None:
    database = _migrate(tmp_path)
    decision = _no_trade()

    assert persist_strategy_decision(decision, database_path=database) == "strategy:no-trade"
    assert persist_strategy_decision(decision, database_path=database) == "strategy:no-trade"

    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT count(*) FROM decisions").fetchone()[0] == 1
        assert connection.execute("SELECT count(*) FROM decision_legs").fetchone()[0] == 0
        stored = connection.execute("SELECT attributes_json FROM decisions").fetchone()[0]
    assert json.loads(stored) == decision
    assert read_strategy_decisions(
        database_path=database,
        session_date="2026-08-07",
    ) == (decision,)


def test_repeated_no_trade_is_sampled_once_per_minute_but_changes_persist(
    tmp_path: Path,
) -> None:
    database = _migrate(tmp_path)
    first = _no_trade()
    repeated_at = NOW + timedelta(seconds=10)
    repeated = {
        **first,
        "decision_id": "strategy:no-trade-repeat",
        "decision_at": repeated_at.isoformat(),
        "available_at": repeated_at.isoformat(),
    }
    changed_at = NOW + timedelta(seconds=20)
    changed = {
        **repeated,
        "decision_id": "strategy:no-trade-changed",
        "decision_at": changed_at.isoformat(),
        "available_at": changed_at.isoformat(),
        "why_not": {"reasons": ["option_structure_unavailable"]},
    }

    assert persist_strategy_decision(first, database_path=database) == "strategy:no-trade"
    assert (
        persist_strategy_decision(
            repeated,
            database_path=database,
            previous_decision=first,
        )
        is None
    )
    assert persist_strategy_decision(
        changed,
        database_path=database,
        previous_decision=repeated,
    ) == "strategy:no-trade-changed"

    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT count(*) FROM decisions").fetchone()[0] == 2


def test_default_database_is_app_root_not_market_data_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _migrate(tmp_path)
    monkeypatch.setenv("SPX_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    try:
        persist_strategy_decision(_no_trade())
    finally:
        get_settings.cache_clear()

    assert (tmp_path / "spx.sqlite").is_file()
    assert not (tmp_path / "data" / "spx.sqlite").exists()


def test_existing_notification_rows_survive_forward_upgrade(tmp_path: Path) -> None:
    _alembic(tmp_path, "0001_notification_tables")
    database = tmp_path / "spx.sqlite"
    with sqlite3.connect(database) as connection:
        connection.execute(
            "INSERT INTO notification_events "
            "(idempotency_key, logical_event_id, source, kind, lane, channel, "
            "payload_json, payload_sha256, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("key", "event", "test", "test", "test", "bark", "{}", "hash", "delivered"),
        )
        connection.commit()

    _alembic(tmp_path, "head")

    with sqlite3.connect(database) as connection:
        assert connection.execute(
            "SELECT logical_event_id, status FROM notification_events"
        ).fetchall() == [("event", "delivered")]
        assert connection.execute(
            "SELECT version_num FROM alembic_version"
        ).fetchone()[0] == "0002_operational_tables"


def test_candidate_and_legs_commit_atomically_and_conflicts_fail(tmp_path: Path) -> None:
    database = _migrate(tmp_path)
    decision = _candidate()
    persist_strategy_decision(decision, database_path=database)

    with sqlite3.connect(database) as connection:
        row = connection.execute(
            "SELECT status, action, side FROM decisions WHERE decision_id=?",
            ("strategy:call-vertical",),
        ).fetchone()
        legs = connection.execute(
            "SELECT instrument_id, quantity, bid, ask FROM decision_legs "
            "WHERE decision_id=? ORDER BY leg_index",
            ("strategy:call-vertical",),
        ).fetchall()
    assert row == ("selected", "manual_limit", "up")
    assert legs == [
        ("option:SPX:SPXW:20260807:7730:C", 1.0, 8.1, 8.4),
        ("option:SPX:SPXW:20260807:7740:C", -1.0, 3.1, 3.3),
    ]

    with pytest.raises(OperationalDecisionConflict):
        persist_strategy_decision(
            {**decision, "execution": {"action": "WAIT"}},
            database_path=database,
        )


def _iron_condor_leg(
    *,
    strike: float,
    right: str,
    bid: float,
    ask: float,
) -> dict[str, object]:
    source_at = NOW - timedelta(seconds=1)
    return {
        "contract_id": f"option:SPX:SPXW:20260807:{strike:g}:{right}",
        "strike": strike,
        "right": right,
        "provider": "ibkr",
        "bid": bid,
        "ask": ask,
        "source_at": source_at.isoformat(),
    }


def test_iron_condor_four_legs_persist_signed_units(tmp_path: Path) -> None:
    database = _migrate(tmp_path)
    legs = (
        _iron_condor_leg(strike=7700.0, right="P", bid=3.1, ask=3.3),
        _iron_condor_leg(strike=7710.0, right="P", bid=4.3, ask=4.5),
        _iron_condor_leg(strike=7775.0, right="C", bid=3.5, ask=3.7),
        _iron_condor_leg(strike=7785.0, right="C", bid=1.9, ask=2.0),
    )
    decision = {
        **_no_trade(),
        "decision_id": "strategy:iron-condor",
        "decision_type": "IRON_CONDOR",
        "candidate": {
            "direction": "NEUTRAL",
            "strategy_type": "IRON_CONDOR",
            "setup_kind": "IRON_CONDOR_DELTA",
            "legs": list(legs),
        },
        "why_not": {"reasons": []},
        "execution": {"action": "MANUAL_LIMIT", "automatic_ordering": False},
    }

    persist_strategy_decision(decision, database_path=database)

    with sqlite3.connect(database) as connection:
        stored = connection.execute(
            "SELECT instrument_id, quantity FROM decision_legs "
            "WHERE decision_id=? ORDER BY leg_index",
            ("strategy:iron-condor",),
        ).fetchall()
    assert stored == [
        ("option:SPX:SPXW:20260807:7700:P", 1.0),
        ("option:SPX:SPXW:20260807:7710:P", -1.0),
        ("option:SPX:SPXW:20260807:7775:C", -1.0),
        ("option:SPX:SPXW:20260807:7785:C", 1.0),
    ]


def test_shadow_candidates_persist_join_observation_queue_and_stay_out_of_replay(
    tmp_path: Path,
) -> None:
    database = _migrate(tmp_path)
    decision = {
        **_candidate(),
        "market_facts": {"spot": {"spx": 7741.0}},
        "regime": {"path_state": "TREND", "terminal_state": "TREND_UP"},
        "shadow_candidates": [
            _shadow_candidate("one"),
            _shadow_candidate("two", offset=10.0),
        ],
    }

    persist_strategy_decision(decision, database_path=database)
    shadow_ids = persist_strategy_shadow_candidates(decision, database_path=database)

    assert shadow_ids == (
        "strategy:call-vertical:cand1",
        "strategy:call-vertical:cand2",
    )
    with sqlite3.connect(database) as connection:
        rows = connection.execute(
            "SELECT decision_id, status FROM decisions ORDER BY decision_id"
        ).fetchall()
    assert rows == [
        ("strategy:call-vertical", "selected"),
        ("strategy:call-vertical:cand1", "shadow_candidate"),
        ("strategy:call-vertical:cand2", "shadow_candidate"),
    ]

    observations = read_due_strategy_observations(
        now=NOW + timedelta(minutes=5, seconds=3),
        horizon_minutes=5,
        database_path=database,
    )
    assert {
        str(row["decision"]["decision_id"])
        for row in observations
    } == {
        "strategy:call-vertical",
        "strategy:call-vertical:cand1",
        "strategy:call-vertical:cand2",
    }
    replay = read_strategy_decisions(
        database_path=database,
        session_date="2026-08-07",
    )
    assert [row["decision_id"] for row in replay] == ["strategy:call-vertical"]


def test_rejected_shadow_candidate_persists_legs_without_trade_authority(
    tmp_path: Path,
) -> None:
    database = _migrate(tmp_path)
    candidate = _candidate()["candidate"]
    decision = {
        **_no_trade(),
        "decision_id": "strategy:shadow-call-vertical",
        "why_not": {
            "reasons": ["candidate_utility_not_positive"],
            "nearest_candidate": {
                **candidate,
                "shadow_only": True,
                "rejection_reasons": ["candidate_utility_not_positive"],
            },
        },
    }

    persist_strategy_decision(decision, database_path=database)

    with sqlite3.connect(database) as connection:
        row = connection.execute(
            "SELECT status, action, side FROM decisions WHERE decision_id=?",
            ("strategy:shadow-call-vertical",),
        ).fetchone()
        legs = connection.execute(
            "SELECT instrument_id, quantity FROM decision_legs "
            "WHERE decision_id=? ORDER BY leg_index",
            ("strategy:shadow-call-vertical",),
        ).fetchall()
    assert row == ("no_trade", "wait", "none")
    assert legs == [
        ("option:SPX:SPXW:20260807:7730:C", 1.0),
        ("option:SPX:SPXW:20260807:7740:C", -1.0),
    ]


def test_future_available_fact_is_rejected_before_write(tmp_path: Path) -> None:
    database = _migrate(tmp_path)
    decision = {
        **_no_trade(),
        "available_at": (NOW + timedelta(seconds=1)).isoformat(),
    }
    with pytest.raises(ValueError, match="unavailable at decision time"):
        persist_strategy_decision(decision, database_path=database)

    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT count(*) FROM decisions").fetchone()[0] == 0


def test_strategy_opportunity_event_dedupes_across_decisions_and_shadows(
    tmp_path: Path,
) -> None:
    database = _migrate(tmp_path)
    candidate = {
        **_candidate()["candidate"],
        "opportunity_id": "strategy-opportunity:o1",
        "strategy_type": "CALL_DEBIT_VERTICAL",
    }
    first = {
        **_candidate(),
        "candidate": candidate,
        "decision_id": "strategy:first-opportunity",
        "shadow_candidates": [_shadow_candidate("shadow-a")],
    }
    second = {
        **_candidate(),
        "candidate": {**candidate, "quote": {"bid": 2.7, "ask": 3.0}},
        "decision_id": "strategy:second-opportunity",
        "decision_at": (NOW + timedelta(minutes=1)).isoformat(),
        "available_at": (NOW + timedelta(minutes=1)).isoformat(),
    }
    expected_event_key = "strategy-opportunity:2026-08-07:strategy-opportunity:o1"

    persist_strategy_decision(first, database_path=database)
    persist_strategy_shadow_candidates(first, database_path=database)
    persist_strategy_decision(second, database_path=database)
    persist_strategy_decision(_no_trade(), database_path=database)

    with sqlite3.connect(database) as connection:
        events_rows = connection.execute(
            "SELECT event_key, event_type FROM events WHERE event_key=?",
            (expected_event_key,),
        ).fetchall()
        decision_rows = connection.execute(
            "SELECT decision_id, event_key FROM decisions "
            "WHERE decision_id IN (?, ?, ?, ?) ORDER BY decision_id",
            (
                "strategy:first-opportunity",
                "strategy:first-opportunity:cand1",
                "strategy:second-opportunity",
                "strategy:no-trade",
            ),
        ).fetchall()
        foreign_key_errors = connection.execute("PRAGMA foreign_key_check").fetchall()
    assert events_rows == [(expected_event_key, "strategy_opportunity")]
    assert decision_rows == [
        ("strategy:first-opportunity", expected_event_key),
        ("strategy:first-opportunity:cand1", expected_event_key),
        ("strategy:no-trade", None),
        ("strategy:second-opportunity", expected_event_key),
    ]
    assert foreign_key_errors == []


def test_read_due_keeps_bounded_window_and_prefers_fresh_over_service_gap(
    tmp_path: Path,
) -> None:
    database = _migrate(tmp_path)
    ancient_at = NOW - timedelta(hours=6)
    ancient_source = (ancient_at - timedelta(seconds=1)).isoformat()
    ancient_candidate = _candidate()["candidate"]
    ancient = {
        **_candidate(),
        "decision_id": "strategy:ancient",
        "decision_at": ancient_at.isoformat(),
        "available_at": ancient_at.isoformat(),
        "session_date": ancient_at.date().isoformat(),
        "candidate": {
            **ancient_candidate,
            "long": {**ancient_candidate["long"], "source_at": ancient_source},
            "short": {**ancient_candidate["short"], "source_at": ancient_source},
        },
    }
    recent_at = NOW - timedelta(minutes=5)
    recent_source = (recent_at - timedelta(seconds=1)).isoformat()
    recent_candidate = _candidate()["candidate"]
    recent = {
        **_candidate(),
        "decision_id": "strategy:recent",
        "decision_at": recent_at.isoformat(),
        "available_at": recent_at.isoformat(),
        "candidate": {
            **recent_candidate,
            "long": {**recent_candidate["long"], "source_at": recent_source},
            "short": {**recent_candidate["short"], "source_at": recent_source},
        },
    }
    persist_strategy_decision(ancient, database_path=database)
    persist_strategy_decision(recent, database_path=database)

    due = read_due_strategy_observations(
        now=NOW,
        horizon_minutes=5,
        maximum_lag_seconds=90.0,
        limit=10,
        database_path=database,
    )
    decision_ids = {str(item["decision"]["decision_id"]) for item in due}
    assert "strategy:ancient" not in decision_ids
    assert "strategy:recent" in decision_ids
