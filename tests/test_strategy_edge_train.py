from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3

from spx_spark.analytics.options.strategy_payoff import policy_mark_horizon_end
from spx_spark.application.order_map.strategy_edge_model import FEATURE_NAMES
from spx_spark.data_platform.research.odte_level_signals import OptionTick
from spx_spark.data_platform.research.strategy_edge_train import (
    ENTRY_EDGE_POLICY,
    load_candidate_labels,
    train_edge_artifact,
)


def test_edge_training_holds_to_1545_without_a_twenty_minute_stop() -> None:
    assert ENTRY_EDGE_POLICY.time_stop_minutes is None
    assert ENTRY_EDGE_POLICY.policy_version == "management_policy.v2"
    assert ENTRY_EDGE_POLICY.hard_exit_et == "15:45"
    assert ENTRY_EDGE_POLICY.premium_stop_fraction == 0.50
    start = datetime(2026, 8, 17, 14, 30, tzinfo=timezone.utc)
    end = policy_mark_horizon_end(
        start,
        ENTRY_EDGE_POLICY,
        session_date=date(2026, 8, 17),
        lookforward_minutes=None,
    )
    assert end == datetime(2026, 8, 17, 19, 45, tzinfo=timezone.utc)
    assert end - start > timedelta(minutes=20)


def test_candidate_loader_expands_hard_gate_universe_and_dedupes_by_geometry(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database = tmp_path / "spx.sqlite"
    connection = sqlite3.connect(database)
    connection.execute(
        """
        CREATE TABLE decisions (
            decision_id TEXT,
            event_key TEXT,
            session_date TEXT,
            strategy_name TEXT,
            decision_at TEXT,
            status TEXT,
            attributes_json TEXT
        )
        """
    )
    session = "2026-08-17"
    facts = {
        "session": {"mode": "rth"},
        "structure": {"strike_differential_context": {"expiry": "20260817"}},
    }

    def full_candidate(
        candidate_id: str,
        strategy_type: str,
        strikes: tuple[float, float],
    ) -> dict[str, object]:
        right = "C" if strategy_type.startswith("CALL") else "P"
        return {
            "candidate_id": candidate_id,
            "opportunity_id": "sticky-winner",
            "strategy_type": strategy_type,
            "direction": "UP" if right == "C" else "DOWN",
            "long": {
                "expiry": session,
                "strike": strikes[0],
                "right": right,
                "provider": "schwab",
            },
            "short": {
                "expiry": session,
                "strike": strikes[1],
                "right": right,
                "provider": "schwab",
            },
        }

    payloads = [
        {
            "candidate": full_candidate("call-a", "CALL_DEBIT_VERTICAL", (6000.0, 6005.0)),
            "market_facts": facts,
            "regime": {},
        },
        {
            "candidate": full_candidate("call-b", "CALL_DEBIT_VERTICAL", (6010.0, 6015.0)),
            "market_facts": facts,
            "regime": {},
        },
        {
            "candidate": {},
            "why_not": {},
            "shadow_candidates": [
                full_candidate("put-shadow", "PUT_DEBIT_VERTICAL", (6045.0, 6040.0))
            ],
            "candidates_considered": [
                {
                    "candidate_id": "call-considered",
                    "strategy_type": "CALL_DEBIT_VERTICAL",
                    "strikes": [6020.0, 6025.0],
                    "gate_failures": [],
                },
                {
                    "candidate_id": "call-gate-failed",
                    "strategy_type": "CALL_DEBIT_VERTICAL",
                    "strikes": [6030.0, 6035.0],
                    "gate_failures": [{"gate": "test"}],
                },
            ],
            "market_facts": facts,
            "regime": {},
        },
        {
            "candidate": full_candidate(
                "call-sticky-reprint", "CALL_DEBIT_VERTICAL", (6000.0, 6005.0)
            ),
            "market_facts": facts,
            "regime": {},
        },
    ]
    for index, payload in enumerate(payloads):
        at = datetime(2026, 8, 17, 14, index, tzinfo=timezone.utc)
        connection.execute(
            "INSERT INTO decisions VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                f"decision-{index}",
                f"event-{index}",
                session,
                "strategy_signal_engine_v2",
                at.isoformat(),
                "selected" if index in {0, 1, 3} else "no_trade",
                json.dumps(payload),
            ),
        )
    connection.commit()
    connection.close()

    class FakeQuoteStore:
        def __init__(self, data_root: Path) -> None:
            del data_root

        def close(self) -> None:
            return None

        def option_series(self, **kwargs) -> list[OptionTick]:
            start, end = kwargs["start"], kwargs["end"]
            at = end if end - start <= timedelta(minutes=1) else start + timedelta(minutes=1)
            return [OptionTick(at=at, bid=1.0, ask=1.1, mid=1.05)]

    monkeypatch.setattr(
        "spx_spark.data_platform.research.strategy_edge_train.QuoteStore",
        FakeQuoteStore,
    )
    funnel: dict[str, object] = {}
    rows = load_candidate_labels(
        database_path=database,
        data_root=tmp_path,
        start_date=session,
        end_date=session,
        funnel=funnel,
    )

    assert len(rows) == 4
    assert {row["candidate_id"] for row in rows} == {
        "call-a",
        "call-b",
        "call-considered",
        "put-shadow",
    }
    assert funnel["candidate_occurrences"] == {
        "primary_debit": 3,
        "shadow_debit": 1,
        "considered_debit_passed_hard_gates": 1,
    }
    assert funnel["considered"]["debit_vertical_gate_failed"] == 1
    assert funnel["unique_candidate_geometries"] == 4
    assert funnel["duplicate_candidate_occurrences_dropped"] == 1
    assert funnel["labeling"]["labeled_rows"] == 4


def test_geometry_dedup_keeps_gth_and_rth_prints_separate(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database = tmp_path / "spx.sqlite"
    connection = sqlite3.connect(database)
    connection.execute(
        """
        CREATE TABLE decisions (
            decision_id TEXT,
            event_key TEXT,
            session_date TEXT,
            strategy_name TEXT,
            decision_at TEXT,
            status TEXT,
            attributes_json TEXT
        )
        """
    )
    session = "2026-08-17"
    candidate = {
        "candidate_id": "same-spread",
        "opportunity_id": "sticky-winner",
        "strategy_type": "CALL_DEBIT_VERTICAL",
        "direction": "UP",
        "long": {
            "expiry": session,
            "strike": 6000.0,
            "right": "C",
            "provider": "schwab",
        },
        "short": {
            "expiry": session,
            "strike": 6010.0,
            "right": "C",
            "provider": "schwab",
        },
        "economics": {"max_loss_points": 1.1, "max_gain_points": 8.9, "width_points": 10.0},
    }
    for decision_id, mode, at in (
        ("gth-print", "gth", "2026-08-17T08:00:00+00:00"),
        ("rth-print", "rth", "2026-08-17T14:30:00+00:00"),
        ("rth-reprint", "rth", "2026-08-17T14:31:00+00:00"),
    ):
        payload = {
            "candidate": candidate,
            "market_facts": {
                "session": {"mode": mode},
                "structure": {"strike_differential_context": {"expiry": "20260817"}},
            },
            "regime": {},
        }
        connection.execute(
            "INSERT INTO decisions VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                decision_id,
                decision_id,
                session,
                "strategy_signal_engine_v2",
                at,
                "selected",
                json.dumps(payload),
            ),
        )
    connection.commit()
    connection.close()

    class FakeQuoteStore:
        def __init__(self, data_root: Path) -> None:
            del data_root

        def close(self) -> None:
            return None

        def option_series(self, **kwargs) -> list[OptionTick]:
            start, end = kwargs["start"], kwargs["end"]
            at = end if end - start <= timedelta(minutes=1) else start + timedelta(minutes=1)
            return [OptionTick(at=at, bid=1.0, ask=1.1, mid=1.05)]

    monkeypatch.setattr(
        "spx_spark.data_platform.research.strategy_edge_train.QuoteStore",
        FakeQuoteStore,
    )
    funnel: dict[str, object] = {}
    rows = load_candidate_labels(
        database_path=database,
        data_root=tmp_path,
        start_date=session,
        end_date=session,
        funnel=funnel,
    )

    assert {row["model_key"] for row in rows} == {"gth|vertical", "rth|vertical"}
    assert len(rows) == 2
    assert funnel["unique_candidate_geometries"] == 2
    assert funnel["duplicate_candidate_occurrences_dropped"] == 1


class _FakeLakeQuoteStore:
    def __init__(self, data_root: Path) -> None:
        del data_root

    def close(self) -> None:
        return None

    def load_option_window(self, **kwargs) -> int:
        del kwargs
        return 40

    def option_snapshot(self, **kwargs) -> dict[tuple[str, float, str], OptionTick]:
        at = kwargs["as_of"] - timedelta(seconds=1)
        ticks = {}
        for strike in range(5940, 6065, 5):
            for right in ("C", "P"):
                bid, ask = self._prices(float(strike), right)
                ticks[("schwab", float(strike), right)] = OptionTick(
                    at=at,
                    bid=bid,
                    ask=ask,
                    mid=(bid + ask) / 2.0,
                    source_at=at,
                    delta=0.45 if right == "C" else -0.45,
                )
        return ticks

    def option_series(self, **kwargs) -> list[OptionTick]:
        start, end = kwargs["start"], kwargs["end"]
        bid, ask = self._prices(float(kwargs["strike"]), str(kwargs["right"]))
        times = [start, min(end, start + timedelta(minutes=1))]
        return [
            OptionTick(
                at=at,
                bid=bid,
                ask=ask,
                mid=(bid + ask) / 2.0,
                source_at=at,
                delta=0.45 if kwargs["right"] == "C" else -0.45,
            )
            for at in dict.fromkeys(times)
        ]

    @staticmethod
    def _prices(strike: float, right: str) -> tuple[float, float]:
        distance = (strike - 6000.0) / 5.0
        bid = max(0.25, 5.0 - distance * 0.75) if right == "C" else max(0.25, 5.0 + distance * 0.75)
        return bid, bid + 0.10


def _heartbeat_database(path: Path) -> None:
    connection = sqlite3.connect(path)
    connection.execute(
        """
        CREATE TABLE decisions (
            decision_id TEXT,
            event_key TEXT,
            session_date TEXT,
            strategy_name TEXT,
            decision_at TEXT,
            status TEXT,
            attributes_json TEXT
        )
        """
    )
    facts = {
        "session_date": "2026-08-17",
        "quality": {"status": "ready"},
        "spot": {"spx": 6000.0},
        "path": {},
        "event": {"state": "normal", "entry_allowed": True},
        "structure": {
            "put_wall": 5970.0,
            "flip_zone": [5990.0, 5995.0],
            "zero_gamma": 5995.0,
            "call_wall": 6030.0,
        },
        "volatility": {"expected_move_points": 30.0},
        "trigger": {"phase": "far", "level": 6000.0},
        "rth_setups": [
            {
                "setup_kind": "ES_VOLUME_MOMENTUM",
                "state": "ENTRY_WINDOW_OPEN",
                "direction": "UP",
                "trigger_level": 6000.0,
                "source": "test",
            }
        ],
    }
    payload = {
        "candidate": {},
        "why_not": {},
        "market_facts": facts,
        "regime": {
            "path_state": "TREND",
            "path_direction": "UP",
            "terminal_state": "UNCERTAIN",
            "pin": {},
        },
    }
    connection.execute(
        "INSERT INTO decisions VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            "heartbeat",
            "heartbeat-event",
            "2026-08-17",
            "strategy_signal_engine_v2",
            "2026-08-17T14:30:00+00:00",
            "no_trade",
            json.dumps(payload),
        ),
    )
    connection.commit()
    connection.close()


def test_empty_heartbeat_lake_snapshot_adds_debit_geometries(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database = tmp_path / "spx.sqlite"
    _heartbeat_database(database)
    monkeypatch.setattr(
        "spx_spark.data_platform.research.strategy_edge_train.QuoteStore",
        _FakeLakeQuoteStore,
    )
    funnel: dict[str, object] = {}

    rows = load_candidate_labels(
        database_path=database,
        data_root=tmp_path,
        start_date="2026-08-17",
        end_date="2026-08-17",
        enumerate_from_lake=True,
        funnel=funnel,
    )

    assert rows, json.dumps(funnel, sort_keys=True)
    assert {row["candidate_source"] for row in rows} == {"lake_enumeration"}
    assert {row["strategy_type"] for row in rows} == {"CALL_DEBIT_VERTICAL"}
    assert funnel["empty_candidate_heartbeats"] == 1
    assert funnel["lake_enumeration"]["sampled_unique_utc_minutes"] == 1
    assert funnel["lake_enumeration"]["unique_candidate_geometries_added"] > 0


def test_enumerate_from_lake_off_preserves_empty_heartbeat_behavior(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database = tmp_path / "spx.sqlite"
    _heartbeat_database(database)
    monkeypatch.setattr(
        "spx_spark.data_platform.research.strategy_edge_train.QuoteStore",
        _FakeLakeQuoteStore,
    )
    funnel: dict[str, object] = {}

    rows = load_candidate_labels(
        database_path=database,
        data_root=tmp_path,
        start_date="2026-08-17",
        end_date="2026-08-17",
        funnel=funnel,
    )

    assert rows == []
    assert funnel["empty_candidate_heartbeats"] == 1
    assert "lake_enumeration" not in funnel


def _rows() -> list[dict[str, object]]:
    start = datetime(2026, 6, 20, 14, 0, tzinfo=timezone.utc)
    rows: list[dict[str, object]] = []
    for session_index in range(24):
        session = (start + timedelta(days=session_index)).date().isoformat()
        for candidate_index in range(4):
            features = {name: 0.0 for name in FEATURE_NAMES}
            features["return_5m_atr_directional"] = candidate_index / 10.0
            pnl = 0.35 + candidate_index / 20.0
            rows.append(
                {
                    "session_date": session,
                    "decision_at": (
                        start + timedelta(days=session_index, minutes=candidate_index)
                    ).isoformat(),
                    "model_key": "rth|vertical",
                    "features": features,
                    "policy_pnl_points": pnl,
                    "profit": 1,
                    "stop_first_5m": 0,
                    "max_loss_points": 1.0,
                }
            )
    return rows


def test_walk_forward_artifact_can_promote_a_stable_synthetic_edge() -> None:
    artifact, report = train_edge_artifact(
        _rows(),
        generated_at=datetime(2026, 8, 18, tzinfo=timezone.utc),
        holdout_sessions=4,
        min_train_sessions=5,
        thresholds={
            "min_expected_pnl_points": -10.0,
            "min_expected_pnl_lcb_points": -10.0,
            "min_p_profit": 0.0,
            "max_p_stop_first_5m": 1.0,
            "min_return_on_risk": -10.0,
        },
        promotion_gates={
            "min_oof_trades": 1,
            "min_holdout_trades": 1,
            "min_profit_factor": 0.0,
            "min_average_pnl_points": 0.0,
            "min_positive_session_ratio": 0.0,
            "max_drawdown_r": 99.0,
            "max_top_session_profit_concentration": 1.0,
        },
    )

    model = artifact["models"]["rth|vertical"]
    assert model["promoted"] is True
    assert model["oof_metrics"]["net_pnl_points"] > 0
    assert model["holdout_metrics"]["net_pnl_points"] > 0
    assert len(model["feature_mean"]) == len(FEATURE_NAMES)
    assert report["promoted_models"] == ["rth|vertical"]
