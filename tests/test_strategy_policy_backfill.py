from __future__ import annotations

from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import subprocess
import sys
from dataclasses import asdict, replace
from types import SimpleNamespace
import pytest

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
        "policy_version": "management_policy.v2",
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


def test_combo_marks_do_not_hide_a_missing_leg_behind_other_leg_updates() -> None:
    from types import SimpleNamespace
    from spx_spark.data_platform.research.strategy_policy_backfill import _combo_bid_marks

    start = datetime(2026, 8, 7, 15, 0, tzinfo=timezone.utc)
    def series(**kwargs):
        offsets = (0, 30, 60, 90, 120) if kwargs["strike"] == 7700 else (0,)
        return [SimpleNamespace(at=start + timedelta(seconds=i), source_at=start + timedelta(seconds=i), bid=2.0, ask=2.1) for i in offsets]
    legs = [{"expiry": "20260807", "strike": strike, "right": "C", "quantity": qty}
            for strike, qty in ((7700, 1), (7715, -1))]
    marks = _combo_bid_marks(SimpleNamespace(option_series=series), legs=legs, provider="schwab", start=start, end=start + timedelta(minutes=2))
    assert marks
    assert marks[-1].at == start


def test_credit_backfill_counts_contracts_and_triggers_real_three_credit_stop():
    from types import SimpleNamespace
    from spx_spark.data_platform.research.odte_level_signals import OptionTick
    from spx_spark.data_platform.research.strategy_policy_backfill import _label_decision
    # Four quote rows omit quantities, as in the actual 2026-09-04 persisted IC.
    legs = [{"expiry": "20260807", "strike": strike, "right": right,
             "provider": "schwab", "bid": price, "ask": price}
            for strike, right, price in ((7680, "P", 1), (7690, "P", 2.25),
                                          (7740, "C", 2.25), (7750, "C", 1))]
    def series(**kwargs):
        prices = (1, 1) if kwargs["strike"] in (7680, 7750) else (2.25, 4.75)
        return [OptionTick(NOW + timedelta(seconds=i * 5), price, price, price,
                           NOW + timedelta(seconds=i * 5)) for i, price in enumerate(prices)]
    decision = {**_decision(), "candidate": {"strategy_type": "IRON_CONDOR", "session_mode": "rth", "legs": legs}}
    label = _label_decision(decision, store=SimpleNamespace(option_series=series), lookforward_minutes=20)
    assert label["entry_ask"] == 2.5
    assert label["contract_count"] == 4
    assert label["label_status"] == "COMPLETE_EXIT"
    assert label["exit_reason"] == "stop_loss"
    assert label["policy_pnl_points"] == -5.1056


def test_received_updates_do_not_rejuvenate_old_source_quotes():
    from types import SimpleNamespace
    from spx_spark.data_platform.research.odte_level_signals import OptionTick
    from spx_spark.data_platform.research.strategy_policy_backfill import _combo_bid_marks
    legs = [{"expiry": "20260807", "strike": strike, "right": "C", "quantity": qty}
            for strike, qty in ((7700, 1), (7715, -1))]
    def series(**kwargs):
        return [OptionTick(NOW + timedelta(seconds=i), 2, 2.1, 2.05,
                           NOW if kwargs["strike"] == 7715 else NOW + timedelta(seconds=i))
                for i in range(0, 601, 5)]
    marks = _combo_bid_marks(SimpleNamespace(option_series=series), legs=legs, provider="schwab",
                            start=NOW, end=NOW + timedelta(minutes=10))
    assert len(marks) == 1  # the other source is already five seconds away on update two


def test_incomplete_labels_keep_the_research_denominator():
    from types import SimpleNamespace
    from spx_spark.data_platform.research.odte_level_signals import OptionTick
    from spx_spark.data_platform.research.strategy_policy_backfill import _label_decision
    def series(**kwargs):
        value = 8.0 if kwargs["strike"] == 7730 else 3.0
        return [OptionTick(NOW, value, value, value, NOW)]
    row = _label_decision(_decision(), store=SimpleNamespace(option_series=series), lookforward_minutes=20)
    assert row["label_status"] == "CENSORED"
    assert row["policy_pnl_points"] is None


def test_preentry_fresh_quote_can_seed_a_mark_but_late_arrival_cannot():
    from spx_spark.data_platform.research.odte_level_signals import OptionTick
    from spx_spark.data_platform.research.strategy_policy_backfill import _candidate_legs, _combo_bid_marks
    legs = _candidate_legs(_decision()["candidate"])
    def series(**query):
        received = NOW - timedelta(seconds=1) if query["strike"] == 7730 else NOW + timedelta(seconds=5)
        return [OptionTick(received, 8 if query["strike"] == 7730 else 3, 8.2 if query["strike"] == 7730 else 3.2,
                           None, NOW - timedelta(seconds=2))]
    marks = _combo_bid_marks(SimpleNamespace(option_series=series), legs=legs, provider="schwab",
                            start=NOW, end=NOW + timedelta(seconds=10))
    assert [mark.at for mark in marks] == [NOW + timedelta(seconds=5)]
    assert marks[0].combo_bid == pytest.approx(4.8)


def test_negative_executable_liquidation_is_a_cash_cost_not_a_zero_exit():
    from spx_spark.data_platform.research.odte_level_signals import OptionTick
    from spx_spark.data_platform.research.strategy_policy_backfill import _label_decision
    def series(**query):
        bid, ask = (3, 3.1) if query["strike"] == 7730 else (3.1, 3.2)
        return [OptionTick(NOW, bid, ask, None, NOW)]
    label = _label_decision(_decision(), store=SimpleNamespace(option_series=series), lookforward_minutes=20)
    assert label["exit_bid"] == -0.2
    assert label["policy_pnl_points"] == pytest.approx(-5.3 - 0.2 - 0.0528)


def test_complete_management_contract_cannot_be_overwritten_by_a_legacy_version():
    from spx_spark.analytics.options.strategy_payoff import DEFAULT_MANAGEMENT_POLICY
    from spx_spark.data_platform.research.strategy_policy_backfill import _label_decision
    decision = _decision()
    decision["management_contract"] = asdict(DEFAULT_MANAGEMENT_POLICY)
    decision["candidate"]["management_plan"] = {"policy_version": "management_policy.v1"}
    assert _label_decision(decision, store=None, lookforward_minutes=20)["label_status"] == "POLICY_MISMATCH"


def test_notification_replay_starts_after_receipt_and_does_not_search_for_a_cheaper_entry():
    from spx_spark.analytics.options.strategy_payoff import DEFAULT_MANAGEMENT_POLICY
    from spx_spark.data_platform.research.odte_level_signals import OptionTick
    from spx_spark.data_platform.research.strategy_decision_replay import audit_strategy_pushes
    decision = _decision()
    decision["management_contract"] = asdict(replace(DEFAULT_MANAGEMENT_POLICY, time_stop_minutes=1))
    decision["candidate"].update({"quote": {"ask": 5.3}, "opportunity_valid_until": (NOW + timedelta(minutes=5)).isoformat()})
    def series(**query):
        ticks = []
        for seconds in range(-5, 211, 5):
            stamp = NOW + timedelta(seconds=seconds)
            bid, ask = ((5.0, 8.4) if seconds < 30 else (9.0, 9.1) if seconds < 45 else (7.0, 7.1)) if query["strike"] == 7730 else (3.1, 3.2)
            if query["start"] <= stamp <= query["end"]:
                ticks.append(OptionTick(stamp, bid, ask, None, stamp))
        return ticks
    receipt = {"lane": "trade_ready", "status": "delivered", "delivered_at": (NOW + timedelta(seconds=30)).isoformat(),
               "envelope": {"event_id": decision["candidate"]["opportunity_id"] + ":ready", "occurred_at": NOW.isoformat()}}
    report = audit_strategy_pushes([decision], [receipt], store=SimpleNamespace(option_series=series), repository=Path.cwd())
    row = report["rows"][0]
    assert row["exit_before_delivery"] is True
    immediate, later = row["delivery_entry_scenarios"][:2]
    assert immediate["label_status"] == "ENTRY_LIMIT_UNAVAILABLE"
    assert later["label_status"] == "COMPLETE_EXIT"
    assert later["entry_at"] == (NOW + timedelta(seconds=45)).isoformat()
    assert later["exit_at"] == (NOW + timedelta(seconds=105)).isoformat()
    assert later["policy_pnl_points"] == pytest.approx(-1.5528)
    assert later["fill_status"] == "UNKNOWN"


def test_frozen_replay_refuses_a_modified_input(tmp_path):
    import hashlib
    import json
    from spx_spark.data_platform.research.strategy_decision_replay import main
    source = tmp_path / "source"
    source.mkdir()
    (source / "selected-decisions.json").write_text('[{"changed": true}]')
    (source / "manifest.json").write_text(json.dumps({
        "inputs": {"selected-decisions": hashlib.sha256(b"[]").hexdigest()},
        "quote_snapshot_sha256": "unused",
    }))
    with pytest.raises(SystemExit) as error:
        main(["--snapshot-root", str(source), "--output-root", str(tmp_path / "result")])
    assert error.value.code == 2
    assert not (tmp_path / "result" / "push-audit.json").exists()
