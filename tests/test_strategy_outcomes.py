from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest

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


def _selected_decision(
    *,
    direction: str,
    invalidation_spx: float,
    decision_at: datetime = NOW,
) -> dict[str, object]:
    return {
        "schema_version": "strategy_decision.v1",
        "decision_id": f"strategy:{direction.lower()}-selected",
        "policy_version": "strategy_policy.bootstrap.v1",
        "decision_at": decision_at.isoformat(),
        "available_at": decision_at.isoformat(),
        "session_date": decision_at.date().isoformat(),
        "decision_type": "CALL_DEBIT_VERTICAL",
        "candidate": {
            "candidate_id": f"candidate:{direction.lower()}",
            "strategy_type": "CALL_DEBIT_VERTICAL",
            "direction": direction,
            "opportunity_id": f"strategy-opportunity:{direction.lower()}",
            "invalidation_spx": invalidation_spx,
            "target_spx": 7750.0,
            "long": _leg(7735.0, 4.8, 5.0),
            "short": _leg(7740.0, 2.0, 2.2),
            "quote": {"bid": 2.6, "ask": 2.8},
        },
        "market_facts": {"spot": {"spx": 7741.0}},
        "regime": {"path_state": "TREND", "terminal_state": "TREND_UP"},
        "desk_view": {"reason": "trend_pullback"},
        "why_not": {"reasons": []},
        "execution": {"action": "MANUAL_LIMIT", "automatic_ordering": False},
        "action_authority": "manual",
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


def _write_spx_minutes(
    root: Path, rows: list[dict[str, object]], *, session_date: str = "2026-08-07"
) -> None:
    path = root / "features" / "spx_standardized_samples" / f"date={session_date}" / "events.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def _spx_row(
    minute: datetime,
    *,
    price: float | None = None,
    low: float | None = None,
    high: float | None = None,
    status: str = "selected",
) -> dict[str, object]:
    selected = None
    if status == "selected":
        selected = {
            "price": price,
            "low": low if low is not None else price,
            "high": high if high is not None else price,
            "provider": "schwab",
            "source_at": minute.isoformat(),
        }
    return {
        "minute": minute.isoformat(),
        "observed_at": minute.isoformat(),
        "session_date": minute.date().isoformat(),
        "official_spx_expected": True,
        "status": status,
        "selected": selected,
        "selected_provider": "schwab" if selected else None,
        "provider_diagnostics": [],
        "drop_reasons": [],
        "snapshot_generation": minute.isoformat(),
        "writer_instance_id": "test",
        "synthetic_price": False,
    }


def _window_rows(
    start: datetime,
    *,
    minutes: int = 6,
    base: float = 7741.0,
    invalidation_touch_minute: int | None = None,
    direction: str = "UP",
    invalidation_spx: float | None = None,
) -> list[dict[str, object]]:
    rows = []
    for offset in range(minutes):
        minute = start + timedelta(minutes=offset)
        price = base - offset * 0.1
        low = price - 0.1
        high = price + 0.1
        if (
            invalidation_touch_minute is not None
            and invalidation_spx is not None
            and offset == invalidation_touch_minute
        ):
            if direction == "UP":
                low = invalidation_spx - 0.1
            else:
                high = invalidation_spx + 0.1
        rows.append(_spx_row(minute, price=price, low=low, high=high))
    return rows


def _latest_quotes(sampled_at: datetime, *, complete: bool = True) -> tuple[Quote, ...]:
    quotes: list[Quote] = [
        _quote(InstrumentId.index("SPX"), 7741.9, 7742.1, sampled_at),
        _quote(
            InstrumentId.option(
                "SPX", expiry="20260807", strike=7735, right="C", trading_class="SPXW"
            ),
            4.5,
            4.7,
            sampled_at,
        ),
    ]
    if complete:
        quotes.append(
            _quote(
                InstrumentId.option(
                    "SPX", expiry="20260807", strike=7740, right="C", trading_class="SPXW"
                ),
                2.0,
                2.2,
                sampled_at,
            )
        )
    return tuple(quotes)


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
        data_root=tmp_path,
        horizon_minutes=5,
        database_path=database,
    )

    assert result["observed"] == 1
    assert result["statuses"] == {"observed": 1}
    assert result["horizons"] == [5]
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
    assert attributes["schema_version"] == "strategy_outcome_mark.v2"
    assert attributes["entry_combo_ask"] == 1.8
    assert attributes["exit_combo_bid"] == 0.7
    assert attributes["combo_bid"] == 0.7
    assert attributes["spot_spx"] is not None
    assert attributes["gross_option_pnl"] == -110.0
    assert attributes["net_option_pnl"] is None
    assert attributes["fill_status"] == "not_observed_no_order_capability"

    repeated = observe_due_strategy_outcomes(
        latest,
        now=sampled_at + timedelta(seconds=1),
        data_root=tmp_path,
        horizon_minutes=5,
        database_path=database,
    )
    assert repeated["observed"] == 0


def test_iron_condor_outcome_uses_entry_credit_minus_close_liability(
    tmp_path: Path,
) -> None:
    database = _migrate(tmp_path)

    def leg(strike: float, right: str, bid: float, ask: float) -> dict[str, object]:
        return {
            "contract_id": f"option:SPX:SPXW:20260807:{strike:g}:{right}",
            "strike": strike,
            "right": right,
            "provider": "schwab",
            "bid": bid,
            "ask": ask,
            "source_at": (NOW - timedelta(seconds=1)).isoformat(),
        }

    entry_legs = [
        leg(7680.0, "P", 0.4, 0.5),
        leg(7690.0, "P", 1.6, 1.7),
        leg(7790.0, "C", 1.6, 1.7),
        leg(7800.0, "C", 0.4, 0.5),
    ]
    decision = {
        "schema_version": "strategy_decision.v2",
        "decision_id": "strategy:iron-condor-outcome",
        "policy_version": "strategy_policy.bootstrap.v46",
        "decision_at": NOW.isoformat(),
        "available_at": NOW.isoformat(),
        "session_date": "2026-08-07",
        "decision_type": "IRON_CONDOR",
        "candidate": {
            "strategy_type": "IRON_CONDOR",
            "setup_kind": "IRON_CONDOR_DELTA",
            "direction": "NEUTRAL",
            "opportunity_id": "strategy-opportunity:iron-condor-outcome",
            "invalidation_spx": [7690.0, 7790.0],
            "target_spx": 7740.0,
            "legs": entry_legs,
            "quote": {"credit": 2.2, "bid": 2.2, "ask": 2.6},
        },
        "market_facts": {"spot": {"spx": 7740.0}},
        "regime": {"path_state": "BALANCED", "terminal_state": "NONE"},
        "desk_view": {"reason": "IRON_CONDOR_DELTA"},
        "why_not": {"reasons": []},
        "execution": {"action": "MANUAL_LIMIT", "automatic_ordering": False},
        "action_authority": "manual",
    }
    persist_strategy_decision(decision, database_path=database)
    sampled_at = NOW + timedelta(minutes=5, seconds=1)
    quotes = (
        _quote(InstrumentId.index("SPX"), 7739.9, 7740.1, sampled_at),
        _quote(InstrumentId.option("SPX", expiry="20260807", strike=7680, right="P", trading_class="SPXW"), 0.3, 0.4, sampled_at),
        _quote(InstrumentId.option("SPX", expiry="20260807", strike=7690, right="P", trading_class="SPXW"), 0.9, 1.0, sampled_at),
        _quote(InstrumentId.option("SPX", expiry="20260807", strike=7790, right="C", trading_class="SPXW"), 0.9, 1.0, sampled_at),
        _quote(InstrumentId.option("SPX", expiry="20260807", strike=7800, right="C", trading_class="SPXW"), 0.3, 0.4, sampled_at),
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
        data_root=tmp_path,
        horizon_minutes=5,
        database_path=database,
    )

    assert result["statuses"] == {"observed": 1}
    with sqlite3.connect(database) as connection:
        option_return, attributes_json = connection.execute(
            "SELECT option_return_bps, attributes_json FROM outcomes"
        ).fetchone()
    attributes = json.loads(attributes_json)
    assert option_return > 0
    assert attributes["entry_combo_ask"] is None
    assert attributes["entry_combo_credit"] == 2.2
    assert attributes["exit_combo_liability"] == 1.4
    assert attributes["gross_option_pnl"] == 80.0


def test_multi_horizon_marks_persist_independently(tmp_path: Path) -> None:
    database = _migrate(tmp_path)
    persist_strategy_decision(_decision(), database_path=database)
    sampled_at = NOW + timedelta(minutes=3, seconds=2)
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
        data_root=tmp_path,
        horizon_minutes=(1, 2, 3, 5),
        database_path=database,
    )

    assert result["observed"] == 3
    assert result["statuses"] == {"censored": 1, "observed": 2}
    assert set(result["horizons"]) == {1, 2, 3, 5}
    with sqlite3.connect(database) as connection:
        horizons = {
            row[0]
            for row in connection.execute(
                "SELECT horizon_minutes FROM outcomes ORDER BY horizon_minutes"
            )
        }
    # Horizon 1 now lands as explicit service_gap censor; 5m is not yet due.
    assert horizons == {1, 2, 3}


@pytest.mark.parametrize(
    ("direction", "invalidation_spx", "rows", "expected_breached", "expected_breach_at"),
    [
        (
            "UP",
            7740.0,
            [
                _spx_row(NOW, price=7741.0, low=7741.0, high=7741.2),
                _spx_row(
                    NOW + timedelta(minutes=1),
                    price=7739.8,
                    low=7739.7,
                    high=7740.2,
                ),
            ],
            True,
            (NOW + timedelta(minutes=1)).replace(second=0, microsecond=0).isoformat(),
        ),
        (
            "DOWN",
            7742.0,
            [
                _spx_row(NOW, price=7741.0, low=7740.9, high=7741.1),
                _spx_row(
                    NOW + timedelta(minutes=2),
                    price=7742.4,
                    low=7741.8,
                    high=7742.5,
                ),
            ],
            True,
            (NOW + timedelta(minutes=2)).replace(second=0, microsecond=0).isoformat(),
        ),
        (
            "UP",
            7735.0,
            [
                _spx_row(NOW, price=7741.0, low=7740.8, high=7741.1),
                _spx_row(
                    NOW + timedelta(minutes=1),
                    price=7740.5,
                    low=7740.3,
                    high=7740.8,
                ),
                _spx_row(
                    NOW + timedelta(minutes=2),
                    price=7740.4,
                    low=7740.2,
                    high=7740.6,
                ),
                _spx_row(
                    NOW + timedelta(minutes=3),
                    price=7740.3,
                    low=7740.1,
                    high=7740.5,
                ),
                _spx_row(
                    NOW + timedelta(minutes=4),
                    price=7740.2,
                    low=7740.0,
                    high=7740.4,
                ),
                _spx_row(
                    NOW + timedelta(minutes=5),
                    price=7740.1,
                    low=7739.9,
                    high=7740.3,
                ),
            ],
            False,
            None,
        ),
    ],
)
def test_structural_invalidation_labels_follow_spx_minute_samples(
    tmp_path: Path,
    direction: str,
    invalidation_spx: float,
    rows: list[dict[str, object]],
    expected_breached: bool,
    expected_breach_at: str | None,
) -> None:
    database = _migrate(tmp_path)
    persist_strategy_decision(
        _selected_decision(direction=direction, invalidation_spx=invalidation_spx),
        database_path=database,
    )
    _write_spx_minutes(tmp_path, rows)
    sampled_at = NOW + timedelta(minutes=5, seconds=3)
    quotes = (
        _quote(InstrumentId.index("SPX"), 7741.9, 7742.1, sampled_at),
        _quote(
            InstrumentId.option(
                "SPX", expiry="20260807", strike=7735, right="C", trading_class="SPXW"
            ),
            4.5,
            4.7,
            sampled_at,
        ),
        _quote(
            InstrumentId.option(
                "SPX", expiry="20260807", strike=7740, right="C", trading_class="SPXW"
            ),
            2.0,
            2.2,
            sampled_at,
        ),
    )
    latest = LatestState(
        created_at=sampled_at,
        as_of=sampled_at,
        quotes=quotes,
        best_quotes=quotes,
    )

    observe_due_strategy_outcomes(
        latest,
        now=sampled_at,
        data_root=tmp_path,
        horizon_minutes=5,
        database_path=database,
    )

    with sqlite3.connect(database) as connection:
        attributes = json.loads(
            connection.execute("SELECT attributes_json FROM outcomes").fetchone()[0]
        )
    assert attributes["invalidation_breached"] is expected_breached
    assert attributes["breach_at"] == expected_breach_at
    assert attributes["label_kind"] == (
        "structural_exit" if expected_breached else "horizon_mark"
    )
    assert attributes["breach_scan_gap"] is False


def test_structural_invalidation_marks_gap_as_unknown_after_two_missing_minutes(
    tmp_path: Path,
) -> None:
    database = _migrate(tmp_path)
    persist_strategy_decision(
        _selected_decision(direction="UP", invalidation_spx=7740.0),
        database_path=database,
    )
    _write_spx_minutes(
        tmp_path,
        [
            _spx_row(NOW, price=7741.0),
            _spx_row(NOW + timedelta(minutes=4), price=7739.8),
        ],
    )
    sampled_at = NOW + timedelta(minutes=5, seconds=3)
    quotes = (
        _quote(InstrumentId.index("SPX"), 7741.9, 7742.1, sampled_at),
        _quote(
            InstrumentId.option(
                "SPX", expiry="20260807", strike=7735, right="C", trading_class="SPXW"
            ),
            4.5,
            4.7,
            sampled_at,
        ),
        _quote(
            InstrumentId.option(
                "SPX", expiry="20260807", strike=7740, right="C", trading_class="SPXW"
            ),
            2.0,
            2.2,
            sampled_at,
        ),
    )
    latest = LatestState(
        created_at=sampled_at,
        as_of=sampled_at,
        quotes=quotes,
        best_quotes=quotes,
    )

    observe_due_strategy_outcomes(
        latest,
        now=sampled_at,
        data_root=tmp_path,
        horizon_minutes=5,
        database_path=database,
    )

    with sqlite3.connect(database) as connection:
        attributes = json.loads(
            connection.execute("SELECT attributes_json FROM outcomes").fetchone()[0]
        )
    assert attributes["invalidation_breached"] is None
    assert attributes["breach_at"] is None
    assert attributes["breach_scan_gap"] is True
    assert attributes["label_kind"] == "horizon_mark"


def test_service_gap_becomes_explicit_censored_label_and_stays_idempotent(
    tmp_path: Path,
) -> None:
    database = _migrate(tmp_path)
    persist_strategy_decision(
        _selected_decision(direction="UP", invalidation_spx=7730.0),
        database_path=database,
    )
    _write_spx_minutes(tmp_path, _window_rows(NOW, invalidation_spx=7730.0))
    sampled_at = NOW + timedelta(minutes=5, seconds=95)
    latest = LatestState(
        created_at=sampled_at,
        as_of=sampled_at,
        quotes=_latest_quotes(sampled_at),
        best_quotes=_latest_quotes(sampled_at),
    )

    result = observe_due_strategy_outcomes(
        latest,
        now=sampled_at,
        data_root=tmp_path,
        horizon_minutes=5,
        database_path=database,
    )

    assert result["statuses"] == {"censored": 1}
    with sqlite3.connect(database) as connection:
        row = connection.execute(
            "SELECT status, spx_return_bps, option_return_bps, attributes_json FROM outcomes"
        ).fetchone()
    attributes = json.loads(row[3])
    assert row[0] == "censored"
    assert row[1] is None
    assert row[2] is None
    assert attributes["censor_kind"] == "service_gap"

    repeated = observe_due_strategy_outcomes(
        latest,
        now=sampled_at + timedelta(seconds=1),
        data_root=tmp_path,
        horizon_minutes=5,
        database_path=database,
    )
    assert repeated["observed"] == 0


def test_session_end_before_horizon_becomes_censored_label(tmp_path: Path) -> None:
    database = _migrate(tmp_path)
    decision_at = datetime(2026, 8, 7, 19, 55, tzinfo=timezone.utc)
    persist_strategy_decision(
        _selected_decision(
            direction="UP",
            invalidation_spx=7730.0,
            decision_at=decision_at,
        ),
        database_path=database,
    )
    _write_spx_minutes(
        tmp_path,
        _window_rows(decision_at, minutes=6, invalidation_spx=7730.0),
        session_date="2026-08-07",
    )
    sampled_at = datetime(2026, 8, 7, 20, 1, tzinfo=timezone.utc)
    latest = LatestState(
        created_at=sampled_at,
        as_of=sampled_at,
        quotes=_latest_quotes(sampled_at),
        best_quotes=_latest_quotes(sampled_at),
    )

    result = observe_due_strategy_outcomes(
        latest,
        now=sampled_at,
        data_root=tmp_path,
        horizon_minutes=10,
        database_path=database,
    )

    assert result["statuses"] == {"censored": 1}
    with sqlite3.connect(database) as connection:
        attributes = json.loads(
            connection.execute("SELECT attributes_json FROM outcomes").fetchone()[0]
        )
    assert attributes["censor_kind"] == "session_end_before_horizon"


def test_breach_quote_unavailable_overrides_horizon_mark(tmp_path: Path) -> None:
    database = _migrate(tmp_path)
    persist_strategy_decision(
        _selected_decision(direction="UP", invalidation_spx=7740.0),
        database_path=database,
    )
    _write_spx_minutes(
        tmp_path,
        _window_rows(
            NOW,
            invalidation_touch_minute=1,
            direction="UP",
            invalidation_spx=7740.0,
        ),
    )
    sampled_at = NOW + timedelta(minutes=5, seconds=3)
    latest = LatestState(
        created_at=sampled_at,
        as_of=sampled_at,
        quotes=_latest_quotes(sampled_at),
        best_quotes=_latest_quotes(sampled_at),
    )

    result = observe_due_strategy_outcomes(
        latest,
        now=sampled_at,
        data_root=tmp_path,
        horizon_minutes=5,
        database_path=database,
    )

    assert result["statuses"] == {"censored": 1}
    with sqlite3.connect(database) as connection:
        row = connection.execute(
            "SELECT status, spx_return_bps, option_return_bps, attributes_json FROM outcomes"
        ).fetchone()
    attributes = json.loads(row[3])
    assert row[0] == "censored"
    assert row[1] is None
    assert row[2] is None
    assert attributes["censor_kind"] == "breach_quote_unavailable"
    assert attributes["label_kind"] == "structural_exit"
    assert attributes["breach_at"] == (NOW + timedelta(minutes=1)).replace(
        second=0, microsecond=0
    ).isoformat()


def test_quote_gap_becomes_explicit_censored_label(tmp_path: Path) -> None:
    database = _migrate(tmp_path)
    persist_strategy_decision(
        _selected_decision(direction="UP", invalidation_spx=7730.0),
        database_path=database,
    )
    _write_spx_minutes(tmp_path, _window_rows(NOW, invalidation_spx=7730.0))
    sampled_at = NOW + timedelta(minutes=5, seconds=3)
    quotes = _latest_quotes(sampled_at, complete=False)
    latest = LatestState(
        created_at=sampled_at,
        as_of=sampled_at,
        quotes=quotes,
        best_quotes=quotes,
    )

    result = observe_due_strategy_outcomes(
        latest,
        now=sampled_at,
        data_root=tmp_path,
        horizon_minutes=5,
        database_path=database,
    )

    assert result["statuses"] == {"censored": 1}
    with sqlite3.connect(database) as connection:
        row = connection.execute(
            "SELECT status, spx_return_bps, option_return_bps, attributes_json FROM outcomes"
        ).fetchone()
    attributes = json.loads(row[3])
    assert row[0] == "censored"
    assert row[1] is None
    assert row[2] is None
    assert attributes["censor_kind"] == "quote_gap"


def test_neutral_confirmation_butterfly_uses_thesis_direction_for_single_stop(
    tmp_path: Path,
) -> None:
    """direction=NEUTRAL + single stop must not fire breach on every bar."""

    database = _migrate(tmp_path)
    decision = _selected_decision(direction="UP", invalidation_spx=7730.0)
    decision["decision_id"] = "strategy:neutral-confirmation"
    decision["decision_type"] = "CALL_BUTTERFLY"
    decision["candidate"] = {
        **decision["candidate"],
        "strategy_type": "CALL_BUTTERFLY",
        "direction": "NEUTRAL",
        "thesis_direction": "UP",
        "invalidation_spx": 7730.0,
        "legs": [
            _leg(7725.0, 6.0, 6.2),
            _leg(7735.0, 3.0, 3.2),
            _leg(7745.0, 1.0, 1.2),
        ],
    }
    persist_strategy_decision(decision, database_path=database)
    # Price stays above the stop the whole window — must NOT breach.
    rows = [
        {
            "minute": (NOW + timedelta(minutes=offset)).isoformat(),
            "status": "selected",
            "selected": {
                "low": 7735.0 + offset,
                "high": 7736.0 + offset,
                "price": 7735.5 + offset,
            },
        }
        for offset in range(6)
    ]
    _write_spx_minutes(tmp_path, rows)
    sampled_at = NOW + timedelta(minutes=5, seconds=3)
    latest = LatestState(
        created_at=sampled_at,
        as_of=sampled_at,
        quotes=_latest_quotes(sampled_at),
        best_quotes=_latest_quotes(sampled_at),
    )
    observe_due_strategy_outcomes(
        latest,
        now=sampled_at,
        data_root=tmp_path,
        horizon_minutes=5,
        database_path=database,
    )
    with sqlite3.connect(database) as connection:
        attributes = json.loads(
            connection.execute("SELECT attributes_json FROM outcomes").fetchone()[0]
        )
    assert attributes["invalidation_breached"] is False
    assert attributes["label_kind"] == "horizon_mark"
