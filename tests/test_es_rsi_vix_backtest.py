from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import numpy as np

from spx_spark.data_platform.research.es_rsi_vix_data import load_outbox_sessions
from spx_spark.data_platform.research.es_rsi_vix_indicators import (
    cutler_rsi,
    signal_events,
    wilder_rsi,
)


def test_rsi_reaches_expected_extremes_for_one_way_prices() -> None:
    rising = np.arange(30, dtype=float)
    falling = rising[::-1]

    assert wilder_rsi(rising, 14)[-1] == 100.0
    assert cutler_rsi(rising, 14)[-1] == 100.0
    assert wilder_rsi(falling, 14)[-1] == 0.0
    assert cutler_rsi(falling, 14)[-1] == 0.0


def test_wilder_rsi_restarts_after_a_data_gap() -> None:
    prices = np.concatenate([np.arange(20, dtype=float), [np.nan], np.arange(20, dtype=float)])
    result = wilder_rsi(prices, 14)

    assert np.isnan(result[20:35]).all()
    assert result[-1] == 100.0


def test_signal_events_require_persistence_and_rearm() -> None:
    direction = np.array([0, 1, 1, 1, 0, 0, -1, -1, 0, 0, 1, 1], dtype=np.int8)

    assert signal_events(direction, cooldown_minutes=1) == [(2, 1), (7, -1), (11, 1)]


def _payload(es: float, vix: float, vix1d: float) -> str:
    entries = []
    for instrument_id, price in (
        ("future:ES", es),
        ("index:VIX", vix),
        ("index:VIX1D", vix1d),
    ):
        entries.append(
            {
                "instrument_id": instrument_id,
                "price": price,
                "freshness": "fresh",
                "research_usable": True,
            }
        )
    return json.dumps({"market_context": {"entries": entries}})


def test_load_outbox_sessions_uses_fresh_rth_minutes(tmp_path: Path) -> None:
    database = tmp_path / "outbox.sqlite"
    connection = sqlite3.connect(database)
    connection.execute(
        "CREATE TABLE domain_event_outbox (source_at TEXT, payload_json TEXT)"
    )
    connection.executemany(
        "INSERT INTO domain_event_outbox VALUES (?, ?)",
        [
            ("2026-07-15T13:30:20+00:00", _payload(7600, 16, 9)),
            ("2026-07-15T13:31:20+00:00", _payload(7601, 15.9, 8.9)),
        ],
    )
    connection.commit()
    connection.close()

    sessions, coverage = load_outbox_sessions(database)

    assert len(sessions) == 1
    assert coverage[0].observed_es_minutes == 2
    assert coverage[0].usable_es_minutes == 4
    assert sessions[0].es[:5].tolist()[:4] == [7600.0, 7601.0, 7601.0, 7601.0]
