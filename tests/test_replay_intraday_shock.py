from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path


UTC = timezone.utc
ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "replay-intraday-shock.py"


def quote_row(
    instrument_id: str,
    *,
    received_at: datetime,
    source_at: datetime,
    price: float,
    provider: str = "ibkr",
    quality: str = "live",
    market_data_type: int = 1,
) -> dict[str, object]:
    return {
        "instrument_id": instrument_id,
        "provider": provider,
        "quality": quality,
        "market_data_type": market_data_type,
        "effective_price": price,
        "received_at": received_at.isoformat(),
        "quote_time": source_at.isoformat(),
    }


def add_pair(
    rows: list[dict[str, object]],
    *,
    received_at: datetime,
    spx_source_at: datetime,
    es_source_at: datetime,
    spx: float,
    es: float,
    es_quality: str = "live",
) -> None:
    rows.extend(
        (
            quote_row(
                "index:SPX",
                received_at=received_at,
                source_at=spx_source_at,
                price=spx,
            ),
            quote_row(
                "future:ES",
                received_at=received_at,
                source_at=es_source_at,
                price=es,
                quality=es_quality,
                market_data_type=1 if es_quality == "live" else 3,
            ),
        )
    )


def run_replay(tmp_path: Path, rows: list[dict[str, object]], *args: str) -> dict[str, object]:
    source = tmp_path / "quotes.jsonl"
    source.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )
    env = os.environ.copy()
    env.update(
        {
            "ALERT_INTRADAY_SHOCK_SPX_MAX_AGE_SECONDS": "15",
            "ALERT_INTRADAY_SHOCK_ES_MAX_AGE_SECONDS": "10",
            "ALERT_INTRADAY_SHOCK_MAX_ANCHOR_SKEW_SECONDS": "5",
        }
    )
    result = subprocess.run(
        [sys.executable, str(SCRIPT), str(source), *args],
        cwd=ROOT,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)
    assert isinstance(payload, dict)
    return payload


def test_replay_applies_live_provider_freshness_and_skew_gates(tmp_path: Path) -> None:
    start = datetime(2026, 7, 10, 14, 0, tzinfo=UTC)
    rows: list[dict[str, object]] = []
    add_pair(
        rows,
        received_at=start,
        spx_source_at=start,
        es_source_at=start,
        spx=7500.0,
        es=7550.0,
    )
    delayed_at = start + timedelta(seconds=5)
    add_pair(
        rows,
        received_at=delayed_at,
        spx_source_at=delayed_at,
        es_source_at=delayed_at,
        spx=7499.0,
        es=7549.0,
        es_quality="delayed",
    )
    stale_at = start + timedelta(seconds=10)
    add_pair(
        rows,
        received_at=stale_at,
        spx_source_at=stale_at - timedelta(seconds=16),
        es_source_at=stale_at,
        spx=7498.0,
        es=7548.0,
    )
    skewed_at = start + timedelta(seconds=20)
    add_pair(
        rows,
        received_at=skewed_at,
        spx_source_at=skewed_at - timedelta(seconds=6),
        es_source_at=skewed_at,
        spx=7497.0,
        es=7547.0,
    )

    payload = run_replay(tmp_path, rows, "--start", start.isoformat())

    assert payload["sample_count"] == 1
    assert payload["processed_sample_count"] == 1
    assert payload["events"] == []


def test_replay_keeps_pre_roll_before_start_for_shock_baseline(tmp_path: Path) -> None:
    start = datetime(2026, 7, 10, 14, 0, tzinfo=UTC)
    anchor_at = start - timedelta(seconds=10)
    shock_at = start + timedelta(seconds=10)
    rows: list[dict[str, object]] = []
    add_pair(
        rows,
        received_at=anchor_at,
        spx_source_at=anchor_at,
        es_source_at=anchor_at,
        spx=7500.0,
        es=7550.0,
    )
    add_pair(
        rows,
        received_at=shock_at,
        spx_source_at=shock_at,
        es_source_at=shock_at,
        spx=7480.0,
        es=7528.0,
    )

    payload = run_replay(tmp_path, rows, "--start", start.isoformat())

    assert payload["sample_count"] == 1
    assert payload["processed_sample_count"] == 2
    assert [event["kind"] for event in payload["events"]] == ["intraday_price_shock"]


def test_replay_uses_source_time_for_horizon_not_received_time(tmp_path: Path) -> None:
    first_received = datetime(2026, 7, 10, 14, 0, 10, tzinfo=UTC)
    first_source = first_received - timedelta(seconds=10)
    second_at = first_received + timedelta(seconds=60)
    rows: list[dict[str, object]] = []
    add_pair(
        rows,
        received_at=first_received,
        spx_source_at=first_source,
        es_source_at=first_source,
        spx=7500.0,
        es=7550.0,
    )
    add_pair(
        rows,
        received_at=second_at,
        spx_source_at=second_at,
        es_source_at=second_at,
        spx=7481.25,
        es=7531.125,
    )

    payload = run_replay(
        tmp_path,
        rows,
        "--start",
        first_source.isoformat(),
    )

    # Receipt timestamps are 60s apart, but source timestamps are 70s apart.
    # The 25bps move therefore misses the 1m horizon and the 35bps 3m gate.
    assert payload["sample_count"] == 2
    assert payload["events"] == []


def test_replay_pairs_latest_live_quotes_with_different_receipt_times(tmp_path: Path) -> None:
    at = datetime(2026, 7, 10, 14, 0, tzinfo=UTC)
    rows = [
        quote_row(
            "index:SPX",
            received_at=at,
            source_at=at,
            price=7500.0,
        ),
        quote_row(
            "future:ES",
            received_at=at + timedelta(seconds=1),
            source_at=at + timedelta(seconds=1),
            price=7550.0,
        ),
    ]

    payload = run_replay(tmp_path, rows, "--start", at.isoformat())

    assert payload["sample_count"] == 1
    assert payload["events"] == []


def test_replay_rejects_samples_outside_spx_rth(tmp_path: Path) -> None:
    at = datetime(2026, 7, 10, 2, 0, tzinfo=UTC)
    rows: list[dict[str, object]] = []
    add_pair(
        rows,
        received_at=at,
        spx_source_at=at,
        es_source_at=at,
        spx=7500.0,
        es=7550.0,
    )
    add_pair(
        rows,
        received_at=at + timedelta(seconds=30),
        spx_source_at=at + timedelta(seconds=30),
        es_source_at=at + timedelta(seconds=30),
        spx=7480.0,
        es=7528.0,
    )

    payload = run_replay(tmp_path, rows, "--start", at.isoformat())

    assert payload["sample_count"] == 0
    assert payload["events"] == []
