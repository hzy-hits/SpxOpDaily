from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from spx_spark.application.runtime.spx_minute_sampler import (
    canonical_state_path,
    sample_spx_minute_once,
)
from spx_spark.config import StorageSettings
from spx_spark.marketdata import (
    InstrumentId,
    MarketDataQuality,
    Provider,
    Quote,
)
from spx_spark.settings.market_features import MarketFeatureSettings
from spx_spark.storage import LatestState


UTC = timezone.utc


def _storage(tmp_path: Path) -> StorageSettings:
    return StorageSettings(
        data_root=str(tmp_path),
        latest_state_path=str(tmp_path / "latest" / "state.json"),
        raw_file_name="quotes.jsonl",
        include_raw_payload=False,
        latest_stale_after_seconds=90.0,
        slow_index_stale_after_seconds=180.0,
        slow_index_labels=frozenset(),
    )


def _quote(provider: Provider, at: datetime, price: float) -> Quote:
    return Quote(
        instrument=InstrumentId.index("SPX"),
        provider=provider,
        provider_symbol="$SPX" if provider is Provider.SCHWAB else "SPX",
        received_at=at,
        last_update_at=at,
        quote_time=at,
        quality=MarketDataQuality.LIVE,
        bid=price - 0.1,
        ask=price + 0.1,
        close=price - 20.0,
    )


def _latest(at: datetime, *quotes: Quote) -> LatestState:
    return LatestState(
        created_at=at,
        as_of=at,
        quotes=tuple(quotes),
        best_quotes=tuple(quotes),
    )


def test_sampler_persists_selected_and_missing_rth_minutes_with_drop_evidence(
    tmp_path: Path,
) -> None:
    storage = _storage(tmp_path)
    start = datetime(2026, 7, 31, 14, 30, tzinfo=UTC)
    quotes = (
        _quote(Provider.SCHWAB, start, 7400.0),
        _quote(Provider.IBKR, start, 7400.2),
    )

    selected = sample_spx_minute_once(
        storage=storage,
        policy=MarketFeatureSettings(),
        now=start,
        latest_state=_latest(start, *quotes),
        writer_instance_id="writer-a",
    )
    missing_at = start + timedelta(minutes=2)
    missing = sample_spx_minute_once(
        storage=storage,
        policy=MarketFeatureSettings(),
        now=missing_at,
        latest_state=_latest(missing_at, *quotes),
        writer_instance_id="writer-a",
    )
    payload = json.loads(canonical_state_path(storage).read_text(encoding="utf-8"))
    rows = {row["minute"]: row for row in payload["rows"] if row["session_date"] == "2026-07-31"}

    assert selected["accepted"] is True
    assert missing["accepted"] is False
    assert rows[start.isoformat()]["selected_provider"] == "schwab"
    assert rows[(start + timedelta(minutes=1)).isoformat()]["drop_reasons"] == [
        "sampler_process_gap"
    ]
    current = rows[missing_at.isoformat()]
    assert current["status"] == "missing"
    assert {item["provider"] for item in current["provider_diagnostics"]} == {
        "schwab",
        "ibkr",
    }
    assert {item["drop_reason"] for item in current["provider_diagnostics"]} == {"source_stale"}
    assert current["snapshot_generation"] == missing_at.isoformat()
    assert payload["coverage"]["persisted_minutes"] == 63
    assert payload["coverage"]["selected_minutes"] == 1
    assert payload["coverage"]["persisted_coverage"] == 1.0
    assert payload["coverage"]["semantics"]["persisted"].startswith("minute_row_written")


def test_gth_cash_spx_is_explicitly_not_expected_and_does_not_create_gap(
    tmp_path: Path,
) -> None:
    storage = _storage(tmp_path)
    gth = datetime(2026, 7, 31, 8, 45, tzinfo=UTC)

    result = sample_spx_minute_once(
        storage=storage,
        policy=MarketFeatureSettings(),
        now=gth,
        latest_state=_latest(gth),
        writer_instance_id="writer-a",
    )

    assert result["official_spx_expected"] is False
    assert result["rejection"] == "official_spx_not_expected_outside_rth"
    assert not canonical_state_path(storage).exists()
