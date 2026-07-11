from __future__ import annotations

import os
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb

from spx_spark.config import StorageSettings
from spx_spark.data_platform.adapters.jsonl_landing import JsonlQuoteLandingWriter
from spx_spark.data_platform.adapters.sqlite_ledger import SQLiteDecisionLedger
from spx_spark.data_platform.contracts import (
    DecisionLegRecord,
    DecisionRecord,
    EventRecord,
    FeatureSnapshotRecord,
    OutcomeRecord,
)
from spx_spark.data_platform.lake.compact import QuoteLakeCompactor
from spx_spark.data_platform.lake.layout import discover_raw_quote_partitions
from spx_spark.data_platform.research import ResearchCatalog
from spx_spark.marketdata import MarketDataQuality, Provider
from spx_spark.schwab.adapter import quote_from_schwab_option_contract


UTC = timezone.utc
RECEIVED_AT = datetime(2026, 7, 10, 10, 5, tzinfo=UTC)


def storage_settings(data_root: Path) -> StorageSettings:
    return StorageSettings(
        data_root=str(data_root),
        latest_state_path=str(data_root / "latest" / "state.json"),
        raw_file_name="quotes.jsonl",
        include_raw_payload=False,
        latest_stale_after_seconds=15,
        slow_index_stale_after_seconds=300,
        slow_index_labels=frozenset(),
    )


def schwab_contract_fixture() -> dict[str, object]:
    """A valid but deliberately sparse Schwab 0DTE contract payload."""

    return {
        "symbol": "SPXW  260710C06300000",
        "putCall": "CALL",
        "expirationDate": "2026-07-10T20:00:00+00:00",
        "strikePrice": 6300.0,
        "bid": 4.0,
        "ask": 4.4,
        "mark": 4.2,
        "quoteTimeInLong": int((RECEIVED_AT - timedelta(seconds=1)).timestamp() * 1000),
        "delta": 0.42,
        "theta": -1.1,
        "volatility": 19.5,
        # last, sizes, open interest, gamma, vega and rho are intentionally absent.
    }


def add_schema_v2_copy(v1_path: Path, v2_path: Path) -> None:
    """Simulate a later writer adding data while retaining the same quote identity."""

    v2_path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect()
    try:
        source = str(v1_path).replace("'", "''")
        destination = str(v2_path).replace("'", "''")
        connection.execute(
            f"""
            COPY (
                SELECT
                    * REPLACE (
                        'v2' AS schema_version,
                        'schwab-fixture-v2' AS writer_version,
                        0.0025 AS gamma
                    ),
                    0.125::DOUBLE AS color
                FROM read_parquet('{source}')
            ) TO '{destination}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
    finally:
        connection.close()


def test_schwab_quote_to_sqlite_parquet_and_duckdb_is_offline_and_deduplicated(
    tmp_path: Path,
) -> None:
    quote = quote_from_schwab_option_contract(
        "SPX",
        schwab_contract_fixture(),
        received_at=RECEIVED_AT,
    )
    assert quote.provider is Provider.SCHWAB
    assert quote.quality is MarketDataQuality.LIVE
    assert quote.greeks is not None
    assert quote.greeks.gamma is None
    assert quote.last is None

    # Landing is at-least-once. The identical retry is preserved in JSONL and
    # compacted Parquet, then collapsed only in the read-only research view.
    landing = JsonlQuoteLandingWriter(storage_settings(tmp_path))
    first = landing.append_quotes((quote,))
    second = landing.append_quotes((quote,))
    assert first.row_count == second.row_count == 1
    raw_path = Path(next(iter(first.path_counts)))
    assert raw_path.read_text(encoding="utf-8").count("\n") == 2
    os.utime(raw_path, (RECEIVED_AT.timestamp(), RECEIVED_AT.timestamp()))

    compactor = QuoteLakeCompactor(tmp_path, settle_seconds=0)
    summary = compactor.run(now=RECEIVED_AT + timedelta(hours=2))
    assert summary.status_counts == {"compacted": 1}
    partition = discover_raw_quote_partitions(tmp_path)[0]
    v1_path = partition.parquet_path
    with duckdb.connect() as connection:
        assert connection.execute(
            "SELECT count(*) FROM read_parquet(?)", [str(v1_path)]
        ).fetchone() == (2,)

    # A schema-v2 partition adds gamma plus an unknown future column. The
    # catalog must union by name and select the richer/newer logical quote.
    v2_path = (
        tmp_path
        / "lake/quotes/schema=v2/date=2026-07-10/provider=schwab/hour=10/quotes.parquet"
    )
    add_schema_v2_copy(v1_path, v2_path)

    ledger_path = tmp_path / "runtime" / "research-ledger.sqlite3"
    ledger = SQLiteDecisionLedger(ledger_path)
    event_at = quote.quote_time or quote.received_at
    decision_at = quote.received_at + timedelta(seconds=1)
    event = EventRecord(
        event_key="schwab-fixture-event",
        event_type="flip_reclaim_call",
        session_date=date(2026, 7, 10),
        source_at=event_at,
        available_at=quote.received_at,
        received_at=quote.received_at,
        phase="reclaim",
        direction="up",
        data_quality=quote.quality.value,
    )
    snapshot = FeatureSnapshotRecord(
        snapshot_id="schwab-fixture-snapshot",
        event_key=event.event_key,
        captured_at=event_at,
        available_at=quote.received_at,
        gamma_regime="unknown",
        payload={"source": "schwab", "gamma_missing": True},
    )
    decision = DecisionRecord(
        decision_id="schwab-fixture-decision",
        event_key=event.event_key,
        feature_snapshot_id=snapshot.snapshot_id,
        strategy_name="flip_reclaim_call",
        strategy_version="fixture-v1",
        decision_at=decision_at,
        available_at=quote.received_at,
        status="triggered",
        action="notify",
        side="CALL",
        gamma_regime="unknown",
    )
    leg = DecisionLegRecord(
        decision_id=decision.decision_id,
        leg_index=0,
        instrument_id=quote.instrument.canonical_id,
        right=quote.instrument.right.value if quote.instrument.right else None,
        expiry=date.fromisoformat("2026-07-10"),
        strike=quote.instrument.strike,
        bid=quote.bid,
        ask=quote.ask,
        delta=quote.greeks.delta,
        gamma=quote.greeks.gamma,
        theta=quote.greeks.theta,
        vega=quote.greeks.vega,
        quote_source_at=event_at,
        quote_available_at=quote.received_at,
    )
    ledger.record_event(event)
    ledger.record_feature_snapshot(snapshot)
    ledger.record_decision(decision, (leg,))
    ledger.record_outcome(
        OutcomeRecord(
            outcome_id="schwab-fixture-outcome",
            event_key=event.event_key,
            decision_id=decision.decision_id,
            horizon_minutes=5,
            status="complete",
            target_at=decision_at + timedelta(minutes=5),
            sampled_at=decision_at + timedelta(minutes=5),
            spx_return_bps=8.0,
            option_return_bps=20.0,
        )
    )

    with ResearchCatalog.in_memory(tmp_path, sqlite_ledger=ledger_path) as catalog:
        quotes = catalog.reader().quotes(provider="schwab")
        outcomes = catalog.reader().strategy_outcomes(strategy_name="flip_reclaim_call")

    assert len(quotes) == 2
    assert sum(row["schema_version"] == "v1" for row in quotes) == 1
    stored_quote = next(row for row in quotes if row["schema_version"] == "v2")
    assert stored_quote["provider"] == "schwab"
    assert stored_quote["instrument_id"] == quote.instrument.canonical_id
    assert stored_quote["schema_version"] == "v2"
    assert stored_quote["gamma"] == 0.0025
    assert stored_quote["rho"] is None
    assert stored_quote["last"] is None

    assert len(outcomes) == 1
    stored_outcome = outcomes[0]
    assert stored_outcome["decision_id"] == decision.decision_id
    assert stored_outcome["instrument_id"] == quote.instrument.canonical_id
    assert stored_outcome["entry_gamma"] is None
    assert stored_outcome["directional_return_bps"] == 8.0
    assert stored_outcome["option_return_bps"] == 20.0


def test_research_quotes_preserve_same_timestamp_price_corrections(tmp_path: Path) -> None:
    quote = quote_from_schwab_option_contract(
        "SPX",
        schwab_contract_fixture(),
        received_at=RECEIVED_AT,
    )
    corrected = replace(quote, bid=4.1, ask=4.5, mark=4.3)
    landing = JsonlQuoteLandingWriter(storage_settings(tmp_path))
    result = landing.append_quotes((quote, corrected))
    raw_path = Path(next(iter(result.path_counts)))
    os.utime(raw_path, (RECEIVED_AT.timestamp(), RECEIVED_AT.timestamp()))

    summary = QuoteLakeCompactor(tmp_path, settle_seconds=0).run(
        now=RECEIVED_AT + timedelta(hours=2)
    )
    assert summary.status_counts == {"compacted": 1}

    with ResearchCatalog.in_memory(tmp_path) as catalog:
        quotes = catalog.reader().quotes(provider="schwab")

    assert len(quotes) == 2
    assert {(row["bid"], row["ask"], row["mark"]) for row in quotes} == {
        (4.0, 4.4, 4.2),
        (4.1, 4.5, 4.3),
    }
