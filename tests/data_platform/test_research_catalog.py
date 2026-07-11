from __future__ import annotations

import sqlite3
from datetime import date
from pathlib import Path

import duckdb
import pytest

from spx_spark.data_platform.research import (
    ResearchCatalog,
    ResearchCatalogConfig,
    ResearchCatalogError,
)


def write_parquet(path: Path, select_sql: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect()
    try:
        escaped = str(path).replace("'", "''")
        connection.execute(f"COPY ({select_sql}) TO '{escaped}' (FORMAT PARQUET)")
    finally:
        connection.close()


def test_empty_catalog_is_queryable_when_no_partitions_exist(tmp_path: Path) -> None:
    with ResearchCatalog.in_memory(tmp_path) as catalog:
        reader = catalog.reader()
        assert reader.strategy_outcomes() == []
        assert reader.put_call_bias() == []
        assert reader.session_data_quality() == []
        assert reader.quotes() == []


def test_strategy_view_joins_context_and_rejects_future_information(tmp_path: Path) -> None:
    facts = tmp_path / "lake" / "facts"
    write_parquet(
        facts / "decisions" / "date=2026-07-10" / "part.parquet",
        """
        SELECT * FROM (VALUES
          ('valid', 'event-1', 'feature-old', DATE '2026-07-10',
           'flip_reclaim_call', 'v2', 'CALL', 'trigger', 'triggered', NULL,
           TIMESTAMPTZ '2026-07-10 14:00:00+00',
           TIMESTAMPTZ '2026-07-10 14:00:01+00',
           TIMESTAMPTZ '2026-07-10 14:00:02+00'),
          ('future-decision', 'event-2', NULL, DATE '2026-07-10',
           'call_wall_breakout_call', 'v2', 'CALL', 'trigger', 'triggered', NULL,
           TIMESTAMPTZ '2026-07-10 14:00:00+00',
           TIMESTAMPTZ '2026-07-10 14:02:00+00',
           TIMESTAMPTZ '2026-07-10 14:01:00+00')
        ) t(decision_id, event_key, feature_snapshot_id, session_date,
            strategy_name, strategy_version, side, action, status, reason_code,
            source_at, available_at, decision_at)
        """,
    )
    write_parquet(
        facts / "events" / "date=2026-07-10" / "part.parquet",
        """
        SELECT * FROM (VALUES
          ('event-1', DATE '2026-07-10', 'shock_reclaim', 'reclaim', 'down',
           TIMESTAMPTZ '2026-07-10 13:59:59+00',
           TIMESTAMPTZ '2026-07-10 14:00:01+00'),
          ('event-2', DATE '2026-07-10', 'wall_breakout', 'breakout', 'up',
           TIMESTAMPTZ '2026-07-10 14:00:00+00',
           TIMESTAMPTZ '2026-07-10 14:00:01+00')
        ) t(event_key, session_date, event_type, phase, direction, source_at, available_at)
        """,
    )
    write_parquet(
        facts / "feature_snapshots" / "date=2026-07-10" / "part.parquet",
        """
        SELECT * FROM (VALUES
          ('feature-old', 'valid', 'event-1', DATE '2026-07-10',
           TIMESTAMPTZ '2026-07-10 14:00:00+00',
           TIMESTAMPTZ '2026-07-10 14:00:01+00', -120.0, 'negative'),
          ('feature-future', 'valid', 'event-1', DATE '2026-07-10',
           TIMESTAMPTZ '2026-07-10 14:05:00+00',
           TIMESTAMPTZ '2026-07-10 14:05:01+00', 999.0, 'positive')
        ) t(feature_snapshot_id, decision_id, event_key, session_date,
            source_at, available_at, net_gamma, gamma_regime)
        """,
    )
    write_parquet(
        facts / "decision_legs" / "date=2026-07-10" / "part.parquet",
        """
        SELECT * FROM (VALUES
          ('leg-1', 'valid', DATE '2026-07-10', 0, 'C', 'opaque-contract',
           DATE '2026-07-10', 6300.0, 4.0, 4.2, 4.1, 0.42, 0.03, -1.2, 0.4,
           TIMESTAMPTZ '2026-07-10 14:00:00+00',
           TIMESTAMPTZ '2026-07-10 14:00:01+00'),
          ('leg-future', 'valid', DATE '2026-07-10', 1, 'C', 'future-contract',
           DATE '2026-07-10', 6310.0, 3.0, 3.2, 3.1, 0.35, 0.02, -1.0, 0.3,
           TIMESTAMPTZ '2026-07-10 14:02:00+00',
           TIMESTAMPTZ '2026-07-10 14:02:01+00')
        ) t(leg_id, decision_id, session_date, leg_index, side, instrument_id,
            expiry, strike, bid, ask, mark, delta, gamma, theta, vega,
            source_at, available_at)
        """,
    )
    write_parquet(
        facts / "alert_deliveries" / "date=2026-07-10" / "part.parquet",
        """
        SELECT 'delivery-1' delivery_id, 'valid' decision_id,
               DATE '2026-07-10' session_date, 'telegram' channel,
               'delivered' status, TIMESTAMPTZ '2026-07-10 14:00:03+00' sent_at
        """,
    )
    write_parquet(
        facts / "outcomes" / "date=2026-07-10" / "part.parquet",
        """
        SELECT 'outcome-1' outcome_id, 'valid' decision_id, 'event-1' event_key,
               DATE '2026-07-10' session_date, 5 horizon_minutes, 'complete' status,
               TIMESTAMPTZ '2026-07-10 14:05:02+00' sample_at,
               6300.0 start_spx, 6306.3 end_spx, 10.0 return_bps,
               14.0 path_high_return_bps, -3.0 path_low_return_bps,
               120.0 option_pnl
        """,
    )

    with ResearchCatalog.in_memory(tmp_path) as catalog:
        rows = catalog.reader().strategy_outcomes()
        bias = catalog.reader().put_call_bias()

    assert len(rows) == 1
    row = rows[0]
    assert row["decision_id"] == "valid"
    assert row["anti_lookahead_valid"] is True
    assert row["net_gamma"] == -120.0
    assert row["gamma_regime"] == "negative"
    assert row["option_side"] == "CALL"
    assert row["leg_count"] == 1
    assert row["instrument_id"] == "opaque-contract"
    assert row["delivered"] is True
    assert row["directional_return_bps"] == 10.0
    assert row["directional_mfe_bps"] == 14.0
    assert row["directional_mae_bps"] == -3.0
    assert bias[0]["decision_count"] == 1
    assert bias[0]["triggered_count"] == 1
    assert bias[0]["directional_win_rate"] == 1.0


def test_parquet_union_by_name_handles_schema_evolution(tmp_path: Path) -> None:
    quotes = tmp_path / "lake" / "quotes"
    write_parquet(
        quotes / "schema=v1" / "date=2026-07-09" / "provider=ibkr" / "old.parquet",
        """
        SELECT 1 schema_version, 'ibkr' provider,
               TIMESTAMPTZ '2026-07-09 14:00:00+00' received_at,
               TIMESTAMPTZ '2026-07-09 14:00:00+00' source_at,
               'old' instrument_id, 1.0 bid, 1.2 ask
        """,
    )
    write_parquet(
        quotes / "schema=v2" / "date=2026-07-10" / "provider=ibkr" / "new.parquet",
        """
        SELECT 2 schema_version, 'writer-2' writer_version, 'ibkr' provider,
               TIMESTAMPTZ '2026-07-10 14:00:00+00' received_at,
               TIMESTAMPTZ '2026-07-10 14:00:00+00' source_at,
               'new' instrument_id, 2.0 bid, 2.2 ask, 0.04 gamma
        """,
    )

    with ResearchCatalog.in_memory(tmp_path) as catalog:
        rows = catalog.reader().quotes(limit=10)

    assert [row["instrument_id"] for row in rows] == ["old", "new"]
    assert rows[0]["gamma"] is None
    assert rows[1]["gamma"] == 0.04
    assert rows[0]["session_date"] == date(2026, 7, 9)


def test_sqlite_ledger_is_attached_read_only_and_can_supply_current_decisions(
    tmp_path: Path,
) -> None:
    ledger = tmp_path / "runtime" / "research-ledger.sqlite3"
    ledger.parent.mkdir(parents=True)
    connection = sqlite3.connect(ledger)
    connection.execute(
        """
        CREATE TABLE decisions (
            decision_id TEXT PRIMARY KEY,
            session_date TEXT,
            strategy_name TEXT,
            side TEXT,
            status TEXT,
            available_at TEXT,
            decision_at TEXT
        )
        """
    )
    connection.execute(
        """
        INSERT INTO decisions VALUES (
            'sqlite-current', '2026-07-10', 'flip_reclaim_call', 'CALL', 'triggered',
            '2026-07-10T14:00:00+00:00', '2026-07-10T14:00:01+00:00'
        )
        """
    )
    connection.commit()
    connection.close()

    with ResearchCatalog.in_memory(tmp_path, sqlite_ledger=ledger) as catalog:
        rows = catalog.reader().strategy_outcomes()
        catalog.rebuild()
        rebuilt_rows = catalog.reader().strategy_outcomes()

    assert rows[0]["decision_id"] == "sqlite-current"
    assert rebuilt_rows == rows
    verify = sqlite3.connect(f"file:{ledger}?mode=ro", uri=True)
    try:
        assert verify.execute("SELECT COUNT(*) FROM decisions").fetchone() == (1,)
    finally:
        verify.close()


def test_session_quality_reads_json_manifests_and_aggregates_partitions(tmp_path: Path) -> None:
    manifests = tmp_path / "manifests" / "compaction" / "date=2026-07-10"
    manifests.mkdir(parents=True)
    (manifests / "first.json").write_text(
        """{
          "manifest_id": "m1",
          "session_date": "2026-07-10",
          "provider": "ibkr",
          "dataset": "quotes",
          "partition": "hour=14",
          "status": "verified",
          "row_count": 100,
          "min_source_at": "2026-07-10T14:00:00Z",
          "max_source_at": "2026-07-10T14:59:59Z",
          "max_gap_seconds": 3.0,
          "stale_ratio": 0.01,
          "missing_ratio": 0.02,
          "verified_at": "2026-07-10T15:01:00Z"
        }""",
        encoding="utf-8",
    )
    (manifests / "second.json").write_text(
        """{
          "manifest_id": "m2",
          "session_date": "2026-07-10",
          "provider": "ibkr",
          "dataset": "quotes",
          "partition": "hour=15",
          "status": "verified",
          "row_count": 300,
          "min_source_at": "2026-07-10T15:00:00Z",
          "max_source_at": "2026-07-10T15:59:59Z",
          "max_gap_seconds": 5.0,
          "stale_ratio": 0.03,
          "missing_ratio": 0.04,
          "verified_at": "2026-07-10T16:01:00Z"
        }""",
        encoding="utf-8",
    )

    with ResearchCatalog.in_memory(tmp_path) as catalog:
        rows = catalog.reader().session_data_quality(provider="ibkr", dataset="quotes")

    assert len(rows) == 1
    assert rows[0]["partition_count"] == 2
    assert rows[0]["verified_partition_count"] == 2
    assert rows[0]["row_count"] == 400
    assert rows[0]["max_gap_seconds"] == 5.0
    assert rows[0]["weighted_stale_ratio"] == pytest.approx(0.025)
    assert rows[0]["weighted_missing_ratio"] == pytest.approx(0.035)
    assert rows[0]["is_research_ready"] is True


def test_reader_validates_allowlisted_limits_and_missing_ledger(tmp_path: Path) -> None:
    with ResearchCatalog.in_memory(tmp_path) as catalog:
        with pytest.raises(ValueError, match="limit"):
            catalog.reader().strategy_outcomes(limit=0)
        with pytest.raises(ValueError, match="allowlisted"):
            catalog._query_view("duckdb_tables")

    with pytest.raises(ResearchCatalogError, match="does not exist"):
        ResearchCatalog(
            ResearchCatalogConfig(
                data_root=tmp_path,
                sqlite_ledger=tmp_path / "missing.sqlite3",
            )
        )
