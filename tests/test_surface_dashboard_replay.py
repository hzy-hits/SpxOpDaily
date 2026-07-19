from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pytest

import spx_spark.surface_dashboard_replay as replay_module
from spx_spark.config import StorageSettings
from spx_spark.surface_dashboard_replay import (
    QUOTE_LAKE_DATASET,
    REPLAY_KIND,
    REPLAY_MODE,
    ReplaySourceError,
    build_replay_snapshot,
    default_replay_output_path,
    generate_replay,
    load_replay_state,
    parse_args,
    replay_id,
)


AS_OF = datetime(2026, 7, 17, 18, 30, tzinfo=timezone.utc)
FRONT = "20260717"
NEXT = "20260720"


def storage_settings(tmp_path: Path) -> StorageSettings:
    data_root = tmp_path / "data"
    return StorageSettings(
        data_root=str(data_root),
        latest_state_path=str(data_root / "latest" / "state.json"),
        raw_file_name="quotes.jsonl",
        include_raw_payload=False,
        latest_stale_after_seconds=15.0,
        slow_index_stale_after_seconds=120.0,
        slow_index_labels=frozenset({"index:SKEW"}),
        delayed_stale_after_seconds=60.0,
        rotation_stale_after_seconds=45.0,
    )


def _row(
    *,
    instrument_id: str,
    symbol: str,
    instrument_type: str,
    received_at: datetime,
    expiry: str | None = None,
    strike: float | None = None,
    right: str | None = None,
    bid: float,
    ask: float,
    mark: float | None = None,
    implied_vol: float | None = None,
    open_interest: float | None = None,
    volume: float | None = None,
    quote_time: datetime | None = None,
    trade_time: datetime | None = None,
    last_update_at: datetime | None = None,
) -> tuple[object, ...]:
    is_option = instrument_type == "option"
    effective_quote_time = quote_time or received_at
    return (
        "v1",
        "test-writer-v1",
        "schwab",
        received_at,
        effective_quote_time,
        effective_quote_time,
        trade_time,
        last_update_at or received_at,
        instrument_id,
        symbol,
        instrument_type,
        instrument_id,
        "CBOE" if is_option else "INDEX",
        "USD",
        datetime.strptime(expiry, "%Y%m%d").date() if expiry else None,
        strike,
        right,
        "100" if is_option else None,
        "SPX" if is_option else None,
        "SPXW" if is_option else None,
        "live",
        bid,
        ask,
        mark,
        mark,
        None,
        10.0,
        12.0,
        None,
        volume,
        open_interest,
        1.0,
        "live",
        "regular",
        implied_vol,
        0.25 if right == "C" else -0.25 if right == "P" else None,
        0.01 if is_option else None,
        -0.1 if is_option else None,
        0.2 if is_option else None,
        0.0 if is_option else None,
        7462.0 if is_option else None,
        "vendor" if is_option else None,
        "test",
        1,
        None,
        "raw/provider=schwab/date=2026-07-17/hour=18/quotes.jsonl",
        "a" * 64,
        AS_OF + timedelta(minutes=1),
    )


def write_quote_partition(
    tmp_path: Path,
    *,
    include_options: bool = True,
    include_ambiguous_top: bool = False,
) -> Path:
    partition = (
        tmp_path
        / "data"
        / QUOTE_LAKE_DATASET
        / "date=2026-07-17"
        / "provider=schwab"
        / "hour=18"
        / "quotes.parquet"
    )
    partition.parent.mkdir(parents=True)
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE quotes (
            schema_version VARCHAR,
            writer_version VARCHAR,
            provider VARCHAR,
            received_at TIMESTAMPTZ,
            source_at TIMESTAMPTZ,
            quote_time TIMESTAMPTZ,
            trade_time TIMESTAMPTZ,
            last_update_at TIMESTAMPTZ,
            instrument_id VARCHAR,
            symbol VARCHAR,
            instrument_type VARCHAR,
            provider_symbol VARCHAR,
            exchange VARCHAR,
            currency VARCHAR,
            expiry DATE,
            strike DOUBLE,
            "right" VARCHAR,
            multiplier VARCHAR,
            underlier VARCHAR,
            trading_class VARCHAR,
            quality VARCHAR,
            bid DOUBLE,
            ask DOUBLE,
            last DOUBLE,
            mark DOUBLE,
            close DOUBLE,
            bid_size DOUBLE,
            ask_size DOUBLE,
            last_size DOUBLE,
            volume DOUBLE,
            open_interest DOUBLE,
            source_latency_ms DOUBLE,
            market_data_type VARCHAR,
            market_session VARCHAR,
            implied_vol DOUBLE,
            delta DOUBLE,
            gamma DOUBLE,
            theta DOUBLE,
            vega DOUBLE,
            rho DOUBLE,
            greeks_underlier_price DOUBLE,
            greeks_model VARCHAR,
            sampling_mode VARCHAR,
            sampling_group BIGINT,
            error VARCHAR,
            source_file VARCHAR,
            source_sha256 VARCHAR,
            compacted_at TIMESTAMPTZ
        )
        """
    )
    rows = [
        _row(
            instrument_id="index:SPX",
            symbol="SPX",
            instrument_type="index",
            received_at=AS_OF - timedelta(seconds=5),
            bid=7458.0,
            ask=7460.0,
            mark=7459.0,
        ),
        _row(
            instrument_id="index:SPX",
            symbol="SPX",
            instrument_type="index",
            received_at=AS_OF - timedelta(seconds=1),
            bid=7461.0,
            ask=7463.0,
            mark=7462.0,
        ),
        _row(
            instrument_id="index:SPX",
            symbol="SPX",
            instrument_type="index",
            received_at=AS_OF - timedelta(milliseconds=500),
            quote_time=AS_OF + timedelta(milliseconds=250),
            bid=7999.0,
            ask=8001.0,
            mark=8000.0,
        ),
        _row(
            instrument_id="index:SPX",
            symbol="SPX",
            instrument_type="index",
            received_at=AS_OF + timedelta(seconds=1),
            bid=7999.0,
            ask=8001.0,
            mark=8000.0,
        ),
    ]
    if include_options:
        for expiry in (FRONT, NEXT):
            for index, strike in enumerate(range(7415, 7515, 10)):
                for right in ("C", "P"):
                    instrument_id = f"option:SPX:SPXW:{expiry}:{strike}:{right}"
                    price = 15.0 + abs(strike - 7465.0) / 10.0
                    open_interest = (
                        100.0 + index * 25.0
                        if right == "C"
                        else 325.0 - index * 25.0
                    )
                    rows.append(
                        _row(
                            instrument_id=instrument_id,
                            symbol="SPX",
                            instrument_type="option",
                            received_at=AS_OF - timedelta(seconds=2),
                            expiry=expiry,
                            strike=float(strike),
                            right=right,
                            bid=price - 0.25,
                            ask=price + 0.25,
                            mark=price,
                            implied_vol=0.16 + index * 0.002,
                            open_interest=open_interest,
                            volume=20.0 + index,
                        )
                    )
        rows.extend(
            [
                _row(
                    instrument_id=f"option:SPX:SPXW:{FRONT}:7415:C",
                    symbol="SPX",
                    instrument_type="option",
                    received_at=AS_OF - timedelta(seconds=1.5),
                    quote_time=AS_OF - timedelta(seconds=1.25),
                    expiry=FRONT,
                    strike=7415.0,
                    right="C",
                    bid=20.0,
                    ask=20.5,
                    mark=20.25,
                    implied_vol=0.17,
                    open_interest=100.0,
                    volume=20.0,
                ),
                _row(
                    instrument_id=f"option:SPX:SPXW:{FRONT}:7415:C",
                    symbol="SPX",
                    instrument_type="option",
                    received_at=AS_OF - timedelta(seconds=1.5),
                    quote_time=AS_OF - timedelta(seconds=1.0),
                    expiry=FRONT,
                    strike=7415.0,
                    right="C",
                    bid=21.0,
                    ask=21.5,
                    mark=21.25,
                    implied_vol=0.18,
                    open_interest=100.0,
                    volume=20.0,
                ),
            ]
        )
        if include_ambiguous_top:
            common = {
                "instrument_id": f"option:SPX:SPXW:{FRONT}:7425:C",
                "symbol": "SPX",
                "instrument_type": "option",
                "received_at": AS_OF - timedelta(milliseconds=750),
                "quote_time": AS_OF - timedelta(milliseconds=500),
                "expiry": FRONT,
                "strike": 7425.0,
                "right": "C",
                "implied_vol": 0.19,
                "open_interest": 125.0,
                "volume": 21.0,
            }
            rows.extend(
                [
                    _row(**common, bid=22.0, ask=22.5, mark=22.25),
                    _row(**common, bid=23.0, ask=23.5, mark=23.25),
                ]
            )
    placeholders = ",".join("?" for _ in rows[0])
    connection.executemany(f"INSERT INTO quotes VALUES ({placeholders})", rows)
    connection.execute("COPY quotes TO ? (FORMAT PARQUET)", [str(partition)])
    connection.close()
    return partition


def test_load_replay_state_enforces_point_in_time_cutoff(tmp_path: Path) -> None:
    partition = write_quote_partition(tmp_path)

    loaded = load_replay_state(
        data_root=tmp_path / "data",
        as_of=AS_OF,
    )

    assert loaded.selected_quote_count == 41
    assert loaded.selected_expiry_counts == {FRONT: 20, NEXT: 20}
    assert loaded.data_as_of == AS_OF - timedelta(seconds=1)
    assert loaded.data_as_of <= loaded.requested_as_of
    assert loaded.max_transport_age_seconds == 2.0
    assert loaded.max_observation_age_seconds == 2.0
    assert loaded.min_observation_age_seconds == 1.0
    assert loaded.selection_audit.source_clock_rows_excluded == 1
    assert loaded.selection_audit.duplicate_received_at_group_count == 1
    assert loaded.selection_audit.ambiguous_top_instrument_count == 0
    assert loaded.source_files == (str(partition.relative_to(tmp_path / "data")),)
    assert len(loaded.source_file_sha256[loaded.source_files[0]]) == 64
    assert loaded.state.best_quote("index:SPX").effective_price == 7462.0
    selected_call = next(
        quote
        for quote in loaded.state.quotes
        if quote.instrument.canonical_id == f"option:SPX:SPXW:{FRONT}:7415:C"
    )
    assert selected_call.bid == 21.0
    assert all(quote.received_at <= AS_OF for quote in loaded.state.quotes)


def test_replay_snapshot_is_frozen_and_has_no_live_lease(tmp_path: Path) -> None:
    write_quote_partition(tmp_path)
    settings = storage_settings(tmp_path)
    loaded = load_replay_state(data_root=settings.data_root, as_of=AS_OF)
    generated_at = datetime(2026, 7, 19, 6, 0, tzinfo=timezone.utc)

    payload = build_replay_snapshot(
        loaded,
        storage_settings=settings,
        generated_at=generated_at,
    )

    assert payload["schema_version"] == 1
    assert payload["kind"] == REPLAY_KIND
    assert payload["mode"] == REPLAY_MODE
    assert payload["replay_id"] == "2026-07-17T183000Z"
    assert payload["frozen"] is True
    assert payload["automatic_ordering"] is False
    assert payload["status"] == "ready"
    assert payload["generated_at"] == generated_at.isoformat()
    assert "valid_until" not in payload
    assert "created_at" not in payload
    assert payload["source"]["lookahead_rows_selected"] == 0
    assert payload["policy_version"] == "spxw_surface_replay.v2"
    assert payload["source"]["cutoff_rule"] == (
        "received_at_and_available_source_clocks_lte_requested_as_of"
    )
    assert payload["source"]["cutoff_fields"] == [
        "received_at",
        "source_at",
        "quote_time",
        "trade_time",
        "last_update_at",
    ]
    assert payload["source"]["replay_loader_field_stitching"] is False
    assert payload["source"]["source_clock_rows_excluded"] == 1
    assert payload["source"]["ambiguous_top_instrument_count"] == 0
    assert payload["source"]["source_files_verified_unchanged_during_read"] is True
    assert payload["source"]["lake_schema_versions"] == ["v1"]
    assert payload["source"]["lake_writer_versions"] == ["test-writer-v1"]
    assert payload["source"]["structure_clock_available"] is False
    assert payload["source"]["raw_source_file_sha256"] == {
        "raw/provider=schwab/date=2026-07-17/hour=18/quotes.jsonl": "a" * 64
    }
    assert payload["source"]["parquet_file_sha256"] == loaded.source_file_sha256
    assert len(payload["projection_policy_sha256"]) == 64
    artifact_sha256 = payload.pop("artifact_sha256")
    assert artifact_sha256 == replay_module._canonical_sha256(payload)
    payload["artifact_sha256"] = artifact_sha256
    assert payload["underlier"]["price"] == 7462.0
    assert [(row["role"], row["expiry"]) for row in payload["expiries"]] == [
        ("front", FRONT),
        ("next", NEXT),
    ]
    json.dumps(payload, allow_nan=False)


def test_replay_snapshot_rejects_generation_before_selected_data(
    tmp_path: Path,
) -> None:
    write_quote_partition(tmp_path)
    settings = storage_settings(tmp_path)
    loaded = load_replay_state(data_root=settings.data_root, as_of=AS_OF)

    with pytest.raises(ReplaySourceError, match="replay_generated_before_data"):
        build_replay_snapshot(
            loaded,
            storage_settings=settings,
            generated_at=loaded.data_as_of - timedelta(microseconds=1),
        )


def test_replay_loader_rejects_incomplete_front_next_slice(tmp_path: Path) -> None:
    write_quote_partition(tmp_path, include_options=False)

    with pytest.raises(ReplaySourceError, match="contract_coverage_too_low"):
        load_replay_state(data_root=tmp_path / "data", as_of=AS_OF)


def test_replay_loader_drops_ambiguous_latest_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    write_quote_partition(tmp_path, include_ambiguous_top=True)
    monkeypatch.setattr(replay_module, "MIN_CONTRACTS_PER_EXPIRY", 19)

    loaded = load_replay_state(data_root=tmp_path / "data", as_of=AS_OF)

    assert loaded.selected_expiry_counts[FRONT] == 19
    assert loaded.selection_audit.ambiguous_top_instrument_count == 1
    assert loaded.selection_audit.dropped_ambiguous_instrument_count == 1


def test_replay_loader_rejects_source_hash_change_during_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    write_quote_partition(tmp_path)
    hashes = iter(["a" * 64, "b" * 64])
    monkeypatch.setattr(replay_module, "_sha256", lambda _path: next(hashes))

    with pytest.raises(ReplaySourceError, match="source_files_changed_during_read"):
        load_replay_state(data_root=tmp_path / "data", as_of=AS_OF)


@pytest.mark.parametrize("lookback", [0.0, float("nan"), 301.0])
def test_replay_loader_bounds_history_window(tmp_path: Path, lookback: float) -> None:
    with pytest.raises(ValueError, match="lookback_seconds"):
        load_replay_state(
            data_root=tmp_path / "data",
            as_of=AS_OF,
            lookback_seconds=lookback,
        )


def test_replay_cli_and_default_path_are_deterministic(tmp_path: Path) -> None:
    args = parse_args(["--as-of", "2026-07-17T18:30:00Z", "--data-root", str(tmp_path)])

    assert args.as_of == AS_OF
    assert replay_id(args.as_of) == "2026-07-17T183000Z"
    assert default_replay_output_path(tmp_path, as_of=args.as_of) == (
        tmp_path
        / "published"
        / "spxw-surface"
        / "replays"
        / "2026-07-17T183000Z.json"
    )


def test_replay_generator_refuses_implicit_overwrite(tmp_path: Path) -> None:
    write_quote_partition(tmp_path)
    settings = storage_settings(tmp_path)
    output_path = tmp_path / "replay.json"

    generate_replay(
        as_of=AS_OF,
        data_root=settings.data_root,
        storage_settings=settings,
        output_path=output_path,
    )
    with pytest.raises(ReplaySourceError, match="replay_output_already_exists"):
        generate_replay(
            as_of=AS_OF,
            data_root=settings.data_root,
            storage_settings=settings,
            output_path=output_path,
        )
    generate_replay(
        as_of=AS_OF,
        data_root=settings.data_root,
        storage_settings=settings,
        output_path=output_path,
        force=True,
    )
    assert not output_path.with_name(f"{output_path.name}.lock").exists()


def test_replay_generator_honors_exclusive_generation_lock(tmp_path: Path) -> None:
    write_quote_partition(tmp_path)
    settings = storage_settings(tmp_path)
    output_path = tmp_path / "replay.json"
    lock_path = output_path.with_name(f"{output_path.name}.lock")
    lock_path.write_text("held", encoding="utf-8")

    with pytest.raises(ReplaySourceError, match="replay_generation_locked"):
        generate_replay(
            as_of=AS_OF,
            data_root=settings.data_root,
            storage_settings=settings,
            output_path=output_path,
        )

    assert lock_path.read_text(encoding="utf-8") == "held"


def test_replay_cli_requires_timezone() -> None:
    with pytest.raises(SystemExit):
        parse_args(["--as-of", "2026-07-17T18:30:00"])
