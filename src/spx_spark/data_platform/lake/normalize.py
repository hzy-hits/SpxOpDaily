from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import duckdb


JSON_COLUMNS = """
{
    'instrument': 'STRUCT(symbol VARCHAR, instrument_type VARCHAR, provider_symbol VARCHAR, exchange VARCHAR, currency VARCHAR, expiry VARCHAR, strike DOUBLE, "right" VARCHAR, multiplier VARCHAR, underlier VARCHAR, trading_class VARCHAR, canonical_id VARCHAR)',
    'instrument_id': 'VARCHAR',
    'provider': 'VARCHAR',
    'provider_symbol': 'VARCHAR',
    'received_at': 'TIMESTAMPTZ',
    'quality': 'VARCHAR',
    'bid': 'DOUBLE',
    'ask': 'DOUBLE',
    'last': 'DOUBLE',
    'mark': 'DOUBLE',
    'close': 'DOUBLE',
    'bid_size': 'DOUBLE',
    'ask_size': 'DOUBLE',
    'last_size': 'DOUBLE',
    'volume': 'DOUBLE',
    'open_interest': 'DOUBLE',
    'quote_time': 'TIMESTAMPTZ',
    'trade_time': 'TIMESTAMPTZ',
    'last_update_at': 'TIMESTAMPTZ',
    'source_latency_ms': 'DOUBLE',
    'market_data_type': 'VARCHAR',
    'market_session': 'VARCHAR',
    'regular_source_at': 'TIMESTAMPTZ',
    'extended_source_at': 'TIMESTAMPTZ',
    'greeks': 'STRUCT(implied_vol DOUBLE, delta DOUBLE, gamma DOUBLE, theta DOUBLE, vega DOUBLE, rho DOUBLE, underlier_price DOUBLE, model VARCHAR)',
    'sampling_mode': 'VARCHAR',
    'sampling_group': 'BIGINT',
    'mid': 'DOUBLE',
    'spread': 'DOUBLE',
    'spread_bps': 'DOUBLE',
    'effective_price': 'DOUBLE',
    'error': 'VARCHAR'
}
"""


@dataclass(frozen=True)
class NormalizedQuoteStats:
    row_count: int
    min_received_at: str
    max_received_at: str
    min_source_at: str
    max_source_at: str


class InvalidQuoteRowsError(ValueError):
    pass


def create_normalized_quotes(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_path: Path,
    expected_provider: str,
    partition_start: datetime,
    partition_end: datetime,
    source_relative_path: str,
    source_sha256: str,
    schema_version: str,
    writer_version: str,
) -> NormalizedQuoteStats:
    """Read newline JSON with an explicit schema and materialize a flat typed table."""

    connection.execute("SET TimeZone = 'UTC'")
    connection.execute("DROP TABLE IF EXISTS raw_quote_input")
    connection.execute("DROP TABLE IF EXISTS normalized_quotes")
    connection.execute(
        f"""
        CREATE TEMP TABLE raw_quote_input AS
        SELECT *
        FROM read_json(
            ?,
            format = 'newline_delimited',
            columns = {JSON_COLUMNS},
            ignore_errors = false
        )
        """,
        [str(source_path)],
    )

    invalid = connection.execute(
        """
        SELECT
            count(*) FILTER (WHERE received_at IS NULL) AS missing_received_at,
            count(*) FILTER (WHERE provider IS NULL OR provider <> ?) AS wrong_provider,
            count(*) FILTER (
                WHERE instrument IS NULL
                   OR nullif(instrument.symbol, '') IS NULL
                   OR nullif(instrument.instrument_type, '') IS NULL
                   OR nullif(coalesce(instrument_id, instrument.canonical_id), '') IS NULL
            ) AS missing_instrument,
            count(*) FILTER (
                WHERE received_at IS NOT NULL
                  AND (received_at < ?::TIMESTAMPTZ OR received_at >= ?::TIMESTAMPTZ)
            ) AS outside_partition
        FROM raw_quote_input
        """,
        [
            expected_provider,
            partition_start.isoformat(),
            partition_end.isoformat(),
        ],
    ).fetchone()
    assert invalid is not None
    labels = ("missing_received_at", "wrong_provider", "missing_instrument", "outside_partition")
    failures = {name: int(value) for name, value in zip(labels, invalid, strict=True) if value}
    if failures:
        rendered = ", ".join(f"{name}={count}" for name, count in failures.items())
        raise InvalidQuoteRowsError(f"invalid quote rows in {source_path}: {rendered}")

    connection.execute(
        """
        CREATE TEMP TABLE normalized_quotes AS
        SELECT
            ?::VARCHAR AS schema_version,
            ?::VARCHAR AS writer_version,
            provider,
            received_at,
            coalesce(quote_time, trade_time, received_at) AS source_at,
            quote_time,
            trade_time,
            last_update_at,
            coalesce(nullif(instrument_id, ''), instrument.canonical_id) AS instrument_id,
            instrument.symbol AS symbol,
            instrument.instrument_type AS instrument_type,
            coalesce(provider_symbol, instrument.provider_symbol) AS provider_symbol,
            instrument.exchange AS exchange,
            instrument.currency AS currency,
            COALESCE(
                TRY_STRPTIME(NULLIF(trim(instrument.expiry), ''), '%Y%m%d')::DATE,
                TRY_CAST(NULLIF(trim(instrument.expiry), '') AS DATE)
            ) AS expiry,
            instrument.strike AS strike,
            instrument."right" AS "right",
            instrument.multiplier AS multiplier,
            instrument.underlier AS underlier,
            instrument.trading_class AS trading_class,
            quality,
            bid,
            ask,
            last,
            mark,
            close,
            bid_size,
            ask_size,
            last_size,
            volume,
            open_interest,
            mid,
            spread,
            spread_bps,
            effective_price,
            source_latency_ms,
            market_data_type,
            market_session,
            regular_source_at,
            extended_source_at,
            greeks.implied_vol AS implied_vol,
            greeks.delta AS delta,
            greeks.gamma AS gamma,
            greeks.theta AS theta,
            greeks.vega AS vega,
            greeks.rho AS rho,
            greeks.underlier_price AS greeks_underlier_price,
            greeks.model AS greeks_model,
            sampling_mode,
            sampling_group,
            error,
            ?::VARCHAR AS source_file,
            ?::VARCHAR AS source_sha256,
            NULL::TIMESTAMPTZ AS compacted_at
        FROM raw_quote_input
        """,
        [
            schema_version,
            writer_version,
            source_relative_path,
            source_sha256,
        ],
    )
    connection.execute("DROP TABLE raw_quote_input")

    stats = connection.execute(
        """
        SELECT
            count(*),
            strftime(min(received_at), '%Y-%m-%dT%H:%M:%S.%fZ'),
            strftime(max(received_at), '%Y-%m-%dT%H:%M:%S.%fZ'),
            strftime(min(source_at), '%Y-%m-%dT%H:%M:%S.%fZ'),
            strftime(max(source_at), '%Y-%m-%dT%H:%M:%S.%fZ')
        FROM normalized_quotes
        """
    ).fetchone()
    assert stats is not None
    if int(stats[0]) == 0:
        raise InvalidQuoteRowsError(f"no quote rows in {source_path}")
    return NormalizedQuoteStats(
        row_count=int(stats[0]),
        min_received_at=str(stats[1]),
        max_received_at=str(stats[2]),
        min_source_at=str(stats[3]),
        max_source_at=str(stats[4]),
    )


def write_normalized_parquet(
    connection: duckdb.DuckDBPyConnection,
    output_path: Path,
    *,
    compression_level: int = 3,
) -> None:
    connection.execute(
        f"""
        COPY normalized_quotes TO ? (
            FORMAT PARQUET,
            COMPRESSION ZSTD,
            COMPRESSION_LEVEL {int(compression_level)},
            ROW_GROUP_SIZE 100000
        )
        """,
        [str(output_path)],
    )


def verify_parquet(
    connection: duckdb.DuckDBPyConnection,
    path: Path,
    expected: NormalizedQuoteStats,
) -> None:
    actual = connection.execute(
        """
        SELECT
            count(*),
            strftime(min(received_at), '%Y-%m-%dT%H:%M:%S.%fZ'),
            strftime(max(received_at), '%Y-%m-%dT%H:%M:%S.%fZ'),
            strftime(min(source_at), '%Y-%m-%dT%H:%M:%S.%fZ'),
            strftime(max(source_at), '%Y-%m-%dT%H:%M:%S.%fZ')
        FROM read_parquet(?)
        """,
        [str(path)],
    ).fetchone()
    expected_tuple = (
        expected.row_count,
        expected.min_received_at,
        expected.max_received_at,
        expected.min_source_at,
        expected.max_source_at,
    )
    if actual != expected_tuple:
        raise ValueError(
            f"Parquet verification failed for {path}: {actual!r} != {expected_tuple!r}"
        )
