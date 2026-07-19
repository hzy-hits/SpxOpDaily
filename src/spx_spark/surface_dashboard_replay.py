"""Recorded-clock-bounded SPXW surface replay from the normalized quote lake."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb

from spx_spark.config import StorageSettings
from spx_spark.features.exposure_surface import SCHEMA_VERSION as SURFACE_SCHEMA_VERSION
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import (
    InstrumentId,
    InstrumentType,
    MarketDataQuality,
    OptionGreeks,
    OptionRight,
    Provider,
    Quote,
    QuoteMarketSession,
    as_utc,
    clean_float,
)
from spx_spark.storage import LatestState, select_best_quotes
from spx_spark.surface_artifact import canonical_sha256 as _canonical_sha256
from spx_spark.state_io import exclusive_state_lock
from spx_spark.surface_dashboard import (
    DASHBOARD_SCHEMA_VERSION,
    build_surface_projection,
    write_dashboard_snapshot,
)


REPLAY_KIND = "spxw_surface_dashboard_replay"
REPLAY_MODE = "replay"
REPLAY_POLICY_VERSION = "spxw_surface_replay.v3"
QUOTE_LAKE_DATASET = "lake/quotes/schema=v1"
DEFAULT_LOOKBACK_SECONDS = 15.0
MAX_LOOKBACK_SECONDS = 300.0
MIN_CONTRACTS_PER_EXPIRY = 20
SUPPORTED_PROVIDER = Provider.SCHWAB


class ReplaySourceError(RuntimeError):
    """The requested historical slice cannot support an honest replay."""


@dataclass(frozen=True)
class ReplaySelectionAudit:
    raw_candidate_count: int
    source_clock_rows_excluded: int
    eligible_candidate_count: int
    duplicate_received_at_group_count: int
    duplicate_received_at_row_count: int
    resolved_by_surface_completeness_instrument_count: int
    ambiguous_top_instrument_count: int
    dropped_ambiguous_instrument_count: int
    identical_top_duplicate_row_count: int


@dataclass(frozen=True)
class ReplayLoadResult:
    state: LatestState
    provider: Provider
    requested_as_of: datetime
    data_as_of: datetime
    window_start: datetime
    lookback_seconds: float
    source_files: tuple[str, ...]
    source_file_sha256: dict[str, str]
    lake_schema_versions: tuple[str, ...]
    lake_writer_versions: tuple[str, ...]
    raw_source_file_sha256: dict[str, str]
    compacted_at: tuple[str, ...]
    selected_quote_count: int
    selected_expiry_counts: dict[str, int]
    max_transport_age_seconds: float
    max_observation_age_seconds: float
    min_observation_age_seconds: float
    selection_audit: ReplaySelectionAudit


def _surface_row_priority(row: dict[str, object]) -> tuple[int, ...]:
    is_option = str(row.get("instrument_type") or "") == InstrumentType.OPTION.value
    if not is_option:
        prices = tuple(clean_float(row.get(field)) for field in ("mark", "bid", "ask", "last"))
        return (0, sum(value is not None for value in prices))
    implied_vol = clean_float(row.get("implied_vol"))
    strike = clean_float(row.get("strike"))
    open_interest = clean_float(row.get("open_interest"))
    volume = clean_float(row.get("volume"))
    return (
        1,
        int(implied_vol is not None and implied_vol > 0),
        int(strike is not None and strike > 0),
        int(str(row.get("right") or "") in {OptionRight.CALL.value, OptionRight.PUT.value}),
        int(open_interest is not None and open_interest >= 0),
        int(volume is not None and volume >= 0),
        int(row.get("error") is None),
    )


def replay_id(as_of: datetime) -> str:
    return as_utc(as_of).strftime("%Y-%m-%dT%H%M%SZ")


def default_replay_output_path(data_root: str | Path, *, as_of: datetime) -> Path:
    return (
        Path(data_root).expanduser()
        / "published"
        / "spxw-surface"
        / "replays"
        / f"{replay_id(as_of)}.json"
    )


def _validate_request(as_of: datetime, lookback_seconds: float) -> datetime:
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise ValueError("replay as_of must be timezone-aware")
    if not math.isfinite(lookback_seconds) or not 0 < lookback_seconds <= MAX_LOOKBACK_SECONDS:
        raise ValueError(
            f"lookback_seconds must be finite and within (0, {MAX_LOOKBACK_SECONDS:g}]"
        )
    requested = as_utc(as_of)
    session = DEFAULT_MARKET_CALENDAR.session(requested.date())
    if session is None or not session.open_at <= requested <= session.close_at:
        raise ReplaySourceError("replay_as_of_outside_rth_session")
    return requested


def _partition_paths(
    data_root: Path,
    *,
    provider: Provider,
    start: datetime,
    end: datetime,
) -> tuple[Path, ...]:
    cursor = start.replace(minute=0, second=0, microsecond=0)
    paths: list[Path] = []
    while cursor <= end:
        path = (
            data_root
            / QUOTE_LAKE_DATASET
            / f"date={cursor.date().isoformat()}"
            / f"provider={provider.value}"
            / f"hour={cursor.strftime('%H')}"
            / "quotes.parquet"
        )
        if path.is_file():
            paths.append(path)
        cursor += timedelta(hours=1)
    if not paths:
        raise ReplaySourceError("replay_quote_lake_partitions_unavailable")
    return tuple(paths)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _is_sha256(value: str) -> bool:
    return len(value) == 64 and all(character in "0123456789abcdef" for character in value)


def _research_expiries(as_of: datetime) -> tuple[date, date]:
    values = DEFAULT_MARKET_CALENDAR.research_expiries(as_of)
    if len(values) < 2:
        raise ReplaySourceError("replay_front_next_expiries_unavailable")
    return values[0], values[1]


def _read_latest_rows(
    connection: duckdb.DuckDBPyConnection,
    *,
    paths: tuple[Path, ...],
    provider: Provider,
    window_start: datetime,
    requested_as_of: datetime,
    expiries: tuple[date, date],
) -> tuple[list[dict[str, object]], ReplaySelectionAudit]:
    connection.execute("SET TimeZone='UTC'")
    parquet_columns = {
        str(row[0])
        for row in connection.execute(
            "DESCRIBE SELECT * FROM read_parquet(?, union_by_name=true)",
            [[str(path) for path in paths]],
        ).fetchall()
        if row and row[0] is not None
    }
    # ``market_session`` was added after the first normalized quote-lake
    # partitions. Keep the projection otherwise fixed and strict: newer rows
    # retain their recorded value while legacy-only inputs receive an explicit
    # nullable value rather than failing during DuckDB binding.
    market_session_projection = (
        "market_session,"
        if "market_session" in parquet_columns
        else "NULL::VARCHAR AS market_session,"
    )
    common_ctes = """
        WITH bounds AS (
            SELECT
                ?::TIMESTAMPTZ AS window_start,
                ?::TIMESTAMPTZ AS requested_as_of,
                ?::DATE AS front_expiry,
                ?::DATE AS next_expiry
        ),
        raw_candidates AS (
            SELECT
                schema_version,
                writer_version,
                provider,
                received_at,
                source_at,
                quote_time,
                trade_time,
                last_update_at,
                instrument_id,
                symbol,
                instrument_type,
                provider_symbol,
                exchange,
                currency,
                expiry,
                strike,
                "right",
                multiplier,
                underlier,
                trading_class,
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
                source_latency_ms,
                market_data_type,
                __MARKET_SESSION_PROJECTION__
                implied_vol,
                delta,
                gamma,
                theta,
                vega,
                rho,
                greeks_underlier_price,
                greeks_model,
                sampling_mode,
                sampling_group,
                error,
                source_file,
                source_sha256,
                compacted_at,
                filename AS _source_file,
                file_row_number AS _source_row,
                (
                    COALESCE(quote_time > bounds.requested_as_of, FALSE)
                    OR COALESCE(source_at > bounds.requested_as_of, FALSE)
                    OR COALESCE(trade_time > bounds.requested_as_of, FALSE)
                    OR COALESCE(last_update_at > bounds.requested_as_of, FALSE)
                ) AS _source_clock_after_cutoff
            FROM read_parquet(
                ?,
                union_by_name=true,
                filename=true,
                file_row_number=true
            )
            CROSS JOIN bounds
            WHERE provider = ?
              AND received_at >= bounds.window_start
              AND received_at <= bounds.requested_as_of
              AND (
                    instrument_id = 'index:SPX'
                    OR (
                        trading_class = 'SPXW'
                        AND expiry IN (bounds.front_expiry, bounds.next_expiry)
                    )
              )
        ),
        eligible AS (
            SELECT *
            FROM raw_candidates
            WHERE NOT _source_clock_after_cutoff
        )
    """.replace("__MARKET_SESSION_PROJECTION__", market_session_projection)
    parameters = [
        window_start.isoformat(),
        requested_as_of.isoformat(),
        expiries[0],
        expiries[1],
        [str(path) for path in paths],
        provider.value,
    ]
    audit_row = connection.execute(
        common_ctes
        + """
        , duplicate_received_at AS (
            SELECT instrument_id, received_at, count(*) AS row_count
            FROM eligible
            GROUP BY instrument_id, received_at
            HAVING count(*) > 1
        )
        SELECT
            (SELECT count(*) FROM raw_candidates) AS raw_candidate_count,
            (
                SELECT count(*)
                FROM raw_candidates
                WHERE _source_clock_after_cutoff
            ) AS source_clock_rows_excluded,
            (SELECT count(*) FROM eligible) AS eligible_candidate_count,
            (SELECT count(*) FROM duplicate_received_at)
                AS duplicate_received_at_group_count,
            COALESCE((SELECT sum(row_count) FROM duplicate_received_at), 0)
                AS duplicate_received_at_row_count
        """,
        parameters,
    ).fetchone()
    if audit_row is None:
        raise ReplaySourceError("replay_selection_audit_unavailable")
    audit_values = tuple(int(value) for value in audit_row)

    rows = connection.execute(
        common_ctes
        + """
        , ranked AS (
            SELECT
                *,
                dense_rank() OVER (
                    PARTITION BY instrument_id
                    ORDER BY
                        received_at DESC,
                        GREATEST(
                            received_at,
                            COALESCE(
                                source_at,
                                '-infinity'::TIMESTAMPTZ
                            ),
                            COALESCE(
                                quote_time,
                                '-infinity'::TIMESTAMPTZ
                            ),
                            COALESCE(
                                trade_time,
                                '-infinity'::TIMESTAMPTZ
                            ),
                            COALESCE(
                                last_update_at,
                                '-infinity'::TIMESTAMPTZ
                            )
                        ) DESC,
                        source_at DESC NULLS LAST,
                        quote_time DESC NULLS LAST,
                        trade_time DESC NULLS LAST,
                        last_update_at DESC NULLS LAST
                ) AS clock_rank
            FROM eligible
        )
        SELECT * EXCLUDE (clock_rank)
        FROM ranked
        WHERE clock_rank = 1
        ORDER BY instrument_id, _source_file DESC, _source_row DESC
        """,
        parameters,
    ).fetchall()
    columns = [description[0] for description in connection.description]
    top_rows = [dict(zip(columns, row, strict=True)) for row in rows]
    grouped: dict[str, list[dict[str, object]]] = {}
    for row in top_rows:
        grouped.setdefault(str(row["instrument_id"]), []).append(row)

    payload_columns = tuple(column for column in columns if not column.startswith("_"))
    selected: list[dict[str, object]] = []
    resolved_by_completeness = 0
    ambiguous = 0
    identical_duplicates = 0
    for group in grouped.values():
        candidates = group
        identities = {
            tuple(
                ("nan",) if isinstance(row[column], float) and math.isnan(row[column])
                else row[column]
                for column in payload_columns
            )
            for row in candidates
        }
        if len(identities) != 1:
            best_priority = max(_surface_row_priority(row) for row in candidates)
            candidates = [
                row
                for row in candidates
                if _surface_row_priority(row) == best_priority
            ]
            identities = {
                tuple(
                    ("nan",)
                    if isinstance(row[column], float) and math.isnan(row[column])
                    else row[column]
                    for column in payload_columns
                )
                for row in candidates
            }
            if len(identities) != 1:
                ambiguous += 1
                continue
            resolved_by_completeness += 1
        identical_duplicates += len(candidates) - 1
        selected.append({column: candidates[0][column] for column in payload_columns})
    audit = ReplaySelectionAudit(
        *audit_values,
        resolved_by_surface_completeness_instrument_count=resolved_by_completeness,
        ambiguous_top_instrument_count=ambiguous,
        dropped_ambiguous_instrument_count=ambiguous,
        identical_top_duplicate_row_count=identical_duplicates,
    )
    selected.sort(key=lambda row: str(row["instrument_id"]))
    return selected, audit


def _enum_value(enum_type: type[Any], value: object, fallback: Any) -> Any:
    try:
        return enum_type(str(value))
    except (TypeError, ValueError):
        return fallback


def _expiry_text(value: object) -> str | None:
    if isinstance(value, datetime):
        return value.strftime("%Y%m%d")
    if isinstance(value, date):
        return value.strftime("%Y%m%d")
    text = str(value or "").strip().replace("-", "")
    return text if len(text) == 8 and text.isdigit() else None


def _quote_from_lake_row(row: dict[str, object]) -> Quote:
    instrument_type = _enum_value(
        InstrumentType,
        row.get("instrument_type"),
        InstrumentType.UNKNOWN,
    )
    right = _enum_value(OptionRight, row.get("right"), None)
    instrument = InstrumentId(
        symbol=str(row.get("symbol") or ""),
        instrument_type=instrument_type,
        provider_symbol=str(row["provider_symbol"])
        if row.get("provider_symbol") is not None
        else None,
        exchange=str(row["exchange"]) if row.get("exchange") is not None else None,
        currency=str(row.get("currency") or "USD"),
        expiry=_expiry_text(row.get("expiry")),
        strike=clean_float(row.get("strike")),
        right=right,
        multiplier=str(row["multiplier"]) if row.get("multiplier") is not None else None,
        underlier=str(row["underlier"]) if row.get("underlier") is not None else None,
        trading_class=str(row["trading_class"])
        if row.get("trading_class") is not None
        else None,
    )
    greek_values = {
        "implied_vol": clean_float(row.get("implied_vol")),
        "delta": clean_float(row.get("delta")),
        "gamma": clean_float(row.get("gamma")),
        "theta": clean_float(row.get("theta")),
        "vega": clean_float(row.get("vega")),
        "rho": clean_float(row.get("rho")),
        "underlier_price": clean_float(row.get("greeks_underlier_price")),
    }
    greeks = None
    if any(value is not None for value in greek_values.values()):
        greeks = OptionGreeks(
            **greek_values,
            model=str(row["greeks_model"])
            if row.get("greeks_model") is not None
            else None,
        )
    received_at = row.get("received_at")
    if not isinstance(received_at, datetime):
        raise ReplaySourceError("replay_row_received_at_missing")
    market_session = _enum_value(
        QuoteMarketSession,
        row.get("market_session"),
        None,
    )
    sampling_group = row.get("sampling_group")
    return Quote(
        instrument=instrument,
        provider=_enum_value(Provider, row.get("provider"), Provider.UNKNOWN),
        provider_symbol=instrument.provider_symbol,
        received_at=as_utc(received_at),
        quality=_enum_value(
            MarketDataQuality,
            row.get("quality"),
            MarketDataQuality.UNKNOWN,
        ),
        bid=clean_float(row.get("bid")),
        ask=clean_float(row.get("ask")),
        last=clean_float(row.get("last")),
        mark=clean_float(row.get("mark")),
        close=clean_float(row.get("close")),
        bid_size=clean_float(row.get("bid_size")),
        ask_size=clean_float(row.get("ask_size")),
        last_size=clean_float(row.get("last_size")),
        volume=clean_float(row.get("volume")),
        open_interest=clean_float(row.get("open_interest")),
        quote_time=row.get("quote_time") if isinstance(row.get("quote_time"), datetime) else None,
        trade_time=row.get("trade_time") if isinstance(row.get("trade_time"), datetime) else None,
        last_update_at=(
            row.get("last_update_at")
            if isinstance(row.get("last_update_at"), datetime)
            else None
        ),
        source_latency_ms=clean_float(row.get("source_latency_ms")),
        market_data_type=row.get("market_data_type"),
        greeks=greeks,
        sampling_mode=str(row["sampling_mode"])
        if row.get("sampling_mode") is not None
        else None,
        sampling_group=int(sampling_group) if sampling_group is not None else None,
        market_session=market_session,
        error=str(row["error"]) if row.get("error") is not None else None,
    )


def _observation_time(quote: Quote) -> datetime:
    clocks = [quote.received_at]
    clocks.extend(
        clock
        for clock in (quote.quote_time, quote.trade_time, quote.last_update_at)
        if clock is not None
    )
    return max(as_utc(clock) for clock in clocks)


def load_replay_state(
    *,
    data_root: str | Path,
    as_of: datetime,
    lookback_seconds: float = DEFAULT_LOOKBACK_SECONDS,
    provider: Provider = SUPPORTED_PROVIDER,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> ReplayLoadResult:
    """Load one single-provider state bounded by every recorded source clock.

    Legacy rows do not record response-finished/available-at, so this does not
    claim that the selected response was observable at the requested instant.
    """

    requested = _validate_request(as_of, lookback_seconds)
    if provider != SUPPORTED_PROVIDER:
        raise ValueError(f"surface replay only supports provider={SUPPORTED_PROVIDER.value}")
    root = Path(data_root).expanduser()
    window_start = requested - timedelta(seconds=lookback_seconds)
    expiries = _research_expiries(requested)
    paths = _partition_paths(
        root,
        provider=provider,
        start=window_start,
        end=requested,
    )
    relative_paths = tuple(str(path.relative_to(root)) for path in paths)
    source_hashes_before = {
        relative: _sha256(path)
        for relative, path in zip(relative_paths, paths, strict=True)
    }

    owns_connection = connection is None
    database = connection or duckdb.connect()
    try:
        rows, selection_audit = _read_latest_rows(
            database,
            paths=paths,
            provider=provider,
            window_start=window_start,
            requested_as_of=requested,
            expiries=expiries,
        )
    finally:
        if owns_connection:
            database.close()
    source_hashes_after = {
        relative: _sha256(path)
        for relative, path in zip(relative_paths, paths, strict=True)
    }
    if source_hashes_before != source_hashes_after:
        raise ReplaySourceError("replay_source_files_changed_during_read")
    lake_schema_versions = tuple(
        sorted({str(row["schema_version"]) for row in rows if row.get("schema_version")})
    )
    lake_writer_versions = tuple(
        sorted({str(row["writer_version"]) for row in rows if row.get("writer_version")})
    )
    compacted_at = tuple(
        sorted(
            {
                as_utc(value).isoformat()
                for row in rows
                if isinstance((value := row.get("compacted_at")), datetime)
            }
        )
    )
    raw_source_file_sha256: dict[str, str] = {}
    for row in rows:
        source_file = str(row.get("source_file") or "").strip()
        source_sha256 = str(row.get("source_sha256") or "").strip()
        if not source_file or not source_sha256:
            raise ReplaySourceError("replay_raw_lineage_unavailable")
        if not _is_sha256(source_sha256):
            raise ReplaySourceError("replay_raw_lineage_hash_invalid")
        existing = raw_source_file_sha256.setdefault(source_file, source_sha256)
        if existing != source_sha256:
            raise ReplaySourceError("replay_raw_lineage_hash_conflict")
    if not lake_schema_versions or not lake_writer_versions:
        raise ReplaySourceError("replay_lake_lineage_unavailable")
    quotes = tuple(_quote_from_lake_row(row) for row in rows)
    if not quotes:
        raise ReplaySourceError("replay_selected_quotes_unavailable")
    if not any(quote.instrument.canonical_id == "index:SPX" for quote in quotes):
        raise ReplaySourceError("replay_spx_underlier_unavailable")

    expiry_counts = {
        expiry.strftime("%Y%m%d"): sum(
            quote.instrument.trading_class == "SPXW"
            and quote.instrument.expiry == expiry.strftime("%Y%m%d")
            for quote in quotes
        )
        for expiry in expiries
    }
    sparse = [
        expiry
        for expiry, count in expiry_counts.items()
        if count < MIN_CONTRACTS_PER_EXPIRY
    ]
    if sparse:
        raise ReplaySourceError(f"replay_contract_coverage_too_low:{','.join(sparse)}")

    data_as_of = max(quote.received_at for quote in quotes)
    if data_as_of > requested:
        raise ReplaySourceError("replay_lookahead_row_selected")
    transport_ages = [
        (requested - quote.received_at).total_seconds() for quote in quotes
    ]
    observation_ages = [
        (requested - _observation_time(quote)).total_seconds() for quote in quotes
    ]
    if min(transport_ages) < 0 or min(observation_ages) < 0:
        raise ReplaySourceError("replay_source_clock_after_cutoff_selected")
    state = LatestState(
        created_at=data_as_of,
        as_of=requested,
        quotes=quotes,
        best_quotes=select_best_quotes(quotes, as_of=requested),
    )
    return ReplayLoadResult(
        state=state,
        provider=provider,
        requested_as_of=requested,
        data_as_of=data_as_of,
        window_start=window_start,
        lookback_seconds=lookback_seconds,
        source_files=relative_paths,
        source_file_sha256=source_hashes_before,
        lake_schema_versions=lake_schema_versions,
        lake_writer_versions=lake_writer_versions,
        raw_source_file_sha256=raw_source_file_sha256,
        compacted_at=compacted_at,
        selected_quote_count=len(quotes),
        selected_expiry_counts=expiry_counts,
        max_transport_age_seconds=max(transport_ages),
        max_observation_age_seconds=max(observation_ages),
        min_observation_age_seconds=min(observation_ages),
        selection_audit=selection_audit,
    )


def _projection_policy(settings: StorageSettings) -> dict[str, object]:
    return {
        "latest_stale_after_seconds": settings.latest_stale_after_seconds,
        "slow_index_stale_after_seconds": settings.slow_index_stale_after_seconds,
        "slow_index_labels": sorted(settings.slow_index_labels),
        "delayed_stale_after_seconds": settings.delayed_stale_after_seconds,
        "rotation_stale_after_seconds": settings.rotation_stale_after_seconds,
        "provider_priority": list(settings.provider_priority),
    }


def build_replay_snapshot(
    loaded: ReplayLoadResult,
    *,
    storage_settings: StorageSettings,
    generated_at: datetime | None = None,
) -> dict[str, object]:
    """Wrap a historical projection in an immutable, non-live contract."""

    projection = build_surface_projection(
        loaded.state,
        storage_settings=storage_settings,
    )
    published = projection["expiries"]
    quality = dict(projection["quality"])
    if projection["status"] == "unavailable" or not isinstance(published, list):
        raise ReplaySourceError("replay_surface_projection_unavailable")
    if len(published) != quality["requested_expiry_count"]:
        raise ReplaySourceError("replay_front_next_projection_incomplete")
    generated = as_utc(generated_at or datetime.now(tz=timezone.utc))
    if generated < loaded.data_as_of:
        raise ReplaySourceError("replay_generated_before_data")
    session_date = loaded.requested_as_of.date().isoformat()
    projection_policy = _projection_policy(storage_settings)
    selection_audit = loaded.selection_audit
    payload: dict[str, object] = {
        "schema_version": DASHBOARD_SCHEMA_VERSION,
        "kind": REPLAY_KIND,
        "mode": REPLAY_MODE,
        "policy_version": REPLAY_POLICY_VERSION,
        "replay_id": replay_id(loaded.requested_as_of),
        "session_date": session_date,
        "requested_as_of": loaded.requested_as_of.isoformat(),
        "data_as_of": loaded.data_as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "frozen": True,
        "automatic_ordering": False,
        "surface_version": SURFACE_SCHEMA_VERSION,
        "status": projection["status"],
        "source": {
            "dataset": QUOTE_LAKE_DATASET,
            "provider": loaded.provider.value,
            "source_files": list(loaded.source_files),
            "parquet_file_sha256": loaded.source_file_sha256,
            "lake_schema_versions": list(loaded.lake_schema_versions),
            "lake_writer_versions": list(loaded.lake_writer_versions),
            "raw_source_file_sha256": loaded.raw_source_file_sha256,
            "compacted_at": list(loaded.compacted_at),
            "compacted_at_available": bool(loaded.compacted_at),
            "duckdb_version": duckdb.__version__,
            "structure_clock_available": False,
            "source_files_verified_unchanged_during_read": True,
            "cutoff_fields": [
                "received_at",
                "source_at",
                "quote_time",
                "trade_time",
                "last_update_at",
            ],
            "cutoff_rule": (
                "received_at_and_available_source_clocks_lte_requested_as_of"
            ),
            "availability_clock_available": False,
            "availability_clock": "unavailable",
            "point_in_time_confidence": "bounded_not_proven",
            "effective_available_at_rule": (
                "max_available_source_clocks_lte_requested_as_of"
            ),
            "known_limitations": [
                "response_finished_at_unavailable",
                "received_at_is_cycle_started_at",
            ],
            "selection_rule": (
                "latest_complete_row_per_instrument_by_available_clocks_"
                "then_surface_input_completeness"
            ),
            "replay_loader_field_stitching": False,
            "lookahead_rows_selected": 0,
            "raw_candidate_count": selection_audit.raw_candidate_count,
            "source_clock_rows_excluded": (
                selection_audit.source_clock_rows_excluded
            ),
            "eligible_candidate_count": selection_audit.eligible_candidate_count,
            "duplicate_received_at_group_count": (
                selection_audit.duplicate_received_at_group_count
            ),
            "duplicate_received_at_row_count": (
                selection_audit.duplicate_received_at_row_count
            ),
            "resolved_by_surface_completeness_instrument_count": (
                selection_audit.resolved_by_surface_completeness_instrument_count
            ),
            "ambiguous_top_instrument_count": (
                selection_audit.ambiguous_top_instrument_count
            ),
            "dropped_ambiguous_instrument_count": (
                selection_audit.dropped_ambiguous_instrument_count
            ),
            "identical_top_duplicate_row_count": (
                selection_audit.identical_top_duplicate_row_count
            ),
            "window_start": loaded.window_start.isoformat(),
            "lookback_seconds": loaded.lookback_seconds,
            "selected_quote_count": loaded.selected_quote_count,
            "selected_expiry_counts": loaded.selected_expiry_counts,
            "max_transport_age_seconds": loaded.max_transport_age_seconds,
            "max_observation_age_seconds": loaded.max_observation_age_seconds,
            "min_observation_age_seconds": loaded.min_observation_age_seconds,
            "coordinate": "SPX",
            "trading_class": "SPXW",
        },
        "projection_policy": projection_policy,
        "projection_policy_sha256": _canonical_sha256(projection_policy),
        "session": projection["session"],
        "underlier": projection["underlier"],
        "quality": quality,
        "expiries": published,
    }
    payload["artifact_sha256"] = _canonical_sha256(payload)
    return payload


def generate_replay(
    *,
    as_of: datetime,
    data_root: str | Path,
    storage_settings: StorageSettings,
    output_path: str | Path,
    lookback_seconds: float = DEFAULT_LOOKBACK_SECONDS,
    generated_at: datetime | None = None,
    force: bool = False,
) -> dict[str, object]:
    destination = Path(output_path).expanduser()
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        with exclusive_state_lock(destination, timeout_seconds=0.0):
            if destination.exists() and not force:
                raise ReplaySourceError("replay_output_already_exists")
            loaded = load_replay_state(
                data_root=data_root,
                as_of=as_of,
                lookback_seconds=lookback_seconds,
            )
            payload = build_replay_snapshot(
                loaded,
                storage_settings=storage_settings,
                generated_at=generated_at,
            )
            write_dashboard_snapshot(payload, output_path=destination)
            return payload
    except TimeoutError as exc:
        raise ReplaySourceError("replay_generation_locked") from exc


def _parse_as_of(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError("--as-of must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("--as-of must include a timezone")
    return as_utc(parsed)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build one immutable SPXW surface replay from the quote lake."
    )
    parser.add_argument("--as-of", type=_parse_as_of, required=True)
    parser.add_argument("--data-root", type=Path)
    parser.add_argument("--output-path", type=Path)
    parser.add_argument(
        "--lookback-seconds",
        type=float,
        default=DEFAULT_LOOKBACK_SECONDS,
    )
    parser.add_argument("--json", action="store_true")
    parser.add_argument(
        "--force",
        action="store_true",
        help="replace an existing replay only after an explicit audit",
    )
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = StorageSettings.from_env()
    data_root = args.data_root or Path(settings.data_root)
    output_path = args.output_path or default_replay_output_path(
        data_root,
        as_of=args.as_of,
    )
    payload = generate_replay(
        as_of=args.as_of,
        data_root=data_root,
        storage_settings=settings,
        output_path=output_path,
        lookback_seconds=args.lookback_seconds,
        force=args.force,
    )
    if args.json:
        print(json.dumps(payload, sort_keys=True, allow_nan=False), flush=True)
    else:
        print(f"{payload['status']} {payload['replay_id']} {output_path}", flush=True)
    return 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
