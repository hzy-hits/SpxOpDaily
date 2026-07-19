"""Read-only HTTP service for point-in-time SPXW surface session replay."""

from __future__ import annotations

import hashlib
import hmac
import json
import re
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from http import HTTPStatus
from pathlib import Path

import duckdb

from spx_spark.config import StorageSettings
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR, ET
from spx_spark.marketdata import as_utc
from spx_spark.state_io import atomic_write_json_secure
from spx_spark.surface_dashboard_replay import (
    DEFAULT_LOOKBACK_SECONDS,
    MAX_LOOKBACK_SECONDS,
    MIN_CONTRACTS_PER_EXPIRY,
    QUOTE_LAKE_DATASET,
    REPLAY_KIND,
    REPLAY_MODE,
    REPLAY_POLICY_VERSION,
    _canonical_sha256,
    _projection_policy,
    _sha256,
    generate_replay,
    replay_id,
)
from spx_spark.surface_replay_http import (
    APIResponse,
    DEFAULT_BIND_HOST,
    DEFAULT_BIND_PORT,
    DEFAULT_FRAME_MINUTES,
    MAX_REQUEST_TARGET_BYTES,
    SERVICE_SCHEMA_VERSION,
    ReplayAPI,
    ReplayBusyError,
    ReplayCacheError,
    ReplayHTTPServer,
    ReplayRequestError,
    ReplayUnixHTTPServer,
    _parse_replay_id,
    main,
    parse_args,
    run,
)
from spx_spark.surface_replay_trend import (
    ReplayTrendBusyError,
    ReplayTrendCacheError,
    TrendContext,
    TrendSelector,
    materialize_trend,
)
from spx_spark.surface_replay_session import (
    ReplaySessionSurfaceBusyError,
    ReplaySessionSurfaceCacheError,
    SESSION_SURFACE_LOCK_TIMEOUT_SECONDS,
    SessionSurfaceBuildCache,
    SessionSurfaceSelector,
    materialize_session_surface,
)


SERVICE_KIND = "spxw_surface_replay_catalog"
TIMELINE_POLICY_VERSION = "spxw_surface_replay_timeline.event_driven.v1"
SESSION_CLOSE_GRACE_SECONDS = 2 * 60 * 60
SESSION_CLOSE_GRACE_POLICY = "session_close_plus_2h_grace"
MAX_CACHE_ARTIFACT_BYTES = 32 * 1024 * 1024
GENERATION_LOCK_TIMEOUT_SECONDS = 2.0

_SESSION_DATE_RE = re.compile(r"\A\d{4}-\d{2}-\d{2}\Z")
__all__ = [
    "APIResponse", "DEFAULT_BIND_HOST", "DEFAULT_BIND_PORT", "DEFAULT_FRAME_MINUTES",
    "MAX_REQUEST_TARGET_BYTES", "ReplayAPI", "ReplayBusyError", "ReplayCacheError",
    "ReplayCatalog", "ReplayHTTPServer", "ReplayRequestError", "ReplaySession",
    "ReplayUnixHTTPServer", "main", "parse_args", "run",
]


@dataclass(frozen=True, slots=True)
class ReplaySession:
    session_date: date
    open_at: datetime
    close_at: datetime
    source_paths: tuple[Path, ...]


class ReplayCatalog:
    """Discover grace-elapsed sessions and cache known-clock-validated frames."""

    def __init__(
        self,
        *,
        data_root: str | Path,
        storage_settings: StorageSettings,
        frame_minutes: int = DEFAULT_FRAME_MINUTES,
        lookback_seconds: float = DEFAULT_LOOKBACK_SECONDS,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if isinstance(frame_minutes, bool) or not 1 <= frame_minutes <= 60:
            raise ValueError("frame_minutes must be within [1, 60]")
        if not 0 < lookback_seconds <= MAX_LOOKBACK_SECONDS:
            raise ValueError(
                f"lookback_seconds must be within (0, {MAX_LOOKBACK_SECONDS:g}]"
            )
        self.data_root = Path(data_root).expanduser().resolve()
        self.storage_settings = storage_settings
        self.frame_minutes = int(frame_minutes)
        self.lookback_seconds = float(lookback_seconds)
        self._clock = clock or (lambda: datetime.now(tz=timezone.utc))
        self.projection_policy = _projection_policy(storage_settings)
        self.projection_policy_sha256 = _canonical_sha256(self.projection_policy)
        self._locks_guard = threading.Lock()
        self._session_locks: dict[date, threading.Lock] = {}
        self._timeline_memory: dict[date, tuple[str, tuple[datetime, ...]]] = {}
        # Surface generation reads and hashes large Parquet partitions. Keep only
        # one uncached build active so concurrent browser prefetches cannot exhaust RAM.
        self._generation_lock = threading.BoundedSemaphore(value=1)
        self._trend_lock = threading.BoundedSemaphore(value=1)
        self._session_surface_lock = threading.BoundedSemaphore(value=1)
        self._session_surface_build_cache = SessionSurfaceBuildCache()
        self._scan_lock = threading.Lock()

    @property
    def quote_lake_root(self) -> Path:
        return self.data_root / QUOTE_LAKE_DATASET

    @property
    def catalog_root(self) -> Path:
        return self.data_root / "published" / "spxw-surface" / "replay-catalog"

    def _lock_for_session(self, session_date: date) -> threading.Lock:
        with self._locks_guard:
            return self._session_locks.setdefault(session_date, threading.Lock())

    @staticmethod
    def _close_grace_elapsed_at(session: ReplaySession) -> datetime:
        return as_utc(session.close_at) + timedelta(
            seconds=SESSION_CLOSE_GRACE_SECONDS
        )

    def _close_grace_elapsed(self, session: ReplaySession) -> bool:
        return as_utc(self._clock()) >= self._close_grace_elapsed_at(session)

    def _source_paths(self, session_date: date) -> tuple[Path, ...]:
        provider_root = (
            self.quote_lake_root
            / f"date={session_date.isoformat()}"
            / "provider=schwab"
        )
        if not provider_root.is_dir():
            return ()
        source_paths: list[Path] = []
        for candidate in provider_root.glob("hour=*/quotes.parquet"):
            try:
                resolved = candidate.resolve(strict=True)
            except OSError:
                continue
            if not resolved.is_file() or not resolved.is_relative_to(self.quote_lake_root):
                continue
            hour_part = resolved.parent.name
            if not re.fullmatch(r"hour=\d{2}", hour_part):
                continue
            source_paths.append(resolved)
        return tuple(sorted(source_paths))

    def _relevant_paths(self, session: ReplaySession) -> tuple[Path, ...]:
        start = as_utc(session.open_at) - timedelta(seconds=self.lookback_seconds)
        end = as_utc(session.close_at)
        relevant: list[Path] = []
        for path in session.source_paths:
            hour_raw = path.parent.name.removeprefix("hour=")
            try:
                hour = int(hour_raw)
            except ValueError:
                continue
            partition_start = datetime.combine(
                session.session_date,
                datetime.min.time(),
                tzinfo=timezone.utc,
            ).replace(hour=hour)
            partition_end = partition_start + timedelta(hours=1)
            if partition_end >= start and partition_start <= end:
                relevant.append(path)
        return tuple(relevant)

    def discover_sessions(self) -> tuple[ReplaySession, ...]:
        sessions: list[ReplaySession] = []
        if not self.quote_lake_root.is_dir():
            return ()
        for candidate in self.quote_lake_root.glob("date=*"):
            if not candidate.is_dir():
                continue
            raw_date = candidate.name.removeprefix("date=")
            if not _SESSION_DATE_RE.fullmatch(raw_date):
                continue
            try:
                session_date = date.fromisoformat(raw_date)
            except ValueError:
                continue
            market_session = DEFAULT_MARKET_CALENDAR.session(session_date)
            if market_session is None:
                continue
            source_paths = self._source_paths(session_date)
            if not source_paths:
                continue
            replay_session = ReplaySession(
                session_date=session_date,
                open_at=market_session.open_at,
                close_at=market_session.close_at,
                source_paths=source_paths,
            )
            if self._close_grace_elapsed(replay_session) and self._relevant_paths(
                replay_session
            ):
                sessions.append(replay_session)
        return tuple(sorted(sessions, key=lambda item: item.session_date, reverse=True))

    def get_session(self, session_date: date) -> ReplaySession:
        market_session = DEFAULT_MARKET_CALENDAR.session(session_date)
        if market_session is None:
            raise ReplayRequestError("replay_session_not_found", status=HTTPStatus.NOT_FOUND)
        source_paths = self._source_paths(session_date)
        session = ReplaySession(
            session_date=session_date,
            open_at=market_session.open_at,
            close_at=market_session.close_at,
            source_paths=source_paths,
        )
        if (
            not source_paths
            or not self._relevant_paths(session)
            or not self._close_grace_elapsed(session)
        ):
            raise ReplayRequestError("replay_session_not_found", status=HTTPStatus.NOT_FOUND)
        return session

    def _manifest_path(self, session_date: date) -> Path:
        return (
            self.catalog_root
            / f"session={session_date.isoformat()}"
            / f"timeline-{self.frame_minutes}m.json"
        )

    def _source_fingerprint(self, paths: tuple[Path, ...]) -> str:
        rows: list[dict[str, object]] = []
        for path in paths:
            stat = path.stat()
            rows.append(
                {
                    "path": str(path.relative_to(self.data_root)),
                    "device": stat.st_dev,
                    "inode": stat.st_ino,
                    "size": stat.st_size,
                    "mtime_ns": stat.st_mtime_ns,
                    "ctime_ns": stat.st_ctime_ns,
                }
            )
        encoded = json.dumps(rows, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def _scan_viable_frames(
        self,
        session: ReplaySession,
        *,
        paths: tuple[Path, ...],
    ) -> tuple[datetime, ...]:
        if not paths:
            return ()
        expiries = DEFAULT_MARKET_CALENDAR.research_expiries(session.open_at)
        if len(expiries) < 2:
            return ()
        session_open = as_utc(session.open_at)
        session_close = as_utc(session.close_at)
        query = """
            WITH data AS MATERIALIZED (
                SELECT
                    received_at,
                    source_at,
                    quote_time,
                    trade_time,
                    last_update_at,
                    instrument_id,
                    expiry,
                    trading_class
                FROM read_parquet(?, union_by_name=true)
                WHERE provider = 'schwab'
                  AND received_at BETWEEN (
                        ?::TIMESTAMPTZ - (?::DOUBLE * INTERVAL '1 second')
                  ) AND ?::TIMESTAMPTZ
            ),
            raw_next_groups AS (
                SELECT
                    received_at AS anchor_received_at,
                    MAX(GREATEST(
                            received_at,
                            COALESCE(source_at, received_at),
                            COALESCE(quote_time, received_at),
                            COALESCE(trade_time, received_at),
                            COALESCE(last_update_at, received_at)
                    )) AS max_clock
                FROM data
                WHERE trading_class = 'SPXW'
                  AND expiry = ?::DATE
                GROUP BY received_at
                HAVING COUNT(DISTINCT instrument_id) >= ?
            ),
            next_groups AS (
                SELECT DISTINCT
                    CASE
                        WHEN max_clock = DATE_TRUNC('second', max_clock)
                        THEN max_clock
                        ELSE DATE_TRUNC('second', max_clock) + INTERVAL '1 second'
                    END AS candidate_at
                FROM raw_next_groups
            ),
            eligible AS (
                SELECT
                    groups.candidate_at,
                    quotes.instrument_id,
                    quotes.expiry
                FROM next_groups AS groups
                JOIN data AS quotes
                  ON quotes.received_at BETWEEN (
                        groups.candidate_at - (?::DOUBLE * INTERVAL '1 second')
                  ) AND groups.candidate_at
                WHERE NOT (
                        COALESCE(quotes.source_at > groups.candidate_at, FALSE)
                        OR COALESCE(quotes.quote_time > groups.candidate_at, FALSE)
                        OR COALESCE(quotes.trade_time > groups.candidate_at, FALSE)
                        OR COALESCE(quotes.last_update_at > groups.candidate_at, FALSE)
                )
                  AND (
                        quotes.instrument_id = 'index:SPX'
                        OR (
                            quotes.trading_class = 'SPXW'
                            AND quotes.expiry IN (?::DATE, ?::DATE)
                        )
                  )
            ),
            coverage AS (
                SELECT
                    candidate_at,
                    COUNT(DISTINCT instrument_id) FILTER (
                        WHERE expiry = ?::DATE
                    ) AS front_count,
                    COUNT(DISTINCT instrument_id) FILTER (
                        WHERE expiry = ?::DATE
                    ) AS next_count,
                    COUNT(DISTINCT instrument_id) FILTER (
                        WHERE instrument_id = 'index:SPX'
                    ) AS spx_count
                FROM eligible
                GROUP BY candidate_at
            ),
            candidates AS (
                SELECT
                    TIME_BUCKET(
                        CAST(? AS INTERVAL),
                        candidate_at,
                        ?::TIMESTAMPTZ
                    ) AS bucket_at,
                    candidate_at
                FROM coverage
                WHERE front_count >= ?
                  AND next_count >= ?
                  AND spx_count >= 1
                  AND candidate_at >= ?::TIMESTAMPTZ
                  AND candidate_at < ?::TIMESTAMPTZ
            )
            SELECT bucket_at, candidate_at
            FROM candidates
            ORDER BY bucket_at, candidate_at DESC
        """
        parameters: list[object] = [
            [str(path) for path in paths],
            session_open,
            self.lookback_seconds,
            session_close,
            expiries[1],
            MIN_CONTRACTS_PER_EXPIRY,
            self.lookback_seconds,
            expiries[0],
            expiries[1],
            expiries[0],
            expiries[1],
            f"{self.frame_minutes} minutes",
            session_open,
            MIN_CONTRACTS_PER_EXPIRY,
            MIN_CONTRACTS_PER_EXPIRY,
            session_open,
            session_close,
        ]
        with self._scan_lock:
            connection = duckdb.connect()
            try:
                connection.execute("SET TimeZone='UTC'")
                connection.execute("SET threads=1")
                rows = connection.execute(query, parameters).fetchall()
            finally:
                connection.close()

        buckets: dict[datetime, list[datetime]] = {}
        for bucket_at, candidate_at in rows:
            if not isinstance(bucket_at, datetime) or not isinstance(candidate_at, datetime):
                continue
            buckets.setdefault(as_utc(bucket_at), []).append(as_utc(candidate_at))

        selected: list[datetime] = []
        for candidates in buckets.values():
            if candidates:
                selected.append(candidates[0])
        return tuple(selected)

    def _ensure_materialized_frame(self, requested: datetime) -> dict[str, object]:
        source_paths, source_fingerprint = self._frame_source_context(requested)
        cache_path = self._cache_path(
            requested,
            source_fingerprint=source_fingerprint,
        )
        if cache_path.is_file():
            return self._read_cached_frame(
                cache_path,
                requested=requested,
                source_paths=source_paths,
                source_fingerprint=source_fingerprint,
            )
        if not self._generation_lock.acquire(timeout=GENERATION_LOCK_TIMEOUT_SECONDS):
            raise ReplayBusyError("replay_generation_busy")
        try:
            # The lake may have been compacted while this request waited. Re-key
            # against the current close-grace-elapsed source fingerprint.
            source_paths, source_fingerprint = self._frame_source_context(requested)
            cache_path = self._cache_path(
                requested,
                source_fingerprint=source_fingerprint,
            )
            if cache_path.is_file():
                return self._read_cached_frame(
                    cache_path,
                    requested=requested,
                    source_paths=source_paths,
                    source_fingerprint=source_fingerprint,
                )
            generate_replay(
                as_of=requested,
                data_root=self.data_root,
                storage_settings=self.storage_settings,
                output_path=cache_path,
                lookback_seconds=self.lookback_seconds,
            )
            return self._read_cached_frame(
                cache_path,
                requested=requested,
                source_paths=source_paths,
                source_fingerprint=source_fingerprint,
            )
        finally:
            self._generation_lock.release()

    def _read_manifest(
        self,
        path: Path,
        *,
        session_date: date,
        source_fingerprint: str,
    ) -> tuple[datetime, ...] | None:
        try:
            if path.stat().st_size > 4 * 1024 * 1024:
                return None
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        if not isinstance(payload, dict):
            return None
        expected = {
            "schema_version": SERVICE_SCHEMA_VERSION,
            "timeline_policy_version": TIMELINE_POLICY_VERSION,
            "session_date": session_date.isoformat(),
            "frame_minutes": self.frame_minutes,
            "lookback_seconds": self.lookback_seconds,
            "source_fingerprint": source_fingerprint,
            "projection_policy_sha256": self.projection_policy_sha256,
        }
        if any(payload.get(key) != value for key, value in expected.items()):
            return None
        raw_frames = payload.get("frames")
        if not isinstance(raw_frames, list):
            return None
        parsed: list[datetime] = []
        try:
            for value in raw_frames:
                if not isinstance(value, str):
                    return None
                parsed.append(_parse_replay_id(value))
        except ReplayRequestError:
            return None
        return tuple(parsed)

    def _write_manifest(
        self,
        path: Path,
        *,
        session: ReplaySession,
        source_fingerprint: str,
        frames: tuple[datetime, ...],
    ) -> None:
        atomic_write_json_secure(
            path,
            {
                "schema_version": SERVICE_SCHEMA_VERSION,
                "kind": SERVICE_KIND,
                "timeline_policy_version": TIMELINE_POLICY_VERSION,
                "session_date": session.session_date.isoformat(),
                "frame_minutes": self.frame_minutes,
                "lookback_seconds": self.lookback_seconds,
                "source_fingerprint": source_fingerprint,
                "projection_policy_sha256": self.projection_policy_sha256,
                "availability_proven": False,
                "availability_clock": "unavailable",
                "point_in_time_confidence": "bounded_not_proven",
                "frame_validation": "known_clock_validation_on_frame_request",
                "timeline_selection": "latest_coverage_candidate_per_bucket",
                "indexed_at": datetime.now(tz=timezone.utc).isoformat(),
                "frames": [replay_id(frame) for frame in frames],
            },
        )

    def viable_frames(self, session_date: date) -> tuple[datetime, ...]:
        session = self.get_session(session_date)
        paths = self._relevant_paths(session)
        fingerprint = self._source_fingerprint(paths)
        manifest_path = self._manifest_path(session_date)
        with self._lock_for_session(session_date):
            memory_value = self._timeline_memory.get(session_date)
            if memory_value is not None and memory_value[0] == fingerprint:
                return memory_value[1]
            cached = self._read_manifest(
                manifest_path,
                session_date=session_date,
                source_fingerprint=fingerprint,
            )
            if cached is not None:
                self._timeline_memory[session_date] = (fingerprint, cached)
                return cached
            frames = self._scan_viable_frames(session, paths=paths)
            self._write_manifest(
                manifest_path,
                session=session,
                source_fingerprint=fingerprint,
                frames=frames,
            )
            self._timeline_memory[session_date] = (fingerprint, frames)
            return frames

    def _frame_source_context(
        self,
        requested: datetime,
    ) -> tuple[tuple[Path, ...], str]:
        session = self.get_session(as_utc(requested).astimezone(ET).date())
        source_paths = self._relevant_paths(session)
        return source_paths, self._source_fingerprint(source_paths)

    def _cache_path(
        self,
        requested: datetime,
        *,
        source_fingerprint: str | None = None,
    ) -> Path:
        policy_path = REPLAY_POLICY_VERSION.replace("spxw_surface_replay.", "policy=")
        fingerprint = source_fingerprint
        if fingerprint is None:
            _source_paths, fingerprint = self._frame_source_context(requested)
        lookback_label = format(self.lookback_seconds, ".15g").replace(".", "p")
        return (
            self.data_root
            / "published"
            / "spxw-surface"
            / "replay-cache"
            / policy_path
            / f"lookback={lookback_label}s"
            / f"projection={self.projection_policy_sha256}"
            / f"source={fingerprint}"
            / f"{replay_id(requested)}.json"
        )

    def _read_cached_frame(
        self,
        path: Path,
        *,
        requested: datetime,
        source_paths: tuple[Path, ...],
        source_fingerprint: str,
        verified_source_hashes: Mapping[str, str] | None = None,
    ) -> dict[str, object]:
        if self._source_fingerprint(source_paths) != source_fingerprint:
            raise ReplayCacheError("replay_cache_source_context_changed")
        try:
            stat = path.stat()
            if stat.st_size <= 0 or stat.st_size > MAX_CACHE_ARTIFACT_BYTES:
                raise ReplayCacheError("replay_cache_size_invalid")
            payload = json.loads(path.read_text(encoding="utf-8"))
        except ReplayCacheError:
            raise
        except (OSError, json.JSONDecodeError) as exc:
            raise ReplayCacheError("replay_cache_unreadable") from exc
        if not isinstance(payload, dict):
            raise ReplayCacheError("replay_cache_contract_invalid")
        expected = {
            "kind": REPLAY_KIND,
            "mode": REPLAY_MODE,
            "policy_version": REPLAY_POLICY_VERSION,
            "replay_id": replay_id(requested),
            "session_date": requested.astimezone(ET).date().isoformat(),
            "requested_as_of": requested.isoformat(),
            "frozen": True,
            "automatic_ordering": False,
        }
        if any(payload.get(key) != value for key, value in expected.items()):
            raise ReplayCacheError("replay_cache_contract_invalid")
        if payload.get("projection_policy_sha256") != self.projection_policy_sha256:
            raise ReplayCacheError("replay_cache_projection_policy_mismatch")
        projection_policy = payload.get("projection_policy")
        if not isinstance(projection_policy, Mapping) or (
            _canonical_sha256(dict(projection_policy)) != self.projection_policy_sha256
        ):
            raise ReplayCacheError("replay_cache_projection_policy_hash_mismatch")
        stored_hash = payload.get("artifact_sha256")
        if not isinstance(stored_hash, str) or not re.fullmatch(r"[0-9a-f]{64}", stored_hash):
            raise ReplayCacheError("replay_cache_hash_invalid")
        unsigned = dict(payload)
        unsigned.pop("artifact_sha256", None)
        actual_hash = _canonical_sha256(unsigned)
        if not hmac.compare_digest(stored_hash, actual_hash):
            raise ReplayCacheError("replay_cache_hash_mismatch")
        source = payload.get("source")
        if not isinstance(source, Mapping):
            raise ReplayCacheError("replay_cache_source_contract_invalid")
        payload_lookback = source.get("lookback_seconds")
        if (
            isinstance(payload_lookback, bool)
            or not isinstance(payload_lookback, int | float)
            or float(payload_lookback) != self.lookback_seconds
        ):
            raise ReplayCacheError("replay_cache_lookback_mismatch")
        source_files = source.get("source_files")
        parquet_hashes = source.get("parquet_file_sha256")
        if (
            not isinstance(source_files, list)
            or not source_files
            or not all(isinstance(value, str) and value for value in source_files)
            or len(source_files) != len(set(source_files))
            or not isinstance(parquet_hashes, Mapping)
            or set(parquet_hashes) != set(source_files)
        ):
            raise ReplayCacheError("replay_cache_source_contract_invalid")
        current_paths = {
            str(source_path.relative_to(self.data_root)): source_path
            for source_path in source_paths
        }
        if verified_source_hashes is not None and set(verified_source_hashes) != set(
            current_paths
        ):
            raise ReplayCacheError("replay_cache_verified_source_context_invalid")
        for relative_path in source_files:
            source_path = current_paths.get(relative_path)
            expected_hash = parquet_hashes.get(relative_path)
            if (
                source_path is None
                or not isinstance(expected_hash, str)
                or not re.fullmatch(r"[0-9a-f]{64}", expected_hash)
            ):
                raise ReplayCacheError("replay_cache_source_file_mismatch")
            actual_source_hash = (
                verified_source_hashes.get(relative_path)
                if verified_source_hashes is not None
                else _sha256(source_path)
            )
            if not isinstance(actual_source_hash, str) or not re.fullmatch(
                r"[0-9a-f]{64}", actual_source_hash
            ):
                raise ReplayCacheError("replay_cache_verified_source_hash_invalid")
            if not hmac.compare_digest(expected_hash, actual_source_hash):
                raise ReplayCacheError("replay_cache_source_hash_mismatch")
        if self._source_fingerprint(source_paths) != source_fingerprint:
            raise ReplayCacheError("replay_cache_source_changed_during_validation")
        return payload

    def frame(self, session_date: date, requested: datetime) -> dict[str, object]:
        requested = as_utc(requested)
        if requested.astimezone(ET).date() != session_date:
            raise ReplayRequestError("replay_at_session_mismatch")
        viable = self.viable_frames(session_date)
        if requested not in viable:
            raise ReplayRequestError(
                "replay_frame_not_found",
                status=HTTPStatus.NOT_FOUND,
            )
        return self._ensure_materialized_frame(requested)

    def trend(
        self,
        session_date: date,
        *,
        role: str,
        weighting: str,
        metric: str,
    ) -> dict[str, object]:
        selector = TrendSelector(role=role, weighting=weighting, metric=metric)
        session = self.get_session(session_date)
        frames = self.viable_frames(session_date)
        source_paths = self._relevant_paths(session)
        source_fingerprint = self._source_fingerprint(source_paths)
        context = TrendContext(
            data_root=self.data_root,
            session_date=session_date,
            open_at=session.open_at,
            close_at=session.close_at,
            close_grace_elapsed_at=self._close_grace_elapsed_at(session),
            close_grace_policy=SESSION_CLOSE_GRACE_POLICY,
            close_grace_seconds=SESSION_CLOSE_GRACE_SECONDS,
            frame_minutes=self.frame_minutes,
            lookback_seconds=self.lookback_seconds,
            timeline_policy_version=TIMELINE_POLICY_VERSION,
            projection_policy=self.projection_policy,
            projection_policy_sha256=self.projection_policy_sha256,
            source_paths=source_paths,
            source_fingerprint=source_fingerprint,
            frames=frames,
        )

        def current_source_fingerprint() -> str:
            current_session = self.get_session(session_date)
            return self._source_fingerprint(self._relevant_paths(current_session))

        if not self._trend_lock.acquire(timeout=GENERATION_LOCK_TIMEOUT_SECONDS):
            raise ReplayBusyError("replay_trend_generation_busy")
        try:
            return materialize_trend(
                context=context,
                selector=selector,
                frame_loader=lambda requested: self.frame(session_date, requested),
                current_source_fingerprint=current_source_fingerprint,
            )
        except ReplayTrendBusyError as exc:
            raise ReplayBusyError("replay_trend_generation_busy") from exc
        except ReplayTrendCacheError as exc:
            raise ReplayCacheError(str(exc)) from exc
        finally:
            self._trend_lock.release()

    def session_surface(
        self,
        session_date: date,
        *,
        at: datetime,
        role: str,
        weighting: str,
        bucket_minutes: int,
        price_step: float,
    ) -> dict[str, object]:
        selector = SessionSurfaceSelector(
            role=role,
            weighting=weighting,
            bucket_minutes=bucket_minutes,
            price_step=price_step,
        )
        requested = as_utc(at)
        if requested.microsecond:
            raise ReplayRequestError("replay_at_subsecond_not_supported")
        if requested.astimezone(ET).date() != session_date:
            raise ReplayRequestError("replay_at_session_mismatch")
        session = self.get_session(session_date)
        if not as_utc(session.open_at) <= requested <= as_utc(session.close_at):
            raise ReplayRequestError("replay_at_outside_session")
        frames = self.viable_frames(session_date)
        source_paths = self._relevant_paths(session)
        source_fingerprint = self._source_fingerprint(source_paths)
        context = TrendContext(
            data_root=self.data_root,
            session_date=session_date,
            open_at=session.open_at,
            close_at=session.close_at,
            close_grace_elapsed_at=self._close_grace_elapsed_at(session),
            close_grace_policy=SESSION_CLOSE_GRACE_POLICY,
            close_grace_seconds=SESSION_CLOSE_GRACE_SECONDS,
            frame_minutes=self.frame_minutes,
            lookback_seconds=self.lookback_seconds,
            timeline_policy_version=TIMELINE_POLICY_VERSION,
            projection_policy=self.projection_policy,
            projection_policy_sha256=self.projection_policy_sha256,
            source_paths=source_paths,
            source_fingerprint=source_fingerprint,
            frames=frames,
        )

        def current_source_fingerprint() -> str:
            current_session = self.get_session(session_date)
            return self._source_fingerprint(self._relevant_paths(current_session))

        verified_source_hashes: dict[str, str] | None = None

        def session_frame_loader(frame_at: datetime) -> dict[str, object]:
            nonlocal verified_source_hashes
            cache_path = self._cache_path(
                frame_at,
                source_fingerprint=source_fingerprint,
            )
            if not cache_path.is_file():
                return self._ensure_materialized_frame(frame_at)
            if verified_source_hashes is None:
                verified_source_hashes = {
                    str(path.relative_to(self.data_root)): _sha256(path)
                    for path in source_paths
                }
            return self._read_cached_frame(
                cache_path,
                requested=frame_at,
                source_paths=source_paths,
                source_fingerprint=source_fingerprint,
                verified_source_hashes=verified_source_hashes,
            )

        if not self._session_surface_lock.acquire(
            timeout=SESSION_SURFACE_LOCK_TIMEOUT_SECONDS
        ):
            raise ReplayBusyError("replay_session_surface_generation_busy")
        try:
            return materialize_session_surface(
                context=context,
                as_of=requested,
                selector=selector,
                frame_loader=session_frame_loader,
                current_source_fingerprint=current_source_fingerprint,
                build_cache=self._session_surface_build_cache,
            )
        except ReplaySessionSurfaceBusyError as exc:
            raise ReplayBusyError("replay_session_surface_generation_busy") from exc
        except ReplaySessionSurfaceCacheError as exc:
            raise ReplayCacheError(str(exc)) from exc
        finally:
            self._session_surface_lock.release()

    def sessions_payload(self) -> dict[str, object]:
        sessions = self.discover_sessions()
        rows: list[dict[str, object]] = []
        for item in sessions:
            manifest_path = self._manifest_path(item.session_date)
            frame_count: int | None = None
            cached_frame_count = 0
            relevant_paths = self._relevant_paths(item)
            try:
                fingerprint = self._source_fingerprint(relevant_paths)
                indexed_frames = self._read_manifest(
                    manifest_path,
                    session_date=item.session_date,
                    source_fingerprint=fingerprint,
                )
            except OSError:
                indexed_frames = None
            if indexed_frames is not None:
                frame_count = len(indexed_frames)
                cached_frame_count = sum(
                    self._cache_path(
                        frame,
                        source_fingerprint=fingerprint,
                    ).is_file()
                    for frame in indexed_frames
                )
            session_date_text = item.session_date.isoformat()
            rows.append(
                {
                    "session_date": session_date_text,
                    "label": item.session_date.strftime("%a, %b %d, %Y"),
                    "open_at": as_utc(item.open_at).isoformat(),
                    "close_at": as_utc(item.close_at).isoformat(),
                    "partition_count": len(relevant_paths),
                    "frame_interval_minutes": self.frame_minutes,
                    "frame_count": frame_count,
                    "cached_frame_count": cached_frame_count,
                    "timeline_status": "indexed" if indexed_frames is not None else "on_demand",
                    "session_close_grace_elapsed": True,
                    "session_close_grace_elapsed_at": (
                        self._close_grace_elapsed_at(item).isoformat()
                    ),
                    "data_finalization_proven": False,
                    "projection_policy_sha256": self.projection_policy_sha256,
                    "timeline_url": (
                        f"/api/v1/replay/sessions/{session_date_text}/timeline"
                        f"?step_minutes={self.frame_minutes}"
                    ),
                }
            )
        return {
            "schema_version": SERVICE_SCHEMA_VERSION,
            "kind": SERVICE_KIND,
            "provider": "schwab",
            "coordinate": "SPX",
            "trading_class": "SPXW",
            "frame_interval_minutes": self.frame_minutes,
            "timeline_policy_version": TIMELINE_POLICY_VERSION,
            "availability_proven": False,
            "availability_clock": "unavailable",
            "point_in_time_confidence": "bounded_not_proven",
            "frame_validation": "known_clock_validation_on_frame_request",
            "timeline_selection": "latest_coverage_candidate_per_bucket",
            "projection_policy_sha256": self.projection_policy_sha256,
            "only_close_grace_elapsed_sessions": True,
            "session_close_grace_policy": SESSION_CLOSE_GRACE_POLICY,
            "session_close_grace_seconds": SESSION_CLOSE_GRACE_SECONDS,
            "data_finalization_proven": False,
            "default_session": rows[0]["session_date"] if rows else None,
            "sessions": rows,
        }

    def timeline_payload(self, session_date: date) -> dict[str, object]:
        session = self.get_session(session_date)
        frames = self.viable_frames(session_date)
        source_paths = self._relevant_paths(session)
        source_fingerprint = self._source_fingerprint(source_paths)
        timeline_sha256 = _canonical_sha256([replay_id(value) for value in frames])
        session_date_text = session_date.isoformat()
        frame_rows: list[dict[str, object]] = []
        for requested in frames:
            identifier = replay_id(requested)
            at = requested.strftime("%Y-%m-%dT%H:%M:%SZ")
            frame_rows.append(
                {
                    "id": identifier,
                    "replay_id": identifier,
                    "at": at,
                    "requested_as_of": at,
                    "label": requested.astimezone(ET).strftime("%H:%M:%S ET"),
                    "label_et": requested.astimezone(ET).strftime("%H:%M:%S ET"),
                    "cached": self._cache_path(
                        requested,
                        source_fingerprint=source_fingerprint,
                    ).is_file(),
                    "projection_policy_sha256": self.projection_policy_sha256,
                    "url": (
                        f"/api/v1/replay/sessions/{session_date_text}/frame?at={at}"
                    ),
                    "frame_url": f"/api/v1/replay/frames/{identifier}",
                }
            )
        return {
            "schema_version": SERVICE_SCHEMA_VERSION,
            "kind": SERVICE_KIND,
            "session_date": session_date_text,
            "provider": "schwab",
            "coordinate": "SPX",
            "trading_class": "SPXW",
            "open_at": as_utc(session.open_at).isoformat(),
            "close_at": as_utc(session.close_at).isoformat(),
            "frame_interval_minutes": self.frame_minutes,
            "step_minutes": self.frame_minutes,
            "timeline_policy_version": TIMELINE_POLICY_VERSION,
            "availability_proven": False,
            "availability_clock": "unavailable",
            "point_in_time_confidence": "bounded_not_proven",
            "frame_validation": "known_clock_validation_on_frame_request",
            "timeline_selection": "latest_coverage_candidate_per_bucket",
            "projection_policy_sha256": self.projection_policy_sha256,
            "source_fingerprint": source_fingerprint,
            "timeline_sha256": timeline_sha256,
            "session_close_grace_elapsed": True,
            "session_close_grace_elapsed_at": (
                self._close_grace_elapsed_at(session).isoformat()
            ),
            "only_close_grace_elapsed_sessions": True,
            "session_close_grace_policy": SESSION_CLOSE_GRACE_POLICY,
            "session_close_grace_seconds": SESSION_CLOSE_GRACE_SECONDS,
            "data_finalization_proven": False,
            "lookback_seconds": self.lookback_seconds,
            "frame_count": len(frame_rows),
            "frames": frame_rows,
        }


if __name__ == "__main__":
    main()
