"""Contracts, shared primitives, and bounded caches for session-surface replay."""

from __future__ import annotations

import math
import re
import threading
from collections import OrderedDict
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any

from spx_spark.features.exposure_surface import SurfaceContract
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR, ET
from spx_spark.marketdata import as_utc
from spx_spark.surface_dashboard_replay import ReplaySourceError
from spx_spark.surface_replay_trend import TrendContext


SESSION_SURFACE_SCHEMA_VERSION = 2
SESSION_SURFACE_KIND = "spxw_session_surface"
SESSION_SURFACE_MODE = "replay"
SESSION_SURFACE_POLICY_VERSION = "spxw_session_surface.v2"
SESSION_SURFACE_CACHE_VERSION = 5
SESSION_SURFACE_BUCKET_MINUTES = 5
SESSION_SURFACE_PRICE_STEP = 5.0
SESSION_SURFACE_PRICE_EXTENT_POINTS = 100.0
SESSION_SURFACE_BUCKET_OPTIONS = frozenset({5, 10, 15})
SESSION_SURFACE_PRICE_STEP_OPTIONS = (2.5, 5.0, 10.0)
MAX_SESSION_SURFACE_CACHE_ARTIFACT_BYTES = 64 * 1024 * 1024
MAX_SINGLE_BUILD_EVALUATIONS = 4_000_000
SESSION_SURFACE_LOCK_TIMEOUT_SECONDS = 15.0
SESSION_SURFACE_REFERENCE_MAX_AGE_SECONDS = 5.0
SESSION_SURFACE_GTH_QUOTE_MAX_AGE_SECONDS = 30.0
SESSION_SURFACE_BASIS_MAX_SKEW_SECONDS = 2.0

SESSION_SURFACE_ROLES = frozenset({"front", "next"})
SESSION_SURFACE_WEIGHTINGS = frozenset({"oi_weighted", "volume_weighted"})
_METRIC_TO_OUTPUT = {
    "signed_gamma": "gamma_surface",
    "gross_gamma": "gross_gamma_surface",
    "charm": "charm_surface",
    "vanna": "vanna_surface",
}
_SHA256_RE = re.compile(r"\A[0-9a-f]{64}\Z")


class ReplaySessionSurfaceCacheError(RuntimeError):
    """A session-surface artifact failed its source or self-hash contract."""


class ReplaySessionSurfaceBusyError(RuntimeError):
    """Another process is materializing the same session-surface artifact."""


@dataclass(frozen=True, slots=True)
class SessionSurfaceSelector:
    role: str
    weighting: str
    bucket_minutes: int = SESSION_SURFACE_BUCKET_MINUTES
    price_step: float = SESSION_SURFACE_PRICE_STEP

    def __post_init__(self) -> None:
        if self.role not in SESSION_SURFACE_ROLES:
            raise ValueError("unsupported session-surface role")
        if self.weighting not in SESSION_SURFACE_WEIGHTINGS:
            raise ValueError("unsupported session-surface weighting")
        if isinstance(self.bucket_minutes, bool) or (
            self.bucket_minutes not in SESSION_SURFACE_BUCKET_OPTIONS
        ):
            raise ValueError("unsupported session-surface bucket minutes")
        try:
            parsed_price_step = float(self.price_step)
        except (TypeError, ValueError) as exc:
            raise ValueError("unsupported session-surface price step") from exc
        if isinstance(self.price_step, bool) or not any(
            math.isclose(
                parsed_price_step,
                supported,
                rel_tol=0.0,
                abs_tol=1e-12,
            )
            for supported in SESSION_SURFACE_PRICE_STEP_OPTIONS
        ):
            raise ValueError("unsupported session-surface price step")
        object.__setattr__(self, "price_step", parsed_price_step)


@dataclass(frozen=True, slots=True)
class _SPXObservation:
    source_at: datetime
    known_at: datetime
    received_at: datetime
    price: float | None
    source_file: str
    source_row: int
    method: str = "direct_index_spx"
    provider: str = "schwab"
    instrument_id: str = "index:SPX"
    valid_until: datetime | None = None
    basis: Mapping[str, Any] | None = None
    usable: bool = True
    error: str | None = None


@dataclass(frozen=True, slots=True)
class _KernelColumn:
    metrics: Mapping[str, tuple[float | None, ...]]
    zero_ridge: float | None
    quality: str
    warnings: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _FrameState:
    at: datetime
    valid_until: datetime
    artifact_sha256: str
    expiry: str
    expiry_close: datetime
    reference_spot: float
    contracts: tuple[SurfaceContract, ...]
    strike_rows: tuple[Mapping[str, Any], ...]
    quality: str
    warnings: tuple[str, ...]
    # Replay artifacts are materialized at their model clock, so ``None``
    # means availability equals ``at``.  Live frames retain the later local
    # acceptance clock separately to avoid using availability as pricing time.
    known_at: datetime | None = None
    session_kind: str = "rth"
    surface_provider: str = "schwab"
    reference_method: str = "direct_index_spx"


@dataclass(frozen=True, slots=True)
class SessionSurfaceWindow:
    """One trading date's fixed GTH -> closed gap -> RTH canvas."""

    session_date: date
    session_start: datetime
    gth_end: datetime
    rth_open: datetime
    session_end: datetime
    previous_rth_open: datetime
    previous_rth_end: datetime

    def segment_kind(self, at: datetime) -> str | None:
        clock = as_utc(at)
        if as_utc(self.session_start) <= clock < as_utc(self.gth_end):
            return "gth"
        if as_utc(self.gth_end) <= clock < as_utc(self.rth_open):
            return "closed_gap"
        if as_utc(self.rth_open) <= clock <= as_utc(self.session_end):
            return "rth"
        return None

    def segments(self) -> tuple[dict[str, object], ...]:
        return (
            {
                "kind": "gth",
                "start_at": _iso(self.session_start),
                "end_at": _iso(self.gth_end),
                "surface_provider": "ibkr",
                "reference_method": "es_basis_inferred_spx",
                "reference_provider": "schwab",
            },
            {
                "kind": "closed_gap",
                "start_at": _iso(self.gth_end),
                "end_at": _iso(self.rth_open),
                "surface_provider": None,
                "reference_method": None,
                "reference_provider": None,
            },
            {
                "kind": "rth",
                "start_at": _iso(self.rth_open),
                "end_at": _iso(self.session_end),
                "surface_provider": "schwab",
                "reference_method": "direct_index_spx",
                "reference_provider": "schwab",
            },
        )


def session_surface_window(session_date: date) -> SessionSurfaceWindow:
    market_session = DEFAULT_MARKET_CALENDAR.session(session_date)
    if market_session is None:
        raise ReplaySourceError("session_surface_session_unavailable")
    previous_date = DEFAULT_MARKET_CALENDAR.previous_trading_day(session_date)
    previous_session = DEFAULT_MARKET_CALENDAR.session(previous_date)
    if previous_session is None:
        raise ReplaySourceError("session_surface_previous_session_unavailable")
    session_start = datetime.combine(
        session_date - timedelta(days=1),
        time(20, 15),
        tzinfo=ET,
    )
    gth_end = datetime.combine(session_date, time(9, 25), tzinfo=ET)
    return SessionSurfaceWindow(
        session_date=session_date,
        session_start=session_start,
        gth_end=gth_end,
        rth_open=market_session.open_at,
        session_end=market_session.close_at,
        previous_rth_open=previous_session.open_at,
        previous_rth_end=previous_session.close_at,
    )


class SessionSurfaceBuildCache:
    """Small source/frame-keyed LRU for playback across adjacent playheads."""

    def __init__(
        self,
        *,
        max_kernel_entries: int = 256,
        max_frame_entries: int = 256,
        max_spx_sessions: int = 8,
    ) -> None:
        if (
            max_kernel_entries <= 0
            or max_frame_entries <= 0
            or max_spx_sessions <= 0
        ):
            raise ValueError("session-surface cache bounds must be positive")
        self.max_kernel_entries = max_kernel_entries
        self.max_frame_entries = max_frame_entries
        self.max_spx_sessions = max_spx_sessions
        self._lock = threading.Lock()
        self._kernels: OrderedDict[tuple[object, ...], tuple[_KernelColumn, ...]] = (
            OrderedDict()
        )
        self._frames: OrderedDict[tuple[str, str, str], _FrameState] = OrderedDict()
        self._spx: OrderedDict[tuple[str, str], tuple[_SPXObservation, ...]] = (
            OrderedDict()
        )

    def get_kernel(self, key: tuple[object, ...]) -> tuple[_KernelColumn, ...] | None:
        with self._lock:
            value = self._kernels.get(key)
            if value is not None:
                self._kernels.move_to_end(key)
            return value

    def put_kernel(
        self,
        key: tuple[object, ...],
        value: tuple[_KernelColumn, ...],
    ) -> None:
        with self._lock:
            self._kernels[key] = value
            self._kernels.move_to_end(key)
            while len(self._kernels) > self.max_kernel_entries:
                self._kernels.popitem(last=False)

    def get_spx(self, key: tuple[str, str]) -> tuple[_SPXObservation, ...] | None:
        with self._lock:
            value = self._spx.get(key)
            if value is not None:
                self._spx.move_to_end(key)
            return value

    def put_spx(
        self,
        key: tuple[str, str],
        value: tuple[_SPXObservation, ...],
    ) -> None:
        with self._lock:
            self._spx[key] = value
            self._spx.move_to_end(key)
            while len(self._spx) > self.max_spx_sessions:
                self._spx.popitem(last=False)

    def get_frame(self, key: tuple[str, str, str]) -> _FrameState | None:
        with self._lock:
            value = self._frames.get(key)
            if value is not None:
                self._frames.move_to_end(key)
            return value

    def put_frame(self, key: tuple[str, str, str], value: _FrameState) -> None:
        with self._lock:
            self._frames[key] = value
            self._frames.move_to_end(key)
            while len(self._frames) > self.max_frame_entries:
                self._frames.popitem(last=False)


FrameLoader = Callable[[datetime], dict[str, object]]
FingerprintLoader = Callable[[], str]


def _finite(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _nonnegative(value: object) -> float | None:
    parsed = _finite(value)
    return parsed if parsed is not None and parsed >= 0 else None


def _mapping(value: object, *, code: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ReplaySourceError(code)
    return value


def _list(value: object, *, code: str) -> list[Any]:
    if not isinstance(value, list):
        raise ReplaySourceError(code)
    return value


def _clock(value: object, *, code: str) -> datetime:
    if not isinstance(value, str):
        raise ReplaySourceError(code)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ReplaySourceError(code) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ReplaySourceError(code)
    return as_utc(parsed)


def _iso(value: datetime) -> str:
    return as_utc(value).isoformat()


def _cache_clock(value: object) -> datetime:
    try:
        return _clock(value, code="session_surface_cache_clock_invalid")
    except ReplaySourceError as exc:
        raise ReplaySessionSurfaceCacheError(
            "session_surface_cache_clock_invalid"
        ) from exc


def _session_buckets(
    context: TrendContext,
    *,
    bucket_minutes: int,
) -> tuple[tuple[datetime, datetime], ...]:
    start = as_utc(context.open_at)
    close = as_utc(context.close_at)
    step = timedelta(minutes=bucket_minutes)
    buckets: list[tuple[datetime, datetime]] = []
    cursor = start
    while cursor < close:
        end = min(cursor + step, close)
        buckets.append((cursor, end))
        cursor = end
    if not buckets or buckets[-1][1] != close:
        raise ReplaySourceError("session_surface_bucket_contract_invalid")
    return tuple(buckets)

__all__ = (
    "MAX_SESSION_SURFACE_CACHE_ARTIFACT_BYTES",
    "MAX_SINGLE_BUILD_EVALUATIONS",
    "ReplaySessionSurfaceBusyError",
    "ReplaySessionSurfaceCacheError",
    "SESSION_SURFACE_BUCKET_MINUTES",
    "SESSION_SURFACE_BUCKET_OPTIONS",
    "SESSION_SURFACE_BASIS_MAX_SKEW_SECONDS",
    "SESSION_SURFACE_CACHE_VERSION",
    "SESSION_SURFACE_GTH_QUOTE_MAX_AGE_SECONDS",
    "SESSION_SURFACE_KIND",
    "SESSION_SURFACE_LOCK_TIMEOUT_SECONDS",
    "SESSION_SURFACE_MODE",
    "SESSION_SURFACE_POLICY_VERSION",
    "SESSION_SURFACE_PRICE_EXTENT_POINTS",
    "SESSION_SURFACE_PRICE_STEP",
    "SESSION_SURFACE_PRICE_STEP_OPTIONS",
    "SESSION_SURFACE_REFERENCE_MAX_AGE_SECONDS",
    "SESSION_SURFACE_SCHEMA_VERSION",
    "SessionSurfaceWindow",
    "SessionSurfaceBuildCache",
    "SessionSurfaceSelector",
    "session_surface_window",
)
