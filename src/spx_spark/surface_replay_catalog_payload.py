"""Catalog and timeline response builders for the SPXW replay service."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Protocol

from spx_spark.market_calendar import ET
from spx_spark.marketdata import as_utc
from spx_spark.surface_dashboard_replay import _canonical_sha256, replay_id
from spx_spark.surface_replay_http import (
    SERVICE_SCHEMA_VERSION,
    ReplayCacheError,
)
from spx_spark.surface_replay_session_models import session_surface_window


SERVICE_KIND = "spxw_surface_replay_catalog"
TIMELINE_POLICY_VERSION = "spxw_surface_replay_timeline.event_driven.v2"
SESSION_CLOSE_GRACE_SECONDS = 2 * 60 * 60
SESSION_CLOSE_GRACE_POLICY = "session_close_plus_2h_grace"


class _ReplaySessionView(Protocol):
    session_date: date
    open_at: datetime
    close_at: datetime


class ReplayCatalogPayloadSource(Protocol):
    frame_minutes: int
    lookback_seconds: float
    projection_policy_sha256: str

    def discover_sessions(self) -> tuple[_ReplaySessionView, ...]: ...

    def get_session(self, session_date: date) -> _ReplaySessionView: ...

    def viable_frames(self, session_date: date) -> tuple[datetime, ...]: ...

    def _manifest_path(self, session_date: date) -> Path: ...

    def _relevant_paths(
        self,
        session: _ReplaySessionView,
    ) -> tuple[Path, ...]: ...

    def _surface_source_paths(self, session_date: date) -> tuple[Path, ...]: ...

    def _source_fingerprint(self, paths: tuple[Path, ...]) -> str: ...

    def _read_manifest(
        self,
        path: Path,
        *,
        session_date: date,
        source_fingerprint: str,
    ) -> tuple[datetime, ...] | None: ...

    def _cache_path(
        self,
        requested: datetime,
        *,
        source_fingerprint: str | None = None,
    ) -> Path: ...

    def _close_grace_elapsed_at(self, session: _ReplaySessionView) -> datetime: ...


def build_sessions_payload(
    catalog: ReplayCatalogPayloadSource,
) -> dict[str, object]:
    rows: list[dict[str, object]] = []
    for item in catalog.discover_sessions():
        manifest_path = catalog._manifest_path(item.session_date)
        frame_count: int | None = None
        cached_frame_count = 0
        relevant_paths = catalog._relevant_paths(item)
        try:
            fingerprint = catalog._source_fingerprint(relevant_paths)
            indexed_frames = catalog._read_manifest(
                manifest_path,
                session_date=item.session_date,
                source_fingerprint=fingerprint,
            )
        except OSError:
            indexed_frames = None
        if indexed_frames is not None:
            frame_count = len(indexed_frames)
            cached_frame_count = sum(
                catalog._cache_path(
                    frame,
                    source_fingerprint=fingerprint,
                ).is_file()
                for frame in indexed_frames
            )
        session_date_text = item.session_date.isoformat()
        surface_window = session_surface_window(item.session_date)
        surface_frame_count = int(
            (
                as_utc(surface_window.session_end)
                - as_utc(surface_window.session_start)
            ).total_seconds()
            // (5 * 60)
        )
        rows.append(
            {
                "session_date": session_date_text,
                "label": item.session_date.strftime("%a, %b %d, %Y"),
                "open_at": as_utc(item.open_at).isoformat(),
                "close_at": as_utc(item.close_at).isoformat(),
                "partition_count": len(relevant_paths),
                "frame_interval_minutes": catalog.frame_minutes,
                "frame_count": frame_count,
                "surface_frame_count": surface_frame_count,
                "surface_timeline_status": "fixed_playhead_canvas",
                "cached_frame_count": cached_frame_count,
                "timeline_status": (
                    "indexed" if indexed_frames is not None else "on_demand"
                ),
                "session_close_grace_elapsed": True,
                "session_close_grace_elapsed_at": (
                    catalog._close_grace_elapsed_at(item).isoformat()
                ),
                "data_finalization_proven": False,
                "projection_policy_sha256": catalog.projection_policy_sha256,
                "timeline_url": (
                    f"/api/v1/replay/sessions/{session_date_text}/timeline"
                    f"?step_minutes={catalog.frame_minutes}"
                ),
            }
        )
    return {
        "schema_version": SERVICE_SCHEMA_VERSION,
        "kind": SERVICE_KIND,
        "provider": "schwab",
        "coordinate": "SPX",
        "trading_class": "SPXW",
        "frame_interval_minutes": catalog.frame_minutes,
        "timeline_policy_version": TIMELINE_POLICY_VERSION,
        "availability_proven": False,
        "availability_clock": "unavailable",
        "point_in_time_confidence": "bounded_not_proven",
        "frame_validation": "known_clock_validation_on_frame_request",
        "timeline_selection": "latest_coverage_candidate_per_bucket",
        "projection_policy_sha256": catalog.projection_policy_sha256,
        "only_close_grace_elapsed_sessions": True,
        "session_close_grace_policy": SESSION_CLOSE_GRACE_POLICY,
        "session_close_grace_seconds": SESSION_CLOSE_GRACE_SECONDS,
        "data_finalization_proven": False,
        "default_session": rows[0]["session_date"] if rows else None,
        "sessions": rows,
    }


def build_timeline_payload(
    catalog: ReplayCatalogPayloadSource,
    session_date: date,
) -> dict[str, object]:
    session = catalog.get_session(session_date)
    frames = catalog.viable_frames(session_date)
    source_paths = catalog._relevant_paths(session)
    source_fingerprint = catalog._source_fingerprint(source_paths)
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
                "cached": catalog._cache_path(
                    requested,
                    source_fingerprint=source_fingerprint,
                ).is_file(),
                "projection_policy_sha256": catalog.projection_policy_sha256,
                "url": (
                    f"/api/v1/replay/sessions/{session_date_text}/frame?at={at}"
                ),
                "frame_url": f"/api/v1/replay/frames/{identifier}",
            }
        )

    surface_window = session_surface_window(session_date)
    surface_rows: list[dict[str, object]] = []
    cursor = as_utc(surface_window.session_start)
    surface_close = as_utc(surface_window.session_end)
    surface_step = timedelta(minutes=5)
    while cursor < surface_close:
        end = min(cursor + surface_step, surface_close)
        session_kind = surface_window.segment_kind(cursor)
        if session_kind is None:
            raise ReplayCacheError("replay_surface_timeline_window_invalid")
        at = end.isoformat()
        surface_rows.append(
            {
                "at": at,
                "requested_as_of": at,
                "id": replay_id(end),
                "session_kind": session_kind,
                "status": (
                    "scheduled_missing"
                    if session_kind == "closed_gap"
                    else "unvalidated_playhead"
                ),
                "projection_policy_sha256": catalog.projection_policy_sha256,
            }
        )
        cursor = end
    surface_hash_body = [
        {
            "at": row["at"],
            "session_kind": row["session_kind"],
            "status": row["status"],
        }
        for row in surface_rows
    ]
    surface_source_paths = catalog._surface_source_paths(session_date)
    return {
        "schema_version": SERVICE_SCHEMA_VERSION,
        "kind": SERVICE_KIND,
        "session_date": session_date_text,
        "provider": "schwab",
        "coordinate": "SPX",
        "trading_class": "SPXW",
        "open_at": as_utc(session.open_at).isoformat(),
        "close_at": as_utc(session.close_at).isoformat(),
        "surface_open_at": as_utc(surface_window.session_start).isoformat(),
        "surface_close_at": surface_close.isoformat(),
        "surface_provider": "mixed",
        "surface_frame_interval_minutes": 5,
        "surface_frame_count": len(surface_rows),
        "surface_timeline_sha256": _canonical_sha256(surface_hash_body),
        "surface_source_fingerprint": catalog._source_fingerprint(
            surface_source_paths
        ),
        "session_segments": list(surface_window.segments()),
        "surface_frames": surface_rows,
        "frame_interval_minutes": catalog.frame_minutes,
        "step_minutes": catalog.frame_minutes,
        "timeline_policy_version": TIMELINE_POLICY_VERSION,
        "availability_proven": False,
        "availability_clock": "unavailable",
        "point_in_time_confidence": "bounded_not_proven",
        "frame_validation": "known_clock_validation_on_frame_request",
        "timeline_selection": "latest_coverage_candidate_per_bucket",
        "projection_policy_sha256": catalog.projection_policy_sha256,
        "source_fingerprint": source_fingerprint,
        "timeline_sha256": timeline_sha256,
        "session_close_grace_elapsed": True,
        "session_close_grace_elapsed_at": (
            catalog._close_grace_elapsed_at(session).isoformat()
        ),
        "only_close_grace_elapsed_sessions": True,
        "session_close_grace_policy": SESSION_CLOSE_GRACE_POLICY,
        "session_close_grace_seconds": SESSION_CLOSE_GRACE_SECONDS,
        "data_finalization_proven": False,
        "lookback_seconds": catalog.lookback_seconds,
        "frame_count": len(frame_rows),
        "frames": frame_rows,
    }


__all__ = (
    "SERVICE_KIND",
    "SESSION_CLOSE_GRACE_POLICY",
    "SESSION_CLOSE_GRACE_SECONDS",
    "TIMELINE_POLICY_VERSION",
    "ReplayCatalogPayloadSource",
    "build_sessions_payload",
    "build_timeline_payload",
)
