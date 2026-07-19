"""Persistent causal accumulator for the live SPXW Session Canvas."""

from __future__ import annotations

import copy
import json
import math
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR, ET
from spx_spark.marketdata import as_utc
from spx_spark.surface_live_session_models import (
    LIVE_BUCKET_MINUTES,
    LIVE_PRICE_EXTENT_POINTS,
    LIVE_PRICE_STEP,
    LIVE_SERVICE_SCHEMA_VERSION,
    LIVE_SESSION_POLICY_VERSION,
    LIVE_SESSION_STATE_SCHEMA_VERSION,
    MAX_LIVE_SNAPSHOT_BYTES,
    LiveSelector,
    LiveSessionError,
    LiveSnapshotError,
    finite,
    frame_state,
    iso,
    list_value,
    mapping,
    parse_clock,
    signed_payload,
    verify_artifact,
)
from spx_spark.surface_live_session_store import LiveSessionStateStore, state_payload
from spx_spark.surface_replay_session_data import (
    _fixed_price_grid,
    _kernel_columns,
)
from spx_spark.surface_replay_session_models import SessionSurfaceBuildCache


DEFAULT_POLL_SECONDS = 0.25
MAX_CANDLE_SAMPLES = 4_096


@dataclass(frozen=True, slots=True)
class LiveInput:
    artifact_sha256: str
    as_of: datetime
    valid_until: datetime
    spot: float
    spot_source_at: datetime
    spot_provider: str
    frames: Mapping[str, dict[str, object]]
    providers: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ValidatedLiveInput:
    artifact_sha256: str
    as_of: datetime
    created_at: datetime
    valid_until: datetime
    spot: float
    spot_source_at: datetime
    spot_provider: str
    frame_templates: Mapping[str, dict[str, object]]
    providers: tuple[str, ...]

    def stamp(self, accepted_at: datetime) -> LiveInput:
        accepted = as_utc(accepted_at)
        session = DEFAULT_MARKET_CALENDAR.session(accepted.astimezone(ET).date())
        if (
            session is None
            or self.as_of.astimezone(ET).date() != accepted.astimezone(ET).date()
            or not session.open_at <= self.as_of <= self.created_at <= accepted < session.close_at
            or not accepted < self.valid_until
        ):
            raise LiveSnapshotError("live_snapshot_acceptance_clock_invalid")
        frames: dict[str, dict[str, object]] = {}
        for role, template in self.frame_templates.items():
            frame = dict(template)
            frame.pop("artifact_sha256", None)
            frame["accepted_at"] = iso(accepted)
            frames[role] = signed_payload(frame)
        return LiveInput(
            artifact_sha256=self.artifact_sha256,
            as_of=self.as_of,
            valid_until=self.valid_until,
            spot=self.spot,
            spot_source_at=self.spot_source_at,
            spot_provider=self.spot_provider,
            frames=frames,
            providers=self.providers,
        )


def _session_buckets(
    start: datetime,
    end: datetime,
) -> tuple[tuple[datetime, datetime], ...]:
    step = timedelta(minutes=LIVE_BUCKET_MINUTES)
    cursor = as_utc(start)
    close = as_utc(end)
    rows: list[tuple[datetime, datetime]] = []
    while cursor < close:
        next_end = min(cursor + step, close)
        rows.append((cursor, next_end))
        cursor = next_end
    if not rows or rows[-1][1] != close:
        raise LiveSessionError("live_bucket_contract_invalid")
    return tuple(rows)


def _frame_payload(
    expiry_row: Mapping[str, Any],
    *,
    role: str,
    accepted_at: datetime,
    valid_until: datetime,
    snapshot_hash: str,
    snapshot_as_of: datetime,
) -> dict[str, object]:
    expiry = expiry_row.get("expiry")
    if not isinstance(expiry, str) or len(expiry) != 8 or not expiry.isdigit():
        raise LiveSnapshotError("live_expiry_invalid")
    expiry_close = parse_clock(
        expiry_row.get("expiry_close"),
        code="live_expiry_close_invalid",
    )
    surface = mapping(expiry_row.get("surface"), code="live_surface_invalid")
    surface_as_of = parse_clock(surface.get("as_of"), code="live_surface_as_of_invalid")
    reference_spot = finite(surface.get("reference_spot"))
    if surface_as_of != snapshot_as_of or reference_spot is None or reference_spot <= 0:
        raise LiveSnapshotError("live_surface_clock_invalid")
    strike_ladder = list_value(
        surface.get("strike_ladder"),
        code="live_strike_ladder_invalid",
    )
    if not strike_ladder:
        raise LiveSnapshotError("live_strike_ladder_empty")
    clocks = mapping(expiry_row.get("input_clocks"), code="live_input_clocks_invalid")
    selection_as_of = parse_clock(
        clocks.get("selection_as_of"),
        code="live_input_selection_clock_invalid",
    )
    max_known_at = parse_clock(
        clocks.get("max_known_at"),
        code="live_input_known_clock_invalid",
    )
    source_at = parse_clock(
        clocks.get("max_source_at"),
        code="live_input_source_clock_invalid",
    )
    clock_count = clocks.get("contract_clock_count")
    future_count = clocks.get("future_clock_count")
    contract_count = expiry_row.get("contract_count")
    if (
        selection_as_of != snapshot_as_of
        or source_at > max_known_at
        or max_known_at > snapshot_as_of
        or source_at > snapshot_as_of
        or future_count != 0
        or isinstance(clock_count, bool)
        or not isinstance(clock_count, int)
        or clock_count <= 0
        or clock_count != contract_count
    ):
        raise LiveSnapshotError("live_input_clock_contract_invalid")
    raw_providers = expiry_row.get("providers")
    if not isinstance(raw_providers, list) or not raw_providers:
        raise LiveSnapshotError("live_frame_providers_invalid")
    providers = sorted({str(value).strip() for value in raw_providers if str(value).strip()})
    if not providers:
        raise LiveSnapshotError("live_frame_providers_invalid")
    raw_warnings = expiry_row.get("warnings")
    payload = {
        "schema_version": LIVE_SESSION_STATE_SCHEMA_VERSION,
        "kind": "spxw_live_session_frame",
        "role": role,
        "expiry": expiry,
        "expiry_close": iso(expiry_close),
        "accepted_at": iso(accepted_at),
        "source_at": iso(source_at),
        "known_at": iso(max_known_at),
        "model_as_of": iso(snapshot_as_of),
        "valid_until": iso(valid_until),
        "reference_spot": reference_spot,
        "quality": str(expiry_row.get("quality") or surface.get("quality") or "unavailable"),
        "warnings": list(raw_warnings) if isinstance(raw_warnings, list) else [],
        "providers": providers,
        "input_clocks": dict(clocks),
        "strike_ladder": strike_ladder,
        "source_snapshot_sha256": snapshot_hash,
    }
    return signed_payload(payload)


def validate_live_snapshot(payload: Mapping[str, Any]) -> ValidatedLiveInput:
    """Fully validate immutable content before assigning local availability."""

    if (
        payload.get("schema_version") != 1
        or payload.get("kind") != "spxw_surface_dashboard"
        or payload.get("automatic_ordering") is not False
    ):
        raise LiveSnapshotError("live_snapshot_identity_invalid")
    snapshot_hash = verify_artifact(payload, code="live_snapshot")
    created_at = parse_clock(payload.get("created_at"), code="live_snapshot_created_at_invalid")
    snapshot_as_of = parse_clock(payload.get("as_of"), code="live_snapshot_as_of_invalid")
    valid_until = parse_clock(
        payload.get("valid_until"),
        code="live_snapshot_valid_until_invalid",
    )
    quality = mapping(payload.get("quality"), code="live_snapshot_quality_invalid")
    lease_seconds = finite(quality.get("lease_seconds"))
    refresh_seconds = finite(quality.get("refresh_interval_seconds"))
    source_state = mapping(payload.get("source_state"), code="live_source_state_invalid")
    source_state_created = parse_clock(
        source_state.get("created_at"),
        code="live_source_state_created_invalid",
    )
    source_selection = parse_clock(
        source_state.get("selection_as_of"),
        code="live_source_state_selection_invalid",
    )
    if (
        lease_seconds is None
        or refresh_seconds is None
        or not 0 < refresh_seconds <= lease_seconds <= 60
        or source_selection != snapshot_as_of
        or source_state_created > snapshot_as_of
        or (snapshot_as_of - source_state_created).total_seconds() > lease_seconds
        or snapshot_as_of > created_at
        or not created_at < valid_until
    ):
        raise LiveSnapshotError("live_snapshot_lease_invalid")
    session = mapping(payload.get("session"), code="live_snapshot_session_invalid")
    # A valid publisher artifact outside RTH is an expected waiting state, not
    # malformed input.  Stop before requiring an RTH calendar, direct SPX, or
    # option frames; none of those should exist on a weekend/holiday snapshot.
    if session.get("rth_open") is not True or session.get("state") != "rth":
        raise LiveSnapshotError("live_snapshot_not_rth")
    source_session = DEFAULT_MARKET_CALENDAR.session(snapshot_as_of.astimezone(ET).date())
    if (
        source_session is None
        or not source_session.open_at <= snapshot_as_of <= created_at < source_session.close_at
        or snapshot_as_of.astimezone(ET).date() != created_at.astimezone(ET).date()
    ):
        raise LiveSnapshotError("live_snapshot_session_clock_invalid")
    underlier = mapping(payload.get("underlier"), code="live_underlier_invalid")
    spot = finite(underlier.get("price"))
    spot_provider = str(underlier.get("provider") or "").strip()
    if (
        underlier.get("source") != "index:SPX"
        or spot is None
        or spot <= 0
        or not spot_provider
    ):
        raise LiveSnapshotError("live_direct_spx_unavailable")
    spot_source_at = parse_clock(
        underlier.get("source_at"),
        code="live_spot_source_clock_invalid",
    )
    if spot_source_at > snapshot_as_of:
        raise LiveSnapshotError("live_spot_lookahead")
    expected_expiries = tuple(
        value.strftime("%Y%m%d")
        for value in DEFAULT_MARKET_CALENDAR.research_expiries(snapshot_as_of)
    )
    frame_templates: dict[str, dict[str, object]] = {}
    providers = {spot_provider}
    for raw in list_value(payload.get("expiries"), code="live_expiries_invalid"):
        row = mapping(raw, code="live_expiry_row_invalid")
        role = row.get("role")
        if role not in {"front", "next"} or role in frame_templates:
            raise LiveSnapshotError("live_expiry_role_invalid")
        expected_index = 0 if role == "front" else 1
        if len(expected_expiries) <= expected_index or row.get("expiry") != expected_expiries[expected_index]:
            raise LiveSnapshotError("live_expiry_role_mismatch")
        frame = _frame_payload(
            row,
            role=role,
            accepted_at=created_at,
            valid_until=valid_until,
            snapshot_hash=snapshot_hash,
            snapshot_as_of=snapshot_as_of,
        )
        frame_templates[role] = frame
        raw_frame_providers = frame.get("providers")
        if isinstance(raw_frame_providers, list):
            providers.update(str(value) for value in raw_frame_providers)
    return ValidatedLiveInput(
        artifact_sha256=snapshot_hash,
        as_of=snapshot_as_of,
        created_at=created_at,
        valid_until=valid_until,
        spot=spot,
        spot_source_at=spot_source_at,
        spot_provider=spot_provider,
        frame_templates=frame_templates,
        providers=tuple(sorted(providers)),
    )


def parse_live_snapshot(
    payload: Mapping[str, Any],
    *,
    accepted_at: datetime,
) -> LiveInput:
    """Compatibility helper for one-shot validation and availability stamping."""

    return validate_live_snapshot(payload).stamp(accepted_at)


class LiveSessionAccumulator:
    """Single-owner state machine; kernels are evaluated lazily by selector."""

    def __init__(
        self,
        *,
        snapshot_path: str | Path,
        state_store: LiveSessionStateStore,
        utcnow: Callable[[], datetime] = lambda: datetime.now(tz=timezone.utc),
    ) -> None:
        self.snapshot_path = Path(snapshot_path).expanduser()
        self.store = state_store
        self.utcnow = utcnow
        self._lock = threading.RLock()
        self._active_date: date | None = None
        self._manifest: dict[str, Any] | None = None
        self._runtime: dict[str, Any] | None = None
        self._boundaries: list[dict[str, Any]] = []
        self._input_identity: tuple[int, int, int, int] | None = None
        self._input_status = "waiting"
        self._last_error: str | None = None
        self._last_poll_at: datetime | None = None
        self._build_cache = SessionSurfaceBuildCache(
            max_kernel_entries=128,
            max_frame_entries=64,
            max_spx_sessions=2,
        )
        self._projection_lock = threading.Lock()
        self.store.ensure_root()
        self._restore_for_clock(as_utc(self.utcnow()))

    def health_payload(self) -> dict[str, object]:
        with self._lock:
            return {
                "schema_version": LIVE_SERVICE_SCHEMA_VERSION,
                "service": "spxw-surface-live",
                "status": "ok",
                "input_status": self._input_status,
                "active_session": self._active_date.isoformat() if self._active_date else None,
                "last_poll_at": iso(self._last_poll_at) if self._last_poll_at else None,
                "last_error": self._last_error,
            }

    def _restore_for_clock(self, now: datetime) -> None:
        session_date = now.astimezone(ET).date()
        manifest = self.store.load_manifest(session_date)
        if manifest is None:
            return
        runtime = self.store.load_runtime(session_date)
        if runtime is None:
            raise LiveSessionError("live_runtime_missing_for_manifest")
        self._active_date = session_date
        self._manifest = manifest
        self._runtime = runtime
        self._boundaries = list(self.store.load_boundaries(session_date))
        self._validate_persisted_contract()
        self._validate_boundary_chain(repair_runtime=True)

    def _validate_persisted_contract(self) -> None:
        """Reject state created under any contract this binary cannot interpret."""

        if self._active_date is None or self._manifest is None or self._runtime is None:
            raise LiveSessionError("live_state_contract_unavailable")
        session = DEFAULT_MARKET_CALENDAR.session(self._active_date)
        if session is None:
            raise LiveSessionError("live_manifest_non_trading_session")
        start = parse_clock(self._manifest.get("session_start"), code="live_manifest_start_invalid")
        close = parse_clock(self._manifest.get("session_end"), code="live_manifest_end_invalid")
        price_step = finite(self._manifest.get("price_step"))
        extent = finite(self._manifest.get("price_extent_points_each_side"))
        bucket_minutes = self._manifest.get("bucket_minutes")
        if (
            self._manifest.get("session_date") != self._active_date.isoformat()
            or self._runtime.get("session_date") != self._active_date.isoformat()
            or self._manifest.get("policy_version") != LIVE_SESSION_POLICY_VERSION
            or isinstance(bucket_minutes, bool)
            or bucket_minutes != LIVE_BUCKET_MINUTES
            or price_step != LIVE_PRICE_STEP
            or extent != LIVE_PRICE_EXTENT_POINTS
            or start != session.open_at
            or close != session.close_at
        ):
            raise LiveSessionError("live_persisted_contract_drift")
        for boundary in self._boundaries:
            if boundary.get("session_date") != self._active_date.isoformat():
                raise LiveSessionError("live_boundary_session_mismatch")

    def _rollover_for_clock(self, now: datetime) -> None:
        current_date = now.astimezone(ET).date()
        if self._active_date == current_date:
            return
        self._active_date = None
        self._manifest = None
        self._runtime = None
        self._boundaries = []
        self._restore_for_clock(now)

    def _validate_boundary_chain(self, *, repair_runtime: bool) -> None:
        if self._manifest is None or self._runtime is None:
            return
        start = parse_clock(self._manifest.get("session_start"), code="live_manifest_start_invalid")
        previous = self._manifest.get("artifact_sha256")
        boundary_positions: dict[object, tuple[int, str]] = {}
        cursor = start
        for index, boundary in enumerate(self._boundaries):
            boundary_start = parse_clock(
                boundary.get("start_at"),
                code="live_boundary_start_invalid",
            )
            boundary_end = parse_clock(
                boundary.get("end_at"),
                code="live_boundary_end_invalid",
            )
            if (
                boundary_start != cursor
                or boundary_end != cursor + timedelta(minutes=LIVE_BUCKET_MINUTES)
                or boundary.get("previous_boundary_sha256") != previous
            ):
                raise LiveSessionError("live_boundary_chain_invalid")
            previous = boundary.get("artifact_sha256")
            cursor = boundary_end
            boundary_positions[previous] = (index, iso(boundary_end))
        runtime_tip = self._runtime.get("boundary_tip_sha256")
        runtime_through = self._runtime.get("history_frozen_through")
        if not self._boundaries:
            if runtime_tip is not None or runtime_through is not None:
                raise LiveSessionError("live_boundary_tip_without_history")
            return
        if runtime_tip is None:
            if runtime_through is not None:
                raise LiveSessionError("live_boundary_runtime_tip_mismatch")
            runtime_position = -1
        else:
            position = boundary_positions.get(runtime_tip)
            if position is None:
                raise LiveSessionError("live_boundary_tip_missing")
            runtime_position, tip_end = position
            if runtime_through != tip_end:
                raise LiveSessionError("live_boundary_runtime_tip_mismatch")
        expected_tip = previous if self._boundaries else None
        expected_through = iso(cursor) if self._boundaries else None
        if runtime_tip == expected_tip:
            return
        if runtime_position >= len(self._boundaries) - 1:
            raise LiveSessionError("live_boundary_disk_not_forward_extension")
        if not repair_runtime:
            raise LiveSessionError("live_boundary_runtime_tip_mismatch")
        self._runtime["boundary_tip_sha256"] = expected_tip
        self._runtime["history_frozen_through"] = expected_through
        samples = self._runtime.get("candle_samples")
        if self._boundaries and isinstance(samples, list):
            retained = []
            for sample in samples:
                if not isinstance(sample, Mapping):
                    continue
                source_at = parse_clock(
                    sample.get("source_at"),
                    code="live_sample_source_invalid",
                )
                if source_at >= cursor:
                    retained.append(dict(sample))
            self._runtime["candle_samples"] = retained
        if self._active_date is not None:
            self._write_runtime(as_utc(self.utcnow()))

    def _session(self):
        if self._active_date is None:
            return None
        return DEFAULT_MARKET_CALENDAR.session(self._active_date)

    def _initialize_session(self, live: LiveInput, *, accepted_at: datetime) -> None:
        session_date = accepted_at.astimezone(ET).date()
        market_session = DEFAULT_MARKET_CALENDAR.session(session_date)
        if market_session is None or not market_session.open_at <= accepted_at < market_session.close_at:
            raise LiveSnapshotError("live_snapshot_outside_rth")
        anchor = math.floor((live.spot / LIVE_PRICE_STEP) + 0.5) * LIVE_PRICE_STEP
        manifest = state_payload(
            kind="spxw_live_session_manifest",
            session_date=session_date.isoformat(),
            values={
                "policy_version": LIVE_SESSION_POLICY_VERSION,
                "session_start": iso(market_session.open_at),
                "session_end": iso(market_session.close_at),
                "accumulator_started_at": iso(accepted_at),
                "anchor": {
                    "price": live.spot,
                    "grid_anchor": anchor,
                    "source_at": iso(live.spot_source_at),
                    "accepted_at": iso(accepted_at),
                    "provider": live.spot_provider,
                },
                "bucket_minutes": LIVE_BUCKET_MINUTES,
                "price_step": LIVE_PRICE_STEP,
                "price_extent_points_each_side": LIVE_PRICE_EXTENT_POINTS,
            },
        )
        runtime = state_payload(
            kind="spxw_live_session_runtime",
            session_date=session_date.isoformat(),
            values={
                "updated_at": iso(accepted_at),
                "last_upstream_sha256": None,
                "last_accepted_at": None,
                "last_source_as_of": None,
                "last_valid_until": None,
                "candidate_by_role": {},
                "baseline_by_role": {},
                "latest_spot": None,
                "candle_samples": [],
                "providers": [],
                "boundary_tip_sha256": None,
                "history_frozen_through": None,
            },
        )
        self.store.write_manifest(session_date, manifest)
        self.store.write_runtime(session_date, runtime)
        self._active_date = session_date
        self._manifest = self.store.load_manifest(session_date)
        self._runtime = self.store.load_runtime(session_date)
        self._boundaries = list(self.store.load_boundaries(session_date))

    def _write_runtime(self, now: datetime) -> None:
        if self._active_date is None or self._runtime is None:
            return
        self._runtime["updated_at"] = iso(now)
        self.store.write_runtime(self._active_date, self._runtime)
        loaded = self.store.load_runtime(self._active_date)
        if loaded is None:
            raise LiveSessionError("live_runtime_write_lost")
        self._runtime = loaded

    @staticmethod
    def _sample_candle(
        samples: list[Mapping[str, Any]],
        *,
        start: datetime,
        end: datetime,
        cutoff: datetime,
        complete: bool,
    ) -> dict[str, object] | None:
        selected: list[tuple[datetime, datetime, float, str]] = []
        for sample in samples:
            try:
                source_at = parse_clock(sample.get("source_at"), code="live_sample_source_invalid")
                accepted_at = parse_clock(
                    sample.get("accepted_at"),
                    code="live_sample_accepted_invalid",
                )
            except LiveSnapshotError:
                continue
            price = finite(sample.get("price"))
            provider = str(sample.get("provider") or "").strip()
            if (
                price is not None
                and price > 0
                and start <= source_at < end
                and accepted_at <= cutoff
            ):
                selected.append((source_at, accepted_at, price, provider))
        if not selected:
            return None
        selected.sort(key=lambda row: (row[0], row[1]))
        prices = [row[2] for row in selected]
        providers = sorted({row[3] for row in selected if row[3]})
        return {
            "start_at": iso(start),
            "end_at": iso(end),
            "open": prices[0],
            "high": max(prices),
            "low": min(prices),
            "close": prices[-1],
            "sample_count": len(prices),
            "complete": complete,
            "source_at": iso(selected[-1][0]),
            "known_at": iso(max(row[1] for row in selected)),
            "quality": "event_sampled",
            "providers": providers,
        }

    def _freeze_due(self, now: datetime) -> bool:
        if self._manifest is None or self._runtime is None or self._active_date is None:
            return False
        start = parse_clock(self._manifest.get("session_start"), code="live_manifest_start_invalid")
        close = parse_clock(self._manifest.get("session_end"), code="live_manifest_end_invalid")
        existing_ends = {row.get("end_at") for row in self._boundaries}
        candidates = self._runtime.get("candidate_by_role")
        candidate_map = candidates if isinstance(candidates, Mapping) else {}
        samples_raw = self._runtime.get("candle_samples")
        samples = [row for row in samples_raw if isinstance(row, Mapping)] if isinstance(samples_raw, list) else []
        changed = False
        frozen_through: datetime | None = None
        for bucket_start, bucket_end in _session_buckets(start, close):
            if bucket_end > min(now, close) or iso(bucket_end) in existing_ends:
                continue
            frame_by_role: dict[str, object] = {}
            frozen_columns: dict[str, object] = {}
            missing_roles: dict[str, str] = {}
            for role in ("front", "next"):
                candidate = candidate_map.get(role)
                if isinstance(candidate, Mapping):
                    accepted = parse_clock(
                        candidate.get("accepted_at"),
                        code="live_candidate_accepted_invalid",
                    )
                    valid_until = parse_clock(
                        candidate.get("valid_until"),
                        code="live_candidate_valid_until_invalid",
                    )
                    if accepted <= bucket_end < valid_until:
                        frame_by_role[role] = dict(candidate)
                        frozen_columns[role] = self._freeze_frame_columns(
                            candidate,
                            bucket_end=bucket_end,
                        )
                        continue
                frame_by_role[role] = None
                frozen_columns[role] = None
                missing_roles[role] = "validated_surface_unavailable_at_bucket_end"
            candle = self._sample_candle(
                samples,
                start=bucket_start,
                end=bucket_end,
                cutoff=bucket_end,
                complete=True,
            )
            boundary = state_payload(
                kind="spxw_live_session_boundary",
                session_date=self._active_date.isoformat(),
                values={
                    "start_at": iso(bucket_start),
                    "end_at": iso(bucket_end),
                    "frozen_at": iso(now),
                    "frame_by_role": frame_by_role,
                    "frozen_columns": frozen_columns,
                    "candle": candle,
                    "missing": {
                        "surface_by_role": missing_roles,
                        "candle": None if candle is not None else "spx_event_samples_unavailable",
                    },
                    "previous_boundary_sha256": (
                        self._boundaries[-1].get("artifact_sha256")
                        if self._boundaries
                        else self._manifest.get("artifact_sha256")
                    ),
                },
            )
            self.store.write_boundary(self._active_date, bucket_end, boundary)
            stored = self.store._read(  # validated immutable reload
                self.store.boundary_path(self._active_date, bucket_end),
                expected_kind="spxw_live_session_boundary",
            )
            if stored is None:
                raise LiveSessionError("live_boundary_write_lost")
            self._boundaries.append(stored)
            existing_ends.add(iso(bucket_end))
            frozen_through = bucket_end
            changed = True
        if changed and frozen_through is not None:
            retained = []
            for sample in samples:
                try:
                    source_at = parse_clock(
                        sample.get("source_at"),
                        code="live_sample_source_invalid",
                    )
                except LiveSnapshotError:
                    continue
                if source_at >= frozen_through:
                    retained.append(dict(sample))
            self._runtime["candle_samples"] = retained
            self._runtime["boundary_tip_sha256"] = self._boundaries[-1].get(
                "artifact_sha256"
            )
            self._runtime["history_frozen_through"] = iso(frozen_through)
            self._write_runtime(now)
        return changed

    def _freeze_frame_columns(
        self,
        candidate: Mapping[str, Any],
        *,
        bucket_end: datetime,
    ) -> dict[str, object]:
        if self._manifest is None:
            raise LiveSessionError("live_manifest_unavailable")
        anchor = mapping(self._manifest.get("anchor"), code="live_manifest_anchor_invalid")
        anchor_spot = finite(anchor.get("grid_anchor"))
        if anchor_spot is None or anchor_spot <= 0:
            raise LiveSessionError("live_anchor_invalid")
        price_grid = _fixed_price_grid(anchor_spot, step=LIVE_PRICE_STEP)
        frame = frame_state(candidate)
        minutes_forward = max((bucket_end - frame.at).total_seconds() / 60.0, 0.0)
        output: dict[str, object] = {}
        for weighting in ("oi_weighted", "volume_weighted"):
            kernel = _kernel_columns(
                frame,
                price_grid=price_grid,
                offsets=(minutes_forward,),
                weighting=weighting,
                build_cache=self._build_cache,
            )[0]
            metrics = {name: list(values) for name, values in kernel.metrics.items()}
            signed_gamma = metrics["signed_gamma"]
            positive = [
                (price, value)
                for price, value in zip(price_grid, signed_gamma, strict=True)
                if value is not None and value > 0
            ]
            negative = [
                (price, value)
                for price, value in zip(price_grid, signed_gamma, strict=True)
                if value is not None and value < 0
            ]
            output[weighting] = {
                "quality": "ready" if kernel.quality == "ok" else kernel.quality,
                "warnings": list(kernel.warnings),
                "source_at": candidate.get("source_at"),
                "known_at": candidate.get("known_at"),
                "accepted_at": candidate.get("accepted_at"),
                "valid_until": candidate.get("valid_until"),
                "source_frame_sha256": candidate.get("artifact_sha256"),
                "model_as_of": candidate.get("model_as_of"),
                "scenario_at": iso(bucket_end),
                "minutes_forward": minutes_forward,
                "metrics": metrics,
                "zero_ridge": kernel.zero_ridge,
                "gamma_positive_peak": (
                    {"price": max(positive, key=lambda row: row[1])[0], "value": max(positive, key=lambda row: row[1])[1]}
                    if positive
                    else None
                ),
                "gamma_negative_trough": (
                    {"price": min(negative, key=lambda row: row[1])[0], "value": min(negative, key=lambda row: row[1])[1]}
                    if negative
                    else None
                ),
            }
        return output

    def accept(self, live: LiveInput, *, accepted_at: datetime) -> bool:
        accepted = as_utc(accepted_at)
        if (
            not math.isfinite(live.spot)
            or live.spot <= 0
            or live.spot_source_at > live.as_of
            or live.as_of > accepted
            or accepted >= live.valid_until
        ):
            raise LiveSnapshotError("live_acceptance_clock_invalid")
        with self._lock:
            if self._active_date != accepted.astimezone(ET).date():
                self._active_date = None
                self._manifest = None
                self._runtime = None
                self._boundaries = []
                self._restore_for_clock(accepted)
            if self._manifest is None:
                self._initialize_session(live, accepted_at=accepted)
            assert self._runtime is not None
            previous_accepted_at = self._runtime.get("last_accepted_at")
            if isinstance(previous_accepted_at, str) and accepted < parse_clock(
                previous_accepted_at,
                code="live_runtime_accepted_clock_invalid",
            ):
                raise LiveSnapshotError("live_acceptance_clock_regressed")
            self._freeze_due(accepted)
            if self._runtime.get("last_upstream_sha256") == live.artifact_sha256:
                return False
            previous_source_as_of = self._runtime.get("last_source_as_of")
            if isinstance(previous_source_as_of, str) and live.as_of < parse_clock(
                previous_source_as_of,
                code="live_runtime_source_clock_invalid",
            ):
                raise LiveSnapshotError("live_source_clock_regressed")
            samples = self._runtime.get("candle_samples")
            sample_rows = list(samples) if isinstance(samples, list) else []
            sample_key = (iso(live.spot_source_at), live.spot)
            existing_keys = {
                (row.get("source_at"), finite(row.get("price")))
                for row in sample_rows
                if isinstance(row, Mapping)
            }
            if sample_key not in existing_keys:
                sample_rows.append(
                    {
                        "source_at": sample_key[0],
                        "accepted_at": iso(accepted),
                        "price": live.spot,
                        "provider": live.spot_provider,
                    }
                )
            self._runtime["candle_samples"] = sample_rows[-MAX_CANDLE_SAMPLES:]
            candidates = self._runtime.get("candidate_by_role")
            candidate_map = dict(candidates) if isinstance(candidates, Mapping) else {}
            baselines = self._runtime.get("baseline_by_role")
            baseline_map = dict(baselines) if isinstance(baselines, Mapping) else {}
            for role, frame in live.frames.items():
                candidate_map[role] = frame
                baseline_map.setdefault(role, frame)
            providers = self._runtime.get("providers")
            provider_values = set(providers) if isinstance(providers, list) else set()
            provider_values.update(live.providers)
            self._runtime.update(
                {
                    "last_upstream_sha256": live.artifact_sha256,
                    "last_accepted_at": iso(accepted),
                    "last_source_as_of": iso(live.as_of),
                    "last_valid_until": iso(live.valid_until),
                    "candidate_by_role": candidate_map,
                    "baseline_by_role": baseline_map,
                    "latest_spot": {
                        "price": live.spot,
                        "source_at": iso(live.spot_source_at),
                        "accepted_at": iso(accepted),
                        "valid_until": iso(live.valid_until),
                        "provider": live.spot_provider,
                        "source_as_of": iso(live.as_of),
                        "source_snapshot_sha256": live.artifact_sha256,
                    },
                    "providers": sorted(str(value) for value in provider_values),
                }
            )
            self._write_runtime(accepted)
            self._input_status = "ready"
            self._last_error = None
            return True

    def _read_snapshot(self) -> dict[str, Any] | None:
        try:
            stat = self.snapshot_path.stat()
        except FileNotFoundError:
            self._input_status = "waiting"
            return None
        identity = (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns)
        if identity == self._input_identity:
            return None
        self._input_identity = identity
        if stat.st_size <= 0 or stat.st_size > MAX_LIVE_SNAPSHOT_BYTES:
            raise LiveSnapshotError("live_snapshot_size_invalid")
        try:
            payload = json.loads(self.snapshot_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise LiveSnapshotError("live_snapshot_unreadable") from exc
        if not isinstance(payload, dict):
            raise LiveSnapshotError("live_snapshot_not_object")
        return payload

    def poll_once(self, *, now: datetime | None = None) -> bool:
        current = as_utc(now or self.utcnow())
        with self._lock:
            self._last_poll_at = current
            self._rollover_for_clock(current)
            changed = self._freeze_due(current)
            try:
                payload = self._read_snapshot()
                if payload is None:
                    self._refresh_input_status(current)
                    return changed
                validated = validate_live_snapshot(payload)
                completed = as_utc(current if now is not None else self.utcnow())
                live = validated.stamp(completed)
                changed = self.accept(live, accepted_at=completed) or changed
                current = completed
            except LiveSnapshotError as exc:
                code = str(exc)
                self._last_error = code
                self._input_status = "waiting" if code == "live_snapshot_not_rth" else "invalid"
            self._refresh_input_status(current)
            return changed

    def _refresh_input_status(self, now: datetime) -> None:
        if self._runtime is None:
            if self._input_status not in {"invalid", "waiting"}:
                self._input_status = "waiting"
            return
        raw_valid = self._runtime.get("last_valid_until")
        if isinstance(raw_valid, str):
            try:
                valid_until = parse_clock(raw_valid, code="live_runtime_lease_invalid")
            except LiveSnapshotError:
                self._input_status = "invalid"
                return
            if now >= valid_until:
                self._input_status = "stale"
            elif self._input_status != "invalid":
                self._input_status = "ready"

    def run_loop(
        self,
        *,
        stop_event: threading.Event,
        poll_seconds: float = DEFAULT_POLL_SECONDS,
    ) -> None:
        if not math.isfinite(poll_seconds) or poll_seconds <= 0:
            raise ValueError("poll_seconds must be positive and finite")
        while not stop_event.is_set():
            started = time.monotonic()
            self.poll_once()
            elapsed = max(time.monotonic() - started, 0.0)
            stop_event.wait(max(poll_seconds - elapsed, 0.0))

    def session_surface(
        self,
        selector: LiveSelector,
        *,
        now: datetime | None = None,
    ) -> dict[str, object]:
        from spx_spark.surface_live_session_projection import build_live_session_surface

        request_clock = as_utc(now or self.utcnow())
        with self._lock:
            self._rollover_for_clock(request_clock)
            self._freeze_due(request_clock)
            if self._manifest is None or self._runtime is None or self._active_date is None:
                raise LiveSessionError("live_session_unavailable")
            active_date = self._active_date
            manifest = self._manifest
            runtime = copy.deepcopy(self._runtime)
            boundaries = tuple(self._boundaries)
        with self._projection_lock:
            return build_live_session_surface(
                selector=selector,
                request_as_of=request_clock,
                active_date=active_date,
                manifest=manifest,
                runtime=runtime,
                boundaries=boundaries,
                build_cache=self._build_cache,
                finished_at=self.utcnow,
            )


__all__ = (
    "DEFAULT_POLL_SECONDS",
    "LiveInput",
    "LiveSessionAccumulator",
    "parse_live_snapshot",
)
