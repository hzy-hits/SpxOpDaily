"""Narrow Python-to-Rust desk-map projection adapter.

Python computes research and market structure.  This module exports those facts
as one atomic, versioned document; it does not decide whether or where to send a
human notification.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

from spx_spark.application.order_map.operator_status import (
    build_desk_map_projection,
    build_desk_message_sections,
)
from spx_spark.application.order_map.report_clock import rth_report_slot
from spx_spark.config import StorageSettings
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR, ET
from spx_spark.state_io import atomic_write_json_secure, read_json_object


SCHEMA_VERSION = "desk_map_projection.v1"
DEFAULT_RTH_TTL = timedelta(minutes=20)
DEFAULT_GTH_TTL = timedelta(minutes=65)


def rust_report_owner_enabled() -> bool:
    """Return the explicit single-writer cutover switch, rejecting typos."""

    raw = os.getenv("SPX_RUST_REPORT_OWNER", "false").strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off", ""}:
        return False
    raise ValueError("SPX_RUST_REPORT_OWNER must be a boolean")


def projection_path(storage: StorageSettings) -> Path:
    configured = os.getenv("SPX_RUST_DESK_PROJECTION_PATH", "").strip()
    if configured:
        return Path(configured)
    return Path(storage.data_root) / "latest" / "desk_map_projection.json"


def build_desk_map_wire(
    payload: Mapping[str, Any],
    changes: list[str],
    *,
    now: datetime,
    trading_date: str,
    storage: StorageSettings,
    published_at: datetime | None = None,
) -> dict[str, Any]:
    """Build the complete source projection consumed by the Rust report lane."""

    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("desk-map projection requires timezone-aware now")
    now = now.astimezone(timezone.utc)
    published_at = published_at or now
    if published_at.tzinfo is None or published_at.utcoffset() is None:
        raise ValueError("desk-map projection requires timezone-aware published_at")
    published_at = published_at.astimezone(timezone.utc)
    if published_at < now:
        raise ValueError("desk-map published_at cannot precede evaluation time")
    rth_slot = rth_report_slot(now)
    rth_open = DEFAULT_MARKET_CALENDAR.is_rth_open(now)
    session = "rth" if rth_open else "gth"
    slot_key = (
        rth_slot.key if rth_slot is not None else _projection_slot_key(now, trading_date, session)
    )
    valid_until = published_at + (DEFAULT_RTH_TTL if rth_open else DEFAULT_GTH_TTL)
    projection = build_desk_map_projection(payload)
    sections = build_desk_message_sections(payload, now)
    stage = projection.stage.value.lower()
    quality = projection.data_quality.lower()
    quality_reasons = list(projection.quality_reasons)
    if stage == "ready" and (
        projection.direction not in {"up", "down"} or projection.thesis not in {"breakout", "fade"}
    ):
        # Preserve the legacy human card, but do not claim a typed READY state
        # at the Rust boundary unless the full direction/thesis is representable.
        stage = "paused"
        quality = "degraded"
        if projection.direction not in {"up", "down"}:
            quality_reasons.append("ready_direction_missing")
        if projection.thesis not in {"breakout", "fade"}:
            quality_reasons.append("ready_thesis_missing")
        quality_reasons = list(dict.fromkeys(quality_reasons))
    fingerprint = _structure_fingerprint(payload, projection, slot_key)
    observed_through = _observed_through(payload, now)
    research_context, research_context_reason = _research_context(
        storage,
        published_at,
        trading_date=trading_date,
        session=session,
    )
    if research_context_reason is not None:
        quality_reasons.append(research_context_reason)
        quality_reasons = list(dict.fromkeys(quality_reasons))
        if quality == "ready":
            quality = "degraded"
    research_document_id = (
        str(research_context["document_id"]) if research_context is not None else None
    )
    structure = sections.structure
    if changes:
        structure = f"{structure}\nChanges: {'；'.join(changes)}"
    desk_view = f"{sections.desk_view}\n{_operator_context(payload)}"
    data_quality = sections.data_quality
    if research_context_reason is not None:
        data_quality = f"{data_quality}\nResearch: unavailable ({research_context_reason})"

    document: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "projection_id": "pending",
        "source_snapshot_id": _source_snapshot_id(payload, fingerprint),
        "source_slot": slot_key,
        "trading_date_et": trading_date,
        "session": session,
        "observed_through": observed_through.isoformat().replace("+00:00", "Z"),
        "available_at": published_at.isoformat().replace("+00:00", "Z"),
        "valid_until": valid_until.isoformat().replace("+00:00", "Z"),
        "structure_fingerprint": fingerprint,
        "stage": stage,
        "phase": projection.phase.value,
        "direction": projection.direction,
        "thesis": projection.thesis,
        "level_kind": projection.level_kind or None,
        "level": projection.level,
        "quality": quality,
        "quality_reasons": quality_reasons,
        "research_context_document_id": research_document_id,
        "research_context": research_context,
        "action_authority": "none",
        "automatic_ordering": False,
        "message": {
            **asdict(sections),
            "desk_view": desk_view,
            "structure": structure,
            "data_quality": data_quality,
        },
    }
    identity_payload = {key: value for key, value in document.items() if key != "projection_id"}
    identity = _sha256(identity_payload)
    document["projection_id"] = f"desk-map:{identity[:24]}"
    return document


def persist_desk_map_projection(
    payload: Mapping[str, Any],
    changes: list[str],
    *,
    now: datetime,
    trading_date: str,
    storage: StorageSettings,
    published_at: datetime | None = None,
) -> dict[str, Any]:
    document = build_desk_map_wire(
        payload,
        changes,
        now=now,
        trading_date=trading_date,
        storage=storage,
        published_at=published_at,
    )
    atomic_write_json_secure(projection_path(storage), document)
    return document


def _operator_context(payload: Mapping[str, Any]) -> str:
    from spx_spark.application.order_map.status_explanation import operator_reason_line

    return operator_reason_line(dict(payload))


def _source_snapshot_id(payload: Mapping[str, Any], fallback: str) -> str:
    for key in ("option_structure_frame", "minute_market_frame"):
        value = payload.get(key)
        if isinstance(value, Mapping):
            candidate = str(value.get("frame_id") or value.get("snapshot_id") or "").strip()
            if candidate:
                return candidate
    decision = payload.get("level_decision")
    if isinstance(decision, Mapping):
        candidate = str(decision.get("event_id") or "").strip()
        if candidate:
            return candidate
    return f"snapshot:{fallback[:24]}"


def _observed_through(payload: Mapping[str, Any], now: datetime) -> datetime:
    candidates: list[object] = []
    for key in ("option_structure_frame", "minute_market_frame"):
        value = payload.get(key)
        if isinstance(value, Mapping):
            candidates.extend((value.get("observed_through"), value.get("available_at")))
    candidates.extend((payload.get("as_of"), payload.get("generated_at")))
    for candidate in candidates:
        if not candidate:
            continue
        try:
            parsed = datetime.fromisoformat(str(candidate).replace("Z", "+00:00"))
        except ValueError:
            continue
        if parsed.tzinfo is not None and parsed <= now:
            return parsed.astimezone(timezone.utc)
    return now


def _research_context(
    storage: StorageSettings,
    published_at: datetime,
    *,
    trading_date: str,
    session: str,
) -> tuple[dict[str, Any] | None, str | None]:
    document = read_json_object(
        Path(storage.data_root) / "latest" / "experimental_research_signals.json"
    )
    if document.get("schema_version") != "research_context.v2":
        return None, None
    if session != "rth":
        return None, None

    frame = document.get("cross_index_frame")
    prior = document.get("prior_rth_context")
    regime = document.get("regime")
    forecasts = document.get("forecasts")
    close_location = document.get("close_location")
    if (
        not isinstance(frame, Mapping)
        or not isinstance(prior, Mapping)
        or (regime is not None and not isinstance(regime, Mapping))
        or not isinstance(forecasts, list)
        or len(forecasts) != 3
        or not isinstance(close_location, Mapping)
        or not str(document.get("document_id") or "").strip()
        or document.get("action_authority") != "none"
        or document.get("automatic_ordering") is not False
    ):
        return None, "research_context_contract_invalid"

    context_dates = {
        frame.get("trading_date_et"),
        prior.get("for_trading_date"),
        regime.get("trading_date_et") if isinstance(regime, Mapping) else trading_date,
    }
    if context_dates != {trading_date}:
        return None, "research_context_trading_date_mismatch"

    try:
        generated_at = datetime.fromisoformat(
            str(document.get("generated_at") or "").replace("Z", "+00:00")
        )
        target_values = [
            *(item.get("target_at") for item in forecasts if isinstance(item, Mapping)),
            close_location.get("target_at"),
        ]
        target_dates = {
            datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            .astimezone(ET)
            .date()
            .isoformat()
            for value in target_values
        }
    except (TypeError, ValueError):
        return None, "research_context_contract_invalid"
    if len(target_values) != 4 or target_dates != {trading_date}:
        return None, "research_context_trading_date_mismatch"
    if generated_at.tzinfo is None or generated_at.astimezone(timezone.utc) > published_at:
        return None, "research_context_from_future"
    return dict(document), None


def _structure_fingerprint(
    payload: Mapping[str, Any],
    projection: object,
    slot_key: str,
) -> str:
    source = {
        "slot": slot_key,
        "stage": getattr(getattr(projection, "stage"), "value"),
        "phase": getattr(getattr(projection, "phase"), "value"),
        "direction": getattr(projection, "direction"),
        "thesis": getattr(projection, "thesis"),
        "level_kind": getattr(projection, "level_kind"),
        "level": getattr(projection, "level"),
        "quality": getattr(projection, "data_quality"),
        "quality_reasons": getattr(projection, "quality_reasons"),
        "underlier": payload.get("underlier"),
        "es_last": payload.get("es_last"),
        "flip_zone": payload.get("flip_zone"),
    }
    return _sha256(source)


def _sha256(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _projection_slot_key(now: datetime, trading_date: str, session: str) -> str:
    return f"{trading_date}:{session}:{now.strftime('%H:%M')}"


__all__ = [
    "SCHEMA_VERSION",
    "build_desk_map_wire",
    "persist_desk_map_projection",
    "projection_path",
    "rust_report_owner_enabled",
]
