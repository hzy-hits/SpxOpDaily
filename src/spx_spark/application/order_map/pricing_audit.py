"""Durable inputs and outputs for replaying order-map pricing reports."""

from __future__ import annotations

import fcntl
import json
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1


def build_pricing_audit_record(
    payload: Mapping[str, Any],
    *,
    generated_at: datetime,
    report_kind: str,
    template: str,
    delivered_text: str,
    writer: str,
    delivered_ok: bool,
    notification_event_id: str | None = None,
    delivery_outcome: str | None = None,
    queued_for_recovery: bool | None = None,
    occurred_at: datetime | None = None,
    report_slot_key: str | None = None,
    persisted_at: datetime | None = None,
) -> dict[str, Any]:
    """Keep the exact model scenario separately from the prose writer."""

    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "persisted_at": persisted_at.isoformat() if persisted_at is not None else None,
        "occurred_at": occurred_at.isoformat() if occurred_at is not None else None,
        "report_slot_key": report_slot_key,
        "snapshot_as_of": payload.get("as_of"),
        "report_kind": report_kind,
        "trading_date": payload.get("trading_date"),
        "expiry": payload.get("expiry"),
        "underlier": payload.get("underlier"),
        "pricing_reference": payload.get("pricing_reference"),
        "expected_move_points": payload.get("expected_move_points"),
        "strike_price_coverage": payload.get("strike_price_coverage"),
        "spring_gamma_v3_shadow": (
            dict(payload["spring_gamma_v3_shadow"])
            if isinstance(payload.get("spring_gamma_v3_shadow"), Mapping)
            else None
        ),
        "spring_gamma_v3_state_window": (
            dict(payload["spring_gamma_v3_state_window"])
            if isinstance(payload.get("spring_gamma_v3_state_window"), Mapping)
            else None
        ),
        "spring_gamma_v3_projection_diagnostic": (
            dict(payload["spring_gamma_v3_projection_diagnostic"])
            if isinstance(payload.get("spring_gamma_v3_projection_diagnostic"), Mapping)
            else None
        ),
        "candidates": payload.get("candidates")
        if isinstance(payload.get("candidates"), list)
        else [],
        "wall_ladder": payload.get("wall_ladder"),
        "warnings": payload.get("warnings") if isinstance(payload.get("warnings"), list) else [],
        "template": template,
        "delivered_text": delivered_text,
        "writer": writer,
        "delivered_ok": delivered_ok,
        "notification_event_id": notification_event_id,
        "delivery_outcome": delivery_outcome,
        "queued_for_recovery": queued_for_recovery,
    }


def append_pricing_audit(data_root: str, record: Mapping[str, Any]) -> Path:
    trading_date = str(record.get("trading_date") or "unknown")
    target = (
        Path(data_root).expanduser()
        / "audit"
        / "order_map_pricing"
        / f"date={trading_date}"
        / "reports.jsonl"
    )
    lock_path = target.with_suffix(".lock")
    target.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(dict(record), sort_keys=True, separators=(",", ":"))
    with lock_path.open("a+", encoding="utf-8") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        try:
            with target.open("a", encoding="utf-8") as handle:
                handle.write(serialized)
                handle.write("\n")
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
    return target
