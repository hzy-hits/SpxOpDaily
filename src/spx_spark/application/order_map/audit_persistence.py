"""Persistence boundaries for order-map reports and Greek audit snapshots."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from typing import Any, Callable

from spx_spark.application.order_map.pricing_audit import (
    append_pricing_audit,
    build_pricing_audit_record,
)
from spx_spark.config import StorageSettings


def persist_zero_dte_greeks_reference(
    payload: dict[str, Any],
    storage_settings: StorageSettings,
    *,
    writer: Callable[..., object],
) -> None:
    reference = payload.get("_spxw_0dte_greeks_audit")
    if not isinstance(reference, dict):
        reference = payload.get("spxw_0dte_greeks_reference")
    data_root = getattr(storage_settings, "data_root", None)
    if not isinstance(reference, dict) or not isinstance(data_root, str) or not data_root:
        return
    try:
        writer(reference, data_root=data_root)
    except OSError as exc:
        print(f"0DTE Greeks snapshot write failed: {exc}", file=sys.stderr)


def persist_order_map_pricing_audit(
    payload: dict[str, Any],
    storage_settings: StorageSettings,
    *,
    now: datetime,
    report_kind: str,
    template: str,
    result: dict[str, Any],
) -> None:
    try:
        append_pricing_audit(
            storage_settings.data_root,
            build_pricing_audit_record(
                payload,
                generated_at=now,
                report_kind=report_kind,
                template=template,
                delivered_text=str(result.get("text") or ""),
                writer=str(result.get("writer") or "unknown"),
                delivered_ok=result.get("delivered_ok") is True,
                notification_event_id=(
                    str(result["notification_event_id"])
                    if result.get("notification_event_id")
                    else None
                ),
                delivery_outcome=(
                    str(result["delivery_outcome"]) if result.get("delivery_outcome") else None
                ),
                queued_for_recovery=(
                    result.get("queued_for_recovery")
                    if isinstance(result.get("queued_for_recovery"), bool)
                    else None
                ),
                occurred_at=(
                    datetime.fromisoformat(str(result["occurred_at"]))
                    if result.get("occurred_at")
                    else None
                ),
                report_slot_key=(
                    str(result["report_slot_key"]) if result.get("report_slot_key") else None
                ),
                persisted_at=datetime.now(tz=timezone.utc),
            ),
        )
    except OSError as exc:
        print(f"Order-map pricing audit write failed: {exc}", file=sys.stderr)
