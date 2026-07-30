"""Cadence and material-change gate for operator status cards."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from spx_spark.analytics.options.pricing import finite_float
from spx_spark.application.order_map.report_clock import rth_report_slot


STATUS_KEY_WINDOW_PHASES = frozenset(
    ("europe_session", "us_data_hour", "us_open_hour", "us_midday_confirmation")
)
GTH_STATUS_PHASES = frozenset({"asia_globex", "europe_session", "us_data_hour"})
STATUS_SUMMARY_CADENCE_SECONDS = 60.0 * 60.0
GTH_STATUS_SUMMARY_CADENCE_SECONDS = 60.0 * 60.0
RTH_SLOT_LOOKBACK_GRACE_SECONDS = 15.0 * 60.0 - 0.001


def status_delivery_reason(
    previous: dict[str, Any],
    fingerprint: dict[str, Any],
    changes: list[str],
    *,
    now: datetime,
    trading_date: str,
    position_risk: bool,
) -> str | None:
    if previous.get("last_status_date") != trading_date:
        return "initial_status"
    current_rth_slot = rth_report_slot(now)
    if current_rth_slot is not None:
        last_status_at = finite_float(previous.get("last_status_at"))
        previous_rth_slot = (
            rth_report_slot(
                datetime.fromtimestamp(last_status_at, tz=timezone.utc),
                start_grace_seconds=RTH_SLOT_LOOKBACK_GRACE_SECONDS,
            )
            if last_status_at is not None
            else None
        )
        if previous_rth_slot is not None and previous_rth_slot.key == current_rth_slot.key:
            return None
        if changes:
            return "material_changes"
        return f"rth_quarter_hour_heartbeat:{current_rth_slot.key}"
    phase = str(fingerprint.get("status_phase") or "")
    previous_fingerprint = previous.get("status_fingerprint") or previous.get("fingerprint")
    previous_phase = (
        str(previous_fingerprint.get("status_phase") or "")
        if isinstance(previous_fingerprint, dict)
        else ""
    )
    if phase in STATUS_KEY_WINDOW_PHASES and previous_phase != phase:
        return f"key_window:{phase}"
    if position_risk:
        last_status_at = finite_float(previous.get("last_status_at"))
        if (
            last_status_at is None
            or now.timestamp() - last_status_at >= STATUS_SUMMARY_CADENCE_SECONDS
        ):
            return "open_position_risk"
        return None
    if phase in GTH_STATUS_PHASES:
        if changes:
            return "material_changes"
        last_status_at = finite_float(previous.get("last_status_at"))
        if (
            last_status_at is None
            or now.timestamp() - last_status_at
            >= GTH_STATUS_SUMMARY_CADENCE_SECONDS
        ):
            return f"gth_hourly_summary:{phase}"
        return None
    return "material_changes" if changes else None
