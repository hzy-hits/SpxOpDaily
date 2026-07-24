"""Fail-closed Spring Gamma v3 projection for order-map reports."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from spx_spark.analytics.options.pricing import finite_float
from spx_spark.application.market_features.spring_gamma_v3_io import (
    latest_spring_gamma_v3_shadow_path,
    validate_spring_gamma_v3_shadow,
)
from spx_spark.application.market_features.state import load_json


def attach_spring_gamma_v3_shadow(
    payload: dict[str, Any],
    data_root: str | Path,
    *,
    settings: object,
    now: datetime,
) -> None:
    """Mount only a current, identity-matched, non-authoritative shadow."""

    payload.pop("spring_gamma_v3_shadow", None)
    report_enabled = bool(getattr(settings, "report_enabled", False))
    interval = finite_float(getattr(settings, "prediction_interval_seconds", 0))
    max_age_seconds = max((interval or 0.0) * 2.0, 120.0)
    if not report_enabled or now.tzinfo is None or now.utcoffset() is None:
        return
    candidate = load_json(latest_spring_gamma_v3_shadow_path(data_root))
    if not candidate:
        return
    try:
        shadow = validate_spring_gamma_v3_shadow(candidate)
        shadow_as_of = datetime.fromisoformat(str(shadow["as_of"]))
    except (TypeError, ValueError):
        return
    age_seconds = (now - shadow_as_of).total_seconds()
    if age_seconds < 0 or age_seconds > max_age_seconds:
        return

    expected_expiry = str(payload.get("expiry") or "")
    if not expected_expiry or str(shadow.get("expiry") or "") != expected_expiry:
        return
    frame = (
        payload.get("minute_market_frame")
        if isinstance(payload.get("minute_market_frame"), dict)
        else {}
    )
    expected_sessions = {
        value
        for value in (
            str(frame.get("session_id") or ""),
            str(payload.get("trading_date") or ""),
        )
        if value
    }
    shadow_session = str(shadow.get("session_id") or "")
    if not expected_sessions or any(
        shadow_session != expected_session for expected_session in expected_sessions
    ):
        return

    diagnostics = frame.get("diagnostics")
    frame_diagnostics = diagnostics if isinstance(diagnostics, dict) else {}
    segment = str(frame.get("session") or frame_diagnostics.get("segment") or "").strip().lower()
    expected_session = (
        "rth"
        if segment == "rth"
        else "gth"
        if segment in {"asia", "europe", "us_premarket", "curb", "gth"}
        else ""
    )
    if segment and not expected_session:
        return
    if expected_session and str(shadow.get("session") or "") != expected_session:
        return
    payload["spring_gamma_v3_shadow"] = shadow
