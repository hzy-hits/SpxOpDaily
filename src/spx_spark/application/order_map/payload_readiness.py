"""Thin-payload / GTH EM helpers for order-map snapshot readiness."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from spx_spark.analytics.options.pricing import finite_float
from spx_spark.application.order_map.state import (
    current_session_is_gth,
    payload_fingerprint,
)
from spx_spark.config import StorageSettings
from spx_spark.ibkr.position_watcher import default_positions_path, load_snapshot


def payload_is_thin(payload: dict[str, Any]) -> bool:
    """True when the snapshot caught a mid-rotation flush (missing spot/OI/plays)."""

    research_reference = payload.get("research_reference")
    if not isinstance(research_reference, dict):
        research_reference = {}
    if payload.get("research_only") is True and research_reference.get("price") is not None:
        return False
    underlier = payload.get("underlier") if isinstance(payload.get("underlier"), dict) else {}
    if underlier.get("price") is None:
        return True
    warnings = payload.get("warnings")
    no_open_interest = isinstance(warnings, list) and any(
        "no open interest" in str(item) for item in warnings
    )
    if no_open_interest and current_session_is_gth(payload, {}):
        return False
    if not payload.get("candidates"):
        return True
    if no_open_interest:
        return True
    fingerprint = payload_fingerprint(payload)
    if (
        fingerprint.get("put_wall") is None
        and fingerprint.get("call_wall") is None
        and fingerprint.get("flip_low") is None
    ):
        return True
    return False


def payload_has_retryable_candidate_gap(payload: dict[str, Any]) -> bool:
    """True when an intended play is missing only because its quote is stale.

    Keep this separate from ``payload_is_thin``: if the retry budget expires,
    the status push should still report the degraded candidate instead of
    silently skipping the whole snapshot. Non-fresh feed modes and structural
    play skips remain fail-closed without delaying the push.
    """

    warnings = payload.get("warnings")
    if not isinstance(warnings, list):
        return False
    return any(
        str(item).startswith("bad_quality_for_") and ":transport_stale_after_" in str(item)
        for item in warnings
    )


def recent_market_frame_es(
    frame: dict[str, Any], *, now: datetime, max_age_seconds: float
) -> tuple[float | None, str | None]:
    if frame.get("quality") == "unavailable":
        return None, None
    try:
        as_of = datetime.fromisoformat(str(frame.get("as_of")))
    except ValueError:
        return None, None
    if abs((now - as_of).total_seconds()) > max_age_seconds:
        return None, None
    es = frame.get("es") if isinstance(frame.get("es"), dict) else {}
    return finite_float(es.get("price")), str(es.get("provider") or "") or None


def apply_gth_em_usage(
    payload: dict[str, Any],
    market_frame: dict[str, Any],
) -> None:
    """Anchor EM usage to the current session's 20:15 ET SPX GTH open."""

    day_move = payload.get("day_move")
    es = market_frame.get("es")
    if not isinstance(day_move, dict) or not isinstance(es, dict):
        return
    frame_session = str(market_frame.get("session_id") or "")
    payload_session = str(payload.get("trading_date") or "")
    if not frame_session or frame_session != payload_session:
        return
    gth_open = finite_float(es.get("gth_open_price"))
    current_es = finite_float(payload.get("es_last"))
    if current_es is None:
        current_es = finite_float(es.get("price"))
    expected_move = finite_float(payload.get("expected_move_points"))
    if gth_open is None or current_es is None or expected_move is None or expected_move <= 0:
        return
    em_move = current_es - gth_open
    day_move.update(
        {
            "em_used_fraction": round(abs(em_move) / expected_move, 2),
            "em_move_points": round(em_move, 1),
            "em_baseline": gth_open,
            "em_baseline_source": "es_gth_open",
            "em_session_id": frame_session,
        }
    )


def has_open_position_risk(storage_settings: StorageSettings) -> bool:
    snapshot = load_snapshot(default_positions_path(storage_settings))
    return bool(snapshot and any(position.qty != 0 for position in snapshot.positions))
