"""Morning-map payload construction from LatestState."""

from __future__ import annotations

import json
import os
import time as time_module
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any

from spx_spark.human_focus import build_human_focus_context
from spx_spark.iv_surface import IvSurfaceSettings
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.options_map import build_options_map
from spx_spark.config import StorageSettings
from spx_spark.storage import LatestState, LatestStateStore


def load_current_iv_surface(
    settings: IvSurfaceSettings | None = None,
    *,
    now: datetime | None = None,
):
    # Resolve snapshot loader through the facade for monkeypatch stability.
    from spx_spark import morning_map as mm

    settings = settings or IvSurfaceSettings.from_env()
    try:
        surface = mm.load_latest_snapshot(settings.latest_surface_path)
    except (OSError, ValueError, json.JSONDecodeError, KeyError):
        return None
    if surface is None:
        return None
    current = now or datetime.now(tz=timezone.utc)
    age_seconds = (current - surface.as_of).total_seconds()
    max_age_seconds = float(os.getenv("ALERT_MAX_IV_SURFACE_AGE_SECONDS", "420"))
    active_expiry = DEFAULT_MARKET_CALENDAR.research_expiry(current).strftime("%Y%m%d")
    if age_seconds < -5.0 or age_seconds > max_age_seconds:
        return None
    if surface.front_expiry != active_expiry:
        return None
    return surface


def overnight_gap(state: LatestState) -> dict[str, Any]:
    es_quote = state.best_quote("future:ES")
    spx_quote = state.best_quote("index:SPX")
    es_last = es_quote.effective_price if es_quote else None
    es_prev_close = es_quote.close if es_quote else None
    spx_prev_close = spx_quote.close if spx_quote else None
    gap_points = None
    gap_pct = None
    if es_last is not None and es_prev_close is not None:
        gap_points = es_last - es_prev_close
        if es_last > 0 and es_prev_close > 0:
            gap_pct = gap_points / es_prev_close
    return {
        "es_last": es_last,
        "es_prev_close": es_prev_close,
        "spx_prev_close": spx_prev_close,
        "gap_points": gap_points,
        "gap_pct": gap_pct,
    }


def build_morning_payload(state: LatestState, *, now: datetime | None = None) -> dict[str, Any]:
    evaluation_time = now or state.as_of
    evaluation_state = replace(state, as_of=evaluation_time)
    options_map = build_options_map(evaluation_state)
    iv_surface = load_current_iv_surface(now=evaluation_time)
    focus = build_human_focus_context(
        evaluation_state,
        options_map=options_map,
        iv_surface=iv_surface,
        iv_surface_history_1h=None,
        window={"name": "premarket_map", "priority": "info"},
    )
    return {
        "kind": "morning_map",
        "as_of": state.as_of.isoformat(),
        "trading_date": DEFAULT_MARKET_CALENDAR.research_expiry(evaluation_time).isoformat(),
        "overnight": overnight_gap(state),
        "human_focus_context": focus,
    }


def _morning_payload_is_thin(payload: dict[str, Any]) -> bool:
    """True when the snapshot caught a slow-poll/rotation gap (no walls at all)."""
    focus = payload.get("human_focus_context")
    if not isinstance(focus, dict):
        return True
    spxw = focus.get("spxw_options") if isinstance(focus.get("spxw_options"), dict) else {}
    expiries = spxw.get("expiries") if isinstance(spxw.get("expiries"), list) else []
    front = expiries[0] if expiries and isinstance(expiries[0], dict) else {}
    return front.get("put_wall") is None and front.get("call_wall") is None


def build_morning_payload_with_retry(
    storage_settings: StorageSettings,
    *,
    now: datetime | None = None,
    attempts: int = 6,
    delay_seconds: float = 10.0,
) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for attempt in range(attempts):
        state = LatestStateStore(storage_settings).load(now=now)
        payload = build_morning_payload(state, now=now)
        if not _morning_payload_is_thin(payload):
            return payload
        if attempt < attempts - 1:
            time_module.sleep(delay_seconds)
    return payload

