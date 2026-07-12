from __future__ import annotations

import json
from datetime import datetime

from spx_spark.alert_engine.rules_data import find_best, quote_health_alert
from spx_spark.alert_engine.rules_options import (
    iv_surface_alerts,
    iv_surface_freshness_alert,
    option_map_alerts,
    persist_gamma_regime_observations,
)
from spx_spark.alert_engine.rules_price import movement_alerts
from spx_spark.alert_engine.rules_system import (
    market_context_alerts,
    proxy_fallback_watch_alerts,
    system_event_alerts,
)
from spx_spark.alert_model import Alert
from spx_spark.alert_profile import AlertWindow, active_window
from spx_spark.config import IvSurfaceSettings, StorageSettings
from spx_spark.human_focus import build_human_focus_context
from spx_spark.iv_surface import (
    IvSurfaceSnapshot,
    load_latest_snapshot,
    load_recent_snapshots,
    summarize_surface_history,
)
from spx_spark.market_context import build_market_context
from spx_spark.options_map import OptionsMap, build_options_map
from spx_spark.position_alerts import position_holdings_alerts
from spx_spark.settings import DEFAULT_ALERT_SETTINGS, AlertSettings
from spx_spark.storage import LatestState
from spx_spark.strategy.steven import (
    annotate_alerts_with_steven_context,
    load_steven_state_for_alerts,
)


def evaluate_alerts(
    state: LatestState,
    *,
    window: AlertWindow,
    options_map: OptionsMap | None = None,
    iv_surface: IvSurfaceSnapshot | None = None,
    iv_surface_history_1h: dict[str, object] | None = None,
    market_context: dict[str, object] | None = None,
    persist_system_events: bool = False,
    persist_movement_state: bool = False,
) -> list[Alert]:
    alerts: list[Alert] = []
    required = set(window.required_instruments)
    optional = set(window.optional_instruments)
    for instrument_id in sorted(required | optional):
        alert = quote_health_alert(
            instrument_id=instrument_id,
            quote=find_best(state, instrument_id),
            window=window,
            required=instrument_id in required,
        )
        if alert is not None:
            alerts.append(alert)

    if market_context is None:
        market_context = build_market_context(state)
    alerts.extend(
        movement_alerts(
            state,
            window=window,
            market_context=market_context,
            persist=persist_movement_state,
            options_map=options_map,
        )
    )

    alerts.extend(option_map_alerts(options_map or build_options_map(state), window=window))
    if iv_surface is not None:
        alerts.extend(
            iv_surface_alerts(
                iv_surface,
                window=window,
                history_1h=iv_surface_history_1h,
            )
        )
    alerts.extend(position_holdings_alerts(state, options_map=options_map, window=window))
    alerts.extend(market_context_alerts(market_context))
    alerts.extend(system_event_alerts(state, persist=persist_system_events))
    alerts.extend(
        proxy_fallback_watch_alerts(
            state, window=window, market_context=market_context, options_map=options_map
        )
    )
    return alerts



def load_current_iv_surface(settings: IvSurfaceSettings | None = None) -> IvSurfaceSnapshot | None:
    settings = settings or IvSurfaceSettings.from_env()
    try:
        return load_latest_snapshot(settings.latest_surface_path)
    except (OSError, ValueError, json.JSONDecodeError, KeyError):
        return None


def evaluate_payload(
    state: LatestState,
    *,
    now: datetime | None = None,
    persist_system_events: bool = True,
    persist_movement_state: bool = False,
    persist_gamma_regime: bool = False,
    alert_settings: AlertSettings | None = None,
) -> dict[str, object]:
    now = now or state.as_of
    policy = alert_settings or DEFAULT_ALERT_SETTINGS
    window = active_window(now)
    window_payload = window.to_dict(now=now)
    options_map = build_options_map(state)
    if persist_gamma_regime:
        # Record observations before alert evaluation: a state seen for the
        # first time starts its hysteresis clock now and only alerts once it
        # has held for the configured window.
        persist_gamma_regime_observations(options_map, as_of=options_map.as_of)
    iv_settings = IvSurfaceSettings.from_env()
    iv_surface = load_current_iv_surface(iv_settings)
    iv_surface_history = load_recent_snapshots(iv_settings, as_of=state.as_of, lookback_minutes=60)
    iv_surface_history_1h = summarize_surface_history(iv_surface, iv_surface_history)
    market_context = build_market_context(state)
    iv_stale_alert = (
        iv_surface_freshness_alert(iv_surface, now=state.as_of) if iv_surface is not None else None
    )
    iv_surface_for_alerts = None if iv_stale_alert is not None else iv_surface
    alerts = evaluate_alerts(
        state,
        window=window,
        options_map=options_map,
        iv_surface=iv_surface_for_alerts,
        iv_surface_history_1h=iv_surface_history_1h,
        market_context=market_context,
        persist_system_events=persist_system_events,
        persist_movement_state=persist_movement_state,
    )
    if iv_stale_alert is not None:
        alerts.append(iv_stale_alert)
    # Steven observe-only context: read-only note on selected alert kinds.
    if policy.steven_alert_context_enabled:
        try:
            steven_state = load_steven_state_for_alerts(StorageSettings.from_env().data_root)
            alerts = annotate_alerts_with_steven_context(
                alerts,
                steven_state,
                as_of=state.as_of,
            )
        except Exception:  # noqa: BLE001 — never block alerts on context failure
            pass
    return {
        "created_at": datetime.now(tz=now.tzinfo).isoformat(),
        "as_of": state.as_of.isoformat(),
        "window": window_payload,
        "market_context": market_context,
        "human_focus_context": build_human_focus_context(
            state,
            options_map=options_map,
            iv_surface=iv_surface,
            iv_surface_history_1h=iv_surface_history_1h,
            window=window_payload,
        ),
        "options_map": options_map.to_dict(),
        "iv_surface": iv_surface.to_dict() if iv_surface is not None else None,
        "iv_surface_history_1h": iv_surface_history_1h,
        "alert_count": len(alerts),
        "alerts": [alert.to_dict() for alert in alerts],
    }


