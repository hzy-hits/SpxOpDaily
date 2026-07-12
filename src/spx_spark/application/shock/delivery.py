"""Delivery acknowledgements, greek-shadow markers, and notify payload shaping."""

from __future__ import annotations

from datetime import datetime, timezone

from spx_spark.alert_model import Alert
from spx_spark.alert_profile import active_window
from spx_spark.application.shock.models import RECLAIM_KIND, SHOCK_KIND
from spx_spark.intraday_strategy import STRATEGY_KINDS, mark_strategy_alert_attempts
from spx_spark.marketdata import as_utc
from spx_spark.storage import LatestState

def mark_alert_attempts(
    state: dict[str, object], alerts: list[Alert], *, at: datetime, delivered: bool
) -> dict[str, object]:
    state = dict(state)
    strategy_event_ids = {
        str(alert.event_id) for alert in alerts if alert.kind in STRATEGY_KINDS and alert.event_id
    }
    if strategy_event_ids:
        state = mark_strategy_alert_attempts(
            state,
            event_ids=strategy_event_ids,
            at=at,
            delivered=delivered,
        )
    active = state.get("active_event")
    if not isinstance(active, dict):
        return state
    event = dict(active)
    for alert in alerts:
        if alert.event_id != event.get("event_id"):
            continue
        if alert.kind == SHOCK_KIND:
            phase = "shock"
        elif alert.kind == RECLAIM_KIND:
            phase = "reclaim"
        else:
            continue
        event[f"{phase}_last_attempt_at"] = as_utc(at).isoformat()
        if delivered:
            event[f"{phase}_delivered"] = True
            event[f"{phase}_delivered_at"] = as_utc(at).isoformat()
    state["active_event"] = event
    return state


def reconcile_acknowledged_alerts(
    state: dict[str, object],
    alerts: list[Alert],
    *,
    acknowledged_event_ids: set[str],
    at: datetime,
) -> tuple[dict[str, object], list[Alert]]:
    """Recover delivery after notifier state committed before monitor state."""

    recovered = [
        alert
        for alert in alerts
        if alert.dedup_group is not None and alert.dedup_group in acknowledged_event_ids
    ]
    if recovered:
        state = mark_alert_attempts(state, recovered, at=at, delivered=True)
    return state, [alert for alert in alerts if alert not in recovered]


def event_greek_shadow_due(state: dict[str, object], alert: Alert) -> bool:
    if alert.kind not in {SHOCK_KIND, RECLAIM_KIND} or not alert.event_id:
        return False
    phase = "shock" if alert.kind == SHOCK_KIND else "reclaim"
    for key in ("active_event", "last_event"):
        event = state.get(key)
        if isinstance(event, dict) and event.get("event_id") == alert.event_id:
            return not bool(event.get(f"{phase}_greeks_sampled_at"))
    return False


def mark_event_greek_shadow_sampled(
    state: dict[str, object],
    alerts: list[Alert],
    *,
    at: datetime,
) -> dict[str, object]:
    state = dict(state)
    for key in ("active_event", "last_event"):
        raw = state.get(key)
        if not isinstance(raw, dict):
            continue
        event = dict(raw)
        for alert in alerts:
            if event.get("event_id") != alert.event_id:
                continue
            if alert.kind == SHOCK_KIND:
                phase = "shock"
            elif alert.kind == RECLAIM_KIND:
                phase = "reclaim"
            else:
                continue
            event[f"{phase}_greeks_sampled_at"] = as_utc(at).isoformat()
        state[key] = event
    return state


def _notification_payload(
    state: LatestState,
    monitor_state: dict[str, object],
    alerts: list[Alert],
) -> dict[str, object]:
    return {
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
        "as_of": state.as_of.isoformat(),
        "window": active_window(state.as_of).to_dict(now=state.as_of),
        "human_focus_context": {
            "prices": {
                "spx": state.best_quote("index:SPX").effective_price
                if state.best_quote("index:SPX")
                else None,
                "es": state.best_quote("future:ES").effective_price
                if state.best_quote("future:ES")
                else None,
            },
            "intraday_shock": monitor_state.get("active_event"),
            "conditional_call_bias": (
                monitor_state.get("call_strategy", {}).get("active_bias")
                if isinstance(monitor_state.get("call_strategy"), dict)
                else None
            ),
        },
        "alert_count": len(alerts),
        "alerts": [alert.to_dict() for alert in alerts],
    }
